"""Sending records to the CRM, the MongoDB state, and one sync of one sheet."""
from __future__ import annotations

import copy
import httpx
import logging
import random
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pymongo import ASCENDING, MongoClient, ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError

from .settings import CRMProfile, Settings
from .sheets import Plan, PlanError, PlannedRow, PublicSheetReader, SheetData, SheetReadError, SheetRef, plan_rows


log = logging.getLogger(__name__)

RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504}
KNOWN_OUTCOMES = {"created", "joined_existing", "rejected"}


class CRMError(Exception):
    pass


class CRMConfigError(CRMError):
    """The key or the address is wrong. Every record would fail the same way: stop and alert."""


class CRMRejectedError(CRMError):
    """The CRM refused this record for its content. Sending it again unchanged will not help."""


class CRMUnavailableError(CRMError):
    """The CRM is busy or down. Try again later."""


@dataclass(frozen=True)
class PostResult:
    outcome: str              # created | joined_existing | rejected | accepted
    record_id: str | None     # the CRM's id for the record, when it says
    detail: str | None = None
    crm_status: str | None = None   # where the CRM's reviewer got to: e.g. needs_person


class RateLimiter:
    """At most `per_second` calls per second across all threads of this process."""

    def __init__(self, per_second: float, clock=time.monotonic, sleep=time.sleep):
        self._interval = 1.0 / per_second
        self._next = 0.0
        self._lock = threading.Lock()
        self._clock, self._sleep = clock, sleep

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            start = max(now, self._next)
            self._next = start + self._interval
        if start > now:
            self._sleep(start - now)


def _parse(resp: httpx.Response) -> tuple[dict, str | None]:
    try:
        body = resp.json()
    except ValueError:
        return {}, None
    if not isinstance(body, dict):
        return {}, None
    inner = body.get("result") if isinstance(body.get("result"), dict) else body
    detail = inner.get("reason") or inner.get("message") or inner.get("error") or body.get("error")
    if isinstance(detail, dict):
        detail = detail.get("message") or str(detail)
    return inner, (str(detail)[:500] if detail else None)


class RecordsClient:
    def __init__(self, profile: CRMProfile, *, http: httpx.Client | None = None, timeout: float = 20.0,
                 max_requests_per_second: float = 5.0, retries: int = 3, backoff_seconds: float = 1.0,
                 max_retry_after: float = 60.0, sleep=time.sleep, jitter=random.random,
                 limiter: RateLimiter | None = None):
        self.profile = profile
        self._http = http or httpx.Client(timeout=timeout)
        self._limiter = limiter or RateLimiter(max_requests_per_second)
        self._retries, self._backoff, self._max_retry_after = retries, backoff_seconds, max_retry_after
        self._sleep, self._jitter = sleep, jitter

    def post(self, record: dict) -> PostResult:
        headers = {
            "Authorization": f"Bearer {self.profile.key}",
            "Content-Type": "application/json",
            "User-Agent": "sheets-connector/1.0",
        }
        if record.get("external_id"):
            headers["Idempotency-Key"] = str(record["external_id"])
        last_error = "no attempt made"
        for attempt in range(self._retries + 1):
            self._limiter.wait()
            try:
                resp = self._http.post(self.profile.records_url, json=record, headers=headers)
            except httpx.HTTPError as exc:
                last_error = f"network error: {exc.__class__.__name__}"
                self._pause(attempt, None)
                continue
            body, detail = _parse(resp)
            if 200 <= resp.status_code < 300:
                outcome = next((str(body[k]) for k in ("outcome", "status", "result")
                                if isinstance(body.get(k), str) and body[k] in KNOWN_OUTCOMES), "accepted")
                record_id = next((str(body[k]) for k in ("external_record_id", "record_id", "id") if body.get(k)), None)
                crm_status = body.get("status") if isinstance(body.get("status"), str) else None
                return PostResult(outcome, record_id, detail, crm_status)
            if resp.status_code in (401, 403):
                raise CRMConfigError(f"The CRM refused the source key (HTTP {resp.status_code}) for profile "
                                     f"{self.profile.name!r}. Make a new key under Account -> Sources.")
            if resp.status_code == 404:
                raise CRMConfigError(f"The records address was not found (HTTP 404) for profile {self.profile.name!r}. "
                                     "Copy it again from Account -> Sources -> Address for records.")
            if resp.status_code in RETRY_STATUSES:
                last_error = f"HTTP {resp.status_code}" + (f": {detail}" if detail else "")
                self._pause(attempt, resp.headers.get("Retry-After"))
                continue
            raise CRMRejectedError(f"HTTP {resp.status_code}" + (f": {detail}" if detail else ""))
        raise CRMUnavailableError(f"gave up after {self._retries + 1} attempts ({last_error})")

    def _pause(self, attempt: int, retry_after: str | None) -> None:
        if attempt >= self._retries:
            return
        delay = self._backoff * (2 ** attempt) + self._jitter()
        if retry_after:
            try:
                delay = max(delay, min(float(retry_after), self._max_retry_after))
            except ValueError:
                pass
        self._sleep(delay)


ROW_STATE_FIELDS = ("content_hash", "status", "attempts", "external_id")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DuplicateSourceError(Exception):
    """This sheet tab is already registered."""


class SourceNotFound(Exception):
    pass


def new_source_id() -> str:
    return uuid.uuid4().hex


class MongoStore:
    def __init__(self, uri: str, db_name: str, client: MongoClient | None = None):
        self._client = client or MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=10_000,
                                             appname="sheets-connector")
        db = self._client[db_name]
        self.sources = db["sheet_sources"]
        self.rows = db["sheet_rows"]

    # ------------------------------------------------------------------ setup / health
    def ensure_indexes(self) -> None:
        self.sources.create_index([("spreadsheet_id", ASCENDING), ("gid", ASCENDING)], unique=True,
                                  name="one_source_per_tab")
        self.sources.create_index([("enabled", ASCENDING), ("next_run_at", ASCENDING)], name="due")
        self.rows.create_index([("source_id", ASCENDING), ("status", ASCENDING)], name="by_status")

    def ping(self) -> bool:
        self._client.admin.command("ping")
        return True

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------ sources
    def create_source(self, doc: dict) -> dict:
        try:
            self.sources.insert_one(doc)
        except DuplicateKeyError as exc:
            raise DuplicateSourceError("This sheet tab is already registered.") from exc
        return doc

    def get_source(self, source_id: str) -> dict:
        doc = self.sources.find_one({"_id": source_id})
        if not doc:
            raise SourceNotFound(source_id)
        return doc

    def list_sources(self) -> list[dict]:
        return list(self.sources.find().sort("created_at", ASCENDING))

    def update_source(self, source_id: str, changes: dict) -> dict:
        doc = self.sources.find_one_and_update({"_id": source_id}, {"$set": {**changes, "updated_at": utcnow()}},
                                               return_document=ReturnDocument.AFTER)
        if not doc:
            raise SourceNotFound(source_id)
        return doc

    def delete_source(self, source_id: str) -> None:
        if self.sources.delete_one({"_id": source_id}).deleted_count == 0:
            raise SourceNotFound(source_id)
        self.rows.delete_many({"source_id": source_id})

    def request_run(self, source_id: str, now: datetime) -> None:
        if self.sources.update_one({"_id": source_id}, {"$set": {"next_run_at": now}}).matched_count == 0:
            raise SourceNotFound(source_id)

    def claim_due_source(self, owner: str, now: datetime, lease_seconds: int) -> dict | None:
        return self.sources.find_one_and_update(
            {"enabled": True, "next_run_at": {"$lte": now},
             "$or": [{"lease_until": None}, {"lease_until": {"$lt": now}}]},
            {"$set": {"lease_owner": owner, "lease_until": now + timedelta(seconds=lease_seconds)}},
            sort=[("next_run_at", ASCENDING)], return_document=ReturnDocument.AFTER)

    def claim_source(self, source_id: str, owner: str, now: datetime, lease_seconds: int) -> dict | None:
        """Take this one sheet for a run, unless another worker holds it."""
        return self.sources.find_one_and_update(
            {"_id": source_id, "$or": [{"lease_until": None}, {"lease_until": {"$lt": now}}]},
            {"$set": {"lease_owner": owner, "lease_until": now + timedelta(seconds=lease_seconds)}},
            return_document=ReturnDocument.AFTER)

    def renew_lease(self, source_id: str, owner: str, until: datetime) -> bool:
        return self.sources.update_one({"_id": source_id, "lease_owner": owner},
                                       {"$set": {"lease_until": until}}).matched_count == 1

    def finish_run(self, source_id: str, owner: str, changes: dict) -> bool:
        """Save the run's result and release the lease. False if the lease was lost to another worker."""
        return self.sources.update_one(
            {"_id": source_id, "lease_owner": owner},
            {"$set": {**changes, "lease_owner": None, "lease_until": None, "updated_at": utcnow()}}).matched_count == 1

    # ------------------------------------------------------------------ rows
    def load_row_states(self, source_id: str) -> dict[str, dict]:
        cursor = self.rows.find({"source_id": source_id}, {f: 1 for f in ("key_hash",) + ROW_STATE_FIELDS})
        return {d["key_hash"]: d for d in cursor}

    def save_row_states(self, source_id: str, states: list[dict], chunk: int = 1000) -> None:
        now = utcnow()
        ops = [UpdateOne({"_id": f"{source_id}:{s['key_hash']}"},
                         {"$set": {**s, "source_id": source_id, "updated_at": now},
                          "$setOnInsert": {"first_seen_at": now}}, upsert=True) for s in states]
        for i in range(0, len(ops), chunk):
            self.rows.bulk_write(ops[i:i + chunk], ordered=False)

    def row_status_counts(self, source_id: str) -> dict[str, int]:
        pipeline = [{"$match": {"source_id": source_id}}, {"$group": {"_id": "$status", "n": {"$sum": 1}}}]
        return {d["_id"]: d["n"] for d in self.rows.aggregate(pipeline)}

    def list_rows(self, source_id: str, status: str | None, limit: int) -> list[dict]:
        query = {"source_id": source_id, **({"status": status} if status else {})}
        return list(self.rows.find(query).sort("updated_at", -1).limit(limit))


class InMemoryStore:
    """Same behaviour as MongoStore, kept in memory. For tests and local trials only."""

    def __init__(self):
        self._sources: dict[str, dict] = {}
        self._rows: dict[str, dict] = {}
        self._lock = threading.RLock()

    def ensure_indexes(self) -> None:
        pass

    def ping(self) -> bool:
        return True

    def close(self) -> None:
        pass

    def create_source(self, doc: dict) -> dict:
        with self._lock:
            if any(s["spreadsheet_id"] == doc["spreadsheet_id"] and s["gid"] == doc["gid"]
                   for s in self._sources.values()):
                raise DuplicateSourceError("This sheet tab is already registered.")
            self._sources[doc["_id"]] = copy.deepcopy(doc)
            return copy.deepcopy(doc)

    def get_source(self, source_id: str) -> dict:
        with self._lock:
            if source_id not in self._sources:
                raise SourceNotFound(source_id)
            return copy.deepcopy(self._sources[source_id])

    def list_sources(self) -> list[dict]:
        with self._lock:
            return [copy.deepcopy(s) for s in sorted(self._sources.values(), key=lambda s: s["created_at"])]

    def update_source(self, source_id: str, changes: dict) -> dict:
        with self._lock:
            if source_id not in self._sources:
                raise SourceNotFound(source_id)
            self._sources[source_id].update(copy.deepcopy(changes), updated_at=utcnow())
            return copy.deepcopy(self._sources[source_id])

    def delete_source(self, source_id: str) -> None:
        with self._lock:
            if self._sources.pop(source_id, None) is None:
                raise SourceNotFound(source_id)
            for key in [k for k, r in self._rows.items() if r["source_id"] == source_id]:
                del self._rows[key]

    def request_run(self, source_id: str, now: datetime) -> None:
        self.update_source(source_id, {"next_run_at": now})

    def claim_due_source(self, owner: str, now: datetime, lease_seconds: int) -> dict | None:
        with self._lock:
            due = [s for s in self._sources.values()
                   if s["enabled"] and s["next_run_at"] <= now and (s.get("lease_until") is None or s["lease_until"] < now)]
            if not due:
                return None
            src = min(due, key=lambda s: s["next_run_at"])
            src.update(lease_owner=owner, lease_until=now + timedelta(seconds=lease_seconds))
            return copy.deepcopy(src)

    def claim_source(self, source_id: str, owner: str, now: datetime, lease_seconds: int) -> dict | None:
        with self._lock:
            src = self._sources.get(source_id)
            if not src or (src.get("lease_until") is not None and src["lease_until"] >= now):
                return None
            src.update(lease_owner=owner, lease_until=now + timedelta(seconds=lease_seconds))
            return copy.deepcopy(src)

    def renew_lease(self, source_id: str, owner: str, until: datetime) -> bool:
        with self._lock:
            src = self._sources.get(source_id)
            if not src or src.get("lease_owner") != owner:
                return False
            src["lease_until"] = until
            return True

    def finish_run(self, source_id: str, owner: str, changes: dict) -> bool:
        with self._lock:
            src = self._sources.get(source_id)
            if not src or src.get("lease_owner") != owner:
                return False
            src.update(copy.deepcopy(changes), lease_owner=None, lease_until=None, updated_at=utcnow())
            return True

    def load_row_states(self, source_id: str) -> dict[str, dict]:
        with self._lock:
            return {r["key_hash"]: {k: r.get(k) for k in ("key_hash",) + ROW_STATE_FIELDS}
                    for r in self._rows.values() if r["source_id"] == source_id}

    def save_row_states(self, source_id: str, states: list[dict], chunk: int = 1000) -> None:
        now = utcnow()
        with self._lock:
            for s in states:
                key = f"{source_id}:{s['key_hash']}"
                row = self._rows.setdefault(key, {"_id": key, "first_seen_at": now})
                row.update(copy.deepcopy(s), source_id=source_id, updated_at=now)

    def row_status_counts(self, source_id: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock:
            for r in self._rows.values():
                if r["source_id"] == source_id:
                    counts[r["status"]] = counts.get(r["status"], 0) + 1
        return counts

    def list_rows(self, source_id: str, status: str | None, limit: int) -> list[dict]:
        with self._lock:
            rows = [copy.deepcopy(r) for r in self._rows.values()
                    if r["source_id"] == source_id and (status is None or r["status"] == status)]
        return sorted(rows, key=lambda r: r["updated_at"], reverse=True)[:limit]


log = logging.getLogger(__name__)

BACKLOG_RETRY_SECONDS = 30          # a run that hit the row cap comes back this soon
CONFIG_ERROR_RETRY_MINUTES = 15     # wrong key, unshared sheet, bad settings
MAX_BACKOFF_MINUTES = 60


def plan_for(source: dict, data: SheetData) -> Plan:
    return plan_rows(data.values, spreadsheet_id=source["spreadsheet_id"], gid=source["gid"] or None,
                     header_row=source.get("header_row", 1), id_columns=source.get("id_columns", []),
                     ignore_columns=source.get("ignore_columns", []), form_key=source.get("form_key"),
                     extra_fields=source.get("extra_fields") or {}, sheet_title=data.title, tab=data.tab)


class SyncService:
    def __init__(self, store, reader: PublicSheetReader, settings: Settings,
                 crm_clients: dict[str, RecordsClient] | None = None, clock=utcnow):
        self.store, self.reader, self.settings, self.clock = store, reader, settings, clock
        self._clients = crm_clients if crm_clients is not None else {
            name: RecordsClient(p, timeout=settings.crm_timeout_seconds,
                                max_requests_per_second=settings.crm_max_requests_per_second)
            for name, p in settings.crm_profiles.items()}

    # ------------------------------------------------------------------ entry point
    def run_source(self, source: dict, owner: str, limit: int | None = None) -> dict:
        started = self.clock()
        t0 = time.monotonic()
        sid = source["_id"]
        poll = timedelta(minutes=source.get("poll_minutes") or self.settings.default_poll_minutes)
        try:
            summary = self._sync(source, owner, limit)
        except (SheetReadError, PlanError, CRMConfigError) as exc:
            permanent = not isinstance(exc, SheetReadError) or exc.permanent
            return self._fail(source, owner, started, t0, str(exc), permanent)
        except Exception as exc:  # never let one sheet stop the worker
            log.exception("sync crashed", extra={"source_id": sid})
            return self._fail(source, owner, started, t0, f"unexpected error: {exc.__class__.__name__}: {exc}", False)

        status = "ok" if not summary.get("error") else "error"
        if summary.get("error"):          # the CRM refused the key mid-run
            next_run = started + max(poll, timedelta(minutes=CONFIG_ERROR_RETRY_MINUTES))
        elif summary.get("backlog"):
            next_run = started + min(poll, timedelta(seconds=BACKLOG_RETRY_SECONDS))
        else:
            next_run = started + poll
        last_run = {"started_at": started, "duration_ms": int((time.monotonic() - t0) * 1000),
                    "status": status, **{k: v for k, v in summary.items() if k not in ("change_token", "error")}}
        changes = {"status": status, "last_run": last_run, "last_error": summary.get("error"),
                   "next_run_at": next_run, "consecutive_failures": 0 if status == "ok" else
                   source.get("consecutive_failures", 0) + 1,
                   "backlog": summary.get("backlog", 0), "pending_retries": summary.get("pending_retries", 0),
                   "held": summary.get("held_by_total_limit", 0)}
        if "change_token" in summary:
            changes["change_token"] = summary["change_token"]
        if not self.store.finish_run(sid, owner, changes):
            log.warning("lease lost before the run finished; result not saved", extra={"source_id": sid})
        log.info("sync finished", extra={"source_id": sid, **{k: v for k, v in last_run.items() if k != "started_at"}})
        return last_run

    # ------------------------------------------------------------------ the work
    def _sync(self, source: dict, owner: str, limit: int | None = None) -> dict:
        sid = source["_id"]
        ref = SheetRef(source["spreadsheet_id"], source["gid"] or None)
        data = self.reader.read(ref)
        nothing_pending = not (source.get("backlog") or source.get("pending_retries") or source.get("held"))
        if limit is None and data.change_token == source.get("change_token") and nothing_pending:
            return {"skipped": "sheet unchanged", "change_token": data.change_token}

        plan = plan_for(source, data)
        states = self.store.load_row_states(sid)
        counts = {"rows": len(plan.rows), "new": 0, "changed": 0, "retry": 0, "unchanged": 0,
                  "waiting_contact": 0, "sent": 0, "rejected": 0, "failed": 0}
        to_send: list[tuple[PlannedRow, dict | None]] = []
        updates: list[dict] = []
        for row in plan.rows:
            state = states.get(row.key_hash)
            if source.get("require_contact", True) and not row.has_contact:
                counts["waiting_contact"] += 1
                if not state or state.get("content_hash") != row.content_hash:
                    updates.append(self._state(row, "waiting_contact", attempts=0))
                continue
            if state is None or state.get("status") == "waiting_contact":
                reason = "new"
            elif state.get("content_hash") != row.content_hash:
                reason = "changed"
            elif state.get("status") == "failed":
                reason = "retry"
            else:                      # sent, rejected or dead with the same content: nothing to do
                counts["unchanged"] += 1
                continue
            counts[reason] += 1
            to_send.append((row, state))

        # MAX_ROWS_TOTAL: a hard limit on rows ever sent per sheet (0 = no limit). Rows beyond it are held, not queued.
        total_limit = 0 if limit else self.settings.max_rows_total
        if limit:                                    # an explicit limit on this call (POST /v1/ingest)
            counts["held_back"] = max(0, len(to_send) - limit)
            to_send = to_send[:limit]
        if total_limit:
            used = sum(1 for st in states.values() if st.get("status") in ("sent", "rejected", "failed", "dead"))
            room = max(0, total_limit - used)
            counts["held_by_total_limit"] = max(0, len(to_send) - room)
            to_send = to_send[:room]

        cap = self.settings.max_rows_per_sync
        backlog = max(0, len(to_send) - cap)
        error = None
        if to_send[:cap]:
            client = self._clients.get(source.get("crm_profile") or "default")
            if client is None:
                raise CRMConfigError(f"No CRM profile named {source.get('crm_profile') or 'default'!r} is configured.")
            sent_updates, error, not_attempted = self._post_all(client, to_send[:cap], sid, owner)
            updates.extend(sent_updates)
            backlog += not_attempted
            for u in sent_updates:
                counts[{"sent": "sent", "rejected": "rejected"}.get(u["status"], "failed")] += 1
        if updates:
            self.store.save_row_states(sid, updates)

        pending_retries = sum(1 for u in updates if u["status"] == "failed")
        pending_retries += sum(1 for row, st in to_send[cap:] if st and st.get("status") == "failed")
        result = {**counts, "backlog": backlog, "pending_retries": pending_retries,
                  "id_strategy": plan.id_strategy, "warnings": plan.warnings, "error": error}
        result["change_token"] = data.change_token
        return result

    def _post_all(self, client: RecordsClient, batch, sid: str, owner: str):
        """Post rows with bounded concurrency. Stops submitting at the first configuration error."""
        updates: list[dict] = []
        error = None
        lease = timedelta(seconds=self.settings.lease_seconds)
        pending = iter(batch)
        attempted = 0
        refused = 0                    # posts the CRM refused for a key/address problem: retried next run
        with ThreadPoolExecutor(max_workers=self.settings.crm_concurrency, thread_name_prefix="crm-post") as pool:
            in_flight = {}
            while True:
                while error is None and len(in_flight) < self.settings.crm_concurrency:
                    item = next(pending, None)
                    if item is None:
                        break
                    in_flight[pool.submit(client.post, item[0].payload)] = item
                    attempted += 1
                if not in_flight:
                    break
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    row, state = in_flight.pop(fut)
                    try:
                        updates.append(self._sent_state(row, fut.result()))
                    except CRMRejectedError as exc:
                        updates.append(self._state(row, "rejected", attempts=0, error=str(exc)))
                    except CRMConfigError as exc:
                        error = str(exc)       # in-flight posts finish; nothing new is started
                        refused += 1
                    except (CRMUnavailableError, Exception) as exc:  # noqa: BLE001 - recorded per row
                        prior = state.get("attempts", 0) if state and state.get("content_hash") == row.content_hash else 0
                        attempts = prior + 1
                        status = "dead" if attempts >= self.settings.max_attempts_per_row else "failed"
                        updates.append(self._state(row, status, attempts=attempts, error=str(exc)[:500]))
                if len(updates) % 50 == 0:
                    self.store.renew_lease(sid, owner, self.clock() + lease)
        not_attempted = len(batch) - attempted + refused
        return updates, error, not_attempted

    # ------------------------------------------------------------------ helpers
    def _state(self, row: PlannedRow, status: str, *, attempts: int, error: str | None = None) -> dict:
        return {"key_hash": row.key_hash, "content_hash": row.content_hash, "external_id": row.external_id,
                "status": status, "attempts": attempts, "error": error, "row_number": row.row_number,
                "keyed_by": row.keyed_by, "data": row.payload}

    def _sent_state(self, row: PlannedRow, result: PostResult) -> dict:
        state = self._state(row, "rejected" if result.outcome == "rejected" else "sent", attempts=0,
                            error=result.detail if result.outcome == "rejected" else None)
        state.update(outcome=result.outcome, record_id=result.record_id, crm_status=result.crm_status,
                     sent_at=self.clock())
        return state

    def _fail(self, source: dict, owner: str, started: datetime, t0: float, message: str, permanent: bool) -> dict:
        failures = source.get("consecutive_failures", 0) + 1
        poll_min = source.get("poll_minutes") or self.settings.default_poll_minutes
        if permanent:
            wait_min = max(poll_min, CONFIG_ERROR_RETRY_MINUTES)
        else:
            wait_min = min(poll_min * (2 ** (failures - 1)), MAX_BACKOFF_MINUTES)
        last_run = {"started_at": started, "duration_ms": int((time.monotonic() - t0) * 1000), "status": "error",
                    "error": message, "permanent": permanent}
        self.store.finish_run(source["_id"], owner, {
            "status": "error", "last_error": message, "last_run": last_run, "consecutive_failures": failures,
            "next_run_at": started + timedelta(minutes=wait_min)})
        log.warning("sync failed", extra={"source_id": source["_id"], "error": message, "permanent": permanent})
        return last_run
