"""
One sync of one sheet: read it, find new or changed rows, post them to the CRM, remember what was sent.

Safe by design:
- a run never raises; every outcome is written to the source (last_run, status, last_error);
- at most MAX_ROWS_PER_SYNC rows are posted per run; the rest are picked up by the next run soon after;
- a row that keeps failing stops being retried after MAX_ATTEMPTS_PER_ROW until its content changes;
- a wrong CRM key or address stops the run at once instead of failing every row.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta

from .config import Settings
from .crm import CRMConfigError, CRMRejectedError, CRMUnavailableError, PostResult, RecordsClient
from .rows import Plan, PlanError, PlannedRow, plan_rows
from .sheets import PublicSheetReader, SheetData, SheetReadError, SheetRef
from .store import utcnow

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
    def run_source(self, source: dict, owner: str) -> dict:
        started = self.clock()
        t0 = time.monotonic()
        sid = source["_id"]
        poll = timedelta(minutes=source.get("poll_minutes") or self.settings.default_poll_minutes)
        try:
            summary = self._sync(source, owner)
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
                   "backlog": summary.get("backlog", 0), "pending_retries": summary.get("pending_retries", 0)}
        if "change_token" in summary:
            changes["change_token"] = summary["change_token"]
        if not self.store.finish_run(sid, owner, changes):
            log.warning("lease lost before the run finished; result not saved", extra={"source_id": sid})
        log.info("sync finished", extra={"source_id": sid, **{k: v for k, v in last_run.items() if k != "started_at"}})
        return last_run

    # ------------------------------------------------------------------ the work
    def _sync(self, source: dict, owner: str) -> dict:
        sid = source["_id"]
        ref = SheetRef(source["spreadsheet_id"], source["gid"] or None)
        data = self.reader.read(ref)
        nothing_pending = not source.get("backlog") and not source.get("pending_retries")
        if data.change_token == source.get("change_token") and nothing_pending:
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
