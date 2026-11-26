"""
State: the registered sheets (sources) and, per row, what was last sent.

Two implementations with the same behaviour: MongoStore (production) and InMemoryStore (tests, local
trials). Sources are claimed with a lease, so any number of workers can run side by side without two
of them syncing the same sheet at once.
"""
from __future__ import annotations

import copy
import threading
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, MongoClient, ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError

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
