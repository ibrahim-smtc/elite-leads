"""Runs the same store checks against a real MongoDB. Skipped unless TEST_MONGODB_URI is set.

Uses a throwaway database (sheets_connector_test_<random>) and drops its collections afterwards.
"""
import os
import uuid
from datetime import timedelta

import pytest

from conftest import REVSPOT_HEADERS, SHEET_ID, revspot_row
from sheets_connector.store import DuplicateSourceError, MongoStore, SourceNotFound, utcnow
from test_service import add_source, run

URI = os.getenv("TEST_MONGODB_URI")
pytestmark = pytest.mark.skipif(not URI, reason="TEST_MONGODB_URI not set")


@pytest.fixture
def store():
    name = f"sheets_connector_test_{uuid.uuid4().hex[:8]}"
    s = MongoStore(URI, name)
    s.ensure_indexes()
    yield s
    s.sources.drop()          # dropping the collections removes the database too (the Atlas user may not drop databases)
    s.rows.drop()
    s.close()


def test_sources_crud_unique_and_leases(store):
    sid = add_source(store)
    with pytest.raises(DuplicateSourceError):
        add_source(store)
    assert store.get_source(sid)["gid"] == "0"
    now = utcnow()
    assert store.claim_due_source("a", now, 600)["_id"] == sid
    assert store.claim_due_source("b", now, 600) is None
    assert store.renew_lease(sid, "b", now) is False and store.renew_lease(sid, "a", now + timedelta(minutes=20))
    assert store.finish_run(sid, "b", {"status": "ok"}) is False
    assert store.finish_run(sid, "a", {"status": "ok", "next_run_at": now + timedelta(minutes=5)}) is True
    assert store.claim_due_source("b", now, 600) is None                      # not due yet
    assert store.claim_due_source("b", now + timedelta(minutes=6), 600)["_id"] == sid
    store.delete_source(sid)
    with pytest.raises(SourceNotFound):
        store.get_source(sid)


def test_full_sync_against_mongo(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882"), revspot_row("Asha", "919845012345")]
    sid = add_source(store)
    result, src = run(service, store, sid)
    assert result["sent"] == 2 and src["status"] == "ok" and src["lease_owner"] is None
    assert store.row_status_counts(sid) == {"sent": 2}
    reader.tables[SHEET_ID][1] = revspot_row("Jason", "918904371882", model="Taigun")
    result, _ = run(service, store, sid)
    assert (result["changed"], result["unchanged"], result["sent"]) == (1, 1, 1)
    assert len(store.list_rows(sid, "sent", 10)) == 2
    assert src["next_run_at"].tzinfo is not None                               # dates come back timezone-aware
