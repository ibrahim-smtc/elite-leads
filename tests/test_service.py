from datetime import timedelta

from conftest import REVSPOT_HEADERS, SHEET_ID, URL, revspot_row
from sheets_connector.sheets import SheetReadError
from sheets_connector.sync import new_source_id, utcnow

OWNER = "test-worker"


def add_source(store, **kw):
    now = utcnow()
    doc = {"_id": new_source_id(), "url": URL, "spreadsheet_id": SHEET_ID, "gid": "0", "name": "t",
           "poll_minutes": 5, "header_row": 1, "id_columns": [], "ignore_columns": [], "require_contact": True,
           "form_key": None, "extra_fields": {}, "crm_profile": "default", "enabled": True, "status": "new",
           "next_run_at": now, "lease_owner": None, "lease_until": None, "consecutive_failures": 0, "backlog": 0,
           "pending_retries": 0, "change_token": None, "last_run": None, "last_error": None,
           "created_at": now, "updated_at": now, **kw}
    store.create_source(doc)
    return doc["_id"]


def run(service, store, sid):
    src = store.claim_due_source(OWNER, utcnow() + timedelta(days=1), 600)   # claim regardless of next_run_at
    assert src and src["_id"] == sid
    return service.run_source(src, OWNER), store.get_source(sid)


def test_first_run_sends_every_row_then_nothing(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882"), revspot_row("Asha", "919845012345")]
    sid = add_source(store)
    result, src = run(service, store, sid)
    assert result["sent"] == 2 and len(crm.received) == 2 and src["status"] == "ok"
    assert {r["Name"] for r in crm.received} == {"Jason", "Asha"}
    assert store.row_status_counts(sid) == {"sent": 2}

    result, _ = run(service, store, sid)
    assert result.get("skipped") == "sheet unchanged" and len(crm.received) == 2


def test_edit_and_new_row_send_only_those(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882"), revspot_row("Asha", "919845012345")]
    sid = add_source(store)
    run(service, store, sid)
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882", model="Taigun"),
                               revspot_row("Asha", "919845012345"), revspot_row("Paul", "918217617421")]
    result, _ = run(service, store, sid)
    assert (result["changed"], result["new"], result["unchanged"], result["sent"]) == (1, 1, 1, 2)
    assert [r["Name"] for r in crm.received[2:]] == ["Jason", "Paul"] or {r["Name"] for r in crm.received[2:]} == {"Jason", "Paul"}
    jason_versions = [r["external_id"] for r in crm.received if r["Name"] == "Jason"]
    assert len(set(jason_versions)) == 2          # each version is its own record


def test_row_without_contact_waits_and_is_sent_once_phone_arrives(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "")]
    sid = add_source(store)
    result, _ = run(service, store, sid)
    assert result["waiting_contact"] == 1 and crm.received == []
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882")]
    result, _ = run(service, store, sid)
    assert result["sent"] == 1 and len(crm.received) == 1       # exactly one record, no empty placeholder


def test_row_cap_leaves_a_backlog_that_the_next_run_drains(store, reader, crm, make_service):
    service, _ = make_service(max_rows_per_sync=2)
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS] + [revspot_row(f"L{i}", f"9198450{i:05d}") for i in range(5)]
    sid = add_source(store)
    result, src = run(service, store, sid)
    assert result["sent"] == 2 and result["backlog"] == 3
    assert src["next_run_at"] - src["last_run"]["started_at"] <= timedelta(seconds=30)
    run(service, store, sid)
    result, src = run(service, store, sid)
    assert len(crm.received) == 5 and result["backlog"] == 0
    result, _ = run(service, store, sid)
    assert result.get("skipped") == "sheet unchanged"


def test_crm_down_marks_failed_and_retries_next_run(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882")]
    sid = add_source(store)
    crm.script = [503, 503, 503, 503]
    result, src = run(service, store, sid)
    assert result["failed"] == 1 and src["pending_retries"] == 1 and store.row_status_counts(sid) == {"failed": 1}
    result, _ = run(service, store, sid)                 # sheet unchanged, but a retry is pending
    assert result["retry"] == 1 and result["sent"] == 1 and len(crm.received) == 1


def test_row_that_keeps_failing_stops_after_max_attempts(store, reader, crm, make_service):
    service, _ = make_service(max_attempts_per_row=2)
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882")]
    sid = add_source(store)
    crm.script = [503] * 8
    run(service, store, sid)
    run(service, store, sid)
    assert store.row_status_counts(sid) == {"dead": 1}
    result, _ = run(service, store, sid)
    assert result.get("skipped") == "sheet unchanged"
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882", model="Taigun")]
    crm.script = []
    result, _ = run(service, store, sid)                 # content changed: tried again
    assert result["sent"] == 1


def test_rejected_record_is_not_retried_until_it_changes(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882")]
    sid = add_source(store)
    crm.script = [(422, {"error": "bad"}, {})]
    result, _ = run(service, store, sid)
    assert result["rejected"] == 1
    result, _ = run(service, store, sid)
    assert result.get("skipped") == "sheet unchanged" and crm.received == []


def test_wrong_key_stops_the_run_and_keeps_rows_for_later(store, reader, crm, make_service):
    service, _ = make_service(crm_concurrency=1)
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS] + [revspot_row(f"L{i}", f"9198450{i:05d}") for i in range(4)]
    sid = add_source(store)
    crm.script = [401]
    result, src = run(service, store, sid)
    assert src["status"] == "error" and "key" in src["last_error"] and crm.received == []
    assert src["backlog"] == 4 and src["next_run_at"] - src["last_run"]["started_at"] >= timedelta(minutes=15)
    result, src = run(service, store, sid)               # key fixed
    assert len(crm.received) == 4 and src["status"] == "ok"


def test_unshared_sheet_is_reported_and_backs_off(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS]
    reader.error = SheetReadError("The sheet cannot be read. Check the link, and that it is shared as 'Anyone with the link can view'.", permanent=True)
    sid = add_source(store)
    _, src = run(service, store, sid)
    assert src["status"] == "error" and "Anyone with the link" in src["last_error"] and src["consecutive_failures"] == 1
    assert src["lease_owner"] is None


def test_transient_errors_back_off_exponentially(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS]
    reader.error = SheetReadError("Google answered HTTP 503", permanent=False)
    sid = add_source(store, poll_minutes=5)
    waits = []
    for _ in range(3):
        _, src = run(service, store, sid)
        waits.append(src["next_run_at"] - src["last_run"]["started_at"])
    assert waits == [timedelta(minutes=5), timedelta(minutes=10), timedelta(minutes=20)]


def test_unknown_crm_profile_is_a_clear_error(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882")]
    sid = add_source(store, crm_profile="acme")
    _, src = run(service, store, sid)
    assert src["status"] == "error" and "acme" in src["last_error"]


def test_leases_stop_two_workers_taking_the_same_sheet(store, reader, make_service):
    sid = add_source(store)
    now = utcnow()
    assert store.claim_due_source("a", now, 600)["_id"] == sid
    assert store.claim_due_source("b", now, 600) is None
    assert store.finish_run(sid, "b", {"status": "ok"}) is False       # not b's lease
    assert store.claim_due_source("b", now + timedelta(seconds=601), 600)["_id"] == sid   # expired lease is taken over


def test_row_data_is_stored_with_its_state(store, reader, crm, make_service):
    service, _ = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882"), revspot_row("NoPhone", "")]
    sid = add_source(store)
    run(service, store, sid)
    rows = {r["data"]["Name"]: r for r in store.list_rows(sid, None, 10)}
    assert rows["Jason"]["status"] == "sent" and rows["Jason"]["data"]["Preferred Model"] == "Virtus GT"
    assert rows["NoPhone"]["status"] == "waiting_contact"


def test_total_limit_is_a_hard_stop_not_a_per_run_cap(store, reader, crm, make_service):
    service, _ = make_service(max_rows_total=3)
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS] + [revspot_row(f"L{i}", f"9198450{i:05d}") for i in range(10)]
    sid = add_source(store)
    result, src = run(service, store, sid)
    assert result["sent"] == 3 and result["held_by_total_limit"] == 7 and result["backlog"] == 0
    assert src["next_run_at"] - src["last_run"]["started_at"] >= timedelta(minutes=5)       # no quick re-run
    for _ in range(3):
        result, _ = run(service, store, sid)
    assert len(crm.received) == 3 and store.row_status_counts(sid) == {"sent": 3}


def test_total_limit_counts_what_was_already_sent_and_zero_means_no_limit(store, reader, crm, make_service):
    service, _ = make_service(max_rows_total=2, max_rows_per_sync=1)
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS] + [revspot_row(f"L{i}", f"9198450{i:05d}") for i in range(5)]
    sid = add_source(store)
    run(service, store, sid)
    run(service, store, sid)
    run(service, store, sid)
    assert len(crm.received) == 2                      # 1 + 1, then the total of 2 is reached
    service, _ = make_service(max_rows_total=0, max_rows_per_sync=100)
    result, _ = run(service, store, sid)
    assert result["sent"] == 3 and len(crm.received) == 5
