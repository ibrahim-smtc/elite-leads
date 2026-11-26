import pytest
from fastapi.testclient import TestClient

from conftest import REVSPOT_HEADERS, SHEET_ID, URL, make_settings, revspot_row
from sheets_connector.api import Components, create_app
from sheets_connector.sheets import SheetReadError

KEY = {"X-Api-Key": "test-admin-key-0123456789abcd"}


@pytest.fixture
def client(store, reader, make_service):
    service, settings = make_service()
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS, revspot_row("Jason", "918904371882", "jason@gmail.com")]
    app = create_app(settings, components=Components(settings, store, reader, service, None))
    with TestClient(app) as c:
        yield c


def test_health_and_ready_need_no_key(client):
    assert client.get("/health").json()["ok"] is True
    body = client.get("/ready").json()
    assert body["ready"] is True and body["crm_profiles"] == ["default"]


def test_every_v1_route_needs_the_key(client):
    assert client.get("/v1/sheets").status_code == 401
    assert client.get("/v1/sheets", headers={"X-Api-Key": "wrong"}).status_code == 401
    assert client.post("/v1/sheets", json={"url": URL}).status_code == 401


def test_register_shows_preview_masked_and_rejects_duplicates(client):
    r = client.post("/v1/sheets", json={"url": URL, "ignore_columns": ["Last Updated"]}, headers=KEY)
    assert r.status_code == 201, r.text
    body = r.json()
    prev = body["preview"]
    assert prev["rows"] == 1 and prev["id_strategy"] == "contact" and prev["rows_with_contact"] == 1
    sample = prev["sample_records"][0]
    assert sample["Name"] == "Jason" and sample["Email"] == "j***@gmail.com" and "4371882" not in str(sample)
    assert body["source"]["name"] == "Test sheet / Sheet1" and body["source"]["status"] == "new"
    assert "change_token" not in body["source"] and "lease_owner" not in body["source"]
    assert client.post("/v1/sheets", json={"url": URL}, headers=KEY).status_code == 409


def test_register_rejects_bad_links_and_unreadable_sheets(client, reader):
    assert client.post("/v1/sheets", json={"url": "https://example.com"}, headers=KEY).status_code == 422
    reader.error = SheetReadError("The sheet cannot be read. Check the link, and that it is shared as 'Anyone with the link can view'.", permanent=True)
    r = client.post("/v1/sheets", json={"url": URL}, headers=KEY)
    assert r.status_code == 422 and "Anyone with the link" in r.json()["detail"]


def test_unknown_profile_and_bad_id_column(client):
    assert client.post("/v1/sheets", json={"url": URL, "crm_profile": "nope"}, headers=KEY).status_code == 422
    r = client.post("/v1/sheets", json={"url": URL, "id_columns": ["Lead ID"]}, headers=KEY)
    assert r.status_code == 422 and "not in the sheet" in r.json()["detail"]


def test_full_cycle_get_patch_sync_rows_delete(client, store, crm, make_service):
    sid = client.post("/v1/sheets", json={"url": URL}, headers=KEY).json()["source"]["id"]
    assert client.post(f"/v1/sheets/{sid}/sync", headers=KEY).status_code == 202

    service, _ = make_service()
    src = store.claim_due_source("w", store.get_source(sid)["next_run_at"], 600)
    service.run_source(src, "w")
    got = client.get(f"/v1/sheets/{sid}", headers=KEY).json()
    assert got["status"] == "ok" and got["rows_by_status"] == {"sent": 1} and got["last_run"]["sent"] == 1
    assert len(crm.received) == 1

    rows = client.get(f"/v1/sheets/{sid}/rows?status=sent", headers=KEY).json()["rows"]
    assert len(rows) == 1 and rows[0]["outcome"] == "created"
    assert client.get(f"/v1/sheets/{sid}/rows?status=bogus", headers=KEY).status_code == 422

    r = client.patch(f"/v1/sheets/{sid}", json={"poll_minutes": 15, "ignore_columns": ["Attempt #"]}, headers=KEY)
    assert r.status_code == 200 and r.json()["source"]["poll_minutes"] == 15 and r.json()["notes"]
    assert client.patch(f"/v1/sheets/{sid}", json={"poll_minutes": 0}, headers=KEY).status_code == 422

    assert client.get(f"/v1/sheets/{sid}/preview?unmask=true", headers=KEY).json()["sample_records"][0]["Email"] == "jason@gmail.com"
    assert client.delete(f"/v1/sheets/{sid}", headers=KEY).status_code == 204
    assert client.get(f"/v1/sheets/{sid}", headers=KEY).status_code == 404
    assert store.row_status_counts(sid) == {}


def test_settings_validation():
    from sheets_connector.config import ConfigError, CRMProfile
    with pytest.raises(ConfigError):
        make_settings(environment="production", admin_api_key="short").validate()
    with pytest.raises(ConfigError):
        make_settings(environment="production", crm_profiles={"default": CRMProfile("default", "http://x", "k")}).validate()
    assert make_settings(admin_api_key=None).validate()          # dev: warnings only


def test_render_defaults_to_production(monkeypatch):
    from sheets_connector.config import Settings
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("RENDER", raising=False)
    assert Settings.from_env().environment == "development"
    monkeypatch.setenv("RENDER", "true")
    assert Settings.from_env().is_production


def test_sheets_from_the_environment_are_registered_once_and_paused_when_removed(store, monkeypatch):
    from dataclasses import replace
    from sheets_connector.api import register_env_sheets
    a = "https://docs.google.com/spreadsheets/d/AAAAAAAAAAAAAAAAAAAAAAAA/edit?gid=0#gid=0"
    b = "https://docs.google.com/spreadsheets/d/BBBBBBBBBBBBBBBBBBBBBBBB/edit#gid=5"
    both = replace(make_settings(), sheet_urls=(a, b))
    assert len(register_env_sheets(both, store)) == 2 and len(store.list_sources()) == 2
    assert register_env_sheets(both, store) == []                     # restart: nothing duplicated
    assert len(store.list_sources()) == 2
    only_a = replace(make_settings(), sheet_urls=(a,))
    register_env_sheets(only_a, store)                                # b removed from the variable
    assert {s["spreadsheet_id"][0]: s["enabled"] for s in store.list_sources()} == {"A": True, "B": False}
    assert len(register_env_sheets(both, store)) == 1                 # b put back: resumes
    assert all(s["enabled"] for s in store.list_sources())


def test_sheet_url_setting_parses_one_or_many(monkeypatch):
    from sheets_connector.config import ConfigError, Settings
    u1 = "https://docs.google.com/spreadsheets/d/AAAAAAAAAAAAAAAAAAAAAAAA/edit"
    u2 = "https://docs.google.com/spreadsheets/d/BBBBBBBBBBBBBBBBBBBBBBBB/edit"
    monkeypatch.setenv("SHEET_URL", u1)
    monkeypatch.setenv("SHEET_URLS", f"{u1}, {u2}\n")
    assert Settings.from_env().sheet_urls == (u1, u2)
    monkeypatch.setenv("SHEET_URLS", "https://example.com/nope")
    with pytest.raises(ConfigError):
        Settings.from_env()


def test_ingest_sends_only_the_limit_and_the_next_call_sends_more(client, reader, crm, store):
    reader.tables[SHEET_ID] = [REVSPOT_HEADERS] + [revspot_row(f"L{i}", f"9198450{i:05d}") for i in range(7)]
    r = client.post("/v1/ingest", json={"sheet_url": URL, "limit": 3}, headers=KEY)
    assert r.status_code == 200, r.text
    assert r.json()["sent_to_crm_inbox"] == 3 and r.json()["held_back_by_limit"] == 4 and r.json()["rows_in_sheet"] == 7
    assert len(crm.received) == 3
    again = client.post("/v1/ingest", json={"sheet_url": URL, "limit": 2}, headers=KEY).json()   # same, unchanged sheet
    assert again["sent_to_crm_inbox"] == 2 and again["already_sent_before"] == 3 and again["held_back_by_limit"] == 2
    assert len(crm.received) == 5
    src = store.list_sources()[0]
    assert src["enabled"] is False and src["lease_owner"] is None          # manual only; timer never touches it


def test_ingest_needs_the_key_and_a_valid_body_and_reports_unreadable_sheets(client, reader):
    assert client.post("/v1/ingest", json={"sheet_url": URL, "limit": 3}).status_code == 401
    assert client.post("/v1/ingest", json={"sheet_url": URL, "limit": 0}, headers=KEY).status_code == 422
    assert client.post("/v1/ingest", json={"sheet_url": "https://example.com", "limit": 3}, headers=KEY).status_code == 422
    reader.error = SheetReadError("The sheet cannot be read. Check the link.", permanent=True)
    r = client.post("/v1/ingest", json={"sheet_url": URL, "limit": 3}, headers=KEY)
    assert r.status_code == 422 and "cannot be read" in r.json()["detail"]
