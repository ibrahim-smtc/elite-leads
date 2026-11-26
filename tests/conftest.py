"""Shared fakes: a sheet reader that serves in-memory tables, and a fake CRM records address."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sheets_connector.config import CRMProfile, Settings  # noqa: E402
from sheets_connector.crm import RecordsClient  # noqa: E402
from sheets_connector.service import SyncService  # noqa: E402
from sheets_connector.sheets import SheetData, SheetReadError  # noqa: E402
from sheets_connector.store import InMemoryStore  # noqa: E402

SHEET_ID = "1RDng896zQ7uLC4y3aA6RxPtzYU4MxZDQwrsKDNoR5og"
URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid=0"

REVSPOT_HEADERS = ["Date", "Name", "Phone", "Email", "Location", "Campaign Name", "Preferred Model",
                   "Purchasing Timeline", "Most Recent Status", "Attempt #", "Current Status", "Remarks",
                   "Last Updated", "Updated By"]


def revspot_row(name, phone, email="", date="2026-09-08T16:20:07", model="Virtus GT", status="", attempt=""):
    return [date, name, phone, email, "Bengaluru", "Volkswagen Bangalore", model, "1-2 weeks", status, attempt,
            "", "", "", ""]


class FakeReader:
    """Serves tables by spreadsheet id; the change token follows the content."""

    def __init__(self):
        self.tables: dict[str, list[list[str]]] = {}
        self.reads = 0
        self.error: SheetReadError | None = None

    def _token(self, sid):
        return "v-" + str(hash(json.dumps(self.tables.get(sid, []))))

    def read(self, ref):
        self.reads += 1
        if self.error:
            raise self.error
        return SheetData("Test sheet", "Sheet1", [list(r) for r in self.tables[ref.spreadsheet_id]],
                         self._token(ref.spreadsheet_id))


class FakeCRM:
    """A records address: records what it receives; responses can be scripted per call."""

    def __init__(self):
        self.received: list[dict] = []
        self.headers: list[dict] = []
        self.script: list = []          # each item: int status, or (status, body, headers)

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.headers.append(dict(request.headers))
        if self.script:
            item = self.script.pop(0)
            status, payload, headers = (item, None, {}) if isinstance(item, int) else item
            if status >= 300:
                return httpx.Response(status, json=payload or {"error": "scripted"}, headers=headers)
        self.received.append(body)
        return httpx.Response(200, json={"status": "created", "external_record_id": f"rec_{len(self.received)}"})


def make_settings(**overrides) -> Settings:
    base = dict(environment="test", mongodb_uri="mongodb://unused", admin_api_key="test-admin-key-0123456789abcd",
                crm_profiles={"default": CRMProfile("default", "https://crm.example/records", "src_key_123")},
                max_rows_per_sync=200, crm_concurrency=3, scheduler_enabled=False)
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def crm():
    return FakeCRM()


@pytest.fixture
def reader():
    return FakeReader()


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def make_service(store, reader, crm):
    def _make(**overrides):
        settings = make_settings(**overrides)
        client = RecordsClient(settings.crm_profiles["default"], http=httpx.Client(transport=httpx.MockTransport(crm.handler)),
                               max_requests_per_second=1000, sleep=lambda s: None, jitter=lambda: 0)
        return SyncService(store, reader, settings, crm_clients={"default": client}), settings
    return _make
