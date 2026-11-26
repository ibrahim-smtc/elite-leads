"""
HTTP API: register Google Sheets, see how each one is doing, and trigger a sync.

Every /v1 route needs the X-Api-Key header (ADMIN_API_KEY). /health and /ready are open for the
platform's health checks and reveal nothing.
"""
from __future__ import annotations

import hmac
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import __version__
from .config import Settings
from .rows import PlanError, mask
from .scheduler import Scheduler
from .service import SyncService, plan_for
from .sheets import PublicSheetReader, SheetReadError, parse_sheet_url
from .store import DuplicateSourceError, MongoStore, SourceNotFound, new_source_id, utcnow

log = logging.getLogger(__name__)
ROW_STATUSES = {"sent", "rejected", "failed", "dead", "waiting_contact"}


# ---------------------------------------------------------------------- request bodies
class _Common(BaseModel):
    name: str | None = Field(None, max_length=200)
    poll_minutes: float | None = Field(None, ge=1, le=1440, description="How often to check the sheet.")
    header_row: int = Field(1, ge=1, le=100, description="Row that holds the column names.")
    id_columns: list[str] = Field(default_factory=list, max_length=5,
                                  description="Columns that identify a row (e.g. 'Lead ID'). Auto-detected if empty.")
    ignore_columns: list[str] = Field(default_factory=list, max_length=100,
                                      description="Columns whose changes should not re-send a row (e.g. 'Last Updated').")
    require_contact: bool = Field(True, description="Hold rows until they have a phone number or email.")
    form_key: str | None = Field(None, max_length=100, description="Sent as form_key: tells the CRM which branch.")
    extra_fields: dict[str, str] = Field(default_factory=dict, description="Fixed fields added to every record.")
    crm_profile: str = Field("default", max_length=60)
    enabled: bool = True

    @field_validator("extra_fields")
    @classmethod
    def _limit_extra(cls, v: dict[str, str]) -> dict[str, str]:
        if len(v) > 20 or any(len(k) > 80 or len(str(val)) > 500 for k, val in v.items()):
            raise ValueError("extra_fields: at most 20 fields, names up to 80 and values up to 500 characters")
        return v


class SheetSourceIn(_Common):
    url: str = Field(..., description="Google Sheets link; the #gid=... part picks the tab.")
    check_now: bool = Field(True, description="Read the sheet before saving, and show what would be sent.")


class SheetSourcePatch(BaseModel):
    name: str | None = Field(None, max_length=200)
    poll_minutes: float | None = Field(None, ge=1, le=1440)
    header_row: int | None = Field(None, ge=1, le=100)
    id_columns: list[str] | None = Field(None, max_length=5)
    ignore_columns: list[str] | None = Field(None, max_length=100)
    require_contact: bool | None = None
    form_key: str | None = Field(None, max_length=100)
    extra_fields: dict[str, str] | None = None
    crm_profile: str | None = Field(None, max_length=60)
    enabled: bool | None = None


# ---------------------------------------------------------------------- wiring
@dataclass
class Components:
    settings: Settings
    store: object
    reader: object
    service: SyncService
    scheduler: Scheduler | None


def build_components(settings: Settings, store=None, reader=None, service=None, with_scheduler=None) -> Components:
    store = store or MongoStore(settings.mongodb_uri, settings.mongodb_db)
    reader = reader or PublicSheetReader()
    service = service or SyncService(store, reader, settings)
    run_scheduler = settings.scheduler_enabled if with_scheduler is None else with_scheduler
    scheduler = Scheduler(store, service, settings) if run_scheduler else None
    return Components(settings, store, reader, service, scheduler)


def new_source_doc(url: str, ref, fields: dict) -> dict:
    now = utcnow()
    return {"_id": new_source_id(), "url": url, "spreadsheet_id": ref.spreadsheet_id, "gid": ref.gid or "",
            **fields, "status": "new", "next_run_at": now, "lease_owner": None, "lease_until": None,
            "consecutive_failures": 0, "backlog": 0, "pending_retries": 0, "change_token": None,
            "last_run": None, "last_error": None, "created_at": now, "updated_at": now}


def register_env_sheets(settings: Settings, store) -> list[str]:
    """Make the sheets named in SHEET_URL / SHEET_URLS exist (and run). Safe to call on every start.

    A sheet removed from the variable is paused, not deleted; put it back and it resumes.
    """
    wanted = {}
    for url in settings.sheet_urls:
        ref = parse_sheet_url(url)
        wanted[(ref.spreadsheet_id, ref.gid or "")] = (url, ref)
    existing = {(s["spreadsheet_id"], s["gid"]): s for s in store.list_sources()}
    started = []
    for key, (url, ref) in wanted.items():
        found = existing.get(key)
        if found is None:
            fields = _Common().model_dump() | {"from_env": True}
            store.create_source(new_source_doc(url, ref, fields))
            started.append(url)
        elif found.get("paused_by_env"):
            store.update_source(found["_id"], {"enabled": True, "paused_by_env": False, "next_run_at": utcnow()})
            started.append(url)
    for key, found in existing.items():
        if found.get("from_env") and key not in wanted and found.get("enabled"):
            store.update_source(found["_id"], {"enabled": False, "paused_by_env": True})
            log.info("sheet removed from SHEET_URL(S): paused", extra={"source_id": found["_id"]})
    for url in started:
        log.info("sheet from environment ready", extra={"spreadsheet_id": parse_sheet_url(url).spreadsheet_id})
    return started


def _jsonable(doc):
    if isinstance(doc, dict):
        return {k: _jsonable(v) for k, v in doc.items() if k not in ("lease_owner",)}
    if isinstance(doc, list):
        return [_jsonable(v) for v in doc]
    if isinstance(doc, datetime):
        return doc.isoformat()
    return doc


def _source_out(doc: dict, counts: dict | None = None) -> dict:
    out = _jsonable(doc)
    out["id"] = out.pop("_id")
    out.pop("change_token", None)
    if counts is not None:
        out["rows_by_status"] = counts
    return out


def _preview(comp: Components, source: dict, limit: int = 3, unmask: bool = False) -> dict:
    data = comp.reader.read(parse_sheet_url(source["url"]))
    plan = plan_for(source, data)
    sample = [r.payload if unmask else mask(r.payload) for r in plan.rows[:limit]]
    return {"sheet_title": data.title, "tab": data.tab, "headers": plan.headers, "rows": len(plan.rows),
            "rows_with_contact": sum(1 for r in plan.rows if r.has_contact), "id_strategy": plan.id_strategy,
            "id_columns": plan.id_columns, "contact_columns": plan.contact_columns, "warnings": plan.warnings,
            "sample_records": sample}


# ---------------------------------------------------------------------- the app
def create_app(settings: Settings | None = None, *, components: Components | None = None) -> FastAPI:
    settings = settings or (components.settings if components else Settings.from_env())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        comp = app.state.components = components or build_components(settings)
        comp.store.ensure_indexes()
        register_env_sheets(comp.settings, comp.store)
        if comp.scheduler:
            comp.scheduler.start()
        try:
            yield
        finally:
            if comp.scheduler:
                comp.scheduler.stop()
            comp.store.close()

    app = FastAPI(title="Google Sheets -> Perfox CRM connector", version=__version__, lifespan=lifespan)

    def comp_of(request: Request) -> Components:
        return request.app.state.components

    def require_key(x_api_key: str | None = Header(default=None)) -> None:
        expected = settings.admin_api_key
        if expected and not hmac.compare_digest((x_api_key or "").encode(), expected.encode()):
            raise HTTPException(401, "missing or wrong X-Api-Key header")

    @app.exception_handler(SourceNotFound)
    async def _not_found(_: Request, exc: SourceNotFound):
        return JSONResponse({"detail": "No such sheet source."}, status_code=404)

    @app.exception_handler(SheetReadError)
    async def _sheet_error(_: Request, exc: SheetReadError):
        return JSONResponse({"detail": str(exc)}, status_code=422 if exc.permanent else 503)

    @app.exception_handler(PlanError)
    async def _plan_error(_: Request, exc: PlanError):
        return JSONResponse({"detail": str(exc)}, status_code=422)

    # ------------------------------------------------------------------ open routes
    @app.get("/health", tags=["ops"])
    def health():
        return {"ok": True, "version": __version__}

    @app.get("/ready", tags=["ops"])
    def ready(request: Request):
        comp = comp_of(request)
        try:
            comp.store.ping()
        except Exception:
            return JSONResponse({"ready": False, "reason": "database unavailable"}, status_code=503)
        return {"ready": True, "crm_profiles": sorted(settings.crm_profiles),
                "scheduler_running": comp.scheduler is not None}

    # ------------------------------------------------------------------ sheet sources
    @app.post("/v1/sheets", status_code=201, tags=["sheets"], dependencies=[Depends(require_key)])
    def register_sheet(body: SheetSourceIn, request: Request):
        comp = comp_of(request)
        try:
            ref = parse_sheet_url(body.url)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if body.crm_profile not in settings.crm_profiles and settings.crm_profiles:
            raise HTTPException(422, f"Unknown crm_profile {body.crm_profile!r}. Known: {sorted(settings.crm_profiles)}")
        doc = new_source_doc(body.url, ref, body.model_dump(exclude={"url", "check_now"}))
        preview = _preview(comp, doc) if body.check_now else None
        if preview and not doc["name"]:
            doc["name"] = f"{preview['sheet_title']} / {preview['tab']}"
        try:
            comp.store.create_source(doc)
        except DuplicateSourceError as exc:
            raise HTTPException(409, str(exc)) from exc
        log.info("sheet registered", extra={"source_id": doc["_id"], "spreadsheet_id": ref.spreadsheet_id})
        return {"source": _source_out(doc), "preview": preview}

    @app.get("/v1/sheets", tags=["sheets"], dependencies=[Depends(require_key)])
    def list_sheets(request: Request):
        return {"sources": [_source_out(s) for s in comp_of(request).store.list_sources()]}

    @app.get("/v1/sheets/{source_id}", tags=["sheets"], dependencies=[Depends(require_key)])
    def get_sheet(source_id: str, request: Request):
        store = comp_of(request).store
        return _source_out(store.get_source(source_id), store.row_status_counts(source_id))

    @app.patch("/v1/sheets/{source_id}", tags=["sheets"], dependencies=[Depends(require_key)])
    def update_sheet(source_id: str, body: SheetSourcePatch, request: Request):
        store = comp_of(request).store
        changes = body.model_dump(exclude_unset=True)
        if "crm_profile" in changes and settings.crm_profiles and changes["crm_profile"] not in settings.crm_profiles:
            raise HTTPException(422, f"Unknown crm_profile {changes['crm_profile']!r}.")
        notes = []
        if {"id_columns", "header_row"} & changes.keys():
            notes.append("Row identity changed: rows will be seen as new and sent again once.")
        elif "ignore_columns" in changes:
            notes.append("Change detection changed: rows may be sent again once.")
        if notes:
            changes.update(change_token=None)        # make the next run read the sheet
        return {"source": _source_out(store.update_source(source_id, changes)), "notes": notes}

    @app.delete("/v1/sheets/{source_id}", status_code=204, tags=["sheets"], dependencies=[Depends(require_key)])
    def delete_sheet(source_id: str, request: Request):
        comp_of(request).store.delete_source(source_id)   # records already in the CRM stay there

    @app.post("/v1/sheets/{source_id}/sync", status_code=202, tags=["sheets"], dependencies=[Depends(require_key)])
    def sync_now(source_id: str, request: Request):
        comp = comp_of(request)
        comp.store.request_run(source_id, utcnow())
        return {"queued": True, "note": "Runs within a few seconds; see GET /v1/sheets/{id} for the result."
                if comp.scheduler else "No scheduler runs in this process; a worker will pick it up."}

    @app.get("/v1/sheets/{source_id}/preview", tags=["sheets"], dependencies=[Depends(require_key)])
    def preview(source_id: str, request: Request, limit: int = Query(3, ge=1, le=20), unmask: bool = False):
        comp = comp_of(request)
        return _preview(comp, comp.store.get_source(source_id), limit, unmask)

    @app.get("/v1/sheets/{source_id}/rows", tags=["sheets"], dependencies=[Depends(require_key)])
    def list_rows(source_id: str, request: Request, status: str | None = None,
                  limit: int = Query(50, ge=1, le=500)):
        if status and status not in ROW_STATUSES:
            raise HTTPException(422, f"status must be one of {sorted(ROW_STATUSES)}")
        store = comp_of(request).store
        store.get_source(source_id)
        return {"rows": [_jsonable(r) for r in store.list_rows(source_id, status, limit)]}

    return app
