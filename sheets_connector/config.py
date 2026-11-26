"""Settings, read once from the environment. Secrets never appear in repr() or logs."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field


class ConfigError(RuntimeError):
    """The service cannot start with these settings."""


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _bool(name: str, default: bool) -> bool:
    value = _env(name)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


def _number(name: str, default, cast=float, minimum=None):
    raw = _env(name)
    if raw is None:
        return default
    try:
        value = cast(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be at least {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class CRMProfile:
    """One CRM account to post records to: its records address and the key of a records source."""
    name: str
    records_url: str
    key: str = field(repr=False)


@dataclass(frozen=True)
class Settings:
    environment: str = "development"
    mongodb_uri: str = field(default="", repr=False)
    mongodb_db: str = "sheets_connector"
    admin_api_key: str | None = field(default=None, repr=False)
    crm_profiles: dict[str, CRMProfile] = field(default_factory=dict, repr=False)
    sheet_urls: tuple[str, ...] = ()
    default_poll_minutes: float = 5.0
    max_rows_per_sync: int = 200
    max_rows_total: int = 0
    max_attempts_per_row: int = 5
    crm_concurrency: int = 4
    crm_max_requests_per_second: float = 5.0
    crm_timeout_seconds: float = 20.0
    worker_concurrency: int = 2
    lease_seconds: int = 900
    tick_seconds: float = 5.0
    scheduler_enabled: bool = True
    log_level: str = "INFO"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    # ------------------------------------------------------------------ loading
    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            environment=(_env("APP_ENV", "production" if os.getenv("RENDER") else "development") or "development").lower(),
            mongodb_uri=_env("MONGODB_URI", "") or "",
            mongodb_db=_env("CONNECTOR_MONGODB_DB", "sheets_connector"),
            admin_api_key=_env("ADMIN_API_KEY"),
            crm_profiles=_load_crm_profiles(),
            sheet_urls=_sheet_urls(),
            default_poll_minutes=_number("DEFAULT_POLL_MINUTES", 5.0, float, 1),
            max_rows_per_sync=_number("MAX_ROWS_PER_SYNC", 200, int, 1),
            max_rows_total=_number("MAX_ROWS_TOTAL", 0, int, 0),
            max_attempts_per_row=_number("MAX_ATTEMPTS_PER_ROW", 5, int, 1),
            crm_concurrency=_number("CRM_CONCURRENCY", 4, int, 1),
            crm_max_requests_per_second=_number("CRM_MAX_REQUESTS_PER_SECOND", 5.0, float, 0.1),
            crm_timeout_seconds=_number("CRM_TIMEOUT_SECONDS", 20.0, float, 1),
            worker_concurrency=_number("WORKER_CONCURRENCY", 2, int, 1),
            lease_seconds=_number("LEASE_SECONDS", 900, int, 60),
            tick_seconds=_number("SCHEDULER_TICK_SECONDS", 5.0, float, 1),
            scheduler_enabled=_bool("SCHEDULER_ENABLED", True),
            log_level=(_env("LOG_LEVEL", "INFO") or "INFO").upper(),
        )

    def validate(self) -> list[str]:
        """Raise ConfigError for settings the service cannot run with; return warnings for the rest."""
        warnings: list[str] = []
        if not self.mongodb_uri:
            raise ConfigError("MONGODB_URI is required.")
        if not self.crm_profiles:
            msg = "No CRM configured: set CRM_RECORDS_URL and CRM_SOURCE_KEY."
            if self.is_production:
                raise ConfigError(msg)
            warnings.append(msg + " Rows can be previewed but not sent.")
        for profile in self.crm_profiles.values():
            if self.is_production and not profile.records_url.startswith("https://"):
                raise ConfigError(f"CRM profile {profile.name!r}: the records address must use https.")
        if self.is_production:
            if not self.admin_api_key or len(self.admin_api_key) < 24:
                raise ConfigError("ADMIN_API_KEY (24+ characters) is required in production.")
        elif not self.admin_api_key:
            warnings.append("ADMIN_API_KEY is not set: the API is open (development only).")
        return warnings


def _sheet_urls() -> tuple[str, ...]:
    """Sheet links from SHEET_URL (one) or SHEET_URLS (several, separated by commas, spaces or new lines)."""
    raw = " ".join(filter(None, (_env("SHEET_URL"), _env("SHEET_URLS"))))
    urls = [u for u in re.split(r"[\s,]+", raw) if u]
    bad = [u for u in urls if "/spreadsheets/d/" not in u]
    if bad:
        raise ConfigError(f"SHEET_URL(S) contains something that is not a Google Sheets link: {bad[0][:80]!r}")
    return tuple(dict.fromkeys(urls))


def _load_crm_profiles() -> dict[str, CRMProfile]:
    """The default profile from CRM_RECORDS_URL / CRM_SOURCE_KEY, plus named ones from CRM_PROFILES.

    CRM_PROFILES='{"acme": {"records_url": "https://...", "key_env": "CRM_KEY_ACME"}}'
    Keys stay in their own environment variables, never in JSON or the database.
    """
    profiles: dict[str, CRMProfile] = {}
    url, key = _env("CRM_RECORDS_URL"), _env("CRM_SOURCE_KEY")
    if url or key:
        if not (url and key):
            raise ConfigError("Set both CRM_RECORDS_URL and CRM_SOURCE_KEY, or neither.")
        profiles["default"] = CRMProfile("default", url, key)
    raw = _env("CRM_PROFILES")
    if raw:
        try:
            extra = json.loads(raw)
        except ValueError as exc:
            raise ConfigError(f"CRM_PROFILES is not valid JSON: {exc}") from exc
        for name, cfg in extra.items():
            profile_key = _env(cfg.get("key_env", ""))
            if not cfg.get("records_url") or not profile_key:
                raise ConfigError(f"CRM profile {name!r} needs records_url and key_env pointing at a set variable.")
            profiles[name] = CRMProfile(name, cfg["records_url"], profile_key)
    return profiles
