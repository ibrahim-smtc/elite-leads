"""Posting records to the CRM's "Address for records", with retries, backoff and a rate limit."""
from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass

import httpx

from .config import CRMProfile

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
