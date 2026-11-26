"""Reading a Google Sheet shared as "Anyone with the link can view" (no Google account or key needed)."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass

import httpx

_SHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9_-]{20,})")
_GID_RE = re.compile(r"[#&?]gid=(\d+)")


@dataclass(frozen=True)
class SheetRef:
    spreadsheet_id: str
    gid: str | None = None          # the tab; None = the first tab


@dataclass
class SheetData:
    title: str
    tab: str
    values: list[list[str]]         # every row, header included
    change_token: str               # differs whenever the content differs


class SheetReadError(Exception):
    """The sheet could not be read. permanent=True means retrying will not help until someone acts."""

    def __init__(self, message: str, *, permanent: bool):
        super().__init__(message)
        self.permanent = permanent


def parse_sheet_url(url: str) -> SheetRef:
    match = _SHEET_ID_RE.search(url or "")
    if not match:
        raise ValueError("Not a Google Sheets link. Expected https://docs.google.com/spreadsheets/d/<id>/edit#gid=<tab>")
    gid = _GID_RE.search(url)
    return SheetRef(match.group(1), gid.group(1) if gid else None)


class PublicSheetReader:
    def __init__(self, http: httpx.Client | None = None, timeout: float = 30.0):
        self._http = http or httpx.Client(timeout=timeout, follow_redirects=True,
                                          headers={"User-Agent": "sheets-connector/1.0"})

    def read(self, ref: SheetRef) -> SheetData:
        url = f"https://docs.google.com/spreadsheets/d/{ref.spreadsheet_id}/export"
        try:
            resp = self._http.get(url, params={"format": "csv", "gid": ref.gid or "0"})
        except httpx.HTTPError as exc:
            raise SheetReadError(f"Google did not answer: {exc}", permanent=False) from exc
        if resp.status_code in (401, 403, 404) or "text/html" in resp.headers.get("content-type", ""):
            raise SheetReadError("The sheet cannot be read. Check the link, and that it is shared as "
                                 "'Anyone with the link can view'.", permanent=True)
        if resp.status_code != 200:
            raise SheetReadError(f"Google answered HTTP {resp.status_code}", permanent=resp.status_code < 500)
        values = list(csv.reader(io.StringIO(resp.content.decode("utf-8-sig"))))
        token = "sha256-" + hashlib.sha256(json.dumps(values, ensure_ascii=False).encode("utf-8")).hexdigest()
        return SheetData(ref.spreadsheet_id, f"gid {ref.gid or '0'}", values, token)
