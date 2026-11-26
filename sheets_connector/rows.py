"""
Turning sheet rows into CRM records, for any column layout.

Each row gets:
- a stable row key (who/what the row is), so an edit is seen as a change, not a new row;
- a content hash (what the row says), so only real changes are sent again;
- the record payload: the row's own column labels and values, sent as they are.
Nothing here guesses what a column means beyond finding ids, phones, emails and dates for the key.
"""
from __future__ import annotations

import hashlib
import json
import re
import string
from collections import Counter
from dataclasses import dataclass, field

EMPTY_VALUES = {"", "na", "n/a", "nan", "null", "none", "-", "--"}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ID_KEYS = {"id", "lead_id", "leadid", "record_id", "external_id", "uuid", "call_id", "enquiry_id",
           "enquiry_no", "enquiry_number", "ticket_id", "ref_id", "reference_id", "submission_id"}
PHONE_HINTS = ("phone", "mobile", "whatsapp", "cell", "tel", "contact_no", "contact_number", "number")
EMAIL_HINTS = ("email", "e_mail", "mail")
DATE_HINTS = ("date", "timestamp", "created", "submitted")
RESERVED_KEYS = {"external_id", "form_key", "_source"}   # keys this connector sets itself


class PlanError(ValueError):
    """The source's settings do not fit the sheet (for example an id column that does not exist)."""


@dataclass(frozen=True)
class PlannedRow:
    row_number: int        # 1-based row in the sheet (informational: rows move)
    key_hash: str          # stable identity of the row
    content_hash: str      # changes when what the row says changes
    external_id: str       # sent to the CRM; one per row version
    payload: dict          # the record, exactly as it will be posted
    has_contact: bool      # a phone number or email is present
    keyed_by: str          # "id", "contact" or "content"


@dataclass
class Plan:
    headers: list[str]
    rows: list[PlannedRow]
    id_strategy: str
    id_columns: list[str] = field(default_factory=list)
    contact_columns: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------- small helpers
def clean_value(value) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return None if text.lower() in EMPTY_VALUES else text


def key_of(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (label or "").lower()).strip("_")


def column_letter(index: int) -> str:
    letters, index = "", index + 1
    while index:
        index, rem = divmod(index - 1, 26)
        letters = string.ascii_uppercase[rem] + letters
    return letters


def normalize_headers(raw: list[str], width: int) -> list[str]:
    """Trimmed labels; empty ones become 'Column C'; repeated ones get ' (2)', ' (3)'."""
    headers, seen = [], Counter()
    for i in range(width):
        label = clean_value(raw[i] if i < len(raw) else None) or f"Column {column_letter(i)}"
        seen[key_of(label)] += 1
        if seen[key_of(label)] > 1:
            label = f"{label} ({seen[key_of(label)]})"
        headers.append(label)
    return headers


def phone_digits(value: str | None) -> str | None:
    digits = re.sub(r"\D", "", value or "")
    return digits[-10:] if 8 <= len(digits) <= 15 else None


_PHONE_VALUE = re.compile(r"^\+?\d[\d\s()-]{8,18}$")


def looks_like_phone(value: str | None) -> bool:
    """A phone number by its shape, whatever the column is called. A plain run of digits counts only if it
    reads like an Indian mobile (10 digits from 6-9, or 91 + that); +country codes and spaced numbers count too."""
    if not value or not _PHONE_VALUE.match(value) or _DATE_SHAPE.match(value):
        return False
    digits = re.sub(r"\D", "", value)
    if not 10 <= len(digits) <= 13:
        return False
    if value.startswith("+") or re.search(r"[\s()-]", value):
        return True
    return bool(re.fullmatch(r"[6-9]\d{9}|91[6-9]\d{9}", digits))


def _sha(text: str, n: int | None = None) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return digest[:n] if n else digest


def _share(values: list[str | None], test) -> float:
    filled = [v for v in values if v]
    return sum(1 for v in filled if test(v)) / len(filled) if filled else 0.0


# ---------------------------------------------------------------------- the planner
def plan_rows(values: list[list[str]], *, spreadsheet_id: str, gid: str | None, header_row: int = 1,
              id_columns: list[str] | tuple = (), ignore_columns: list[str] | tuple = (),
              form_key: str | None = None, extra_fields: dict | None = None,
              sheet_title: str | None = None, tab: str | None = None) -> Plan:
    if len(values) < header_row:
        return Plan([], [], "none", warnings=["The sheet is empty."])
    data = values[header_row:]
    width = max([len(values[header_row - 1])] + [len(r) for r in data] or [0])
    headers = normalize_headers(values[header_row - 1], width)
    keys = [key_of(h) for h in headers]
    cells = [[clean_value(r[i] if i < len(r) else None) for i in range(width)] for r in data]
    columns = list(zip(*cells)) if cells else [() for _ in headers]
    warnings: list[str] = []

    ignored = {key_of(c) for c in ignore_columns}
    unknown = sorted(c for c in ignore_columns if key_of(c) not in keys)
    if unknown:
        warnings.append(f"ignore_columns not found in the sheet: {', '.join(unknown)}")

    # Columns that hold phones / emails (by name, confirmed by the values), used for the key and contact check.
    phone_cols = [i for i, k in enumerate(keys)
                  if (any(h in k for h in PHONE_HINTS) and _share(list(columns[i]), phone_digits) >= 0.5)
                  or (k not in ID_KEYS and _share(list(columns[i]), looks_like_phone) >= 0.8)]   # any name: "WA no", "Cell"
    email_cols = [i for i, k in enumerate(keys)
                  if (any(h in k for h in EMAIL_HINTS) or _share(list(columns[i]), EMAIL_RE.match) >= 0.8)
                  and _share(list(columns[i]), EMAIL_RE.match) >= 0.5]
    # A creation-type date (never "last updated"): part of the key, so two enquiries from one phone stay apart.
    date_col = next((i for i, k in enumerate(keys) if any(h in k for h in DATE_HINTS)
                     and not any(w in k for w in ("updated", "modified", "edited"))), None)

    # How rows are identified, decided once for the whole sheet.
    if id_columns:
        id_idx = []
        for col in id_columns:
            if key_of(col) not in keys:
                raise PlanError(f"id column {col!r} is not in the sheet. Columns: {', '.join(headers)}")
            id_idx.append(keys.index(key_of(col)))
        strategy = "id"
    else:
        auto = next((i for i, k in enumerate(keys) if k in ID_KEYS), None)
        filled_rows = [r for r in cells if any(r)]
        if auto is not None and filled_rows and sum(1 for r in filled_rows if r[auto]) / len(filled_rows) >= 0.9:
            id_idx, strategy = [auto], "id"
        elif phone_cols or email_cols:
            id_idx, strategy = [], "contact"
        else:
            id_idx, strategy = [], "content"
            warnings.append("No id, phone or email column found: an edited row will be sent as a new record. "
                            "Set id_columns for this sheet.")

    extra = {k: v for k, v in (extra_fields or {}).items() if clean_value(v)}
    rows: list[PlannedRow] = []
    occurrences: Counter = Counter()
    for offset, row in enumerate(cells):
        if not any(row):
            continue
        fields = {headers[i]: v for i, v in enumerate(row) if v is not None}
        hashed = sorted((headers[i], v) for i, v in enumerate(row) if v is not None and keys[i] not in ignored)
        content_hash = _sha(json.dumps(hashed, ensure_ascii=False))

        phone = next((phone_digits(row[i]) for i in phone_cols if phone_digits(row[i])), None)
        email = next((row[i].lower() for i in email_cols if row[i] and EMAIL_RE.match(row[i])), None)
        has_contact = bool(phone or email or any(v and EMAIL_RE.match(v) for v in row))

        keyed_by = strategy
        if strategy == "id" and all(row[i] for i in id_idx):
            key = "id:" + "|".join(row[i] for i in id_idx)
        elif strategy == "contact" and (phone or email):
            key = f"contact:{phone or email}|{row[date_col] if date_col is not None and row[date_col] else ''}"
        else:
            key, keyed_by = "content:" + content_hash, "content"
        occurrences[key] += 1
        if occurrences[key] > 1:            # the same key twice in one sheet: keep both rows
            key = f"{key}#{occurrences[key]}"

        key_hash = _sha(f"{spreadsheet_id}:{gid or ''}:{key}", 24)
        external_id = f"gs-{key_hash[:20]}-{content_hash[:10]}"
        payload = {(f"{label} (sheet)" if key_of(label) in RESERVED_KEYS else label): value
                   for label, value in fields.items()}
        for label, value in extra.items():
            payload.setdefault(label, value)
        if form_key:
            payload["form_key"] = form_key
        payload["external_id"] = external_id
        payload["_source"] = {"system": "google_sheets", "sheet": sheet_title or spreadsheet_id, "tab": tab or gid or ""}
        rows.append(PlannedRow(header_row + offset + 1, key_hash, content_hash, external_id, payload,
                               has_contact, keyed_by))

    return Plan(headers, rows, strategy, [headers[i] for i in id_idx],
                [headers[i] for i in phone_cols + email_cols], warnings)


# ---------------------------------------------------------------------- privacy for previews and logs
def mask(value):
    if isinstance(value, dict):
        return {k: mask(v) for k, v in value.items()}
    if not isinstance(value, str):
        return value
    if EMAIL_RE.match(value):
        name, _, domain = value.partition("@")
        return f"{name[:1]}***@{domain}"
    digits = re.sub(r"\D", "", value)
    if _PHONE_SHAPE.match(value) and not _DATE_SHAPE.match(value) and 8 <= len(digits) <= 15:
        return "*" * (len(digits) - 3) + digits[-3:]
    return value


_PHONE_SHAPE = re.compile(r"^[+\d\s().-]{8,22}$")
_DATE_SHAPE = re.compile(r"^\d{4}-\d{2}-\d{2}|^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}")
