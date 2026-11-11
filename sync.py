"""
Pulls a Google Sheet as CSV and upserts the data into MongoDB Atlas.

Can be run directly:
    python sync.py --dry-run
    python sync.py

Or imported into other apps:
    from sync import sync_sheet
"""
import os
import sys
import csv
import io
import re
import hashlib
import json
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

# Helper functions to clean up data
def snake(h: str) -> str:
    h = h.strip().replace("#", "")
    h = re.sub(r"[^A-Za-z0-9]+", "_", h)
    return h.strip("_").lower()

def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return None if (s == "" or s.upper() == "NA") else s

def num(v):
    if v is not None and re.match(r"^-?\d+(\.\d+)?$", str(v)):
        return float(v) if "." in str(v) else int(v)
    return v

def bool_val(v):
    if v is None:
        return None
    s = str(v).lower()
    if s in ("true", "yes", "1"):
        return True
    if s in ("false", "no", "0"):
        return False
    return v

def sync_sheet(sheet_url: str = None, sheet_id: str = None, gid: str = None, db_name: str = None, collection_name: str = None, dry_run: bool = False):
    """
    Core sync function: downloads the sheet, cleans it, and upserts to MongoDB Atlas.
    """
    # 1. Resolve sheet_id & gid from URL or fallback to .env
    if sheet_url:
        id_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
        if id_match:
            sheet_id = id_match.group(1)
        gid_match = re.search(r"[#&?]gid=([0-9]+)", sheet_url)
        if gid_match:
            gid = gid_match.group(1)

    sheet_id = sheet_id or os.getenv("SHEET_ID")  # no default: the sheet link comes from the request or .env, never from code
    gid = gid or os.getenv("SHEET_GID", "0")
    target_db = db_name or os.getenv("MONGODB_DB", "crm_staging")
    target_col = collection_name or os.getenv("MONGODB_COLLECTION", "elite_vw_leads")

    # 2. Fetch the sheet CSV from Google
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    res = requests.get(url, allow_redirects=True)
    if not res.ok:
        raise RuntimeError(f"Sheet fetch failed: HTTP {res.status_code} (Make sure link sharing is set to 'Anyone with the link can view')")

    csv_text = res.text
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows or len(rows) < 2:
        return {"status": "success", "message": "Sheet is empty.", "rows": 0, "inserted": 0}

    headers = rows[0]
    data_rows = rows[1:]

    # 3. Clean and transform rows
    now = datetime.now(timezone.utc)
    docs = []

    for i, row in enumerate(data_rows):
        d = {}
        for h, v in zip(headers, row):
            k = snake(h)
            val = clean(v)
            if k not in d or d[k] is None:
                d[k] = val

        # Phone formatting (checks 'phone', 'number', 'mobile', etc.)
        raw_phone = d.get("phone") or d.get("number") or d.get("mobile") or d.get("contact") or d.get("phone_number")
        d["phone_raw"] = raw_phone
        raw_digits = re.sub(r"\D", "", raw_phone or "")
        if len(raw_digits) == 10:
            d["phone"] = f"+91{raw_digits}"
        elif raw_digits:
            d["phone"] = f"+{raw_digits}"
        else:
            d["phone"] = None

        # Date parsing
        raw_date = d.get("date") or d.get("created_at") or d.get("timestamp") or d.get("call_date")
        parsed_date = None
        if raw_date:
            try:
                parsed_date = datetime.fromisoformat(raw_date)
            except Exception:
                parsed_date = None
        d["date"] = parsed_date

        if "attempt" in d and d["attempt"] is not None:
            d["attempt"] = num(d["attempt"])
        else:
            d["attempt"] = None

        for k in ["age", "years_of_experience"]:
            if k in d:
                d[k] = num(d[k])

        for k in ["iit_iim", "mba", "studied_abroad", "employed", "engineer", "enriched", "valid_indian_name"]:
            if k in d:
                d[k] = bool_val(d[k])

        # Generate unique _id so leads don't collide
        date_iso = parsed_date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z") if parsed_date else ""
        if d.get("call_id"):
            key = f"call_id|{d['call_id']}"
        elif d.get("phone"):
            sub_key = d.get("campaign_name") or d.get("enquiry") or d.get("name") or ""
            key = f"lead|{d['phone']}|{date_iso}|{sub_key}"
        else:
            key = f"row|{sheet_id}|{gid}|{i + 2}"

        d["_id"] = hashlib.sha1(key.encode("utf-8")).hexdigest()
        d["_source"] = {"sheet_id": sheet_id, "gid": gid, "row_number": i + 2}
        docs.append(d)

    # 4. Diagnostic report
    ids = {d["_id"] for d in docs}
    report = {
        "rows": len(docs),
        "uniqueKeys": len(ids),
        "collidingKeys": len(docs) - len(ids),
        "missingPhone": sum(1 for d in docs if not d.get("phone")),
        "missingDate": sum(1 for d in docs if not d.get("date")),
        "fields": len(docs[0].keys()) if docs else 0
    }
    print(report)

    if dry_run:
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        print("Sample doc:\n", json.dumps(docs[-1], indent=2, default=json_serial))
        return {"status": "dry_run", "report": report}

    # 5. Bulk upsert into MongoDB Atlas
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise RuntimeError("MONGODB_URI missing — check your .env file.")

    client = MongoClient(mongodb_uri)
    try:
        col = client[target_db][target_col]
        by_id = {d["_id"]: d for d in docs}
        ops = []
        for _id, doc in by_id.items():
            doc_copy = {k: v for k, v in doc.items() if k != "_id"}
            ops.append(
                UpdateOne(
                    {"_id": _id},
                    {
                        "$set": {**doc_copy, "synced_at": now},
                        "$setOnInsert": {"first_synced_at": now}
                    },
                    upsert=True
                )
            )

        r = col.bulk_write(ops, ordered=False)
        col.create_index([("phone", 1)])
        unchanged = r.matched_count - r.modified_count
        summary = {
            "status": "success",
            "database": target_db,
            "collection": target_col,
            "sheet_id": sheet_id,
            "rows_read": len(docs),
            "inserted": r.upserted_count,
            "updated": r.modified_count,
            "unchanged": unchanged
        }
        print(f"Upserted into {target_db}.{target_col}: inserted={r.upserted_count} updated={r.modified_count} unchanged={unchanged}")
        return summary
    finally:
        client.close()

if __name__ == "__main__":
    is_dry = "--dry-run" in sys.argv
    env_url = os.getenv("GOOGLE_SHEET_URL") or os.getenv("SHEET_URL")
    sync_sheet(sheet_url=env_url, dry_run=is_dry)
