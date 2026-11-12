"""
Timer that keeps MongoDB up to date without anyone pressing anything.

Every AUTO_SYNC_MINUTES the server downloads the latest sheet (same code as POST /sync), upserts it
into MongoDB, and hands new/changed leads to the CRM agent (same cap and batching as always).

Turn it on by setting AUTO_SYNC_SHEET_URL (the sheet link). Leave it unset and the timer stays off.
"""
import os
import threading
import time
from datetime import datetime, timezone

from lead_to_crm import count_pending, process_pending
from sync import sync_sheet

SHEET_URL = os.getenv("AUTO_SYNC_SHEET_URL", "").strip()
try:
    MINUTES = float(os.getenv("AUTO_SYNC_MINUTES", "10") or 10)    # any number: 15, 30, 45, 60 ...
except ValueError:
    print("[auto-sync] AUTO_SYNC_MINUTES is not a number, using 10")
    MINUTES = 10.0
MINUTES = max(MINUTES, 0.5)                                         # never faster than every 30 seconds
FIRST_DELAY_SECONDS = 20          # let the server finish starting before the first run

STATE = {"enabled": bool(SHEET_URL), "every_minutes": MINUTES, "runs": 0,
         "last_run": None, "last_result": None, "last_error": None}
_started = False


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def run_once():
    """One round: download sheet -> MongoDB -> CRM agent (only if there is something pending)."""
    STATE["runs"] += 1
    STATE["last_run"] = _now()
    try:
        res = sync_sheet(sheet_url=SHEET_URL)
        summary = {k: res.get(k) for k in ("rows_read", "inserted", "updated", "unchanged", "removed_placeholders")}
        if res.get("status") == "success" and res.get("database"):
            pending = count_pending(res["database"], res["collection"])
            summary["crm_leads_pending"] = pending
            if pending:
                process_pending(db_name=res["database"], collection_name=res["collection"])
        STATE["last_result"], STATE["last_error"] = summary, None
        print(f"[auto-sync] {STATE['last_run']} ok {summary}")
    except Exception as e:                       # never let one bad round kill the timer
        STATE["last_error"] = str(e)
        print(f"[auto-sync] {STATE['last_run']} FAILED: {e}")


def _loop():
    time.sleep(FIRST_DELAY_SECONDS)
    while True:
        started = time.time()
        run_once()
        time.sleep(max(30, MINUTES * 60 - (time.time() - started)))


def start():
    """Start the timer thread once (does nothing when AUTO_SYNC_SHEET_URL is not set)."""
    global _started
    if _started or not SHEET_URL:
        return
    _started = True
    threading.Thread(target=_loop, daemon=True, name="auto-sync").start()
    print(f"[auto-sync] ON: every {MINUTES:g} min")
