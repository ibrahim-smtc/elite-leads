"""
FastAPI service for Google Sheets to MongoDB Atlas sync.
Imports the sync logic directly from sync.py. After the upload, new or changed leads are handed
to the CRM agent (lead_to_crm.py) in the background.
"""
import hmac
import os
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel
import uvicorn
import auto_sync
from sync import sync_sheet
from lead_to_crm import BATCH_SIZE, MAX_PER_RUN, SETTLE_SECONDS, count_pending, process_pending

# Shared secret the caller (Postman, the timer, or any caller) sends in the X-Sync-Secret header.
# Without it, anyone who knows the URL could start agent runs (tokens) or write to any collection.
SYNC_SECRET = os.getenv("SYNC_SECRET")
if not SYNC_SECRET:
    print("WARNING: SYNC_SECRET is not set - POST /sync is open to anyone who knows the URL.")


def check_secret(x_sync_secret: str | None = Header(default=None)):
    if SYNC_SECRET and not hmac.compare_digest((x_sync_secret or "").encode(), SYNC_SECRET.encode()):
        raise HTTPException(status_code=401, detail="invalid or missing X-Sync-Secret header")

app = FastAPI(
    title="Google Sheets to MongoDB Sync API",
    version="1.0.0"
)


@app.on_event("startup")
def start_timer():
    auto_sync.start()      # only runs when AUTO_SYNC_SHEET_URL is set

# Request schema: accepts any Google Sheet link
class SheetSyncRequest(BaseModel):
    sheet_url: str
    db_name: str | None = None
    collection_name: str | None = None
    process_crm: bool = True       # after the upload, let the agent build customer profiles

# POST endpoint: uploads the sheet to MongoDB, then hands new/changed leads to the CRM agent
@app.post("/sync", summary="Sync a Google Sheet to MongoDB Atlas, then build CRM profiles",
          dependencies=[Depends(check_secret)])
def sync_endpoint(payload: SheetSyncRequest, background: BackgroundTasks):
    try:
        result = sync_sheet(
            sheet_url=payload.sheet_url,
            db_name=payload.db_name,
            collection_name=payload.collection_name
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # The agent works through the leads in the background (it takes a while), so the upload
    # response is not held up. Progress: crm_sync on each lead in MongoDB, and crm_agent_runs.jsonl.
    if payload.process_crm and result.get("status") == "success" and result.get("database"):
        try:
            pending = count_pending(result["database"], result["collection"])
        except Exception:
            pending = None  # could not count; still run
        result["crm_leads_pending"] = pending
        result["crm_cap_per_run"] = MAX_PER_RUN or "none"
        # how many leads the agent will actually take this time (the cap applies per run)
        result["crm_leads_this_run"] = pending if (pending is None or not MAX_PER_RUN) else min(pending, MAX_PER_RUN)
        if pending == 0:
            result["crm_processing"] = "nothing to do"
        else:
            background.add_task(
                process_pending,
                db_name=result["database"],
                collection_name=result["collection"],
            )
            result["crm_processing"] = "started in background"
    return result

@app.get("/health", summary="Live server settings (no secrets, changes nothing)")
def health():
    return {
        "ok": True,
        "crm_cap_per_run": MAX_PER_RUN or "none",
        "leads_per_agent_call": BATCH_SIZE,
        "sync_secret_required": bool(SYNC_SECRET),
        "settle_seconds": SETTLE_SECONDS,
        "auto_sync": auto_sync.STATE,          # timer on/off, last run time, last result or error
        "commit": os.getenv("RENDER_GIT_COMMIT", "local")[:7],  # which version of the code is running
    }

if __name__ == "__main__":
    print("\nStarting server on http://127.0.0.1:8000")
    print("API Documentation: http://127.0.0.1:8000/docs\n")
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
