# elite-leads

Google Sheet -> MongoDB -> AI agent -> CRM customer profiles.

```
POST /sync (sheet link)
  1. sync.py      downloads the sheet and upserts every row into MongoDB
  2. lead_to_crm  picks the leads that are new or whose profile data changed,
                  queues them 5 per agent call, and for each group:
                    - code creates the customer in the CRM
                    - the Perfox agent decides which of the filled fields matter
                      and writes them to the CRM profile (city, preferred model,
                      purchasing timeline, budget, AI summary in Remarks)
                    - the result is saved on each lead in MongoDB (crm_sync)
```

## Files

| File | Purpose |
|---|---|
| `app.py` | FastAPI service. `POST /sync` uploads the sheet, then starts the CRM step in the background |
| `sync.py` | Sheet -> MongoDB upload |
| `lead_to_crm.py` | Change detection, queue, CRM customer creation, calls the Perfox agent |
| `.env.example` | All settings (copy to `.env`, never commit `.env`) |

## Run locally

```
pip install -r requirements.txt
python app.py                                  # http://127.0.0.1:8000/docs
python lead_to_crm.py --show --limit 3         # dry run: nothing is written or sent
python lead_to_crm.py --live                   # process every pending lead
python lead_to_crm.py --baseline --live        # mark leads already in Mongo as handled
```

## Deploy (Render web service)

- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Set the variables from `.env.example` as environment variables (at least `MONGODB_URI`,
  `PERFOX_API_KEY`, `PERFOX_AGENT_WEBHOOK_URL`, `PERFOX_AGENT_WEBHOOK_SECRET`) and `PYTHON_VERSION=3.12.10`.
- In MongoDB Atlas, allow Render's IPs under Network Access.
- A `/sync` request can name `db_name` and `collection_name`. If it does not, they default to
  `elite-test-db` and `og-leads` (or `MONGODB_DB` / `MONGODB_COLLECTION` when set).
- If `SYNC_SECRET` is set, `/sync` requires a matching `X-Sync-Secret` header. If it is unset,
  `/sync` is open to anyone who knows the URL: fine for testing, set it before production.

## Notes

- A lead goes back to the agent only when its profile data changes. Row position and call/admin
  columns (call status, attempt, timestamps, campaign) do not count as a change.
- The sheet must be shared as "Anyone with the link can view" so `sync.py` can download it.
