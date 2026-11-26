# Google Sheets → Perfox CRM (production)

Any Google Sheet, in any column layout, becomes leads in the Perfox CRM. Register a sheet once.
From then on, every new or edited row is picked up within minutes.

```
Google Sheet(s)
   │  public "anyone with the link can view" sheets, downloaded as CSV every N minutes
   ▼
Sheets connector  (this repo: sheets_connector/; no AI, no tokens)
   │  each new or changed row, posted as it is, with the sheet's own column names
   ▼
CRM "Address for records"  (Bearer key of a records source)
   │  the CRM removes duplicate records, drops anyone who refused consent, and queues the rest
   ▼
Records reviewer  (Perfox agent, scheduled; runs the AI only when records are waiting)
   │  finds or adds the lead, keeps the fields, facts and opportunity, then closes the record
   ▼
Lead in the CRM
```

## Why this design

- **Built on the CRM's own intake.** Account → Sources → "Records from another system" is the CRM's
  supported way to bring records in. It removes duplicates, checks consent, keeps an audit trail
  (Account → Changes) and makes each change undoable. We do not rebuild any of that.
- **Works with any sheet layout.** The connector does not map columns. It sends each row with its own column
  names, and the records reviewer works out what each column means. A new sheet needs no code.
- **No access to the client's Google account.** Only a public read-only link is needed.
- **Token cost scales with real changes, not with sheet size.** The connector only sends rows that are new
  or changed. The reviewer's schedule first asks the CRM how many records are waiting (a plain HTTP call)
  and starts the AI only when that number is above 0.
- **Safe to run on many servers.** Each sheet is claimed with a lease in MongoDB, so any number of
  workers can run side by side and never sync the same sheet twice at once.

## Setup (one time)

1. **CRM: records source and key.** As owner or admin: Account → Sources → *Add a source*, kind
   **"Records from another system"** → *Make a key* (it is shown once; copy it). Copy the **Address for records**
   from the same page.
2. **CRM: records reviewer.** In Perfox Studio, import `records-reviewer.elite-motors.workflow.json`.
   In the *HTTP Request* node, set the CRM bearer key. Check the Integration node's enabled actions
   (17 tools, already limited), then publish. Back on Account → Sources, pick it under
   *The assistant that reviews records*.
3. **Sheets.** Each client shares their sheet as **"Anyone with the link can view"** (read only) and gives you
   the link. No Google account, key or script is needed, and nothing is installed in the client's sheet.
4. **Deploy** (Docker or Render) with the variables in `.env.example` (production section):
   `APP_ENV=production`, `MONGODB_URI`, `ADMIN_API_KEY`, `CRM_RECORDS_URL`, `CRM_SOURCE_KEY`. The service will not start if a required one is missing.
   Use an always-on instance. A free instance that sleeps also stops the schedule.
5. **Set `SHEET_URL`** (above). Optionally register more sheets with the API (below).

## Deploy on Render

| Setting | Value |
|---|---|
| Build command | `pip install -r requirements.txt` |
| Start command | `uvicorn sheets_connector.main:app --host 0.0.0.0 --port $PORT` (the older `uvicorn app:app ...` also works) |
| Python | `.python-version` (3.12.10) |
| Plan | A paid, always-on instance. A free one sleeps and the schedule stops. |

Environment variables (Render sets `RENDER`, which makes the app run in production mode and refuse to start
without these): `MONGODB_URI`, `ADMIN_API_KEY` (24+ characters), `CRM_RECORDS_URL`, `CRM_SOURCE_KEY`.
Optional: `CONNECTOR_MONGODB_DB` and the tuning values in `.env.example`. Remove variables left over from the
earlier demo (`PERFOX_*`, `AUTO_SYNC_*`, `GOOGLE_SHEET_URL`, `MONGODB_DB`, `MONGODB_COLLECTION`, `SYNC_SECRET`).
Check the deploy at `/health` (version) and `/ready` (database and CRM profile).

## The records reviewer agent

The agent that turns records into leads runs inside Perfox, not in this service. Its definition is in
`agent/records-reviewer.workflow.json`: import it in the tenant's Perfox Studio, pick that tenant's CRM server on the
Integration node, set the CRM bearer key on the HTTP node (left empty in the file on purpose), publish it, and select
it on the CRM's Account -> Sources page. If you change the agent in Studio, export it and update the file.

## The three settings you will change

All are environment variables (Render: Environment tab):

| Variable | What it controls | Example |
|---|---|---|
| `SHEET_URL` (or `SHEET_URLS`, comma separated) | Which Google Sheet(s) to read. Public "anyone with the link can view" links. Added on the next start; a link removed is paused. | `https://docs.google.com/spreadsheets/d/<id>/edit?gid=0` |
| `DEFAULT_POLL_MINUTES` | How often each sheet is checked. | `15` |
| `MAX_ROWS_PER_SYNC` | Most rows sent to the CRM per run. The rest follow in the next runs. | `3` |

The API below is optional: use it for per-sheet settings or to add sheets without restarting.

## Using the API

All `/v1` calls need the header `X-Api-Key: <ADMIN_API_KEY>`. Interactive docs are at `/docs`.

| Call | What it does |
|---|---|
| `POST /v1/sheets` | Register a sheet. It reads the sheet first and answers with a preview of what will be sent (phones and emails masked). |
| `GET /v1/sheets` | All sheets, with status and last run. |
| `GET /v1/sheets/{id}` | One sheet: settings, last run, rows by status. |
| `PATCH /v1/sheets/{id}` | Change settings (`poll_minutes`, `enabled`, `ignore_columns`, ...). |
| `POST /v1/sheets/{id}/sync` | Sync now, without waiting for the schedule. |
| `GET /v1/sheets/{id}/preview` | What would be sent now. Nothing is posted. |
| `GET /v1/sheets/{id}/rows?status=failed` | Rows that failed, were refused, or are waiting for a phone or email. Each sent row shows the CRM's own status (`crm_status`, for example `needs_person` when its reviewer wants a person to look). |
| `DELETE /v1/sheets/{id}` | Stop syncing this sheet. Records already in the CRM stay there. |
| `GET /health`, `GET /ready` | For the platform's health checks. No key needed. Nothing sensitive is shown. |

Register the Revspot sheet:

```json
POST /v1/sheets
{
  "url": "https://docs.google.com/spreadsheets/d/<id>/edit#gid=0",
  "name": "Revspot - VW Bangalore",
  "poll_minutes": 5,
  "ignore_columns": ["Most Recent Status", "Attempt #", "Current Status", "Last Updated", "Updated By"],
  "extra_fields": {"Lead source": "Revspot"}
}
```

Per-sheet settings:

| Setting | Default | Meaning |
|---|---|---|
| `poll_minutes` | 5 | How often the sheet is checked (1 to 1440). |
| `ignore_columns` | none | Columns whose changes should not send the row again, such as call-tracking columns. They are still included when the row is sent for another reason. |
| `id_columns` | auto | Columns that identify a row (for example `Lead ID`). If left empty: an id-like column, else phone/email plus the creation date, else the whole row. |
| `require_contact` | true | Hold a row until it has a phone or email, so half-typed rows are never sent. |
| `header_row` | 1 | The row with the column names. |
| `form_key` | none | Sent as `form_key`, which tells the CRM which branch the lead belongs to. |
| `extra_fields` | none | Fixed fields added to every record from this sheet. |
| `crm_profile` | default | Which CRM account (see `CRM_PROFILES`, for several clients). |

## How a row is handled

| Situation | What happens |
|---|---|
| New row with a phone or email | Sent on the next check. |
| Row edited | Sent again as a new version (`external_id` = row id + content hash). The reviewer updates the same lead. |
| Row moved up or down, or rows inserted above it | Nothing is sent. Its identity does not depend on its position. |
| Only ignored columns changed | Nothing is sent. |
| Row without phone or email | Held (`waiting_contact`). Sent once a phone or email is added. |
| More than `MAX_ROWS_PER_SYNC` (200) changes | The first 200 are sent. The next run starts 30 s later and continues. |
| CRM busy or down (429, 5xx, timeouts) | Retried with backoff (Retry-After honoured). If it still fails, the row is retried on the next runs, up to `MAX_ATTEMPTS_PER_ROW`, then marked `dead` until it changes. |
| CRM refuses the record (400/422) | Marked `rejected` with the reason. Not retried until the row changes. |
| Wrong CRM key or address (401/403/404) | The run stops at once and the sheet shows `status: error` with the reason. It is retried every 15 min, and no rows are lost. |
| Sheet not public, or deleted | `status: error`: "Check the link, and that it is shared as 'Anyone with the link can view'". Retried every 15 min. |
| Google or the network down | Retried with growing waits (5, 10, 20 ... up to 60 min). |
| Row deleted from the sheet | Nothing is sent. The lead stays in the CRM. |

## Scaling

- **More sheets:** raise `WORKER_CONCURRENCY` (sheets synced at once per process), or run more workers.
- **Separate API and workers:** run the API with `SCHEDULER_ENABLED=false`, and N workers with
  `python -m sheets_connector.main worker`. Leases keep them from overlapping, and a crashed worker's
  sheet is picked up again when its lease (`LEASE_SECONDS`, default 15 min) runs out.
- **Protect the CRM:** `CRM_CONCURRENCY` (parallel posts per sync) and `CRM_MAX_REQUESTS_PER_SECOND` (per process).
- **Large sheets:** one sheet is downloaded as one CSV file per check. Row states are kept per sheet in MongoDB, with one
  small document per row. Sheets of tens of thousands of rows are fine.
- **Several CRM accounts:** `CRM_PROFILES`, plus `crm_profile` on each sheet.

## Security

- Sheets are read-only public links: the connector never writes to a sheet and holds no Google credentials.
  Anyone with a link can read that sheet, so share only what the client agrees to.
- Keys (CRM source key, admin key) come from environment variables only. They are never stored
  in MongoDB, returned by the API, or written to logs.
- Every `/v1` route needs `X-Api-Key`, compared in constant time. Production refuses to start without it.
- Logs are JSON, one line per event, and never contain phone numbers or emails. Previews mask them by default.
- The container runs as a non-root user.

## Operations

- **Is it healthy?** `GET /ready` returns 503 if the database is unreachable. Alert on any sheet with
  `status: "error"` in `GET /v1/sheets`.
- **What happened to a row?** `GET /v1/sheets/{id}/rows?status=failed` (also `rejected`, `dead`,
  `waiting_contact`). For what the reviewer did with it, see CRM → Account → Changes.
- **Records piling up in the CRM?** Account → Sources shows "Records held" when the reviewer is not collecting
  them. Check that the reviewer agent is published and its schedule is on.
- **Send everything again:** `PATCH` with a changed `id_columns`, or delete and register the sheet again.
  The CRM removes duplicates of identical records.

## Tests

`pip install -r requirements-dev.txt` and run `pytest`. There are 53 tests: row planning, the CRM client
(retries, backoff, refusals), the sync engine (edits, moves, held rows, row cap, failures, leases) and the API.
Set `TEST_MONGODB_URI` to also run the MongoDB tests. They use a throwaway database and clean it up.

## To confirm with the first real key

- The exact response of the records address (the client accepts `created` / `joined_existing` /
  `rejected` and anything else as `accepted`).
- That a new version of an edited row is accepted. The client sends a new `external_id` per version
  so that duplicate removal can never drop an edit.
