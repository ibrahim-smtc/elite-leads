# Google Sheets -> Perfox CRM

Register a public read-only Google Sheet once. Every new or edited row becomes a lead in the Perfox CRM.

Full guide: [PRODUCTION.md](PRODUCTION.md)

```
sheets_connector/   the service (API + scheduler)
tests/              pytest suite (set TEST_MONGODB_URI to include the MongoDB tests)
Dockerfile          container image
.env.example        every setting
```

Run locally: `pip install -r requirements-dev.txt`, copy `.env.example` to `.env`, then
`uvicorn sheets_connector.server:app --port 8000` and open http://127.0.0.1:8000/docs.
