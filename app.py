"""Keeps the old start command (uvicorn app:app) working. The service lives in sheets_connector/."""
from sheets_connector.main import app  # noqa: F401
