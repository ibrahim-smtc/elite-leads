"""Keeps the old start command (uvicorn app:app) working. The service lives in sheets_connector/."""
from sheets_connector.server import app  # noqa: F401
