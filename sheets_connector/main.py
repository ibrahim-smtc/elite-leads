"""
Entry points.

  API + scheduler in one process (simplest):  uvicorn sheets_connector.main:app --host 0.0.0.0 --port $PORT
  Scaled out:  run the API with SCHEDULER_ENABLED=false, and one or more workers with
               python -m sheets_connector.main worker
"""
from __future__ import annotations

import logging
import signal
import sys
import threading

from dotenv import load_dotenv

from .api import build_components, create_app
from .config import ConfigError, Settings
from .logs import setup_logging

load_dotenv()
log = logging.getLogger("sheets_connector")


def _settings() -> Settings:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    try:
        for warning in settings.validate():
            log.warning(warning)
    except ConfigError as exc:
        log.error("cannot start: %s", exc)
        raise SystemExit(2) from exc
    return settings


def run_worker() -> None:
    """Scheduler only, no HTTP. Stops cleanly on SIGTERM / Ctrl+C."""
    settings = _settings()
    comp = build_components(settings, with_scheduler=True)
    comp.store.ensure_indexes()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    comp.scheduler.start()
    while not stop.wait(1.0):     # short waits keep Ctrl+C responsive on Windows
        pass
    comp.scheduler.stop()
    comp.store.close()


if __name__ == "__main__" and sys.argv[1:2] == ["worker"]:
    run_worker()
else:
    app = create_app(_settings())
