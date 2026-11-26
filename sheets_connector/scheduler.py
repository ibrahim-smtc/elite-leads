"""Background loop: claims sheets that are due and syncs them on a small thread pool."""
from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from .config import Settings
from .service import SyncService
from .store import utcnow

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, store, service: SyncService, settings: Settings, owner: str | None = None):
        self.store, self.service, self.settings = store, service, settings
        self.owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self._stop = threading.Event()
        self._running = 0
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=settings.worker_concurrency, thread_name_prefix="sheet-sync")
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> int:
        with self._lock:
            return self._running

    def start(self) -> None:
        if self._thread:
            return
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()
        log.info("scheduler started", extra={"owner": self.owner, "workers": self.settings.worker_concurrency})

    def stop(self, timeout: float = 30.0) -> None:
        """Stop claiming new work and let running syncs finish (their leases expire if we are killed)."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)
        self._pool.shutdown(wait=True, cancel_futures=True)
        log.info("scheduler stopped", extra={"owner": self.owner})

    def tick(self) -> int:
        """Claim and start as many due sheets as there are free workers. Returns how many were started."""
        started = 0
        while not self._stop.is_set() and self.running < self.settings.worker_concurrency:
            try:
                source = self.store.claim_due_source(self.owner, utcnow(), self.settings.lease_seconds)
            except Exception:
                log.exception("could not claim work (database unavailable?)")
                break
            if source is None:
                break
            with self._lock:
                self._running += 1
            self._pool.submit(self._run, source)
            started += 1
        return started

    def _run(self, source: dict) -> None:
        try:
            self.service.run_source(source, self.owner)
        finally:
            with self._lock:
                self._running -= 1

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(self.settings.tick_seconds)
