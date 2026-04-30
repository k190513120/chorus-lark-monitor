"""
FastAPI backend for Chorus Lark Monitor.

Endpoints (phase 1 only — health + sanity):
  GET  /healthz                -> {"ok": true, "version": ...}
  GET  /api/dashboard/data     -> reads Base, returns same shape as data.jsx (TODO)
  POST /api/bulk-send          -> queue a bulk send job (TODO)
  GET  /ws/bulk-progress/{id}  -> WebSocket progress stream (TODO)

Cron tasks (managed by APScheduler):
  daily-sync         -> sync_feishu_groups_to_base.main() at 02:00 UTC
  external-join      -> ensure_bot_in_external_chats.main() at 14:00 UTC
  bulk-stats-refresh -> bulk_message_probe.cmd_refresh() at 12:00 UTC
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("server")

VERSION = "0.1.0"
SCHEDULER: AsyncIOScheduler | None = None


def _check_required_env() -> dict[str, bool]:
    return {
        "LARK_APP_ID": bool(os.getenv("LARK_APP_ID")),
        "LARK_APP_SECRET": bool(os.getenv("LARK_APP_SECRET")),
        "LARK_BASE_URL": bool(os.getenv("LARK_BASE_URL")),
    }


def _start_scheduler() -> AsyncIOScheduler:
    """Wire all the cron jobs. Each job imports its target lazily so that
    a misconfigured job doesn't take the whole service down."""
    sch = AsyncIOScheduler(timezone="UTC")
    enable_jobs = os.getenv("ENABLE_SCHEDULED_JOBS", "true").lower() == "true"
    if not enable_jobs:
        log.warning("ENABLE_SCHEDULED_JOBS=false; cron jobs are DISABLED")
        sch.start()
        return sch

    # Phase 1 just registers placeholder jobs that log; phase 2 wires them
    # to the real script entrypoints.
    sch.add_job(
        lambda: log.info("[cron] daily-sync placeholder fired"),
        CronTrigger(hour=2, minute=0, timezone="UTC"),
        id="daily-sync",
        replace_existing=True,
    )
    sch.add_job(
        lambda: log.info("[cron] external-join placeholder fired"),
        CronTrigger(hour=14, minute=0, timezone="UTC"),
        id="external-join",
        replace_existing=True,
    )
    sch.add_job(
        lambda: log.info("[cron] bulk-stats-refresh placeholder fired"),
        CronTrigger(hour=12, minute=0, timezone="UTC"),
        id="bulk-stats-refresh",
        replace_existing=True,
    )
    sch.start()
    for job in sch.get_jobs():
        log.info("[cron] registered job=%s next_run=%s", job.id, job.next_run_time)
    return sch


@asynccontextmanager
async def lifespan(app: FastAPI):
    global SCHEDULER
    SCHEDULER = _start_scheduler()
    log.info("server v%s up; env_ok=%s", VERSION, _check_required_env())
    try:
        yield
    finally:
        if SCHEDULER:
            SCHEDULER.shutdown(wait=False)
            log.info("scheduler shut down")


app = FastAPI(title="Chorus Lark Monitor", version=VERSION, lifespan=lifespan)

# Permissive CORS for now — tighten when the dashboard origin is known.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
async def healthz() -> dict:
    return {
        "ok": True,
        "version": VERSION,
        "env": _check_required_env(),
        "scheduler_jobs": [j.id for j in SCHEDULER.get_jobs()] if SCHEDULER else [],
    }


@app.get("/")
async def root() -> dict:
    return {
        "service": "chorus-lark-monitor",
        "version": VERSION,
        "endpoints": ["/healthz", "/api/dashboard/data", "/api/bulk-send", "/ws/bulk-progress/{id}"],
    }
