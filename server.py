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

import asyncio
import importlib
import logging
import os
import sys
import threading
import time
from contextlib import asynccontextmanager

from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("server")

VERSION = "0.2.0"
SCHEDULER: AsyncIOScheduler | None = None
_argv_lock = threading.Lock()


def _run_script(module_name: str, argv: list[str], job_name: str) -> int:
    """Import a script module fresh and call its main() with patched sys.argv.

    sys.argv mutation is global so we serialize via _argv_lock. Each call
    re-imports so module-level state (e.g. token caches) starts clean.
    """
    log.info("[job/%s] start argv=%s", job_name, argv)
    started = time.time()
    with _argv_lock:
        old_argv = sys.argv
        try:
            mod = importlib.import_module(module_name)
            sys.argv = [module_name] + argv
            rc = mod.main()
            log.info("[job/%s] finished rc=%s in %.1fs", job_name, rc, time.time() - started)
            return int(rc) if rc is not None else 0
        except SystemExit as exc:
            code = int(exc.code) if isinstance(exc.code, int) else 1
            log.warning("[job/%s] SystemExit(%d) after %.1fs", job_name, code, time.time() - started)
            return code
        except Exception:
            log.exception("[job/%s] failed after %.1fs", job_name, time.time() - started)
            raise
        finally:
            sys.argv = old_argv


def _job_daily_sync() -> int:
    return _run_script(
        "sync_feishu_groups_to_base",
        [
            "--scheduled-daily",
            "--refresh-metadata-tables",
            "--skip-share-links",
            "--fast-metadata",
            "--skip-groupchat-field-updates",
            "--sync-batch-size", "200",
            "--read-concurrency", "12",
            "--sync-timezone", "Asia/Shanghai",
            "--chat-order", "created_desc",
        ],
        "daily-sync",
    )


def _job_external_join() -> int:
    return _run_script(
        "ensure_bot_in_external_chats",
        ["--apply", "--allow-chat-failures"],
        "external-join",
    )


def _job_bulk_refresh() -> int:
    return _run_script(
        "bulk_message_probe",
        ["refresh", "--max-age-days", "7"],
        "bulk-stats-refresh",
    )


JOB_FUNCS = {
    "daily-sync": _job_daily_sync,
    "external-join": _job_external_join,
    "bulk-stats-refresh": _job_bulk_refresh,
}


def _check_required_env() -> dict[str, bool]:
    return {
        "LARK_APP_ID": bool(os.getenv("LARK_APP_ID")),
        "LARK_APP_SECRET": bool(os.getenv("LARK_APP_SECRET")),
        "LARK_BASE_URL": bool(os.getenv("LARK_BASE_URL")),
    }


def _start_scheduler() -> AsyncIOScheduler:
    """Wire all the cron jobs. Each job runs in a single-worker thread pool
    so they never block the asyncio event loop and never overlap with each
    other (sys.argv patching needs serialization)."""
    executors = {"default": APSThreadPoolExecutor(max_workers=1)}
    sch = AsyncIOScheduler(timezone="UTC", executors=executors)
    enable_jobs = os.getenv("ENABLE_SCHEDULED_JOBS", "true").lower() == "true"
    if not enable_jobs:
        log.warning("ENABLE_SCHEDULED_JOBS=false; cron jobs are DISABLED")
        sch.start()
        return sch

    sch.add_job(
        _job_daily_sync,
        CronTrigger(hour=2, minute=0, timezone="UTC"),
        id="daily-sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    sch.add_job(
        _job_external_join,
        CronTrigger(hour=14, minute=0, timezone="UTC"),
        id="external-join",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    sch.add_job(
        _job_bulk_refresh,
        CronTrigger(hour=12, minute=0, timezone="UTC"),
        id="bulk-stats-refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
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
        "endpoints": [
            "/healthz",
            "/admin/jobs",
            "/admin/run/{job_id}",
            "/api/dashboard/data",
            "/api/bulk-send",
            "/ws/bulk-progress/{id}",
        ],
    }


@app.get("/admin/jobs")
async def list_jobs() -> dict:
    if not SCHEDULER:
        raise HTTPException(503, "scheduler not running")
    return {
        "jobs": [
            {
                "id": j.id,
                "next_run": j.next_run_time.isoformat() if j.next_run_time else None,
                "trigger": str(j.trigger),
            }
            for j in SCHEDULER.get_jobs()
        ]
    }


@app.post("/admin/run/{job_id}")
async def run_job_now(job_id: str) -> dict:
    """Fire a job in the background; returns immediately. Tail the
    server logs for the job's stdout/stderr."""
    if job_id not in JOB_FUNCS:
        raise HTTPException(404, f"unknown job {job_id} (valid: {list(JOB_FUNCS)})")
    func = JOB_FUNCS[job_id]
    asyncio.create_task(asyncio.to_thread(func))
    log.info("[admin] manually queued job=%s", job_id)
    return {"ok": True, "queued": job_id, "note": "follow logs for progress"}
