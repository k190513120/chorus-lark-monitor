"""
FastAPI backend for Chorus Lark Monitor.

Endpoints:
  GET  /healthz                  health check + env probe
  GET  /                          service banner

  GET  /admin/jobs                list APScheduler cron jobs
  POST /admin/run/{job_id}        manually fire a cron job

  GET  /api/dashboard/data        AppData JSON for the dashboard (60s cache)
  POST /api/bulk-send             start a bulk message job
  GET  /api/bulk-send/{batch_id}  poll a bulk job's progress (REST fallback)
  WS   /ws/bulk-progress/{id}     stream a bulk job's progress

Cron tasks (managed by APScheduler):
  daily-sync         -> sync_feishu_groups_to_base.main() at 02:00 UTC
  external-join      -> ensure_bot_in_external_chats.main() at 14:00 UTC
  bulk-stats-refresh -> bulk_message_probe.cmd_refresh() at 12:00 UTC
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("server")

VERSION = "0.3.0"
SCHEDULER: AsyncIOScheduler | None = None
_argv_lock = threading.Lock()
SYNC_TZ = ZoneInfo("Asia/Shanghai")

# Dashboard payload cache (60s) — building it scans Base, can take 30s+ for 16k chats.
_dashboard_cache: dict[str, Any] = {"data": None, "expires_at": 0.0}
DASHBOARD_TTL_SEC = 60

# In-memory bulk-send job tracker. Lost on container restart, fine for short-lived jobs.
_bulk_jobs: dict[str, dict[str, Any]] = {}
_bulk_jobs_lock = threading.Lock()


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


@app.get("/info")
async def info() -> dict:
    return {
        "service": "chorus-lark-monitor",
        "version": VERSION,
        "endpoints": [
            "/healthz",
            "/info",
            "/admin/jobs",
            "/admin/run/{job_id}",
            "/api/dashboard/data",
            "/api/bulk-send",
            "/api/bulk-send/{batch_id}",
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


# ─── /api/dashboard/data ──────────────────────────────────────────────────────

def _build_dashboard_payload() -> dict:
    """Read all 4 Base tables and produce the same shape as web/src/data.jsx
    used to embed. Heavy — scans every chat/member/message row."""
    import export_to_web
    from sync_feishu_groups_to_base import (
        CHAT_TABLE_NAME,
        MEMBER_TABLE_NAME,
        MESSAGE_TABLE_NAME,
        FeishuClient,
        load_timezone,
        parse_base_token,
    )

    app_id = os.environ["LARK_APP_ID"]
    app_secret = os.environ["LARK_APP_SECRET"]
    base_url = os.environ["LARK_BASE_URL"]
    sync_tz = load_timezone(os.getenv("SYNC_TIMEZONE", "Asia/Shanghai"))
    max_groups = int(os.getenv("WEB_MAX_GROUPS", "0"))
    max_messages_per_group = int(os.getenv("WEB_MAX_MESSAGES_PER_GROUP", "0"))

    base_token = parse_base_token(base_url)
    client = FeishuClient(app_id, app_secret)
    client.authenticate()

    chat_table_id = export_to_web.find_table_id(client, base_token, CHAT_TABLE_NAME)
    member_table_id = export_to_web.find_table_id(client, base_token, MEMBER_TABLE_NAME)
    message_table_id = export_to_web.find_table_id(client, base_token, MESSAGE_TABLE_NAME)

    chats = export_to_web.load_chats(client, base_token, chat_table_id)
    members_by_chat = export_to_web.load_members(client, base_token, member_table_id)
    messages_by_chat = export_to_web.load_messages(client, base_token, message_table_id, sync_tz)
    broadcasts = export_to_web.load_broadcasts(client, base_token, sync_tz)

    selected = export_to_web.pick_active_groups(chats, members_by_chat, messages_by_chat, max_groups)
    payload = export_to_web.build_app_data(
        selected,
        members_by_chat,
        messages_by_chat,
        sync_tz,
        max_messages_per_group,
        broadcasts=broadcasts,
    )
    payload["_meta"] = {
        "generated_at": int(time.time()),
        "chat_count": len(selected),
        "broadcast_count": len(broadcasts),
    }
    return payload


async def _get_or_build_dashboard_payload(force_refresh: bool = False) -> dict:
    now = time.time()
    if not force_refresh and _dashboard_cache["data"] and _dashboard_cache["expires_at"] > now:
        return _dashboard_cache["data"]
    log.info("[dashboard] building fresh payload (force=%s)", force_refresh)
    started = time.time()
    payload = await asyncio.to_thread(_build_dashboard_payload)
    _dashboard_cache["data"] = payload
    _dashboard_cache["expires_at"] = now + DASHBOARD_TTL_SEC
    log.info(
        "[dashboard] payload built in %.1fs (chats=%d broadcasts=%d)",
        time.time() - started,
        payload.get("_meta", {}).get("chat_count", 0),
        payload.get("_meta", {}).get("broadcast_count", 0),
    )
    return payload


@app.get("/api/dashboard/data")
async def get_dashboard_data(force_refresh: bool = False) -> dict:
    """Returns the AppData payload as JSON. Cached for 60s in memory."""
    return await _get_or_build_dashboard_payload(force_refresh)


@app.get("/src/data.jsx")
async def serve_dashboard_data_jsx(force_refresh: bool = False) -> Response:
    """Drop-in replacement for the static web/src/data.jsx. Front-end loads this
    via <script src="src/data.jsx"> and gets fresh AppData on every page load."""
    import export_to_web as ex
    payload = await _get_or_build_dashboard_payload(force_refresh)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    js = ex.DATA_TEMPLATE.format(
        generated_at=datetime.now(SYNC_TZ).strftime("%Y-%m-%d %H:%M:%S%z"),
        base_token=os.getenv("LARK_BASE_URL", "?"),
        group_count=len(payload.get("GROUPS") or []),
        msg_count=sum(len(g.get("messages", [])) for g in payload.get("GROUPS") or []),
        member_count=sum(len(g.get("members", [])) for g in payload.get("GROUPS") or []),
        payload=serialized,
    )
    return Response(content=js, media_type="application/javascript; charset=utf-8")


# ─── /api/bulk-send + /ws/bulk-progress ───────────────────────────────────────

def _new_bulk_job(chat_targets: list[dict], text: str, title: str) -> str:
    batch_id = datetime.now(SYNC_TZ).strftime("%Y%m%d%H%M%S") + f"_{len(_bulk_jobs):03d}"
    job = {
        "batch_id": batch_id,
        "title": title,
        "text": text,
        "total": len(chat_targets),
        "sent": 0,
        "failed": 0,
        "status": "queued",  # queued -> running -> done
        "current_chat": None,
        "errors": [],
        "started_at": int(time.time()),
        "ended_at": None,
        # asyncio.Queue carries (event, payload) tuples to subscribed websockets.
        # We use a list of queues so multiple subscribers can listen.
        "subscribers": [],
        "_lock": threading.Lock(),
    }
    with _bulk_jobs_lock:
        _bulk_jobs[batch_id] = job
    return batch_id


def _public_job_view(job: dict) -> dict:
    return {
        k: v
        for k, v in job.items()
        if k not in ("subscribers", "_lock")
    }


def _broadcast_progress(job: dict, event_type: str, payload: dict) -> None:
    """Push a snapshot to every subscriber's queue."""
    snapshot = {
        "type": event_type,
        "batch_id": job["batch_id"],
        "total": job["total"],
        "sent": job["sent"],
        "failed": job["failed"],
        "status": job["status"],
        "current_chat": job["current_chat"],
        **payload,
    }
    with job["_lock"]:
        subscribers = list(job["subscribers"])
    for q in subscribers:
        try:
            q.put_nowait(snapshot)
        except Exception:  # full queue or closed
            pass


def _run_bulk_send_thread(batch_id: str, chat_targets: list[dict]) -> None:
    """Sync function — runs in a worker thread. Sends one chat at a time
    with a small delay, broadcasts each step to subscribers."""
    job = _bulk_jobs[batch_id]
    job["status"] = "running"
    _broadcast_progress(job, "started", {})

    from bulk_message_probe import send_one
    from sync_feishu_groups_to_base import FeishuClient

    app_id = os.environ["LARK_APP_ID"]
    app_secret = os.environ["LARK_APP_SECRET"]
    client = FeishuClient(app_id, app_secret)
    client.authenticate()

    for target in chat_targets:
        chat_id = target["chat_id"]
        chat_name = target.get("chat_name") or ""
        job["current_chat"] = chat_name or chat_id
        result = send_one(client, chat_id, job["text"])
        if result.get("ok"):
            job["sent"] += 1
        else:
            job["failed"] += 1
            job["errors"].append({"chat_id": chat_id, "chat_name": chat_name, "error": result.get("error")})
        _broadcast_progress(
            job,
            "step",
            {"last_result": {"chat_id": chat_id, "chat_name": chat_name, "ok": result.get("ok"), "message_id": result.get("message_id"), "error": result.get("error")}},
        )
        time.sleep(0.05)

    job["status"] = "done"
    job["ended_at"] = int(time.time())
    job["current_chat"] = None
    _broadcast_progress(job, "done", {})
    log.info("[bulk] batch=%s done sent=%d failed=%d", batch_id, job["sent"], job["failed"])


@app.post("/api/bulk-send")
async def bulk_send(payload: dict) -> dict:
    """Start a bulk send. Body:
        { "chat_targets": [{"chat_id":"oc_..","chat_name":"..."}, ...],
          "text": "...",
          "title": "..." }
    Returns batch_id; poll /api/bulk-send/{batch_id} or subscribe to
    /ws/bulk-progress/{batch_id}.
    """
    chat_targets = payload.get("chat_targets") or []
    text = (payload.get("text") or "").strip()
    title = (payload.get("title") or text[:30]).strip()
    if not chat_targets:
        raise HTTPException(400, "chat_targets is empty")
    if not text:
        raise HTTPException(400, "text is empty")
    # normalize entries
    norm = []
    for t in chat_targets:
        cid = (t.get("chat_id") or "").strip()
        if not cid:
            continue
        norm.append({"chat_id": cid, "chat_name": (t.get("chat_name") or "").strip()})
    if not norm:
        raise HTTPException(400, "no valid chat_id in chat_targets")

    batch_id = _new_bulk_job(norm, text, title)
    asyncio.create_task(asyncio.to_thread(_run_bulk_send_thread, batch_id, norm))
    log.info("[bulk] batch=%s queued (n=%d)", batch_id, len(norm))
    return {"ok": True, "batch_id": batch_id, "queued": len(norm)}


@app.get("/api/bulk-send/{batch_id}")
async def get_bulk_status(batch_id: str) -> dict:
    job = _bulk_jobs.get(batch_id)
    if not job:
        raise HTTPException(404, "batch_id not found (may have expired since restart)")
    return _public_job_view(job)


@app.websocket("/ws/bulk-progress/{batch_id}")
async def bulk_progress_ws(ws: WebSocket, batch_id: str) -> None:
    await ws.accept()
    job = _bulk_jobs.get(batch_id)
    if not job:
        await ws.send_json({"type": "error", "message": "batch_id not found"})
        await ws.close()
        return

    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    with job["_lock"]:
        job["subscribers"].append(queue)

    # Replay current state immediately so a late-joining client doesn't see a blank UI
    await ws.send_json({
        "type": "snapshot",
        "batch_id": batch_id,
        "total": job["total"],
        "sent": job["sent"],
        "failed": job["failed"],
        "status": job["status"],
        "current_chat": job["current_chat"],
    })

    try:
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=30)
            except asyncio.TimeoutError:
                await ws.send_json({"type": "ping"})
                continue
            await ws.send_json(msg)
            if msg.get("type") == "done":
                break
    except WebSocketDisconnect:
        pass
    finally:
        with job["_lock"]:
            try:
                job["subscribers"].remove(queue)
            except ValueError:
                pass


# ─── 静态前端 ─────────────────────────────────────────────────────────────────
# 必须放在所有 /api /admin /ws 路由之后，否则 mount("/") 会拦截显式路由。
if os.path.isdir("web"):
    app.mount("/", StaticFiles(directory="web", html=True), name="static")
