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
  daily-sync         -> sync_feishu_groups_to_base.main() at 16:00 UTC (= SGT 00:00)
  external-join      -> ensure_bot_in_external_chats.main() at 14:00 UTC (= SGT 22:00)
  bulk-stats-refresh -> bulk_message_probe.cmd_refresh() at 12:00 UTC (= SGT 20:00)
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
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
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

# Lark event subscription — recent events ring buffer for debugging.
_lark_event_log: deque = deque(maxlen=200)
_lark_event_counts: dict[str, int] = {}
_lark_persist_counts: dict[str, int] = {}  # 已持久化到 Base 的事件计数

# Background pool for event processing — webhook returns 200 immediately,
# actual Base writes happen here.
_event_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lark-event")

# Cached Lark client + Base table ids (lazy-init on first event).
_lark_state: dict[str, Any] = {"ready": False}
_lark_state_lock = threading.Lock()

# chat_id → record_id cache (refreshed every 10 min)
_chat_record_cache: dict[str, str] = {}
_chat_record_cache_built_at = 0.0
_chat_record_cache_lock = threading.Lock()
CHAT_RECORD_CACHE_TTL_SEC = 600


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

    # Singapore 00:00 = UTC 16:00（前一天）。在新加坡数据中心是真·跨日界点，
    # 业务侧也希望"昨天的全量同步今天上班前已就绪"。
    sch.add_job(
        _job_daily_sync,
        CronTrigger(hour=16, minute=0, timezone="UTC"),
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
    # Warm the dashboard cache asynchronously so the first browser visit
    # doesn't trigger a 503 / blank dashboard.
    if all(_check_required_env().values()):
        log.info("[dashboard] kicking off initial cache warm-up...")
        _ensure_dashboard_rebuild_running()
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


def _rebuild_cache_sync() -> None:
    """Runs in a worker thread. Builds payload + writes cache. Catches errors so
    the building flag is always cleared."""
    started = time.time()
    try:
        payload = _build_dashboard_payload()
        _dashboard_cache["data"] = payload
        _dashboard_cache["expires_at"] = time.time() + DASHBOARD_TTL_SEC
        _dashboard_cache["last_build_seconds"] = time.time() - started
        _dashboard_cache["last_error"] = None
        log.info(
            "[dashboard] cache rebuilt in %.1fs (chats=%d broadcasts=%d)",
            _dashboard_cache["last_build_seconds"],
            payload.get("_meta", {}).get("chat_count", 0),
            payload.get("_meta", {}).get("broadcast_count", 0),
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("[dashboard] rebuild failed after %.1fs", time.time() - started)
        _dashboard_cache["last_error"] = str(exc)
    finally:
        _dashboard_cache["building"] = False


def _ensure_dashboard_rebuild_running() -> bool:
    """Kick off a rebuild if not already in flight. Returns True if started."""
    if _dashboard_cache.get("building"):
        return False
    _dashboard_cache["building"] = True
    asyncio.create_task(asyncio.to_thread(_rebuild_cache_sync))
    return True


@app.get("/api/dashboard/data")
async def get_dashboard_data(force_refresh: bool = False) -> dict:
    """Returns AppData JSON.

    Strategy: serve cache immediately if any data exists (even stale). If
    cache is stale or missing, kick off background rebuild — never block the
    request thread, since payload build can take several minutes for 16k chats.
    """
    now = time.time()
    cached = _dashboard_cache.get("data")
    expires = _dashboard_cache.get("expires_at") or 0
    is_stale = expires <= now

    if force_refresh or is_stale:
        _ensure_dashboard_rebuild_running()

    if cached:
        # Always return cached data (even if stale) so dashboard can render.
        out = dict(cached)
        out.setdefault("_meta", {})["served_at"] = int(now)
        out["_meta"]["is_stale"] = is_stale
        out["_meta"]["building"] = bool(_dashboard_cache.get("building"))
        return out

    # Cold start, no cache yet.
    if _dashboard_cache.get("building"):
        raise HTTPException(503, "dashboard cache is being built — retry in ~30s")
    raise HTTPException(503, "dashboard cache not warmed yet — retry in ~30s")


@app.get("/src/data.jsx")
async def serve_dashboard_data_jsx() -> Response:
    """Drop-in replacement for the static web/src/data.jsx."""
    import export_to_web as ex
    cached = _dashboard_cache.get("data")
    if not cached:
        # First page load before cache warmed: kick off build, return a minimal
        # AppData with empty arrays so the dashboard at least mounts. The user
        # will see "building" state and can refresh.
        _ensure_dashboard_rebuild_running()
        payload = {
            "TEAM": [],
            "GROUPS": [],
            "SENTIMENTS": [],
            "TAGS": [],
            "DASHBOARD": {
                "totalGroups": 0,
                "activeGroups": 0,
                "todayMsgs": 0,
                "avgResponseMin": 0,
                "pendingClient": 0,
                "stalled": [],
                "sentimentBreakdown": [],
                "speakerDist": [],
                "hourlyMsgs": [0] * 24,
            },
            "BROADCASTS": [],
            "_meta": {"building": True, "chat_count": 0, "broadcast_count": 0},
        }
    else:
        payload = cached

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


@app.post("/admin/rebuild-dashboard-cache")
async def admin_rebuild_dashboard() -> dict:
    """Force a cache rebuild. Returns immediately; background task does the work."""
    started = _ensure_dashboard_rebuild_running()
    return {
        "ok": True,
        "queued": started,
        "already_building": not started,
        "current_meta": (_dashboard_cache.get("data") or {}).get("_meta", {}),
    }


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


# ─── /lark/events Webhook ─────────────────────────────────────────────────────
# Lark 事件订阅回调入口。Webhook 必须在 3s 内返回 200，所以实际写 Base 的工作
# 抛到 _event_pool 后台线程执行。

def _ensure_lark_state() -> dict[str, Any]:
    """Lazy-init shared FeishuClient + Base table id cache. Thread-safe."""
    if _lark_state.get("ready"):
        return _lark_state
    with _lark_state_lock:
        if _lark_state.get("ready"):
            return _lark_state
        from sync_feishu_groups_to_base import (
            CHAT_TABLE_NAME,
            FeishuClient,
            MEMBER_TABLE_NAME,
            MESSAGE_TABLE_NAME,
            parse_base_token,
        )
        client = FeishuClient(os.environ["LARK_APP_ID"], os.environ["LARK_APP_SECRET"])
        client.authenticate()
        base_token = parse_base_token(os.environ["LARK_BASE_URL"])
        table_ids: dict[str, str] = {}
        for t in client.list_tables(base_token):
            name = t.get("name") or t.get("table_name")
            if name in (CHAT_TABLE_NAME, MEMBER_TABLE_NAME, MESSAGE_TABLE_NAME):
                tid = t.get("table_id") or t.get("id")
                if tid:
                    table_ids[str(name)] = str(tid)
        _lark_state.update({
            "ready": True,
            "client": client,
            "base_token": base_token,
            "table_ids": table_ids,
        })
        log.info("[event] lark state initialized; table_ids=%s", table_ids)
    return _lark_state


def _refresh_chat_record_cache_if_stale() -> None:
    """Rebuild chat_id → record_id map from chat 表 (rate-limited)."""
    global _chat_record_cache_built_at
    if time.time() - _chat_record_cache_built_at < CHAT_RECORD_CACHE_TTL_SEC:
        return
    with _chat_record_cache_lock:
        if time.time() - _chat_record_cache_built_at < CHAT_RECORD_CACHE_TTL_SEC:
            return
        try:
            from sync_feishu_groups_to_base import CHAT_TABLE_NAME
            state = _ensure_lark_state()
            client = state["client"]
            chat_table_id = state["table_ids"].get(CHAT_TABLE_NAME)
            if not chat_table_id:
                return
            new_map = client.list_existing_record_ids_v1(
                state["base_token"], chat_table_id, "群ID"
            )
            _chat_record_cache.clear()
            _chat_record_cache.update(new_map)
            _chat_record_cache_built_at = time.time()
            log.info("[event] chat-record cache rebuilt: %d entries", len(_chat_record_cache))
        except Exception:
            log.exception("[event] chat-record cache refresh failed")


def _bump_persist_count(label: str) -> None:
    _lark_persist_counts[label] = _lark_persist_counts.get(label, 0) + 1


def _process_message_event(body: dict) -> None:
    """im.message.receive_v1 → append 1 row to 「机器人群消息记录」."""
    try:
        from sync_feishu_groups_to_base import (
            BASE_MESSAGE_FIELD_DEFS,
            CHAT_TABLE_NAME,
            MESSAGE_TABLE_NAME,
            build_message_row,
            materialize_field_defs,
        )
        event = body.get("event") or {}
        msg = event.get("message") or {}
        sender = event.get("sender") or {}
        chat_id = msg.get("chat_id") or ""
        message_id = msg.get("message_id") or ""
        if not chat_id or not message_id:
            log.warning("[event/message] missing chat_id/message_id; skip")
            return

        state = _ensure_lark_state()
        msg_table_id = state["table_ids"].get(MESSAGE_TABLE_NAME)
        chat_table_id = state["table_ids"].get(CHAT_TABLE_NAME)
        if not msg_table_id or not chat_table_id:
            log.warning("[event/message] tables not cached; skip")
            return

        _refresh_chat_record_cache_if_stale()
        chat_record_id = _chat_record_cache.get(chat_id) or ""

        # Convert webhook event → iter_messages-shaped dict (build_message_row's input)
        synthetic_msg = {
            "message_id": message_id,
            "msg_type": msg.get("message_type"),
            "create_time": msg.get("create_time"),
            "update_time": msg.get("update_time"),
            "chat_id": chat_id,
            "body": {"content": msg.get("content")},
            "sender": {
                "id": (sender.get("sender_id") or {}).get("open_id"),
                "id_type": "open_id",
                "sender_type": sender.get("sender_type"),
                "tenant_key": sender.get("tenant_key"),
            },
            "deleted": False,
            "updated": False,
            "thread_id": msg.get("thread_id"),
            "root_id": msg.get("root_id"),
            "parent_id": msg.get("parent_id"),
        }

        sync_run_id = "event-" + datetime.now(SYNC_TZ).strftime("%Y%m%d%H%M%S")
        row = build_message_row(synthetic_msg, "", chat_record_id, sync_run_id, SYNC_TZ)
        message_field_defs = materialize_field_defs(BASE_MESSAGE_FIELD_DEFS, chat_table_id)
        message_fields = [f["name"] for f in message_field_defs]
        state["client"].batch_create_records(
            state["base_token"], msg_table_id, message_fields, [row]
        )
        _bump_persist_count("message")
        log.info("[event/message] persisted msg=%s chat=%s", message_id, chat_id)
    except Exception:
        log.exception("[event/message] failed")


def _process_member_added_event(body: dict) -> None:
    """im.chat.member.user.added_v1 → append member rows to 「机器人群成员记录」."""
    try:
        from sync_feishu_groups_to_base import (
            BASE_MEMBER_FIELD_DEFS,
            CHAT_TABLE_NAME,
            MEMBER_TABLE_NAME,
            build_member_rows,
            materialize_field_defs,
        )
        event = body.get("event") or {}
        chat_id = event.get("chat_id") or ""
        chat_name = event.get("name") or ""
        users = event.get("users") or []
        if not chat_id or not users:
            log.warning("[event/member-add] missing chat_id/users; skip")
            return

        state = _ensure_lark_state()
        member_table_id = state["table_ids"].get(MEMBER_TABLE_NAME)
        chat_table_id = state["table_ids"].get(CHAT_TABLE_NAME)
        if not member_table_id or not chat_table_id:
            log.warning("[event/member-add] tables not cached; skip")
            return

        _refresh_chat_record_cache_if_stale()
        chat_record_id = _chat_record_cache.get(chat_id) or ""

        # Convert event users → list_chat_members-shaped dicts
        members = []
        for u in users:
            uid = u.get("user_id") or {}
            members.append({
                "member_id": uid.get("open_id") or "",
                "member_id_type": "open_id",
                "name": u.get("name") or "",
                "tenant_key": u.get("tenant_key") or "",
            })

        sync_run_id = "event-" + datetime.now(SYNC_TZ).strftime("%Y%m%d%H%M%S")
        rows = build_member_rows(chat_id, chat_name, chat_record_id, members, sync_run_id)
        if not rows:
            return
        member_field_defs = materialize_field_defs(BASE_MEMBER_FIELD_DEFS, chat_table_id)
        member_fields = [f["name"] for f in member_field_defs]
        state["client"].batch_create_records(
            state["base_token"], member_table_id, member_fields, rows
        )
        _bump_persist_count("member-add")
        log.info("[event/member-add] +%d users in chat %s", len(rows), chat_id)
    except Exception:
        log.exception("[event/member-add] failed")


def _extract_cell_text(cell: object) -> str:
    if not cell:
        return ""
    if isinstance(cell, str):
        return cell
    if isinstance(cell, list) and cell:
        seg = cell[0]
        if isinstance(seg, dict):
            return str(seg.get("text") or seg.get("value") or "")
        return str(seg)
    if isinstance(cell, dict):
        return str(cell.get("text") or cell.get("value") or "")
    return str(cell)


def _process_member_deleted_event(body: dict) -> None:
    """im.chat.member.user.deleted_v1 → delete matching member rows."""
    try:
        from sync_feishu_groups_to_base import MEMBER_TABLE_NAME
        event = body.get("event") or {}
        chat_id = event.get("chat_id") or ""
        users = event.get("users") or []
        if not chat_id or not users:
            return
        target_open_ids = {
            (u.get("user_id") or {}).get("open_id") for u in users
        }
        target_open_ids.discard(None)
        target_open_ids.discard("")
        if not target_open_ids:
            return

        state = _ensure_lark_state()
        member_table_id = state["table_ids"].get(MEMBER_TABLE_NAME)
        if not member_table_id:
            return
        client = state["client"]
        base_token = state["base_token"]

        # search by 群ID, then filter member open_id client-side
        search_url = f"/open-apis/bitable/v1/apps/{base_token}/tables/{member_table_id}/records/search"
        record_ids: list[str] = []
        page_token = None
        while True:
            payload = {
                "field_names": ["成员ID"],
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "群ID", "operator": "is", "value": [chat_id]}],
                },
                "page_size": 200,
            }
            params = {"page_size": 200}
            if page_token:
                params["page_token"] = page_token
            data = client.request("POST", search_url, params=params, data=payload)
            for it in (data.get("items") or []):
                fields = it.get("fields") or {}
                mid = _extract_cell_text(fields.get("成员ID"))
                if mid in target_open_ids:
                    record_ids.append(str(it.get("record_id") or ""))
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
            if not page_token:
                break

        record_ids = [rid for rid in record_ids if rid]
        if record_ids:
            client.batch_delete_records_v1(base_token, member_table_id, record_ids)
            log.info("[event/member-del] -%d rows in chat %s", len(record_ids), chat_id)
            _bump_persist_count("member-del")
        else:
            log.info("[event/member-del] no matching rows for chat %s users=%s", chat_id, list(target_open_ids))
    except Exception:
        log.exception("[event/member-del] failed")


def _process_message_recalled_event(body: dict) -> None:
    """im.message.recalled_v1 → flip 是否已删除 to 'true' on the matching row."""
    try:
        from sync_feishu_groups_to_base import MESSAGE_TABLE_NAME
        event = body.get("event") or {}
        msg_id = event.get("message_id") or ""
        if not msg_id:
            return
        state = _ensure_lark_state()
        msg_table_id = state["table_ids"].get(MESSAGE_TABLE_NAME)
        if not msg_table_id:
            return
        client = state["client"]
        base_token = state["base_token"]

        search_url = f"/open-apis/bitable/v1/apps/{base_token}/tables/{msg_table_id}/records/search"
        payload = {
            "field_names": ["消息ID"],
            "filter": {
                "conjunction": "and",
                "conditions": [{"field_name": "消息ID", "operator": "is", "value": [msg_id]}],
            },
            "page_size": 5,
        }
        data = client.request("POST", search_url, data=payload)
        items = data.get("items") or []
        if not items:
            log.info("[event/recall] msg %s not in Base, skip", msg_id)
            return

        record_ids = [str(it.get("record_id") or "") for it in items if it.get("record_id")]
        if not record_ids:
            return
        rows = [["true"] for _ in record_ids]
        client.batch_update_records_v1(base_token, msg_table_id, ["是否已删除"], record_ids, rows)
        log.info("[event/recall] marked msg %s deleted (%d rows)", msg_id, len(record_ids))
        _bump_persist_count("message-recall")
    except Exception:
        log.exception("[event/recall] failed")


def _process_chat_disbanded_event(body: dict) -> None:
    """im.chat.disbanded_v1 → set 群状态 = 'dissolved' on chat row."""
    try:
        from sync_feishu_groups_to_base import CHAT_TABLE_NAME
        event = body.get("event") or {}
        chat_id = event.get("chat_id") or ""
        if not chat_id:
            return
        state = _ensure_lark_state()
        chat_table_id = state["table_ids"].get(CHAT_TABLE_NAME)
        if not chat_table_id:
            return
        _refresh_chat_record_cache_if_stale()
        record_id = _chat_record_cache.get(chat_id)
        if not record_id:
            log.info("[event/disbanded] chat %s not in Base, skip", chat_id)
            return
        client = state["client"]
        client.batch_update_records_v1(
            state["base_token"], chat_table_id, ["群状态"], [record_id], [["dissolved"]]
        )
        # Drop from cache so future lookups don't return a dissolved record_id silently
        _chat_record_cache.pop(chat_id, None)
        log.info("[event/disbanded] marked chat %s dissolved", chat_id)
        _bump_persist_count("chat-disbanded")
    except Exception:
        log.exception("[event/disbanded] failed")


def _process_bot_added_event(body: dict) -> None:
    """im.chat.member.bot.added_v1 → upsert chat row + write member rows + 24h message backfill."""
    try:
        from sync_feishu_groups_to_base import (
            BASE_MEMBER_FIELD_DEFS,
            BASE_MESSAGE_FIELD_DEFS,
            CHAT_FIELDS,
            CHAT_TABLE_NAME,
            MEMBER_TABLE_NAME,
            MESSAGE_TABLE_NAME,
            build_chat_row,
            build_member_rows,
            build_message_row,
            materialize_field_defs,
        )
        event = body.get("event") or {}
        chat_id = event.get("chat_id") or ""
        chat_name = event.get("name") or ""
        external = event.get("external")
        if not chat_id:
            return

        state = _ensure_lark_state()
        client = state["client"]
        base_token = state["base_token"]
        chat_table_id = state["table_ids"].get(CHAT_TABLE_NAME)
        if not chat_table_id:
            return

        # 1) chat detail (best-effort; bot may not yet have full perms)
        try:
            detail = client.get_chat_detail(chat_id)
        except Exception:
            detail = {"chat_id": chat_id, "name": chat_name, "external": external}

        # 2) members (best-effort)
        try:
            members_payload = client.list_chat_members(chat_id)
            members = list(members_payload.get("items") or [])
            member_total = int(members_payload.get("member_total") or len(members))
        except Exception:
            members = []
            member_total = 0

        # 3) upsert chat row
        sync_run_id = "event-bot-added-" + datetime.now(SYNC_TZ).strftime("%Y%m%d%H%M%S")
        sync_time_text = datetime.now(SYNC_TZ).strftime("%Y-%m-%d %H:%M:%S")
        chat_row = build_chat_row(
            {"chat_id": chat_id, "name": chat_name, "external": external},
            detail, {}, members, member_total,
            sync_run_id, sync_time_text, SYNC_TZ,
        )
        existing_map = client.list_existing_record_ids_v1(base_token, chat_table_id, "群ID")
        existing_rid = existing_map.get(chat_id)
        if existing_rid:
            client.batch_update_records_v1(base_token, chat_table_id, CHAT_FIELDS, [existing_rid], [chat_row])
            chat_record_id = existing_rid
        else:
            result = client.batch_create_records(base_token, chat_table_id, CHAT_FIELDS, [chat_row])
            new_ids = result.get("record_id_list") or []
            chat_record_id = str(new_ids[0]) if new_ids else ""
        if chat_record_id:
            _chat_record_cache[chat_id] = chat_record_id

        # 4) member rows
        if members and chat_record_id:
            member_table_id = state["table_ids"].get(MEMBER_TABLE_NAME)
            if member_table_id:
                mfd = materialize_field_defs(BASE_MEMBER_FIELD_DEFS, chat_table_id)
                m_fields = [f["name"] for f in mfd]
                m_rows = build_member_rows(chat_id, chat_name, chat_record_id, members, sync_run_id)
                if m_rows:
                    client.batch_create_records(base_token, member_table_id, m_fields, m_rows)

        # 5) recent 24h messages backfill (best-effort; capped to 200 to keep handler bounded)
        msg_count = 0
        msg_table_id = state["table_ids"].get(MESSAGE_TABLE_NAME)
        if msg_table_id and chat_record_id:
            now_ts = int(time.time())
            start_ts = now_ts - 86400
            try:
                messages = list(client.iter_messages(
                    chat_id, page_size=50, start_time=start_ts, end_time=now_ts, max_messages=200,
                ))
            except Exception:
                messages = []
            if messages:
                msfd = materialize_field_defs(BASE_MESSAGE_FIELD_DEFS, chat_table_id)
                msg_fields = [f["name"] for f in msfd]
                rows = [build_message_row(m, chat_name, chat_record_id, sync_run_id, SYNC_TZ) for m in messages]
                client.batch_create_records(base_token, msg_table_id, msg_fields, rows)
                msg_count = len(rows)

        log.info(
            "[event/bot-added] chat=%s name=%s members=%d msgs=%d",
            chat_id, chat_name, len(members), msg_count,
        )
        _bump_persist_count("bot-added")
    except Exception:
        log.exception("[event/bot-added] failed")


# Map event_type → handler. Events not in this map are logged but not persisted.
EVENT_HANDLERS = {
    "im.message.receive_v1":          _process_message_event,
    "im.message.recalled_v1":         _process_message_recalled_event,
    "im.chat.member.user.added_v1":   _process_member_added_event,
    "im.chat.member.user.deleted_v1": _process_member_deleted_event,
    "im.chat.member.bot.added_v1":    _process_bot_added_event,
    "im.chat.disbanded_v1":           _process_chat_disbanded_event,
}


@app.post("/lark/events")
async def lark_events(req: Request) -> dict:
    try:
        body = await req.json()
    except Exception:
        raw = await req.body()
        log.warning("[lark/events] non-JSON body: %r", raw[:200])
        return {"code": 0}

    # 1) URL 验证握手
    if body.get("type") == "url_verification":
        challenge = body.get("challenge")
        log.info("[lark/events] url_verification challenge=%s", challenge)
        return {"challenge": challenge}

    # 2) schema 2.0 事件
    header = body.get("header") or {}
    event_type = header.get("event_type") or "unknown"
    event_id = header.get("event_id") or ""

    _lark_event_counts[event_type] = _lark_event_counts.get(event_type, 0) + 1
    _lark_event_log.append({
        "ts": int(time.time()),
        "event_id": event_id,
        "event_type": event_type,
        "preview": json.dumps(body.get("event") or {}, ensure_ascii=False)[:300],
    })
    log.info("[lark/events] type=%s id=%s", event_type, event_id)

    # 3) 派发到后台线程持久化（webhook 必须 3s 内返回）
    handler = EVENT_HANDLERS.get(event_type)
    if handler:
        _event_pool.submit(handler, body)

    return {"code": 0}


@app.get("/lark/events/recent")
async def lark_events_recent(limit: int = 50) -> dict:
    return {
        "counts": dict(_lark_event_counts),
        "persisted": dict(_lark_persist_counts),
        "chat_record_cache_size": len(_chat_record_cache),
        "recent": list(_lark_event_log)[-limit:],
    }


# ─── 静态前端 ─────────────────────────────────────────────────────────────────
# 必须放在所有 /api /admin /ws 路由之后，否则 mount("/") 会拦截显式路由。
if os.path.isdir("web"):
    app.mount("/", StaticFiles(directory="web", html=True), name="static")
