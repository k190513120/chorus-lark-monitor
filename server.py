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
from apscheduler.triggers.interval import IntervalTrigger
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response

import local_db
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
DASHBOARD_TTL_SEC = 600  # 10 min. rebuild 实际要 5-9 min，TTL 太短会持续触发重叠重建

# In-memory bulk-send job tracker. Lost on container restart, fine for short-lived jobs.
_bulk_jobs: dict[str, dict[str, Any]] = {}
_bulk_jobs_lock = threading.Lock()

# Lark event subscription — recent events ring buffer for debugging.
_lark_event_log: deque = deque(maxlen=200)
_lark_event_counts: dict[str, int] = {}
_lark_persist_counts: dict[str, int] = {}  # 已持久化到 Base 的事件计数
_lark_dedup_count = 0


class _LRUSet:
    """Thread-safe set of last N keys; returns False on re-seen keys."""
    def __init__(self, maxsize: int = 10000) -> None:
        self.maxsize = maxsize
        self.cache: OrderedDict = OrderedDict()
        self.lock = threading.Lock()

    def add_if_new(self, key: str) -> bool:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return False
            self.cache[key] = None
            if len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)
            return True


# Lark may retry events; dedup by event_id (set in header.event_id).
_seen_event_ids = _LRUSet(maxsize=10000)

# Background pool for event processing — webhook returns 200 immediately,
# actual Base writes happen here.
# 容量推算：Base 单表写限频 ~20/s，单条写延时 ~1s。worker=12 时理论吞吐
# 12 msg/s，仍低于飞书限频。SQLite WAL 单写者会自动串行化，不会冲突。
# 可通过 LARK_EVENT_POOL_SIZE 环境变量临时调整。
_event_pool = ThreadPoolExecutor(
    max_workers=int(os.getenv("LARK_EVENT_POOL_SIZE", "12")),
    thread_name_prefix="lark-event",
)

# Cached Lark client + Base table ids (lazy-init on first event).
_lark_state: dict[str, Any] = {"ready": False}
_lark_state_lock = threading.Lock()

# 第二目标 Base（dual-write 阶段）。env: LARK_BASE_URL_SECONDARY 设了才启用。
_lark_state_secondary: dict[str, Any] = {"ready": False, "enabled": False}
_lark_state_secondary_lock = threading.Lock()

# Sync worker 计数器（监控用）
_sync_stats: dict[str, int] = {
    "chats_synced": 0, "chats_failed": 0,
    "messages_synced": 0, "messages_failed": 0,
    "members_synced": 0, "members_failed": 0,
    "last_run_ts": 0,
}

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
    rc = _run_script(
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
            # Lite mode: 已有 chat 跳过 members + messages 拉取（webhook 事件接管）。
            # 只对新加入的 chat 做完整同步。daily-sync 时间从 ~8h 降到 ~分钟。
            "--lite-mode",
        ],
        "daily-sync",
    )
    # --refresh-metadata-tables 会重建 chat/member 表，table_id 和 record_id 全变。
    # 必须失效缓存，否则 webhook 事件会继续写到旧表 → 800030104 not_found。
    _invalidate_lark_state()
    # Tier 1: daily-sync 跑完后从 Base 重新 seed 本地 SQLite，
    # 把过去 24h 内 webhook 漏掉/被限频丢掉的事件捞回来对齐。
    try:
        state = _ensure_lark_state()
        from sync_feishu_groups_to_base import load_timezone
        sync_tz = load_timezone(os.getenv("SYNC_TIMEZONE", "Asia/Shanghai"))
        result = local_db.seed_from_lark_base(
            state["client"], state["base_token"], state["table_ids"], sync_tz,
        )
        log.info("[local-db] post-daily-sync reseed: %s", result)
    except Exception:  # noqa: BLE001
        log.exception("[local-db] post-daily-sync reseed failed (non-fatal)")
    return rc


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


# CF Edge cache 预热目标。打公网 URL 让 CF Edge 始终持有热副本，避免首次访问踩
# tunnel 冷路径（~12s/MB）。Worker 端 EDGE_CACHE_TTL=300s，所以间隔 240s。
CF_PREWARM_URL = os.getenv("CF_PREWARM_URL", "https://chorus.xiaomiao.win")
CF_PREWARM_PATHS = [
    "/",
    "/src/data.jsx",
    "/src/v2-styles.css",
    "/src/app.jsx",
    "/src/broadcast.jsx",
    "/src/icons.jsx",
    "/src/ui.jsx",
    "/src/v2-components.jsx",
    "/src/v2-data-adapter.js",
    "/src/v2-tweaks-panel.jsx",
    "/src/v2-views.jsx",
]


def _job_cf_prewarm() -> None:
    if not CF_PREWARM_URL:
        return
    try:
        import httpx
    except Exception:  # noqa: BLE001
        log.warning("[prewarm] httpx not available, skipping")
        return
    headers = {"Accept-Encoding": "gzip", "User-Agent": "chorus-prewarm/1.0"}
    with httpx.Client(timeout=30.0, headers=headers, follow_redirects=False) as cli:
        for p in CF_PREWARM_PATHS:
            url = f"{CF_PREWARM_URL.rstrip('/')}{p}"
            try:
                r = cli.get(url)
                log.info(
                    "[prewarm] %s -> %d %dB x-cache=%s",
                    p, r.status_code, len(r.content), r.headers.get("x-cache", "-"),
                )
            except Exception as e:  # noqa: BLE001
                log.warning("[prewarm] %s failed: %s", url, e)


JOB_FUNCS = {
    "daily-sync": _job_daily_sync,
    "external-join": _job_external_join,
    "bulk-stats-refresh": _job_bulk_refresh,
    "cf-prewarm": _job_cf_prewarm,
    "secondary-sync": lambda: _job_secondary_sync(),  # lazy ref，避免引用前定义问题
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
    # CF Edge cache 预热：每 4 分钟跑一次（Worker EDGE_CACHE_TTL=300s），
    # 让公网首访也走 cache，不再吃 tunnel 12s 冷路径。
    sch.add_job(
        _job_cf_prewarm,
        IntervalTrigger(seconds=240),
        id="cf-prewarm",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
        next_run_time=datetime.now(),  # 启动后立即跑一次
    )
    # Secondary Base 同步 worker：每 15s 一批，回填 + 持续 dual-sync。
    # LARK_BASE_URL_SECONDARY 未设则 _job_secondary_sync 自动 noop。
    sch.add_job(
        _job_secondary_sync,
        IntervalTrigger(seconds=15),
        id="secondary-sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
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
    if all(_check_required_env().values()):
        # Warm the dashboard cache asynchronously so the first browser visit
        # doesn't trigger a 503 / blank dashboard.
        log.info("[dashboard] kicking off initial cache warm-up...")
        _ensure_dashboard_rebuild_running()
        # Pre-warm the chat_record_cache so the first webhook event doesn't
        # block 30-60s waiting for 16k records to load from Lark Base.
        log.info("[event] kicking off chat-record cache warm-up...")
        _event_pool.submit(_refresh_chat_record_cache_if_stale)
    try:
        yield
    finally:
        if SCHEDULER:
            SCHEDULER.shutdown(wait=False)
            log.info("scheduler shut down")


app = FastAPI(title="Chorus Lark Monitor", version=VERSION, lifespan=lifespan)

# gzip 压缩 /src/data.jsx 和 /api/dashboard/data 这种大 JSON 响应
# 9.7 MB → 1 MB (9x)。压缩 minimum_size 设 1024B 避免短响应也压
app.add_middleware(GZipMiddleware, minimum_size=1024)

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

    # ─── Tier 1：本地 SQLite 副本读路径 ───
    # 如果本地 DB 已有数据（seed 过 + webhook 双写），用 SQL 读 sub-second 完成。
    # SQLite 空时 fallback 到 Lark Base 全量拉（首次启动 / 灾后恢复）。
    use_local_db = _local_db_has_data()
    if use_local_db:
        cap = max_groups if max_groups > 0 else 5000  # 即使 0=不限，本地也限个上限避免极端
        top_ids = local_db.top_n_chat_ids_by_activity(cap)
        chats = local_db.load_chats(chat_ids=top_ids)
        members_by_chat = local_db.load_members(chat_ids=top_ids)
        per_chat = max_messages_per_group if max_messages_per_group > 0 else 50
        messages_by_chat = local_db.load_messages(chat_ids=top_ids, max_per_chat=per_chat)
        log.info("[dashboard] loaded from local SQLite: %d chats, %d msg-bundles",
                 len(chats), len(messages_by_chat))
    else:
        chats = export_to_web.load_chats(client, base_token, chat_table_id)
        members_by_chat = export_to_web.load_members(client, base_token, member_table_id)
        messages_by_chat = export_to_web.load_messages(client, base_token, message_table_id, sync_tz)
        log.info("[dashboard] loaded from Lark Base (local SQLite empty)")

    broadcasts = export_to_web.load_broadcasts(client, base_token, sync_tz)

    selected = export_to_web.pick_active_groups(chats, members_by_chat, messages_by_chat, max_groups)
    payload = export_to_web.build_app_data(
        selected,
        members_by_chat,
        messages_by_chat,
        sync_tz,
        max_messages_per_group,
        broadcasts=broadcasts,
        client=client,  # let build_app_data live-fetch DR names from per-chat members
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
    is_empty_warmup = not cached
    if is_empty_warmup:
        # First page load before cache warmed: kick off build, return a minimal
        # AppData with empty arrays so the dashboard at least mounts. The user
        # will see "building" state and can refresh.
        # CRITICAL: must NOT cache this empty payload at CF Edge — otherwise the
        # empty version sticks around for cache TTL even after data is ready,
        # leading to the dashboard being permanently "0 群在册".
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
    if is_empty_warmup:
        cache_ctrl = "no-store, no-cache, must-revalidate"
    else:
        # Cache-Control: CF Edge 缓存 60s（business 看到的 staleness 最多 60s，可接受），
        # browser 1min。这样跨海上行只用一次，后续从 CF Edge 秒级响应。
        cache_ctrl = "public, max-age=60, s-maxage=60"
    return Response(
        content=js,
        media_type="application/javascript; charset=utf-8",
        headers={
            "Cache-Control": cache_ctrl,
            "Vary": "Accept-Encoding",
        },
    )


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


# ─── Tier 1 本地 SQLite admin ─────────────────────────────────────────────────

def _local_db_has_data() -> bool:
    """SQLite 是否被 seed 过 + 有 chat 数据。决定 dashboard 走 SQL 还是 fallback 拉 Base。"""
    try:
        return local_db.get_stats().get("chats", 0) > 0
    except Exception:  # noqa: BLE001
        return False


@app.get("/admin/local-db-stats")
async def admin_local_db_stats() -> dict:
    stats = local_db.get_stats()
    stats["last_seeded_at"] = local_db.get_meta("last_seeded_at")
    stats["last_seed_counts"] = local_db.get_meta("last_seed_counts")
    return stats


@app.get("/admin/sync-stats")
async def admin_sync_stats() -> dict:
    """Secondary Base 同步进度。"""
    queue = local_db.get_sync_queue_stats()
    state = _ensure_lark_state_secondary()
    return {
        "secondary_enabled": bool(state.get("enabled")),
        "secondary_table_ids": state.get("table_ids", {}),
        "queue": queue,
        "counters": dict(_sync_stats),
    }


_seed_lock = threading.Lock()
_seed_in_progress = {"running": False, "started_at": None, "result": None}


def _do_seed_local_db() -> dict:
    try:
        state = _ensure_lark_state()
        from sync_feishu_groups_to_base import load_timezone
        sync_tz = load_timezone(os.getenv("SYNC_TIMEZONE", "Asia/Shanghai"))
        result = local_db.seed_from_lark_base(
            state["client"], state["base_token"], state["table_ids"], sync_tz,
        )
        log.info("[local-db] seed done: %s", result)
        _seed_in_progress["result"] = result
        return result
    except Exception as exc:  # noqa: BLE001
        log.exception("[local-db] seed failed")
        err = {"error": str(exc)}
        _seed_in_progress["result"] = err
        return err
    finally:
        _seed_in_progress["running"] = False


@app.post("/admin/seed-local-db")
async def admin_seed_local_db() -> dict:
    """从 Lark Base 全量拉一次回填 SQLite。一次性，~5-9 min。"""
    with _seed_lock:
        if _seed_in_progress["running"]:
            return {"ok": True, "queued": False, "already_running": True,
                    "started_at": _seed_in_progress["started_at"]}
        _seed_in_progress["running"] = True
        _seed_in_progress["started_at"] = int(time.time())
        _seed_in_progress["result"] = None
    asyncio.create_task(asyncio.to_thread(_do_seed_local_db))
    return {"ok": True, "queued": True,
            "started_at": _seed_in_progress["started_at"],
            "note": "5-9 min for fresh seed. Poll /admin/local-db-stats for progress."}


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


# ─── /api/bulk-send/refresh — 手动触发群发任务统计刷新 ────────────────────────

_manual_refresh_state: dict[str, Any] = {
    "running": False,
    "started_at": None,
    "ended_at": None,
    "last_error": None,
}
_manual_refresh_lock = threading.Lock()


def _run_manual_refresh(max_age_days: int) -> None:
    with _manual_refresh_lock:
        _manual_refresh_state.update({
            "running": True,
            "started_at": int(time.time()),
            "ended_at": None,
            "last_error": None,
        })
    try:
        rc = _run_script(
            "bulk_message_probe",
            ["refresh", "--max-age-days", str(max_age_days)],
            "manual-bulk-refresh",
        )
        # 让下次 dashboard / 分析 拉取时获取新数据
        _dashboard_cache["data"] = None
        _dashboard_cache["expires_at"] = 0
        _broadcast_cache["data"] = None
        _broadcast_cache["expires_at"] = 0
        _manual_refresh_state.update({
            "running": False,
            "ended_at": int(time.time()),
            "last_rc": rc,
        })
    except Exception as exc:
        _manual_refresh_state.update({
            "running": False,
            "ended_at": int(time.time()),
            "last_error": str(exc)[:500],
        })


@app.post("/api/bulk-send/refresh")
async def manual_bulk_refresh(max_age_days: int = 7) -> dict:
    """手动触发 bulk-stats refresh。返回立即；前端可以轮询 GET /api/bulk-send/refresh/status 看进度。"""
    if _manual_refresh_state.get("running"):
        return {
            "ok": False,
            "running": True,
            "started_at": _manual_refresh_state.get("started_at"),
            "message": "已经在跑，请等当前 refresh 完成",
        }
    asyncio.create_task(asyncio.to_thread(_run_manual_refresh, max_age_days))
    return {"ok": True, "queued": True, "max_age_days": max_age_days}


@app.get("/api/bulk-send/refresh/status")
async def manual_bulk_refresh_status() -> dict:
    return dict(_manual_refresh_state)


# ─── /api/broadcast/analysis — 群发数据分析报告 ──────────────────────────────

def _load_broadcasts_only() -> list[dict]:
    """Load broadcasts directly from Base (skip the heavy dashboard payload build)."""
    import export_to_web
    from sync_feishu_groups_to_base import FeishuClient, load_timezone, parse_base_token
    sync_tz = load_timezone(os.getenv("SYNC_TIMEZONE", "Asia/Shanghai"))
    base_token = parse_base_token(os.environ["LARK_BASE_URL"])
    client = FeishuClient(os.environ["LARK_APP_ID"], os.environ["LARK_APP_SECRET"])
    client.authenticate()
    return export_to_web.load_broadcasts(client, base_token, sync_tz)


_broadcast_cache: dict[str, Any] = {"data": None, "expires_at": 0.0}
BROADCAST_CACHE_TTL_SEC = 60


async def _get_broadcasts_cached() -> list[dict]:
    now = time.time()
    if _broadcast_cache["data"] is not None and _broadcast_cache["expires_at"] > now:
        return _broadcast_cache["data"]
    data = await asyncio.to_thread(_load_broadcasts_only)
    _broadcast_cache["data"] = data
    _broadcast_cache["expires_at"] = now + BROADCAST_CACHE_TTL_SEC
    return data


@app.get("/api/broadcast/analysis")
async def broadcast_analysis() -> dict:
    """从 BROADCASTS 数据聚合出可读的分析报告。直接读 broadcasts 表，不依赖 dashboard。"""
    broadcasts: list[dict] = list(await _get_broadcasts_cached())

    # 全局 KPI
    total_batches = len(broadcasts)
    total_chats = sum(int(b.get("chatCount") or 0) for b in broadcasts)
    total_success = sum(int(b.get("successCount") or 0) for b in broadcasts)
    total_failure = sum(int(b.get("failureCount") or 0) for b in broadcasts)
    total_audience = sum(int(b.get("targetAudience") or 0) for b in broadcasts)
    total_read = sum(int(b.get("readCount") or 0) for b in broadcasts)
    total_reply = sum(int(b.get("replyCount") or 0) for b in broadcasts)
    total_reply_users = sum(int(b.get("replyUniqueSenders") or 0) for b in broadcasts)

    avg_read_rate = total_read / total_audience if total_audience else 0.0
    avg_reply_rate = total_reply_users / total_audience if total_audience else 0.0

    # 任务级排序（按已读率）
    sorted_by_read = sorted(
        broadcasts,
        key=lambda b: float(b.get("avgReadRate") or 0),
        reverse=True,
    )
    top_tasks = sorted_by_read[:5]
    bottom_tasks = sorted_by_read[-5:][::-1] if len(sorted_by_read) >= 5 else []

    # 群级聚合：同一群在多次群发里的整体表现
    chat_aggregates: dict[str, dict] = {}
    for b in broadcasts:
        for c in (b.get("chats") or []):
            cid = c.get("chatId")
            if not cid:
                continue
            entry = chat_aggregates.setdefault(cid, {
                "chatId": cid,
                "chatName": c.get("chatName") or "",
                "broadcastCount": 0,
                "totalReadRate": 0.0,
                "totalReplyRate": 0.0,
                "anyRead": False,
                "anyReply": False,
            })
            if c.get("chatName"):
                entry["chatName"] = c["chatName"]
            entry["broadcastCount"] += 1
            entry["totalReadRate"] += float(c.get("readRate") or 0)
            entry["totalReplyRate"] += float(c.get("replyRate") or 0)
            if (c.get("readRate") or 0) > 0:
                entry["anyRead"] = True
            if (c.get("replyRate") or 0) > 0:
                entry["anyReply"] = True
    for entry in chat_aggregates.values():
        n = max(entry["broadcastCount"], 1)
        entry["avgReadRate"] = round(entry["totalReadRate"] / n, 4)
        entry["avgReplyRate"] = round(entry["totalReplyRate"] / n, 4)
        del entry["totalReadRate"]
        del entry["totalReplyRate"]

    chat_list = list(chat_aggregates.values())
    # 沉默群：被群发 ≥2 次但没人读
    silent_chats = sorted(
        [c for c in chat_list if c["broadcastCount"] >= 2 and not c["anyRead"]],
        key=lambda c: -c["broadcastCount"],
    )[:20]
    # 高质量群：平均已读率 ≥ 50% 的群（受过多次群发）
    high_quality_chats = sorted(
        [c for c in chat_list if c["broadcastCount"] >= 2 and c["avgReadRate"] >= 0.5],
        key=lambda c: -c["avgReadRate"],
    )[:20]

    return {
        "generatedAt": int(time.time()),
        "kpis": {
            "totalBatches": total_batches,
            "totalChatRows": total_chats,
            "totalSuccess": total_success,
            "totalFailure": total_failure,
            "successRate": round(total_success / total_chats, 4) if total_chats else 0.0,
            "totalAudience": total_audience,
            "totalRead": total_read,
            "totalReply": total_reply,
            "totalReplyUsers": total_reply_users,
            "avgReadRate": round(avg_read_rate, 4),
            "avgReplyRate": round(avg_reply_rate, 4),
        },
        "topTasks": top_tasks,
        "bottomTasks": bottom_tasks,
        "silentChats": silent_chats,
        "highQualityChats": high_quality_chats,
    }


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


def _ensure_lark_state_secondary() -> dict[str, Any]:
    """新 Base 的 lark state，独立于 primary。env LARK_BASE_URL_SECONDARY 未设
    则 enabled=False，sync worker 跳过执行。"""
    if _lark_state_secondary.get("ready"):
        return _lark_state_secondary
    base_url = os.getenv("LARK_BASE_URL_SECONDARY")
    if not base_url:
        _lark_state_secondary.update({"ready": True, "enabled": False})
        return _lark_state_secondary
    with _lark_state_secondary_lock:
        if _lark_state_secondary.get("ready"):
            return _lark_state_secondary
        from sync_feishu_groups_to_base import (
            CHAT_TABLE_NAME,
            FeishuClient,
            MEMBER_TABLE_NAME,
            MESSAGE_TABLE_NAME,
            parse_base_token,
        )
        client = FeishuClient(os.environ["LARK_APP_ID"], os.environ["LARK_APP_SECRET"])
        client.authenticate()
        base_token = parse_base_token(base_url)
        table_ids: dict[str, str] = {}
        for t in client.list_tables(base_token):
            name = t.get("name") or t.get("table_name")
            if name in (CHAT_TABLE_NAME, MEMBER_TABLE_NAME, MESSAGE_TABLE_NAME):
                tid = t.get("table_id") or t.get("id")
                if tid:
                    table_ids[str(name)] = str(tid)
        _lark_state_secondary.update({
            "ready": True,
            "enabled": True,
            "client": client,
            "base_token": base_token,
            "table_ids": table_ids,
        })
        log.info("[sync/secondary] state initialized; table_ids=%s", table_ids)
    return _lark_state_secondary


def _invalidate_lark_state() -> None:
    """Forces table_ids + chat-record cache to refetch on next access.

    Call after daily-sync `--refresh-metadata-tables`: tables get recreated,
    table_ids and chat record_ids both change."""
    global _chat_record_cache_built_at
    with _lark_state_lock:
        _lark_state["ready"] = False
        _lark_state["table_ids"] = {}
    with _chat_record_cache_lock:
        _chat_record_cache.clear()
        _chat_record_cache_built_at = 0.0
    log.info("[event] lark_state + chat_record_cache invalidated")


# ─── Secondary Base 同步 worker ──────────────────────────────────────────
#
# 思路：SQLite 是真源。每个 chat/message/member 行有 secondary_synced 标志。
# 后台任务定期扫 pending 行，批量写 secondary Base，回填 record_id。
# 失败不会破坏主链路，下一轮会重试。

def _build_chat_row_minimal(chat_id: str, name: str, description: str,
                            member_total: int, owner_id: str) -> list:
    """构造最小可用 chat 行。字段顺序必须与 CHAT_FIELD_DEFS 一致；
    没数据的字段填空串。"""
    from sync_feishu_groups_to_base import CHAT_FIELD_DEFS
    row = []
    for f in CHAT_FIELD_DEFS:
        n = f["name"]
        if n == "群ID":
            row.append(chat_id or "")
        elif n == "群名称":
            row.append(name or "")
        elif n == "群描述":
            row.append(description or "")
        elif n == "用户数":
            row.append(str(member_total) if member_total else "")
        elif n == "成员总数":
            row.append(str(member_total) if member_total else "")
        elif n == "群主ID":
            row.append(owner_id or "")
        elif n == "同步批次":
            row.append("secondary-sync-backfill")
        elif n == "同步时间":
            row.append(datetime.now(SYNC_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        elif f.get("type") == "select":
            row.append(None)  # select 类型留空否则会报 invalid option
        elif f.get("type") == "user":
            row.append(None)
        else:
            row.append("")
    return row


def _sync_chats_to_secondary(batch_size: int = 200) -> dict[str, int]:
    """Pull pending chats from SQLite → write secondary Base → mark synced."""
    state = _ensure_lark_state_secondary()
    if not state.get("enabled"):
        return {"synced": 0, "failed": 0, "skipped": 0}

    from sync_feishu_groups_to_base import CHAT_FIELD_DEFS, CHAT_TABLE_NAME

    client = state["client"]
    base_token = state["base_token"]
    chat_table_id = state["table_ids"].get(CHAT_TABLE_NAME)
    if not chat_table_id:
        log.warning("[sync/secondary] %s table not found in new base", CHAT_TABLE_NAME)
        return {"synced": 0, "failed": 0, "skipped": 1}

    field_names = [f["name"] for f in CHAT_FIELD_DEFS]
    pending = local_db.get_pending_chats_for_secondary(limit=batch_size)
    if not pending:
        return {"synced": 0, "failed": 0, "skipped": 0}

    rows = [_build_chat_row_minimal(
        p["chat_id"], p["name"], p["description"],
        p["member_total"], p["owner_id"],
    ) for p in pending]

    try:
        resp = client.batch_create_records(base_token, chat_table_id, field_names, rows)
        new_ids = resp.get("record_id_list") or []
        if len(new_ids) != len(pending):
            log.warning("[sync/secondary] chat batch len mismatch: req=%d got=%d", len(pending), len(new_ids))
        chat_to_rec = {p["chat_id"]: str(rid) for p, rid in zip(pending, new_ids) if rid}
        marked = local_db.mark_chats_secondary_synced(chat_to_rec)
        _sync_stats["chats_synced"] += marked
        return {"synced": marked, "failed": 0, "skipped": 0}
    except Exception as e:  # noqa: BLE001
        err = str(e)[:300]
        log.exception("[sync/secondary] chat batch failed (size=%d): %s", len(pending), err)
        local_db.bump_sync_failure("chats", "chat_id", [p["chat_id"] for p in pending], err)
        _sync_stats["chats_failed"] += len(pending)
        return {"synced": 0, "failed": len(pending), "skipped": 0}


def _build_message_row_minimal(chat_record_id_secondary: str, chat_id: str, chat_name: str,
                               msg_id: str, sender_id: str, sender_type: str,
                               time_ms: int, text: str, msg_type: str, is_deleted: int) -> list:
    """根据 BASE_MESSAGE_FIELD_DEFS 字段顺序构造行。link 字段必须用 record_id 列表。"""
    from sync_feishu_groups_to_base import BASE_MESSAGE_FIELD_DEFS
    sent_at = ""
    if time_ms:
        try:
            sent_at = datetime.fromtimestamp(time_ms / 1000, tz=SYNC_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    row = []
    for f in BASE_MESSAGE_FIELD_DEFS:
        n = f["name"]
        ftype = f.get("type")
        if n == "消息ID":
            row.append(msg_id or "")
        elif n == "群ID":
            row.append(chat_id or "")
        elif n == "群名称":
            row.append(chat_name or "")
        elif n == "关联群组":
            row.append([{"id": chat_record_id_secondary}] if chat_record_id_secondary else None)
        elif n == "消息类型":
            row.append(msg_type or "")
        elif n == "发送时间":
            row.append(sent_at)
        elif n == "发送者ID":
            row.append(sender_id or "")
        elif n == "发送者类型":
            row.append(sender_type or "")
        elif n == "是否已删除":
            row.append("是" if is_deleted else "否")
        elif n == "消息内容":
            row.append(text or "")
        elif n == "提取消息内容":
            row.append(text or "")
        elif n == "同步批次":
            row.append("secondary-sync-backfill")
        elif ftype in ("user", "select"):
            row.append(None)
        else:
            row.append("")
    return row


def _sync_messages_to_secondary(batch_size: int = 200) -> dict[str, int]:
    state = _ensure_lark_state_secondary()
    if not state.get("enabled"):
        return {"synced": 0, "failed": 0, "skipped": 0}
    from sync_feishu_groups_to_base import BASE_MESSAGE_FIELD_DEFS, MESSAGE_TABLE_NAME
    msg_table_id = state["table_ids"].get(MESSAGE_TABLE_NAME)
    if not msg_table_id:
        return {"synced": 0, "failed": 0, "skipped": 1}
    pending = local_db.get_pending_messages_for_secondary(limit=batch_size)
    if not pending:
        return {"synced": 0, "failed": 0, "skipped": 0}
    field_names = [f["name"] for f in BASE_MESSAGE_FIELD_DEFS]
    rows = [_build_message_row_minimal(
        p["chat_secondary_record_id"], p["chat_id"], p["chat_name"],
        p["msg_id"], p["sender_id"], p["sender_type"],
        p["time_ms"] or 0, p["text"] or "", p["msg_type"] or "",
        p["is_deleted"] or 0,
    ) for p in pending]
    try:
        resp = state["client"].batch_create_records(state["base_token"], msg_table_id, field_names, rows)
        new_ids = resp.get("record_id_list") or []
        msg_to_rec = {p["msg_id"]: str(rid) for p, rid in zip(pending, new_ids) if rid}
        marked = local_db.mark_messages_secondary_synced(msg_to_rec)
        _sync_stats["messages_synced"] += marked
        return {"synced": marked, "failed": 0, "skipped": 0}
    except Exception as e:  # noqa: BLE001
        err = str(e)[:300]
        log.exception("[sync/secondary] message batch failed (size=%d): %s", len(pending), err)
        local_db.bump_sync_failure("messages", "msg_id", [p["msg_id"] for p in pending], err)
        _sync_stats["messages_failed"] += len(pending)
        return {"synced": 0, "failed": len(pending), "skipped": 0}


def _build_member_row_minimal(chat_record_id_secondary: str, chat_id: str, chat_name: str,
                              member_open_id: str, name: str, tenant_key: str) -> list:
    from sync_feishu_groups_to_base import BASE_MEMBER_FIELD_DEFS
    row = []
    for f in BASE_MEMBER_FIELD_DEFS:
        n = f["name"]
        ftype = f.get("type")
        if n == "群ID":
            row.append(chat_id or "")
        elif n == "群名称":
            row.append(chat_name or "")
        elif n == "关联群组":
            row.append([{"id": chat_record_id_secondary}] if chat_record_id_secondary else None)
        elif n == "成员ID":
            row.append(member_open_id or "")
        elif n == "成员ID类型":
            row.append("open_id")
        elif n == "成员姓名":
            row.append(name or "")
        elif n == "成员租户Key":
            row.append(tenant_key or "")
        elif n == "同步批次":
            row.append("secondary-sync-backfill")
        elif ftype in ("user", "select"):
            row.append(None)
        else:
            row.append("")
    return row


def _sync_members_to_secondary(batch_size: int = 200) -> dict[str, int]:
    state = _ensure_lark_state_secondary()
    if not state.get("enabled"):
        return {"synced": 0, "failed": 0, "skipped": 0}
    from sync_feishu_groups_to_base import BASE_MEMBER_FIELD_DEFS, MEMBER_TABLE_NAME
    mem_table_id = state["table_ids"].get(MEMBER_TABLE_NAME)
    if not mem_table_id:
        return {"synced": 0, "failed": 0, "skipped": 1}
    pending = local_db.get_pending_members_for_secondary(limit=batch_size)
    if not pending:
        return {"synced": 0, "failed": 0, "skipped": 0}
    field_names = [f["name"] for f in BASE_MEMBER_FIELD_DEFS]
    rows = [_build_member_row_minimal(
        p["chat_secondary_record_id"], p["chat_id"], p["chat_name"],
        p["member_open_id"], p["name"] or "", p["tenant_key"] or "",
    ) for p in pending]
    try:
        resp = state["client"].batch_create_records(state["base_token"], mem_table_id, field_names, rows)
        new_ids = resp.get("record_id_list") or []
        items = [{"chat_id": p["chat_id"], "member_open_id": p["member_open_id"], "secondary_record_id": str(rid)}
                 for p, rid in zip(pending, new_ids) if rid]
        marked = local_db.mark_members_secondary_synced(items)
        _sync_stats["members_synced"] += marked
        return {"synced": marked, "failed": 0, "skipped": 0}
    except Exception as e:  # noqa: BLE001
        err = str(e)[:300]
        log.exception("[sync/secondary] member batch failed (size=%d): %s", len(pending), err)
        local_db.bump_sync_failure(
            "members", "chat_id",
            [p["chat_id"] for p in pending],
            err,
        )
        _sync_stats["members_failed"] += len(pending)
        return {"synced": 0, "failed": len(pending), "skipped": 0}


def _job_secondary_sync() -> None:
    """APScheduler 入口：按顺序跑 chat → msg → member 各一批。"""
    state = _ensure_lark_state_secondary()
    if not state.get("enabled"):
        return
    _sync_stats["last_run_ts"] = int(time.time())
    c = _sync_chats_to_secondary()
    m = _sync_messages_to_secondary()
    mb = _sync_members_to_secondary()
    if any(x["synced"] or x["failed"] for x in (c, m, mb)):
        log.info(
            "[sync/secondary] tick chat=%s msg=%s member=%s",
            c, m, mb,
        )


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


# 飞书 Base batch_create_records 单批硬上限：200 行。超过会返回
# code=800010701 "Array must contain at most 200 element(s)"
_BASE_BATCH_MAX = 200


def _create_member_rows_safely(
    client,
    base_token: str,
    member_table_id: str,
    member_field_defs: list,
    rows: list,
    *,
    chat_record_id: str,
) -> None:
    """Write member rows handling三类 link 失败：
    (1) chat 还没在 chat 表里 -> chat_record_id 为空 -> 整列 '关联群组' 丢弃
    (2) chat 刚 insert 完最终一致性还没到 -> 800030104 not_found -> 退避 retry 2 次
    (3) 单批 >200 行 -> 800010701 -> 按 _BASE_BATCH_MAX 切片
    """
    from sync_feishu_groups_to_base import FeishuAPIError

    field_names = [f["name"] for f in member_field_defs]
    link_idx = next((i for i, f in enumerate(member_field_defs) if f.get("name") == "关联群组"), -1)

    # case (1): 没有 link，把列丢掉
    if not chat_record_id and link_idx >= 0:
        field_names = [n for i, n in enumerate(field_names) if i != link_idx]
        rows = [[v for i, v in enumerate(r) if i != link_idx] for r in rows]

    # case (3): 切片，每片单独写。每片内部独立 retry 处理 not_found (case 2)
    for start in range(0, len(rows), _BASE_BATCH_MAX):
        chunk = rows[start : start + _BASE_BATCH_MAX]
        for attempt in range(3):
            try:
                client.batch_create_records(base_token, member_table_id, field_names, chunk)
                break
            except FeishuAPIError as e:
                if "800030104" in str(e) and attempt < 2:
                    sleep_s = 0.5 * (2 ** attempt)
                    log.info(
                        "[member-write] link not_found, retry %d/3 in %.1fs (chunk %d-%d)",
                        attempt + 1, sleep_s, start, start + len(chunk),
                    )
                    time.sleep(sleep_s)
                    continue
                raise


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
        # 关联群组 link 在 BASE_MESSAGE_FIELD_DEFS 里是第 4 列（index 3）。
        # 当 chat_record_id 为空（如 bot 刚加群，cache 还没更新），跳过 link，
        # 否则 [{"id": ""}] 会被 Base 拒掉 invalid_request。
        if not chat_record_id:
            row[3] = None
        message_field_defs = materialize_field_defs(BASE_MESSAGE_FIELD_DEFS, chat_table_id)
        message_fields = [f["name"] for f in message_field_defs]
        state["client"].batch_create_records(
            state["base_token"], msg_table_id, message_fields, [row]
        )
        # Tier 1: 同步写本地 SQLite，让 dashboard 看到最新消息
        try:
            try:
                ct = int(msg.get("create_time"))
                time_ms = ct if ct > 10**12 else ct * 1000
            except (TypeError, ValueError):
                time_ms = int(time.time() * 1000)
            content_obj = msg.get("content")
            if isinstance(content_obj, str):
                try:
                    content_obj = json.loads(content_obj)
                except Exception:  # noqa: BLE001
                    content_obj = None
            text_val = (content_obj or {}).get("text") if isinstance(content_obj, dict) else None
            local_db.upsert_message(
                message_id, chat_id,
                sender_id=(sender.get("sender_id") or {}).get("open_id"),
                sender_type=sender.get("sender_type"),
                time_ms=time_ms,
                text=text_val or str(msg.get("content") or "")[:1000],
                msg_type=msg.get("message_type"),
            )
        except Exception:  # noqa: BLE001
            log.exception("[event/message] local_db upsert failed (non-fatal)")
        _bump_persist_count("message")
        log.info("[event/message] persisted msg=%s chat=%s record_id=%s", message_id, chat_id, chat_record_id or "(missing)")
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
        _create_member_rows_safely(
            state["client"], state["base_token"], member_table_id,
            member_field_defs, rows, chat_record_id=chat_record_id,
        )
        # Tier 1: 双写本地 SQLite
        try:
            local_db.upsert_chat(chat_id, name=chat_name or None)
            local_db.upsert_members_bulk(chat_id, [
                {"id": m["member_id"], "name": m.get("name"), "tenant_key": m.get("tenant_key")}
                for m in members
            ])
        except Exception:  # noqa: BLE001
            log.exception("[event/member-add] local_db upsert failed (non-fatal)")
        _bump_persist_count("member-add")
        log.info("[event/member-add] +%d users in chat %s (link=%s)",
                 len(rows), chat_id, "yes" if chat_record_id else "dropped")
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
        # Tier 1: 双写本地 SQLite
        try:
            for oid in target_open_ids:
                local_db.delete_member(chat_id, oid)
        except Exception:  # noqa: BLE001
            log.exception("[event/member-del] local_db delete failed (non-fatal)")
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
        # Tier 1: 双写本地 SQLite
        try:
            local_db.mark_message_deleted(msg_id)
        except Exception:  # noqa: BLE001
            log.exception("[event/recall] local_db mark failed (non-fatal)")
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
        # Tier 1: 本地 SQLite 也清掉
        try:
            local_db.delete_chat(chat_id)
        except Exception:  # noqa: BLE001
            log.exception("[event/disbanded] local_db delete failed (non-fatal)")
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
                m_rows = build_member_rows(chat_id, chat_name, chat_record_id, members, sync_run_id)
                if m_rows:
                    _create_member_rows_safely(
                        client, base_token, member_table_id, mfd, m_rows,
                        chat_record_id=chat_record_id,
                    )

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

        # Tier 1: 双写本地 SQLite（chat / members / 24h 消息全部写一遍）
        try:
            local_db.upsert_chat(
                chat_id,
                name=chat_name or None,
                record_id=chat_record_id or None,
                member_total=member_total,
            )
            if members:
                local_db.upsert_members_bulk(chat_id, [
                    {"id": m.get("member_id"), "name": m.get("name"), "tenant_key": m.get("tenant_key")}
                    for m in members
                ])
            if messages:
                local_msgs = []
                for m in messages:
                    body_content = (m.get("body") or {}).get("content") or m.get("body") or ""
                    if isinstance(body_content, dict):
                        try:
                            body_content = body_content.get("text") or json.dumps(body_content, ensure_ascii=False)[:1000]
                        except Exception:  # noqa: BLE001
                            body_content = str(body_content)[:1000]
                    elif isinstance(body_content, str):
                        try:
                            parsed = json.loads(body_content)
                            if isinstance(parsed, dict):
                                body_content = parsed.get("text") or body_content[:1000]
                        except Exception:  # noqa: BLE001
                            pass
                    sender = m.get("sender") or {}
                    try:
                        ct = int(m.get("create_time") or 0)
                        t_ms = ct if ct > 10**12 else ct * 1000
                    except (TypeError, ValueError):
                        t_ms = 0
                    local_msgs.append({
                        "id": m.get("message_id"),
                        "chat_id": chat_id,
                        "sender_id": sender.get("id"),
                        "sender_type": sender.get("sender_type") or sender.get("id_type"),
                        "time": t_ms,
                        "text": body_content,
                        "msg_type": m.get("msg_type"),
                    })
                local_db.upsert_messages_bulk(local_msgs)
        except Exception:  # noqa: BLE001
            log.exception("[event/bot-added] local_db upsert failed (non-fatal)")

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

    # Dedup by event_id (Lark may retry on transient errors)
    if event_id:
        global _lark_dedup_count
        if not _seen_event_ids.add_if_new(event_id):
            _lark_dedup_count += 1
            log.info("[lark/events] dedup skip type=%s id=%s", event_type, event_id)
            return {"code": 0}

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
        "deduped": _lark_dedup_count,
        "chat_record_cache_size": len(_chat_record_cache),
        "recent": list(_lark_event_log)[-limit:],
    }


# ─── 静态前端 ─────────────────────────────────────────────────────────────────
# 必须放在所有 /api /admin /ws 路由之后，否则 mount("/") 会拦截显式路由。

# 容器启动时间作为 asset 版本号；新部署 → 新版本号 → 浏览器自动重新拉取。
ASSETS_VERSION = str(int(time.time()))

@app.get("/")
async def serve_root() -> Response:
    """动态注入版本戳到所有 <script src> 上，强制让浏览器在新部署时拉新代码。"""
    try:
        with open("web/index.html", "r", encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        raise HTTPException(404, "web/index.html missing")
    import re
    html = re.sub(
        r'(<script[^>]*src="src/[^"?]*)(")',
        rf'\1?v={ASSETS_VERSION}\2',
        html,
    )
    html = re.sub(
        r'(<link[^>]*href="src/[^"?]*)(")',
        rf'\1?v={ASSETS_VERSION}\2',
        html,
    )
    return Response(content=html, media_type="text/html; charset=utf-8", headers={
        "Cache-Control": "no-store, no-cache, must-revalidate",
    })


if os.path.isdir("web"):
    app.mount("/", StaticFiles(directory="web", html=True), name="static")
