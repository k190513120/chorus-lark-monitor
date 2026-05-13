"""SQLite 本地副本 — Lark Base 的只读镜像，给 dashboard cache rebuild 加速用。

数据流：
  webhook 事件 → server.py 处理器 → 同时写 Lark Base + 本地 SQLite
  daily-sync   → 同时写 Lark Base + 本地 SQLite
  dashboard    → 从本地 SQLite 读（SQL 查询毫秒级）

之所以保留 Lark Base 双写：
  - Lark Base 仍然是跨团队共享的数据视图（业务侧用户在飞书里直接查）
  - 本地 SQLite 只是 server.py 的 read accelerator

输出 shape 与 export_to_web.load_chats / load_members / load_messages 完全一致，
所以 build_app_data() 不用改。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional


DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chorus_local.db")

_db_lock = threading.RLock()
_initialized = False


SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id TEXT PRIMARY KEY,
    record_id TEXT,
    name TEXT,
    description TEXT,
    chat_type_label TEXT,
    member_total INTEGER DEFAULT 0,
    owner_id TEXT,
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_chats_member_total ON chats(member_total DESC);

CREATE TABLE IF NOT EXISTS members (
    chat_id TEXT NOT NULL,
    member_open_id TEXT NOT NULL,
    name TEXT,
    tenant_key TEXT,
    updated_at INTEGER,
    PRIMARY KEY (chat_id, member_open_id)
);
CREATE INDEX IF NOT EXISTS idx_members_chat ON members(chat_id);

CREATE TABLE IF NOT EXISTS messages (
    msg_id TEXT PRIMARY KEY,
    chat_id TEXT NOT NULL,
    sender_id TEXT,
    sender_type TEXT,
    time_ms INTEGER,
    text TEXT,
    msg_type TEXT,
    is_deleted INTEGER DEFAULT 0,
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, time_ms DESC);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at INTEGER
);
"""


def connect() -> sqlite3.Connection:
    """Per-thread connection. WAL mode for concurrent reads + single writer."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db() -> None:
    """Create schema if missing. Idempotent."""
    global _initialized
    if _initialized:
        return
    with _db_lock:
        if _initialized:
            return
        conn = connect()
        try:
            conn.executescript(SCHEMA)
            # ─── sync-state 字段（Base 同步用，2026-05-13 加）────────────────
            # primary  = 旧 Base (LARK_BASE_URL)
            # secondary = 新 Base (LARK_BASE_URL_SECONDARY)
            # 历史数据 (来自 daily-sync seed) 一律标 primary_synced=1，避免被 worker 重写。
            # secondary 默认 0，等 worker 回补。
            _migrate_add_column(conn, "chats", "secondary_record_id", "TEXT")
            _migrate_add_column(conn, "chats", "primary_synced", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "chats", "secondary_synced", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "chats", "sync_attempts", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "chats", "sync_last_error", "TEXT")
            _migrate_add_column(conn, "messages", "primary_record_id", "TEXT")
            _migrate_add_column(conn, "messages", "secondary_record_id", "TEXT")
            _migrate_add_column(conn, "messages", "primary_synced", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "messages", "secondary_synced", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "messages", "sync_attempts", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "messages", "sync_last_error", "TEXT")
            _migrate_add_column(conn, "messages", "raw_event_json", "TEXT")
            _migrate_add_column(conn, "members", "primary_record_id", "TEXT")
            _migrate_add_column(conn, "members", "secondary_record_id", "TEXT")
            _migrate_add_column(conn, "members", "primary_synced", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "members", "secondary_synced", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "members", "sync_attempts", "INTEGER DEFAULT 0")
            _migrate_add_column(conn, "members", "sync_last_error", "TEXT")
            # 一次性补：所有历史 chats/messages/members 视为已同步到 primary
            # (这些数据来自 daily-sync seed，已经在旧 Base 里了)
            already_marked = conn.execute(
                "SELECT value FROM meta WHERE key = 'sync_history_marked'"
            ).fetchone()
            if not already_marked:
                conn.execute("UPDATE chats SET primary_synced = 1 WHERE record_id IS NOT NULL")
                conn.execute("UPDATE messages SET primary_synced = 1")
                conn.execute("UPDATE members SET primary_synced = 1")
                conn.execute(
                    "INSERT INTO meta (key, value, updated_at) VALUES ('sync_history_marked', ?, ?)",
                    (str(_now_ts()), _now_ts()),
                )
            # 索引：sync worker 查询用
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chats_secondary_pending ON chats(secondary_synced) WHERE secondary_synced = 0")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_secondary_pending ON messages(secondary_synced) WHERE secondary_synced = 0")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_members_secondary_pending ON members(secondary_synced) WHERE secondary_synced = 0")
            conn.commit()
        finally:
            conn.close()
        _initialized = True


def _migrate_add_column(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    """Idempotent ALTER TABLE ADD COLUMN. Skips if column already exists."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def _now_ts() -> int:
    return int(time.time())


# ─── sync queue helpers（给 Base sync worker 用） ──────────────────────────────

def get_pending_chats_for_secondary(limit: int = 200) -> List[Dict[str, Any]]:
    init_db()
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT chat_id, name, description, chat_type_label, member_total, owner_id
            FROM chats
            WHERE secondary_synced = 0
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_pending_messages_for_secondary(limit: int = 200) -> List[Dict[str, Any]]:
    """只取那些其 chat 已经同步到 secondary 的消息（保证 link 字段有 record_id）。"""
    init_db()
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT m.msg_id, m.chat_id, m.sender_id, m.sender_type, m.time_ms,
                   m.text, m.msg_type, m.is_deleted, m.raw_event_json,
                   c.secondary_record_id AS chat_secondary_record_id,
                   c.name AS chat_name
            FROM messages m
            JOIN chats c ON c.chat_id = m.chat_id
            WHERE m.secondary_synced = 0
              AND c.secondary_record_id IS NOT NULL
            ORDER BY m.time_ms ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_pending_members_for_secondary(limit: int = 200) -> List[Dict[str, Any]]:
    init_db()
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT mb.chat_id, mb.member_open_id, mb.name, mb.tenant_key,
                   c.secondary_record_id AS chat_secondary_record_id,
                   c.name AS chat_name
            FROM members mb
            JOIN chats c ON c.chat_id = mb.chat_id
            WHERE mb.secondary_synced = 0
              AND c.secondary_record_id IS NOT NULL
            ORDER BY mb.updated_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_chats_secondary_synced(chat_to_record_id: Dict[str, str]) -> int:
    """传入 {chat_id: secondary_record_id}，批量 update。"""
    if not chat_to_record_id:
        return 0
    init_db()
    conn = connect()
    n = 0
    try:
        for chat_id, rec_id in chat_to_record_id.items():
            cur = conn.execute(
                """
                UPDATE chats
                SET secondary_record_id = ?, secondary_synced = 1, sync_attempts = 0, sync_last_error = NULL
                WHERE chat_id = ?
                """,
                (rec_id, chat_id),
            )
            n += cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def mark_messages_secondary_synced(msg_to_record_id: Dict[str, str]) -> int:
    if not msg_to_record_id:
        return 0
    init_db()
    conn = connect()
    n = 0
    try:
        for msg_id, rec_id in msg_to_record_id.items():
            cur = conn.execute(
                """
                UPDATE messages
                SET secondary_record_id = ?, secondary_synced = 1, sync_attempts = 0, sync_last_error = NULL
                WHERE msg_id = ?
                """,
                (rec_id, msg_id),
            )
            n += cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def mark_members_secondary_synced(items: List[Dict[str, str]]) -> int:
    """items: [{chat_id, member_open_id, secondary_record_id}, ...]"""
    if not items:
        return 0
    init_db()
    conn = connect()
    n = 0
    try:
        for it in items:
            cur = conn.execute(
                """
                UPDATE members
                SET secondary_record_id = ?, secondary_synced = 1, sync_attempts = 0, sync_last_error = NULL
                WHERE chat_id = ? AND member_open_id = ?
                """,
                (it["secondary_record_id"], it["chat_id"], it["member_open_id"]),
            )
            n += cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def bump_sync_failure(table: str, key_col: str, key_vals: List[Any], error_msg: str) -> None:
    """sync 失败时调，递增 attempts + 写 last_error。table ∈ {chats, messages, members}, key_col ∈ {chat_id, msg_id, member_open_id}"""
    if not key_vals:
        return
    init_db()
    conn = connect()
    try:
        placeholders = ",".join(["?"] * len(key_vals))
        conn.execute(
            f"""
            UPDATE {table}
            SET sync_attempts = COALESCE(sync_attempts, 0) + 1, sync_last_error = ?
            WHERE {key_col} IN ({placeholders})
            """,
            [error_msg[:500]] + list(key_vals),
        )
        conn.commit()
    finally:
        conn.close()


def get_sync_queue_stats() -> Dict[str, int]:
    init_db()
    conn = connect()
    try:
        out = {}
        for tbl in ("chats", "messages", "members"):
            out[f"{tbl}_pending_secondary"] = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE secondary_synced = 0"
            ).fetchone()[0]
            out[f"{tbl}_secondary_done"] = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE secondary_synced = 1"
            ).fetchone()[0]
        return out
    finally:
        conn.close()


# ─── upsert helpers (写路径，webhook 处理器 + daily-sync 调用) ────────────────

def upsert_chat(
    chat_id: str,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    chat_type_label: Optional[str] = None,
    member_total: Optional[int] = None,
    owner_id: Optional[str] = None,
    record_id: Optional[str] = None,
) -> None:
    if not chat_id:
        return
    init_db()
    now = _now_ts()
    conn = connect()
    try:
        # UPSERT — 已存在的字段如果新值是 None 就保留原值
        conn.execute(
            """
            INSERT INTO chats (chat_id, record_id, name, description, chat_type_label,
                               member_total, owner_id, updated_at)
            VALUES (?, ?, ?, ?, ?, COALESCE(?, 0), ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                record_id = COALESCE(excluded.record_id, chats.record_id),
                name = COALESCE(excluded.name, chats.name),
                description = COALESCE(excluded.description, chats.description),
                chat_type_label = COALESCE(excluded.chat_type_label, chats.chat_type_label),
                member_total = CASE WHEN excluded.member_total > 0 THEN excluded.member_total ELSE chats.member_total END,
                owner_id = COALESCE(excluded.owner_id, chats.owner_id),
                updated_at = excluded.updated_at
            """,
            (chat_id, record_id, name, description, chat_type_label, member_total, owner_id, now),
        )
        conn.commit()
    finally:
        conn.close()


def set_secondary_record_id(chat_id: str, secondary_record_id: str) -> None:
    """记录新 Base 里这个 chat 的 record_id，给 dual-write 链接字段用。"""
    if not chat_id or not secondary_record_id:
        return
    init_db()
    conn = connect()
    try:
        conn.execute(
            "UPDATE chats SET secondary_record_id = ? WHERE chat_id = ?",
            (secondary_record_id, chat_id),
        )
        if conn.total_changes == 0:
            # row 不存在就插入个空骨架记录
            conn.execute(
                "INSERT OR IGNORE INTO chats (chat_id, secondary_record_id, updated_at) VALUES (?, ?, ?)",
                (chat_id, secondary_record_id, _now_ts()),
            )
        conn.commit()
    finally:
        conn.close()


def get_secondary_record_id(chat_id: str) -> Optional[str]:
    """读取新 Base 里这个 chat 的 record_id。返回 None 表示还没回补。"""
    if not chat_id:
        return None
    init_db()
    conn = connect()
    try:
        row = conn.execute(
            "SELECT secondary_record_id FROM chats WHERE chat_id = ?", (chat_id,),
        ).fetchone()
        return row["secondary_record_id"] if row and row["secondary_record_id"] else None
    finally:
        conn.close()


def load_secondary_record_id_map() -> Dict[str, str]:
    """一次性 load 所有 chat 的 secondary_record_id 到内存 map，给热路径用。"""
    init_db()
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT chat_id, secondary_record_id FROM chats WHERE secondary_record_id IS NOT NULL"
        ).fetchall()
        return {r["chat_id"]: r["secondary_record_id"] for r in rows}
    finally:
        conn.close()


def upsert_chats_bulk(rows: List[Dict[str, Any]]) -> int:
    """批量 upsert，给 seed/daily-sync 用。"""
    if not rows:
        return 0
    init_db()
    now = _now_ts()
    conn = connect()
    n = 0
    try:
        for r in rows:
            cid = r.get("id") or r.get("chat_id")
            if not cid:
                continue
            conn.execute(
                """
                INSERT INTO chats (chat_id, record_id, name, description, chat_type_label,
                                   member_total, owner_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    record_id = COALESCE(excluded.record_id, chats.record_id),
                    name = excluded.name,
                    description = excluded.description,
                    chat_type_label = excluded.chat_type_label,
                    member_total = excluded.member_total,
                    owner_id = excluded.owner_id,
                    updated_at = excluded.updated_at
                """,
                (cid, r.get("record_id"), r.get("name"), r.get("description"),
                 r.get("chat_type_label"), int(r.get("member_total") or 0),
                 r.get("owner_id"), now),
            )
            n += 1
        conn.commit()
    finally:
        conn.close()
    return n


def upsert_member(chat_id: str, member_open_id: str, *,
                  name: Optional[str] = None, tenant_key: Optional[str] = None) -> None:
    if not chat_id or not member_open_id:
        return
    init_db()
    now = _now_ts()
    conn = connect()
    try:
        conn.execute(
            """
            INSERT INTO members (chat_id, member_open_id, name, tenant_key, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, member_open_id) DO UPDATE SET
                name = COALESCE(excluded.name, members.name),
                tenant_key = COALESCE(excluded.tenant_key, members.tenant_key),
                updated_at = excluded.updated_at
            """,
            (chat_id, member_open_id, name, tenant_key, now),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_members_bulk(chat_id: str, members: List[Dict[str, Any]]) -> int:
    if not chat_id or not members:
        return 0
    init_db()
    now = _now_ts()
    conn = connect()
    n = 0
    try:
        for m in members:
            mid = m.get("id") or m.get("member_open_id") or m.get("member_id")
            if not mid:
                continue
            conn.execute(
                """
                INSERT INTO members (chat_id, member_open_id, name, tenant_key, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, member_open_id) DO UPDATE SET
                    name = excluded.name,
                    tenant_key = excluded.tenant_key,
                    updated_at = excluded.updated_at
                """,
                (chat_id, mid, m.get("name"), m.get("tenant_key"), now),
            )
            n += 1
        conn.commit()
    finally:
        conn.close()
    return n


def delete_member(chat_id: str, member_open_id: str) -> None:
    if not chat_id or not member_open_id:
        return
    init_db()
    conn = connect()
    try:
        conn.execute(
            "DELETE FROM members WHERE chat_id = ? AND member_open_id = ?",
            (chat_id, member_open_id),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_message(
    msg_id: str, chat_id: str, *,
    sender_id: Optional[str] = None,
    sender_type: Optional[str] = None,
    time_ms: Optional[int] = None,
    text: Optional[str] = None,
    msg_type: Optional[str] = None,
    is_deleted: Optional[int] = None,
) -> None:
    if not msg_id or not chat_id:
        return
    init_db()
    now = _now_ts()
    conn = connect()
    try:
        conn.execute(
            """
            INSERT INTO messages (msg_id, chat_id, sender_id, sender_type, time_ms,
                                  text, msg_type, is_deleted, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, 0), ?)
            ON CONFLICT(msg_id) DO UPDATE SET
                sender_id = COALESCE(excluded.sender_id, messages.sender_id),
                sender_type = COALESCE(excluded.sender_type, messages.sender_type),
                time_ms = COALESCE(excluded.time_ms, messages.time_ms),
                text = COALESCE(excluded.text, messages.text),
                msg_type = COALESCE(excluded.msg_type, messages.msg_type),
                is_deleted = COALESCE(excluded.is_deleted, messages.is_deleted),
                updated_at = excluded.updated_at
            """,
            (msg_id, chat_id, sender_id, sender_type, time_ms, text, msg_type, is_deleted, now),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_messages_bulk(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    init_db()
    now = _now_ts()
    conn = connect()
    n = 0
    try:
        for r in rows:
            msg_id = r.get("id") or r.get("msg_id")
            chat_id = r.get("chat_id")
            if not msg_id or not chat_id:
                continue
            conn.execute(
                """
                INSERT INTO messages (msg_id, chat_id, sender_id, sender_type, time_ms,
                                      text, msg_type, is_deleted, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(msg_id) DO UPDATE SET
                    sender_id = excluded.sender_id,
                    sender_type = excluded.sender_type,
                    time_ms = excluded.time_ms,
                    text = excluded.text,
                    msg_type = excluded.msg_type,
                    is_deleted = excluded.is_deleted,
                    updated_at = excluded.updated_at
                """,
                (msg_id, chat_id, r.get("sender_id"), r.get("sender_type"),
                 r.get("time"), r.get("text"), r.get("msg_type"),
                 int(r.get("is_deleted") or 0), now),
            )
            n += 1
        conn.commit()
    finally:
        conn.close()
    return n


def mark_message_deleted(msg_id: str) -> None:
    if not msg_id:
        return
    init_db()
    conn = connect()
    try:
        conn.execute(
            "UPDATE messages SET is_deleted = 1, updated_at = ? WHERE msg_id = ?",
            (_now_ts(), msg_id),
        )
        conn.commit()
    finally:
        conn.close()


def delete_chat(chat_id: str) -> None:
    """群解散时清理。messages/members 通过 ON DELETE CASCADE 不可行（schema 没声明），
    所以这里手动 DELETE 三张表对应行。"""
    if not chat_id:
        return
    init_db()
    conn = connect()
    try:
        conn.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        conn.execute("DELETE FROM members WHERE chat_id = ?", (chat_id,))
        conn.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
        conn.commit()
    finally:
        conn.close()


# ─── load helpers (读路径，server._build_dashboard_payload 调用) ─────────────

def load_chats(chat_ids: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """与 export_to_web.load_chats 返回 shape 一致。
    如果传 chat_ids，只返回这些 chat（用来过滤 top N）。"""
    init_db()
    conn = connect()
    try:
        if chat_ids is not None:
            ids = list(chat_ids)
            if not ids:
                return []
            placeholders = ",".join("?" * len(ids))
            rows = conn.execute(
                f"SELECT * FROM chats WHERE chat_id IN ({placeholders})", ids
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM chats").fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r["chat_id"],
            "record_id": r["record_id"],
            "name": r["name"] or r["chat_id"],
            "description": r["description"],
            "chat_type_label": r["chat_type_label"],
            "member_total": r["member_total"] or 0,
            "owner_id": r["owner_id"],
        }
        for r in rows
    ]


def load_members(chat_ids: Optional[Iterable[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    init_db()
    conn = connect()
    try:
        if chat_ids is not None:
            ids = list(chat_ids)
            if not ids:
                return {}
            placeholders = ",".join("?" * len(ids))
            rows = conn.execute(
                f"SELECT * FROM members WHERE chat_id IN ({placeholders})", ids
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM members").fetchall()
    finally:
        conn.close()
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        out[r["chat_id"]].append({
            "id": r["member_open_id"],
            "name": r["name"] or "未知成员",
            "tenant_key": r["tenant_key"],
        })
    return dict(out)


def load_messages(
    chat_ids: Optional[Iterable[str]] = None,
    max_per_chat: Optional[int] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """如果传 max_per_chat，每个 chat 只取最近 N 条（按 time_ms desc）。"""
    init_db()
    conn = connect()
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    try:
        if chat_ids is None:
            # 全量，但是 max_per_chat 时必须按 chat 分组限制
            if max_per_chat:
                # 先列所有 chat_id 再循环；规模 ~500 群可接受
                chat_ids_in_db = [r[0] for r in conn.execute(
                    "SELECT DISTINCT chat_id FROM messages"
                ).fetchall()]
            else:
                chat_ids_in_db = None
        else:
            chat_ids_in_db = list(chat_ids)

        if chat_ids_in_db is None:
            # 真正的全量 (max_per_chat 也未设)
            rows = conn.execute(
                "SELECT * FROM messages WHERE is_deleted = 0 ORDER BY chat_id, time_ms"
            ).fetchall()
            for r in rows:
                out[r["chat_id"]].append({
                    "id": r["msg_id"],
                    "sender_id": r["sender_id"],
                    "sender_type": r["sender_type"],
                    "time": r["time_ms"] or 0,
                    "text": r["text"] or "[无内容]",
                    "msg_type": r["msg_type"],
                })
        else:
            for cid in chat_ids_in_db:
                if max_per_chat:
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE chat_id = ? AND is_deleted = 0 "
                        "ORDER BY time_ms DESC LIMIT ?",
                        (cid, max_per_chat),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM messages WHERE chat_id = ? AND is_deleted = 0 "
                        "ORDER BY time_ms",
                        (cid,),
                    ).fetchall()
                msgs = [{
                    "id": r["msg_id"],
                    "sender_id": r["sender_id"],
                    "sender_type": r["sender_type"],
                    "time": r["time_ms"] or 0,
                    "text": r["text"] or "[无内容]",
                    "msg_type": r["msg_type"],
                } for r in rows]
                # 与 export_to_web 一致：按时间正序
                msgs.sort(key=lambda m: m["time"])
                out[cid] = msgs
    finally:
        conn.close()
    return dict(out)


def top_n_chat_ids_by_activity(n: int) -> List[str]:
    """与 pick_active_groups 同口径：先按消息数降序，再按成员数。
    返回 top N 的 chat_id 列表。"""
    init_db()
    conn = connect()
    try:
        rows = conn.execute("""
            SELECT c.chat_id,
                   COALESCE((SELECT COUNT(*) FROM messages m WHERE m.chat_id = c.chat_id AND m.is_deleted = 0), 0) AS msg_count,
                   COALESCE((SELECT COUNT(*) FROM members mb WHERE mb.chat_id = c.chat_id), 0) AS member_count
            FROM chats c
            ORDER BY msg_count DESC, member_count DESC
            LIMIT ?
        """, (n,)).fetchall()
    finally:
        conn.close()
    return [r["chat_id"] for r in rows]


def get_stats() -> Dict[str, int]:
    init_db()
    conn = connect()
    try:
        chats_n = conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0]
        members_n = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
        msgs_n = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        msgs_active_n = conn.execute("SELECT COUNT(*) FROM messages WHERE is_deleted = 0").fetchone()[0]
        return {
            "chats": chats_n,
            "members": members_n,
            "messages_total": msgs_n,
            "messages_active": msgs_active_n,
            "db_path": DB_PATH,
            "db_size_bytes": os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
        }
    finally:
        conn.close()


def set_meta(key: str, value: str) -> None:
    init_db()
    conn = connect()
    try:
        conn.execute(
            """
            INSERT INTO meta (key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, _now_ts()),
        )
        conn.commit()
    finally:
        conn.close()


def get_meta(key: str) -> Optional[str]:
    init_db()
    conn = connect()
    try:
        r = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return r["value"] if r else None
    finally:
        conn.close()


def seed_from_lark_base(client, base_token: str, table_ids: Dict[str, str], sync_tz) -> Dict[str, Any]:
    """从 Lark Base 全量拉一次回填到本地 SQLite。耗时跟当前 cache rebuild 一样（5-9 min）
    但只此一次，之后由 webhook 事件 + daily-sync 双写保持同步。"""
    import export_to_web as ex
    from sync_feishu_groups_to_base import (
        CHAT_TABLE_NAME, MEMBER_TABLE_NAME, MESSAGE_TABLE_NAME,
    )

    started = time.time()
    counts: Dict[str, Any] = {"chats": 0, "members": 0, "messages": 0}

    chat_table_id = table_ids.get(CHAT_TABLE_NAME)
    member_table_id = table_ids.get(MEMBER_TABLE_NAME)
    message_table_id = table_ids.get(MESSAGE_TABLE_NAME)

    if chat_table_id:
        chats = ex.load_chats(client, base_token, chat_table_id)
        counts["chats"] = upsert_chats_bulk(chats)

    if member_table_id:
        members_by_chat = ex.load_members(client, base_token, member_table_id)
        for cid, members in members_by_chat.items():
            counts["members"] += upsert_members_bulk(cid, members)

    if message_table_id:
        messages_by_chat = ex.load_messages(client, base_token, message_table_id, sync_tz)
        # 摊平到一个 list 做批量 upsert
        all_msgs: List[Dict[str, Any]] = []
        for cid, msgs in messages_by_chat.items():
            for m in msgs:
                m2 = dict(m)
                m2["chat_id"] = cid
                all_msgs.append(m2)
        counts["messages"] = upsert_messages_bulk(all_msgs)

    counts["elapsed_seconds"] = round(time.time() - started, 1)
    set_meta("last_seeded_at", str(_now_ts()))
    set_meta("last_seed_counts", json.dumps(counts, ensure_ascii=False))
    return counts
