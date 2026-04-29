#!/usr/bin/env python3
"""
最小可行验证：
  - send    : 给若干 chat 发同一条文本消息，记录 (chat_id, message_id, sent_at) 到 state file
  - collect : 从 state file 读取，回收每条消息的已读人数 + 回复明细，打印汇总

依赖现有 sync_feishu_groups_to_base.FeishuClient 处理鉴权 / 重试 / token 刷新。

环境变量：
  LARK_APP_ID, LARK_APP_SECRET 必填
  LARK_BASE_URL              选填（仅 send --from-base 时用，可传 token 或完整 base URL）

用法：
  # 显式指定 chat_id
  python3 bulk_message_probe.py send --chat-ids oc_a,oc_b --text "测试消息"

  # 从 Base 表里取前 N 个群（默认 N=2）
  export LARK_BASE_URL=PnRtbGmTpaVXwDsWBWPcPaEpnwh
  python3 bulk_message_probe.py send --from-base --limit 2 --text "测试消息"

  # 1 小时后回收
  python3 bulk_message_probe.py collect
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

from sync_feishu_groups_to_base import (
    FeishuAPIError,
    FeishuClient,
    chunked,
    ensure_table_and_fields,
    epoch_seconds_to_text,
    parse_base_token,
    stringify,
)
from zoneinfo import ZoneInfo


DEFAULT_STATE_FILE = ".bulk_message_probe_state.json"
SEND_THROTTLE_SECONDS = 0.05  # 50ms between sends → ~20 QPS
SYNC_TZ = ZoneInfo("Asia/Shanghai")

# 统计时从分子和分母中都排除这些 tenant_key 下的用户（默认排除内部组织成员）。
# 可通过环境变量 EXCLUDE_TENANT_KEYS 覆盖（逗号分隔）。
EXCLUDED_TENANT_KEYS = {
    s.strip()
    for s in (os.environ.get("EXCLUDE_TENANT_KEYS") or "736588c9260f175d").split(",")
    if s.strip()
}

BULK_TASK_TABLE_NAME = "群发任务记录"
BULK_TASK_GROUPCHAT_FIELD = "群组字段-允许添加多个群组"
BULK_TASK_FIELD_DEFS = [
    {"name": "群发批次", "type": "text"},
    {"name": "任务标题", "type": "text"},
    {"name": "群ID", "type": "text"},
    {"name": "群名称", "type": "text"},
    {"name": "消息ID", "type": "text"},
    {"name": "发送时间", "type": "text"},
    {"name": "发送状态", "type": "text"},
    {"name": "错误信息", "type": "text"},
    {"name": "消息内容", "type": "text"},
    {"name": "群人数", "type": "text"},
    {"name": "目标受众", "type": "text"},
    {"name": "已读人数", "type": "text"},
    {"name": "已读率", "type": "text"},
    {"name": "回复条数", "type": "text"},
    {"name": "回复人数", "type": "text"},
    {"name": "回复率", "type": "text"},
    {"name": "回复明细", "type": "text"},
    {"name": "排除tenant_keys", "type": "text"},
    {"name": "最后采集时间", "type": "text"},
]
BULK_TASK_FIELDS = [f["name"] for f in BULK_TASK_FIELD_DEFS]


def make_client() -> FeishuClient:
    app_id = os.environ.get("LARK_APP_ID")
    app_secret = os.environ.get("LARK_APP_SECRET")
    if not app_id or not app_secret:
        sys.exit("LARK_APP_ID / LARK_APP_SECRET environment variables required")
    client = FeishuClient(app_id=app_id, app_secret=app_secret)
    client.authenticate()
    return client


def resolve_chat_ids_from_base(client: FeishuClient, base_token: str, limit: int) -> List[Dict[str, str]]:
    tables = client.list_tables(base_token)
    chat_tbl = None
    for t in tables:
        name = t.get("name") or t.get("table_name")
        if name == "机器人群列表":
            chat_tbl = t
            break
    if not chat_tbl:
        raise FeishuAPIError("chat table '机器人群列表' not found in base")
    table_id = chat_tbl.get("table_id") or chat_tbl.get("id")
    if not table_id:
        raise FeishuAPIError("chat table id missing")

    out: List[Dict[str, str]] = []
    page_token: Optional[str] = None
    while len(out) < limit:
        params: Dict[str, object] = {
            "page_size": min(100, limit),
            "field_names": json.dumps(["群ID", "群名称"], ensure_ascii=False),
        }
        if page_token:
            params["page_token"] = page_token
        data = client.request(
            "GET",
            f"/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records",
            params=params,
        )
        for item in data.get("items") or []:
            fields = item.get("fields") or {}
            chat_id = _extract_text(fields.get("群ID"))
            chat_name = _extract_text(fields.get("群名称"))
            if chat_id:
                out.append({"chat_id": chat_id, "chat_name": chat_name})
                if len(out) >= limit:
                    break
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
        if not page_token:
            break
    return out


def _extract_text(cell: object) -> str:
    if cell in (None, ""):
        return ""
    if isinstance(cell, str):
        return cell
    if isinstance(cell, list):
        parts: List[str] = []
        for seg in cell:
            if isinstance(seg, dict):
                t = seg.get("text") or seg.get("value")
                if isinstance(t, str):
                    parts.append(t)
            elif isinstance(seg, str):
                parts.append(seg)
        return "".join(parts)
    if isinstance(cell, dict):
        t = cell.get("text") or cell.get("value")
        if isinstance(t, str):
            return t
    return str(cell)


def send_one(client: FeishuClient, chat_id: str, text: str) -> Dict[str, object]:
    payload = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    try:
        data = client.request(
            "POST",
            "/open-apis/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            data=payload,
        )
    except FeishuAPIError as exc:
        return {"chat_id": chat_id, "ok": False, "error": str(exc)}
    return {
        "chat_id": chat_id,
        "ok": True,
        "message_id": data.get("message_id"),
        "create_time": data.get("create_time"),
    }


def cmd_send(args: argparse.Namespace) -> int:
    client = make_client()

    chats: List[Dict[str, str]] = []
    if args.chat_ids:
        for cid in args.chat_ids.split(","):
            cid = cid.strip()
            if cid:
                chats.append({"chat_id": cid, "chat_name": ""})
    elif args.from_base:
        base_url = os.environ.get("LARK_BASE_URL")
        if not base_url:
            sys.exit("--from-base requires LARK_BASE_URL env var (token or /base/<token> URL)")
        base_token = parse_base_token(base_url)
        chats = resolve_chat_ids_from_base(client, base_token, args.limit)
        if not chats:
            sys.exit("no chats found in Base 机器人群列表 table")
    else:
        sys.exit("specify --chat-ids or --from-base")

    print(f"准备发送到 {len(chats)} 个群：")
    for c in chats:
        print(f"  - {c['chat_id']}  {c['chat_name'] or ''}")
    if not args.yes:
        confirm = input("确认发送？(y/N) ").strip().lower()
        if confirm != "y":
            print("已取消。")
            return 1

    batch_id = datetime.now(SYNC_TZ).strftime("%Y%m%d%H%M%S")
    title = args.title or args.text[:30]

    results: List[Dict[str, object]] = []
    for c in chats:
        sent_at = int(time.time())
        r = send_one(client, c["chat_id"], args.text)
        r["chat_name"] = c.get("chat_name", "")
        r["sent_at"] = sent_at
        r["sent_at_text"] = epoch_seconds_to_text(sent_at, SYNC_TZ)
        r["text"] = args.text
        results.append(r)
        flag = "✓" if r.get("ok") else "✗"
        print(f"  {flag} {c['chat_id']}  {c.get('chat_name','')}  msg_id={r.get('message_id') or r.get('error')}")
        time.sleep(SEND_THROTTLE_SECONDS)

    state = {
        "batch_id": batch_id,
        "title": title,
        "saved_at": int(time.time()),
        "text": args.text,
        "results": results,
    }
    with open(args.state_file, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)

    ok_n = sum(1 for r in results if r.get("ok"))
    print(f"\n完成：{ok_n}/{len(results)} 成功，state file: {args.state_file}（batch_id={batch_id}）")
    return 0 if ok_n == len(results) else 2


def list_read_users(client: FeishuClient, message_id: str) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, object] = {"user_id_type": "open_id", "page_size": 100}
        if page_token:
            params["page_token"] = page_token
        data = client.request(
            "GET",
            f"/open-apis/im/v1/messages/{message_id}/read_users",
            params=params,
        )
        out.extend(data.get("items") or [])
        if not data.get("has_more"):
            return out
        page_token = data.get("page_token")
        if not page_token:
            return out


def list_replies(
    client: FeishuClient,
    chat_id: str,
    parent_message_id: str,
    sent_at: int,
    *,
    horizon_days: int = 14,
) -> List[Dict[str, object]]:
    """Fetch chat history starting from sent_at, filter messages whose root_id or parent_id matches."""
    end_time = int(time.time())
    start_time = sent_at  # inclusive
    out: List[Dict[str, object]] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, object] = {
            "container_id_type": "chat",
            "container_id": chat_id,
            "page_size": 50,
            "start_time": start_time,
            "end_time": end_time,
        }
        if page_token:
            params["page_token"] = page_token
        data = client.request("GET", "/open-apis/im/v1/messages", params=params)
        for msg in data.get("items") or []:
            root_id = stringify(msg.get("root_id"))
            parent_id = stringify(msg.get("parent_id"))
            if root_id == parent_message_id or parent_id == parent_message_id:
                out.append(msg)
        if not data.get("has_more"):
            return out
        page_token = data.get("page_token")
        if not page_token:
            return out


def list_all_chat_members(client: FeishuClient, chat_id: str) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, object] = {"page_size": 100, "member_id_type": "open_id"}
        if page_token:
            params["page_token"] = page_token
        data = client.request(
            "GET",
            f"/open-apis/im/v1/chats/{chat_id}/members",
            params=params,
        )
        out.extend(data.get("items") or [])
        if not data.get("has_more"):
            return out
        page_token = data.get("page_token")
        if not page_token:
            return out


def is_excluded_tenant(tenant_key: object) -> bool:
    return stringify(tenant_key) in EXCLUDED_TENANT_KEYS


def extract_message_text(msg: Dict[str, object]) -> str:
    body = msg.get("body") or {}
    content = body.get("content") if isinstance(body, dict) else None
    if not content:
        return ""
    try:
        parsed = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return str(content)[:200]
    if isinstance(parsed, dict):
        if "text" in parsed:
            return str(parsed["text"])
        # post / card 等结构粗暴 stringify
        return json.dumps(parsed, ensure_ascii=False)[:200]
    return str(parsed)[:200]


def format_rate(value: object) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return ""


def build_bulk_task_row(
    r: Dict[str, object],
    state: Dict[str, object],
    collected_at_text: str,
) -> List[str]:
    return [
        stringify(state.get("batch_id")),
        stringify(state.get("title") or ""),
        stringify(r.get("chat_id")),
        stringify(r.get("chat_name")),
        stringify(r.get("message_id")),
        stringify(r.get("sent_at_text") or ""),
        "成功" if r.get("send_ok") else "失败",
        stringify(r.get("send_error") or ""),
        stringify(state.get("text")),
        stringify(r.get("member_total_all")),
        stringify(r.get("target_audience")),
        stringify(r.get("read_count")),
        format_rate(r.get("read_rate")),
        stringify(r.get("reply_count")),
        stringify(r.get("reply_unique_senders")),
        format_rate(r.get("reply_rate")),
        json.dumps(r.get("reply_samples") or [], ensure_ascii=False),
        ",".join(sorted(EXCLUDED_TENANT_KEYS)),
        collected_at_text,
    ]


def write_rows_to_base(client: FeishuClient, base_token: str, rows: List[Dict[str, object]], state: Dict[str, object]) -> Dict[str, int]:
    """Upsert bulk-task rows into Base by 消息ID. Returns {created, updated}."""
    table_id = ensure_table_and_fields(
        client,
        base_token,
        BULK_TASK_TABLE_NAME,
        BULK_TASK_FIELD_DEFS,
        recreate_tables=False,
    )
    client.ensure_groupchat_field_v1(base_token, table_id, BULK_TASK_GROUPCHAT_FIELD)
    existing = client.list_existing_record_ids_v1(base_token, table_id, "消息ID")

    collected_at_text = epoch_seconds_to_text(int(time.time()), SYNC_TZ)
    update_record_ids: List[str] = []
    update_rows: List[List[object]] = []
    update_chats: List[Dict[str, str]] = []
    create_rows: List[List[object]] = []
    create_chats: List[Dict[str, str]] = []

    for r in rows:
        bitable_row = build_bulk_task_row(r, state, collected_at_text)
        msg_id = stringify(r.get("message_id"))
        chat_meta = {
            "chat_id": stringify(r.get("chat_id")),
            "chat_name": stringify(r.get("chat_name")),
        }
        if msg_id and msg_id in existing:
            update_record_ids.append(existing[msg_id])
            update_rows.append(bitable_row)
            update_chats.append(chat_meta)
        else:
            create_rows.append(bitable_row)
            create_chats.append(chat_meta)

    if update_record_ids:
        client.batch_update_records_v1(
            base_token, table_id, BULK_TASK_FIELDS, update_record_ids, update_rows
        )
        for rid, meta in zip(update_record_ids, update_chats):
            if meta["chat_id"]:
                client.batch_update_groupchat_fields_v1(
                    base_token, table_id, [rid], meta["chat_id"], meta["chat_name"]
                )

    created_n = 0
    if create_rows:
        for batch_idx, batch in enumerate(chunked(create_rows, 200)):
            chat_batch = create_chats[batch_idx * 200 : batch_idx * 200 + len(batch)]
            result = client.batch_create_records(
                base_token, table_id, BULK_TASK_FIELDS, batch
            )
            new_ids = [str(rid) for rid in (result.get("record_id_list") or [])]
            for rid, meta in zip(new_ids, chat_batch):
                if meta["chat_id"]:
                    client.batch_update_groupchat_fields_v1(
                        base_token, table_id, [rid], meta["chat_id"], meta["chat_name"]
                    )
            created_n += len(new_ids)

    return {"created": created_n, "updated": len(update_record_ids)}


def compute_message_stats(
    client: FeishuClient,
    chat_id: str,
    chat_name: str,
    msg_id: str,
    sent_at: int,
    *,
    sent_at_text: str = "",
) -> Dict[str, object]:
    """Pull read/reply/member stats for one (chat, message) pair and return a normalized row."""
    try:
        readers = list_read_users(client, msg_id)
        read_err = ""
    except FeishuAPIError as exc:
        readers = []
        read_err = str(exc)

    try:
        replies = list_replies(client, chat_id, msg_id, sent_at)
        reply_err = ""
    except FeishuAPIError as exc:
        replies = []
        reply_err = str(exc)

    try:
        members = list_all_chat_members(client, chat_id)
        member_err = ""
    except FeishuAPIError as exc:
        members = []
        member_err = str(exc)

    member_total_all = len(members)
    target_audience = [m for m in members if not is_excluded_tenant(m.get("tenant_key"))]
    target_count = len(target_audience)
    external_readers = [r for r in readers if not is_excluded_tenant(r.get("tenant_key"))]
    external_reply_senders = {
        stringify((m.get("sender") or {}).get("id"))
        for m in replies
        if not is_excluded_tenant((m.get("sender") or {}).get("tenant_key"))
    }
    external_reply_senders.discard("")
    external_replies = [
        m for m in replies if not is_excluded_tenant((m.get("sender") or {}).get("tenant_key"))
    ]

    denominator = max(target_count, 1)
    read_count = len(external_readers)
    read_rate = read_count / denominator if target_count else 0.0
    reply_count = len(external_replies)
    reply_unique = len(external_reply_senders)
    reply_rate = reply_unique / denominator if target_count else 0.0

    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "message_id": msg_id,
        "send_ok": True,
        "send_error": "",
        "sent_at_text": sent_at_text or epoch_seconds_to_text(sent_at, SYNC_TZ),
        "member_total_all": member_total_all,
        "target_audience": target_count,
        "excluded_tenant_keys": sorted(EXCLUDED_TENANT_KEYS),
        "read_count": read_count,
        "read_rate": round(read_rate, 4),
        "reply_count": reply_count,
        "reply_unique_senders": reply_unique,
        "reply_rate": round(reply_rate, 4),
        "errors": {k: v for k, v in {"read": read_err, "reply": reply_err, "member": member_err}.items() if v},
        "reply_samples": [
            {
                "sender": (m.get("sender") or {}).get("id"),
                "tenant_key": (m.get("sender") or {}).get("tenant_key"),
                "create_time": m.get("create_time"),
                "text": extract_message_text(m)[:120],
            }
            for m in external_replies[:5]
        ],
    }


def list_recent_bulk_tasks(
    client: FeishuClient,
    base_token: str,
    table_id: str,
    *,
    max_age_days: int,
) -> List[Dict[str, str]]:
    """Yield task rows (record_id, message_id, chat_id, chat_name, sent_at_text, sent_at, batch_id, title)
    that were sent within max_age_days. Skips rows without 消息ID (failed sends)."""
    cutoff_ts = int(time.time()) - max_age_days * 86400
    out: List[Dict[str, str]] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, object] = {
            "page_size": 200,
            "field_names": json.dumps(
                ["消息ID", "群ID", "群名称", "发送时间", "群发批次", "任务标题", "消息内容"],
                ensure_ascii=False,
            ),
        }
        if page_token:
            params["page_token"] = page_token
        data = client.request(
            "GET",
            f"/open-apis/bitable/v1/apps/{base_token}/tables/{table_id}/records",
            params=params,
        )
        for item in data.get("items") or []:
            fields = item.get("fields") or {}
            msg_id = _extract_text(fields.get("消息ID"))
            if not msg_id:
                continue
            sent_at_text = _extract_text(fields.get("发送时间"))
            sent_at_ts = 0
            if sent_at_text:
                try:
                    sent_at_ts = int(
                        datetime.strptime(sent_at_text, "%Y-%m-%d %H:%M:%S")
                        .replace(tzinfo=SYNC_TZ)
                        .timestamp()
                    )
                except ValueError:
                    sent_at_ts = 0
            if sent_at_ts and sent_at_ts < cutoff_ts:
                continue
            out.append(
                {
                    "record_id": str(item.get("record_id") or ""),
                    "message_id": msg_id,
                    "chat_id": _extract_text(fields.get("群ID")),
                    "chat_name": _extract_text(fields.get("群名称")),
                    "sent_at_text": sent_at_text,
                    "sent_at_ts": sent_at_ts,
                    "batch_id": _extract_text(fields.get("群发批次")),
                    "title": _extract_text(fields.get("任务标题")),
                    "text": _extract_text(fields.get("消息内容")),
                }
            )
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
        if not page_token:
            break
    return out


def cmd_refresh(args: argparse.Namespace) -> int:
    """无 state file 模式：直接读 Base 任务表，对最近 N 天的任务重新采集统计并 upsert 回去。"""
    base_url = os.environ.get("LARK_BASE_URL")
    if not base_url:
        sys.exit("LARK_BASE_URL is required")
    base_token = parse_base_token(base_url)
    client = make_client()

    table_id = ensure_table_and_fields(
        client,
        base_token,
        BULK_TASK_TABLE_NAME,
        BULK_TASK_FIELD_DEFS,
        recreate_tables=False,
    )
    client.ensure_groupchat_field_v1(base_token, table_id, BULK_TASK_GROUPCHAT_FIELD)

    tasks = list_recent_bulk_tasks(client, base_token, table_id, max_age_days=args.max_age_days)
    if not tasks:
        print(f"近 {args.max_age_days} 天内没有可刷新的任务（消息ID 非空且发送时间在窗口内）。")
        return 0

    print(f"找到 {len(tasks)} 条最近 {args.max_age_days} 天内的任务，开始刷新统计...")
    collected_at_text = epoch_seconds_to_text(int(time.time()), SYNC_TZ)

    update_record_ids: List[str] = []
    update_rows: List[List[object]] = []
    update_chats: List[Dict[str, str]] = []

    for idx, task in enumerate(tasks, 1):
        row = compute_message_stats(
            client,
            task["chat_id"],
            task["chat_name"],
            task["message_id"],
            task["sent_at_ts"],
            sent_at_text=task["sent_at_text"],
        )
        # 把 task 的 batch_id / title / text 注入 state-like 容器，供 build_bulk_task_row 用
        synth_state = {
            "batch_id": task["batch_id"],
            "title": task["title"],
            "text": task["text"],
        }
        bitable_row = build_bulk_task_row(row, synth_state, collected_at_text)
        update_record_ids.append(task["record_id"])
        update_rows.append(bitable_row)
        update_chats.append({"chat_id": task["chat_id"], "chat_name": task["chat_name"]})

        print(
            f"  [{idx}/{len(tasks)}] {task['chat_name'] or task['chat_id']}  "
            f"read={row['read_count']}/{row['target_audience']}={row['read_rate']:.1%}  "
            f"reply={row['reply_count']}"
        )

    if update_record_ids:
        for batch_idx, batch_ids in enumerate(chunked(update_record_ids, 200)):
            batch_rows = update_rows[batch_idx * 200 : batch_idx * 200 + len(batch_ids)]
            client.batch_update_records_v1(
                base_token, table_id, BULK_TASK_FIELDS, batch_ids, batch_rows
            )
        for rid, meta in zip(update_record_ids, update_chats):
            if meta["chat_id"]:
                client.batch_update_groupchat_fields_v1(
                    base_token, table_id, [rid], meta["chat_id"], meta["chat_name"]
                )
    print(f"\n完成：更新 {len(update_record_ids)} 条任务统计。")
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    if not os.path.exists(args.state_file):
        sys.exit(f"state file not found: {args.state_file}")
    with open(args.state_file, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    results: List[Dict[str, object]] = state.get("results") or []
    if not results:
        sys.exit("state file has no results")

    client = make_client()

    rows: List[Dict[str, object]] = []
    for r in results:
        chat_id = str(r.get("chat_id") or "")
        chat_name = r.get("chat_name") or ""
        if not r.get("ok"):
            rows.append({
                "chat_id": chat_id,
                "chat_name": chat_name,
                "message_id": "",
                "send_ok": False,
                "send_error": str(r.get("error") or ""),
                "sent_at_text": r.get("sent_at_text") or "",
                "member_total_all": 0,
                "target_audience": 0,
                "excluded_tenant_keys": sorted(EXCLUDED_TENANT_KEYS),
                "read_count": 0,
                "read_rate": 0.0,
                "reply_count": 0,
                "reply_unique_senders": 0,
                "reply_rate": 0.0,
                "errors": {},
                "reply_samples": [],
            })
            print(f"\n--- {chat_name or chat_id} ({chat_id}) 发送失败：{r.get('error')}")
            continue
        msg_id = str(r["message_id"])
        sent_at = int(r.get("sent_at") or 0)
        row = compute_message_stats(
            client,
            chat_id,
            chat_name,
            msg_id,
            sent_at,
            sent_at_text=r.get("sent_at_text") or "",
        )
        rows.append(row)

        print(f"\n--- {chat_name or chat_id} ({chat_id})")
        print(f"  msg_id           : {msg_id}")
        print(f"  群人数            : {row['member_total_all']}（排除 {row['member_total_all'] - row['target_audience']} 个内部 tenant）")
        print(f"  目标受众          : {row['target_audience']}")
        print(f"  read   {row['read_count']}  rate={row['read_rate']:.1%}")
        print(f"  reply  {row['reply_count']} 条 / {row['reply_unique_senders']} 人  rate={row['reply_rate']:.1%}")
        if row["errors"]:
            print(f"  errors: {row['errors']}")
        if row["reply_samples"]:
            print(f"  样例回复:")
            for s in row["reply_samples"]:
                print(f"    - {s['sender']}: {s['text']}")

    print(f"\n=== 汇总（已排除 tenant_key: {sorted(EXCLUDED_TENANT_KEYS)}）===")
    total_audience = sum(r["target_audience"] for r in rows)
    total_read = sum(r["read_count"] for r in rows)
    total_reply = sum(r["reply_count"] for r in rows)
    total_reply_users = sum(r["reply_unique_senders"] for r in rows)
    print(f"  发送 {len(rows)} 群")
    if total_audience:
        print(f"  目标受众 {total_audience} 人  已读 {total_read}  整体已读率 {total_read/total_audience:.1%}")
        print(f"  回复 {total_reply} 条 / 去重 {total_reply_users} 人  整体回复率 {total_reply_users/total_audience:.1%}")
    else:
        print("  没有外部受众，无法计算比率")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump({"collected_at": int(time.time()), "rows": rows}, fh, ensure_ascii=False, indent=2)
        print(f"\n结果已写入 {args.output}")

    if args.write_to_base:
        base_url = os.environ.get("LARK_BASE_URL")
        if not base_url:
            print("\n--write-to-base 跳过：LARK_BASE_URL 未设置", file=sys.stderr)
        else:
            base_token = parse_base_token(base_url)
            print(f"\n写入 Base「{BULK_TASK_TABLE_NAME}」...", file=sys.stderr)
            stats = write_rows_to_base(client, base_token, rows, state)
            print(f"  新增 {stats['created']} 条，更新 {stats['updated']} 条。")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk message + analytics probe")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_send = sub.add_parser("send", help="发送消息并记录 message_id")
    p_send.add_argument("--text", required=True)
    p_send.add_argument("--title", help="任务标题（默认取 text 前 30 字）")
    p_send.add_argument("--chat-ids", help="逗号分隔的 chat_id 列表")
    p_send.add_argument("--from-base", action="store_true", help="从 Base 机器人群列表 表前 N 个群读取")
    p_send.add_argument("--limit", type=int, default=2)
    p_send.add_argument("-y", "--yes", action="store_true", help="跳过确认")
    p_send.set_defaults(func=cmd_send)

    p_collect = sub.add_parser("collect", help="回收已读 + 回复数据")
    p_collect.add_argument("--output", help="额外把汇总结果写入 JSON 文件")
    p_collect.add_argument(
        "--write-to-base",
        action="store_true",
        help=f"upsert 到 Base「{BULK_TASK_TABLE_NAME}」表（按 消息ID 主键）。需要 LARK_BASE_URL 环境变量。",
    )
    p_collect.set_defaults(func=cmd_collect)

    p_refresh = sub.add_parser(
        "refresh",
        help=f"从 Base「{BULK_TASK_TABLE_NAME}」表读最近 N 天的任务，重新统计并写回（无需 state file，给 CI 用）。",
    )
    p_refresh.add_argument(
        "--max-age-days",
        type=int,
        default=7,
        help="只刷新发送时间在这么多天内的任务（默认 7）",
    )
    p_refresh.set_defaults(func=cmd_refresh)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except FeishuAPIError as exc:
        print(f"飞书 API 失败: {exc}", file=sys.stderr)
        sys.exit(1)
