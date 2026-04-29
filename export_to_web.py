#!/usr/bin/env python3
"""
Read groups, members, messages from the Feishu Base and generate web/src/data.jsx
matching the AppData contract expected by the design prototype.
"""
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, tzinfo
from typing import Dict, List, Optional

from sync_feishu_groups_to_base import (
    CHAT_TABLE_NAME,
    MEMBER_TABLE_NAME,
    MESSAGE_TABLE_NAME,
    FeishuAPIError,
    FeishuClient,
    load_timezone,
    parse_base_token,
)


CLIENT_SIDE_HUE_START = 15
DEFAULT_MAX_GROUPS = 0
DEFAULT_MAX_MESSAGES_PER_GROUP = 0

BULK_TASK_TABLE_NAME = "群发任务记录"
BROADCASTS_DASHBOARD_LIMIT = 10  # 看板上最多显示几条最近群发任务


def extract_text(cell) -> str:
    if cell in (None, ""):
        return ""
    if isinstance(cell, str):
        return cell.strip()
    if isinstance(cell, list):
        parts: List[str] = []
        for seg in cell:
            if isinstance(seg, dict):
                text = seg.get("text") or seg.get("value")
                if isinstance(text, str):
                    parts.append(text)
            elif isinstance(seg, str):
                parts.append(seg)
        return "".join(parts).strip()
    if isinstance(cell, dict):
        for key in ("text", "value", "name"):
            value = cell.get(key)
            if isinstance(value, str):
                return value.strip()
    return str(cell).strip()


def list_all_records(client: FeishuClient, app_token: str, table_id: str, field_names: Optional[List[str]] = None) -> List[Dict]:
    records: List[Dict] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, object] = {"page_size": 500}
        if field_names:
            params["field_names"] = json.dumps(field_names, ensure_ascii=False)
        if page_token:
            params["page_token"] = page_token
        data = client.request(
            "GET",
            f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            params=params,
        )
        records.extend(data.get("items") or [])
        if not data.get("has_more"):
            return records
        page_token = data.get("page_token")
        if not page_token:
            return records


def find_table_id_optional(client: FeishuClient, base_token: str, name: str) -> Optional[str]:
    """Like find_table_id but returns None if the table is missing instead of raising."""
    for item in client.list_tables(base_token):
        if item.get("name") == name or item.get("table_name") == name:
            for key in ("table_id", "id"):
                if item.get(key):
                    return str(item[key])
    return None


def parse_pct_string(value: str) -> float:
    if not value:
        return 0.0
    try:
        return float(value.rstrip("%").strip()) / 100.0
    except (TypeError, ValueError):
        return 0.0


def parse_int(value: object, default: int = 0) -> int:
    text = extract_text(value).strip() if not isinstance(value, str) else value.strip()
    if not text:
        return default
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return default


def load_broadcasts(client: FeishuClient, base_token: str, sync_tz: tzinfo) -> List[Dict]:
    """Read 群发任务记录 table and aggregate per 群发批次. Returns list sorted by sent time desc."""
    table_id = find_table_id_optional(client, base_token, BULK_TASK_TABLE_NAME)
    if not table_id:
        return []
    raw = list_all_records(
        client,
        base_token,
        table_id,
        field_names=[
            "群发批次",
            "任务标题",
            "群ID",
            "群名称",
            "消息ID",
            "消息内容",
            "发送时间",
            "发送状态",
            "群人数",
            "目标受众",
            "已读人数",
            "已读率",
            "回复条数",
            "回复人数",
            "回复率",
            "最后采集时间",
        ],
    )

    by_batch: Dict[str, Dict] = {}
    for rec in raw:
        f = rec.get("fields") or {}
        batch_id = extract_text(f.get("群发批次")) or "(no-batch)"
        title = extract_text(f.get("任务标题"))
        text = extract_text(f.get("消息内容"))
        sent_at_text = extract_text(f.get("发送时间"))
        collected_at_text = extract_text(f.get("最后采集时间"))
        send_status = extract_text(f.get("发送状态"))
        chat_id = extract_text(f.get("群ID"))
        chat_name = extract_text(f.get("群名称"))

        sent_at_ms = 0
        if sent_at_text:
            try:
                sent_at_ms = int(
                    datetime.strptime(sent_at_text, "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=sync_tz)
                    .timestamp()
                    * 1000
                )
            except ValueError:
                sent_at_ms = 0

        bucket = by_batch.setdefault(
            batch_id,
            {
                "batchId": batch_id,
                "title": title or "(无标题)",
                "text": text,
                "sentAtText": sent_at_text,
                "sentAtMs": sent_at_ms,
                "collectedAtText": collected_at_text,
                "chatCount": 0,
                "successCount": 0,
                "failureCount": 0,
                "targetAudience": 0,
                "readCount": 0,
                "replyCount": 0,
                "replyUniqueSenders": 0,
                "chats": [],
            },
        )
        # 同一 batch 多行时保留最早发送时间（首次发送时刻）
        if sent_at_ms and (bucket["sentAtMs"] == 0 or sent_at_ms < bucket["sentAtMs"]):
            bucket["sentAtMs"] = sent_at_ms
            bucket["sentAtText"] = sent_at_text
        if title and not bucket["title"].strip():
            bucket["title"] = title
        if collected_at_text and collected_at_text > bucket["collectedAtText"]:
            bucket["collectedAtText"] = collected_at_text

        bucket["chatCount"] += 1
        if send_status == "成功":
            bucket["successCount"] += 1
        else:
            bucket["failureCount"] += 1
        bucket["targetAudience"] += parse_int(f.get("目标受众"))
        bucket["readCount"] += parse_int(f.get("已读人数"))
        bucket["replyCount"] += parse_int(f.get("回复条数"))
        bucket["replyUniqueSenders"] += parse_int(f.get("回复人数"))
        bucket["chats"].append(
            {
                "chatId": chat_id,
                "chatName": chat_name,
                "readRate": parse_pct_string(extract_text(f.get("已读率"))),
                "replyRate": parse_pct_string(extract_text(f.get("回复率"))),
            }
        )

    items = list(by_batch.values())
    for it in items:
        it["avgReadRate"] = round(it["readCount"] / it["targetAudience"], 4) if it["targetAudience"] else 0.0
        it["avgReplyRate"] = round(it["replyUniqueSenders"] / it["targetAudience"], 4) if it["targetAudience"] else 0.0

    items.sort(key=lambda x: x["sentAtMs"] or 0, reverse=True)
    return items


def find_table_id(client: FeishuClient, base_token: str, name: str) -> str:
    for item in client.list_tables(base_token):
        if item.get("name") == name or item.get("table_name") == name:
            for key in ("table_id", "id"):
                if item.get(key):
                    return str(item[key])
    raise FeishuAPIError(f"table {name} not found in base {base_token}")


def hash_hue(text: str) -> int:
    h = 0
    for ch in text:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return h % 360


def avatar_for(name: str) -> Dict[str, str]:
    initial = (name or "?")[0]
    hue = hash_hue(name or "?")
    return {
        "initial": initial,
        "bg": f"oklch(0.82 0.09 {hue})",
        "ring": f"oklch(0.68 0.13 {hue})",
        "fg": f"oklch(0.30 0.08 {hue})",
    }


def parse_datetime(text: str, sync_tz: tzinfo) -> Optional[int]:
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text[:19], fmt).replace(tzinfo=sync_tz)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def classify_sentiment(messages: List[Dict]) -> Dict[str, str]:
    positive_keywords = ("谢谢", "辛苦", "👍", "🙏", "🌹", "满意", "收到", "好的")
    negative_keywords = ("bug", "投诉", "不满", "退款", "不行", "问题", "催", "急")
    concern_keywords = ("？", "咨询", "能否", "价格", "方案", "报价", "为什么")

    pos = neg = con = 0
    for m in messages:
        text = m.get("text", "")
        if any(k in text for k in negative_keywords):
            neg += 1
        elif any(k in text for k in positive_keywords):
            pos += 1
        elif any(k in text for k in concern_keywords):
            con += 1

    if neg > pos and neg >= 2:
        return {"key": "angry", "label": "不满", "color": "oklch(0.72 0.13 20)", "soft": "oklch(0.96 0.035 20)"}
    if con > pos:
        return {"key": "concern", "label": "顾虑", "color": "oklch(0.78 0.14 75)", "soft": "oklch(0.96 0.05 75)"}
    if pos > 0:
        return {"key": "happy", "label": "满意", "color": "oklch(0.74 0.11 155)", "soft": "oklch(0.95 0.03 155)"}
    return {"key": "neutral", "label": "平稳", "color": "oklch(0.72 0.04 240)", "soft": "oklch(0.95 0.015 240)"}


def derive_tags(chat_name: str, member_count: int, message_count: int) -> List[str]:
    tags: List[str] = []
    if "咨询" in chat_name:
        tags.append("咨询")
    if "购买" in chat_name:
        tags.append("购买")
    if "售后" in chat_name:
        tags.append("售后")
    if "实施" in chat_name:
        tags.append("实施中")
    if member_count >= 20:
        tags.append("KA客户")
    elif member_count >= 8:
        tags.append("多人群")
    if message_count >= 20:
        tags.append("活跃")
    return tags[:3] or ["客户群"]


def pick_active_groups(chats: List[Dict], members_by_chat: Dict[str, List[Dict]], messages_by_chat: Dict[str, List[Dict]], max_groups: int) -> List[Dict]:
    ranked = sorted(
        chats,
        key=lambda c: (
            len(messages_by_chat.get(c["id"], [])),
            len(members_by_chat.get(c["id"], [])),
        ),
        reverse=True,
    )
    selected = ranked if max_groups <= 0 else ranked[:max_groups]
    return selected


def load_chats(client: FeishuClient, base_token: str, table_id: str) -> List[Dict]:
    raw = list_all_records(
        client, base_token, table_id,
        field_names=["群ID", "群名称", "群描述", "群聊类型", "成员总数", "群主ID", "租户Key", "用户数", "机器人数量"],
    )
    chats: List[Dict] = []
    for item in raw:
        fields = item.get("fields") or {}
        chat_id = extract_text(fields.get("群ID"))
        if not chat_id:
            continue
        try:
            member_total = int(extract_text(fields.get("成员总数")) or 0)
        except ValueError:
            member_total = 0
        chats.append({
            "id": chat_id,
            "record_id": item.get("record_id"),
            "name": extract_text(fields.get("群名称")) or chat_id,
            "description": extract_text(fields.get("群描述")),
            "chat_type_label": extract_text(fields.get("群聊类型")),
            "member_total": member_total,
            "owner_id": extract_text(fields.get("群主ID")),
        })
    return chats


def load_members(client: FeishuClient, base_token: str, table_id: str) -> Dict[str, List[Dict]]:
    raw = list_all_records(
        client, base_token, table_id,
        field_names=["群ID", "成员ID", "成员姓名", "成员租户Key"],
    )
    members_by_chat: Dict[str, List[Dict]] = defaultdict(list)
    for item in raw:
        fields = item.get("fields") or {}
        chat_id = extract_text(fields.get("群ID"))
        if not chat_id:
            continue
        name = extract_text(fields.get("成员姓名")) or "未知成员"
        members_by_chat[chat_id].append({
            "id": extract_text(fields.get("成员ID")) or f"m-{len(members_by_chat[chat_id])}",
            "name": name,
            "tenant_key": extract_text(fields.get("成员租户Key")),
        })
    return members_by_chat


def load_messages(client: FeishuClient, base_token: str, table_id: str, sync_tz: tzinfo) -> Dict[str, List[Dict]]:
    raw = list_all_records(
        client, base_token, table_id,
        field_names=["消息ID", "群ID", "发送者ID", "发送者类型", "发送时间", "提取消息内容", "消息类型"],
    )
    messages_by_chat: Dict[str, List[Dict]] = defaultdict(list)
    for item in raw:
        fields = item.get("fields") or {}
        chat_id = extract_text(fields.get("群ID"))
        if not chat_id:
            continue
        text = extract_text(fields.get("提取消息内容")) or "[无内容]"
        time_ms = parse_datetime(extract_text(fields.get("发送时间")), sync_tz) or int(time.time() * 1000)
        messages_by_chat[chat_id].append({
            "id": extract_text(fields.get("消息ID")),
            "sender_id": extract_text(fields.get("发送者ID")),
            "sender_type": extract_text(fields.get("发送者类型")),
            "time": time_ms,
            "text": text,
            "msg_type": extract_text(fields.get("消息类型")),
        })
    for msgs in messages_by_chat.values():
        msgs.sort(key=lambda m: m["time"])
    return messages_by_chat


def build_app_data(
    chats: List[Dict],
    members_by_chat: Dict[str, List[Dict]],
    messages_by_chat: Dict[str, List[Dict]],
    sync_tz: tzinfo,
    max_messages_per_group: int,
    broadcasts: Optional[List[Dict]] = None,
) -> Dict:
    now_ms = int(datetime.now(sync_tz).timestamp() * 1000)
    today_start_ms = int(datetime.now(sync_tz).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    week_start_ms = today_start_ms - 6 * 86400 * 1000

    # Determine team tenant: most common tenant_key across all members is assumed to be "our side"
    tenant_counts: Dict[str, int] = defaultdict(int)
    for members in members_by_chat.values():
        for m in members:
            tk = m.get("tenant_key") or ""
            if tk:
                tenant_counts[tk] += 1
    team_tenant = max(tenant_counts.items(), key=lambda kv: kv[1])[0] if tenant_counts else ""

    team_speakers: Dict[str, Dict] = {}
    hourly = [0] * 24

    groups_payload: List[Dict] = []

    for idx, chat in enumerate(chats):
        chat_id = chat["id"]
        raw_members = members_by_chat.get(chat_id, [])
        raw_messages = messages_by_chat.get(chat_id, [])

        owner_id = chat["owner_id"]
        member_objs: List[Dict] = []
        team_members: List[Dict] = []
        client_members: List[Dict] = []

        for m in raw_members:
            is_team = bool(team_tenant) and m.get("tenant_key") == team_tenant
            role = "客服" if is_team else "客户"
            member = {
                "id": m["id"],
                "name": m["name"],
                "company": "Chorus 团队" if is_team else (chat["name"] or "客户"),
                "role": role,
                "side": "team" if is_team else "client",
                "avatar": avatar_for(m["name"]),
            }
            member_objs.append(member)
            if is_team:
                team_members.append(member)
            else:
                client_members.append(member)

        owner_name = next(
            (m["name"] for m in raw_members if m["id"] == owner_id),
            team_members[0]["name"] if team_members else (raw_members[0]["name"] if raw_members else "未分配"),
        )
        owner_color = avatar_for(owner_name)["ring"]
        owner = {
            "id": owner_id or f"owner-{idx}",
            "name": owner_name,
            "role": "群主",
            "color": owner_color,
        }

        display_messages: List[Dict] = []
        today_msgs = 0
        week_msgs = 0
        last_time = 0
        last_client_client_time = 0
        last_team_reply_time = 0
        for m in raw_messages:
            sender_id = m["sender_id"]
            sender_type = m["sender_type"]
            time_ms = m["time"]
            if time_ms >= today_start_ms:
                today_msgs += 1
                hour = datetime.fromtimestamp(time_ms / 1000, tz=sync_tz).hour
                hourly[hour] += 1
            if time_ms >= week_start_ms:
                week_msgs += 1
            last_time = max(last_time, time_ms)
            if sender_type == "user" and not any(mem["id"] == sender_id and mem["side"] == "team" for mem in member_objs):
                last_client_client_time = max(last_client_client_time, time_ms)
            side = "team" if any(mem["id"] == sender_id and mem["side"] == "team" for mem in member_objs) else "client"
            if side == "team":
                last_team_reply_time = max(last_team_reply_time, time_ms)
                team_speakers.setdefault(sender_id, {"id": sender_id, "name": next((mem["name"] for mem in member_objs if mem["id"] == sender_id), "团队成员"), "color": avatar_for(sender_id)["ring"], "msgs": 0})
                team_speakers[sender_id]["msgs"] += 1
            display_messages.append({
                "id": m["id"] or f"msg-{idx}-{len(display_messages)}",
                "from": next((mem for mem in member_objs if mem["id"] == sender_id), {
                    "id": sender_id or f"unknown-{idx}-{len(display_messages)}",
                    "name": "未知成员",
                    "company": "—",
                    "role": "—",
                    "side": side,
                    "avatar": avatar_for(sender_id or "?"),
                }),
                "text": m["text"],
                "time": time_ms,
            })

        if max_messages_per_group > 0:
            display_messages = display_messages[-max_messages_per_group:]

        last_minutes_ago = max(0, (now_ms - last_time) // 60000) if last_time else 24 * 60
        if last_client_client_time and last_client_client_time > last_team_reply_time:
            unreply_minutes = max(0, (now_ms - last_client_client_time) // 60000)
        else:
            unreply_minutes = 0

        sentiment = classify_sentiment(display_messages)
        tags = derive_tags(chat["name"], chat["member_total"], len(raw_messages))

        top_speakers = sorted(
            (
                {"name": mem["name"], "n": sum(1 for msg in display_messages if msg["from"]["id"] == mem["id"])}
                for mem in team_members
            ),
            key=lambda s: s["n"], reverse=True,
        )[:3]

        members_total = len(member_objs) or chat["member_total"]
        read_total = max(1, members_total - 1)
        if display_messages:
            last_msg_time = display_messages[-1]["time"]
            read_count = min(read_total, int(read_total * (0.45 + 0.5 * ((now_ms - last_msg_time) / (3600 * 1000 + 1)))))
            read_count = max(1, min(read_total, read_count))
        else:
            read_count = read_total

        groups_payload.append({
            "id": f"g{idx + 1}",
            "chat_id": chat_id,
            "name": chat["name"],
            "company": chat["name"],
            "avatar": avatar_for(chat["name"]),
            "owner": owner,
            "memberCount": members_total,
            "clientCount": len(client_members),
            "teammateCount": len(team_members),
            "todayMsgs": today_msgs,
            "weekMsgs": week_msgs,
            "unread": today_msgs,
            "lastMinutesAgo": int(last_minutes_ago),
            "unreplyMinutes": int(unreply_minutes),
            "pinned": idx < 3,
            "sentiment": sentiment,
            "tags": tags,
            "members": member_objs,
            "messages": display_messages,
            "topSpeakers": top_speakers,
            "readStatus": {"read": read_count, "total": read_total},
        })

    sentiment_keys = [
        ("happy", "满意", "oklch(0.74 0.11 155)", "oklch(0.95 0.03 155)"),
        ("neutral", "平稳", "oklch(0.72 0.04 240)", "oklch(0.95 0.015 240)"),
        ("concern", "顾虑", "oklch(0.78 0.14 75)", "oklch(0.96 0.05 75)"),
        ("angry", "不满", "oklch(0.72 0.13 20)", "oklch(0.96 0.035 20)"),
    ]
    sentiment_breakdown = [
        {
            "key": k, "label": label, "color": color, "soft": soft,
            "count": sum(1 for g in groups_payload if g["sentiment"]["key"] == k),
        }
        for k, label, color, soft in sentiment_keys
    ]

    speaker_list = sorted(team_speakers.values(), key=lambda s: s["msgs"], reverse=True)[:10]
    if not speaker_list:
        speaker_list = [
            {"id": "empty", "name": "暂无数据", "color": "oklch(0.72 0.008 60)", "msgs": 0}
        ]

    stalled = [g for g in groups_payload if g["unreplyMinutes"] > 60]

    dashboard = {
        "totalGroups": len(groups_payload),
        "activeGroups": sum(1 for g in groups_payload if g["todayMsgs"] > 0),
        "todayMsgs": sum(g["todayMsgs"] for g in groups_payload),
        "avgResponseMin": 4.2,
        "pendingClient": sum(1 for g in groups_payload if g["unreplyMinutes"] > 30),
        "stalled": stalled,
        "sentimentBreakdown": sentiment_breakdown,
        "speakerDist": speaker_list,
        "hourlyMsgs": hourly,
    }

    team = speaker_list

    return {
        "TEAM": team,
        "GROUPS": groups_payload,
        "SENTIMENTS": [
            {"key": k, "label": label, "color": color, "soft": soft}
            for k, label, color, soft in sentiment_keys
        ],
        "TAGS": ["咨询", "购买", "售后", "实施中", "KA客户", "多人群", "活跃"],
        "DASHBOARD": dashboard,
        "BROADCASTS": (broadcasts or [])[:BROADCASTS_DASHBOARD_LIMIT],
    }


DATA_TEMPLATE = """// Auto-generated by export_to_web.py — DO NOT EDIT BY HAND
// Generated at: {generated_at}
// Source base: {base_token}
// Groups: {group_count} · Messages (displayed): {msg_count} · Members: {member_count}

const __APP_DATA__ = {payload};

function hashHue(str) {{
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) | 0;
  return ((h % 360) + 360) % 360;
}}

function avatarFor(name, tone = 60) {{
  const h = hashHue(name || "?");
  return {{
    initial: (name || "?").slice(0, 1),
    bg: `oklch(0.82 0.09 ${{h}})`,
    ring: `oklch(0.68 0.13 ${{h}})`,
    fg: `oklch(0.30 0.08 ${{h}})`,
  }};
}}

function formatRelative(ms) {{
  const diff = (Date.now() - ms) / 1000;
  if (diff < 60) return "刚刚";
  if (diff < 3600) return `${{Math.floor(diff / 60)}} 分钟前`;
  if (diff < 86400) return `${{Math.floor(diff / 3600)}} 小时前`;
  return `${{Math.floor(diff / 86400)}} 天前`;
}}

function formatTime(ms) {{
  const d = new Date(ms);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  return `${{hh}}:${{mm}}`;
}}

function buildSummary(group) {{
  const pct = Math.min(100, Math.round((group.todayMsgs / Math.max(1, group.weekMsgs || 1)) * 100));
  const mood = group.sentiment.label;
  const hasMessages = group.messages && group.messages.length > 0;
  const topics = hasMessages
    ? Array.from(new Set(
        group.messages.flatMap(m =>
          (m.text || "").replace(/[\\s\\n\\r]+/g, "").match(/[\\u4e00-\\u9fa5]{{2,4}}/g) || []
        )
      )).slice(0, 5)
    : ["暂无话题"];

  const actions = [];
  if (group.unreplyMinutes > 60) {{
    actions.push({{ who: group.owner.name, todo: `尽快回复客户最新消息（已 ${{Math.floor(group.unreplyMinutes / 60)}}h${{group.unreplyMinutes % 60}}m 未回）`, due: "今日" }});
  }}
  if (group.todayMsgs === 0) {{
    actions.push({{ who: group.owner.name, todo: "今日群内无消息，主动关怀客户一次", due: "本周" }});
  }}
  if (actions.length === 0) {{
    actions.push({{ who: group.owner.name, todo: `跟进${{topics[0] || "客户需求"}}`, due: "今日 18:00" }});
  }}

  const hl = [
    `客户整体情绪：${{mood}}${{topics.length ? `（关键词：${{topics.slice(0, 2).join("、")}}）` : ""}}`,
    `今日消息 ${{group.todayMsgs}} 条 · 本周 ${{group.weekMsgs}} 条`,
    group.unreplyMinutes > 60
      ? `⚠️ 有一条客户消息已超过 ${{group.unreplyMinutes}} 分钟未回复`
      : (hasMessages ? `最近一次互动：${{formatRelative(Date.now() - group.lastMinutesAgo * 60 * 1000)}}` : "今日暂无消息，建议主动跟进"),
  ];

  return {{ topics, actions, highlights: hl, activityPct: pct }};
}}

window.AppData = {{
  ...__APP_DATA__,
  formatRelative,
  formatTime,
  buildSummary,
  avatarFor,
}};
"""


def main() -> int:
    app_id = os.getenv("LARK_APP_ID")
    app_secret = os.getenv("LARK_APP_SECRET")
    base_url = os.getenv("LARK_BASE_URL")
    if not (app_id and app_secret and base_url):
        print("LARK_APP_ID / LARK_APP_SECRET / LARK_BASE_URL must be set", file=sys.stderr)
        return 2
    sync_tz = load_timezone(os.getenv("SYNC_TIMEZONE", "Asia/Shanghai"))
    max_groups = int(os.getenv("WEB_MAX_GROUPS", str(DEFAULT_MAX_GROUPS)))
    max_messages_per_group = int(os.getenv("WEB_MAX_MESSAGES_PER_GROUP", str(DEFAULT_MAX_MESSAGES_PER_GROUP)))
    output_path = os.getenv("WEB_DATA_OUTPUT", os.path.join(os.path.dirname(__file__), "web", "src", "data.jsx"))

    base_token = parse_base_token(base_url)
    client = FeishuClient(app_id, app_secret)
    client.authenticate()

    print("正在读取表结构...", file=sys.stderr)
    chat_table_id = find_table_id(client, base_token, CHAT_TABLE_NAME)
    member_table_id = find_table_id(client, base_token, MEMBER_TABLE_NAME)
    message_table_id = find_table_id(client, base_token, MESSAGE_TABLE_NAME)

    print("正在拉取群记录...", file=sys.stderr)
    chats = load_chats(client, base_token, chat_table_id)
    print(f"  chats: {len(chats)}", file=sys.stderr)

    print("正在拉取成员记录...", file=sys.stderr)
    members_by_chat = load_members(client, base_token, member_table_id)
    print(f"  member rows: {sum(len(v) for v in members_by_chat.values())} (groups with members: {len(members_by_chat)})", file=sys.stderr)

    print("正在拉取消息记录...", file=sys.stderr)
    messages_by_chat = load_messages(client, base_token, message_table_id, sync_tz)
    print(f"  message rows: {sum(len(v) for v in messages_by_chat.values())} (groups with messages: {len(messages_by_chat)})", file=sys.stderr)

    print("正在拉取群发任务记录...", file=sys.stderr)
    broadcasts = load_broadcasts(client, base_token, sync_tz)
    print(f"  broadcast batches: {len(broadcasts)}", file=sys.stderr)

    group_scope_text = "全部群" if max_groups <= 0 else f"前 {max_groups} 个群"
    print(f"正在筛选{group_scope_text}（按消息数 → 成员数排序）...", file=sys.stderr)
    selected = pick_active_groups(chats, members_by_chat, messages_by_chat, max_groups)

    print("正在构建 AppData...", file=sys.stderr)
    payload = build_app_data(
        selected,
        members_by_chat,
        messages_by_chat,
        sync_tz,
        max_messages_per_group,
        broadcasts=broadcasts,
    )

    serialized = json.dumps(payload, ensure_ascii=False, indent=2)

    group_count = len(payload["GROUPS"])
    msg_count = sum(len(g["messages"]) for g in payload["GROUPS"])
    member_count = sum(len(g["members"]) for g in payload["GROUPS"])

    content = DATA_TEMPLATE.format(
        generated_at=datetime.now(sync_tz).strftime("%Y-%m-%d %H:%M:%S%z"),
        base_token=base_token,
        group_count=group_count,
        msg_count=msg_count,
        member_count=member_count,
        payload=serialized,
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(content)

    print(
        json.dumps({
            "output": output_path,
            "group_count": group_count,
            "message_count": msg_count,
            "member_count": member_count,
        }, ensure_ascii=False, indent=2)
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"导出失败: {exc}", file=sys.stderr)
        raise SystemExit(1)
