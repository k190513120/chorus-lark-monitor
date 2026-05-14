#!/usr/bin/env python3
"""一次性回填脚本：修复 chats / members / messages 三张表的 user 类型字段。

之前 _build_*_row_minimal 在 user 类型字段填 None，导致新 Base 里：
- 群主（chat 表）
- 成员（member 表）
- 发送者（message 表）
三个字段都是空的。本脚本扫所有行，对 user 字段为空但 ID 字段有值的，
batch_update 把 user 字段从 ID 重建为 [{"id": open_id}] 格式。

幂等：已有 user 值的行跳过。可重跑。

Usage:
    set -a; source .env; set +a
    .venv/bin/python scripts/backfill_user_fields.py        # 默认两个 Base 都跑
    .venv/bin/python scripts/backfill_user_fields.py --primary-only
    .venv/bin/python scripts/backfill_user_fields.py --secondary-only
    .venv/bin/python scripts/backfill_user_fields.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

LARK_OPEN = "https://open.feishu.cn"

# 两个 Base 的 table_id 映射
BASES = {
    "primary": {
        "name": "新 Base (G42ybVmN... 现 primary)",
        "app": "G42ybVmN9aAeYdsHW06cysTonmF",
        "chat_table": "tblXhxEs8Y5IvFbw",
        "member_table": "tbl7aSVqPLBk1iRv",
        "message_table": "tblK0WYR1ebTarjR",
    },
    "secondary": {
        "name": "旧 Base (PnRtbGm... 现 secondary)",
        "app": "PnRtbGmTpaVXwDsWBWPcPaEpnwh",
        "chat_table": "tbl7PZ9s9yoKSHtJ",
        "member_table": "tblODVD4U82fn21P",
        "message_table": "tbl9oNBOQrekT1O4",
    },
}

# 每张表要回填的 user 字段：(显示名, ID 字段, ID 类型字段或固定值)
TABLE_USER_SPECS = {
    "chat": {
        "user_field": "群主",
        "id_field": "群主ID",
        "id_type_value": "open_id",     # chat 表 owner 没有 id_type 字段，固定 open_id
    },
    "member": {
        "user_field": "成员",
        "id_field": "成员ID",
        "id_type_field": "成员ID类型",  # 必须是 open_id
    },
    "message": {
        "user_field": "发送者",
        "id_field": "发送者ID",
        "id_type_field": "发送者类型",  # 必须是 open_id
    },
}


def get_token() -> str:
    req = urllib.request.Request(
        f"{LARK_OPEN}/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({
            "app_id": os.environ["LARK_APP_ID"],
            "app_secret": os.environ["LARK_APP_SECRET"],
        }).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["tenant_access_token"]


def api(token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{LARK_OPEN}{path}",
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    last_err = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="ignore")
            last_err = f"HTTP {e.code}: {body_text[:300]}"
            # 限频 retry
            if e.code in (429, 500, 502, 503, 504):
                sleep_s = 0.5 * (2 ** attempt)
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"{method} {path}: {last_err}")
        except Exception as e:  # noqa: BLE001
            last_err = str(e)
            time.sleep(1.0)
            continue
    raise RuntimeError(f"{method} {path} after retries: {last_err}")


def text_value(v) -> str:
    """Base 文本字段值通常是 list[dict]，提取纯文本。"""
    if isinstance(v, str):
        return v
    if isinstance(v, list) and v:
        seg = v[0]
        if isinstance(seg, dict):
            return str(seg.get("text") or seg.get("value") or "")
    return str(v) if v else ""


def list_all_records(token: str, app: str, table: str, page_size: int = 500) -> list:
    """分页列出表所有行。"""
    out: list = []
    page_token = ""
    while True:
        path = f"/open-apis/bitable/v1/apps/{app}/tables/{table}/records?page_size={page_size}"
        if page_token:
            path += f"&page_token={urllib.parse.quote(page_token)}"
        d = api(token, "GET", path)
        if int(d.get("code", -1)) != 0:
            raise RuntimeError(f"list failed: {d}")
        data = d.get("data", {})
        items = data.get("items", [])
        out.extend(items)
        if not data.get("has_more"):
            break
        page_token = data.get("page_token", "")
        if not page_token:
            break
    return out


def batch_update(token: str, app: str, table: str, records: list) -> int:
    """records: [{"record_id": ..., "fields": {...}}, ...]，每批 ≤500."""
    if not records:
        return 0
    d = api(token, "POST",
            f"/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_update",
            body={"records": records})
    if int(d.get("code", -1)) != 0:
        raise RuntimeError(f"batch_update failed: {d}")
    return len(d.get("data", {}).get("records", []))


def backfill_table(token: str, base_label: str, app: str, table: str, spec: dict, *,
                   dry_run: bool, batch_size: int = 200) -> dict:
    """对单张表回填 user 字段。"""
    user_field = spec["user_field"]
    id_field = spec["id_field"]
    id_type_field = spec.get("id_type_field")
    id_type_value = spec.get("id_type_value")

    print(f"\n=== {base_label} / {user_field} (table={table}) ===")
    print(f"    listing all records...")
    records = list_all_records(token, app, table)
    print(f"    found {len(records)} records")

    to_update: list = []
    skip_has_user = 0
    skip_no_id = 0
    skip_wrong_id_type = 0

    for r in records:
        f = r.get("fields", {})
        # 已经有 user 值就跳过
        if f.get(user_field):
            skip_has_user += 1
            continue
        # 提取 ID
        open_id = text_value(f.get(id_field))
        if not open_id or not open_id.startswith("ou_"):
            skip_no_id += 1
            continue
        # 验证 ID 类型
        if id_type_field:
            id_type = text_value(f.get(id_type_field))
            if id_type and id_type != "open_id":
                skip_wrong_id_type += 1
                continue
        elif id_type_value and id_type_value != "open_id":
            skip_wrong_id_type += 1
            continue
        to_update.append({
            "record_id": r["record_id"],
            "fields": {user_field: [{"id": open_id}]},
        })

    print(f"    skip(已有 user): {skip_has_user}  skip(无 ID): {skip_no_id}  skip(非 open_id): {skip_wrong_id_type}")
    print(f"    待 update: {len(to_update)} 条")

    if dry_run:
        print(f"    [DRY-RUN] 不实际写。前 3 条预览:")
        for u in to_update[:3]:
            print(f"      {u}")
        return {"total": len(records), "updated": 0, "pending_update": len(to_update)}

    updated = 0
    for start in range(0, len(to_update), batch_size):
        chunk = to_update[start : start + batch_size]
        for attempt in range(3):
            try:
                n = batch_update(token, app, table, chunk)
                updated += n
                break
            except RuntimeError as e:
                # 限频时退避
                msg = str(e)
                if "limited" in msg or "429" in msg or "800004135" in msg:
                    sleep_s = 1.0 * (2 ** attempt)
                    print(f"    rate-limited, sleep {sleep_s}s and retry...")
                    time.sleep(sleep_s)
                    continue
                print(f"    batch failed @ {start}: {msg[:200]}")
                break
        print(f"    progress: {min(start + batch_size, len(to_update))}/{len(to_update)} updated")
        time.sleep(0.3)  # 给 Base API 喘口气

    return {"total": len(records), "updated": updated, "pending_update": len(to_update)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary-only", action="store_true")
    ap.add_argument("--secondary-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-messages", action="store_true", help="跳过消息表（41k 行最大）")
    args = ap.parse_args()

    if not (os.environ.get("LARK_APP_ID") and os.environ.get("LARK_APP_SECRET")):
        print("ERR: need LARK_APP_ID / LARK_APP_SECRET", file=sys.stderr)
        return 2

    token = get_token()

    targets = []
    if not args.secondary_only:
        targets.append("primary")
    if not args.primary_only:
        targets.append("secondary")

    table_kinds = ["chat", "member"]
    if not args.skip_messages:
        table_kinds.append("message")

    grand = {"total": 0, "updated": 0, "pending_update": 0}
    for base_key in targets:
        cfg = BASES[base_key]
        for kind in table_kinds:
            spec = TABLE_USER_SPECS[kind]
            table_id = cfg[f"{kind}_table"]
            stat = backfill_table(
                token, cfg["name"], cfg["app"], table_id, spec,
                dry_run=args.dry_run,
            )
            for k, v in stat.items():
                grand[k] = grand.get(k, 0) + v

    print(f"\n========== 总计 ==========")
    print(f"扫描: {grand['total']}  待 update: {grand['pending_update']}  实际 updated: {grand['updated']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
