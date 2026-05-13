#!/usr/bin/env python3
"""一次性脚本：在 LARK_BASE_URL_SECONDARY 指向的新 Base 里创建/确认
机器人群列表 / 机器人群成员记录 / 机器人群消息记录 三张表与字段。

复用 sync_feishu_groups_to_base.py 的 ensure_table_and_fields 逻辑，
完成后打印 table_id 三元组方便后续 dual-write 配置核对。

Usage:
    set -a; source .env; set +a
    LARK_BASE_URL_SECONDARY='https://...' .venv/bin/python bootstrap_secondary_base.py
"""
from __future__ import annotations

import os
import sys

from sync_feishu_groups_to_base import (
    BASE_MEMBER_FIELD_DEFS,
    BASE_MESSAGE_FIELD_DEFS,
    CHAT_FIELD_DEFS,
    CHAT_TABLE_NAME,
    FeishuClient,
    MEMBER_TABLE_NAME,
    MESSAGE_TABLE_NAME,
    ensure_table_and_fields,
    materialize_field_defs,
    parse_base_token,
)


def main() -> int:
    app_id = os.environ.get("LARK_APP_ID")
    app_secret = os.environ.get("LARK_APP_SECRET")
    base_url = os.environ.get("LARK_BASE_URL_SECONDARY") or os.environ.get("LARK_BASE_URL_NEW")
    if not (app_id and app_secret and base_url):
        print("ERR: need LARK_APP_ID / LARK_APP_SECRET / LARK_BASE_URL_SECONDARY", file=sys.stderr)
        return 2

    base_token = parse_base_token(base_url)
    print(f"target base_token = {base_token}")

    client = FeishuClient(app_id, app_secret)
    client.authenticate()

    print(f"\n[1/3] ensuring '{CHAT_TABLE_NAME}' ...")
    chat_table_id = ensure_table_and_fields(
        client, base_token, CHAT_TABLE_NAME, CHAT_FIELD_DEFS, recreate_tables=False,
    )
    print(f"      -> {chat_table_id}")

    member_field_defs = materialize_field_defs(BASE_MEMBER_FIELD_DEFS, chat_table_id)
    message_field_defs = materialize_field_defs(BASE_MESSAGE_FIELD_DEFS, chat_table_id)

    print(f"\n[2/3] ensuring '{MEMBER_TABLE_NAME}' ...")
    member_table_id = ensure_table_and_fields(
        client, base_token, MEMBER_TABLE_NAME, member_field_defs, recreate_tables=False,
    )
    print(f"      -> {member_table_id}")

    print(f"\n[3/3] ensuring '{MESSAGE_TABLE_NAME}' ...")
    message_table_id = ensure_table_and_fields(
        client, base_token, MESSAGE_TABLE_NAME, message_field_defs, recreate_tables=False,
    )
    print(f"      -> {message_table_id}")

    print("\nALL OK. New base ready for dual-write:")
    print(f"  base_token         = {base_token}")
    print(f"  {CHAT_TABLE_NAME:<10} = {chat_table_id}")
    print(f"  {MEMBER_TABLE_NAME:<10} = {member_table_id}")
    print(f"  {MESSAGE_TABLE_NAME:<10} = {message_table_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
