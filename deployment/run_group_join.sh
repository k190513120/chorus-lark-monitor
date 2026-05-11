#!/bin/bash
# 拉机器人进外部群
set -euo pipefail

cd /Users/bytedance/Desktop/群聊消息统计

source .env 2>/dev/null || true

export PYTHONUNBUFFERED=1
export EXTERNAL_GROUP_JOIN_APPLY=true

exec python3 ensure_bot_in_external_chats.py --apply
