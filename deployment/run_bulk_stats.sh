#!/bin/bash
# 刷新群发消息已读统计
set -euo pipefail

cd /Users/bytedance/chorus-lark-monitor

set -a; source .env 2>/dev/null || true; set +a

exec python3 bulk_message_probe.py refresh --max-age-days "${BULK_MAX_AGE_DAYS:-7}"
