#!/bin/bash
# 导出群聊数据到前端 web/src/data.jsx
set -euo pipefail

cd /Users/bytedance/Desktop/群聊消息统计

source .env 2>/dev/null || true

exec python3 export_to_web.py
