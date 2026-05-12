#!/bin/bash
# 群聊监控台静态服务（用 Python 自带 http.server，避免依赖 Node）
set -euo pipefail

cd /Users/bytedance/chorus-lark-monitor/web

PORT="${PORT:-5678}"
HOST="${HOST:-127.0.0.1}"

exec python3 -m http.server "$PORT" --bind "$HOST"
