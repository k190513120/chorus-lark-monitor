#!/bin/bash
# chorus-lark-monitor server.py (FastAPI + APScheduler)
# 取代旧的 4 个 cron + http.server 静态服。
set -euo pipefail

cd /Users/bytedance/chorus-lark-monitor

set -a; source .env 2>/dev/null || true; set +a

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5678}"

exec .venv/bin/uvicorn server:app \
  --host "$HOST" \
  --port "$PORT" \
  --log-level info
