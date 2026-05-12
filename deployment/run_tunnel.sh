#!/bin/bash
# cloudflared tunnel · remote-managed 模式
# token 存在 ~/.cloudflared/chorus-lark-monitor.token (mode 600)
set -euo pipefail

TOKEN_FILE="$HOME/.cloudflared/chorus-lark-monitor.token"
if [[ ! -s "$TOKEN_FILE" ]]; then
  echo "missing $TOKEN_FILE" >&2
  exit 1
fi

TOKEN="$(cat "$TOKEN_FILE")"
exec /opt/homebrew/bin/cloudflared tunnel --no-autoupdate run --token "$TOKEN"
