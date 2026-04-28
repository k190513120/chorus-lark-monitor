#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

: "${LARK_APP_ID:=${FEISHU_APP_ID:-}}"
: "${LARK_APP_SECRET:=${FEISHU_APP_SECRET:-}}"
: "${LARK_APP_ID:?LARK_APP_ID or FEISHU_APP_ID is required}"
: "${LARK_APP_SECRET:?LARK_APP_SECRET or FEISHU_APP_SECRET is required}"

export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

if [[ "${EXTERNAL_GROUP_JOIN_APPLY:-false}" == "true" ]]; then
  set -- --apply "$@"
fi

python3 ensure_bot_in_external_chats.py "$@"
