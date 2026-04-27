#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

: "${LARK_APP_ID:?LARK_APP_ID is required}"
: "${LARK_APP_SECRET:?LARK_APP_SECRET is required}"
: "${LARK_BASE_URL:?LARK_BASE_URL is required}"

python3 sync_feishu_groups_to_base.py \
  --scheduled-daily \
  --refresh-metadata-tables \
  --skip-share-links \
  --fast-metadata \
  --skip-groupchat-field-updates \
  --sync-batch-size "${SYNC_BATCH_SIZE:-200}" \
  --sync-timezone "${SYNC_TIMEZONE:-Asia/Shanghai}" \
  --chat-order created_desc \
  "$@"
