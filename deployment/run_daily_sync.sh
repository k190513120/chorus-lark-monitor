#!/bin/bash
# 群聊消息每日增量同步
set -euo pipefail

cd /Users/bytedance/chorus-lark-monitor

set -a; source .env 2>/dev/null || true; set +a

exec python3 sync_feishu_groups_to_base.py \
  --scheduled-daily \
  --refresh-metadata-tables \
  --skip-share-links \
  --fast-metadata \
  --skip-groupchat-field-updates \
  --sync-batch-size "${SYNC_BATCH_SIZE:-200}" \
  --read-concurrency "${READ_CONCURRENCY:-12}" \
  --sync-timezone "${SYNC_TIMEZONE:-Asia/Shanghai}" \
  --chat-order created_desc
