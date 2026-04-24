#!/usr/bin/env bash
# 本地查看 web 页面（因为 babel standalone + script src 需要 http，file:// 不行）
set -e
cd "$(dirname "$0")"
PORT="${PORT:-5678}"
echo "Serving at http://localhost:${PORT}/"
python3 -m http.server "${PORT}"
