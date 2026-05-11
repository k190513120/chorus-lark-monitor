#!/bin/bash
# 群聊监控台静态服务
set -euo pipefail

cd /Users/bytedance/Desktop/群聊消息统计/web

PORT="${PORT:-5678}"
HOST="${HOST:-127.0.0.1}"

exec /opt/homebrew/bin/node -e "
const http = require('http');
const fs = require('fs');
const path = require('path');
const ROOT = __dirname;
const MIME = {
  '.html':'text/html; charset=utf-8',
  '.css':'text/css; charset=utf-8',
  '.js':'application/javascript; charset=utf-8',
  '.jsx':'application/javascript; charset=utf-8',
  '.json':'application/json; charset=utf-8',
  '.svg':'image/svg+xml',
  '.png':'image/png',
};
http.createServer((req, res) => {
  let p = req.url.split('?')[0];
  if (p === '/' || p === '') p = '/index.html';
  const fp = path.join(ROOT, p);
  if (!fp.startsWith(ROOT)) { res.writeHead(400); return res.end('bad'); }
  const ext = path.extname(fp).toLowerCase();
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, {'Content-Type': MIME[ext]||'application/octet-stream','Cache-Control':'no-cache'});
    res.end(data);
  });
}).listen(${PORT}, '${HOST}', () => console.log('Serving on http://${HOST}:${PORT}'));
"
