#!/usr/bin/env python3
"""Chorus Docx 完整重写脚本（V4：使用 scripts/lark_docx.py 库）。

只保留项目相关内容（SVG 字符串 + 章节 build_doc_items）。所有 Lark API /
画板 / 上传 boilerplate 都在 lark_docx.py 里。

Run:
    set -a; source .env; set +a
    .venv/bin/python scripts/rewrite_docx.py
"""
import os
import sys

# 让本目录里的 lark_docx 能 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lark_docx import (
    LarkDocxClient,
    h, p, bullet, code, divider,
    text_item, svg_item,
)

DOC_ID = os.getenv("CHORUS_DOC_ID", "VqlCdpASboikidxVuTMcth1rnAh")


SVG_STYLE = """
  .box { stroke-width: 1.5; }
  .label { font-family: -apple-system, "PingFang SC", sans-serif; font-size: 14px; fill: #111827; }
  .label-bold { font-weight: 700; }
  .label-sm { font-size: 11px; fill: #6b7280; }
  .label-mono { font-family: "JetBrains Mono", monospace; font-size: 11px; fill: #4b5563; }
  .label-mono-md { font-family: "JetBrains Mono", monospace; font-size: 13px; fill: #1f2937; }
  .section-title { font-family: -apple-system, sans-serif; font-weight: 700; font-size: 13px; fill: #111827; }
"""

ARCH_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 720" width="1280" height="720">
  <style>
    {SVG_STYLE}
    .arrow {{ stroke: #4b5563; stroke-width: 1.8; fill: none; }}
    .arrow-async {{ stroke: #059669; stroke-dasharray: 5,3; }}
    .arrow-disabled {{ stroke: #9ca3af; stroke-width: 1.5; stroke-dasharray: 3,3; fill: none; }}
    .arrow-label {{ font-family: -apple-system; font-size: 10px; fill: #4b5563; }}
  </style>
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#4b5563"/>
    </marker>
    <marker id="arr-a" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#059669"/>
    </marker>
  </defs>
  <text x="40" y="30" font-family="-apple-system" font-weight="700" font-size="16" fill="#111827">数据流（2026-05-15 终态）</text>

  <rect class="box" x="40" y="60" width="220" height="80" rx="8" fill="#fef3c7" stroke="#d97706"/>
  <text class="label label-bold" x="150" y="90" text-anchor="middle">飞书 (Lark)</text>
  <text class="label-sm" x="150" y="108" text-anchor="middle">15 授权用户 + bot</text>
  <text class="label-sm" x="150" y="124" text-anchor="middle">bot 在 49,908 群</text>

  <rect class="box" x="320" y="60" width="220" height="80" rx="8" fill="#fee2e2" stroke="#dc2626"/>
  <text class="label label-bold" x="430" y="90" text-anchor="middle">CF Worker</text>
  <text class="label-mono" x="430" y="108" text-anchor="middle">chorus.xiaomiao.win</text>
  <text class="label-sm" x="430" y="124" text-anchor="middle">Cache API + 白名单</text>

  <rect class="box" x="600" y="60" width="220" height="80" rx="8" fill="#fee2e2" stroke="#dc2626"/>
  <text class="label label-bold" x="710" y="90" text-anchor="middle">cloudflared tunnel</text>
  <text class="label-mono" x="710" y="108" text-anchor="middle">--protocol http2</text>
  <text class="label-sm" x="710" y="124" text-anchor="middle">chorus-origin.xiaomiao.win</text>

  <rect class="box" x="970" y="60" width="270" height="80" rx="8" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="1105" y="90" text-anchor="middle">浏览器 Dashboard</text>
  <text class="label-mono" x="1105" y="108" text-anchor="middle">chorus.xiaomiao.win/</text>
  <text class="label-sm" x="1105" y="124" text-anchor="middle">React 18 + JSX</text>

  <rect class="box" x="40" y="190" width="780" height="320" rx="8" fill="#dbeafe" stroke="#2563eb"/>
  <text class="section-title" x="60" y="218" fill="#1e40af">Mac mini · server.py (FastAPI + APScheduler + 127.0.0.1:5678)</text>

  <rect class="box" x="60" y="240" width="240" height="100" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="180" y="262" text-anchor="middle">webhook handler</text>
  <text class="label-mono" x="180" y="280" text-anchor="middle">POST /lark/events</text>
  <text class="label-sm" x="180" y="298" text-anchor="middle">12 后台线程</text>
  <text class="label-sm" x="180" y="314" text-anchor="middle">event_id LRU 防重</text>
  <text class="label-sm" x="180" y="330" text-anchor="middle">5ms 入 SQLite 返 200</text>

  <rect class="box" x="350" y="240" width="240" height="100" rx="6" fill="#d1fae5" stroke="#059669"/>
  <text class="label label-bold" x="470" y="262" text-anchor="middle">SQLite 真源</text>
  <text class="label-mono" x="470" y="280" text-anchor="middle">chorus_local.db</text>
  <text class="label-sm" x="470" y="298" text-anchor="middle">chats 49.9k · msgs 72k</text>
  <text class="label-sm" x="470" y="314" text-anchor="middle">members 106k · WAL</text>
  <text class="label-sm" x="470" y="330" text-anchor="middle">primary_synced flag</text>

  <rect class="box" x="630" y="240" width="170" height="60" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="715" y="262" text-anchor="middle">primary-sync</text>
  <text class="label-sm" x="715" y="282" text-anchor="middle">IntervalTrigger 10s</text>

  <rect class="box" x="630" y="320" width="170" height="40" rx="6" fill="#f3f4f6" stroke="#9ca3af" stroke-dasharray="3,3"/>
  <text class="label label-bold" x="715" y="340" text-anchor="middle" fill="#9ca3af">secondary-sync</text>
  <text class="label-sm" x="715" y="354" text-anchor="middle" fill="#9ca3af">disabled (旧 Base)</text>

  <rect class="box" x="60" y="380" width="540" height="60" rx="6" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="330" y="404" text-anchor="middle">Dashboard endpoint · GET /src/data.jsx</text>
  <text class="label-sm" x="330" y="424" text-anchor="middle">SQLite 直读，top-500 群 × 20 消息 · CF 缓存 ~1.2MB gzipped</text>

  <rect class="box" x="60" y="460" width="540" height="40" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="330" y="484" text-anchor="middle">APScheduler · 6 个定时任务</text>

  <rect class="box" x="900" y="190" width="340" height="160" rx="8" fill="#ede9fe" stroke="#7c3aed"/>
  <text class="label label-bold" x="1070" y="218" text-anchor="middle" fill="#5b21b6">新 Base · 唯一写入（PRIMARY）</text>
  <text class="label-mono" x="1070" y="240" text-anchor="middle">G42ybVmN9aAeYdsHW06cysTonmF</text>
  <text class="label-sm" x="1070" y="258" text-anchor="middle">Helix · 项目门户</text>
  <text class="label-sm" x="1070" y="280" text-anchor="middle">机器人群列表 tblXhxEs8Y5IvFbw</text>
  <text class="label-sm" x="1070" y="298" text-anchor="middle">机器人群消息记录 tblK0WYR1ebTarjR</text>
  <text class="label-sm" x="1070" y="316" text-anchor="middle">机器人群成员记录 tbl7aSVqPLBk1iRv</text>
  <text class="label-sm" x="1070" y="336" text-anchor="middle" fill="#059669">★ user 字段全量补齐（42.8k 回填）</text>

  <rect class="box" x="900" y="370" width="340" height="80" rx="8" fill="#f9fafb" stroke="#9ca3af" stroke-dasharray="3,3"/>
  <text class="label label-bold" x="1070" y="394" text-anchor="middle" fill="#6b7280">旧 Base · 已停写（2026-05-14）</text>
  <text class="label-mono" x="1070" y="414" text-anchor="middle" fill="#9ca3af">PnRtbGmTpaVXwDsWBWPcPaEpnwh</text>
  <text class="label-sm" x="1070" y="432" text-anchor="middle">作为历史归档保留</text>

  <rect class="box" x="40" y="540" width="1200" height="160" rx="8" fill="#ffffff" stroke="#d1d5db"/>
  <text class="section-title" x="60" y="568">关键定时任务</text>
  <text class="label-sm" x="60" y="592">daily-sync · 每日 00:00 SGT · 飞书全量增量同步 (~10min, lite-mode 安全不重建表)</text>
  <text class="label-sm" x="60" y="612">external-join · 每日 22:00 SGT · bot 自动拉群</text>
  <text class="label-bold" x="700" y="612" fill="#dc2626">[已 DISABLED]</text>
  <text class="label-sm" x="60" y="632">bulk-stats-refresh · 每日 20:00 SGT · 广播效果统计</text>
  <text class="label-sm" x="60" y="652">cf-prewarm · 每 240s · 预热 CF Edge 缓存</text>
  <text class="label-sm" x="60" y="672">primary-sync · 每 10s · 把新 SQLite 行推到新 Base</text>
  <text class="label-sm" x="60" y="692">监控：/admin/sync-stats · /admin/jobs · /admin/local-db-stats</text>

  <path class="arrow" d="M260,100 L315,100" marker-end="url(#arr)"/>
  <path class="arrow" d="M540,100 L595,100" marker-end="url(#arr)"/>
  <path class="arrow" d="M820,100 L965,100" marker-end="url(#arr)"/>
  <path class="arrow" d="M710,140 L710,180 L180,180 L180,235" marker-end="url(#arr)"/>
  <text class="arrow-label" x="450" y="174" text-anchor="middle">tunnel → 127.0.0.1:5678</text>
  <path class="arrow" d="M300,290 L345,290" marker-end="url(#arr)"/>
  <text class="arrow-label" x="322" y="282" text-anchor="middle">5ms</text>
  <path class="arrow arrow-async" d="M590,270 L625,270" marker-end="url(#arr-a)"/>
  <path class="arrow-disabled" d="M590,335 L625,335"/>
  <path class="arrow arrow-async" d="M800,270 L895,260" marker-end="url(#arr-a)"/>
  <path class="arrow-disabled" d="M800,340 L895,400"/>
  <path class="arrow" d="M470,340 L470,378" marker-end="url(#arr)"/>
  <text class="arrow-label" x="476" y="358">SQLite 直读</text>
  <path class="arrow" d="M600,410 L1100,410 L1100,140" marker-end="url(#arr)"/>
</svg>
"""

CRON_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 480" width="1280" height="480">
  <style>{SVG_STYLE}
    .timeline {{ stroke: #d1d5db; stroke-width: 2; }}
    .tick {{ stroke: #9ca3af; stroke-width: 1; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">cron 定时任务（按 SGT 时间排）</text>

  <line class="timeline" x1="80" y1="100" x2="1240" y2="100"/>
  <g font-family="-apple-system" font-size="11" fill="#6b7280">
    <text x="80" y="120">00:00</text>
    <text x="226" y="120" text-anchor="middle">03</text>
    <text x="372" y="120" text-anchor="middle">06</text>
    <text x="518" y="120" text-anchor="middle">09</text>
    <text x="664" y="120" text-anchor="middle">12</text>
    <text x="810" y="120" text-anchor="middle">15</text>
    <text x="956" y="120" text-anchor="middle">18</text>
    <text x="1102" y="120" text-anchor="middle">21</text>
    <text x="1240" y="120" text-anchor="end">24:00</text>
  </g>
  <g>
    <line class="tick" x1="80" y1="95" x2="80" y2="105"/>
    <line class="tick" x1="664" y1="95" x2="664" y2="105"/>
    <line class="tick" x1="1054" y1="95" x2="1054" y2="105"/>
    <line class="tick" x1="1152" y1="95" x2="1152" y2="105"/>
  </g>

  <circle cx="80" cy="100" r="9" fill="#2563eb"/>
  <line stroke="#2563eb" stroke-width="2" x1="80" y1="100" x2="80" y2="160"/>
  <rect x="50" y="160" width="240" height="90" rx="6" fill="#dbeafe" stroke="#2563eb"/>
  <text class="label label-bold" x="170" y="182" text-anchor="middle">daily-sync · 00:00</text>
  <text class="label-sm" x="170" y="200" text-anchor="middle">cron[hour=16,minute=0] UTC</text>
  <text class="label-sm" x="170" y="216" text-anchor="middle">飞书全量同步 → 新 Base + SQLite</text>
  <text class="label-sm" x="170" y="232" text-anchor="middle">--lite-mode 安全（不重建表）</text>
  <text class="label-sm" x="170" y="246" text-anchor="middle">~10min · 24k 群 / 72k 消息</text>

  <circle cx="1054" cy="100" r="9" fill="#d97706"/>
  <line stroke="#d97706" stroke-width="2" x1="1054" y1="100" x2="1054" y2="160"/>
  <rect x="934" y="160" width="240" height="80" rx="6" fill="#fef3c7" stroke="#d97706"/>
  <text class="label label-bold" x="1054" y="182" text-anchor="middle">bulk-stats-refresh · 20:00</text>
  <text class="label-sm" x="1054" y="200" text-anchor="middle">cron[hour=12,minute=0] UTC</text>
  <text class="label-sm" x="1054" y="216" text-anchor="middle">广播效果统计回流</text>
  <text class="label-sm" x="1054" y="232" text-anchor="middle">~7s</text>

  <circle cx="1152" cy="100" r="9" fill="#9ca3af" stroke="#dc2626" stroke-width="2" stroke-dasharray="2,2"/>
  <line stroke="#9ca3af" stroke-width="2" stroke-dasharray="3,3" x1="1152" y1="100" x2="1152" y2="280"/>
  <rect x="1010" y="280" width="220" height="100" rx="6" fill="#f9fafb" stroke="#9ca3af" stroke-dasharray="3,3"/>
  <text class="label label-bold" x="1120" y="302" text-anchor="middle" fill="#6b7280">external-join · 22:00</text>
  <text class="label-sm" x="1120" y="320" text-anchor="middle" fill="#6b7280">cron[hour=14,minute=0] UTC</text>
  <text class="label-sm" x="1120" y="336" text-anchor="middle" fill="#dc2626" font-weight="700">[已 DISABLED 2026-05-14]</text>
  <text class="label-sm" x="1120" y="354" text-anchor="middle">EXTERNAL_JOIN_DISABLED=true</text>
  <text class="label-sm" x="1120" y="370" text-anchor="middle">一夜入 24k 群后停手</text>

  <text class="section-title" x="40" y="410" font-size="13">持续型（IntervalTrigger）</text>
  <rect x="40" y="420" width="280" height="44" rx="6" fill="#d1fae5" stroke="#059669"/>
  <text class="label label-bold" x="60" y="442">primary-sync</text>
  <text class="label-sm" x="60" y="458">每 10s · SQLite → 新 Base (唯一写)</text>

  <rect x="340" y="420" width="280" height="44" rx="6" fill="#f3f4f6" stroke="#9ca3af" stroke-dasharray="3,3"/>
  <text class="label label-bold" x="360" y="442" fill="#9ca3af">secondary-sync · disabled</text>
  <text class="label-sm" x="360" y="458" fill="#9ca3af">env 未设 → worker 自动 noop</text>

  <rect x="640" y="420" width="280" height="44" rx="6" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="660" y="442">cf-prewarm</text>
  <text class="label-sm" x="660" y="458">每 240s · 预热 CF Edge 缓存</text>
</svg>
"""

ENDPOINTS_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 460" width="1280" height="460">
  <style>{SVG_STYLE}
    .row {{ fill: #f9fafb; stroke: #e5e7eb; stroke-width: 1; }}
    .row-alt {{ fill: #ffffff; stroke: #e5e7eb; }}
    .verb-get {{ fill: #10b981; }}
    .verb-post {{ fill: #3b82f6; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">监控端点（http://127.0.0.1:5678）</text>

  <rect x="40" y="60" width="1200" height="34" fill="#1f2937"/>
  <text x="60" y="83" fill="#fff" font-family="-apple-system" font-weight="700" font-size="13">METHOD</text>
  <text x="170" y="83" fill="#fff" font-family="-apple-system" font-weight="700" font-size="13">路径</text>
  <text x="600" y="83" fill="#fff" font-family="-apple-system" font-weight="700" font-size="13">用途</text>

  <g font-family="-apple-system" font-size="13">
    <rect class="row" x="40" y="94" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="106" width="50" height="20" rx="3"/>
    <text x="85" y="121" fill="#fff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="121">/healthz</text>
    <text class="label" x="600" y="121">存活探测</text>

    <rect class="row-alt" x="40" y="134" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="146" width="50" height="20" rx="3"/>
    <text x="85" y="161" fill="#fff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="161">/admin/jobs</text>
    <text class="label" x="600" y="161">列出全部 cron + next_run</text>

    <rect class="row" x="40" y="174" width="1200" height="40"/>
    <rect class="verb-post" x="60" y="186" width="50" height="20" rx="3"/>
    <text x="85" y="201" fill="#fff" font-weight="700" text-anchor="middle" font-size="11">POST</text>
    <text class="label-mono-md" x="170" y="201">/admin/run/{{job_id}}</text>
    <text class="label" x="600" y="201">手动触发某 job</text>

    <rect class="row-alt" x="40" y="214" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="226" width="50" height="20" rx="3"/>
    <text x="85" y="241" fill="#fff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="241">/admin/local-db-stats</text>
    <text class="label" x="600" y="241">SQLite 行数 + db 大小</text>

    <rect class="row" x="40" y="254" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="266" width="50" height="20" rx="3"/>
    <text x="85" y="281" fill="#fff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="281">/admin/sync-stats</text>
    <text class="label" x="600" y="281">primary 队列深度 + 失败计数</text>

    <rect class="row-alt" x="40" y="294" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="306" width="50" height="20" rx="3"/>
    <text x="85" y="321" fill="#fff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="321">/lark/events/recent</text>
    <text class="label" x="600" y="321">最近 webhook 事件 + 统计</text>
  </g>

  <text class="label-sm" x="40" y="390">公网入口：https://chorus.xiaomiao.win/  | 仅放白名单 GET + POST /lark/events</text>
  <text class="label-sm" x="40" y="410">admin 路径只在本机 127.0.0.1:5678 可达</text>
</svg>
"""

ENV_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 540" width="1280" height="540">
  <style>{SVG_STYLE}
    .group {{ fill: #ffffff; stroke: #d1d5db; }}
    .group-title {{ font-family: -apple-system; font-weight: 700; font-size: 14px; fill: #1f2937; }}
    .req {{ fill: #dc2626; }}
    .opt {{ fill: #9ca3af; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">关键配置（~/chorus-lark-monitor/.env）</text>

  <rect class="group" x="40" y="60" width="600" height="190" rx="8"/>
  <rect x="40" y="60" width="600" height="32" rx="8" fill="#fef3c7"/>
  <text class="group-title" x="60" y="82">飞书 App 凭据（必填）</text>
  <g font-size="12">
    <text class="label-mono-md" x="60" y="115">LARK_APP_ID</text>
    <text class="label-sm" x="320" y="115">飞书 App ID（cli_xxx）</text>
    <circle class="req" cx="612" cy="111" r="5"/>
    <text class="label-mono-md" x="60" y="145">LARK_APP_SECRET</text>
    <text class="label-sm" x="320" y="145">飞书 App Secret</text>
    <circle class="req" cx="612" cy="141" r="5"/>
    <text class="label-mono-md" x="60" y="175">LARK_BASE_URL</text>
    <text class="label-sm" x="320" y="175">新 Base URL（唯一写入目标）</text>
    <circle class="req" cx="612" cy="171" r="5"/>
    <text class="label-mono-md" x="60" y="205">#LARK_BASE_URL_SECONDARY</text>
    <text class="label-sm" x="320" y="205">已注释（旧 Base 已停写）</text>
    <circle class="opt" cx="612" cy="201" r="5"/>
  </g>

  <rect class="group" x="660" y="60" width="580" height="190" rx="8"/>
  <rect x="660" y="60" width="580" height="32" rx="8" fill="#ede9fe"/>
  <text class="group-title" x="680" y="82">外部群入群代理（feishu-bot-proxy）</text>
  <g>
    <text class="label-mono-md" x="680" y="115">GROUP_JOIN_PROXY_URL</text>
    <text class="label-sm" x="950" y="115">https://feishu-bot.xiaomiao.win</text>
    <circle class="req" cx="1212" cy="111" r="5"/>
    <text class="label-mono-md" x="680" y="145">GROUP_JOIN_ADMIN_TOKEN</text>
    <text class="label-sm" x="950" y="145">CF Worker Secret 鉴权</text>
    <circle class="req" cx="1212" cy="141" r="5"/>
    <text class="label-mono-md" x="680" y="180">EXTERNAL_JOIN_DISABLED=true</text>
    <text class="label-sm" x="950" y="180">紧急 kill switch（已置 true）</text>
    <circle class="opt" cx="1212" cy="176" r="5"/>
    <text class="label-sm" x="680" y="210">备份：~/chorus-lark-monitor/.group_join_admin_token.local (mode 600)</text>
  </g>

  <rect class="group" x="40" y="280" width="1200" height="220" rx="8"/>
  <rect x="40" y="280" width="1200" height="32" rx="8" fill="#d1fae5"/>
  <text class="group-title" x="60" y="302">应用调优（可选，都有默认值）</text>
  <g>
    <text class="label-mono-md" x="60" y="335">WEB_MAX_GROUPS=500</text>
    <text class="label-sm" x="380" y="335">dashboard 展示群数上限</text>
    <circle class="opt" cx="1212" cy="331" r="5"/>
    <text class="label-mono-md" x="60" y="365">WEB_MAX_MESSAGES_PER_GROUP=20</text>
    <text class="label-sm" x="380" y="365">每群展示消息数</text>
    <circle class="opt" cx="1212" cy="361" r="5"/>
    <text class="label-mono-md" x="60" y="395">LARK_EVENT_POOL_SIZE=12</text>
    <text class="label-sm" x="380" y="395">webhook 后台线程池大小</text>
    <circle class="opt" cx="1212" cy="391" r="5"/>
    <text class="label-mono-md" x="60" y="425">EXTERNAL_GROUP_JOIN_ACTIVE_SINCE_DAYS=30</text>
    <text class="label-sm" x="380" y="425">外部群活跃度过滤天数</text>
    <circle class="opt" cx="1212" cy="421" r="5"/>
    <text class="label-mono-md" x="60" y="455">CF_PREWARM_URL=https://chorus.xiaomiao.win</text>
    <text class="label-sm" x="380" y="455">CF Edge cache 预热目标</text>
    <circle class="opt" cx="1212" cy="451" r="5"/>
    <text class="label-mono-md" x="60" y="485">ENABLE_SCHEDULED_JOBS=true</text>
    <text class="label-sm" x="380" y="485">禁用所有 cron（调试用）</text>
    <circle class="opt" cx="1212" cy="481" r="5"/>
  </g>

  <circle class="req" cx="60" cy="525" r="5"/>
  <text class="label-sm" x="75" y="529">必填</text>
  <circle class="opt" cx="135" cy="525" r="5"/>
  <text class="label-sm" x="150" y="529">可选（有默认值）</text>
</svg>
"""

FILES_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 660" width="1280" height="660">
  <style>{SVG_STYLE}
    .folder {{ fill: #fef3c7; stroke: #d97706; }}
    .file-py {{ fill: #dbeafe; stroke: #2563eb; }}
    .file-js {{ fill: #fef9c3; stroke: #ca8a04; }}
    .file-md {{ fill: #f3f4f6; stroke: #6b7280; }}
    .file-script {{ fill: #d1fae5; stroke: #059669; }}
    .tree-line {{ stroke: #9ca3af; stroke-width: 1.2; fill: none; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">关键代码文件</text>

  <text class="label-mono-md" x="40" y="80" font-weight="700">chorus-lark-monitor/</text>

  <line class="tree-line" x1="70" y1="90" x2="70" y2="240"/>
  <g>
    <line class="tree-line" x1="70" y1="110" x2="90" y2="110"/>
    <rect class="file-py" x="95" y="98" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="116" font-weight="700">server.py</text>
    <text class="label-sm" x="385" y="116">FastAPI + APScheduler 主入口</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="140" x2="90" y2="140"/>
    <rect class="file-py" x="95" y="128" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="146" font-weight="700">local_db.py</text>
    <text class="label-sm" x="385" y="146">SQLite schema + helpers</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="170" x2="90" y2="170"/>
    <rect class="file-py" x="95" y="158" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="176">sync_feishu_groups_to_base.py</text>
    <text class="label-sm" x="385" y="176">全量同步 + FeishuClient + Base schema</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="200" x2="90" y2="200"/>
    <rect class="file-py" x="95" y="188" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="206">ensure_bot_in_external_chats.py</text>
    <text class="label-sm" x="385" y="206">外部群自动入群 + 30 天活跃过滤</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="230" x2="90" y2="230"/>
    <rect class="file-py" x="95" y="218" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="236">bulk_message_probe.py</text>
    <text class="label-sm" x="385" y="236">群发消息效果统计</text>
  </g>

  <text class="label-mono-md" x="40" y="280" font-weight="700">scripts/ · 一次性脚本</text>
  <line class="tree-line" x1="70" y1="290" x2="70" y2="410"/>
  <g>
    <line class="tree-line" x1="70" y1="310" x2="90" y2="310"/>
    <rect class="file-script" x="95" y="298" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="316">bootstrap_secondary_base.py</text>
    <text class="label-sm" x="385" y="316">在新 Base 建 3 张表（已用过）</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="340" x2="90" y2="340"/>
    <rect class="file-script" x="95" y="328" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="346">backfill_user_fields.py</text>
    <text class="label-sm" x="385" y="346">回填 user 字段（已修 42.8k 行）</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="370" x2="90" y2="370"/>
    <rect class="file-script" x="95" y="358" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="376">rewrite_docx.py</text>
    <text class="label-sm" x="385" y="376">本文档生成器</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="400" x2="90" y2="400"/>
    <rect class="file-script" x="95" y="388" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="406">append_weekly_report.py</text>
    <text class="label-sm" x="385" y="406">周报追加章节</text>
  </g>

  <text class="label-mono-md" x="40" y="450" font-weight="700">deployment/</text>
  <line class="tree-line" x1="70" y1="460" x2="70" y2="520"/>
  <g>
    <line class="tree-line" x1="70" y1="480" x2="90" y2="480"/>
    <rect class="folder" x="95" y="468" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="486">worker/</text>
    <text class="label-sm" x="385" y="486">CF Worker 源码（wrangler 部署）</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="510" x2="90" y2="510"/>
    <rect class="file-md" x="95" y="498" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="516">com.feishu-chat.{{server,tunnel}}.plist</text>
    <text class="label-sm" x="385" y="516">launchd 任务定义</text>
  </g>

  <text class="label-mono-md" x="40" y="560" font-weight="700">web/</text>
  <line class="tree-line" x1="70" y1="570" x2="70" y2="610"/>
  <g>
    <line class="tree-line" x1="70" y1="590" x2="90" y2="590"/>
    <rect class="file-js" x="95" y="578" width="280" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="596">src/*.jsx · v2-styles.css</text>
    <text class="label-sm" x="385" y="596">dashboard 前端（React 18 + JSX）</text>
  </g>

  <g transform="translate(750, 90)">
    <text class="section-title" x="0" y="0" font-size="13">类型图例</text>
    <rect class="file-py" x="0" y="20" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="34">Python 主代码（常驻进程依赖）</text>
    <rect class="file-script" x="0" y="50" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="64">一次性 / 运维脚本</text>
    <rect class="file-js" x="0" y="80" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="94">前端 / Worker（JS / JSX）</text>
    <rect class="folder" x="0" y="110" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="124">子目录</text>
    <rect class="file-md" x="0" y="140" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="154">配置 / 文档</text>
  </g>
</svg>
"""

def build_doc_items():
    items = []

    items.append(text_item([
        h(1, "Chorus Lark Monitor · 飞书群聊监控系统"),
        p("把飞书群聊的消息 / 成员变更 / 群事件实时汇聚到 SQLite 真源 + 新 Base 写入，给客户运营提供「DR · 客户对话健康度」面板。"),
    ]))

    items.append(text_item([
        h(2, "一、Chorus 是什么"),
        p("Mac mini 本机部署的飞书机器人群聊监控平台。bot 自动加入授权用户的外部群，实时捕获消息事件，聚合后给客户经理 / 主管看「群健康度看板」。"),
        h(3, "现状（2026-05-15）"),
        bullet("部署在 Mac mini（M4 / 32GB），launchd 跑 server.py（uvicorn）+ cloudflared tunnel"),
        bullet("公网入口：https://chorus.xiaomiao.win（Cloudflare Worker + Tunnel）"),
        bullet("数据规模：49,908 群 / 72,396 消息 / 106,155 成员"),
        bullet("15 个授权用户授权 bot 拉外部群；bot 在 49.9k 群里"),
        bullet("Base 写入：新 Base「Helix · 项目门户」唯一写入（旧 Base 已停写归档）"),
        bullet("dual-write 阶段已结束，迁移完成（2026-05-14）"),
    ]))

    items.append(text_item([
        h(2, "二、整体架构"),
        p("数据从飞书出发，经过 CF Worker + tunnel 进入本机 server.py，落到 SQLite 真源，由 primary-sync worker 异步推送到新 Base。dashboard 直读 SQLite，绕开 Base API 限频。"),
    ]))
    items.append(svg_item(ARCH_SVG, "arch.svg"))
    items.append(text_item([
        h(3, "三段职责"),
        bullet("Webhook 入站：5ms 内只写 SQLite，飞书 webhook 永不超时"),
        bullet("后台 sync worker：异步把 SQLite 行批量推到新 Base，失败可重试可监控"),
        bullet("Dashboard 读路径：SQLite 直读，绕过 Base 全部限频"),
    ]))

    items.append(text_item([
        h(2, "三、SQLite 真源（chorus_local.db）"),
        p("本机持久层 ~83 MB，WAL 模式支持并发读 + 单写。所有 Base 数据从这里 fanout。"),
        h(3, "4 张表"),
        bullet("chats — 群信息（chat_id 主键）"),
        bullet("members — 群成员（chat_id + member_open_id 复合主键）"),
        bullet("messages — 消息流（msg_id 主键，索引 chat_id + time_ms）"),
        bullet("meta — KV 元数据（last_seeded_at 等）"),
        h(3, "sync state 字段"),
        bullet("primary_synced / secondary_synced — 0 = 待同步，1 = 已同步"),
        bullet("record_id / secondary_record_id — 写入各 Base 后返回的 record_id（用于链接字段）"),
        bullet("sync_attempts / sync_last_error — 重试次数 + 最近错误"),
        p("2026-05-14 迁移时做过列翻转：原 record_id（旧 Base）与 secondary_record_id（新 Base）交换，让 primary 路径指向新 Base。"),
    ]))

    items.append(text_item([
        h(2, "四、Webhook 事件处理"),
        p("server.py 暴露 POST /lark/events。事件分两类：CREATE 类异步走 SQLite + worker，UPDATE/DELETE 类同步操作 Base 已有 record_id（量小）。"),
        h(3, "CREATE 类（异步入 SQLite，worker 推 Base）"),
        bullet("im.message.receive_v1 — 新消息 → 写 messages 表"),
        bullet("im.chat.member.user.added_v1 — 成员入群 → 写 members 表"),
        bullet("im.chat.member.bot.added_v1 — bot 入新群 → 调 Lark API 抓 chat detail + 成员 + 24h 消息回填，全部入 SQLite"),
        h(3, "UPDATE/DELETE 类（同步写 Base，量小不解耦）"),
        bullet("im.message.recalled_v1 — 消息撤回，标 is_deleted = 1"),
        bullet("im.chat.member.user.deleted_v1 — 成员退群，移除 members 行"),
        bullet("im.chat.disbanded_v1 — 群解散，标 chat 为 dissolved"),
        h(3, "防抖与并发"),
        bullet("event_id LRU set（10k 容量）防飞书重发"),
        bullet("ThreadPoolExecutor max_workers=12 处理 webhook 事件"),
        bullet("SQLite WAL 单写者自动 serialize，无写冲突"),
    ]))

    items.append(text_item([
        h(2, "五、多维表格（新 Base 唯一写入）"),
        h(3, "新 Base · Helix · 项目门户（PRIMARY）"),
        bullet("token: G42ybVmN9aAeYdsHW06cysTonmF"),
        bullet("机器人群列表 tblXhxEs8Y5IvFbw"),
        bullet("机器人群消息记录 tblK0WYR1ebTarjR"),
        bullet("机器人群成员记录 tbl7aSVqPLBk1iRv"),
        h(3, "旧 Base · 已停写（归档保留）"),
        bullet("token: PnRtbGmTpaVXwDsWBWPcPaEpnwh"),
        bullet(".env 里 LARK_BASE_URL_SECONDARY 已注释（secondary_enabled=False）"),
        h(3, "user 类型字段修复 + 历史回填（2026-05-14）"),
        bullet("修：_build_*_row_minimal 在 user 字段填 [{\"id\": open_id}] 而不是 None"),
        bullet("回填：chats 群主 3,051 行 + members 成员 13,067 行 + messages 发送者 26,726 行 = 42,844 行"),
        bullet("Base /records GET API 在表 >50k 行时报 1254103 RecordExceedLimit；改用 POST /records/search，page_token 放 query string"),
    ]))

    items.append(text_item([h(2, "六、定时任务（APScheduler）")]))
    items.append(svg_item(CRON_SVG, "cron.svg"))
    items.append(text_item([
        h(3, "daily-sync 详解"),
        bullet("调用 sync_feishu_groups_to_base.py，参数 --scheduled-daily --lite-mode --refresh-metadata-tables"),
        bullet("--lite-mode 下不会重建表，只增量 upsert（确认过对新 Base 安全）"),
        bullet("结束时自动 _invalidate_lark_state() + local_db.seed_from_lark_base() 重 seed"),
        bullet("当前耗时 ~10min"),
        h(3, "external-join · 已 DISABLED"),
        bullet("一次 cron 触发拉 24k+ 新群，导致 cron pool max_workers=1 被堵 14h，sync worker 串行卡死"),
        bullet("加 EXTERNAL_JOIN_DISABLED env kill switch，job 启动即跳过"),
        bullet("如需重启：.env 删 EXTERNAL_JOIN_DISABLED + 重启 server.py"),
    ]))

    items.append(text_item([
        h(2, "七、公网部署（CF Worker + Tunnel）"),
        h(3, "Cloudflare 资源"),
        bullet("Account: Kelan656691@gmail.com (acct 2e2b291e8f3e011ca7824f19bcb77236)"),
        bullet("Zone: xiaomiao.win (81498ac216563761c63636b270a4caf1)"),
        bullet("Tunnel UUID: 9c7e347a-3b11-47d7-8e5f-18bb5d463397，必须 --protocol http2"),
        bullet("Worker: chorus-lark-events-gateway · caches.default 边缘缓存 300s + Cache API"),
        bullet("DNS: chorus.xiaomiao.win (custom domain), chorus-origin.xiaomiao.win (CNAME → tunnel UUID)"),
        h(3, "Worker 关键逻辑"),
        bullet("白名单：POST /lark/events + GET /、/src/*、/api/dashboard/*"),
        bullet("Edge cache：仅缓存 GET /src/*；origin 回 no-store/no-cache/private 时跳过写 cache"),
        bullet("x-cache 响应头：HIT/MISS/BYPASS 三态，便于诊断"),
        h(3, "feishu-bot-proxy（另一个 Worker）"),
        bullet("绑 feishu-bot.xiaomiao.win，做用户 OAuth + access_token 中转"),
        bullet("KV namespace GROUP_JOIN_TOKENS 存用户 token；secret GROUP_JOIN_ADMIN_TOKEN 鉴权"),
    ]))

    items.append(text_item([
        h(2, "八、Dashboard 前端"),
        p("地址：https://chorus.xiaomiao.win/"),
        bullet("前端栈：React 18 UMD（unpkg）+ Babel standalone + JSX 源码"),
        bullet("data.jsx 由 server.py 动态生成，从 SQLite 取 top-N 活跃群（默认 N=500）+ 每群最近 20 条消息"),
        bullet("payload 体积：~10MB raw / ~1.2MB gzipped，CF cache hit 首访 ~1.8s"),
        bullet("已知瓶颈：Google Fonts + unpkg.com 境外 CDN 首访 3-5s 主要花在这里"),
    ]))

    items.append(text_item([h(2, "九、运维 / 监控")]))
    items.append(svg_item(ENDPOINTS_SVG, "endpoints.svg"))
    items.append(text_item([
        h(3, "日志文件"),
        bullet("~/chorus-lark-monitor/logs/server.err.log — server.py 主日志"),
        bullet("~/chorus-lark-monitor/logs/server.out.log — uvicorn 访问日志 + 脚本 stdout"),
        bullet("~/chorus-lark-monitor/logs/tunnel.err.log — cloudflared tunnel 错误"),
        h(3, "launchd 任务"),
        bullet("com.feishu-chat.server — uvicorn server:app 监听 127.0.0.1:5678"),
        bullet("com.feishu-chat.tunnel — cloudflared tunnel run，token 在 ~/.cloudflared/chorus-lark-monitor.token (mode 600)"),
        p("plist 在 deployment/ 下，launchctl unload / load 重启。"),
    ]))

    items.append(text_item([h(2, "十、关键配置（.env）")]))
    items.append(svg_item(ENV_SVG, "env.svg"))

    items.append(text_item([h(2, "十一、关键代码文件")]))
    items.append(svg_item(FILES_SVG, "files.svg"))

    items.append(text_item([
        h(2, "十二、踩过的坑（按时间倒序）"),
        bullet("Lark Base /records GET API 在表 >50k 行时报 1254103 RecordExceedLimit → 改用 /records/search（page_token 必须在 query string 不是 body）"),
        bullet("_build_*_row_minimal 给 user 类型字段填 None，导致 3 张表的 群主/成员/发送者 字段全空 → 修后回填 42.8k 历史行"),
        bullet("external-join 一次 cron 拉 36k 候选群，cron pool max_workers=1 被堵 14h，sync worker 串行卡死 → 加 EXTERNAL_JOIN_DISABLED kill switch"),
        bullet("CF Worker 覆写 cache-control 写 caches.default，warm-up 空 payload + prewarm 循环 → 看板永远 0 群。修：origin 回 no-store 时 Worker 跳过写 cache"),
        bullet("ensure_bot_in_external_chats.py 看到 GROUP_JOIN_ADMIN_TOKEN= 是空字符串就抛 RuntimeError → CF API PUT 新 secret 三处同步"),
        bullet("_create_member_rows_safely 一次写 200+ 行触发 Lark Base 800010701 invalid_request → 按 200 切片"),
        bullet("Babel standalone 编译 10MB+ JSON 在浏览器 OOM → data.jsx 不打 type=text/babel"),
        bullet("cloudflared 默认 QUIC 协议路由到美西 timeout → --protocol http2"),
        bullet("Lark Base 限频 800004135 OpenAPIBatchAddRecords ~20/s/table → FeishuClient 加 RATE_LIMIT_CODES set 自动 retry"),
        bullet("daily-sync --refresh-metadata-tables 重建表后旧 table_id 失效 → daily-sync 完成后 _invalidate_lark_state() + 重 seed 本地 SQLite"),
    ]))

    items.append(text_item([
        h(2, "十三、待办 / 后续演进"),
        h(3, "ID 转换接入（tenant_key → tenant_id F 码）"),
        bullet("当前可用：fsopen.bytedance.net/exchange/v3/ 已实测通，支持 open_id ↔ lark_id / message_id / chat_id 四类"),
        bullet("缺：tenant_key → tenant_id（F 码）—— fsopen 没包，需要直调字节内部 Kitex RPC lark.oapi.api_runtime"),
        bullet("Mac mini 受限网段：到不了 paas-gw.byted.org（TQS gateway）也无 Mesh 环境"),
        bullet("可行路径：在 byted DevBox 上跑 Go sidecar 调 Kitex RPC，本地 Mac mini 通过 byted 内网 HTTP 调它"),
        bullet("备选路径：BPM 申请 TQS app 走 SQL 查 lark 用户表（也需要 DevBox 跑因 paas-gw 不通）"),
        h(3, "性能 / 容量"),
        bullet("Dashboard 首屏延迟：删 Google Fonts + 换 jsdelivr CDN，预计 5.8s → 1.5s"),
        bullet("Lark Base 单表 50k 行限制：当前 chats 表已接近上限，未来按月分表"),
        bullet("如用户规模 ×5 后：消息写入 500ms 缓冲批量化，减少 Base API 调用"),
        divider(),
        p("最后更新：2026-05-15。本文由 scripts/rewrite_docx.py V3 通过 docx API 重写。"),
    ]))

    return items



def main():
    if not (os.environ.get("LARK_APP_ID") and os.environ.get("LARK_APP_SECRET")):
        print("ERR: need LARK_APP_ID / LARK_APP_SECRET", file=sys.stderr)
        return 2
    doc = LarkDocxClient.from_env(DOC_ID)
    doc.write(build_doc_items(), mode="rewrite")
    return 0


if __name__ == "__main__":
    sys.exit(main())
