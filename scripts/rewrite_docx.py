#!/usr/bin/env python3
"""Chorus Docx 完整重写脚本（V2：用 SVG 替换所有结构化代码块）。

Items 数组顺序驱动：每个 item 是一组 blocks 或一张 SVG 图。
SVG 通过 image block 上传到文档。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

DOC_ID = os.getenv("CHORUS_DOC_ID", "VqlCdpASboikidxVuTMcth1rnAh")
LARK_OPEN = "https://open.feishu.cn"


# ─── HTTP / API ─────────────────────────────────────────────────────────

def lark_token() -> str:
    req = urllib.request.Request(
        f"{LARK_OPEN}/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({"app_id": os.environ["LARK_APP_ID"], "app_secret": os.environ["LARK_APP_SECRET"]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["tenant_access_token"]


def api(method: str, path: str, token: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{LARK_OPEN}{path}",
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {msg[:500]}") from e


# ─── block builders ──────────────────────────────────────────────────────

def h(level: int, text: str) -> dict:
    btype = {1: 3, 2: 4, 3: 5, 4: 6}[level]
    return {"block_type": btype, f"heading{level}": {"elements": [{"text_run": {"content": text}}], "style": {}}}


def p(text: str) -> dict:
    return {"block_type": 2, "text": {"elements": [{"text_run": {"content": text}}], "style": {}}}


def bullet(text: str) -> dict:
    return {"block_type": 12, "bullet": {"elements": [{"text_run": {"content": text}}], "style": {}}}


def divider() -> dict:
    return {"block_type": 22, "divider": {}}


def image_placeholder() -> dict:
    return {"block_type": 27, "image": {"token": ""}}


# ─── SVG helpers ─────────────────────────────────────────────────────────

SVG_STYLE = """
  .box { stroke-width: 1.5; }
  .label { font-family: -apple-system, "PingFang SC", "Helvetica Neue", sans-serif; font-size: 14px; fill: #111827; }
  .label-bold { font-weight: 700; }
  .label-sm { font-size: 11px; fill: #6b7280; }
  .label-mono { font-family: "JetBrains Mono", "SF Mono", monospace; font-size: 11px; fill: #4b5563; }
  .label-mono-md { font-family: "JetBrains Mono", "SF Mono", monospace; font-size: 13px; fill: #1f2937; }
  .section-title { font-family: -apple-system, sans-serif; font-weight: 700; font-size: 13px; fill: #111827; }
"""


# ─── SVG 1: 架构总图 ────────────────────────────────────────────────────
ARCH_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 760" width="1280" height="760">
  <style>
    {SVG_STYLE}
    .arrow {{ stroke: #4b5563; stroke-width: 1.8; fill: none; }}
    .arrow-async {{ stroke: #059669; stroke-dasharray: 5,3; }}
    .arrow-label {{ font-family: -apple-system, sans-serif; font-size: 10px; fill: #4b5563; }}
  </style>
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#4b5563" />
    </marker>
    <marker id="arr-a" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#059669" />
    </marker>
  </defs>
  <rect class="box" x="40" y="30" width="220" height="80" rx="8" fill="#fef3c7" stroke="#d97706"/>
  <text class="label label-bold" x="150" y="60" text-anchor="middle">飞书 (Lark)</text>
  <text class="label label-sm" x="150" y="80" text-anchor="middle">webhook 事件 / Base API</text>
  <text class="label label-sm" x="150" y="96" text-anchor="middle">App: cli_a75bb415d8ff9013</text>
  <rect class="box" x="340" y="30" width="240" height="80" rx="8" fill="#fee2e2" stroke="#dc2626"/>
  <text class="label label-bold" x="460" y="55" text-anchor="middle">CF Worker</text>
  <text class="label-mono" x="460" y="74" text-anchor="middle">chorus.xiaomiao.win</text>
  <text class="label label-sm" x="460" y="92" text-anchor="middle">白名单 + Edge cache 300s</text>
  <rect class="box" x="660" y="30" width="220" height="80" rx="8" fill="#fee2e2" stroke="#dc2626"/>
  <text class="label label-bold" x="770" y="55" text-anchor="middle">cloudflared tunnel</text>
  <text class="label-mono" x="770" y="74" text-anchor="middle">--protocol http2</text>
  <text class="label label-sm" x="770" y="92" text-anchor="middle">chorus-origin.xiaomiao.win</text>
  <rect class="box" x="960" y="30" width="280" height="80" rx="8" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="1100" y="55" text-anchor="middle">浏览器 Dashboard</text>
  <text class="label-mono" x="1100" y="74" text-anchor="middle">chorus.xiaomiao.win/</text>
  <text class="label label-sm" x="1100" y="92" text-anchor="middle">React 18 + JSX runtime</text>
  <rect class="box" x="40" y="180" width="1200" height="280" rx="8" fill="#dbeafe" stroke="#2563eb"/>
  <text class="section-title" x="60" y="206">Mac mini · FastAPI server.py (127.0.0.1:5678)</text>
  <rect class="box" x="60" y="230" width="280" height="100" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="200" y="252" text-anchor="middle">webhook handler</text>
  <text class="label-mono" x="200" y="272" text-anchor="middle">POST /lark/events</text>
  <text class="label label-sm" x="200" y="290" text-anchor="middle">ThreadPoolExecutor(12)</text>
  <text class="label label-sm" x="200" y="306" text-anchor="middle">event dedup (LRU 10k)</text>
  <text class="label label-sm" x="200" y="322" text-anchor="middle">5ms 入 SQLite 立即返回</text>
  <rect class="box" x="430" y="230" width="280" height="100" rx="6" fill="#d1fae5" stroke="#059669"/>
  <text class="label label-bold" x="570" y="252" text-anchor="middle">SQLite 真源</text>
  <text class="label-mono" x="570" y="272" text-anchor="middle">chorus_local.db</text>
  <text class="label label-sm" x="570" y="290" text-anchor="middle">chats / members / messages</text>
  <text class="label label-sm" x="570" y="306" text-anchor="middle">primary_synced / secondary_synced</text>
  <text class="label label-sm" x="570" y="322" text-anchor="middle">WAL · ~24MB · 40k 行</text>
  <rect class="box" x="800" y="230" width="200" height="60" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="900" y="252" text-anchor="middle">primary-sync</text>
  <text class="label label-sm" x="900" y="272" text-anchor="middle">IntervalTrigger 10s</text>
  <rect class="box" x="800" y="320" width="200" height="60" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="900" y="342" text-anchor="middle">secondary-sync</text>
  <text class="label label-sm" x="900" y="362" text-anchor="middle">IntervalTrigger 15s</text>
  <rect class="box" x="60" y="370" width="280" height="70" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="200" y="392" text-anchor="middle">Dashboard endpoint</text>
  <text class="label-mono" x="200" y="412" text-anchor="middle">GET /src/data.jsx</text>
  <text class="label label-sm" x="200" y="430" text-anchor="middle">top-500 群 · 每群 20 条消息</text>
  <rect class="box" x="430" y="370" width="280" height="70" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="570" y="392" text-anchor="middle">APScheduler</text>
  <text class="label label-sm" x="570" y="410" text-anchor="middle">daily-sync · external-join</text>
  <text class="label label-sm" x="570" y="426" text-anchor="middle">bulk-stats · cf-prewarm · 2×sync</text>
  <rect class="box" x="1050" y="180" width="190" height="120" rx="8" fill="#ede9fe" stroke="#7c3aed"/>
  <text class="label label-bold" x="1145" y="204" text-anchor="middle">旧 Base (primary)</text>
  <text class="label-mono" x="1145" y="220" text-anchor="middle">PnRtbGm...</text>
  <text class="label label-sm" x="1145" y="240" text-anchor="middle">机器人群列表</text>
  <text class="label label-sm" x="1145" y="256" text-anchor="middle">机器人群消息记录</text>
  <text class="label label-sm" x="1145" y="272" text-anchor="middle">机器人群成员记录</text>
  <text class="label label-sm" x="1145" y="290" text-anchor="middle">(验证期)</text>
  <rect class="box" x="1050" y="320" width="190" height="120" rx="8" fill="#ede9fe" stroke="#7c3aed"/>
  <text class="label label-bold" x="1145" y="344" text-anchor="middle">新 Base (secondary)</text>
  <text class="label-mono" x="1145" y="360" text-anchor="middle">G42ybVmN...</text>
  <text class="label label-sm" x="1145" y="380" text-anchor="middle">Helix · 项目门户</text>
  <text class="label label-sm" x="1145" y="396" text-anchor="middle">三同名表 · 字段一致</text>
  <text class="label label-sm" x="1145" y="412" text-anchor="middle">回补 + 持续 dual-sync</text>
  <text class="label label-sm" x="1145" y="428" text-anchor="middle">(迁移目标)</text>
  <rect class="box" x="40" y="510" width="600" height="100" rx="8" fill="#f3f4f6" stroke="#374151"/>
  <text class="section-title" x="60" y="534">夜间 daily-sync · 直接对接飞书 API</text>
  <text class="label label-sm" x="60" y="556">每天 00:00 SGT 全量同步：拉群列表 + 成员 + 消息 → 旧 Base + 重 seed SQLite</text>
  <text class="label label-sm" x="60" y="574">sync_feishu_groups_to_base.py · --lite-mode --refresh-metadata-tables</text>
  <text class="label label-sm" x="60" y="592">耗时 ~10min · 24k 群 / 43k 消息 / 1.2k 成员</text>
  <rect class="box" x="680" y="510" width="560" height="100" rx="8" fill="#f3f4f6" stroke="#374151"/>
  <text class="section-title" x="700" y="534">每晚 external-join · bot 自动入群</text>
  <text class="label label-sm" x="700" y="556">22:00 SGT · 通过 15 个授权用户 access_token 列出他们所在外部群</text>
  <text class="label label-sm" x="700" y="574">--active-since-days 30 跳过死群 · diff bot 已在群 → 自动拉机器人入新群</text>
  <text class="label label-sm" x="700" y="592">代理 worker: feishu-bot.xiaomiao.win (KV 存 OAuth token)</text>
  <path class="arrow" d="M260,70 L335,70" marker-end="url(#arr)"/>
  <text class="arrow-label" x="297" y="62" text-anchor="middle">webhook</text>
  <path class="arrow" d="M580,70 L655,70" marker-end="url(#arr)"/>
  <path class="arrow" d="M880,70 L955,70" marker-end="url(#arr)"/>
  <path class="arrow" d="M770,115 L770,180 L200,180 L200,225" marker-end="url(#arr)"/>
  <text class="arrow-label" x="500" y="174" text-anchor="middle">tunnel → 127.0.0.1:5678</text>
  <path class="arrow" d="M340,280 L425,280" marker-end="url(#arr)"/>
  <text class="arrow-label" x="382" y="272" text-anchor="middle">5ms</text>
  <path class="arrow arrow-async" d="M710,260 L795,260" marker-end="url(#arr-a)"/>
  <text class="arrow-label" x="752" y="252" text-anchor="middle">异步</text>
  <path class="arrow arrow-async" d="M710,310 L795,350" marker-end="url(#arr-a)"/>
  <path class="arrow arrow-async" d="M1000,260 L1045,240" marker-end="url(#arr-a)"/>
  <path class="arrow arrow-async" d="M1000,350 L1045,380" marker-end="url(#arr-a)"/>
  <path class="arrow" d="M570,330 L570,355 L200,355 L200,370" marker-end="url(#arr)"/>
  <text class="arrow-label" x="385" y="348" text-anchor="middle">SQLite 直读</text>
  <path class="arrow" d="M340,405 L1100,405 L1100,110" marker-end="url(#arr)"/>
  <path class="arrow" d="M340,510 L340,140 L260,140 L260,110" marker-end="url(#arr)"/>
  <text class="arrow-label" x="350" y="320" text-anchor="middle">读飞书 群/消息/成员 API</text>
  <path class="arrow" d="M640,560 L850,560 L850,260 L1045,260" marker-end="url(#arr)"/>
  <rect x="40" y="640" width="500" height="100" rx="6" fill="#ffffff" stroke="#d1d5db"/>
  <text class="section-title" x="60" y="660">图例</text>
  <line x1="60" y1="678" x2="100" y2="678" stroke="#4b5563" stroke-width="1.8" marker-end="url(#arr)"/>
  <text class="label-sm" x="110" y="682">同步写入（webhook 入 SQLite / dashboard 直读）</text>
  <line x1="60" y1="700" x2="100" y2="700" stroke="#059669" stroke-width="1.8" stroke-dasharray="5,3" marker-end="url(#arr-a)"/>
  <text class="label-sm" x="110" y="704">异步 sync worker（SQLite → Base，可重试可监控）</text>
  <text class="label-sm" x="60" y="725">监控：GET /admin/sync-stats · /admin/jobs · /admin/local-db-stats</text>
</svg>
"""


# ─── SVG 2: cron 时间轴 ─────────────────────────────────────────────────
CRON_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 460" width="1280" height="460">
  <style>{SVG_STYLE}
    .timeline {{ stroke: #d1d5db; stroke-width: 2; }}
    .tick {{ stroke: #9ca3af; stroke-width: 1; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">cron 定时任务（按 SGT 时间排）</text>

  <!-- 24h 时间轴 -->
  <line class="timeline" x1="80" y1="100" x2="1240" y2="100"/>
  <g font-family="-apple-system, sans-serif" font-size="11" fill="#6b7280">
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
  <!-- 小刻度 -->
  <g>
    <line class="tick" x1="80" y1="95" x2="80" y2="105"/>
    <line class="tick" x1="226" y1="95" x2="226" y2="105"/>
    <line class="tick" x1="372" y1="95" x2="372" y2="105"/>
    <line class="tick" x1="518" y1="95" x2="518" y2="105"/>
    <line class="tick" x1="664" y1="95" x2="664" y2="105"/>
    <line class="tick" x1="810" y1="95" x2="810" y2="105"/>
    <line class="tick" x1="956" y1="95" x2="956" y2="105"/>
    <line class="tick" x1="1102" y1="95" x2="1102" y2="105"/>
    <line class="tick" x1="1240" y1="95" x2="1240" y2="105"/>
  </g>

  <!-- daily-sync 00:00 SGT -->
  <circle cx="80" cy="100" r="9" fill="#2563eb"/>
  <line stroke="#2563eb" stroke-width="2" x1="80" y1="100" x2="80" y2="160"/>
  <rect x="50" y="160" width="240" height="80" rx="6" fill="#dbeafe" stroke="#2563eb"/>
  <text class="label label-bold" x="170" y="182" text-anchor="middle">daily-sync · 00:00</text>
  <text class="label label-sm" x="170" y="200" text-anchor="middle">cron[hour=16,minute=0] UTC</text>
  <text class="label label-sm" x="170" y="216" text-anchor="middle">飞书全量 → 旧 Base + SQLite</text>
  <text class="label label-sm" x="170" y="232" text-anchor="middle">~10min · 24k 群 / 43k 消息</text>

  <!-- bulk-stats-refresh 20:00 SGT (12:00 UTC) -->
  <circle cx="1054" cy="100" r="9" fill="#d97706"/>
  <line stroke="#d97706" stroke-width="2" x1="1054" y1="100" x2="1054" y2="160"/>
  <rect x="934" y="160" width="240" height="80" rx="6" fill="#fef3c7" stroke="#d97706"/>
  <text class="label label-bold" x="1054" y="182" text-anchor="middle">bulk-stats-refresh · 20:00</text>
  <text class="label label-sm" x="1054" y="200" text-anchor="middle">cron[hour=12,minute=0] UTC</text>
  <text class="label label-sm" x="1054" y="216" text-anchor="middle">消息广播效果统计回流</text>
  <text class="label label-sm" x="1054" y="232" text-anchor="middle">~7s</text>

  <!-- external-join 22:00 SGT (14:00 UTC) -->
  <circle cx="1152" cy="100" r="9" fill="#7c3aed"/>
  <line stroke="#7c3aed" stroke-width="2" x1="1152" y1="100" x2="1152" y2="270"/>
  <rect x="1010" y="270" width="220" height="80" rx="6" fill="#ede9fe" stroke="#7c3aed"/>
  <text class="label label-bold" x="1120" y="292" text-anchor="middle">external-join · 22:00</text>
  <text class="label label-sm" x="1120" y="310" text-anchor="middle">cron[hour=14,minute=0] UTC</text>
  <text class="label label-sm" x="1120" y="326" text-anchor="middle">bot 自动入授权用户外部群</text>
  <text class="label label-sm" x="1120" y="342" text-anchor="middle">~10min · 跳 30 天无活跃</text>

  <!-- interval 任务（持续型） -->
  <text class="section-title" x="40" y="390" font-size="13">持续型（IntervalTrigger）</text>
  <rect x="40" y="400" width="240" height="40" rx="6" fill="#d1fae5" stroke="#059669"/>
  <text class="label label-bold" x="60" y="420">primary-sync</text>
  <text class="label label-sm" x="60" y="434">10s · SQLite → 旧 Base</text>
  <rect x="300" y="400" width="240" height="40" rx="6" fill="#d1fae5" stroke="#059669"/>
  <text class="label label-bold" x="320" y="420">secondary-sync</text>
  <text class="label label-sm" x="320" y="434">15s · SQLite → 新 Base</text>
  <rect x="560" y="400" width="240" height="40" rx="6" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="580" y="420">cf-prewarm</text>
  <text class="label label-sm" x="580" y="434">240s · CF Edge cache 预热</text>
</svg>
"""


# ─── SVG 3: 监控端点表 ─────────────────────────────────────────────────
ENDPOINTS_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 420" width="1280" height="420">
  <style>{SVG_STYLE}
    .row {{ fill: #f9fafb; stroke: #e5e7eb; stroke-width: 1; }}
    .row-alt {{ fill: #ffffff; stroke: #e5e7eb; }}
    .verb-get {{ fill: #10b981; }}
    .verb-post {{ fill: #3b82f6; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">监控端点（http://127.0.0.1:5678）</text>

  <!-- header -->
  <rect x="40" y="60" width="1200" height="34" fill="#1f2937"/>
  <text x="60" y="83" fill="#ffffff" font-family="-apple-system" font-weight="700" font-size="13">METHOD</text>
  <text x="170" y="83" fill="#ffffff" font-family="-apple-system" font-weight="700" font-size="13">路径</text>
  <text x="550" y="83" fill="#ffffff" font-family="-apple-system" font-weight="700" font-size="13">用途</text>

  <!-- rows -->
  <g font-family="-apple-system" font-size="13">
    <rect class="row" x="40" y="94" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="106" width="50" height="20" rx="3"/>
    <text x="85" y="121" fill="#ffffff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="121">/healthz</text>
    <text class="label" x="550" y="121">存活探测（uptime / 状态码检查）</text>

    <rect class="row-alt" x="40" y="134" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="146" width="50" height="20" rx="3"/>
    <text x="85" y="161" fill="#ffffff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="161">/admin/jobs</text>
    <text class="label" x="550" y="161">列出全部 cron + next_run 时间</text>

    <rect class="row" x="40" y="174" width="1200" height="40"/>
    <rect class="verb-post" x="60" y="186" width="50" height="20" rx="3"/>
    <text x="85" y="201" fill="#ffffff" font-weight="700" text-anchor="middle" font-size="11">POST</text>
    <text class="label-mono-md" x="170" y="201">/admin/run/{{job_id}}</text>
    <text class="label" x="550" y="201">手动触发某 job（异步排队，看日志确认完成）</text>

    <rect class="row-alt" x="40" y="214" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="226" width="50" height="20" rx="3"/>
    <text x="85" y="241" fill="#ffffff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="241">/admin/local-db-stats</text>
    <text class="label" x="550" y="241">SQLite 行数 + db 大小 + 上次 seed 时间</text>

    <rect class="row" x="40" y="254" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="266" width="50" height="20" rx="3"/>
    <text x="85" y="281" fill="#ffffff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="281">/admin/sync-stats</text>
    <text class="label" x="550" y="281">双 Base 队列深度 + 失败计数（看 dual-sync 健康度）</text>

    <rect class="row-alt" x="40" y="294" width="1200" height="40"/>
    <rect class="verb-get" x="60" y="306" width="50" height="20" rx="3"/>
    <text x="85" y="321" fill="#ffffff" font-weight="700" text-anchor="middle" font-size="11">GET</text>
    <text class="label-mono-md" x="170" y="321">/lark/events/recent</text>
    <text class="label" x="550" y="321">最近 webhook 事件 + 各 event_type 统计</text>
  </g>

  <text class="label-sm" x="40" y="380">公网入口：https://chorus.xiaomiao.win/  | 仅放部分 GET + POST /lark/events 白名单，admin 路径不暴露</text>
</svg>
"""


# ─── SVG 4: env 配置卡片 ─────────────────────────────────────────────────
ENV_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 540" width="1280" height="540">
  <style>{SVG_STYLE}
    .group {{ fill: #ffffff; stroke: #d1d5db; }}
    .group-title {{ font-family: -apple-system; font-weight: 700; font-size: 14px; fill: #1f2937; }}
    .req {{ fill: #dc2626; }}
    .opt {{ fill: #9ca3af; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">关键配置（~/chorus-lark-monitor/.env）</text>

  <!-- 飞书凭据 -->
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
    <text class="label-sm" x="320" y="175">旧 Base URL（验证期 = primary）</text>
    <circle class="req" cx="612" cy="171" r="5"/>
    <text class="label-mono-md" x="60" y="205">LARK_BASE_URL_SECONDARY</text>
    <text class="label-sm" x="320" y="205">新 Base URL（迁移目标，可选）</text>
    <circle class="opt" cx="612" cy="201" r="5"/>
  </g>

  <!-- group-join 代理 -->
  <rect class="group" x="660" y="60" width="580" height="190" rx="8"/>
  <rect x="660" y="60" width="580" height="32" rx="8" fill="#ede9fe"/>
  <text class="group-title" x="680" y="82">外部群入群代理（feishu-bot-proxy）</text>
  <g>
    <text class="label-mono-md" x="680" y="115">GROUP_JOIN_PROXY_URL</text>
    <text class="label-sm" x="930" y="115">https://feishu-bot.xiaomiao.win</text>
    <circle class="req" cx="1212" cy="111" r="5"/>
    <text class="label-mono-md" x="680" y="145">GROUP_JOIN_ADMIN_TOKEN</text>
    <text class="label-sm" x="930" y="145">CF Worker Secret 鉴权</text>
    <circle class="req" cx="1212" cy="141" r="5"/>
    <text class="label-sm" x="680" y="180">备份：~/chorus-lark-monitor/.group_join_admin_token.local</text>
    <text class="label-sm" x="680" y="200">CF Worker secret 不可读，本地备份方便取值；mode 600 gitignored</text>
  </g>

  <!-- 应用调优 -->
  <rect class="group" x="40" y="280" width="1200" height="220" rx="8"/>
  <rect x="40" y="280" width="1200" height="32" rx="8" fill="#d1fae5"/>
  <text class="group-title" x="60" y="302">应用调优（可选，都有默认值）</text>
  <g>
    <text class="label-mono-md" x="60" y="335">WEB_MAX_GROUPS=500</text>
    <text class="label-sm" x="380" y="335">dashboard 展示群数上限（top-N 活跃）</text>
    <circle class="opt" cx="1212" cy="331" r="5"/>
    <text class="label-mono-md" x="60" y="365">WEB_MAX_MESSAGES_PER_GROUP=20</text>
    <text class="label-sm" x="380" y="365">每群展示消息数</text>
    <circle class="opt" cx="1212" cy="361" r="5"/>
    <text class="label-mono-md" x="60" y="395">LARK_EVENT_POOL_SIZE=12</text>
    <text class="label-sm" x="380" y="395">webhook 后台线程池大小</text>
    <circle class="opt" cx="1212" cy="391" r="5"/>
    <text class="label-mono-md" x="60" y="425">EXTERNAL_GROUP_JOIN_ACTIVE_SINCE_DAYS=30</text>
    <text class="label-sm" x="380" y="425">外部群活跃度过滤（0=关闭）</text>
    <circle class="opt" cx="1212" cy="421" r="5"/>
    <text class="label-mono-md" x="60" y="455">CF_PREWARM_URL=https://chorus.xiaomiao.win</text>
    <text class="label-sm" x="380" y="455">CF Edge cache 预热目标</text>
    <circle class="opt" cx="1212" cy="451" r="5"/>
    <text class="label-mono-md" x="60" y="485">ENABLE_SCHEDULED_JOBS=true</text>
    <text class="label-sm" x="380" y="485">禁用所有 cron（调试用）</text>
    <circle class="opt" cx="1212" cy="481" r="5"/>
  </g>

  <!-- 图例 -->
  <circle class="req" cx="60" cy="525" r="5"/>
  <text class="label-sm" x="75" y="529">必填</text>
  <circle class="opt" cx="135" cy="525" r="5"/>
  <text class="label-sm" x="150" y="529">可选（有默认值）</text>
</svg>
"""


# ─── SVG 5: 代码文件树 ─────────────────────────────────────────────────
FILES_SVG = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 600" width="1280" height="600">
  <style>{SVG_STYLE}
    .folder {{ fill: #fef3c7; stroke: #d97706; }}
    .file-py {{ fill: #dbeafe; stroke: #2563eb; }}
    .file-js {{ fill: #fef9c3; stroke: #ca8a04; }}
    .file-md {{ fill: #f3f4f6; stroke: #6b7280; }}
    .tree-line {{ stroke: #9ca3af; stroke-width: 1.2; fill: none; }}
  </style>
  <text class="section-title" x="40" y="36" font-size="16">关键代码文件</text>

  <!-- 根目录 -->
  <text class="label-mono-md" x="40" y="80" font-weight="700">chorus-lark-monitor/</text>

  <!-- 主入口 -->
  <line class="tree-line" x1="70" y1="90" x2="70" y2="240"/>
  <g>
    <line class="tree-line" x1="70" y1="110" x2="90" y2="110"/>
    <rect class="file-py" x="95" y="98" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="116" font-weight="700">server.py</text>
    <text class="label-sm" x="345" y="116">FastAPI + APScheduler 主入口</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="140" x2="90" y2="140"/>
    <rect class="file-py" x="95" y="128" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="146" font-weight="700">local_db.py</text>
    <text class="label-sm" x="345" y="146">SQLite schema + upsert/load helpers</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="170" x2="90" y2="170"/>
    <rect class="file-py" x="95" y="158" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="176">sync_feishu_groups_to_base.py</text>
    <text class="label-sm" x="345" y="176">全量同步 + FeishuClient + Base schema 定义</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="200" x2="90" y2="200"/>
    <rect class="file-py" x="95" y="188" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="206">ensure_bot_in_external_chats.py</text>
    <text class="label-sm" x="345" y="206">外部群自动入群（--active-since-days）</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="230" x2="90" y2="230"/>
    <rect class="file-py" x="95" y="218" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="236">bulk_message_probe.py</text>
    <text class="label-sm" x="345" y="236">群发消息效果统计（refresh / send）</text>
  </g>

  <!-- scripts/ -->
  <text class="label-mono-md" x="40" y="280" font-weight="700">scripts/</text>
  <line class="tree-line" x1="70" y1="290" x2="70" y2="380"/>
  <g>
    <line class="tree-line" x1="70" y1="310" x2="90" y2="310"/>
    <rect class="file-py" x="95" y="298" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="316">bootstrap_secondary_base.py</text>
    <text class="label-sm" x="345" y="316">一次性建 3 张表到新 Base</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="340" x2="90" y2="340"/>
    <rect class="file-py" x="95" y="328" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="346">rewrite_docx.py</text>
    <text class="label-sm" x="345" y="346">飞书 Docx 文档全量重写（本文档生成器）</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="370" x2="90" y2="370"/>
    <rect class="file-py" x="95" y="358" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="376">write_docx_project.py</text>
    <text class="label-sm" x="345" y="376">追加式写法（已过时，留作参考）</text>
  </g>

  <!-- deployment/ -->
  <text class="label-mono-md" x="40" y="420" font-weight="700">deployment/</text>
  <line class="tree-line" x1="70" y1="430" x2="70" y2="490"/>
  <g>
    <line class="tree-line" x1="70" y1="450" x2="90" y2="450"/>
    <rect class="folder" x="95" y="438" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="456">worker/</text>
    <text class="label-sm" x="345" y="456">CF Worker 源码（wrangler 部署）</text>
  </g>
  <g>
    <line class="tree-line" x1="70" y1="480" x2="90" y2="480"/>
    <rect class="file-md" x="95" y="468" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="486">com.feishu-chat.{{server,tunnel}}.plist</text>
    <text class="label-sm" x="345" y="486">launchd 任务定义</text>
  </g>

  <!-- web/ -->
  <text class="label-mono-md" x="40" y="530" font-weight="700">web/</text>
  <line class="tree-line" x1="70" y1="540" x2="70" y2="580"/>
  <g>
    <line class="tree-line" x1="70" y1="560" x2="90" y2="560"/>
    <rect class="file-js" x="95" y="548" width="240" height="26" rx="4"/>
    <text class="label-mono-md" x="110" y="566">src/*.jsx · v2-styles.css</text>
    <text class="label-sm" x="345" y="566">dashboard 前端（React 18 + JSX 直跑）</text>
  </g>

  <!-- Legend -->
  <g transform="translate(700, 90)">
    <text class="section-title" x="0" y="0" font-size="13">类型图例</text>
    <rect class="file-py" x="0" y="20" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="34">Python 主代码 / 一次性脚本</text>
    <rect class="file-js" x="0" y="50" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="64">前端 / Worker（JS / JSX）</text>
    <rect class="folder" x="0" y="80" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="94">子目录</text>
    <rect class="file-md" x="0" y="110" width="40" height="20" rx="3"/>
    <text class="label-sm" x="50" y="124">配置 / 文档</text>
  </g>
</svg>
"""


# ─── 文档章节定义（items 数组）──────────────────────────────────────────

def items_text(blocks: list) -> dict:
    return {"type": "blocks", "data": blocks}


def items_image(svg: str, name: str = "chart.svg") -> dict:
    return {"type": "image", "svg": svg.encode("utf-8"), "name": name}


def build_doc_items() -> list:
    items: list = []

    # 标题 + 简介
    items.append(items_text([
        h(1, "Chorus Lark Monitor · 飞书群聊监控系统"),
        p("把飞书群聊的消息 / 成员变更 / 群事件实时汇聚到 SQLite 真源 + 多维表格双写，给客户运营提供「DR · 客户对话健康度」面板。"),
    ]))

    # 一、是什么
    items.append(items_text([
        h(2, "一、Chorus 是什么"),
        p("一个本机部署的飞书机器人群聊监控平台。bot 自动加入授权用户的外部群，实时捕获消息事件，聚合后给客户经理 / 主管看「群健康度看板」。"),
        h(3, "现状（2026-05-13）"),
        bullet("部署在 Mac mini（M4 / 32GB），launchd 跑 server.py（uvicorn）+ cloudflared tunnel"),
        bullet("公网入口：https://chorus.xiaomiao.win（Cloudflare Worker + Tunnel）"),
        bullet("当前数据规模：24029 群 / 43388 消息 / 1196 成员（来自 daily-sync 全量 + webhook 实时增量）"),
        bullet("15 个授权用户授权 bot 拉外部群；bot 已在 23611 个群里"),
        bullet("双 Lark Base 写入：旧 Base 验证期 · 新 Base Helix · 项目门户 迁移目标"),
    ]))

    # 二、整体架构 + SVG
    items.append(items_text([
        h(2, "二、整体架构"),
        p("数据从飞书出发，经过 CF Worker + tunnel 进入本机 server.py，落到 SQLite 真源，再由两个后台 sync worker 异步推送到两个多维表格。dashboard 直读 SQLite，绕开 Base 限频。"),
    ]))
    items.append(items_image(ARCH_SVG, "arch.svg"))
    items.append(items_text([
        h(3, "三段职责"),
        bullet("Webhook 入站：5ms 内只写 SQLite，飞书永不超时（< 3s 要求）"),
        bullet("后台 sync worker：异步把 SQLite 行批量推到 Base，失败可重试可监控"),
        bullet("Dashboard 读路径：SQLite 直读，绕过 Base 全部限频"),
    ]))

    # 三、SQLite 真源
    items.append(items_text([
        h(2, "三、SQLite 真源（chorus_local.db）"),
        p("本机持久层，WAL 模式支持并发读 + 单写。所有 Base 数据都从这里 fanout。"),
        h(3, "4 张表"),
        bullet("chats — 群信息（chat_id 主键），含 record_id / secondary_record_id"),
        bullet("members — 群成员（chat_id + member_open_id 复合主键）"),
        bullet("messages — 消息流（msg_id 主键，索引 chat_id + time_ms）"),
        bullet("meta — KV 元数据（last_seeded_at 等）"),
        h(3, "sync state 字段（2026-05-13 架构升级后新增）"),
        bullet("primary_synced / secondary_synced — 0 = 待同步，1 = 已同步"),
        bullet("primary_record_id / secondary_record_id — 各 Base 写入返回的 record_id（消息 / 成员链接用）"),
        bullet("sync_attempts / sync_last_error — 重试次数 + 最近错误"),
    ]))

    # 四、Webhook
    items.append(items_text([
        h(2, "四、Webhook 事件处理"),
        p("server.py 暴露 POST /lark/events。事件分两类，CREATE 类异步走 SQLite + worker，UPDATE/DELETE 类同步操作旧 Base 已有 record_id。"),
        h(3, "CREATE 类（异步入 SQLite，worker 推 Base）"),
        bullet("im.message.receive_v1 — 新消息 → 写 messages 表"),
        bullet("im.chat.member.user.added_v1 — 成员入群 → 写 members 表"),
        bullet("im.chat.member.bot.added_v1 — bot 入新群 → 调 Lark API 抓 chat detail + 当前成员 + 24h 消息回填，全部入 SQLite"),
        h(3, "UPDATE/DELETE 类（同步写旧 Base，量小不解耦）"),
        bullet("im.message.recalled_v1 — 消息撤回，标 is_deleted = 1"),
        bullet("im.chat.member.user.deleted_v1 — 成员退群，移除 members 行"),
        bullet("im.chat.disbanded_v1 — 群解散，标 chat 为 dissolved"),
        h(3, "防抖与并发"),
        bullet("event_id LRU set（10k 容量）防飞书重发"),
        bullet("ThreadPoolExecutor(max_workers=12) 处理 webhook 事件"),
        bullet("SQLite WAL 单写者自动 serialize，无写冲突"),
    ]))

    # 五、多维表格
    items.append(items_text([
        h(2, "五、多维表格 schema（旧 + 新双写）"),
        h(3, "旧 Base · 验证期"),
        bullet("token: PnRtbGmTpaVXwDsWBWPcPaEpnwh"),
        bullet("机器人群列表 tbl7PZ9s9yoKSHtJ"),
        bullet("机器人群消息记录 tbl9oNBOQrekT1O4"),
        bullet("机器人群成员记录 tblODVD4U82fn21P"),
        h(3, "新 Base · Helix · 项目门户 · 迁移目标"),
        bullet("token: G42ybVmN9aAeYdsHW06cysTonmF"),
        bullet("机器人群列表 tblXhxEs8Y5IvFbw"),
        bullet("机器人群消息记录 tblK0WYR1ebTarjR"),
        bullet("机器人群成员记录 tbl7aSVqPLBk1iRv"),
        p("3 张表字段完全相同，由 sync_feishu_groups_to_base.py 的 CHAT_FIELD_DEFS / BASE_MESSAGE_FIELD_DEFS / BASE_MEMBER_FIELD_DEFS 定义。bootstrap_secondary_base.py 一键复制结构到新 Base。"),
        h(3, "迁移切换路径"),
        bullet("等 secondary 队列清零（参 /admin/sync-stats）"),
        bullet("人工抽查新 Base 字段完整、数据一致"),
        bullet("把 .env 里 LARK_BASE_URL 和 LARK_BASE_URL_SECONDARY 对调 + 重启 server"),
        bullet("完全切换后 unset LARK_BASE_URL_SECONDARY（旧 Base sync worker 自动 noop）"),
    ]))

    # 六、定时任务 + cron SVG
    items.append(items_text([h(2, "六、定时任务（APScheduler）")]))
    items.append(items_image(CRON_SVG, "cron.svg"))
    items.append(items_text([
        h(3, "daily-sync 详解"),
        bullet("调用 sync_feishu_groups_to_base.py main()，参数 --scheduled-daily --lite-mode --refresh-metadata-tables"),
        bullet("--refresh-metadata-tables 会重建表，结束后必须 _invalidate_lark_state() 清缓存"),
        bullet("结束时自动 local_db.seed_from_lark_base() 重 seed 本地副本做对账"),
        h(3, "external-join 详解"),
        bullet("调用 ensure_bot_in_external_chats.py，依赖 feishu-bot-proxy 代理拿用户授权 access_token"),
        bullet("通过用户 access_token 列出他们所在群，diff 出 bot 不在的群"),
        bullet("--active-since-days 30 过滤：跳过 30 天无消息的死群"),
        bullet("失败码常见：232017 用户非群主 / 232009 群已解散，靠 --allow-chat-failures 单群失败不中断"),
    ]))

    # 七、公网部署
    items.append(items_text([
        h(2, "七、公网部署（CF Worker + Tunnel）"),
        h(3, "Cloudflare 资源"),
        bullet("Account: Kelan656691@gmail.com (acct 2e2b291e8f3e011ca7824f19bcb77236)"),
        bullet("Zone: xiaomiao.win (81498ac216563761c63636b270a4caf1)"),
        bullet("Tunnel: chorus-lark-monitor · UUID 9c7e347a-3b11-47d7-8e5f-18bb5d463397，必须 --protocol http2"),
        bullet("Worker: chorus-lark-events-gateway · 路径白名单 + caches.default 边缘缓存 300s"),
        bullet("DNS: chorus.xiaomiao.win (Worker custom_domain), chorus-origin.xiaomiao.win (CNAME → tunnel UUID)"),
        h(3, "Worker 关键逻辑"),
        bullet("白名单：POST /lark/events + GET /、/src/*、/api/dashboard/*"),
        bullet("Edge cache：仅缓存 GET /src/*；origin 回 no-store/no-cache/private 时跳过写 cache"),
        bullet("x-cache 响应头：HIT/MISS/BYPASS 三态，便于诊断"),
        h(3, "feishu-bot-proxy（另一个 Worker）"),
        bullet("绑 feishu-bot.xiaomiao.win，做用户 OAuth + access_token 中转"),
        bullet("KV namespace GROUP_JOIN_TOKENS 存用户 token；secret GROUP_JOIN_ADMIN_TOKEN 鉴权"),
    ]))

    # 八、Dashboard
    items.append(items_text([
        h(2, "八、Dashboard 前端"),
        p("地址：https://chorus.xiaomiao.win/"),
        bullet("前端栈：React 18 UMD（unpkg）+ Babel standalone（unpkg）+ JSX 源码"),
        bullet("data.jsx 由 server.py 动态生成，从 SQLite 取 top-N 活跃群（默认 N=500）+ 每群最近 20 条消息"),
        bullet("payload 体积：~10MB raw / ~1.2MB gzipped，首次加载 ~1.8s（CF cache hit）"),
        bullet("已知瓶颈：Google Fonts + unpkg.com 境外 CDN 慢，首访 3-5s 主要花在这"),
    ]))

    # 九、运维 + endpoints SVG
    items.append(items_text([h(2, "九、运维 / 监控")]))
    items.append(items_image(ENDPOINTS_SVG, "endpoints.svg"))
    items.append(items_text([
        h(3, "日志文件"),
        bullet("~/chorus-lark-monitor/logs/server.err.log — server.py 主日志"),
        bullet("~/chorus-lark-monitor/logs/server.out.log — uvicorn 访问日志 + 脚本 stdout"),
        bullet("~/chorus-lark-monitor/logs/tunnel.err.log — cloudflared tunnel 错误"),
        h(3, "launchd 任务"),
        bullet("com.feishu-chat.server — uvicorn server:app 监听 127.0.0.1:5678"),
        bullet("com.feishu-chat.tunnel — cloudflared tunnel run，token 在 ~/.cloudflared/chorus-lark-monitor.token (mode 600)"),
        p("plist 在 deployment/ 下，launchctl unload / load 重启。"),
    ]))

    # 十、env + ENV SVG
    items.append(items_text([h(2, "十、关键配置（.env）")]))
    items.append(items_image(ENV_SVG, "env.svg"))

    # 十一、代码文件 + tree SVG
    items.append(items_text([h(2, "十一、关键代码文件")]))
    items.append(items_image(FILES_SVG, "files.svg"))

    # 十二、踩过的坑
    items.append(items_text([
        h(2, "十二、踩过的坑（按时间倒序）"),
        bullet("CF Worker 覆写 cache-control 写 caches.default，遇到 warm-up 空 payload + prewarm 循环 → 看板永远 0 群。修法：origin 回 no-store 时 Worker 跳过写 cache"),
        bullet("ensure_bot_in_external_chats.py 看 .env 里 GROUP_JOIN_ADMIN_TOKEN= 是空字符串就抛 RuntimeError 0s 挂掉。修法：CF API 直接 PUT 新 secret，三处同步"),
        bullet("_create_member_rows_safely 一次性写 200+ 行触发 800010701 invalid_request。修法：按 200 切片"),
        bullet("Babel standalone 编译 10MB+ JSON 在浏览器 OOM。修法：data.jsx 不打 type=text/babel，按普通 JS 加载"),
        bullet("cloudflared 默认 QUIC 协议路由到美西节点 timeout。修法：--protocol http2"),
        bullet("Lark Base 限频 800004135 OpenAPIBatchAddRecords ~20/s/table。修法：FeishuClient.request 加 RATE_LIMIT_CODES set 自动 retry"),
        bullet("daily-sync --refresh-metadata-tables 重建表后旧 table_id 失效。修法：daily-sync 完成后 _invalidate_lark_state() + 重 seed 本地 SQLite"),
        bullet("Lark Docx list children API 返回字段是 items（POST 返回 children），混用导致老内容删不干净"),
    ]))

    # 十三、待办
    items.append(items_text([
        h(2, "十三、待办 / 后续演进"),
        bullet("等 secondary 回补稳定 → 切换 primary / secondary"),
        bullet("dashboard 首屏延迟根本治理：删 Google Fonts + 换 jsdelivr CDN，预计 5.8s → 1.5s"),
        bullet("如用户规模 ×5 后：消息写入按 500ms 缓冲批量化，减少 Base API 调用"),
        bullet("Lark Base 单表 ~100k 行后需要按月分表，避免触上限"),
        divider(),
        p("最后更新：2026-05-13。本文由 scripts/rewrite_docx.py 通过 docx API 重写。"),
    ]))

    return items


# ─── 执行流程 ───────────────────────────────────────────────────────────

def list_existing_children(token: str) -> list:
    """GET 响应字段是 items（POST 才是 children）。"""
    out: list = []
    page_token = ""
    while True:
        path = f"/open-apis/docx/v1/documents/{DOC_ID}/blocks/{DOC_ID}/children?page_size=500"
        if page_token:
            path += f"&page_token={page_token}"
        d = api("GET", path, token)
        if int(d.get("code", -1)) != 0:
            raise RuntimeError(d)
        data = d.get("data", {})
        items = data.get("items") or data.get("children") or []
        out.extend(items)
        if not data.get("has_more"):
            break
        page_token = data.get("page_token", "")
        if not page_token:
            break
    return out


def delete_all_children(token: str) -> int:
    children = list_existing_children(token)
    n = len(children)
    print(f"  found {n} existing children")
    if n == 0:
        return 0
    chunk = 100
    deleted = 0
    while deleted < n:
        end = n - deleted
        start = max(end - chunk, 0)
        d = api(
            "DELETE",
            f"/open-apis/docx/v1/documents/{DOC_ID}/blocks/{DOC_ID}/children/batch_delete",
            token,
            body={"start_index": start, "end_index": end},
        )
        if int(d.get("code", -1)) != 0:
            raise RuntimeError(f"delete failed: {d}")
        deleted += (end - start)
        print(f"  deleted {deleted}/{n}")
        time.sleep(0.3)
    return n


def append_blocks(token: str, blocks: list, chunk: int = 20) -> list:
    new_ids: list = []
    for start in range(0, len(blocks), chunk):
        batch = blocks[start : start + chunk]
        d = api(
            "POST",
            f"/open-apis/docx/v1/documents/{DOC_ID}/blocks/{DOC_ID}/children",
            token,
            body={"children": batch, "index": -1},
        )
        if int(d.get("code", -1)) != 0:
            raise RuntimeError(f"append failed: {d}")
        created = d["data"].get("children", [])
        new_ids.extend(c["block_id"] for c in created)
        time.sleep(0.3)
    return new_ids


def insert_image_block(token: str) -> str:
    d = api(
        "POST",
        f"/open-apis/docx/v1/documents/{DOC_ID}/blocks/{DOC_ID}/children",
        token,
        body={"children": [image_placeholder()], "index": -1},
    )
    if int(d.get("code", -1)) != 0:
        raise RuntimeError(f"create image block failed: {d}")
    return d["data"]["children"][0]["block_id"]


def upload_image_to_block(token: str, block_id: str, svg_bytes: bytes, name: str = "chart.svg") -> str:
    boundary = "----chorus" + str(int(time.time()))
    parts: list = []
    def add(field_name: str, val):
        if isinstance(val, (int, float)):
            val = str(val)
        if isinstance(val, str):
            val = val.encode()
        parts.extend([
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="{field_name}"'.encode(),
            b"",
            val,
        ])
    add("file_name", name)
    add("parent_type", "docx_image")
    add("parent_node", block_id)
    add("size", len(svg_bytes))
    add("extra", json.dumps({"drive_route_token": DOC_ID}))
    parts.extend([
        f"--{boundary}".encode(),
        f'Content-Disposition: form-data; name="file"; filename="{name}"'.encode(),
        b"Content-Type: image/svg+xml",
        b"",
        svg_bytes,
    ])
    parts.append(f"--{boundary}--".encode())
    body = b"\r\n".join(parts)
    req = urllib.request.Request(
        f"{LARK_OPEN}/open-apis/drive/v1/medias/upload_all",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            d = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print("HTTPError:", e.code, e.read().decode("utf-8", errors="ignore")[:600])
        raise
    if int(d.get("code", -1)) != 0:
        raise RuntimeError(f"upload failed: {d}")
    return d["data"]["file_token"]


def main() -> int:
    if not (os.environ.get("LARK_APP_ID") and os.environ.get("LARK_APP_SECRET")):
        print("ERR: need LARK_APP_ID / LARK_APP_SECRET", file=sys.stderr)
        return 2

    token = lark_token()
    print(f"target doc: {DOC_ID}")

    print("\n[1/3] deleting existing blocks...")
    n = delete_all_children(token)
    print(f"      removed {n} blocks")

    items = build_doc_items()
    print(f"\n[2/3] writing {len(items)} items ({sum(len(it['data']) for it in items if it['type']=='blocks')} text blocks + {sum(1 for it in items if it['type']=='image')} SVG images)...")

    img_idx = 0
    text_total = 0
    for i, it in enumerate(items):
        if it["type"] == "blocks":
            append_blocks(token, it["data"])
            text_total += len(it["data"])
            print(f"  [{i+1}/{len(items)}] +{len(it['data'])} blocks (text_total={text_total})")
        else:
            block_id = insert_image_block(token)
            file_token = upload_image_to_block(token, block_id, it["svg"], it["name"])
            img_idx += 1
            print(f"  [{i+1}/{len(items)}] image #{img_idx} {it['name']} (block={block_id} file={file_token})")

    print(f"\n[3/3] done. open: https://bytedance.larkoffice.com/docx/{DOC_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
