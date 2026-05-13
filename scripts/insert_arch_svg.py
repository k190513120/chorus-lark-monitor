#!/usr/bin/env python3
"""往 Chorus Docx 里塞一个架构图（SVG 上传成 image block）。

Lark Docx image 块工作流：
  1. POST blocks/children 加一个空 image 块（拿到 block_id，token 字段空）
  2. POST drive/v1/medias/upload_all parent_type=docx_image parent_node=<block_id>
     把 SVG 文件上传，关联到那个 image 块
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DOC_ID = os.getenv("CHORUS_DOC_ID", "VqlCdpASboikidxVuTMcth1rnAh")
LARK_OPEN = "https://open.feishu.cn"


def get_token() -> str:
    req = urllib.request.Request(
        f"{LARK_OPEN}/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({"app_id": os.environ["LARK_APP_ID"], "app_secret": os.environ["LARK_APP_SECRET"]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["tenant_access_token"]


# ─── 架构图 SVG（手画 + 文字标注，飞书图片块支持 SVG）──────────────────
ARCH_SVG = """<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 760" width="1280" height="760">
  <style>
    .box { fill: #f3f4f6; stroke: #374151; stroke-width: 1.5; }
    .box-lark { fill: #fef3c7; stroke: #d97706; }
    .box-cf { fill: #fee2e2; stroke: #dc2626; }
    .box-server { fill: #dbeafe; stroke: #2563eb; }
    .box-db { fill: #d1fae5; stroke: #059669; }
    .box-base { fill: #ede9fe; stroke: #7c3aed; }
    .box-dash { fill: #fce7f3; stroke: #db2777; }
    .label { font-family: -apple-system, "PingFang SC", sans-serif; font-size: 14px; fill: #111827; }
    .label-bold { font-weight: 700; }
    .label-sm { font-size: 11px; fill: #6b7280; }
    .label-mono { font-family: "JetBrains Mono", monospace; font-size: 11px; fill: #4b5563; }
    .arrow { stroke: #4b5563; stroke-width: 1.8; fill: none; }
    .arrow-async { stroke: #059669; stroke-dasharray: 5,3; }
    .arrow-label { font-family: -apple-system, sans-serif; font-size: 10px; fill: #4b5563; }
    .section-title { font-family: -apple-system, sans-serif; font-weight: 700; font-size: 13px; fill: #111827; }
  </style>
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#4b5563" />
    </marker>
    <marker id="arr-a" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#059669" />
    </marker>
  </defs>

  <!-- 顶部：飞书 -->
  <rect class="box box-lark" x="40" y="30" width="220" height="80" rx="8"/>
  <text class="label label-bold" x="150" y="60" text-anchor="middle">飞书 (Lark)</text>
  <text class="label label-sm" x="150" y="80" text-anchor="middle">webhook 事件 / Base API</text>
  <text class="label label-sm" x="150" y="96" text-anchor="middle">App: cli_a75bb415d8ff9013</text>

  <!-- CF Worker (网关) -->
  <rect class="box box-cf" x="340" y="30" width="240" height="80" rx="8"/>
  <text class="label label-bold" x="460" y="55" text-anchor="middle">CF Worker</text>
  <text class="label label-mono" x="460" y="74" text-anchor="middle">chorus.xiaomiao.win</text>
  <text class="label label-sm" x="460" y="92" text-anchor="middle">白名单 + Edge cache 300s</text>

  <!-- Tunnel -->
  <rect class="box box-cf" x="660" y="30" width="220" height="80" rx="8"/>
  <text class="label label-bold" x="770" y="55" text-anchor="middle">cloudflared tunnel</text>
  <text class="label label-mono" x="770" y="74" text-anchor="middle">--protocol http2</text>
  <text class="label label-sm" x="770" y="92" text-anchor="middle">chorus-origin.xiaomiao.win</text>

  <!-- 浏览器 -->
  <rect class="box box-dash" x="960" y="30" width="280" height="80" rx="8"/>
  <text class="label label-bold" x="1100" y="55" text-anchor="middle">浏览器 Dashboard</text>
  <text class="label label-mono" x="1100" y="74" text-anchor="middle">chorus.xiaomiao.win/</text>
  <text class="label label-sm" x="1100" y="92" text-anchor="middle">React 18 + JSX runtime</text>

  <!-- 中间大盒：Mac mini server.py -->
  <rect class="box box-server" x="40" y="180" width="1200" height="280" rx="8"/>
  <text class="section-title" x="60" y="206">Mac mini · FastAPI server.py (127.0.0.1:5678)</text>

  <!-- webhook handler -->
  <rect class="box" x="60" y="230" width="280" height="100" rx="6"/>
  <text class="label label-bold" x="200" y="252" text-anchor="middle">webhook handler</text>
  <text class="label-mono" x="200" y="272" text-anchor="middle">POST /lark/events</text>
  <text class="label label-sm" x="200" y="290" text-anchor="middle">ThreadPoolExecutor(12)</text>
  <text class="label label-sm" x="200" y="306" text-anchor="middle">event dedup (LRU 10k)</text>
  <text class="label label-sm" x="200" y="322" text-anchor="middle">5ms 入 SQLite 立即返回</text>

  <!-- SQLite -->
  <rect class="box box-db" x="430" y="230" width="280" height="100" rx="6"/>
  <text class="label label-bold" x="570" y="252" text-anchor="middle">SQLite 真源</text>
  <text class="label label-mono" x="570" y="272" text-anchor="middle">chorus_local.db</text>
  <text class="label label-sm" x="570" y="290" text-anchor="middle">chats / members / messages</text>
  <text class="label label-sm" x="570" y="306" text-anchor="middle">primary_synced / secondary_synced</text>
  <text class="label label-sm" x="570" y="322" text-anchor="middle">WAL · ~24MB · 40k 行</text>

  <!-- sync worker primary -->
  <rect class="box" x="800" y="230" width="200" height="60" rx="6"/>
  <text class="label label-bold" x="900" y="252" text-anchor="middle">primary-sync</text>
  <text class="label label-sm" x="900" y="272" text-anchor="middle">IntervalTrigger 10s</text>

  <!-- sync worker secondary -->
  <rect class="box" x="800" y="320" width="200" height="60" rx="6"/>
  <text class="label label-bold" x="900" y="342" text-anchor="middle">secondary-sync</text>
  <text class="label label-sm" x="900" y="362" text-anchor="middle">IntervalTrigger 15s</text>

  <!-- dashboard endpoint -->
  <rect class="box" x="60" y="370" width="280" height="70" rx="6"/>
  <text class="label label-bold" x="200" y="392" text-anchor="middle">Dashboard endpoint</text>
  <text class="label label-mono" x="200" y="412" text-anchor="middle">GET /src/data.jsx</text>
  <text class="label label-sm" x="200" y="430" text-anchor="middle">top-500 群 · 每群 20 条消息</text>

  <!-- APScheduler cron -->
  <rect class="box" x="430" y="370" width="280" height="70" rx="6"/>
  <text class="label label-bold" x="570" y="392" text-anchor="middle">APScheduler</text>
  <text class="label label-sm" x="570" y="410" text-anchor="middle">daily-sync · external-join</text>
  <text class="label label-sm" x="570" y="426" text-anchor="middle">bulk-stats · cf-prewarm · 2×sync</text>

  <!-- 旧 Base -->
  <rect class="box box-base" x="1050" y="180" width="190" height="120" rx="8"/>
  <text class="label label-bold" x="1145" y="204" text-anchor="middle">旧 Base (primary)</text>
  <text class="label label-mono" x="1145" y="220" text-anchor="middle">PnRtbGm...</text>
  <text class="label label-sm" x="1145" y="240" text-anchor="middle">机器人群列表</text>
  <text class="label label-sm" x="1145" y="256" text-anchor="middle">机器人群消息记录</text>
  <text class="label label-sm" x="1145" y="272" text-anchor="middle">机器人群成员记录</text>
  <text class="label label-sm" x="1145" y="290" text-anchor="middle">(验证期)</text>

  <!-- 新 Base -->
  <rect class="box box-base" x="1050" y="320" width="190" height="120" rx="8"/>
  <text class="label label-bold" x="1145" y="344" text-anchor="middle">新 Base (secondary)</text>
  <text class="label label-mono" x="1145" y="360" text-anchor="middle">G42ybVmN...</text>
  <text class="label label-sm" x="1145" y="380" text-anchor="middle">Helix · 项目门户</text>
  <text class="label label-sm" x="1145" y="396" text-anchor="middle">三同名表 · 字段一致</text>
  <text class="label label-sm" x="1145" y="412" text-anchor="middle">回补 + 持续 dual-sync</text>
  <text class="label label-sm" x="1145" y="428" text-anchor="middle">(迁移目标)</text>

  <!-- 底部：daily-sync 链路 -->
  <rect class="box" x="40" y="510" width="600" height="100" rx="8"/>
  <text class="section-title" x="60" y="534">夜间 daily-sync · 直接对接飞书 API</text>
  <text class="label label-sm" x="60" y="556">每天 00:00 SGT 全量同步：拉群列表 + 成员 + 消息 → 旧 Base + 重 seed SQLite</text>
  <text class="label label-sm" x="60" y="574">sync_feishu_groups_to_base.py · --lite-mode --refresh-metadata-tables</text>
  <text class="label label-sm" x="60" y="592">耗时 ~10min · 24k 群 / 43k 消息 / 1.2k 成员</text>

  <!-- 右下：external-join -->
  <rect class="box" x="680" y="510" width="560" height="100" rx="8"/>
  <text class="section-title" x="700" y="534">每晚 external-join · bot 自动入群</text>
  <text class="label label-sm" x="700" y="556">22:00 SGT · 通过 15 个授权用户 access_token 列出他们所在外部群</text>
  <text class="label label-sm" x="700" y="574">--active-since-days 30 跳过死群 · diff bot 已在群 → 自动拉机器人入新群</text>
  <text class="label label-sm" x="700" y="592">代理 worker: feishu-bot.xiaomiao.win (KV 存 OAuth token)</text>

  <!-- 箭头 -->
  <!-- 飞书 → CF Worker -->
  <path class="arrow" d="M260,70 L335,70" marker-end="url(#arr)"/>
  <text class="arrow-label" x="297" y="62" text-anchor="middle">webhook</text>

  <!-- CF Worker → Tunnel -->
  <path class="arrow" d="M580,70 L655,70" marker-end="url(#arr)"/>

  <!-- Tunnel → 浏览器（dashboard 资源）-->
  <path class="arrow" d="M880,70 L955,70" marker-end="url(#arr)"/>

  <!-- Tunnel → server -->
  <path class="arrow" d="M770,115 L770,180 L200,180 L200,225" marker-end="url(#arr)"/>
  <text class="arrow-label" x="500" y="174" text-anchor="middle">tunnel → 127.0.0.1:5678</text>

  <!-- webhook handler → SQLite -->
  <path class="arrow" d="M340,280 L425,280" marker-end="url(#arr)"/>
  <text class="arrow-label" x="382" y="272" text-anchor="middle">5ms</text>

  <!-- SQLite → primary worker -->
  <path class="arrow arrow-async" d="M710,260 L795,260" marker-end="url(#arr-a)"/>
  <text class="arrow-label" x="752" y="252" text-anchor="middle">异步</text>

  <!-- SQLite → secondary worker -->
  <path class="arrow arrow-async" d="M710,310 L795,350" marker-end="url(#arr-a)"/>

  <!-- primary worker → 旧 Base -->
  <path class="arrow arrow-async" d="M1000,260 L1045,240" marker-end="url(#arr-a)"/>

  <!-- secondary worker → 新 Base -->
  <path class="arrow arrow-async" d="M1000,350 L1045,380" marker-end="url(#arr-a)"/>

  <!-- SQLite → dashboard endpoint -->
  <path class="arrow" d="M570,330 L570,355 L200,355 L200,370" marker-end="url(#arr)"/>
  <text class="arrow-label" x="385" y="348" text-anchor="middle">SQLite 直读</text>

  <!-- dashboard endpoint → 浏览器（回路）-->
  <path class="arrow" d="M340,405 L1100,405 L1100,110" marker-end="url(#arr)"/>

  <!-- daily-sync → 飞书 -->
  <path class="arrow" d="M340,510 L340,140 L260,140 L260,110" marker-end="url(#arr)"/>
  <text class="arrow-label" x="350" y="320" text-anchor="middle">读飞书 群/消息/成员 API</text>

  <!-- daily-sync → 旧 Base -->
  <path class="arrow" d="M640,560 L850,560 L850,260 L1045,260" marker-end="url(#arr)"/>

  <!-- 图例 -->
  <rect x="40" y="640" width="500" height="100" rx="6" fill="#ffffff" stroke="#d1d5db"/>
  <text class="section-title" x="60" y="660">图例</text>
  <line x1="60" y1="678" x2="100" y2="678" stroke="#4b5563" stroke-width="1.8" marker-end="url(#arr)"/>
  <text class="label-sm" x="110" y="682">同步写入（webhook 入 SQLite / dashboard 直读）</text>
  <line x1="60" y1="700" x2="100" y2="700" stroke="#059669" stroke-width="1.8" stroke-dasharray="5,3" marker-end="url(#arr-a)"/>
  <text class="label-sm" x="110" y="704">异步 sync worker（SQLite → Base，可重试可监控）</text>
  <text class="label-sm" x="60" y="725">监控：GET /admin/sync-stats · /admin/jobs · /admin/local-db-stats</text>
</svg>
"""


def post_image_block(token: str) -> str:
    """加一个空 image 块到文档末尾，返回 block_id。"""
    payload = {
        "children": [{"block_type": 27, "image": {"token": ""}}],
        "index": -1,
    }
    req = urllib.request.Request(
        f"{LARK_OPEN}/open-apis/docx/v1/documents/{DOC_ID}/blocks/{DOC_ID}/children",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        d = json.loads(resp.read())
    if int(d.get("code", -1)) != 0:
        raise RuntimeError(f"create image block failed: {d}")
    children = d["data"]["children"]
    return children[0]["block_id"]


def upload_image(token: str, block_id: str, svg_bytes: bytes, name: str = "chorus-arch.svg") -> str:
    """multipart 上传 SVG，关联到 image block。返回 file_token。"""
    boundary = "----chorus" + str(int(time.time()))
    parts = []
    def add(name_, val):
        if isinstance(val, (int, float)):
            val = str(val)
        if isinstance(val, str):
            val = val.encode()
        parts.extend([
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="{name_}"'.encode(),
            b"",
            val,
        ])
    add("file_name", name)
    add("parent_type", "docx_image")
    add("parent_node", block_id)
    add("size", len(svg_bytes))
    add("extra", json.dumps({"drive_route_token": DOC_ID}))
    # file (binary)
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
    token = get_token()
    print("Creating empty image block in doc...")
    block_id = post_image_block(token)
    print(f"  block_id = {block_id}")
    print("Uploading SVG to that block...")
    file_token = upload_image(token, block_id, ARCH_SVG.encode("utf-8"))
    print(f"  file_token = {file_token}")
    print(f"DONE. open: https://bytedance.larkoffice.com/docx/{DOC_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
