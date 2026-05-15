#!/usr/bin/env python3
"""把 chorus-lark-monitor 本周（2026-05-11 ~ 05-14）进展 append 到周报 Docx。

Append-only — 不删除现有 Helix 部分。SVG 走原生画板（block_type=43 + svg node）。

V2 重构：用 scripts/lark_docx.py 库，省掉 API boilerplate。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lark_docx import (
    LarkDocxClient,
    h, p, bullet, code, divider,
    text_item, svg_item,
)

DOC_ID = "DA9NdgnajosntQx1elTcPOYRnjc"


# ─── SVG 1: 架构演进 timeline ─────────────────────────────────────────────
SVG_TIMELINE = """<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 540" width="1280" height="540">
  <style>
    .box { stroke-width: 1.5; }
    .label { font-family: -apple-system, "PingFang SC", sans-serif; font-size: 13px; fill: #111827; }
    .label-bold { font-weight: 700; }
    .label-sm { font-size: 11px; fill: #6b7280; }
    .mono { font-family: "JetBrains Mono", monospace; font-size: 11px; fill: #4b5563; }
    .day { font-family: -apple-system; font-weight: 700; font-size: 14px; fill: #1f2937; }
    .arrow { stroke: #4b5563; stroke-width: 2; fill: none; }
  </style>
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#4b5563"/>
    </marker>
  </defs>
  <text x="40" y="40" font-family="-apple-system" font-weight="700" font-size="18" fill="#111827">Chorus Lark Monitor · 一周架构演进</text>
  <rect class="box" x="40" y="80" width="220" height="120" rx="8" fill="#fef3c7" stroke="#d97706"/>
  <text class="day" x="60" y="105">5-11 起点</text>
  <text class="label-sm" x="60" y="125">Koyeb 云部署</text>
  <text class="label-sm" x="60" y="143">静态 cron 拉数据</text>
  <text class="label-sm" x="60" y="161">前端 Pages 静态 dashboard</text>
  <text class="label-sm" x="60" y="179">~300 群上限（性能崖）</text>
  <text class="label-sm" x="60" y="194" fill="#dc2626">每月 ¥30 费用 ⚠</text>
  <rect class="box" x="300" y="80" width="240" height="160" rx="8" fill="#dbeafe" stroke="#2563eb"/>
  <text class="day" x="320" y="105">5-12 实时化</text>
  <text class="label-sm" x="320" y="125">迁 Mac mini 本机部署</text>
  <text class="label-sm" x="320" y="143">FastAPI + APScheduler</text>
  <text class="label-sm" x="320" y="161">CF Worker + Tunnel 暴公网</text>
  <text class="label-sm" x="320" y="179">飞书 webhook 实时接</text>
  <text class="label-sm" x="320" y="197">Koyeb 已 pause（停付费）</text>
  <text class="label-sm" x="320" y="215">dashboard 30s → 5.8s</text>
  <text class="label-sm" x="320" y="233">SQLite Tier-1 副本</text>
  <rect class="box" x="580" y="80" width="280" height="200" rx="8" fill="#d1fae5" stroke="#059669"/>
  <text class="day" x="600" y="105">5-13 真源化 + 双写</text>
  <text class="label-sm" x="600" y="125">webhook 改只写 SQLite (5ms)</text>
  <text class="label-sm" x="600" y="143">后台 primary-sync 推旧 Base</text>
  <text class="label-sm" x="600" y="161">后台 secondary-sync 推新 Base</text>
  <text class="label-sm" x="600" y="179">新 Base 一键 bootstrap 3 表</text>
  <text class="label-sm" x="600" y="197">23k 群 + 40k 消息全量回补</text>
  <text class="label-sm" x="600" y="215">Helix 项目门户文档 + 5 SVG</text>
  <text class="label-sm" x="600" y="233">external-join 30 天活跃过滤</text>
  <text class="label-sm" x="600" y="253" fill="#059669" font-weight="700">★ 关键架构升级</text>
  <rect class="box" x="900" y="80" width="340" height="220" rx="8" fill="#ede9fe" stroke="#7c3aed"/>
  <text class="day" x="920" y="105">5-14 迁移 + 修复</text>
  <text class="label-sm" x="920" y="125">新 Base 切为 PRIMARY（唯一写）</text>
  <text class="label-sm" x="920" y="143">旧 Base 完全停写（保留）</text>
  <text class="label-sm" x="920" y="161">SQLite 列翻转保持 record_id 一致</text>
  <text class="label-sm" x="920" y="179">external-join 一夜入 24k 新群</text>
  <text class="label-sm" x="920" y="197">触发 user 字段 builder bug 发现</text>
  <text class="label-sm" x="920" y="215">回填 42.8k 行（chat 3k + member 13k + msg 27k）</text>
  <text class="label-sm" x="920" y="233">Lark Base 单表 50k 行 RecordExceedLimit 探</text>
  <text class="label-sm" x="920" y="251">search API 翻页 page_token 在 query string 不是 body</text>
  <text class="label-sm" x="920" y="269">EXTERNAL_JOIN_DISABLED kill switch</text>
  <path class="arrow" d="M260,140 L298,140" marker-end="url(#arr)"/>
  <path class="arrow" d="M540,160 L578,160" marker-end="url(#arr)"/>
  <path class="arrow" d="M860,180 L898,180" marker-end="url(#arr)"/>
  <rect class="box" x="40" y="340" width="1200" height="170" rx="8" fill="#ffffff" stroke="#d1d5db"/>
  <text x="60" y="368" font-family="-apple-system" font-weight="700" font-size="14" fill="#111827">数据规模演化（5-11 → 5-14）</text>
  <g transform="translate(60, 388)">
    <rect class="box" x="0" y="0" width="220" height="100" rx="6" fill="#fef3c7" stroke="#d97706"/>
    <text x="110" y="22" text-anchor="middle" class="label-bold" fill="#92400e">机器人群数（chats）</text>
    <text x="110" y="50" text-anchor="middle" font-size="22" font-weight="700" fill="#1f2937">23,545 → 49,908</text>
    <text x="110" y="72" text-anchor="middle" class="label-sm">+26k（external-join 入 24k 外部群）</text>
    <text x="110" y="90" text-anchor="middle" class="label-sm">SQLite + 新 Base 一致</text>
  </g>
  <g transform="translate(300, 388)">
    <rect class="box" x="0" y="0" width="220" height="100" rx="6" fill="#dbeafe" stroke="#2563eb"/>
    <text x="110" y="22" text-anchor="middle" class="label-bold" fill="#1e40af">消息数（messages）</text>
    <text x="110" y="50" text-anchor="middle" font-size="22" font-weight="700" fill="#1f2937">37,531 → 72,396</text>
    <text x="110" y="72" text-anchor="middle" class="label-sm">+35k（webhook 实时 + bot-added 回填）</text>
    <text x="110" y="90" text-anchor="middle" class="label-sm">关联群组 link 100% 覆盖</text>
  </g>
  <g transform="translate(540, 388)">
    <rect class="box" x="0" y="0" width="220" height="100" rx="6" fill="#d1fae5" stroke="#059669"/>
    <text x="110" y="22" text-anchor="middle" class="label-bold" fill="#065f46">成员数（members）</text>
    <text x="110" y="50" text-anchor="middle" font-size="22" font-weight="700" fill="#1f2937">516 → 106,155</text>
    <text x="110" y="72" text-anchor="middle" class="label-sm">+105k（新群入群拉成员）</text>
    <text x="110" y="90" text-anchor="middle" class="label-sm">user 字段回填 13k 修复</text>
  </g>
  <g transform="translate(780, 388)">
    <rect class="box" x="0" y="0" width="220" height="100" rx="6" fill="#ede9fe" stroke="#7c3aed"/>
    <text x="110" y="22" text-anchor="middle" class="label-bold" fill="#5b21b6">授权用户（OAuth）</text>
    <text x="110" y="50" text-anchor="middle" font-size="22" font-weight="700" fill="#1f2937">4 → 15</text>
    <text x="110" y="72" text-anchor="middle" class="label-sm">+11 业务同事走 /group-join/auth</text>
    <text x="110" y="90" text-anchor="middle" class="label-sm">5-13 一天内全部完成授权</text>
  </g>
  <g transform="translate(1020, 388)">
    <rect class="box" x="0" y="0" width="220" height="100" rx="6" fill="#fce7f3" stroke="#db2777"/>
    <text x="110" y="22" text-anchor="middle" class="label-bold" fill="#9d174d">公网访问性能</text>
    <text x="110" y="50" text-anchor="middle" font-size="22" font-weight="700" fill="#1f2937">30s → 1.8s</text>
    <text x="110" y="72" text-anchor="middle" class="label-sm">gzip + CF Cache API + prewarm</text>
    <text x="110" y="90" text-anchor="middle" class="label-sm">chorus.xiaomiao.win 永远秒开</text>
  </g>
</svg>
"""


SVG_FLOW = """<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 460" width="1280" height="460">
  <style>
    .box { stroke-width: 1.5; }
    .label { font-family: -apple-system, "PingFang SC", sans-serif; font-size: 13px; fill: #111827; }
    .label-bold { font-weight: 700; }
    .label-sm { font-size: 11px; fill: #6b7280; }
    .mono { font-family: "JetBrains Mono", monospace; font-size: 11px; fill: #4b5563; }
    .arrow { stroke: #4b5563; stroke-width: 1.8; fill: none; }
    .arrow-async { stroke: #059669; stroke-dasharray: 5,3; }
  </style>
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#4b5563"/>
    </marker>
    <marker id="arr-a" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#059669"/>
    </marker>
  </defs>
  <text x="40" y="36" font-family="-apple-system" font-weight="700" font-size="16" fill="#111827">最终架构（5-14 切换后稳定）</text>
  <rect class="box" x="40" y="70" width="200" height="80" rx="8" fill="#fef3c7" stroke="#d97706"/>
  <text class="label label-bold" x="140" y="100" text-anchor="middle">飞书 (Lark)</text>
  <text class="label-sm" x="140" y="120" text-anchor="middle">15 授权用户 + bot</text>
  <text class="label-sm" x="140" y="136" text-anchor="middle">23k → 49k 群</text>
  <rect class="box" x="290" y="70" width="200" height="80" rx="8" fill="#fee2e2" stroke="#dc2626"/>
  <text class="label label-bold" x="390" y="100" text-anchor="middle">CF Worker</text>
  <text class="mono" x="390" y="120" text-anchor="middle">chorus.xiaomiao.win</text>
  <text class="label-sm" x="390" y="136" text-anchor="middle">Edge cache + 白名单</text>
  <rect class="box" x="540" y="70" width="200" height="80" rx="8" fill="#fee2e2" stroke="#dc2626"/>
  <text class="label label-bold" x="640" y="100" text-anchor="middle">cloudflared tunnel</text>
  <text class="mono" x="640" y="120" text-anchor="middle">--protocol http2</text>
  <text class="label-sm" x="640" y="136" text-anchor="middle">公网 → 本机</text>
  <rect class="box" x="40" y="200" width="700" height="220" rx="8" fill="#dbeafe" stroke="#2563eb"/>
  <text class="label label-bold" x="60" y="226" fill="#1e40af">Mac mini · server.py（FastAPI + APScheduler）</text>
  <rect class="box" x="60" y="250" width="200" height="80" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="160" y="272" text-anchor="middle">webhook handler</text>
  <text class="mono" x="160" y="290" text-anchor="middle">POST /lark/events</text>
  <text class="label-sm" x="160" y="308" text-anchor="middle">12 worker · 仅写 SQLite</text>
  <text class="label-sm" x="160" y="324" text-anchor="middle">5ms 立即返 200</text>
  <rect class="box" x="300" y="250" width="200" height="80" rx="6" fill="#d1fae5" stroke="#059669"/>
  <text class="label label-bold" x="400" y="272" text-anchor="middle">SQLite 真源</text>
  <text class="mono" x="400" y="290" text-anchor="middle">chorus_local.db</text>
  <text class="label-sm" x="400" y="308" text-anchor="middle">chats / msgs / members</text>
  <text class="label-sm" x="400" y="324" text-anchor="middle">primary_synced flag</text>
  <rect class="box" x="540" y="250" width="180" height="50" rx="6" fill="#f3f4f6" stroke="#374151"/>
  <text class="label label-bold" x="630" y="270" text-anchor="middle">primary-sync</text>
  <text class="label-sm" x="630" y="287" text-anchor="middle">10s tick</text>
  <rect class="box" x="540" y="310" width="180" height="50" rx="6" fill="#f3f4f6" stroke="#9ca3af" stroke-dasharray="3,3"/>
  <text class="label label-bold" x="630" y="330" text-anchor="middle" fill="#9ca3af">secondary-sync</text>
  <text class="label-sm" x="630" y="347" text-anchor="middle" fill="#9ca3af">disabled (旧 Base)</text>
  <rect class="box" x="60" y="350" width="440" height="60" rx="6" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="280" y="372" text-anchor="middle">Dashboard endpoint /src/data.jsx</text>
  <text class="label-sm" x="280" y="392" text-anchor="middle">SQLite 直读，top 500 群 × 20 消息 · CF Cache 1.2MB gzip</text>
  <rect class="box" x="800" y="200" width="220" height="110" rx="8" fill="#ede9fe" stroke="#7c3aed"/>
  <text class="label label-bold" x="910" y="224" text-anchor="middle" fill="#5b21b6">新 Base · PRIMARY</text>
  <text class="mono" x="910" y="244" text-anchor="middle">G42ybVmN...</text>
  <text class="label-sm" x="910" y="262" text-anchor="middle">Helix · 项目门户</text>
  <text class="label-sm" x="910" y="278" text-anchor="middle">机器人群列表 / 消息 / 成员</text>
  <text class="label-sm" x="910" y="294" text-anchor="middle">3 张表 ★ 唯一写入</text>
  <rect class="box" x="800" y="330" width="220" height="80" rx="8" fill="#f3f4f6" stroke="#9ca3af" stroke-dasharray="3,3"/>
  <text class="label label-bold" x="910" y="354" text-anchor="middle" fill="#6b7280">旧 Base · 已停写</text>
  <text class="mono" x="910" y="374" text-anchor="middle" fill="#9ca3af">PnRtbGm...</text>
  <text class="label-sm" x="910" y="392" text-anchor="middle">保留作历史归档</text>
  <rect class="box" x="1060" y="70" width="180" height="80" rx="8" fill="#fce7f3" stroke="#db2777"/>
  <text class="label label-bold" x="1150" y="100" text-anchor="middle">浏览器 Dashboard</text>
  <text class="label-sm" x="1150" y="120" text-anchor="middle">React 18 + JSX</text>
  <text class="label-sm" x="1150" y="136" text-anchor="middle">首访 ~1.8s</text>
  <path class="arrow" d="M240,110 L285,110" marker-end="url(#arr)"/>
  <path class="arrow" d="M490,110 L535,110" marker-end="url(#arr)"/>
  <path class="arrow" d="M740,110 L1055,110" marker-end="url(#arr)"/>
  <path class="arrow" d="M640,150 L640,200" marker-end="url(#arr)"/>
  <path class="arrow" d="M260,290 L295,290" marker-end="url(#arr)"/>
  <path class="arrow arrow-async" d="M500,290 L535,275" marker-end="url(#arr-a)"/>
  <path class="arrow arrow-async" d="M720,275 L795,250" marker-end="url(#arr-a)"/>
  <path class="arrow" d="M400,330 L400,350" marker-end="url(#arr)"/>
</svg>
"""


def build_items():
    return [
        text_item([
            divider(),
            h(1, "九、Chorus Lark Monitor 群聊监控（部署 + 架构升级 + 迁移）"),
            p("本周把这个项目从 Koyeb 静态部署完整搬到 Mac mini 本地实时部署，又顺手做了 SQLite 真源化重构 + 双 Base 灰度迁移 + 大量 bug 修复。3 天 25 个 commit。"),
            h(2, "本周成果摘要"),
            bullet("Koyeb 云部署 → Mac mini 本机部署（节省每月 ¥30 + 取消 300 群上限）"),
            bullet("公网入口 https://chorus.xiaomiao.win（CF Worker + Cloudflare Tunnel）"),
            bullet("架构重构：webhook 同步写 Base → 只写 SQLite，后台 worker 异步 fanout 双 Base"),
            bullet("数据迁移：旧 Base PnRtbGm 全量复制到新 Base Helix · 项目门户 G42ybVmN，对齐后切换 primary"),
            bullet("公网 dashboard 性能：30s → 1.8s（gzip + CF Cache API + 4min prewarm）"),
            bullet("外部群入群一夜从 23,545 → 49,908 群（external-join 拉了 24k 新外部群）"),
            bullet("3 张 Base 表 user 字段 builder bug 修复 + 历史回填 42,844 行"),
            bullet("项目说明 Docx 写入飞书（13 章节 + 5 张 SVG，全自动从代码生成）"),
            bullet("授权用户 4 → 15（11 个业务同事走 /group-join/auth 完成 OAuth）"),
            h(2, "一周架构演进 + 数据增长"),
        ]),
        svg_item(SVG_TIMELINE, "timeline.svg"),
        text_item([
            h(2, "新架构：webhook → SQLite → 后台 sync worker"),
            p("旧设计：webhook 同步调 Lark Base batch_create_records，1-3s 才能返 200。20 msg/s 限频时 webhook 排队，飞书 3s 超时重发循环。"),
            p("新设计：webhook 只写本机 SQLite（5ms 返 200），primary-sync / secondary-sync 两个后台 IntervalTrigger worker 异步把 SQLite 行批量推到双 Base。任一 Base 挂了不影响 webhook，可重试可监控。"),
            h(3, "切换后的稳定形态"),
        ]),
        svg_item(SVG_FLOW, "flow.svg"),
        text_item([
            h(2, "Base 迁移过程"),
            bullet("Phase 0: bootstrap_secondary_base.py 在新 Base 创建 3 张匹配表"),
            bullet("Phase 1: secondary-sync worker 15s 一批回补 23k chats + 40k msgs + 1.2k members，~30 min 跑完，100% 链接字段覆盖率"),
            bullet("Phase 2: 验证数据对账（SQLite vs 新 Base 行数完全对齐），切换 .env LARK_BASE_URL 主备对调 + SQLite record_id 列翻转"),
            bullet("Phase 3 (2026-05-14): 完全停旧 Base 写入（unset LARK_BASE_URL_SECONDARY），新 Base 成为唯一 source of truth"),
            bullet("旧 Base 保留数据可回查，不再接收新写入"),
            h(2, "关键 bug / 踩坑（按时间倒序）"),
            bullet("Lark Base /records GET API 在表 > 50k 行时报 1254103 RecordExceedLimit，必须改用 /records/search（且 page_token 在 query string 不是 body）"),
            bullet("_build_*_row_minimal 给 user 类型字段填 None，导致新 Base 三张表 群主/成员/发送者 字段全空 → 修后 + 历史 42k 行回填"),
            bullet("external-join 一次 cron 拉 36k 候选群，cron pool max_workers=1 被堵 14h，sync worker 串行卡死 → 加 EXTERNAL_JOIN_DISABLED kill switch"),
            bullet("ensure_bot_in_external_chats.py 看到 GROUP_JOIN_ADMIN_TOKEN= 是空字符串就抛 RuntimeError → CF API 直接 PUT 新 secret 三处同步"),
            bullet("_create_member_rows_safely 一次写 200+ 行触发 Lark Base 800010701 invalid_request → 按 200 切片"),
            bullet("CF Worker 覆写 cache-control 写 caches.default，遇到 warm-up 空 payload + prewarm 循环 → 看板永远 0 群。修：origin 回 no-store 时 Worker 跳过写 cache（x-cache: BYPASS）"),
            bullet("Babel standalone 编译 10MB+ JSON 在浏览器 OOM → data.jsx 不打 type=text/babel"),
            bullet("cloudflared 默认 QUIC 协议路由到美西节点 timeout → --protocol http2"),
            bullet("Lark Base 限频 800004135 OpenAPIBatchAddRecords ~20/s/table → FeishuClient.request 加 RATE_LIMIT_CODES set 自动 retry"),
            bullet("daily-sync --refresh-metadata-tables 重建表后旧 table_id 失效 → daily-sync 完成后 _invalidate_lark_state() + 重 seed 本地 SQLite"),
            h(2, "GitHub 增量提交（k190513120/chorus-lark-monitor · 25 commits）"),
            code("""ab8da26  fix(backfill)     去掉冗余 id_type 校验，只看 ou_ 前缀（救回 26k 条 user 消息）
6afde73  fix(backfill)     加 1255002 internal error 到 transient retry
209a45b  fix(backfill)     /records/search 的 page_token 必须放 query string 不是 body
c390a4b  fix(backfill)     大表走 /records/search 替代 /records (>50k 行 RecordExceedLimit)
0f23d6a  fix(backfill)     加 Lark transient code retry (1254607 / 800004135)
9c8e828  fix               minimal row builder 给 user 类型字段填 [{"id": open_id}]
941dded  feat              EXTERNAL_JOIN_DISABLED kill switch
318338e  docs              rewrite_docx.py V2 — 5 张 SVG 替换全部结构化代码块
2b87111  docs              加 rewrite_docx.py 全文档重写脚本（含 SVG 架构图嵌入）
743af98  docs              加 insert_arch_svg.py 把架构图 SVG 推到 Docx 图片块
e3e6317  docs              加 write_docx_project.py 把项目文档 push 到飞书 Docx
d433190  refactor          webhook only writes SQLite; primary sync worker handles Base
c2ea73f  feat              secondary Base 异步同步 worker (Phase 1)
3e20a1b  feat              bootstrap_secondary_base.py 在新 Base 里建 3 张表
e1e2cd5  feat(external-join) 新增活跃度过滤 --active-since-days
087c647  perf              webhook 并发 4→12 + 成员写入 200/批切片
2127fde  chore             gitignore admin token local backup file
b0b13f2  fix(cache)        warm-up 期空 payload 不再被 CF Edge 固化
bac311c  perf(dashboard)   CF Cache API + 240s prewarm 让公网首访 1.8s
0cad37e  perf(dashboard)   gzip + CF Edge cache for /src/data.jsx (9.7MB -> 1MB)
d1372de  feat(local-db)    SQLite local replica + bypass Babel for data.jsx
80887ec  feat(worker+tunnel) serve dashboard via chorus.xiaomiao.win
dfc7386  fix(events)       invalidate caches after daily-sync; retry rate limits
3495995  feat(deployment)  self-host on Mac mini via uvicorn + Tunnel + Worker
03162de  feat(deployment)  add Mac mini launchd deployment configs""", "plain"),
            h(2, "下周计划"),
            bullet("观察新 Base 稳定性，尤其大表（>50k 行）的写入限制"),
            bullet("评估 tenant_key → tenant_id (F 码) 转换接入（字节内网 Kitex RPC，需要 Go sidecar 或找 fsopen wrapper 加 endpoint）"),
            bullet("Dashboard 首屏延迟根本治理：删 Google Fonts + 换 jsdelivr CDN，预计 5.8s → 1.5s"),
            bullet("等用户规模 ×5 后再加：消息写入 500ms 缓冲批量化、Lark Base 单表按月分表"),
            bullet("如需 bot 加入更多外部群，重新打开 EXTERNAL_JOIN_DISABLED（当前已禁，避免再撞 cron 池）"),
        ]),
    ]


def main():
    if not (os.environ.get("LARK_APP_ID") and os.environ.get("LARK_APP_SECRET")):
        print("ERR: need LARK_APP_ID / LARK_APP_SECRET", file=sys.stderr)
        return 2
    doc = LarkDocxClient.from_env(DOC_ID)
    doc.write(build_items(), mode="append")
    return 0


if __name__ == "__main__":
    sys.exit(main())
