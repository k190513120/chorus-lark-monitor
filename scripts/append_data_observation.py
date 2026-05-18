#!/usr/bin/env python3
"""把"61k 群实际画像"洞察 append 到 Helix 项目 doc。

数据来源：2026-05-18 SQLite 现场快照。SVG 一张人数分布漏斗图。
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

DOC_ID = "VqlCdpASboikidxVuTMcth1rnAh"  # Helix · chorus-lark-monitor 项目门户


SVG_FUNNEL = """<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 540" width="1280" height="540">
  <style>
    .label { font-family: -apple-system, "PingFang SC", sans-serif; font-size: 13px; fill: #111827; }
    .label-bold { font-weight: 700; font-size: 14px; }
    .label-sm { font-size: 11px; fill: #6b7280; }
    .num { font-family: -apple-system; font-weight: 700; font-size: 28px; fill: #111827; }
    .pct { font-family: -apple-system; font-size: 13px; fill: #6b7280; }
    .bar { stroke: none; }
  </style>
  <text x="40" y="36" font-family="-apple-system" font-weight="700" font-size="18" fill="#111827">61,136 群 — 实际画像漏斗（2026-05-18 快照）</text>

  <!-- 总群数 -->
  <rect class="bar" x="40" y="70" width="1200" height="60" rx="6" fill="#dbeafe"/>
  <text class="label-bold" x="60" y="105" fill="#1e40af">监控群总数（chats 表）</text>
  <text class="num" x="1100" y="111" text-anchor="end" fill="#1e40af">61,136</text>
  <text class="pct" x="1180" y="111" text-anchor="end">100%</text>

  <!-- members 表覆盖 -->
  <rect class="bar" x="80" y="155" width="1120" height="50" rx="6" fill="#fef3c7"/>
  <text class="label-bold" x="100" y="186" fill="#92400e">members 表里有数据的群</text>
  <text class="num" x="1060" y="190" text-anchor="end" font-size="22" fill="#92400e">37,913</text>
  <text class="pct" x="1140" y="190" text-anchor="end">62.0%</text>

  <rect class="bar" x="80" y="215" width="1120" height="50" rx="6" fill="#fee2e2"/>
  <text class="label-bold" x="100" y="246" fill="#991b1b">members 表 0 行的群</text>
  <text class="num" x="1060" y="250" text-anchor="end" font-size="22" fill="#991b1b">23,223</text>
  <text class="pct" x="1140" y="250" text-anchor="end">38.0% ⚠</text>

  <text class="label-sm" x="100" y="282" fill="#6b7280">└─ 9,198 个 0 成员群但有消息进来（客户进群说 1-2 句就退）</text>
  <text class="label-sm" x="100" y="298" fill="#6b7280">└─ 14,025 个 0 成员 + 0 消息（飞书购买咨询群模板自动创建，客户从未进群）</text>

  <!-- 活跃度 -->
  <rect class="bar" x="120" y="320" width="1080" height="50" rx="6" fill="#fde68a"/>
  <text class="label-bold" x="140" y="351" fill="#92400e">最近 7 天有消息的群</text>
  <text class="num" x="1020" y="355" text-anchor="end" font-size="22" fill="#92400e">38,267</text>
  <text class="pct" x="1100" y="355" text-anchor="end">62.6%</text>

  <rect class="bar" x="160" y="380" width="1040" height="50" rx="6" fill="#fecaca"/>
  <text class="label-bold" x="180" y="411" fill="#991b1b">最近 24h 有消息的群（真活跃）</text>
  <text class="num" x="980" y="415" text-anchor="end" font-size="22" fill="#991b1b">500</text>
  <text class="pct" x="1060" y="415" text-anchor="end">0.8%</text>

  <!-- 提示 -->
  <rect class="bar" x="40" y="450" width="1200" height="70" rx="6" fill="#f3f4f6" stroke="#9ca3af" stroke-width="1"/>
  <text class="label-bold" x="60" y="475" fill="#111827">业务结论</text>
  <text class="label" x="60" y="495" fill="#4b5563">• 对外可说"监控 6 万群"，对内做决策用"7 天活跃 3.8 万 群"，看板真正服务的是"24h 活跃 500 群"的头部</text>
  <text class="label" x="60" y="513" fill="#4b5563">• 飞书"购买咨询群"模板自动批量创建大量空群，转化率极低 — 这是产品观察，不是数据 bug</text>
</svg>
"""


def build_items():
    return [
        text_item([
            divider(),
            h(1, "十四、数据画像观察（2026-05-18 现场快照）"),
            p("产品对外说「监控 6 万群」没错，但 61,136 群里实际只有少部分是有效信号 — 这一节记录抽样观察。"),
        ]),
        svg_item(SVG_FUNNEL, "data-funnel.svg"),
        text_item([
            h(2, "现场快照（2026-05-18 10:50 SGT）"),
            bullet("chats: 61,136"),
            bullet("messages: 93,428"),
            bullet("members 行（chat_id × open_id）: 167,183"),
            bullet("唯一成员 open_id: 52,106"),
            bullet("最近 7 天有消息的群: 38,267"),
            bullet("最近 24h 有消息的群: 500"),
            h(2, "真实群人数分布（从 members 表反算）"),
            bullet("0 成员（从未拉过 / 群内真无人）: 23,223 群（38%）"),
            bullet("1 成员（只剩 bot 自己）: 322 群"),
            bullet("2 人（bot + 1 销售/客户）: 6,077 群（10%）"),
            bullet("3 人（bot + 2，典型客服路径）: 22,953 群（37.5%）"),
            bullet("4-9 人（小项目群）: 8,130 群（13%）"),
            bullet("10-49 人: 309 群"),
            bullet("50-199 人: 107 群"),
            bullet("≥200 人: 15 群（top tier 社区/打单群）"),
            h(2, "对 38% 空成员群的诊断"),
            p("23,223 个群的 members 表完全空 — 经过抽样 5 个直接调 list_chat_members 验证，结论："),
            bullet("这些不是数据丢失 — 飞书 API 现场调用都能拿到数据，返回的就是 0-2 个真实成员"),
            bullet("91% 这些群的 owner_id 字段也是空（daily-sync 的 list_chats API 不返回 owner_id）"),
            bullet("updated_at 全是同一时刻 — 都是 daily-sync 在 lite-mode 下批量写入的"),
            bullet("绝大多数是「XX 的飞书购买咨询群」/「企业专属服务群」/「MQL 讨论群」模板"),
            h(3, "产生路径"),
            p("飞书产品侧「购买咨询」入口自动创建一批空群（bot 已加入，但客户/销售可能没进），webhook im.chat.member.bot.added_v1 触发时 list_chat_members 拉到的就是 0 真人。这是飞书自动化的副作用，不是 chorus 数据质量问题。"),
            h(3, "判断"),
            bullet("不建议补数据 — 补完看板数字也只从 0 变 1-2，无信息增量"),
            bullet("建议看板默认筛「7 天有消息」或「≥2 人 + 24h 活跃」，把空群从默认视图过滤掉"),
            bullet("如果产品要看「群创建 → 真实使用」漏斗，这 23k 空群是分子，38k 活跃群是分母的负样本"),
            h(2, "Top 10 消息量群（活跃头部）"),
            bullet("【飞书生态赋能】伙伴打单交流群 — 2,176 条"),
            bullet("OpenClaw 飞书插件体验互助 12 群 — 1,446 条"),
            bullet("飞书 aily 智能伙伴交流群 — 691 条"),
            bullet("飞书 CLI 交流互助群（2 群） — 575 条"),
            bullet("大仙 AI 帮｜一起实战一起赢 — 502 条"),
            bullet("客服 & DR 线索问题反馈 — 409 条"),
            bullet("飞书个人版的飞书购买咨询群 — 373 条"),
            bullet("渠道伙伴 Customer 360 系统问题反馈群 — 349 条"),
            bullet("OpenClaw 飞书插件体验互助 11 群 — 328 条"),
            bullet("学清 B 座餐饮话题群 — 292 条"),
            h(2, "下一步建议（仅观察，未执行）"),
            bullet("看板默认隐藏空成员群（加 WHERE members_count > 0）"),
            bullet("daily-sync lite-mode 不补 members 是有意为之（量大），但可以选择性补「7 天有消息但 0 成员」这 9k 群（量级可控）"),
            bullet("如果想做「群生命周期漏斗」，这套数据已经够 — 不需要再加埋点"),
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
