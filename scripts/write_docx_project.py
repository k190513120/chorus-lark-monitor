#!/usr/bin/env python3
"""Write a full project doc into the Lark Docx VqlCdpASboikidxVuTMcth1rnAh.

Usage:
    set -a; source .env; set +a
    .venv/bin/python scripts/write_docx_project.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

DOC_ID = os.getenv("CHORUS_DOC_ID", "VqlCdpASboikidxVuTMcth1rnAh")
LARK_OPEN = "https://open.feishu.cn"


def get_tenant_token() -> str:
    req = urllib.request.Request(
        f"{LARK_OPEN}/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({
            "app_id": os.environ["LARK_APP_ID"],
            "app_secret": os.environ["LARK_APP_SECRET"],
        }).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        d = json.loads(resp.read())
    if int(d.get("code", -1)) != 0:
        raise RuntimeError(d)
    return d["tenant_access_token"]


def h(level: int, text: str) -> dict:
    """Heading block. level=1..3 mapped to block_type 3..5."""
    btype = {1: 3, 2: 4, 3: 5}[level]
    key = f"heading{level}"
    return {"block_type": btype, key: {"elements": [{"text_run": {"content": text}}], "style": {}}}


def p(text: str, bold: bool = False, italic: bool = False) -> dict:
    """Paragraph block."""
    style = {}
    if bold: style["bold"] = True
    if italic: style["italic"] = True
    el: dict = {"text_run": {"content": text}}
    if style:
        el["text_run"]["text_element_style"] = style
    return {"block_type": 2, "text": {"elements": [el], "style": {}}}


def b(text: str) -> dict:
    """Bullet list item."""
    return {"block_type": 12, "bullet": {"elements": [{"text_run": {"content": text}}], "style": {}}}


def code(text: str, lang: str = "bash") -> dict:
    """Code block. lang names from Lark spec; 1=plain, 5=bash, 12=python, 28=sql."""
    lang_map = {"plain": 1, "bash": 5, "python": 12, "sql": 28, "json": 28, "yaml": 56, "js": 36}
    return {
        "block_type": 14,
        "code": {
            "elements": [{"text_run": {"content": text}}],
            "style": {"language": lang_map.get(lang, 1)},
        },
    }


def divider() -> dict:
    return {"block_type": 22, "divider": {}}


def quote(text: str) -> dict:
    return {"block_type": 34, "quote_container": {"elements": [{"text_run": {"content": text}}]}}


def build_blocks() -> list:
    blocks: list = []

    # === 标题 ===
    blocks.append(h(1, "Chorus Lark Monitor · 飞书群聊监控系统"))
    blocks.append(p("把飞书群聊的消息 / 成员变更 / 群事件实时汇聚到 SQLite 真源 + 多维表格双写，给客户运营提供「DR · 客户对话健康度」面板。"))

    # === 一、是什么 ===
    blocks.append(h(2, "一、Chorus 是什么"))
    blocks.append(p("一个本机部署的飞书机器人群聊监控平台。bot 自动加入授权用户的外部群，实时捕获消息事件，聚合后给客户经理 / 主管看「群健康度看板」。"))
    blocks.append(h(3, "现状（2026-05-13）"))
    blocks.append(b("部署在 Mac mini（M4 / 32GB），launchd 跑 server.py（uvicorn）+ cloudflared tunnel"))
    blocks.append(b("公网入口：https://chorus.xiaomiao.win（Cloudflare Worker + Tunnel）"))
    blocks.append(b("当前数据规模：24029 群 / 43388 消息 / 1196 成员（来自 daily-sync 全量 + webhook 实时增量）"))
    blocks.append(b("15 个授权用户授权 bot 拉外部群；bot 已在 23611 个群里"))
    blocks.append(b("双 Lark Base 写入：旧 Base（PnRtbGmTpaVXwDsWBWPcPaEpnwh，验证期），新 Base（G42ybVmN9aAeYdsHW06cysTonmF，迁移目标）"))

    # === 二、整体架构 ===
    blocks.append(h(2, "二、整体架构"))
    blocks.append(p("数据流：飞书 webhook → CF Worker → tunnel → server.py → SQLite（真源）→ 两个后台 sync worker → 双 Base"))
    blocks.append(code("""飞书 (Lark)
   │  webhook POST
   ▼
chorus.xiaomiao.win (CF Worker · chorus-lark-events-gateway)
   │  路径白名单 + 边缘缓存
   ▼
chorus-origin.xiaomiao.win (cloudflared tunnel · HTTP/2)
   │
   ▼
Mac mini 127.0.0.1:5678 (FastAPI · uvicorn)
   │  ① /lark/events → ThreadPoolExecutor(12) → handler
   │
   ├─→ SQLite (chorus_local.db) ← Tier-1 真源（5ms 写入）
   │        │
   │        ├─→ primary-sync (10s tick) → 旧 Base
   │        └─→ secondary-sync (15s tick) → 新 Base
   │
   ├─→ APScheduler cron：daily-sync / external-join / bulk-stats /
   │                     cf-prewarm / primary-sync / secondary-sync
   │
   └─→ Dashboard (/src/data.jsx) ← SQLite 直读，浏览器渲染
""", "plain"))
    blocks.append(p("三段责任清晰："))
    blocks.append(b("Webhook 入站：5ms 内只写 SQLite，飞书永不超时（< 3s 要求）"))
    blocks.append(b("后台 sync worker：异步把 SQLite 行批量推到 Base，失败可 retry"))
    blocks.append(b("Dashboard 读路径：SQLite 直读，绕过 Base 全部限频"))

    # === 三、SQLite 真源 ===
    blocks.append(h(2, "三、SQLite 真源（chorus_local.db）"))
    blocks.append(p("本机持久层，WAL 模式支持并发读 + 单写。所有 Base 数据都从这里 fanout。"))
    blocks.append(h(3, "4 张表"))
    blocks.append(b("chats — 群信息（chat_id 主键），含 record_id / secondary_record_id（双 Base 的链接 record）"))
    blocks.append(b("members — 群成员（chat_id + member_open_id 复合主键）"))
    blocks.append(b("messages — 消息流（msg_id 主键，索引 chat_id + time_ms）"))
    blocks.append(b("meta — KV 元数据（last_seeded_at 等）"))
    blocks.append(h(3, "sync state 字段（2026-05-13 新增）"))
    blocks.append(b("primary_synced / secondary_synced — 0=待同步，1=已同步"))
    blocks.append(b("primary_record_id / secondary_record_id — 各 Base 写入返回的 record_id（消息 / 成员链接用）"))
    blocks.append(b("sync_attempts / sync_last_error — 重试次数 + 最近错误"))

    # === 四、Webhook 事件处理 ===
    blocks.append(h(2, "四、Webhook 事件处理"))
    blocks.append(p("server.py 暴露 POST /lark/events。事件分两类："))
    blocks.append(h(3, "CREATE 类（异步入 SQLite，worker 推 Base）"))
    blocks.append(b("im.message.receive_v1 — 新消息 → 写 messages 表"))
    blocks.append(b("im.chat.member.user.added_v1 — 成员入群 → 写 members 表"))
    blocks.append(b("im.chat.member.bot.added_v1 — bot 入新群 → 调 Lark API 抓 chat detail + 当前成员 + 24h 消息回填，全部写 SQLite"))
    blocks.append(h(3, "UPDATE/DELETE 类（同步写旧 Base，量小不解耦）"))
    blocks.append(b("im.message.recalled_v1 — 消息撤回，标 is_deleted=1"))
    blocks.append(b("im.chat.member.user.deleted_v1 — 成员退群，移除 members 行"))
    blocks.append(b("im.chat.disbanded_v1 — 群解散，标 chat 为 dissolved"))
    blocks.append(p("Webhook 入站防重：event_id LRU set（10k 容量），飞书重发自动 dedup。"))
    blocks.append(p("并发：ThreadPoolExecutor(max_workers=12)。SQLite WAL 单写者自动 serialize。"))

    # === 五、多维表格 schema ===
    blocks.append(h(2, "五、多维表格 schema（旧 + 新双写）"))
    blocks.append(h(3, "旧 Base（PnRtbGmTpaVXwDsWBWPcPaEpnwh · 验证期）"))
    blocks.append(b("机器人群列表 tbl7PZ9s9yoKSHtJ"))
    blocks.append(b("机器人群消息记录 tbl9oNBOQrekT1O4"))
    blocks.append(b("机器人群成员记录 tblODVD4U82fn21P"))
    blocks.append(h(3, "新 Base（G42ybVmN9aAeYdsHW06cysTonmF · Helix · 项目门户 · 迁移目标）"))
    blocks.append(b("机器人群列表 tblXhxEs8Y5IvFbw"))
    blocks.append(b("机器人群消息记录 tblK0WYR1ebTarjR"))
    blocks.append(b("机器人群成员记录 tbl7aSVqPLBk1iRv"))
    blocks.append(p("3 张表字段完全相同，由 sync_feishu_groups_to_base.py 的 CHAT_FIELD_DEFS / BASE_MESSAGE_FIELD_DEFS / BASE_MEMBER_FIELD_DEFS 定义，bootstrap_secondary_base.py 一键复制结构到新 Base。"))
    blocks.append(h(3, "迁移切换路径"))
    blocks.append(b("等 secondary 队列清零（参 /admin/sync-stats）"))
    blocks.append(b("人工抽查新 Base 字段完整、数据一致"))
    blocks.append(b("把 .env 里 LARK_BASE_URL 和 LARK_BASE_URL_SECONDARY 对调 + 重启 server"))
    blocks.append(b("完全切换后 unset LARK_BASE_URL_SECONDARY（旧 Base sync worker 自动 noop）"))

    # === 六、定时任务 ===
    blocks.append(h(2, "六、定时任务（APScheduler）"))
    blocks.append(p("server.py 内置 6 个 job，全部独立线程池避免阻塞 webhook。"))
    blocks.append(code("""daily-sync          cron 16:00 UTC (= SGT 00:00)  全量同步飞书群 → 旧 Base + SQLite
external-join       cron 14:00 UTC (= SGT 22:00)  bot 自动加入授权用户的外部群
bulk-stats-refresh  cron 12:00 UTC (= SGT 20:00)  消息广播效果统计回流
cf-prewarm          interval 4min                  预热 CF Edge cache（dashboard 永远秒开）
primary-sync        interval 10s                   SQLite → 旧 Base 异步推送
secondary-sync      interval 15s                   SQLite → 新 Base 异步推送
""", "plain"))
    blocks.append(h(3, "daily-sync 详解"))
    blocks.append(b("调用 sync_feishu_groups_to_base.py main()，参数 --scheduled-daily --lite-mode --refresh-metadata-tables"))
    blocks.append(b("--refresh-metadata-tables 会重建群列表/成员/消息表，所以 daily-sync 结束后必须 _invalidate_lark_state() 清缓存"))
    blocks.append(b("结束时自动 local_db.seed_from_lark_base() 重 seed 本地副本做对账"))
    blocks.append(b("当前耗时 ~10min，24029 群 / 43388 消息 / 1196 成员"))
    blocks.append(h(3, "external-join 详解"))
    blocks.append(b("调用 ensure_bot_in_external_chats.py，依赖 feishu-bot-proxy 代理拿用户授权 access_token"))
    blocks.append(b("通过用户 access_token 列出他们所在群，diff 出 bot 不在的群"))
    blocks.append(b("新增 --active-since-days 30 过滤：跳过 30 天无消息的死群"))
    blocks.append(b("失败码常见：232017 用户非群主 / 232009 群已解散，均靠 --allow-chat-failures 单群失败不中断"))

    # === 七、公网部署 ===
    blocks.append(h(2, "七、公网部署（CF Worker + Tunnel）"))
    blocks.append(h(3, "Cloudflare 资源"))
    blocks.append(b("Account: Kelan656691@gmail.com (acct 2e2b291e8f3e011ca7824f19bcb77236)"))
    blocks.append(b("Zone: xiaomiao.win (81498ac216563761c63636b270a4caf1)"))
    blocks.append(b("Tunnel: chorus-lark-monitor UUID 9c7e347a-3b11-47d7-8e5f-18bb5d463397，必须 --protocol http2（默认 QUIC 走美西节点会超时）"))
    blocks.append(b("Worker: chorus-lark-events-gateway 路径白名单 + caches.default 边缘缓存 300s"))
    blocks.append(b("DNS: chorus.xiaomiao.win (Worker custom_domain), chorus-origin.xiaomiao.win (CNAME → tunnel UUID)"))
    blocks.append(h(3, "Worker 关键逻辑"))
    blocks.append(b("白名单：POST /lark/events + GET /、/src/*、/api/dashboard/*"))
    blocks.append(b("Edge cache：仅缓存 GET /src/* 静态资源；origin 回 no-store/no-cache/private 时跳过写 cache（避免空 payload 固化）"))
    blocks.append(b("x-cache 响应头：HIT/MISS/BYPASS 三态，便于诊断"))
    blocks.append(h(3, "feishu-bot-proxy（另一个 Worker）"))
    blocks.append(b("绑 feishu-bot.xiaomiao.win，做用户 OAuth + access_token 中转"))
    blocks.append(b("KV namespace GROUP_JOIN_TOKENS 存用户 token；secret GROUP_JOIN_ADMIN_TOKEN 鉴权"))

    # === 八、Dashboard ===
    blocks.append(h(2, "八、Dashboard 前端"))
    blocks.append(p("地址：https://chorus.xiaomiao.win/"))
    blocks.append(b("前端栈：React 18 UMD（unpkg）+ Babel standalone（unpkg）+ JSX 源码"))
    blocks.append(b("data.jsx 由 server.py 动态生成，从 SQLite 取 top-N 活跃群（默认 N=500）+ 每群最近 20 条消息"))
    blocks.append(b("payload 体积：~10MB raw / ~1.2MB gzipped，首次加载 ~1.8s（CF cache hit）"))
    blocks.append(b("已知瓶颈：Google Fonts + unpkg.com 境外 CDN 慢，首访 3-5s 主要花在这；后续可换 jsdelivr 或预编译 JSX"))

    # === 九、运维 / 监控 ===
    blocks.append(h(2, "九、运维 / 监控"))
    blocks.append(h(3, "常用端点（127.0.0.1:5678）"))
    blocks.append(code("""GET  /healthz                     存活探测
GET  /admin/jobs                  cron 列表 + next_run
POST /admin/run/{job_id}          手动触发某 job
GET  /admin/local-db-stats        SQLite 行数 + db 大小
GET  /admin/sync-stats            双 Base 队列深度 + 失败计数
GET  /lark/events/recent          webhook 事件统计 + 持久化计数
""", "plain"))
    blocks.append(h(3, "日志文件"))
    blocks.append(b("/Users/bytedance/chorus-lark-monitor/logs/server.err.log — server.py 主日志"))
    blocks.append(b("/Users/bytedance/chorus-lark-monitor/logs/server.out.log — uvicorn 访问日志 + 脚本 stdout"))
    blocks.append(b("/Users/bytedance/chorus-lark-monitor/logs/tunnel.err.log — cloudflared tunnel 错误"))
    blocks.append(h(3, "launchd 任务"))
    blocks.append(b("com.feishu-chat.server — uvicorn server:app 监听 127.0.0.1:5678"))
    blocks.append(b("com.feishu-chat.tunnel — cloudflared tunnel run，使用 ~/.cloudflared/chorus-lark-monitor.token"))
    blocks.append(p("plist 在 deployment/ 下，launchctl unload/load 重启。"))

    # === 十、关键配置（.env） ===
    blocks.append(h(2, "十、关键配置（.env）"))
    blocks.append(code("""LARK_APP_ID=cli_a75bb415d8ff9013       # 飞书 App ID
LARK_APP_SECRET=...                    # 飞书 App Secret
LARK_BASE_URL=...                      # 旧 Base URL (验证期 = primary)
LARK_BASE_URL_SECONDARY=...            # 新 Base URL (迁移目标)
GROUP_JOIN_PROXY_URL=https://feishu-bot.xiaomiao.win
GROUP_JOIN_ADMIN_TOKEN=...             # feishu-bot-proxy Worker 鉴权
WEB_MAX_GROUPS=500                     # dashboard 展示群数上限
WEB_MAX_MESSAGES_PER_GROUP=20          # 每群展示消息数
LARK_EVENT_POOL_SIZE=12                # webhook 后台线程池
EXTERNAL_GROUP_JOIN_ACTIVE_SINCE_DAYS=30  # 外部群活跃度过滤
""", "bash"))

    # === 十一、关键代码文件 ===
    blocks.append(h(2, "十一、关键代码文件"))
    blocks.append(code("""server.py                     # FastAPI + APScheduler 主入口
local_db.py                   # SQLite schema + helpers
sync_feishu_groups_to_base.py # 全量同步脚本 + FeishuClient + Base schema 定义
ensure_bot_in_external_chats.py  # 外部群自动入群
bulk_message_probe.py         # 群发消息效果统计
export_to_web.py              # 生成 data.jsx 模板
bootstrap_secondary_base.py   # 一次性建 3 张表到新 Base
deployment/worker/            # CF Worker 源码（wrangler 部署）
web/                          # dashboard 前端
""", "plain"))

    # === 十二、踩过的坑 ===
    blocks.append(h(2, "十二、踩过的坑（按时间倒序）"))
    blocks.append(b("CF Worker 覆写 cache-control 写 caches.default，遇到 warm-up 空 payload + prewarm 循环 → 看板永远 0 群。修法：origin 回 no-store 时 Worker 跳过写 cache"))
    blocks.append(b("ensure_bot_in_external_chats.py 看 .env 里 GROUP_JOIN_ADMIN_TOKEN= 是空字符串就抛 RuntimeError 0s 挂掉。修法：CF API 直接 PUT 新 secret 同步三处"))
    blocks.append(b("_create_member_rows_safely 一次性写 200+ 行触发 800010701 invalid_request。修法：按 200 切片"))
    blocks.append(b("Babel standalone 编译 10MB+ JSON 在浏览器 OOM。修法：data.jsx 不打 type=\"text/babel\"，按普通 JS 加载"))
    blocks.append(b("cloudflared 默认 QUIC 协议路由到美西节点 timeout。修法：--protocol http2"))
    blocks.append(b("Lark Base 限频 800004135 OpenAPIBatchAddRecords ~20/s/table。修法：FeishuClient.request 加 RATE_LIMIT_CODES set 自动 retry"))
    blocks.append(b("daily-sync --refresh-metadata-tables 重建表后旧 table_id 失效。修法：daily-sync 完成后 _invalidate_lark_state() + 重 seed 本地 SQLite"))

    # === 十三、TODO ===
    blocks.append(h(2, "十三、待办 / 后续演进"))
    blocks.append(b("等 secondary 回补稳定 → 切换 primary/secondary"))
    blocks.append(b("dashboard 首屏延迟根本治理：删 Google Fonts + 换 jsdelivr CDN，预计 5.8s → 1.5s"))
    blocks.append(b("如用户规模 ×5 后：消息写入按 500ms 缓冲批量化，减少 Base API 调用"))
    blocks.append(b("Lark Base 单表 ~100k 行后需要按月分表，避免触上限"))

    blocks.append(divider())
    blocks.append(p("最后更新：2026-05-13。本文由 server.py 同款 bot 通过 docx API 写入。"))
    return blocks


def write_blocks_in_chunks(token: str, doc_id: str, blocks: list, chunk_size: int = 20) -> None:
    """飞书 Docx blocks POST 单次 children 数量有上限 (一般 50)，分批写。"""
    total = len(blocks)
    for start in range(0, total, chunk_size):
        chunk = blocks[start : start + chunk_size]
        payload = {"children": chunk, "index": -1}
        req = urllib.request.Request(
            f"{LARK_OPEN}/open-apis/docx/v1/documents/{doc_id}/blocks/{doc_id}/children",
            data=json.dumps(payload).encode(),
            method="POST",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                d = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            print(f"  HTTP {e.code} writing chunk {start}-{start+len(chunk)}: {body[:500]}")
            raise
        if int(d.get("code", -1)) != 0:
            print(f"  ERR writing chunk {start}-{start+len(chunk)}: {d.get('msg')} (code={d.get('code')})")
            print("  payload preview:", json.dumps(chunk[0], ensure_ascii=False)[:300])
            raise SystemExit(2)
        print(f"  wrote chunk {start}-{start+len(chunk)} ({len(chunk)} blocks)")
        time.sleep(0.3)  # 限频避免连发


def main() -> int:
    if not (os.environ.get("LARK_APP_ID") and os.environ.get("LARK_APP_SECRET")):
        print("ERR: need LARK_APP_ID / LARK_APP_SECRET", file=sys.stderr)
        return 2
    token = get_tenant_token()
    blocks = build_blocks()
    print(f"target doc: {DOC_ID}")
    print(f"total blocks: {len(blocks)}")
    write_blocks_in_chunks(token, DOC_ID, blocks)
    print("\nDONE. open:")
    print(f"  https://bytedance.larkoffice.com/docx/{DOC_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
