# 飞书群聊消息同步到多维表格

脚本文件：`sync_feishu_groups_to_base.py`

前端监控台：`web/index.html`（按设计稿 `Chorus · 群聊管理台` 实现，数据由 `export_to_web.py` 从多维表格导出）

这个脚本会：

- 拉取机器人所在的群列表
- 拉取每个群的基础资料、进群链接、成员列表
- 拉取每个群的消息
- 在目标 Base 中创建或复用三张表
- 自动补齐缺失字段
- 批量写入群列表、群成员记录和消息记录

默认表名：

- `机器人群列表`
- `机器人群成员记录`
- `机器人群消息记录`

## 环境变量

```bash
export LARK_APP_ID='你的 app id'
export LARK_APP_SECRET='你的 app secret'
export LARK_BASE_URL='你的多维表格链接'
```

## 常用命令

先验证小样本：

```bash
python3 sync_feishu_groups_to_base.py --max-chats 2 --max-messages-per-chat 3 --recreate-tables
```

按时间范围同步：

```bash
python3 sync_feishu_groups_to_base.py --start 2026-04-01 --end 2026-04-22 --recreate-tables
```

按群分片同步：

```bash
python3 sync_feishu_groups_to_base.py --skip-chats 0 --max-chats 200 --recreate-tables
python3 sync_feishu_groups_to_base.py --skip-chats 200 --max-chats 200
python3 sync_feishu_groups_to_base.py --skip-chats 400 --max-chats 200
```

按创建时间从近到远拉最近创建的群：

```bash
python3 sync_feishu_groups_to_base.py --chat-order created_desc --max-chats 10 --max-messages-per-chat 20
```

定时任务增量同步：

```bash
./run_daily_sync.sh
```

## 前端监控台

`web/` 下是根据设计稿实现的只读前端，通过 Babel standalone 直接加载 jsx，不需要构建。

- 数据来源：`export_to_web.py` 从飞书 Base 读取群、成员、消息，生成 `web/src/data.jsx`
- 设计稿照搬：index.html + 8 个组件 jsx 未做样式修改

### 更新前端数据

```bash
export LARK_APP_ID='...'
export LARK_APP_SECRET='...'
export LARK_BASE_URL='...'
export WEB_MAX_GROUPS=0                 # 可选，默认 0 = 全部群
export WEB_MAX_MESSAGES_PER_GROUP=0     # 可选，默认 0 = 每群全部消息
export SYNC_TIMEZONE=Asia/Shanghai      # 可选，默认北京时间
python3 export_to_web.py
```

会重写 `web/src/data.jsx`，按 `消息数 → 成员数` 排序；`WEB_MAX_GROUPS=0` 时导出全部群。

### 本地查看

```bash
./web/serve.sh          # 默认 http://localhost:5678/
# 或指定端口：PORT=8000 ./web/serve.sh
```

浏览器打开 `http://localhost:5678/`。**不能直接 file:// 打开**，babel 加载外部 jsx 需要 http 协议。

### 数据字段说明

- `team` vs `client`：根据成员表中的 `成员租户Key`，出现频次最高的租户视为 `team`（即 Chorus / 我方），其余视为 `client`
- `sentiment`：从消息文本关键词粗糙推断（😀关键词→满意；bug/投诉等→不满；？/咨询→顾虑）
- `tags`：按群名关键字派生（咨询/售后/实施中 等）
- `unreplyMinutes`：客户最后一条发言距今 - 我方随后回复时间差
- `readStatus`：按最近互动时间粗糙估算，飞书 OpenAPI 暂无批量群消息已读状态接口

## 参数说明

- `--start`：消息起始时间，支持 `YYYY-MM-DD` 或 ISO 8601
- `--end`：消息结束时间，支持 `YYYY-MM-DD` 或 ISO 8601
- `--skip-chats`：跳过前 N 个群，适合分片跑
- `--max-chats`：最多同步多少个群
- `--max-messages-per-chat`：每个群最多同步多少条消息
- `--chat-order`：按群创建时间排序，支持 `created_asc` / `created_desc`
- `--sync-timezone`：日期窗口使用的时区，默认 `Asia/Shanghai`
- `--scheduled-daily`：启用定时增量模式，自动用本地状态文件记住上次同步位置
- `--scheduled-baseline`：定时任务首次进入新群范围时从哪里开始，默认 `today`，即从北京时间当天 00:00 开始补一次
- `--state-file`：定时增量模式的本地状态文件路径
- `--initial-lookback-hours`：首次没有状态文件时，往前回看多少小时
- `--refresh-metadata-tables`：每天刷新群表和成员表快照，但保留消息历史表
- `--skip-share-links`：不请求每个群的进群链接，适合全量群每日同步，减少 API 调用
- `--recreate-tables`：删除同名同步表并重建
- `--verbose`：输出更多调试日志

## 表结构说明

- `机器人群列表`：群名称、群类型、官方群标签、群标签、成员总数、成员摘要、进群链接等
- `机器人群成员记录`：每个群成员一行，含成员 ID、姓名、租户信息，以及“成员”人员字段
- `机器人群消息记录`：每条群消息一行，含 `消息内容`、`提取消息内容`、`消息体JSON`，以及“发送者”人员字段
- 群表新增了 `群主` 人员字段和 `群聊类型` 单选字段
- 成员表和消息表新增了 `关联群组` 关联记录字段，可直接点回群表
- 如果表里已经存在飞书原生 `群聊` 群组字段，脚本会额外通过 bitable v1 接口补写该字段

## 当前规模建议

当前这个机器人能看到的群很多时，不建议直接跑“全量群 + 全量历史消息”。

更稳妥的做法：

1. 先用 `--max-chats` 做小样本验证。
2. 再按 `--skip-chats` 分批跑。
3. 或者先加 `--start` / `--end` 限定时间范围。
4. 日常任务推荐跑“全量群 + 当天起的消息增量”，这是当前 `run_daily_sync.sh` 的默认策略。

## 定时任务建议

推荐直接用：

```bash
./run_daily_sync.sh
```

它会：

- 用本地 `.sync_state.json` 记录上次成功同步到的时间
- 第一次进入新的群范围时，从北京时间当天 00:00 开始补一次消息；之后从 `上次成功结束时间 + 1秒` 继续拉（避免边界重复）
- 写入消息前预读消息表的 `消息ID` 集合做去重兜底（避免上次部分失败导致的重复）
- 默认覆盖应用机器人所在全部群，刷新 `机器人群列表` 和 `机器人群成员记录`
- 保留 `机器人群消息记录` 的历史增量数据

### Cloudflare Pages 部署

前端 `web/` 是纯静态，直接用 Cloudflare Pages 托管：

1. 把仓库推到 GitHub，在 Cloudflare Dashboard `Workers & Pages` → `Create` → `Pages` → `Connect to Git` 选中该仓库
2. 构建配置：
   - **Framework preset**：None
   - **Build command**：留空
   - **Build output directory**：`web`
   - **Root directory**：留空
3. 首次部署完成后，GitHub Actions 每天 10:00 跑完 sync 会把新 `web/src/data.jsx` commit 回仓库，Pages 检测到 push 自动触发重建
4. 自定义域名：Pages 设置 → `Custom domains` 绑定即可

### GitHub Actions 定时任务（推荐）

仓库已自带 `.github/workflows/daily-sync.yml`，每天北京时间 10:00（UTC 02:00）自动跑一次。

启用步骤：

1. 把项目推到 GitHub 仓库
2. 在仓库的 `Settings` → `Secrets and variables` → `Actions` 里添加三个 secret：
   - `LARK_APP_ID`
   - `LARK_APP_SECRET`
   - `LARK_BASE_URL`
3. 工作流每次跑完会：
   - 提交更新后的 `.sync_state.json`（增量记忆）
   - 跑 `export_to_web.py` 重新生成 `web/src/data.jsx` 并一起提交
   - 后者触发 Cloudflare Pages 自动重建
4. 可选：`Settings` → `Secrets and variables` → `Actions` → `Variables` 里加 `WEB_MAX_GROUPS` 控制前端最多展示的群数（默认 `0`，即全部群），加 `WEB_MAX_MESSAGES_PER_GROUP` 控制每群展示消息数（默认 `0`，即全部消息）
5. 在 `Actions` 页可以手动 `Run workflow` 触发一次跑通验证

注意：GitHub Actions 的 cron 偶尔会延迟几分钟，并非严格 10:00。

## 外部群自动拉机器人

脚本文件：`ensure_bot_in_external_chats.py`

它会先用机器人身份列出当前 bot 已在的群，再逐个读取已授权用户的 `user_access_token`，列出这些用户所在的外部群，按 `chat_id` 做差集后把当前应用机器人拉进缺失的外部群。默认是 dry-run，不会改群。

```bash
export LARK_APP_ID='...'
export LARK_APP_SECRET='...'
export GROUP_JOIN_PROXY_URL='https://feishu-bot.xiaomiao.win'
export GROUP_JOIN_ADMIN_TOKEN='...'

./run_external_group_join.sh                 # dry-run
./run_external_group_join.sh --apply         # 真正拉机器人入群
```

授权池优先推荐用 `wechat_bot` 里的 Cloudflare Worker：让用户访问 `/group-join/auth?include_chat_scope=1` 完成授权。这里必须包含列群权限，否则只能拉单个群，不能枚举用户所在外部群。

需要的权限：

- 用户授权：`offline_access`、`auth:user.id:read`、`im:chat:read`、`im:chat.members:write_only`
- 机器人/应用：能用 `tenant_access_token` 调用 `im/v1/chats`，也就是当前日同步已经在使用的群列表权限

如果不用 Worker，也可以用本地 JSON token 池：

```bash
export GROUP_JOIN_USER_TOKEN_POOL='/path/to/.feishu_group_join_user_tokens.json'
./run_external_group_join.sh --apply
```

仓库已新增 `.github/workflows/external-group-join.yml`，每天北京时间 22:00 定时拉机器人进已授权用户加入的外部群。启用前需要在 GitHub Actions secrets 里增加：

- `LARK_APP_ID`
- `LARK_APP_SECRET`
- `GROUP_JOIN_ADMIN_TOKEN`

可选变量：

- `GROUP_JOIN_PROXY_URL`：不配置时默认 `https://feishu-bot.xiaomiao.win`

### 本地 cron 备选

```bash
0 10 * * * cd /Users/bytedance/Desktop/群聊消息统计 && export LARK_APP_ID='你的 app id' && export LARK_APP_SECRET='你的 app secret' && export LARK_BASE_URL='你的多维表格链接' && ./run_daily_sync.sh >> logs/daily_sync.log 2>&1
```

首次执行前建议先建日志目录：

```bash
mkdir -p /Users/bytedance/Desktop/群聊消息统计/logs
```
