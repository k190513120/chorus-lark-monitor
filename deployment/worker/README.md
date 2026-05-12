# chorus-lark-events-gateway

公网 webhook 入口的 Cloudflare Worker。

## 它做什么

- **唯一公网入口**：`https://chorus.xiaomiao.win/lark/events`
- 只接收 `POST`，其他 method 一律 405
- 其他 path 一律 404
- 校验通过后把请求透传给 `BACKEND_BASE`（cloudflared tunnel 暴露的子域）
- 飞书 3 秒超时，Worker 端 abort 2.5 秒避免上游慢导致飞书重试

## 部署

```bash
cd deployment/worker
npm install -g wrangler   # 如果没装
wrangler login            # 浏览器登录 Cloudflare（一次性）
wrangler deploy           # 部署
```

部署后会自动：
1. 把代码 push 到 Cloudflare Workers
2. 在 xiaomiao.win 区下建 route `chorus.xiaomiao.win/lark/events` → 本 Worker
3. 自动签 HTTPS 证书

## 后端（Mac mini cloudflared tunnel）

Worker 转发的目标是 `BACKEND_BASE`，默认 `https://chorus-origin.xiaomiao.win`。这是 cloudflared tunnel 在 xiaomiao.win 区下注册的子域，proxied=on（橙色云），指向 Mac 上 127.0.0.1:5678 的 uvicorn。

部署 tunnel 的命令在 deployment/run_tunnel.sh（见下）。

## 校验

```bash
# 1. 路径白名单：应该 404
curl -i https://chorus.xiaomiao.win/

# 2. 方法白名单：应该 405
curl -i https://chorus.xiaomiao.win/lark/events

# 3. 真转发：应该返回飞书希望的 challenge 响应
curl -i -X POST https://chorus.xiaomiao.win/lark/events \
  -H 'content-type: application/json' \
  -d '{"challenge":"test123","type":"url_verification"}'
# 预期：{"challenge":"test123"} 或类似（取决于 server.py 实现）
```
