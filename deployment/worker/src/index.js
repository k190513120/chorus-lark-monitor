// Cloudflare Worker · chorus.xiaomiao.win 公网网关
//
// 同时承担两个职责：
//   1) 飞书 webhook 入口：POST /lark/events
//   2) Dashboard 只读访问：GET /、GET /src/*、GET /api/dashboard/data 等
//
// 显式 block：/admin/*、POST /api/bulk-send、/ws/*、/lark/events/recent 等敏感面。
// 没在白名单里的统一 404。

const WEBHOOK_PATH = "/lark/events";

// GET 白名单（精确匹配）
const GET_EXACT = new Set([
  "/",
  "/index.html",
  "/api/dashboard/data",
  "/api/broadcast/analysis",
]);

// GET 白名单（前缀匹配）
const GET_PREFIX = [
  "/src/",                     // 前端 jsx/js/css 静态资源 + /src/data.jsx 动态
  "/api/bulk-send/refresh/",   // 广播刷新状态轮询
];

const WEBHOOK_TIMEOUT_MS = 2500;     // 飞书 3s 内要响应
const DASHBOARD_TIMEOUT_MS = 25000;  // dashboard 首次冷启动 cache 可能慢

function isReadAllowed(path) {
  if (GET_EXACT.has(path)) return true;
  for (const p of GET_PREFIX) if (path.startsWith(p)) return true;
  return false;
}

async function forward(request, env, pathAndQuery, timeoutMs) {
  const backend = env.BACKEND_BASE;
  if (!backend) return new Response("Misconfigured: BACKEND_BASE missing", { status: 500 });

  const headers = new Headers();
  for (const h of [
    "content-type", "accept", "accept-language", "accept-encoding", "user-agent",
    "x-lark-signature", "x-lark-request-timestamp", "x-lark-request-nonce",
  ]) {
    const v = request.headers.get(h);
    if (v) headers.set(h, v);
  }
  const cfIp = request.headers.get("cf-connecting-ip");
  if (cfIp) headers.set("x-forwarded-for", cfIp);

  let body = null;
  if (request.method !== "GET" && request.method !== "HEAD") {
    body = await request.arrayBuffer();
    if (body.byteLength > 1024 * 1024) {
      return new Response("Payload too large", { status: 413 });
    }
  }

  // CF Edge cache 配置：只缓存 GET 的 dashboard 资源（webhook POST 不缓存，避免回 Lark 错误响应）
  const path = pathAndQuery.split("?")[0];
  const cacheable = request.method === "GET" && (
    path === "/" || path === "/index.html" ||
    path.startsWith("/src/") || path.startsWith("/api/dashboard/")
  );

  const fetchOpts = {
    method: request.method,
    headers,
    body,
    signal: AbortSignal.timeout(timeoutMs),
  };
  if (cacheable) {
    fetchOpts.cf = { cacheEverything: true, cacheTtl: 60 };  // 60s edge cache
  }

  try {
    const upstream = await fetch(`${backend}${pathAndQuery}`, fetchOpts);
    const respHeaders = new Headers();
    for (const h of [
      "content-type", "cache-control", "etag", "last-modified",
      "content-encoding", "content-length", "vary",
    ]) {
      const v = upstream.headers.get(h);
      if (v) respHeaders.set(h, v);
    }
    return new Response(upstream.body, { status: upstream.status, headers: respHeaders });
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    return new Response(`Backend unreachable: ${msg}`, { status: 502 });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;
    const pathAndQuery = path + url.search;

    // 1) 飞书 webhook —— 只放 POST /lark/events
    if (path === WEBHOOK_PATH) {
      if (method !== "POST") return new Response("Method Not Allowed", { status: 405 });
      return forward(request, env, WEBHOOK_PATH, WEBHOOK_TIMEOUT_MS);
    }

    // 2) Dashboard 只读 —— 只放白名单 GET/HEAD
    if (method === "GET" || method === "HEAD") {
      if (isReadAllowed(path)) {
        return forward(request, env, pathAndQuery, DASHBOARD_TIMEOUT_MS);
      }
    }

    // 3) 其他全部 404
    return new Response("Not Found", { status: 404 });
  },
};
