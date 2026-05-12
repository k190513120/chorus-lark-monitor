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
const EDGE_CACHE_TTL = 300;          // 5 分钟边缘缓存（用 Cache API 显式实现）

function isReadAllowed(path) {
  if (GET_EXACT.has(path)) return true;
  for (const p of GET_PREFIX) if (path.startsWith(p)) return true;
  return false;
}

function isCacheable(method, path) {
  if (method !== "GET") return false;
  return path === "/" || path === "/index.html" ||
    path.startsWith("/src/") || path.startsWith("/api/dashboard/");
}

// 构造一个稳定的 cache key：用 host + path + 是否接受 gzip 区分
// 把 Accept-Encoding 二值化（gzip / no-gzip），避免每个 ua 一份 cache
function buildCacheKey(request) {
  const url = new URL(request.url);
  const acceptsGzip = (request.headers.get("accept-encoding") || "").includes("gzip");
  url.searchParams.set("__enc", acceptsGzip ? "gz" : "id");
  return new Request(url.toString(), { method: "GET" });
}

async function fetchFromOrigin(request, env, pathAndQuery, timeoutMs) {
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

  try {
    const upstream = await fetch(`${backend}${pathAndQuery}`, {
      method: request.method,
      headers,
      body,
      signal: AbortSignal.timeout(timeoutMs),
    });
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

async function forward(request, env, ctx, pathAndQuery, timeoutMs) {
  const path = pathAndQuery.split("?")[0];

  // 不缓存的：直通
  if (!isCacheable(request.method, path)) {
    return fetchFromOrigin(request, env, pathAndQuery, timeoutMs);
  }

  // 缓存的：先查 Cache API
  const cache = caches.default;
  const cacheKey = buildCacheKey(request);
  let cached = await cache.match(cacheKey);
  if (cached) {
    const resp = new Response(cached.body, cached);
    resp.headers.set("x-cache", "HIT");
    return resp;
  }

  // miss：回源 + 写 cache
  const origin = await fetchFromOrigin(request, env, pathAndQuery, timeoutMs);
  if (origin.status === 200) {
    const toCache = origin.clone();
    // 覆写 cache-control 让 CF Cache API 强制缓存 EDGE_CACHE_TTL 秒
    const cacheHeaders = new Headers(toCache.headers);
    cacheHeaders.set("cache-control", `public, max-age=${EDGE_CACHE_TTL}, s-maxage=${EDGE_CACHE_TTL}`);
    const cacheResp = new Response(toCache.body, {
      status: toCache.status,
      statusText: toCache.statusText,
      headers: cacheHeaders,
    });
    ctx.waitUntil(cache.put(cacheKey, cacheResp));
  }
  const resp = new Response(origin.body, origin);
  resp.headers.set("x-cache", "MISS");
  return resp;
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
      return fetchFromOrigin(request, env, WEBHOOK_PATH, WEBHOOK_TIMEOUT_MS);
    }

    // 2) Dashboard 只读 —— 只放白名单 GET/HEAD
    if (method === "GET" || method === "HEAD") {
      if (isReadAllowed(path)) {
        return forward(request, env, ctx, pathAndQuery, DASHBOARD_TIMEOUT_MS);
      }
    }

    // 3) 其他全部 404
    return new Response("Not Found", { status: 404 });
  },
};
