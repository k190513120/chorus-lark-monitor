// Cloudflare Worker · /lark/events 边缘网关
// 公网只有这一条 path 能进，其他全部 404。
// 拦截非法请求，把合法的 webhook 透传给 tunnel 后端（Mac mini）。

const ALLOWED_PATH = "/lark/events";
const ALLOWED_METHODS = new Set(["POST"]);

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 1. 路径白名单
    if (url.pathname !== ALLOWED_PATH) {
      return new Response("Not Found", { status: 404 });
    }

    // 2. 方法白名单
    if (!ALLOWED_METHODS.has(request.method)) {
      return new Response("Method Not Allowed", { status: 405 });
    }

    // 3. 后端地址（tunnel 暴露出来的 origin）
    const backend = env.BACKEND_BASE;
    if (!backend) {
      return new Response("Misconfigured: BACKEND_BASE missing", { status: 500 });
    }

    // 4. 透传 body + 关键 header
    const backendUrl = `${backend}${ALLOWED_PATH}`;
    const headers = new Headers();
    // 复制 Content-Type + 飞书签名相关头
    for (const h of ["content-type", "x-lark-signature", "x-lark-request-timestamp", "x-lark-request-nonce"]) {
      const v = request.headers.get(h);
      if (v) headers.set(h, v);
    }
    // 可选：透传客户端 IP（飞书出口 IP）给后端日志
    const cfIp = request.headers.get("cf-connecting-ip");
    if (cfIp) headers.set("x-forwarded-for", cfIp);

    // 5. 可选签名校验（如果 env.LARK_EVENT_VERIFY_TOKEN 配置了）
    //    飞书 URL challenge 阶段需要后端解开 encrypt 字段，这里不做硬校验，让后端处理
    //    但可以加 rate limit / size limit
    const body = await request.arrayBuffer();
    if (body.byteLength > 1024 * 1024) {
      return new Response("Payload too large", { status: 413 });
    }

    // 6. 转发
    try {
      const upstream = await fetch(backendUrl, {
        method: "POST",
        headers,
        body,
        // 严格控制超时（飞书 3s 内要响应；后端 1.5s 留 buffer）
        signal: AbortSignal.timeout(2500),
      });

      // 透传上游响应
      const respHeaders = new Headers();
      const ct = upstream.headers.get("content-type");
      if (ct) respHeaders.set("content-type", ct);
      return new Response(upstream.body, {
        status: upstream.status,
        headers: respHeaders,
      });
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      return new Response(`Backend unreachable: ${msg}`, { status: 502 });
    }
  },
};
