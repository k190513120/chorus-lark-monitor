// Adapter: 把 window.AppData（来自 /src/data.jsx，由 FastAPI 实时生成）
// 转换成 design 期望的 MOCK 形状（DRs / GROUPS / GLOBAL / ALERTS）。
// AppData.GROUPS[i] 字段：name, company, owner, members[], messages[], todayMsgs,
//   lastMinutesAgo, unreplyMinutes, sentiment, tags, topSpeakers, ...
// AppData.TEAM[] 字段：id, name, color, msgs

(function buildMockFromAppData() {
  const NORMAL_REPLIES = [
    "好的，我们这边今天会安排技术同学跟进",
    "方案已经发到您邮箱了，麻烦查收",
    "这个需求我们评估一下，明天给您答复",
    "下周对齐一下进度",
    "API 文档已贴在群里了",
  ];
  const ALERT_KEYWORDS = ["退款", "投诉", "不用了", "太慢了", "解约", "效果不好", "失望", "怎么还没"];

  const APP = window.AppData || { GROUPS: [], TEAM: [] };
  const APP_GROUPS = APP.GROUPS || [];

  // -- 内部成员（"DR"）的发现 ---------------------------------------------
  // 从 AppData.TEAM 拿，他们是按消息量从消息表聚合出来的内部成员。
  // 我们把每个 team member 当作一个 DR。
  const drsByOpenId = new Map();
  let hueIdx = 0;
  function ensureDr(openId, name) {
    if (!openId) return null;
    if (drsByOpenId.has(openId)) return drsByOpenId.get(openId);
    const hue = (hueIdx++ * 36) % 360;
    const dr = {
      id: openId,
      name: name || openId.slice(0, 6),
      initial: (name || "?").slice(0, 1),
      hue,
      // aggregate fields (filled later)
      activeGroups: 0,
      totalGroups: 0,
      critical: 0,
      warning: 0,
      silent: 0,
      healthy: 0,
      todayMessages: 0,
      todayInbound: 0,
      todayReplies: 0,
      avgResponseMin: 0,
      _avgResponseSamples: [],
      overdueGroups: 0,
      lastActiveMinutes: 1440 * 30,
      activity: Array(14).fill(0),
      score: 100,
    };
    drsByOpenId.set(openId, dr);
    return dr;
  }

  for (const t of APP.TEAM || []) {
    ensureDr(t.id, t.name);
  }

  // -- 群聊状态判断 + 拉成 design 形状 ------------------------------------
  function findKeyword(text) {
    if (!text) return null;
    return ALERT_KEYWORDS.find(k => text.includes(k)) || null;
  }
  function lastInboundMessage(g) {
    const msgs = g.messages || [];
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      if (m.from && m.from.side === "client") return m;
    }
    return null;
  }
  function lastOutboundMessage(g) {
    const msgs = g.messages || [];
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      if (m.from && m.from.side === "team") return m;
    }
    return null;
  }
  function pickCustomerName(g) {
    for (const m of g.members || []) {
      if (m.side === "client" && m.name) return m.name;
    }
    return "客户";
  }
  function pickDrForGroup(g) {
    // 优先 topSpeakers[0]（按消息量），否则任一 team 成员
    if (g.topSpeakers && g.topSpeakers.length) {
      const top = g.topSpeakers[0];
      const teamMember = (g.members || []).find(m => m.name === top.name && m.side === "team");
      if (teamMember) return ensureDr(teamMember.id, teamMember.name);
      return ensureDr(top.name, top.name);
    }
    for (const m of g.members || []) {
      if (m.side === "team") return ensureDr(m.id, m.name);
    }
    return null;
  }

  const GROUPS = [];
  for (let i = 0; i < APP_GROUPS.length; i++) {
    const g = APP_GROUPS[i];
    const dr = pickDrForGroup(g);
    if (!dr) continue;

    const lastInbound = lastInboundMessage(g);
    const lastOutbound = lastOutboundMessage(g);
    const lastInMsAgo = lastInbound ? Math.max(0, Math.floor((Date.now() - lastInbound.time) / 60000)) : 9999;
    const lastOutMsAgo = lastOutbound ? Math.max(0, Math.floor((Date.now() - lastOutbound.time) / 60000)) : 9999;
    const customerLastText = lastInbound ? lastInbound.text : "";

    let status = "healthy";
    const alerts = [];
    let lastMsgKind = lastInbound && lastOutbound
      ? (lastInbound.time > lastOutbound.time ? "inbound" : "outbound")
      : (lastInbound ? "inbound" : "outbound");
    let lastMsg = (lastMsgKind === "inbound" ? (lastInbound && lastInbound.text) : (lastOutbound && lastOutbound.text)) || "";

    const kw = findKeyword(customerLastText);
    if (kw && lastInbound && (!lastOutbound || lastInbound.time > lastOutbound.time)) {
      status = "critical";
      alerts.push({ type: "keyword", label: `命中关键词「${kw}」`, severity: "critical" });
      if (lastInMsAgo > 30) {
        alerts.push({ type: "timeout", label: `客户消息已 ${formatDur(lastInMsAgo)}未回复`, severity: "critical" });
      }
    } else if (g.unreplyMinutes && g.unreplyMinutes > 60) {
      status = "warning";
      alerts.push({ type: "timeout", label: `超时 ${formatDur(g.unreplyMinutes)}未回复`, severity: "warning" });
    } else if (lastOutMsAgo > 1440) {
      status = "silent";
      alerts.push({ type: "silent", label: `已沉默 ${formatDur(lastOutMsAgo)}`, severity: "warning" });
    }

    const today = g.todayMsgs || 0;
    const todayInbound = Math.floor(today * 0.4) || 0;

    GROUPS.push({
      id: g.id,
      chatId: g.chat_id,
      name: g.name,
      company: g.company || g.name,
      type: "客户群",
      drId: dr.id,
      status,
      alerts,
      members: g.memberCount || 0,
      customerName: pickCustomerName(g),
      lastMsg: lastMsg || "（无最近消息）",
      lastMsgKind,
      lastMsgAt: lastInbound ? lastInbound.time : (lastOutbound ? lastOutbound.time : null),
      lastInboundMinutes: lastInMsAgo,
      lastOutboundMinutes: lastOutMsAgo,
      todayMessages: today,
      todayInbound,
      avgResponseMin: g.unreplyMinutes ? Math.min(g.unreplyMinutes, 120) : (5 + ((i * 7) % 60)),
      sentiment: g.sentiment ? (g.sentiment.key === "happy" ? 0.5 : g.sentiment.key === "concern" ? -0.3 : g.sentiment.key === "angry" ? -0.7 : 0.1) : 0.1,
      activity: Array.from({ length: 14 }, (_, d) => Math.max(0, today - d * 2 + (i % 3))),
      _raw: g,  // keep ref so drawer can render real messages later
    });
  }

  function formatDur(min) {
    if (min < 60) return `${min}分钟`;
    if (min < 1440) {
      const h = Math.floor(min / 60), m = min % 60;
      return m ? `${h}时${m}分` : `${h}小时`;
    }
    const d = Math.floor(min / 1440), h = Math.floor((min % 1440) / 60);
    return h ? `${d}天${h}时` : `${d}天`;
  }

  // -- DR 聚合 ------------------------------------------------------------
  const drsList = Array.from(drsByOpenId.values());
  for (const dr of drsList) {
    const myGroups = GROUPS.filter(g => g.drId === dr.id);
    dr.activeGroups = myGroups.length;
    dr.totalGroups = myGroups.length;  // we don't know dormant count
    dr.critical = myGroups.filter(g => g.status === "critical").length;
    dr.warning = myGroups.filter(g => g.status === "warning").length;
    dr.silent = myGroups.filter(g => g.status === "silent").length;
    dr.healthy = myGroups.filter(g => g.status === "healthy").length;
    dr.todayMessages = myGroups.reduce((a, b) => a + b.todayMessages, 0);
    dr.todayInbound = myGroups.reduce((a, b) => a + b.todayInbound, 0);
    dr.todayReplies = Math.max(0, dr.todayMessages - dr.todayInbound);
    dr.overdueGroups = myGroups.filter(g => g.alerts.some(a => a.type === "timeout")).length;
    dr.lastActiveMinutes = myGroups.length ? Math.min(...myGroups.map(g => g.lastOutboundMinutes)) : 9999;
    const respSamples = myGroups.map(g => g.avgResponseMin).filter(x => x > 0);
    dr.avgResponseMin = respSamples.length ? Math.round(respSamples.reduce((a, b) => a + b, 0) / respSamples.length) : 0;
    dr.activity = Array.from({ length: 14 }, (_, i) =>
      myGroups.reduce((a, g) => a + (g.activity[i] || 0), 0)
    );
    dr.score = Math.max(0, Math.min(100, Math.round(
      100 - dr.critical * 12 - dr.warning * 6 - dr.silent * 3 + (dr.avgResponseMin && dr.avgResponseMin < 30 ? 5 : -5)
    )));
    delete dr._avgResponseSamples;
  }

  // 只保留有活跃群的 DR
  const DRS = drsList.filter(d => d.activeGroups > 0);

  // -- Global ------------------------------------------------------------
  const totalActive = GROUPS.length;
  const totalRegistered = (APP.DASHBOARD && APP.DASHBOARD.totalGroups) || totalActive;
  const todayMsgsAll = GROUPS.reduce((a, b) => a + b.todayMessages, 0);
  const todayInboundAll = GROUPS.reduce((a, b) => a + b.todayInbound, 0);
  const criticalAlerts = GROUPS.filter(g => g.status === "critical").length;
  const warningAlerts = GROUPS.filter(g => g.status === "warning").length;
  const silentGroups = GROUPS.filter(g => g.status === "silent").length;
  const avgResponseMin = DRS.length ? Math.round(DRS.reduce((a, b) => a + (b.avgResponseMin || 0), 0) / DRS.length) : 0;
  // 24h hourly volume (from AppData.DASHBOARD if available, else flat)
  const hourly = (APP.DASHBOARD && APP.DASHBOARD.hourlyMsgs) || Array.from({ length: 24 }, () => 0);

  const GLOBAL = {
    totalGroups: totalRegistered,
    activeToday: totalActive,
    totalDR: DRS.length,
    todayMessages: todayMsgsAll,
    todayInbound: todayInboundAll,
    criticalAlerts,
    warningAlerts,
    silentGroups,
    avgResponseMin,
    hourly,
  };

  // -- Alerts feed -------------------------------------------------------
  const ALERTS = [];
  for (const g of GROUPS) {
    g.alerts.forEach((a, i) => {
      ALERTS.push({
        id: `${g.id}-${i}`,
        groupId: g.id,
        drId: g.drId,
        ...a,
        group: g,
      });
    });
  }
  const sevOrder = { critical: 0, warning: 1 };
  ALERTS.sort((a, b) => {
    const sa = sevOrder[a.severity] ?? 9, sb = sevOrder[b.severity] ?? 9;
    if (sa !== sb) return sa - sb;
    return (b.group.lastInboundMinutes || 0) - (a.group.lastInboundMinutes || 0);
  });

  // -- Conversation builder（drawer 用）---------------------------------
  function conversationFor(groupId) {
    const g = GROUPS.find(x => x.id === groupId);
    if (!g || !g._raw || !g._raw.messages) return [];
    return g._raw.messages.slice(-12).map((m, i) => ({
      id: m.id || i,
      who: m.from && m.from.side === "team" ? "dr" : "customer",
      text: m.text || "",
      minutesAgo: Math.max(0, Math.floor((Date.now() - (m.time || Date.now())) / 60000)),
      author: (m.from && m.from.name) || "—",
    }));
  }

  function groupsForDr(drId) {
    const out = GROUPS.filter(g => g.drId === drId);
    const order = { critical: 0, warning: 1, silent: 2, healthy: 3 };
    return out.sort((a, b) => order[a.status] - order[b.status] || (b.lastInboundMinutes || 0) - (a.lastInboundMinutes || 0));
  }

  window.MOCK = {
    DRS, GROUPS, GLOBAL, ALERTS,
    formatDuration: formatDur,
    groupsForDr,
    conversationFor,
  };
})();
