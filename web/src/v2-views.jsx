// Three main views: DR Roster, Alert Triage, Dashboard Overview

function FilterChip({ active, onClick, children, dotColor }) {
  return (
    <button className={`chip ${active ? "active" : ""}`} onClick={onClick}>
      {dotColor && <span className="chip-dot" style={{ background: dotColor }}></span>}
      {children}
    </button>
  );
}

function TopBar({ now, activeTab = "dashboard", onSelectTab }) {
  const time = now.toLocaleTimeString("zh-CN", { hour12: false });
  return (
    <div className="topbar">
      <div className="brand">
        <div className="brand-mark"></div>
        <div>
          <div>群聊监控</div>
          <div style={{ fontSize: 10, fontWeight: 500, color: "var(--fg-3)", letterSpacing: "0.05em", marginTop: 1 }}>
            DR · 客户对话健康度
          </div>
        </div>
      </div>
      <nav className="topbar-tabs">
        <button
          className={`tb-tab ${activeTab === "dashboard" ? "active" : ""}`}
          onClick={() => onSelectTab && onSelectTab("dashboard")}
        >监控看板</button>
        <button
          className={`tb-tab ${activeTab === "broadcast" ? "active" : ""}`}
          onClick={() => onSelectTab && onSelectTab("broadcast")}
        >群发消息</button>
      </nav>
      <div className="topbar-spacer"></div>
      <div className="topbar-meta">
        <span className="live">实时同步</span>
        <span>{time}</span>
        <span>·</span>
        <span>{MOCK.GLOBAL.totalGroups.toLocaleString()} 群在册 · {MOCK.GLOBAL.activeToday} 今日活跃</span>
      </div>
    </div>
  );
}

function FilterBar({ view, setView, drFilter, setDrFilter, timeRange, setTimeRange, statusFilter, setStatusFilter }) {
  return (
    <div className="filterbar">
      <div className="view-switch">
        <button className={view === "roster" ? "active" : ""} onClick={() => setView("roster")}>DR 视角</button>
        <button className={view === "alerts" ? "active" : ""} onClick={() => setView("alerts")}>告警优先</button>
        <button className={view === "overview" ? "active" : ""} onClick={() => setView("overview")}>全局总览</button>
      </div>
      <div className="fb-divider"></div>
      <div className="fb-group">
        <span className="fb-label">时间</span>
        {[["today","今日"],["week","本周"],["month","本月"]].map(([k,v]) => (
          <FilterChip key={k} active={timeRange === k} onClick={() => setTimeRange(k)}>{v}</FilterChip>
        ))}
      </div>
      <div className="fb-divider"></div>
      <div className="fb-group">
        <span className="fb-label">状态</span>
        <FilterChip active={statusFilter === "all"} onClick={() => setStatusFilter("all")}>全部</FilterChip>
        <FilterChip active={statusFilter === "critical"} onClick={() => setStatusFilter("critical")} dotColor="var(--critical)">紧急</FilterChip>
        <FilterChip active={statusFilter === "warning"} onClick={() => setStatusFilter("warning")} dotColor="var(--warning)">告警</FilterChip>
        <FilterChip active={statusFilter === "silent"} onClick={() => setStatusFilter("silent")} dotColor="var(--silent)">沉默</FilterChip>
      </div>
      <div className="fb-divider"></div>
      <div className="fb-group">
        <span className="fb-label">DR</span>
        <FilterChip active={drFilter === "all"} onClick={() => setDrFilter("all")}>全部 10 人</FilterChip>
        {MOCK.DRS.slice(0, 5).map(d => (
          <FilterChip key={d.id} active={drFilter === d.id} onClick={() => setDrFilter(d.id)}>
            <Avatar dr={d} size="sm" />
            {d.name}
          </FilterChip>
        ))}
        <FilterChip onClick={() => {}} active={false}>+5</FilterChip>
      </div>
    </div>
  );
}

function StatStrip() {
  const G = MOCK.GLOBAL;
  return (
    <div className="stat-strip">
      <StatCard
        label="紧急告警"
        value={G.criticalAlerts}
        sub="关键词命中或超时严重"
        alertLevel="critical"
        sparkData={[3,5,4,7,6,9,8,10,8,11]}
        sparkColor="var(--critical)"
      />
      <StatCard
        label="一般告警"
        value={G.warningAlerts}
        sub="响应超时未处理"
        alertLevel="warning"
        sparkData={[8,10,7,12,9,11,10,13,11,12]}
        sparkColor="var(--warning)"
      />
      <StatCard
        label="沉默群"
        value={G.silentGroups}
        sub=">24h 无 DR 发言"
        alertLevel="silent"
        sparkData={[4,5,4,6,5,7,6,8,7,9]}
        sparkColor="var(--silent)"
      />
      <StatCard
        label="今日活跃群"
        value={G.activeToday}
        unit={`/ ${(G.totalGroups/1000).toFixed(0)}k`}
        sub="占比 0.78%"
        sparkData={[60,58,65,62,68,72,70,75,73,78]}
      />
      <StatCard
        label="今日消息"
        value={G.todayMessages.toLocaleString()}
        sub={`其中客户来信 ${G.todayInbound}`}
        sparkData={[200,260,340,420,520,610,680,720,690,750]}
      />
      <StatCard
        label="平均响应"
        value={G.avgResponseMin}
        unit="分钟"
        sub="目标 ≤ 30 分钟"
        sparkData={[45,42,38,40,35,32,30,28,32,30]}
        sparkColor="var(--accent)"
      />
    </div>
  );
}

function RosterView({ density, drFilter, statusFilter, onOpenDr, onOpenGroup }) {
  let drs = MOCK.DRS;
  if (drFilter !== "all") drs = drs.filter(d => d.id === drFilter);
  // sort by severity (criticals first, then warnings, then by score asc)
  drs = [...drs].sort((a, b) =>
    b.critical - a.critical ||
    b.warning - a.warning ||
    a.score - b.score
  );
  return (
    <div>
      <div className={`dr-grid ${density === "compact" ? "compact" : ""}`}>
        {drs.map(dr => (
          <DRCard key={dr.id} dr={dr} density={density} onOpen={onOpenDr} onOpenGroup={onOpenGroup} />
        ))}
      </div>
    </div>
  );
}

function AlertsView({ statusFilter, drFilter, onOpenGroup }) {
  let alerts = MOCK.ALERTS;
  if (statusFilter !== "all") {
    alerts = alerts.filter(a =>
      statusFilter === "critical" ? a.severity === "critical" :
      statusFilter === "warning" ? (a.severity === "warning" && a.type !== "silent") :
      statusFilter === "silent" ? a.type === "silent" : true
    );
  }
  if (drFilter !== "all") alerts = alerts.filter(a => a.drId === drFilter);

  const grouped = {
    critical: alerts.filter(a => a.severity === "critical"),
    warning: alerts.filter(a => a.severity === "warning" && a.type !== "silent"),
    silent: alerts.filter(a => a.type === "silent"),
  };

  const sectionDef = [
    { key: "critical", title: "紧急 · 立即处理", desc: "客户消息命中风险关键词或长时间未回复", color: "var(--critical)" },
    { key: "warning", title: "告警 · 待跟进", desc: "DR 响应超出 SLA", color: "var(--warning)" },
    { key: "silent", title: "沉默 · 关注", desc: "DR 已超过 24 小时未在群内发言", color: "var(--silent)" },
  ];

  return (
    <div>
      {sectionDef.map(s => grouped[s.key].length > 0 && (
        <div className="section" key={s.key}>
          <div className="section-head">
            <div style={{ width: 8, height: 8, borderRadius: 50, background: s.color, boxShadow: `0 0 10px ${s.color}` }}></div>
            <div className="section-title">{s.title}</div>
            <span className="section-count mono">{grouped[s.key].length} 项</span>
            <div style={{ flex: 1 }}></div>
            <span className="section-count" style={{ fontSize: 11 }}>{s.desc}</span>
          </div>
          <div className="alert-list">
            {grouped[s.key].slice(0, 12).map(a => (
              <AlertRow key={a.id} alert={a} onClick={() => onOpenGroup(a.group)} />
            ))}
            {grouped[s.key].length > 12 && (
              <div style={{ padding: "12px 18px", fontSize: 11.5, color: "var(--fg-2)", textAlign: "center" }}>
                还有 {grouped[s.key].length - 12} 项 · <span style={{ color: "var(--accent)", fontWeight: 600 }}>展开全部 →</span>
              </div>
            )}
          </div>
        </div>
      ))}
      {sectionDef.every(s => grouped[s.key].length === 0) && (
        <div className="section">
          <div className="empty-state">
            <div className="empty-state-icon" style={{ background: "var(--healthy-bg)", color: "var(--healthy)" }}>✓</div>
            <div style={{ fontWeight: 600, color: "var(--fg-0)", fontSize: 13 }}>所有群组当前状态良好</div>
            <div style={{ fontSize: 11, marginTop: 4 }}>无紧急告警、超时或沉默情况</div>
          </div>
        </div>
      )}
    </div>
  );
}

function OverviewView({ aiOn, onOpenDr, onOpenGroup }) {
  const G = MOCK.GLOBAL;
  const drsRanked = [...MOCK.DRS].sort((a, b) => a.score - b.score);
  const nowHr = new Date().getHours();

  return (
    <div>
      {aiOn && (
        <div className="section" style={{ marginBottom: 16 }}>
          <div style={{ padding: 16 }}>
            <div className="ai-block">
              <strong>今日态势：</strong>共发现 {G.criticalAlerts} 起紧急告警与 {G.warningAlerts} 起一般告警。
              <strong> 周梓涵</strong> 名下 <strong>云汐科技-项目交付群</strong> 客户连续两次提到「退款」，建议优先介入；
              <strong> 黄俊杰</strong> 名下有 4 个群已超 24 小时无 DR 发言，请确认是否在休假。
              全员平均响应时间为 {G.avgResponseMin} 分钟，较上周下降 12%。
            </div>
          </div>
        </div>
      )}

      <div className="overview-grid">
        {/* DR 排行 */}
        <div className="section">
          <div className="section-head">
            <div className="section-title">DR 健康度排行</div>
            <span className="section-count mono">10 人 · 升序</span>
            <div style={{ flex: 1 }}></div>
            <span className="section-count" style={{ fontSize: 11 }}>分值越低越需关注</span>
          </div>
          <div style={{ padding: "8px 16px", fontSize: 10, fontWeight: 600, color: "var(--fg-3)",
            textTransform: "uppercase", letterSpacing: "0.08em",
            display: "grid", gridTemplateColumns: "24px 28px 1fr 70px 80px 60px", gap: 12 }}>
            <span>#</span><span></span><span>DR</span>
            <span style={{ textAlign: "right" }}>14天活跃</span>
            <span style={{ textAlign: "right" }}>响应/告警</span>
            <span style={{ textAlign: "right" }}>分值</span>
          </div>
          {drsRanked.map((d, i) => (
            <div key={d.id} className="dr-leaderboard-row" onClick={() => onOpenDr(d)}>
              <div className="rank">{String(i + 1).padStart(2, "0")}</div>
              <Avatar dr={d} size="md" />
              <div>
                <div className="name">{d.name}</div>
                <div style={{ fontSize: 11, color: "var(--fg-3)", marginTop: 1 }}>
                  {d.activeGroups} 群 · {d.todayMessages} 消息
                </div>
              </div>
              <div style={{ display: "flex", justifyContent: "flex-end" }}>
                <Sparkline data={d.activity} width={70} height={20}
                  color={d.score < 60 ? "var(--critical)" : d.score < 80 ? "var(--warning)" : "var(--healthy)"} fill />
              </div>
              <div className="num">
                <span style={{ color: "var(--fg-1)" }}>{d.avgResponseMin}m</span>
                {(d.critical + d.warning) > 0 && (
                  <span style={{ color: d.critical > 0 ? "var(--critical)" : "var(--warning)", marginLeft: 6 }}>
                    ⚠{d.critical + d.warning}
                  </span>
                )}
              </div>
              <div className="num" style={{
                fontSize: 14, fontWeight: 700,
                color: d.score < 60 ? "var(--critical)" : d.score < 80 ? "var(--warning)" : "var(--healthy)"
              }}>{d.score}</div>
            </div>
          ))}
        </div>

        {/* Right column */}
        <div>
          <div className="section">
            <div className="section-head">
              <div className="section-title">24h 消息流量</div>
              <span className="section-count mono">总 {G.todayMessages} 条</span>
            </div>
            <div className="hourly">
              {G.hourly.map((v, i) => {
                const max = Math.max(...G.hourly);
                return (
                  <div key={i} className={`bar ${i === nowHr ? "now" : ""}`}
                    style={{ height: `${(v / max) * 100}%` }}
                    title={`${i}:00 · ${v} 条`}></div>
                );
              })}
            </div>
            <div className="hourly-axis">
              <span>00</span><span>06</span><span>12</span><span>18</span><span>23</span>
            </div>
          </div>

          <div className="section" style={{ marginTop: 16 }}>
            <div className="section-head">
              <div className="section-title">状态分布</div>
              <span className="section-count mono">{MOCK.GROUPS.length} 活跃群</span>
            </div>
            <div style={{ padding: "12px 18px" }}>
              {[
                { k: "critical", l: "紧急", n: MOCK.GROUPS.filter(g => g.status === "critical").length, c: "var(--critical)" },
                { k: "warning", l: "告警", n: MOCK.GROUPS.filter(g => g.status === "warning").length, c: "var(--warning)" },
                { k: "silent", l: "沉默", n: MOCK.GROUPS.filter(g => g.status === "silent").length, c: "var(--silent)" },
                { k: "healthy", l: "正常", n: MOCK.GROUPS.filter(g => g.status === "healthy").length, c: "var(--healthy)" },
              ].map(s => {
                const pct = (s.n / MOCK.GROUPS.length) * 100;
                return (
                  <div key={s.k} className="bar-row">
                    <div className="row" style={{ gap: 6 }}>
                      <span className="chip-dot" style={{ background: s.c }}></span>
                      <span style={{ fontSize: 12 }}>{s.l}</span>
                    </div>
                    <div className="bar-track">
                      <span style={{ width: `${pct}%`, background: s.c, opacity: 0.85 }}></span>
                    </div>
                    <div className="num">{s.n} · {pct.toFixed(0)}%</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* Top alert preview */}
      <div className="section">
        <div className="section-head">
          <div className="section-title">需立即处理</div>
          <span className="section-count mono">最紧急 6 项</span>
          <div style={{ flex: 1 }}></div>
          <button className="chip">查看全部告警 →</button>
        </div>
        <div className="alert-list">
          {MOCK.ALERTS.slice(0, 6).map(a => (
            <AlertRow key={a.id} alert={a} onClick={() => onOpenGroup(a.group)} />
          ))}
        </div>
      </div>
    </div>
  );
}

// Drawer contents
function DRDetailContent({ dr, onOpenGroup, aiOn }) {
  const groups = MOCK.groupsForDr(dr.id);
  const [tab, setTab] = useState("groups");
  return (
    <div>
      {/* DR header summary */}
      <div className="row" style={{ marginBottom: 16, padding: "14px 16px", background: "var(--bg-2)", borderRadius: 10 }}>
        <Avatar dr={dr} size="lg" />
        <div style={{ flex: 1 }}>
          <div style={{ fontWeight: 700, fontSize: 16 }}>{dr.name}</div>
          <div style={{ fontSize: 11.5, color: "var(--fg-2)", marginTop: 2 }}>
            活跃 {dr.activeGroups} 群 · 总计 {dr.totalGroups} · 最近活跃 {fmtMin(dr.lastActiveMinutes)}
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 24, fontWeight: 700, color:
            dr.score < 60 ? "var(--critical)" : dr.score < 80 ? "var(--warning)" : "var(--healthy)" }}>
            {dr.score}
          </div>
          <div style={{ fontSize: 9.5, color: "var(--fg-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>健康度</div>
        </div>
      </div>

      {aiOn && (
        <div className="ai-block" style={{ marginBottom: 14 }}>
          <strong>{dr.name}</strong> 今日共参与 {dr.todayMessages} 条消息，回复 {dr.todayReplies} 条；
          {dr.critical > 0 && <span> 有 <strong style={{color: "var(--critical)"}}>{dr.critical}</strong> 个群处于紧急状态，</span>}
          {dr.silent > 0 && <span> {dr.silent} 个群已沉默超过 24 小时，</span>}
          建议优先处理列表顶部的标红群组。
        </div>
      )}

      <div className="tabs">
        <button className={`tab ${tab === "groups" ? "active" : ""}`} onClick={() => setTab("groups")}>
          负责的群 · {groups.length}
        </button>
        <button className={`tab ${tab === "stats" ? "active" : ""}`} onClick={() => setTab("stats")}>
          指标趋势
        </button>
      </div>

      {tab === "groups" && (
        <div>
          {groups.map(g => <GroupRow key={g.id} group={g} onClick={() => onOpenGroup(g)} />)}
        </div>
      )}
      {tab === "stats" && (
        <div className="col" style={{ gap: 16 }}>
          <div>
            <div style={{ fontSize: 11, color: "var(--fg-3)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
              14 日消息趋势
            </div>
            <div style={{ background: "var(--bg-2)", borderRadius: 10, padding: 16 }}>
              <Sparkline data={dr.activity} width={620} height={60}
                color="var(--accent)" fill />
            </div>
          </div>
          <div className="dr-stats" style={{ borderRadius: 10, border: "0.5px solid var(--line-1)" }}>
            <div className="dr-stat"><div className="dr-stat-lbl">紧急</div><div className="dr-stat-val danger">{dr.critical}</div></div>
            <div className="dr-stat"><div className="dr-stat-lbl">告警</div><div className="dr-stat-val warn">{dr.warning}</div></div>
            <div className="dr-stat"><div className="dr-stat-lbl">沉默</div><div className="dr-stat-val">{dr.silent}</div></div>
            <div className="dr-stat"><div className="dr-stat-lbl">正常</div><div className="dr-stat-val">{dr.healthy}</div></div>
          </div>
        </div>
      )}
    </div>
  );
}

function GroupConvContent({ group, aiOn }) {
  const dr = MOCK.DRS.find(d => d.id === group.drId);
  const conv = MOCK.conversationFor(group.id);
  return (
    <div>
      <div className="row" style={{ marginBottom: 14, padding: "14px 16px", background: "var(--bg-2)", borderRadius: 10 }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontWeight: 700, fontSize: 14 }}>{group.name}</div>
          <div style={{ fontSize: 11.5, color: "var(--fg-2)", marginTop: 2 }}>
            {group.members} 人 · 客户主联系人：{group.customerName}
          </div>
        </div>
        <StatusPill status={group.status} />
      </div>

      {group.alerts.length > 0 && (
        <div style={{ marginBottom: 14 }}>
          {group.alerts.map((a, i) => (
            <div key={i} className={`alert-trigger s-${a.severity}`}
              style={{ padding: "8px 12px", background: a.severity === "critical" ? "var(--critical-bg)" : "var(--warning-bg)",
                borderRadius: 8, marginBottom: 6, fontSize: 12, fontWeight: 600 }}>
              ⚠ {a.label}
            </div>
          ))}
        </div>
      )}

      {aiOn && (
        <div className="ai-block">
          <strong>对话摘要：</strong>客户主要咨询集成方案进度，并追问报表 bug。
          {group.status === "critical" && <span> 最新一条消息含负面情绪关键词，需立即跟进。</span>}
          {group.status === "warning" && <span> 客户已等待多次，建议尽快给出明确时间。</span>}
        </div>
      )}

      <div style={{ fontSize: 11, color: "var(--fg-3)", textTransform: "uppercase", letterSpacing: "0.08em", margin: "8px 0" }}>
        最近对话 · {conv.length} 条
      </div>
      <div className="conv">
        {conv.map((m, i) => {
          const flagged = m.who === "customer" && i === conv.length - 1 && group.status === "critical";
          return (
            <div key={m.id} className={`conv-msg ${m.who} ${flagged ? "flagged" : ""}`}>
              <div className="av" style={{
                background: m.who === "dr" ? `oklch(0.78 0.12 ${dr.hue})` : "var(--bg-3)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 700,
                color: m.who === "dr" ? "oklch(0.18 0 0)" : "var(--fg-1)"
              }}>
                {m.who === "dr" ? dr.initial : group.customerName[0]}
              </div>
              <div>
                <div className="bub">{m.text}</div>
                <div className="meta">
                  {m.author} · {fmtMin(m.minutesAgo)}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

Object.assign(window, {
  TopBar, FilterBar, StatStrip, RosterView, AlertsView, OverviewView,
  DRDetailContent, GroupConvContent
});
