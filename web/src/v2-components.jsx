// Shared atomic components (avatars, pills, sparklines, helpers)

const { useState, useMemo, useEffect, useRef } = React;

function fmtMin(min) {
  if (min == null || isNaN(min)) return "—";
  if (min < 1) return "刚刚";
  if (min < 60) return `${min}分钟前`;
  if (min < 1440) return `${Math.floor(min / 60)}小时前`;
  return `${Math.floor(min / 1440)}天前`;
}
function fmtDuration(min) {
  if (min < 60) return `${min}分钟`;
  if (min < 1440) {
    const h = Math.floor(min / 60), m = min % 60;
    return m ? `${h}时${m}分` : `${h}小时`;
  }
  const d = Math.floor(min / 1440), h = Math.floor((min % 1440) / 60);
  return h ? `${d}天${h}时` : `${d}天`;
}

function Avatar({ dr, size = "md" }) {
  if (!dr) return null;
  const sz = size === "lg" ? 36 : size === "sm" ? 22 : 28;
  const cls = size === "lg" ? "avatar" : size === "sm" ? "avatar avatar-sm" : "avatar avatar-md";
  const bg = `oklch(0.78 0.12 ${dr.hue})`;
  return (
    <div className={cls} style={{ background: bg, width: sz, height: sz }}>
      {dr.initial}
    </div>
  );
}

function StatusPill({ status, count, label }) {
  const map = {
    critical: { cls: "pill-critical", txt: "紧急" },
    warning: { cls: "pill-warning", txt: "告警" },
    silent: { cls: "pill-silent", txt: "沉默" },
    healthy: { cls: "pill-healthy", txt: "正常" },
  };
  const m = map[status] || map.healthy;
  return (
    <span className={`pill ${m.cls}`}>
      <span className="dot"></span>
      {label || m.txt}
      {count != null && <span style={{ opacity: 0.85, marginLeft: 2 }}>{count}</span>}
    </span>
  );
}

// SVG sparkline
function Sparkline({ data, width = 60, height = 24, color = "currentColor", fill = false }) {
  if (!data || !data.length) return null;
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const w = width, h = height;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - ((v - min) / (max - min || 1)) * (h - 2) - 1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <svg className="spark" width={w} height={h} viewBox={`0 0 ${w} ${h}`}>
      {fill && (
        <polygon
          points={`0,${h} ${pts} ${w},${h}`}
          fill={color}
          opacity="0.18"
        />
      )}
      <polyline
        points={pts}
        fill="none"
        stroke={color}
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

// Health bar split into 4 segments by status
function HealthBar({ critical, warning, silent, healthy }) {
  const total = critical + warning + silent + healthy || 1;
  return (
    <div className="health-bar">
      {critical > 0 && <span style={{ width: `${(critical / total) * 100}%`, background: "var(--critical)" }}></span>}
      {warning > 0 && <span style={{ width: `${(warning / total) * 100}%`, background: "var(--warning)" }}></span>}
      {silent > 0 && <span style={{ width: `${(silent / total) * 100}%`, background: "var(--silent)" }}></span>}
      {healthy > 0 && <span style={{ width: `${(healthy / total) * 100}%`, background: "var(--healthy)" }}></span>}
    </div>
  );
}

function GroupRow({ group, onClick }) {
  const dr = MOCK.DRS.find(d => d.id === group.drId);
  return (
    <div className={`group-row s-${group.status}`} onClick={onClick}>
      <div className="group-status"></div>
      <div className="group-row-body">
        <div className="group-row-name">
          {group.name}
          {group.status !== "healthy" && (
            <StatusPill status={group.status} />
          )}
        </div>
        <div className={`group-row-msg ${group.lastMsgKind}`}>
          {group.lastMsgKind === "inbound" ? `${group.customerName}：` : "DR："}
          {group.lastMsg}
        </div>
      </div>
      <div className="group-row-time">
        {fmtMin(group.lastInboundMinutes)}
      </div>
    </div>
  );
}

function DRCard({ dr, density, onOpen, onOpenGroup }) {
  const groups = MOCK.groupsForDr(dr.id);
  const visible = density === "compact" ? groups.slice(0, 3) : groups.slice(0, 5);
  const hidden = groups.length - visible.length;
  const hasCritical = dr.critical > 0;

  return (
    <div className={`dr-card ${density === "compact" ? "compact" : ""} ${hasCritical ? "has-critical" : ""}`}>
      <div className="dr-head" onClick={() => onOpen(dr)}>
        <Avatar dr={dr} size="lg" />
        <div className="dr-meta">
          <div className="dr-name">
            {dr.name}
            {hasCritical && <StatusPill status="critical" count={dr.critical} />}
          </div>
          <div className="dr-sub">
            活跃 {dr.activeGroups} / 总计 {dr.totalGroups} 群 · 最近 {fmtMin(dr.lastActiveMinutes)}
          </div>
        </div>
        <div className="dr-score">
          <div className="dr-score-num" style={{
            color: dr.score < 60 ? "var(--critical)" :
                   dr.score < 80 ? "var(--warning)" :
                   "var(--healthy)"
          }}>{dr.score}</div>
          <div className="dr-score-lbl">健康度</div>
        </div>
      </div>

      <div style={{ padding: "10px 16px 4px" }}>
        <HealthBar critical={dr.critical} warning={dr.warning} silent={dr.silent} healthy={dr.healthy} />
      </div>

      <div className="dr-stats">
        <div className="dr-stat">
          <div className="dr-stat-lbl">今日消息</div>
          <div className="dr-stat-val">{dr.todayMessages}</div>
        </div>
        <div className="dr-stat">
          <div className="dr-stat-lbl">回复数</div>
          <div className="dr-stat-val">{dr.todayReplies}</div>
        </div>
        <div className="dr-stat">
          <div className="dr-stat-lbl">平均响应</div>
          <div className={`dr-stat-val ${dr.avgResponseMin > 60 ? "warn" : ""}`}>
            {dr.avgResponseMin}<span className="u">分</span>
          </div>
        </div>
        <div className="dr-stat">
          <div className="dr-stat-lbl">超时群</div>
          <div className={`dr-stat-val ${dr.overdueGroups > 0 ? "danger" : ""}`}>
            {dr.overdueGroups}
          </div>
        </div>
      </div>

      <div className="dr-groups">
        <div className="dr-groups-head">
          关注列表 · {groups.filter(g => g.status !== "healthy").length} 项需处理
        </div>
        {visible.map(g => (
          <GroupRow key={g.id} group={g} onClick={() => onOpenGroup(g)} />
        ))}
        {hidden > 0 && (
          <div style={{ padding: "8px 10px", fontSize: 11, color: "var(--fg-3)", textAlign: "center" }}>
            还有 {hidden} 个群 · 点击 DR 查看全部
          </div>
        )}
        {visible.length === 0 && (
          <div className="dr-empty">无关注项 · 状态良好</div>
        )}
      </div>
    </div>
  );
}

function AlertRow({ alert, onClick }) {
  const dr = MOCK.DRS.find(d => d.id === alert.drId);
  const g = alert.group;
  return (
    <div className={`alert-row s-${alert.severity}`} onClick={onClick}>
      <div className="stripe"></div>
      <StatusPill status={alert.severity === "critical" ? "critical" : alert.type === "silent" ? "silent" : "warning"} />
      <div className="alert-content">
        <div className="alert-group-name">{g.name}</div>
        <div className={`alert-trigger s-${alert.severity}`}>
          {alert.label}
        </div>
      </div>
      <div className="alert-msg">「{g.lastMsg}」</div>
      <div className="alert-dr">
        <Avatar dr={dr} size="sm" />
        <span>{dr.name}</span>
      </div>
      <div className="alert-time mono">
        {fmtMin(g.lastInboundMinutes)}
      </div>
    </div>
  );
}

// Drawer for DR detail and group conversation
function Drawer({ open, onClose, children, title, subtitle, headExtra }) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <>
      <div className="drawer-bg" onClick={onClose}></div>
      <div className="drawer">
        <div className="drawer-hd">
          {headExtra}
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontWeight: 700, fontSize: 14 }}>{title}</div>
            {subtitle && <div style={{ fontSize: 11, color: "var(--fg-2)", marginTop: 2 }}>{subtitle}</div>}
          </div>
          <button className="close" onClick={onClose}>✕</button>
        </div>
        <div className="drawer-body">{children}</div>
      </div>
    </>
  );
}

// Stat card with optional sparkline
function StatCard({ label, value, unit, sub, alertLevel, sparkData, sparkColor }) {
  return (
    <div className={`stat ${alertLevel ? `alert-${alertLevel}` : ""}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">
        {value}
        {unit && <span className="unit">{unit}</span>}
      </div>
      {sub && <div className="stat-sub">{sub}</div>}
      {sparkData && (
        <div className="stat-spark">
          <Sparkline data={sparkData} color={sparkColor || "var(--fg-2)"} fill />
        </div>
      )}
    </div>
  );
}

Object.assign(window, {
  fmtMin, fmtDuration, Avatar, StatusPill, Sparkline, HealthBar,
  GroupRow, DRCard, AlertRow, Drawer, StatCard
});
