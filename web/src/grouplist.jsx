// Left column — group list with filters + alert highlight

const GroupListItem = ({ group, active, onClick }) => {
  const stalled = group.unreplyMinutes > 60;
  const isActive = active;
  return (
    <button
      onClick={onClick}
      style={{
        width: "100%", textAlign: "left",
        display: "flex", alignItems: "flex-start", gap: 10,
        padding: "10px 12px",
        borderRadius: 12,
        background: isActive ? "var(--accent-soft)" : "transparent",
        border: `1px solid ${isActive ? "transparent" : "transparent"}`,
        transition: "background 120ms ease",
        position: "relative",
      }}
      onMouseEnter={e => { if (!isActive) e.currentTarget.style.background = "var(--bg-sunk)"; }}
      onMouseLeave={e => { if (!isActive) e.currentTarget.style.background = "transparent"; }}
    >
      {stalled && (
        <span style={{
          position: "absolute", left: 0, top: 14, bottom: 14, width: 3,
          borderRadius: "0 3px 3px 0", background: "var(--amber)",
        }}/>
      )}
      <Avatar avatar={group.avatar} size={40} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
          {group.pinned && <Icon name="pin" size={11} color="var(--accent)" strokeWidth={2}/>}
          <div style={{
            fontSize: 13.5, fontWeight: 500, color: "var(--ink)",
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", minWidth: 0, flex: 1,
          }}>
            {group.name}
          </div>
          <span style={{ fontSize: 10.5, color: "var(--ink-4)", flexShrink: 0 }} className="num">
            {AppData.formatRelative(Date.now() - group.lastMinutesAgo * 60 * 1000)}
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 3, color: "var(--ink-3)", fontSize: 11.5 }}>
          <span>{group.owner.name}</span>
          <span style={{ color: "var(--ink-4)" }}>·</span>
          <span>{group.memberCount} 人</span>
          <span style={{ color: "var(--ink-4)" }}>·</span>
          <span className="num">{group.todayMsgs} 条/今</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 4, marginTop: 6, flexWrap: "wrap" }}>
          {stalled ? (
            <Pill tone="amber" icon={<Icon name="clock" size={10} strokeWidth={2.3}/>}>
              {Math.floor(group.unreplyMinutes / 60)}h{group.unreplyMinutes % 60}m 未回
            </Pill>
          ) : group.unread > 0 ? (
            <Pill tone="accent">
              <span className="num">{group.unread}</span> 未读
            </Pill>
          ) : null}
          {group.tags.slice(0, 1).map(t => (
            <Pill key={t} tone="outline">{t}</Pill>
          ))}
        </div>
      </div>
    </button>
  );
};

const GroupList = ({ groups, activeId, onSelect, onOpenBroadcast }) => {
  const [query, setQuery] = React.useState("");
  const [filter, setFilter] = React.useState("all");

  const filters = [
    { id: "all",     label: "全部",     count: groups.length },
    { id: "alert",   label: "需回复",   count: groups.filter(g => g.unreplyMinutes > 60).length },
    { id: "active",  label: "活跃",     count: groups.filter(g => g.todayMsgs > 30).length },
    { id: "pinned",  label: "置顶",     count: groups.filter(g => g.pinned).length },
  ];

  const filtered = groups.filter(g => {
    if (query && !g.name.includes(query) && !g.company.includes(query)) return false;
    if (filter === "alert") return g.unreplyMinutes > 60;
    if (filter === "active") return g.todayMsgs > 30;
    if (filter === "pinned") return g.pinned;
    return true;
  }).sort((a, b) => {
    if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
    if ((a.unreplyMinutes > 60) !== (b.unreplyMinutes > 60)) return a.unreplyMinutes > 60 ? -1 : 1;
    return a.lastMinutesAgo - b.lastMinutesAgo;
  });

  return (
    <div style={{
      width: 380, flexShrink: 0,
      borderRight: "1px solid var(--line)",
      display: "flex", flexDirection: "column",
      background: "var(--bg-elev)",
      height: "100%",
    }}>
      {/* header */}
      <div style={{ padding: "16px 16px 10px" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
          <div>
            <div style={{ fontSize: 15, fontWeight: 600 }}>群聊监控</div>
            <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginTop: 1 }}>
              <span className="num">{groups.length}</span> 个活跃群 · 实时同步
            </div>
          </div>
          <Btn variant="primary" size="sm" icon={<Icon name="send" size={12}/>}
            onClick={onOpenBroadcast}>
            群发消息
          </Btn>
        </div>

        <div style={{
          position: "relative",
          background: "var(--bg-sunk)",
          borderRadius: 10,
          padding: "6px 10px",
          display: "flex", alignItems: "center", gap: 8,
          border: "1px solid transparent",
        }}>
          <Icon name="search" size={14} color="var(--ink-3)"/>
          <input
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="搜索群名、公司、成员…"
            style={{
              flex: 1, background: "transparent", border: "none",
              fontSize: 13, color: "var(--ink)", minWidth: 0,
            }}
          />
          {query && (
            <button onClick={() => setQuery("")}
              style={{ color: "var(--ink-3)", display: "grid", placeItems: "center" }}>
              <Icon name="x" size={12}/>
            </button>
          )}
        </div>

        <div style={{ display: "flex", gap: 4, marginTop: 10, overflowX: "auto" }}>
          {filters.map(f => (
            <button key={f.id} onClick={() => setFilter(f.id)}
              style={{
                padding: "5px 10px", borderRadius: 999,
                fontSize: 12,
                background: filter === f.id ? "var(--ink)" : "transparent",
                color: filter === f.id ? "var(--bg-elev)" : "var(--ink-3)",
                fontWeight: 500, whiteSpace: "nowrap",
                display: "inline-flex", alignItems: "center", gap: 4,
                border: `1px solid ${filter === f.id ? "var(--ink)" : "var(--line)"}`,
                transition: "all 120ms",
              }}>
              {f.label}
              <span className="num" style={{
                fontSize: 10.5,
                color: filter === f.id ? "var(--bg-elev)" : "var(--ink-4)",
                opacity: 0.8,
              }}>{f.count}</span>
            </button>
          ))}
        </div>
      </div>

      {/* list */}
      <div className="scroll" style={{ flex: 1, overflowY: "auto", padding: "4px 8px 16px" }}>
        {filtered.length === 0 ? (
          <div style={{ textAlign: "center", color: "var(--ink-4)", fontSize: 12, padding: 40 }}>
            没有匹配的群
          </div>
        ) : filtered.map(g => (
          <GroupListItem key={g.id} group={g}
            active={g.id === activeId}
            onClick={() => onSelect(g)} />
        ))}
      </div>
    </div>
  );
};

Object.assign(window, { GroupList });
