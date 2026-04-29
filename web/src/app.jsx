// Root App

const { useState, useEffect } = React;

function App() {
  const [activeId, setActiveId] = useState(() => {
    return localStorage.getItem("cm.activeId") || AppData.GROUPS[0].id;
  });
  const [broadcastOpen, setBroadcastOpen] = useState(false);
  const [broadcastPrefill, setBroadcastPrefill] = useState(null);

  const [editMode, setEditMode] = useState(false);
  const [tweaks, setTweaks] = useState(() => ({ ...window.TWEAK_DEFAULTS }));

  useEffect(() => { window.applyTweaks(tweaks); }, []);
  useEffect(() => { localStorage.setItem("cm.activeId", activeId); }, [activeId]);

  // Edit-mode host protocol
  useEffect(() => {
    const onMsg = (e) => {
      if (!e.data || typeof e.data !== "object") return;
      if (e.data.type === "__activate_edit_mode") setEditMode(true);
      if (e.data.type === "__deactivate_edit_mode") setEditMode(false);
    };
    window.addEventListener("message", onMsg);
    window.parent.postMessage({ type: "__edit_mode_available" }, "*");
    return () => window.removeEventListener("message", onMsg);
  }, []);

  const activeGroup = AppData.GROUPS.find(g => g.id === activeId) || AppData.GROUPS[0];

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar onOpenBroadcast={() => { setBroadcastPrefill(null); setBroadcastOpen(true); }}/>
      <div style={{ flex: 1, display: "flex", minHeight: 0 }}>
        <GroupList
          groups={AppData.GROUPS}
          activeId={activeId}
          onSelect={g => setActiveId(g.id)}
          onOpenBroadcast={() => { setBroadcastPrefill(null); setBroadcastOpen(true); }}
        />
        <div className="scroll" style={{ flex: 1, overflowY: "auto", background: "var(--bg-sunk)" }}>
          <div style={{ padding: "18px 22px 0" }}>
            <StatsOverview D={AppData.DASHBOARD} broadcasts={AppData.BROADCASTS} onAlertOpen={g => setActiveId(g.id)} />
          </div>
          <div style={{ padding: "18px 22px 22px" }}>
            <Card padded={false} style={{ overflow: "hidden", height: "calc(100vh - 470px)", minHeight: 560, display: "flex", flexDirection: "column" }}>
              <DetailPane group={activeGroup} />
            </Card>
          </div>
        </div>
      </div>

      <BroadcastModal
        open={broadcastOpen}
        onClose={() => setBroadcastOpen(false)}
        groups={AppData.GROUPS}
        prefillGroup={broadcastPrefill}
      />

      {editMode && (
        <TweaksPanel tweaks={tweaks} setTweaks={setTweaks} onClose={() => setEditMode(false)}/>
      )}
    </div>
  );
}

const TopBar = ({ onOpenBroadcast }) => {
  return (
    <header style={{
      height: 56, padding: "0 22px",
      borderBottom: "1px solid var(--line)",
      background: "var(--bg-elev)",
      display: "flex", alignItems: "center", gap: 16,
      flexShrink: 0,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{
          width: 30, height: 30, borderRadius: 9,
          background: "var(--ink)", color: "var(--bg-elev)",
          display: "grid", placeItems: "center",
          fontWeight: 600, fontSize: 13,
          fontFamily: "var(--font-num)",
        }}>
          C
        </div>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, lineHeight: 1.1 }}>Chorus · 群聊管理台</div>
          <div style={{ fontSize: 11, color: "var(--ink-3)" }}>飞书群消息实时监控</div>
        </div>
      </div>

      <nav style={{ display: "flex", gap: 4, marginLeft: 14 }}>
        {["监控看板", "团队", "客户画像", "设置"].map((t, i) => (
          <button key={t} style={{
            padding: "6px 12px", fontSize: 13, borderRadius: 8,
            color: i === 0 ? "var(--ink)" : "var(--ink-3)",
            background: i === 0 ? "var(--bg-sunk)" : "transparent",
            fontWeight: i === 0 ? 500 : 400,
          }}>{t}</button>
        ))}
      </nav>

      <div style={{ flex: 1 }}/>

      <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12, color: "var(--ink-3)" }}>
        <span style={{
          display: "inline-flex", alignItems: "center", gap: 5,
          padding: "4px 8px", borderRadius: 999,
          background: "var(--sage-soft)", color: "oklch(0.35 0.08 155)",
        }}>
          <span style={{
            width: 6, height: 6, borderRadius: 999, background: "oklch(0.55 0.13 155)",
            boxShadow: "0 0 0 3px oklch(0.55 0.13 155 / 0.25)",
          }}/>
          实时同步中
        </span>
      </div>

      <Btn variant="primary" size="md" icon={<Icon name="send" size={13}/>} onClick={onOpenBroadcast}>
        群发消息
      </Btn>

      <div style={{
        width: 34, height: 34, borderRadius: 999,
        background: "oklch(0.82 0.09 45)", color: "oklch(0.32 0.10 45)",
        display: "grid", placeItems: "center",
        fontWeight: 600, fontSize: 13,
      }}>管</div>
    </header>
  );
};

ReactDOM.createRoot(document.getElementById("root")).render(<App/>);
