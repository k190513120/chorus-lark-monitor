// 顶层 App — 把 design 的 3 视图（DR/告警/总览）和 broadcast tab 串起来。

const { useState, useEffect } = React;

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "view": "roster",
  "density": "regular",
  "theme": "dark",
  "aiOn": true,
  "accent": "blue"
}/*EDITMODE-END*/;

const ACCENT_HUES = {
  blue:   { h: 220, h2: 145 },
  purple: { h: 285, h2: 195 },
  green:  { h: 165, h2: 220 },
  amber:  { h: 65,  h2: 200 },
};

function App() {
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

  // 顶层 tab：监控看板 / 群发消息
  const [activeTab, setActiveTab] = useState(() =>
    localStorage.getItem("cm.activeTab") || "dashboard"
  );
  useEffect(() => { localStorage.setItem("cm.activeTab", activeTab); }, [activeTab]);

  // 时钟（用于 TopBar 实时同步显示）
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 30000);
    return () => clearInterval(id);
  }, []);

  // 主题
  useEffect(() => {
    document.documentElement.setAttribute("data-theme", t.theme || "dark");
  }, [t.theme]);

  // 强调色
  useEffect(() => {
    const a = ACCENT_HUES[t.accent] || ACCENT_HUES.blue;
    document.documentElement.style.setProperty("--accent",   `oklch(0.78 0.14 ${a.h})`);
    document.documentElement.style.setProperty("--accent-2", `oklch(0.82 0.14 ${a.h2})`);
  }, [t.accent]);

  // dashboard 内部状态
  const [drFilter, setDrFilter] = useState("all");
  const [timeRange, setTimeRange] = useState("today");
  const [statusFilter, setStatusFilter] = useState("all");
  const [openDr, setOpenDr] = useState(null);
  const [openGroup, setOpenGroup] = useState(null);

  // broadcast modal 状态
  const [broadcastOpen, setBroadcastOpen] = useState(false);
  const [broadcastPrefill, setBroadcastPrefill] = useState(null);
  const openBroadcastWizard = (prefill = null) => {
    setBroadcastPrefill(prefill);
    setBroadcastOpen(true);
  };

  const broadcasts = (window.AppData && window.AppData.BROADCASTS) || [];
  const groups = (window.AppData && window.AppData.GROUPS) || [];

  return (
    <div className="app">
      <div className="shell">
        <TopBar now={now} activeTab={activeTab} onSelectTab={setActiveTab} />

        {activeTab === "dashboard" && (
          <>
            <FilterBar
              view={t.view} setView={(v) => setTweak("view", v)}
              drFilter={drFilter} setDrFilter={setDrFilter}
              timeRange={timeRange} setTimeRange={setTimeRange}
              statusFilter={statusFilter} setStatusFilter={setStatusFilter}
            />
            <div className="main">
              <StatStrip />
              {t.view === "roster" && (
                <RosterView
                  density={t.density}
                  drFilter={drFilter}
                  statusFilter={statusFilter}
                  onOpenDr={setOpenDr}
                  onOpenGroup={setOpenGroup}
                />
              )}
              {t.view === "alerts" && (
                <AlertsView
                  statusFilter={statusFilter}
                  drFilter={drFilter}
                  onOpenGroup={setOpenGroup}
                />
              )}
              {t.view === "overview" && (
                <OverviewView
                  aiOn={t.aiOn}
                  onOpenDr={setOpenDr}
                  onOpenGroup={setOpenGroup}
                />
              )}
            </div>
          </>
        )}

        {activeTab === "broadcast" && (
          <div className="main broadcast-main">
            <BroadcastView
              broadcasts={broadcasts}
              groupCount={groups.length}
              onNew={() => openBroadcastWizard()}
            />
          </div>
        )}
      </div>

      {/* DR 详情抽屉 */}
      <Drawer
        open={!!openDr}
        onClose={() => setOpenDr(null)}
        title={openDr?.name}
        subtitle={openDr ? `DR · ${openDr.activeGroups} 活跃 / ${openDr.totalGroups} 总群` : ""}
      >
        {openDr && (
          <DRDetailContent
            dr={openDr}
            aiOn={t.aiOn}
            onOpenGroup={(g) => { setOpenDr(null); setOpenGroup(g); }}
          />
        )}
      </Drawer>

      {/* 群对话抽屉 */}
      <Drawer
        open={!!openGroup}
        onClose={() => setOpenGroup(null)}
        title={openGroup?.name}
        subtitle={
          openGroup
            ? `${(MOCK.DRS.find(d => d.id === openGroup.drId) || {}).name || "—"} 负责`
            : ""
        }
      >
        {openGroup && <GroupConvContent group={openGroup} aiOn={t.aiOn} />}
      </Drawer>

      {/* 群发向导（modal） */}
      <BroadcastModal
        open={broadcastOpen}
        onClose={() => setBroadcastOpen(false)}
        groups={groups}
        prefillGroup={broadcastPrefill}
      />

      {/* Tweaks 面板 */}
      <TweaksPanel>
        <TweakSection label="视图" />
        <TweakRadio label="主视图" value={t.view}
          options={[
            { value: "roster",   label: "DR 视角" },
            { value: "alerts",   label: "告警优先" },
            { value: "overview", label: "全局总览" },
          ]}
          onChange={(v) => setTweak("view", v)} />
        <TweakRadio label="卡片密度" value={t.density}
          options={["compact", "regular"]}
          onChange={(v) => setTweak("density", v)} />

        <TweakSection label="外观" />
        <TweakRadio label="主题" value={t.theme}
          options={[
            { value: "dark",  label: "深色" },
            { value: "light", label: "浅色" },
          ]}
          onChange={(v) => setTweak("theme", v)} />
        <TweakRadio label="强调色" value={t.accent}
          options={[
            { value: "blue",   label: "冷蓝" },
            { value: "purple", label: "靛紫" },
            { value: "green",  label: "翠绿" },
            { value: "amber",  label: "琥珀" },
          ]}
          onChange={(v) => setTweak("accent", v)} />

        <TweakSection label="AI 助手" />
        <TweakToggle label="显示 AI 摘要" value={t.aiOn}
          onChange={(v) => setTweak("aiOn", v)} />
      </TweaksPanel>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
