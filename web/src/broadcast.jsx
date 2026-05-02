// Broadcast modal — pick groups, compose, schedule, read-status after send

const BroadcastModal = ({ open, onClose, groups, onSent, prefillGroup }) => {
  const [step, setStep] = React.useState(1);
  const [pickedIds, setPickedIds] = React.useState(new Set());
  const [query, setQuery] = React.useState("");
  const [text, setText] = React.useState("");
  const [scheduled, setScheduled] = React.useState(false);
  const [scheduleTime, setScheduleTime] = React.useState("");
  const [sendingProgress, setSendingProgress] = React.useState(0);
  const [sent, setSent] = React.useState(false);
  const [filter, setFilter] = React.useState("all");
  const [errors, setErrors] = React.useState({});
  const [selectedOwners, setSelectedOwners] = React.useState(new Set());
  const [perOwnerLimit, setPerOwnerLimit] = React.useState("");  // "" 表示不限

  React.useEffect(() => {
    if (open) {
      setStep(1);
      setText("");
      setScheduled(false);
      setScheduleTime("");
      setSendingProgress(0);
      setSent(false);
      setErrors({});
      setQuery("");
      setFilter("all");
      setSelectedOwners(new Set());
      setPerOwnerLimit("");
      setPickedIds(new Set(prefillGroup ? [prefillGroup.id] : []));
    }
  }, [open, prefillGroup]);

  // 群主清单 + 每人群数（用于群主多选 UI）—— useMemo 必须在条件 return 之前调用
  const ownerStats = React.useMemo(() => {
    const m = new Map();
    for (const g of (groups || [])) {
      const name = (g.owner && g.owner.name) || "(未知)";
      m.set(name, (m.get(name) || 0) + 1);
    }
    return [...m.entries()].sort((a, b) => b[1] - a[1]);  // 按群数降序
  }, [groups]);

  if (!open) return null;

  const toggle = (id) => {
    setPickedIds(prev => {
      const s = new Set(prev);
      if (s.has(id)) s.delete(id); else s.add(id);
      return s;
    });
  };

  const filteredGroups = groups.filter(g => {
    if (query && !g.name.includes(query) && !g.company.includes(query)) return false;
    if (selectedOwners.size > 0 && !selectedOwners.has((g.owner && g.owner.name) || "(未知)")) return false;
    if (filter === "vip") return g.tags.some(t => ["VIP", "KA客户", "战略客户", "高价值"].includes(t));
    if (filter === "renewal") return g.tags.some(t => t === "续约期");
    if (filter === "alert") return g.unreplyMinutes > 60;
    return true;
  });

  // 应用每群主限额：每个 owner 只取前 N 个（按 filteredGroups 当前顺序）
  const applyPerOwnerLimit = () => {
    const lim = parseInt(perOwnerLimit, 10);
    if (!Number.isFinite(lim) || lim <= 0) {
      // 不限额：直接全选当前过滤结果
      setPickedIds(new Set(filteredGroups.map(g => g.id)));
      return;
    }
    const counts = new Map();
    const newPicked = new Set(pickedIds);
    for (const g of filteredGroups) {
      const o = (g.owner && g.owner.name) || "(未知)";
      const n = counts.get(o) || 0;
      if (n >= lim) continue;
      counts.set(o, n + 1);
      newPicked.add(g.id);
    }
    setPickedIds(newPicked);
  };

  const pickedGroups = groups.filter(g => pickedIds.has(g.id));

  const validate = () => {
    const e = {};
    if (pickedIds.size === 0) e.groups = "请至少选择 1 个群";
    if (!text.trim()) e.text = "消息内容不能为空";
    else if (text.trim().length < 5) e.text = "消息至少 5 个字符";
    if (scheduled && !scheduleTime) e.time = "请选择发送时间";
    setErrors(e);
    return Object.keys(e).length === 0;
  };

  const handleSend = async () => {
    if (!validate()) return;
    setStep(3);
    setSendingProgress(0);
    setErrors({});

    const targets = pickedGroups
      .filter(g => g.chat_id)  // synthetic g.id 不能用于发消息
      .map(g => ({ chat_id: g.chat_id, chat_name: g.name }));
    if (targets.length === 0) {
      setErrors({ groups: "选中的群没有 chat_id，无法发送" });
      return;
    }

    let batchId;
    try {
      const res = await fetch("/api/bulk-send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chat_targets: targets, text, title: text.slice(0, 30) }),
      });
      if (!res.ok) {
        const err = await res.text();
        setErrors({ send: `发送启动失败: ${err}` });
        return;
      }
      const data = await res.json();
      batchId = data.batch_id;
    } catch (err) {
      setErrors({ send: `网络错误: ${err.message}` });
      return;
    }

    // 订阅 WebSocket 进度推送
    const wsProto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${wsProto}://${location.host}/ws/bulk-progress/${batchId}`);
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (typeof msg.sent === "number") {
          setSendingProgress(msg.sent + msg.failed);
        }
        if (msg.type === "done" || msg.status === "done") {
          setSent(true);
          ws.close();
        }
      } catch {
        // ignore non-JSON pings
      }
    };
    ws.onerror = () => setErrors(prev => ({ ...prev, ws: "进度连接断开，结果以 Base 为准" }));
  };

  return (
    <div style={{
      position: "fixed", inset: 0, zIndex: 1000,
      background: "oklch(0.2 0.02 60 / 0.45)",
      display: "grid", placeItems: "center",
      padding: 24,
      animation: "fadeIn 160ms",
    }} onClick={onClose}>
      <div
        onClick={e => e.stopPropagation()}
        style={{
          width: "min(820px, 100%)", maxHeight: "90vh",
          background: "var(--bg-elev)", borderRadius: "var(--r-xl)",
          boxShadow: "var(--sh-3)",
          overflow: "hidden",
          display: "flex", flexDirection: "column",
          animation: "slideUp 200ms cubic-bezier(.2,.9,.25,1)",
        }}>
        {/* header */}
        <div style={{
          padding: "16px 22px", display: "flex", alignItems: "center", gap: 14,
          borderBottom: "1px solid var(--line)",
        }}>
          <div style={{
            width: 34, height: 34, borderRadius: 10,
            background: "var(--accent-soft)", color: "var(--accent-ink)",
            display: "grid", placeItems: "center",
          }}>
            <Icon name="send" size={16}/>
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 15, fontWeight: 600 }}>批量发送群消息</div>
            <div style={{ fontSize: 12, color: "var(--ink-3)" }}>
              {sent ? "已发送完成" : `第 ${step} / 2 步 · ${step === 1 ? "选择目标群" : "撰写消息内容"}`}
            </div>
          </div>
          <button onClick={onClose} style={{
            width: 30, height: 30, borderRadius: 8, color: "var(--ink-3)",
            display: "grid", placeItems: "center",
          }}
            onMouseEnter={e => e.currentTarget.style.background = "var(--bg-sunk)"}
            onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
            <Icon name="x" size={16}/>
          </button>
        </div>

        {/* step indicator */}
        {!sent && (
          <div style={{ padding: "10px 22px", background: "var(--bg-sunk)", display: "flex", gap: 10, alignItems: "center" }}>
            <StepDot n={1} label="选群" active={step === 1} done={step > 1}/>
            <div style={{ flex: 1, height: 1, background: "var(--line-strong)" }}/>
            <StepDot n={2} label="写消息" active={step === 2} done={step > 2}/>
            <div style={{ flex: 1, height: 1, background: "var(--line-strong)" }}/>
            <StepDot n={3} label="发送 & 回执" active={step === 3} done={false}/>
          </div>
        )}

        {/* body */}
        <div className="scroll" style={{ flex: 1, overflowY: "auto", padding: 22 }}>
          {step === 1 && (
            <PickGroups
              groups={filteredGroups}
              pickedIds={pickedIds}
              toggle={toggle}
              query={query} setQuery={setQuery}
              filter={filter} setFilter={setFilter}
              ownerStats={ownerStats}
              selectedOwners={selectedOwners}
              toggleOwner={(name) => setSelectedOwners(prev => {
                const s = new Set(prev);
                if (s.has(name)) s.delete(name); else s.add(name);
                return s;
              })}
              clearOwners={() => setSelectedOwners(new Set())}
              perOwnerLimit={perOwnerLimit}
              setPerOwnerLimit={setPerOwnerLimit}
              applyPerOwnerLimit={applyPerOwnerLimit}
              selectAll={() => setPickedIds(new Set(filteredGroups.map(g => g.id)))}
              clearAll={() => setPickedIds(new Set())}
              errors={errors}
              total={groups.length}
            />
          )}
          {step === 2 && (
            <ComposeMessage
              text={text} setText={setText}
              scheduled={scheduled} setScheduled={setScheduled}
              scheduleTime={scheduleTime} setScheduleTime={setScheduleTime}
              pickedGroups={pickedGroups}
              errors={errors}
            />
          )}
          {step === 3 && (
            <SendProgress
              pickedGroups={pickedGroups}
              progress={sendingProgress}
              text={text}
              sent={sent}
              scheduled={scheduled}
              scheduleTime={scheduleTime}
            />
          )}
        </div>

        {/* footer */}
        <div style={{
          padding: "12px 22px", borderTop: "1px solid var(--line)",
          display: "flex", alignItems: "center", gap: 10,
          background: "var(--bg-elev)",
        }}>
          <div style={{ flex: 1, fontSize: 12, color: "var(--ink-3)" }}>
            {step === 1 && (
              <>已选 <span className="num" style={{ color: "var(--ink)", fontWeight: 600 }}>{pickedIds.size}</span> 个群，将覆盖约 <span className="num">{pickedGroups.reduce((a,g)=>a+g.memberCount,0)}</span> 人</>
            )}
            {step === 2 && (
              <>字数 <span className="num" style={{ color: text.length > 300 ? "var(--accent-ink)" : "var(--ink)" }}>{text.length}</span> / 500</>
            )}
            {step === 3 && !sent && <>发送中 · 请勿关闭窗口…</>}
            {sent && <>✓ 已成功发送到 {pickedIds.size} 个群</>}
          </div>

          {step === 1 && <>
            <Btn variant="ghost" onClick={onClose}>取消</Btn>
            <Btn variant="primary" onClick={() => {
              if (pickedIds.size === 0) { setErrors({ groups: "请至少选择 1 个群" }); return; }
              setErrors({}); setStep(2);
            }} iconRight={<Icon name="chevronRight" size={12}/>}>
              下一步
            </Btn>
          </>}
          {step === 2 && <>
            <Btn variant="ghost" onClick={() => setStep(1)}>上一步</Btn>
            <Btn variant="primary" onClick={handleSend}
              icon={<Icon name={scheduled ? "clock" : "send"} size={12}/>}>
              {scheduled ? "定时发送" : `立即发送到 ${pickedIds.size} 个群`}
            </Btn>
          </>}
          {step === 3 && <>
            {sent && <Btn variant="outline" onClick={() => { onSent && onSent(); onClose(); }}>关闭</Btn>}
          </>}
        </div>
      </div>

      <style>{`
        @keyframes fadeIn { from { opacity: 0 } to { opacity: 1 } }
        @keyframes slideUp { from { transform: translateY(12px); opacity: 0 } to { transform: translateY(0); opacity: 1 } }
      `}</style>
    </div>
  );
};

const StepDot = ({ n, label, active, done }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
    <div style={{
      width: 22, height: 22, borderRadius: 999,
      background: active ? "var(--accent)" : done ? "var(--sage)" : "var(--bg-elev)",
      color: (active || done) ? "white" : "var(--ink-3)",
      display: "grid", placeItems: "center",
      fontSize: 11, fontWeight: 600,
      border: `1px solid ${active || done ? "transparent" : "var(--line-strong)"}`,
    }} className="num">
      {done ? <Icon name="check" size={12} strokeWidth={2.5}/> : n}
    </div>
    <span style={{ fontSize: 12, color: active ? "var(--ink)" : "var(--ink-3)", fontWeight: active ? 500 : 400 }}>
      {label}
    </span>
  </div>
);

const PickGroups = ({
  groups, pickedIds, toggle,
  query, setQuery,
  filter, setFilter,
  ownerStats, selectedOwners, toggleOwner, clearOwners,
  perOwnerLimit, setPerOwnerLimit, applyPerOwnerLimit,
  selectAll, clearAll, errors, total,
}) => {
  const [showAllOwners, setShowAllOwners] = React.useState(false);
  const filters = [
    { id: "all", label: "全部" },
    { id: "vip", label: "VIP / 高价值" },
    { id: "renewal", label: "续约期" },
    { id: "alert", label: "待回复" },
  ];
  // 群主选择默认显示前 8 个，"更多"展开全部
  const visibleOwners = showAllOwners ? ownerStats : ownerStats.slice(0, 8);

  // 过滤后按群主分组统计
  const ownerCountInFiltered = React.useMemo(() => {
    const m = new Map();
    for (const g of groups) {
      const o = (g.owner && g.owner.name) || "(未知)";
      m.set(o, (m.get(o) || 0) + 1);
    }
    return m;
  }, [groups]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div style={{
          flex: 1,
          background: "var(--bg-sunk)", borderRadius: 10, padding: "6px 10px",
          display: "flex", alignItems: "center", gap: 8,
        }}>
          <Icon name="search" size={14} color="var(--ink-3)"/>
          <input value={query} onChange={e => setQuery(e.target.value)} placeholder="搜索群或公司（如：增购）"
            style={{ flex: 1, background: "transparent", border: "none", fontSize: 13 }}/>
        </div>
        <Btn variant="ghost" size="sm" onClick={selectAll}>全选过滤后</Btn>
        <Btn variant="ghost" size="sm" onClick={clearAll}>清空</Btn>
      </div>

      <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
        {filters.map(f => (
          <button key={f.id} onClick={() => setFilter(f.id)}
            style={{
              padding: "4px 10px", borderRadius: 999, fontSize: 12,
              background: filter === f.id ? "var(--ink)" : "transparent",
              color: filter === f.id ? "var(--bg-elev)" : "var(--ink-3)",
              border: `1px solid ${filter === f.id ? "var(--ink)" : "var(--line)"}`,
            }}>{f.label}</button>
        ))}
      </div>

      {/* 群主多选 */}
      <div style={{ background: "var(--bg-sunk)", borderRadius: 10, padding: "10px 12px", display: "flex", flexDirection: "column", gap: 8 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
          <div style={{ fontSize: 12, color: "var(--ink-3)" }}>
            按群主筛选 {selectedOwners.size > 0 && <span style={{ color: "var(--ink)" }}>· 已选 {selectedOwners.size} 人</span>}
          </div>
          {selectedOwners.size > 0 && (
            <button onClick={clearOwners} style={{ fontSize: 11, color: "var(--ink-3)" }}>清除</button>
          )}
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
          {visibleOwners.map(([name, count]) => {
            const on = selectedOwners.has(name);
            return (
              <button key={name} onClick={() => toggleOwner(name)}
                style={{
                  padding: "4px 10px", borderRadius: 999, fontSize: 12,
                  background: on ? "var(--ink)" : "var(--bg-elev)",
                  color: on ? "var(--bg-elev)" : "var(--ink)",
                  border: `1px solid ${on ? "var(--ink)" : "var(--line)"}`,
                }}>
                {name} <span style={{ opacity: 0.6 }}>{count}</span>
              </button>
            );
          })}
          {!showAllOwners && ownerStats.length > 8 && (
            <button onClick={() => setShowAllOwners(true)} style={{ padding: "4px 10px", fontSize: 12, color: "var(--ink-3)" }}>
              +{ownerStats.length - 8} 更多 ▼
            </button>
          )}
        </div>

        {/* 每群主限额 */}
        <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 4 }}>
          <span style={{ fontSize: 12, color: "var(--ink-3)" }}>每群主最多</span>
          <input
            type="number" min="1" placeholder="不限"
            value={perOwnerLimit}
            onChange={e => setPerOwnerLimit(e.target.value)}
            style={{
              width: 64, padding: "4px 8px", borderRadius: 6,
              border: "1px solid var(--line)", fontSize: 12, textAlign: "center",
              background: "var(--bg-elev)",
            }}
          />
          <span style={{ fontSize: 12, color: "var(--ink-3)" }}>个</span>
          <Btn variant="primary" size="sm" onClick={applyPerOwnerLimit}>
            按规则勾选 ({groups.length} 候选)
          </Btn>
        </div>

        {/* 过滤后每群主多少 */}
        {groups.length > 0 && groups.length < total && (
          <div style={{ fontSize: 11.5, color: "var(--ink-3)", display: "flex", flexWrap: "wrap", gap: 8 }}>
            过滤后:
            {[...ownerCountInFiltered.entries()]
              .sort((a, b) => b[1] - a[1])
              .slice(0, 6)
              .map(([n, c]) => (
                <span key={n}><b style={{ color: "var(--ink)" }}>{n}</b> {c} 个</span>
              ))}
            {ownerCountInFiltered.size > 6 && <span>...</span>}
          </div>
        )}
      </div>

      {errors.groups && (
        <div style={{ fontSize: 12, color: "oklch(0.5 0.15 20)", padding: "4px 2px" }}>
          ⚠ {errors.groups}
        </div>
      )}

      <div style={{
        border: "1px solid var(--line)", borderRadius: 12,
        maxHeight: 380, overflowY: "auto",
      }} className="scroll">
        {groups.map((g, i) => {
          const picked = pickedIds.has(g.id);
          return (
            <button key={g.id} onClick={() => toggle(g.id)}
              style={{
                width: "100%", textAlign: "left",
                padding: "10px 12px",
                display: "flex", alignItems: "center", gap: 10,
                background: picked ? "var(--accent-soft)" : "transparent",
                borderBottom: i < groups.length - 1 ? "1px solid var(--line)" : "none",
              }}>
              <div style={{
                width: 18, height: 18, borderRadius: 5,
                background: picked ? "var(--accent)" : "transparent",
                border: `1.5px solid ${picked ? "var(--accent)" : "var(--line-strong)"}`,
                display: "grid", placeItems: "center", flexShrink: 0,
                transition: "all 120ms",
              }}>
                {picked && <Icon name="check" size={11} color="white" strokeWidth={3}/>}
              </div>
              <Avatar avatar={g.avatar} size={30}/>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 13, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {g.name}
                </div>
                <div style={{ fontSize: 11.5, color: "var(--ink-3)" }}>
                  {g.memberCount} 人 · {g.owner.name}
                </div>
              </div>
              <div style={{ display: "flex", gap: 4 }}>
                {g.tags.slice(0, 1).map(t => <Pill key={t} tone="outline">{t}</Pill>)}
              </div>
            </button>
          );
        })}
        {groups.length === 0 && (
          <div style={{ padding: 32, textAlign: "center", color: "var(--ink-4)", fontSize: 12 }}>
            没有匹配的群
          </div>
        )}
      </div>
    </div>
  );
};

const TEMPLATES = [
  { label: "节日问候", text: "各位老朋友：\n中秋将至，Chorus 团队祝您和家人团圆安康。近期上线了全新报表导出功能，欢迎随时体验，有任何问题直接群内@我们就好 🌕" },
  { label: "新版本通知", text: "Hi 各位：\n我们在本周二（23 号）发布了新版本，本次更新包含【群聊监控看板】【消息批量发送】两项您反馈过的功能。更新日志见附件，欢迎联系您的客户经理预约培训。" },
  { label: "会议邀请", text: "Hi 各位伙伴：\n想和大家对齐下季度合作节奏，拟于周四（25 号）下午 15:00 线上开一个 30 分钟的对接会，合适的请在群里扣 1～～" },
];

const ComposeMessage = ({ text, setText, scheduled, setScheduled, scheduleTime, setScheduleTime, pickedGroups, errors }) => {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      {/* selected preview strip */}
      <div style={{
        background: "var(--bg-sunk)", borderRadius: 12, padding: "10px 12px",
        display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap",
      }}>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }}>发送至</span>
        <div style={{ display: "flex", gap: -6 }}>
          {pickedGroups.slice(0, 6).map((g, i) => (
            <div key={g.id} style={{ marginLeft: i === 0 ? 0 : -8 }}>
              <Avatar avatar={g.avatar} size={24} ring/>
            </div>
          ))}
        </div>
        <span className="num" style={{ fontSize: 12, fontWeight: 600 }}>
          {pickedGroups.length} 个群
        </span>
        <span style={{ fontSize: 11, color: "var(--ink-4)" }}>
          · 覆盖约 {pickedGroups.reduce((a,g)=>a+g.memberCount,0)} 人
        </span>
      </div>

      {/* templates */}
      <div>
        <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>
          快捷模板
        </div>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          {TEMPLATES.map(t => (
            <button key={t.label} onClick={() => setText(t.text)}
              style={{
                padding: "6px 12px", borderRadius: 999, fontSize: 12,
                background: "var(--bg-sunk)", color: "var(--ink-2)",
                border: "1px solid var(--line)",
              }}
              onMouseEnter={e => e.currentTarget.style.background = "var(--accent-soft)"}
              onMouseLeave={e => e.currentTarget.style.background = "var(--bg-sunk)"}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* textarea */}
      <div>
        <textarea
          value={text} onChange={e => setText(e.target.value.slice(0, 500))}
          placeholder="输入消息内容…支持使用 {群名} {负责人} {公司} 等变量自动替换"
          style={{
            width: "100%", minHeight: 160, resize: "vertical",
            padding: 14,
            background: "var(--bg-elev)",
            border: `1.5px solid ${errors.text ? "oklch(0.7 0.14 20)" : "var(--line-strong)"}`,
            borderRadius: 12,
            fontSize: 13.5, lineHeight: 1.6,
            color: "var(--ink)",
            fontFamily: "var(--font-cn)",
          }}
        />
        {errors.text && (
          <div style={{ fontSize: 12, color: "oklch(0.5 0.15 20)", marginTop: 4 }}>⚠ {errors.text}</div>
        )}
      </div>

      {/* schedule */}
      <div style={{
        border: "1px solid var(--line)", borderRadius: 12, padding: "10px 14px",
        display: "flex", alignItems: "center", gap: 12,
      }}>
        <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontSize: 13 }}>
          <div style={{
            width: 34, height: 20, borderRadius: 999,
            background: scheduled ? "var(--accent)" : "var(--line-strong)",
            position: "relative", transition: "all 160ms",
          }} onClick={() => setScheduled(!scheduled)}>
            <div style={{
              position: "absolute", top: 2, left: scheduled ? 16 : 2, width: 16, height: 16,
              borderRadius: 999, background: "white", transition: "all 160ms",
              boxShadow: "0 1px 3px oklch(0.2 0.02 60 / 0.2)",
            }}/>
          </div>
          <span>定时发送</span>
        </label>
        {scheduled && (
          <input type="datetime-local" value={scheduleTime}
            onChange={e => setScheduleTime(e.target.value)}
            style={{
              flex: 1, padding: "6px 10px", fontSize: 13,
              background: "var(--bg-sunk)", border: "1px solid var(--line)",
              borderRadius: 8, color: "var(--ink)",
            }}
          />
        )}
        {scheduled && errors.time && (
          <span style={{ fontSize: 11, color: "oklch(0.5 0.15 20)" }}>⚠ {errors.time}</span>
        )}
      </div>
    </div>
  );
};

const SendProgress = ({ pickedGroups, progress, text, sent, scheduled, scheduleTime }) => {
  const pct = pickedGroups.length > 0 ? progress / pickedGroups.length : 0;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {!sent ? (
        <div style={{
          textAlign: "center", padding: "24px 0",
        }}>
          <div style={{
            width: 64, height: 64, borderRadius: 999,
            background: "var(--accent-soft)", color: "var(--accent-ink)",
            margin: "0 auto 12px",
            display: "grid", placeItems: "center",
            animation: "pulse 1.4s infinite",
          }}>
            <Icon name="send" size={24}/>
          </div>
          <div style={{ fontSize: 15, fontWeight: 500, marginBottom: 4 }}>正在发送…</div>
          <div style={{ fontSize: 12, color: "var(--ink-3)" }}>
            <span className="num">{progress}</span> / {pickedGroups.length} 个群已完成
          </div>
          <div style={{ maxWidth: 320, margin: "14px auto 0" }}>
            <Progress value={progress} total={pickedGroups.length} height={8}/>
          </div>
        </div>
      ) : (
        <div style={{ textAlign: "center", padding: "16px 0 8px" }}>
          <div style={{
            width: 56, height: 56, borderRadius: 999,
            background: "var(--sage-soft)", color: "oklch(0.35 0.08 155)",
            margin: "0 auto 10px",
            display: "grid", placeItems: "center",
          }}>
            <Icon name="check" size={24} strokeWidth={2.5}/>
          </div>
          <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 2 }}>
            {scheduled ? "定时任务已创建" : "发送完成"}
          </div>
          <div style={{ fontSize: 12, color: "var(--ink-3)" }}>
            {scheduled
              ? `将于 ${scheduleTime.replace("T", " ")} 发送至 ${pickedGroups.length} 个群`
              : `已发送至 ${pickedGroups.length} 个群 · 共 ${pickedGroups.reduce((a,g)=>a+g.memberCount,0)} 人`
            }
          </div>
        </div>
      )}

      {/* per-group results list */}
      <div style={{
        border: "1px solid var(--line)", borderRadius: 12,
        maxHeight: 280, overflowY: "auto",
      }} className="scroll">
        <div style={{
          padding: "8px 14px", fontSize: 11, color: "var(--ink-3)",
          background: "var(--bg-sunk)",
          display: "grid", gridTemplateColumns: "2fr 1fr 1fr 0.7fr", gap: 8,
          fontWeight: 500, textTransform: "uppercase", letterSpacing: 0.5,
          position: "sticky", top: 0, borderBottom: "1px solid var(--line)",
        }}>
          <span>群名</span>
          <span>状态</span>
          <span>阅读</span>
          <span style={{ textAlign: "right" }}>时间</span>
        </div>
        {pickedGroups.map((g, i) => {
          const done = i < progress;
          const readRatio = done ? (0.35 + Math.random() * 0.55) : 0;
          const readCount = done ? Math.floor(g.memberCount * readRatio) : 0;
          return (
            <div key={g.id} style={{
              padding: "10px 14px",
              display: "grid", gridTemplateColumns: "2fr 1fr 1fr 0.7fr", gap: 8,
              alignItems: "center", fontSize: 12.5,
              borderBottom: i < pickedGroups.length - 1 ? "1px solid var(--line)" : "none",
              opacity: done ? 1 : 0.5,
              transition: "opacity 200ms",
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 0 }}>
                <Avatar avatar={g.avatar} size={22}/>
                <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{g.name}</span>
              </div>
              <div>
                {!done ? (
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 6, color: "var(--ink-3)", fontSize: 11.5 }}>
                    <span style={{
                      width: 10, height: 10, borderRadius: 999,
                      border: "1.5px solid var(--line-strong)",
                      borderTopColor: "var(--accent)",
                      animation: "spin 800ms linear infinite",
                    }}/>
                    等待中
                  </span>
                ) : scheduled ? (
                  <Pill tone="sky">已排期</Pill>
                ) : (
                  <Pill tone="sage" icon={<Icon name="check" size={10} strokeWidth={2.5}/>}>已送达</Pill>
                )}
              </div>
              <div>
                {done && !scheduled ? (
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <Progress value={readCount} total={g.memberCount} height={5}/>
                    <span className="num" style={{ fontSize: 10.5, color: "var(--ink-3)", minWidth: 36 }}>
                      {readCount}/{g.memberCount}
                    </span>
                  </div>
                ) : (
                  <span style={{ color: "var(--ink-4)", fontSize: 11 }}>—</span>
                )}
              </div>
              <span className="num" style={{ fontSize: 10.5, color: "var(--ink-4)", textAlign: "right" }}>
                {done ? "刚刚" : "—"}
              </span>
            </div>
          );
        })}
      </div>

      <style>{`
        @keyframes pulse { 0%, 100% { transform: scale(1) } 50% { transform: scale(1.05) } }
        @keyframes spin { to { transform: rotate(360deg) } }
      `}</style>
    </div>
  );
};

// 群发消息 tab 页：列出历史群发任务，提供"新建"入口（点开后打开 BroadcastModal 向导）
const BroadcastView = ({ broadcasts, groupCount, onNew }) => {
  const formatPct = (v) => `${(v * 100).toFixed(1)}%`;
  const sorted = (broadcasts || []).slice().sort((a, b) => (b.sentAtMs || 0) - (a.sentAtMs || 0));

  const [refreshing, setRefreshing] = React.useState(false);
  const [refreshMsg, setRefreshMsg] = React.useState("");
  const [analysisOpen, setAnalysisOpen] = React.useState(false);

  const handleRefresh = async () => {
    if (refreshing) return;
    setRefreshing(true);
    setRefreshMsg("已触发，约 30 秒...");
    try {
      const r = await fetch("/api/bulk-send/refresh?max_age_days=7", { method: "POST" });
      const j = await r.json();
      if (!j.ok && j.running) {
        setRefreshMsg("已经在跑，等当前完成");
      }
      // 轮询状态
      let attempts = 0;
      const poll = setInterval(async () => {
        attempts += 1;
        try {
          const sr = await fetch("/api/bulk-send/refresh/status").then(x => x.json());
          if (!sr.running && sr.ended_at) {
            clearInterval(poll);
            const dur = sr.ended_at - sr.started_at;
            setRefreshMsg(`✓ 刷新完成（耗时 ${dur}s），即将重载...`);
            setTimeout(() => window.location.reload(), 1200);
          } else if (attempts > 60) {
            clearInterval(poll);
            setRefreshing(false);
            setRefreshMsg("超时，请手动刷新页面");
          }
        } catch (e) { /* keep polling */ }
      }, 2000);
    } catch (e) {
      setRefreshing(false);
      setRefreshMsg("失败：" + e.message);
    }
  };

  return (
    <div style={{ padding: "22px 28px", display: "flex", flexDirection: "column", gap: 18, maxWidth: 1200, margin: "0 auto" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <div style={{ fontSize: 22, fontWeight: 600 }}>群发消息</div>
          <div style={{ fontSize: 13, color: "var(--ink-3)", marginTop: 4 }}>
            可发送范围 {groupCount} 个群 · 已记录 {sorted.length} 次群发任务
            {refreshMsg && <span style={{ marginLeft: 12, color: "var(--accent)" }}>{refreshMsg}</span>}
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <Btn variant="outline" size="md" onClick={() => setAnalysisOpen(true)}>
            数据分析报告
          </Btn>
          <Btn variant="outline" size="md" onClick={handleRefresh} disabled={refreshing}>
            {refreshing ? "刷新中..." : "刷新统计"}
          </Btn>
          <Btn variant="primary" size="md" icon={<Icon name="send" size={14}/>} onClick={onNew}>
            新建群发任务
          </Btn>
        </div>
      </div>

      <BroadcastAnalysisModal open={analysisOpen} onClose={() => setAnalysisOpen(false)} />


      <Card style={{ padding: 0, overflow: "hidden" }}>
        <div style={{ padding: "14px 18px", borderBottom: "1px solid var(--line)" }}>
          <div style={{ fontSize: 14, fontWeight: 500 }}>历史任务</div>
          <div style={{ fontSize: 12, color: "var(--ink-3)", marginTop: 2 }}>
            已读率 / 回复率排除内部 tenant；statistics 每天 20:00 自动滚动刷新
          </div>
        </div>
        {sorted.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "var(--ink-3)" }}>
            还没有群发记录。点击右上角"新建群发任务"开始第一条。
          </div>
        ) : (
          <div>
            <div style={{
              display: "grid",
              gridTemplateColumns: "1.4fr 110px 90px 110px 130px 110px",
              gap: 12,
              padding: "10px 18px",
              fontSize: 11.5,
              color: "var(--ink-3)",
              background: "var(--bg-sunk)",
            }}>
              <div>任务标题 / 时间</div>
              <div style={{ textAlign: "right" }}>群数</div>
              <div style={{ textAlign: "right" }}>受众</div>
              <div style={{ textAlign: "right" }}>已读率</div>
              <div style={{ textAlign: "right" }}>回复率</div>
              <div style={{ textAlign: "right" }}>采集时间</div>
            </div>
            {sorted.map((b) => {
              const heatColor = (rate) => {
                if (rate >= 0.6) return "oklch(0.55 0.13 155)";
                if (rate >= 0.3) return "oklch(0.55 0.13 75)";
                return "oklch(0.55 0.13 20)";
              };
              return (
                <div key={b.batchId} style={{
                  display: "grid",
                  gridTemplateColumns: "1.4fr 110px 90px 110px 130px 110px",
                  gap: 12,
                  padding: "12px 18px",
                  fontSize: 13,
                  alignItems: "center",
                  borderTop: "1px solid var(--line)",
                }}>
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontWeight: 500, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }} title={b.text}>
                      {b.title}
                    </div>
                    <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginTop: 2 }}>
                      {b.sentAtText || "—"}
                    </div>
                  </div>
                  <div style={{ textAlign: "right", color: b.failureCount ? "oklch(0.55 0.12 20)" : "var(--ink)" }} className="num">
                    {b.successCount}{b.failureCount ? ` / -${b.failureCount}` : ""}
                  </div>
                  <div style={{ textAlign: "right" }} className="num">{b.targetAudience}</div>
                  <div style={{ textAlign: "right", color: heatColor(b.avgReadRate), fontWeight: 500 }} className="num">
                    {formatPct(b.avgReadRate)}
                    <span style={{ color: "var(--ink-3)", fontWeight: 400, fontSize: 11 }}> ({b.readCount})</span>
                  </div>
                  <div style={{ textAlign: "right", color: heatColor(b.avgReplyRate), fontWeight: 500 }} className="num">
                    {formatPct(b.avgReplyRate)}
                    <span style={{ color: "var(--ink-3)", fontWeight: 400, fontSize: 11 }}> ({b.replyUniqueSenders}人/{b.replyCount}条)</span>
                  </div>
                  <div style={{ textAlign: "right", fontSize: 11.5, color: "var(--ink-3)" }}>
                    {b.collectedAtText ? b.collectedAtText.slice(5, 16) : "—"}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Card>
    </div>
  );
};

// 数据分析报告 modal —— 拉 /api/broadcast/analysis 显示 KPI + 排行 + 沉默群清单
const BroadcastAnalysisModal = ({ open, onClose }) => {
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState("");

  React.useEffect(() => {
    if (!open) return;
    setLoading(true);
    setErr("");
    setData(null);
    fetch("/api/broadcast/analysis")
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(e => { setErr(String(e)); setLoading(false); });
  }, [open]);

  if (!open) return null;

  const formatPct = (v) => v == null ? "—" : `${(v * 100).toFixed(1)}%`;
  const heatColor = (rate) => {
    if (rate >= 0.6) return "oklch(0.55 0.13 155)";
    if (rate >= 0.3) return "oklch(0.55 0.13 75)";
    return "oklch(0.55 0.13 20)";
  };

  return (
    <div style={{
      position: "fixed", inset: 0, zIndex: 1000,
      background: "oklch(0.2 0.02 60 / 0.45)",
      display: "grid", placeItems: "center", padding: 24,
    }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{
        width: "min(960px, 100%)", maxHeight: "90vh",
        background: "var(--bg-elev)", borderRadius: "var(--r-xl)",
        boxShadow: "var(--sh-3)", overflow: "hidden",
        display: "flex", flexDirection: "column",
      }}>
        <div style={{
          padding: "16px 22px", display: "flex", alignItems: "center", gap: 14,
          borderBottom: "1px solid var(--line)",
        }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 15, fontWeight: 600 }}>数据分析报告</div>
            <div style={{ fontSize: 12, color: "var(--ink-3)" }}>
              基于「群发任务记录」表的实时聚合
            </div>
          </div>
          <button onClick={onClose} style={{ width: 30, height: 30, borderRadius: 8, color: "var(--ink-3)", display: "grid", placeItems: "center" }}>
            <Icon name="x" size={16}/>
          </button>
        </div>
        <div className="scroll" style={{ flex: 1, overflowY: "auto", padding: 22 }}>
          {loading && <div style={{ textAlign: "center", color: "var(--ink-3)", padding: 40 }}>分析中...</div>}
          {err && <div style={{ color: "oklch(0.5 0.15 20)", padding: 20 }}>失败：{err}</div>}
          {data && (
            <div style={{ display: "flex", flexDirection: "column", gap: 22 }}>
              {/* KPI 块 */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                <KpiCell label="任务批次" value={data.kpis.totalBatches} />
                <KpiCell label="覆盖目标受众" value={data.kpis.totalAudience} />
                <KpiCell label="平均已读率" value={formatPct(data.kpis.avgReadRate)} color={heatColor(data.kpis.avgReadRate)} />
                <KpiCell label="平均回复率" value={formatPct(data.kpis.avgReplyRate)} color={heatColor(data.kpis.avgReplyRate)} />
              </div>

              {/* 任务排行 */}
              {data.topTasks.length > 0 && (
                <Section title="🏆 已读率最高的任务">
                  <TaskList tasks={data.topTasks} formatPct={formatPct} heatColor={heatColor} />
                </Section>
              )}
              {data.bottomTasks.length > 0 && (
                <Section title="⚠️ 已读率最低的任务">
                  <TaskList tasks={data.bottomTasks} formatPct={formatPct} heatColor={heatColor} />
                </Section>
              )}

              {/* 高质量群 */}
              {data.highQualityChats.length > 0 && (
                <Section title={`💎 高互动群（共 ${data.highQualityChats.length} 个，平均已读率 ≥50%）`}>
                  <ChatList chats={data.highQualityChats} formatPct={formatPct} heatColor={heatColor} />
                </Section>
              )}

              {/* 沉默群 */}
              {data.silentChats.length > 0 && (
                <Section title={`🔕 沉默群（被群发 ≥2 次但 0 已读，建议清理 ${data.silentChats.length} 个）`}>
                  <ChatList chats={data.silentChats} formatPct={formatPct} heatColor={heatColor} silent />
                </Section>
              )}

              <div style={{ fontSize: 11, color: "var(--ink-3)", textAlign: "right" }}>
                生成时间：{new Date(data.generatedAt * 1000).toLocaleString()}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const KpiCell = ({ label, value, color }) => (
  <div style={{ background: "var(--bg-sunk)", borderRadius: 10, padding: 14 }}>
    <div style={{ fontSize: 11.5, color: "var(--ink-3)" }}>{label}</div>
    <div style={{ fontSize: 22, fontWeight: 600, marginTop: 4, color: color || "var(--ink)" }} className="num">{value}</div>
  </div>
);

const Section = ({ title, children }) => (
  <div>
    <div style={{ fontSize: 13, fontWeight: 500, color: "var(--ink-2)", marginBottom: 8 }}>{title}</div>
    {children}
  </div>
);

const TaskList = ({ tasks, formatPct, heatColor }) => (
  <div style={{ border: "1px solid var(--line)", borderRadius: 10, overflow: "hidden" }}>
    {tasks.map((t, i) => (
      <div key={t.batchId} style={{
        display: "grid", gridTemplateColumns: "1.5fr 90px 110px 110px",
        padding: "10px 14px", fontSize: 12.5, alignItems: "center",
        borderTop: i === 0 ? "none" : "1px solid var(--line)",
      }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontWeight: 500, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{t.title}</div>
          <div style={{ fontSize: 11, color: "var(--ink-3)" }}>{t.sentAtText}</div>
        </div>
        <div style={{ textAlign: "right" }} className="num">{t.targetAudience} 人</div>
        <div style={{ textAlign: "right", color: heatColor(t.avgReadRate), fontWeight: 500 }} className="num">已读 {formatPct(t.avgReadRate)}</div>
        <div style={{ textAlign: "right", color: heatColor(t.avgReplyRate), fontWeight: 500 }} className="num">回复 {formatPct(t.avgReplyRate)}</div>
      </div>
    ))}
  </div>
);

const ChatList = ({ chats, formatPct, heatColor, silent }) => (
  <div style={{ border: "1px solid var(--line)", borderRadius: 10, overflow: "hidden", maxHeight: 320, overflowY: "auto" }}>
    {chats.map((c, i) => (
      <div key={c.chatId} style={{
        display: "grid", gridTemplateColumns: "1.5fr 80px 110px 110px",
        padding: "10px 14px", fontSize: 12.5, alignItems: "center",
        borderTop: i === 0 ? "none" : "1px solid var(--line)",
      }}>
        <div style={{ minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }} title={c.chatName || c.chatId}>
          {c.chatName || c.chatId}
        </div>
        <div style={{ textAlign: "right", color: "var(--ink-3)" }}>{c.broadcastCount} 次</div>
        <div style={{ textAlign: "right", color: silent ? "var(--ink-3)" : heatColor(c.avgReadRate), fontWeight: 500 }} className="num">
          已读 {silent ? "0%" : formatPct(c.avgReadRate)}
        </div>
        <div style={{ textAlign: "right", color: silent ? "var(--ink-3)" : heatColor(c.avgReplyRate) }} className="num">
          回复 {formatPct(c.avgReplyRate)}
        </div>
      </div>
    ))}
  </div>
);

Object.assign(window, { BroadcastModal, BroadcastView, BroadcastAnalysisModal });
