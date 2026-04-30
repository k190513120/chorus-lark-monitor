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
      setPickedIds(new Set(prefillGroup ? [prefillGroup.id] : []));
    }
  }, [open, prefillGroup]);

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
    if (filter === "vip") return g.tags.some(t => ["VIP", "KA客户", "战略客户", "高价值"].includes(t));
    if (filter === "renewal") return g.tags.some(t => t === "续约期");
    if (filter === "alert") return g.unreplyMinutes > 60;
    return true;
  });

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

const PickGroups = ({ groups, pickedIds, toggle, query, setQuery, filter, setFilter, selectAll, clearAll, errors, total }) => {
  const filters = [
    { id: "all", label: "全部" },
    { id: "vip", label: "VIP / 高价值" },
    { id: "renewal", label: "续约期" },
    { id: "alert", label: "待回复" },
  ];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div style={{
          flex: 1,
          background: "var(--bg-sunk)", borderRadius: 10, padding: "6px 10px",
          display: "flex", alignItems: "center", gap: 8,
        }}>
          <Icon name="search" size={14} color="var(--ink-3)"/>
          <input value={query} onChange={e => setQuery(e.target.value)} placeholder="搜索群或公司"
            style={{ flex: 1, background: "transparent", border: "none", fontSize: 13 }}/>
        </div>
        <Btn variant="ghost" size="sm" onClick={selectAll}>全选</Btn>
        <Btn variant="ghost" size="sm" onClick={clearAll}>清空</Btn>
      </div>

      <div style={{ display: "flex", gap: 4 }}>
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

Object.assign(window, { BroadcastModal });
