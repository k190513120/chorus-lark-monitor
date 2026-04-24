// Detail pane — AI summary + messages + members + read-status

const SummaryCard = ({ group }) => {
  const [loading, setLoading] = React.useState(true);
  const [summary, setSummary] = React.useState(null);

  React.useEffect(() => {
    setLoading(true);
    setSummary(null);
    const t = setTimeout(() => {
      setSummary(AppData.buildSummary(group));
      setLoading(false);
    }, 650);
    return () => clearTimeout(t);
  }, [group.id]);

  return (
    <Card style={{
      padding: 0, overflow: "hidden",
      background: "linear-gradient(180deg, oklch(0.98 0.015 45) 0%, var(--bg-elev) 70%)",
      border: "1px solid oklch(0.90 0.03 45)",
    }}>
      <div style={{ padding: "16px 18px 4px", display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{
          width: 28, height: 28, borderRadius: 9,
          background: "var(--accent)", color: "white",
          display: "grid", placeItems: "center",
        }}>
          <Icon name="spark" size={14} strokeWidth={2.2}/>
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 13.5, fontWeight: 600 }}>AI 群聊摘要</div>
          <div style={{ fontSize: 11, color: "var(--ink-3)" }}>
            {loading ? "正在分析今日消息…" : `基于 ${group.todayMsgs} 条今日消息生成 · ${AppData.formatRelative(Date.now() - 30000)}`}
          </div>
        </div>
        <Btn variant="ghost" size="sm" icon={<Icon name="spark" size={12}/>}
          onClick={() => { setLoading(true); setTimeout(() => { setSummary(AppData.buildSummary(group)); setLoading(false); }, 500); }}>
          重新生成
        </Btn>
      </div>

      {loading ? (
        <div style={{ padding: "12px 18px 18px", display: "flex", flexDirection: "column", gap: 8 }}>
          {[100, 85, 70].map((w, i) => (
            <div key={i} style={{
              height: 10, width: `${w}%`, borderRadius: 4,
              background: "linear-gradient(90deg, var(--bg-sunk), oklch(0.94 0.012 60), var(--bg-sunk))",
              backgroundSize: "200% 100%",
              animation: "shimmer 1.4s infinite linear",
            }}/>
          ))}
        </div>
      ) : (
        <div style={{ padding: "10px 18px 18px", display: "flex", flexDirection: "column", gap: 14 }}>
          {/* highlights */}
          <ul style={{ margin: 0, padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 6 }}>
            {summary.highlights.map((h, i) => (
              <li key={i} style={{ display: "flex", gap: 8, fontSize: 13, color: "var(--ink-2)", lineHeight: 1.55 }}>
                <span style={{
                  marginTop: 8, width: 4, height: 4, borderRadius: 999, flexShrink: 0,
                  background: "var(--accent)",
                }}/>
                <span>{h}</span>
              </li>
            ))}
          </ul>

          {/* topics */}
          <div>
            <div style={{ fontSize: 11, color: "var(--ink-3)", marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>
              讨论话题
            </div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {summary.topics.map(t => (
                <Pill key={t} tone="outline" style={{ fontSize: 12 }}>#{t}</Pill>
              ))}
            </div>
          </div>

          {/* action items */}
          <div>
            <div style={{ fontSize: 11, color: "var(--ink-3)", marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>
              待跟进项（{summary.actions.length}）
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {summary.actions.map((a, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "center", gap: 10,
                  padding: "8px 10px", borderRadius: 10,
                  background: "var(--bg-elev)",
                  border: "1px solid var(--line)",
                  fontSize: 12.5,
                }}>
                  <div style={{
                    width: 16, height: 16, borderRadius: 5,
                    border: "1.5px solid var(--line-strong)",
                    flexShrink: 0,
                  }}/>
                  <div style={{ flex: 1 }}>
                    <div style={{ color: "var(--ink)" }}>{a.todo}</div>
                    <div style={{ color: "var(--ink-3)", fontSize: 11, marginTop: 1 }}>
                      @{a.who} · 截止 {a.due}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      <style>{`
        @keyframes shimmer {
          0% { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
      `}</style>
    </Card>
  );
};

const MessageBubble = ({ msg, isTeam, showName }) => {
  return (
    <div style={{
      display: "flex", gap: 8,
      flexDirection: isTeam ? "row-reverse" : "row",
      alignItems: "flex-end",
      marginBottom: 2,
    }}>
      {showName ? <Avatar avatar={msg.from.avatar} size={28}/> : <div style={{ width: 28, flexShrink: 0 }}/>}
      <div style={{ maxWidth: "65%", display: "flex", flexDirection: "column", alignItems: isTeam ? "flex-end" : "flex-start" }}>
        {showName && (
          <div style={{
            fontSize: 10.5, color: "var(--ink-3)", marginBottom: 2,
            display: "flex", gap: 6, alignItems: "center",
          }}>
            <span>{msg.from.name}</span>
            {msg.from.side === "team" && (
              <span style={{ color: "var(--ink-4)" }}>· {msg.from.role}</span>
            )}
            <span className="num" style={{ color: "var(--ink-4)" }}>{AppData.formatTime(msg.time)}</span>
          </div>
        )}
        <div style={{
          padding: "8px 12px",
          borderRadius: isTeam ? "14px 14px 4px 14px" : "14px 14px 14px 4px",
          background: isTeam ? "var(--accent)" : "var(--bg-elev)",
          color: isTeam ? "white" : "var(--ink)",
          fontSize: 13, lineHeight: 1.55,
          border: isTeam ? "none" : "1px solid var(--line)",
          wordBreak: "break-word",
        }}>
          {msg.text}
        </div>
      </div>
    </div>
  );
};

const TimeDivider = ({ time }) => (
  <div style={{ display: "flex", justifyContent: "center", margin: "14px 0 6px" }}>
    <span className="num" style={{
      fontSize: 10.5, color: "var(--ink-4)",
      background: "var(--bg-sunk)", padding: "2px 8px", borderRadius: 999,
    }}>
      {new Date(time).toLocaleString("zh-CN", { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" })}
    </span>
  </div>
);

const Messages = ({ group }) => {
  const scrollRef = React.useRef(null);
  React.useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [group.id]);
  return (
    <div ref={scrollRef} className="scroll" style={{
      flex: 1, overflowY: "auto",
      padding: "8px 18px 16px",
      display: "flex", flexDirection: "column", gap: 6,
    }}>
      {group.messages.map((m, i) => {
        const prev = group.messages[i - 1];
        const showDivider = !prev || (m.time - prev.time > 30 * 60 * 1000);
        const showName = !prev || prev.from.id !== m.from.id || showDivider;
        return (
          <React.Fragment key={m.id}>
            {showDivider && <TimeDivider time={m.time} />}
            <MessageBubble msg={m} isTeam={m.from.side === "team"} showName={showName} />
          </React.Fragment>
        );
      })}
    </div>
  );
};

const MembersPanel = ({ group }) => {
  const clients = group.members.filter(m => m.side === "client");
  const team = group.members.filter(m => m.side === "team");
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <Card padded={false} style={{ padding: 14 }}>
        <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginBottom: 10, textTransform: "uppercase", letterSpacing: 0.5 }}>
          客户方 · {clients.length}
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {clients.map(m => (
            <div key={m.id} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <Avatar avatar={m.avatar} size={28}/>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 12.5, fontWeight: 500 }}>{m.name}</div>
                <div style={{ fontSize: 11, color: "var(--ink-3)" }}>{m.company}</div>
              </div>
            </div>
          ))}
        </div>
      </Card>

      <Card padded={false} style={{ padding: 14 }}>
        <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginBottom: 10, textTransform: "uppercase", letterSpacing: 0.5 }}>
          我方 · {team.length}
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {team.map(m => (
            <div key={m.id} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <Avatar avatar={m.avatar} size={28}/>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 12.5, fontWeight: 500 }}>{m.name}</div>
                <div style={{ fontSize: 11, color: "var(--ink-3)" }}>{m.role}</div>
              </div>
              {m.id === group.owner.id && <Pill tone="accent" style={{ fontSize: 10 }}>负责人</Pill>}
            </div>
          ))}
        </div>
      </Card>

      <Card padded={false} style={{ padding: 14 }}>
        <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginBottom: 10, textTransform: "uppercase", letterSpacing: 0.5 }}>
          群画像
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8, fontSize: 12.5 }}>
          <Row label="情绪" value={<Pill tone={group.sentiment.key === "happy" ? "sage" : group.sentiment.key === "concern" ? "amber" : group.sentiment.key === "angry" ? "rose" : "sky"}>{group.sentiment.label}</Pill>} />
          <Row label="本周消息" value={<span className="num">{group.weekMsgs}</span>} />
          <Row label="今日消息" value={<span className="num">{group.todayMsgs}</span>} />
          <Row label="负责人" value={group.owner.name} />
          <Row label="标签" value={
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap", justifyContent: "flex-end" }}>
              {group.tags.map(t => <Pill key={t} tone="outline">{t}</Pill>)}
            </div>
          } />
        </div>
      </Card>
    </div>
  );
};

const Row = ({ label, value }) => (
  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
    <span style={{ color: "var(--ink-3)" }}>{label}</span>
    <span style={{ color: "var(--ink)", textAlign: "right" }}>{value}</span>
  </div>
);

const DetailPane = ({ group }) => {
  const [tab, setTab] = React.useState("summary");
  const stalled = group.unreplyMinutes > 60;

  return (
    <div style={{
      flex: 1, display: "flex", flexDirection: "column",
      background: "var(--bg-sunk)",
      minWidth: 0, minHeight: 0,
    }}>
      {/* header */}
      <div style={{
        padding: "14px 22px",
        background: "var(--bg-elev)",
        borderBottom: "1px solid var(--line)",
        display: "flex", alignItems: "center", gap: 14,
      }}>
        <Avatar avatar={group.avatar} size={44} ring/>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <h2 style={{ margin: 0, fontSize: 16, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {group.name}
            </h2>
            {stalled && (
              <Pill tone="amber" icon={<Icon name="alert" size={10} strokeWidth={2.2}/>}>
                超时未回复
              </Pill>
            )}
          </div>
          <div style={{ fontSize: 12, color: "var(--ink-3)", marginTop: 2 }}>
            {group.memberCount} 位成员 · 负责人 {group.owner.name} · 今日 <span className="num">{group.todayMsgs}</span> 条消息
          </div>
        </div>
        <Btn variant="outline" size="sm" icon={<Icon name="bell" size={12}/>}>关注</Btn>
        <Btn variant="outline" size="sm" icon={<Icon name="flag" size={12}/>}>标记跟进</Btn>
        <Btn variant="ghost" size="sm"><Icon name="more" size={16}/></Btn>
      </div>

      {/* body: 2-col with right sidebar */}
      <div style={{ flex: 1, display: "flex", minHeight: 0 }}>
        {/* center */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0, padding: "14px 18px 0" }}>
          <SummaryCard group={group} />

          <div style={{ display: "flex", gap: 2, margin: "14px 0 8px", borderBottom: "1px solid var(--line)" }}>
            {[
              { id: "summary", label: "消息流" },
              { id: "keywords", label: "关键词" },
              { id: "read", label: "消息阅读" },
            ].map(t => (
              <button key={t.id} onClick={() => setTab(t.id)}
                style={{
                  padding: "8px 12px", fontSize: 13,
                  color: tab === t.id ? "var(--ink)" : "var(--ink-3)",
                  fontWeight: 500,
                  borderBottom: `2px solid ${tab === t.id ? "var(--accent)" : "transparent"}`,
                  marginBottom: -1,
                }}>
                {t.label}
              </button>
            ))}
          </div>

          {tab === "summary" && <Messages group={group} />}
          {tab === "keywords" && <KeywordsView group={group} />}
          {tab === "read" && <ReadStatusView group={group} />}
        </div>

        {/* right rail */}
        <div className="scroll" style={{
          width: 260, flexShrink: 0, padding: "14px 16px 20px",
          borderLeft: "1px solid var(--line)",
          background: "var(--bg-elev)",
          overflowY: "auto",
        }}>
          <MembersPanel group={group} />
        </div>
      </div>
    </div>
  );
};

const KeywordsView = ({ group }) => {
  const sample = [
    { word: "续约", count: 14, tone: "accent" },
    { word: "价格", count: 9, tone: "amber" },
    { word: "报表", count: 7, tone: "sky" },
    { word: "bug", count: 6, tone: "rose" },
    { word: "排期", count: 5, tone: "sage" },
    { word: "合同", count: 4, tone: "violet" },
    { word: "对接", count: 4, tone: "outline" },
    { word: "培训", count: 3, tone: "outline" },
  ];
  return (
    <div className="scroll" style={{ flex: 1, overflowY: "auto", padding: "4px 4px 16px" }}>
      <Card style={{ padding: 18 }}>
        <div style={{ fontSize: 13, color: "var(--ink-3)", marginBottom: 12 }}>本周出现频次最高的关键词</div>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          {sample.map(k => (
            <div key={k.word} style={{
              display: "inline-flex", alignItems: "center", gap: 6,
              padding: "6px 10px", borderRadius: 999,
              fontSize: 13,
              background: `var(--${k.tone === "outline" ? "bg-sunk" : k.tone}-soft)`,
              color: "var(--ink)",
            }}>
              {k.word}
              <span className="num" style={{ color: "var(--ink-3)", fontSize: 11 }}>{k.count}</span>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
};

const ReadStatusView = ({ group }) => {
  const { read, total } = group.readStatus;
  const pct = Math.round((read / total) * 100);
  const readers = group.members.slice(0, read);
  const unreaders = group.members.slice(read, total);
  return (
    <div className="scroll" style={{ flex: 1, overflowY: "auto", padding: "4px 4px 16px", display: "flex", flexDirection: "column", gap: 12 }}>
      <Card style={{ padding: 18 }}>
        <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 10 }}>
          <span className="num" style={{ fontSize: 28, fontWeight: 600 }}>{pct}%</span>
          <span style={{ fontSize: 12, color: "var(--ink-3)" }}>
            {read} / {total} 人已读最新消息
          </span>
        </div>
        <Progress value={read} total={total} height={8}/>
      </Card>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card style={{ padding: 14 }}>
          <div style={{ fontSize: 11.5, color: "oklch(0.35 0.08 155)", marginBottom: 10, fontWeight: 500 }}>
            已读 · {readers.length}
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {readers.map(m => (
              <div key={m.id} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12.5 }}>
                <Avatar avatar={m.avatar} size={24}/>
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.name}</span>
                <Icon name="check" size={12} color="oklch(0.55 0.11 155)" strokeWidth={2.2}/>
              </div>
            ))}
          </div>
        </Card>
        <Card style={{ padding: 14 }}>
          <div style={{ fontSize: 11.5, color: "oklch(0.40 0.09 20)", marginBottom: 10, fontWeight: 500 }}>
            未读 · {unreaders.length}
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {unreaders.map(m => (
              <div key={m.id} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12.5, color: "var(--ink-3)" }}>
                <Avatar avatar={m.avatar} size={24} style={{ opacity: 0.7 }}/>
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.name}</span>
                <Icon name="dot" size={12} color="var(--ink-4)"/>
              </div>
            ))}
            {unreaders.length === 0 && <div style={{ fontSize: 11.5, color: "var(--ink-4)" }}>全员已读 🎉</div>}
          </div>
        </Card>
      </div>
    </div>
  );
};

Object.assign(window, { DetailPane });
