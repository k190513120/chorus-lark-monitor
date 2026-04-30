// Stats overview — 7 metric cards + trend/sentiment/speakers

const StatCard = ({ label, value, unit, delta, trend, icon, color = "var(--ink)", tone = "neutral", sparkValues }) => {
  const up = delta && delta > 0;
  const toneColors = {
    neutral: "var(--bg-sunk)",
    accent: "var(--accent-soft)",
    sage: "var(--sage-soft)",
    amber: "var(--amber-soft)",
    rose: "var(--rose-soft)",
    sky: "var(--sky-soft)",
    violet: "var(--violet-soft)",
  };
  return (
    <Card style={{ padding: 18, display: "flex", flexDirection: "column", gap: 10, minWidth: 0 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--ink-3)", fontSize: 12.5 }}>
          <div style={{
            width: 26, height: 26, borderRadius: 8,
            background: toneColors[tone], color,
            display: "grid", placeItems: "center",
          }}>
            {icon}
          </div>
          {label}
        </div>
        {delta != null && (
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 2,
            fontSize: 11.5, color: up ? "oklch(0.5 0.1 155)" : "oklch(0.5 0.1 20)",
            fontWeight: 500,
          }}>
            <Icon name={up ? "arrowUp" : "arrowDown"} size={11} strokeWidth={2.2}/>
            <span className="num">{Math.abs(delta)}%</span>
          </span>
        )}
      </div>
      <div style={{ display: "flex", alignItems: "baseline", gap: 4, marginTop: 2 }}>
        <span className="metric-val" style={{ fontSize: 30, fontWeight: 600, color: "var(--ink)", lineHeight: 1 }}>
          {value}
        </span>
        {unit && <span style={{ fontSize: 12.5, color: "var(--ink-3)" }}>{unit}</span>}
      </div>
      {sparkValues && (
        <Sparkline values={sparkValues} color={color} height={22} width={120} />
      )}
      {trend && !sparkValues && (
        <div style={{ fontSize: 11.5, color: "var(--ink-3)" }}>{trend}</div>
      )}
    </Card>
  );
};

const HourlyChart = ({ values }) => {
  const max = Math.max(...values);
  const now = new Date().getHours();
  return (
    <Card style={{ padding: 18 }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 14 }}>
        <div>
          <div style={{ fontSize: 13, color: "var(--ink-3)", marginBottom: 2 }}>今日消息时段分布</div>
          <div style={{ fontSize: 18, fontWeight: 600 }} className="num">
            {values.reduce((a,b)=>a+b,0)} <span style={{ fontSize: 12, color: "var(--ink-3)", fontWeight: 400 }}>条</span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 12, fontSize: 11.5, color: "var(--ink-3)" }}>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 5 }}>
            <span style={{ width: 8, height: 8, borderRadius: 2, background: "var(--accent)" }}/>
            客户
          </span>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 5 }}>
            <span style={{ width: 8, height: 8, borderRadius: 2, background: "var(--sage)" }}/>
            团队
          </span>
        </div>
      </div>
      <div style={{ display: "flex", alignItems: "flex-end", gap: 3, height: 84 }}>
        {values.map((v, h) => {
          const pct = (v / max) * 100;
          const isBiz = h >= 9 && h <= 19;
          const isNow = h === now;
          const clientH = v * 0.45;
          const teamH = v - clientH;
          return (
            <div key={h} style={{ flex: 1, display: "flex", flexDirection: "column", gap: 2, alignItems: "center", minWidth: 0 }}>
              <div style={{
                height: `${pct}%`, width: "100%", display: "flex", flexDirection: "column", justifyContent: "flex-end",
                position: "relative",
              }}>
                <div style={{
                  height: `${(teamH / v) * 100}%`,
                  background: "var(--sage)",
                  opacity: isBiz ? 0.85 : 0.35,
                  borderRadius: "3px 3px 0 0",
                }}/>
                <div style={{
                  height: `${(clientH / v) * 100}%`,
                  background: "var(--accent)",
                  opacity: isBiz ? 0.9 : 0.4,
                }}/>
                {isNow && (
                  <div style={{
                    position: "absolute", top: -14, left: "50%", transform: "translateX(-50%)",
                    fontSize: 9, color: "var(--accent-ink)", fontWeight: 600,
                    background: "var(--accent-soft)", padding: "1px 4px", borderRadius: 3, whiteSpace: "nowrap",
                  }}>NOW</div>
                )}
              </div>
              <div className="num" style={{ fontSize: 9.5, color: h % 3 === 0 ? "var(--ink-4)" : "transparent" }}>
                {String(h).padStart(2, "0")}
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
};

const SentimentDonut = ({ items }) => {
  const total = items.reduce((a, s) => a + s.count, 0);
  let acc = 0;
  const R = 34, CX = 50, CY = 50, STROKE = 14;
  const C = 2 * Math.PI * R;
  return (
    <Card style={{ padding: 18 }}>
      <div style={{ fontSize: 13, color: "var(--ink-3)", marginBottom: 4 }}>客户情绪分析</div>
      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <div style={{ position: "relative", width: 100, height: 100, flexShrink: 0 }}>
          <svg width="100" height="100" viewBox="0 0 100 100" style={{ transform: "rotate(-90deg)" }}>
            <circle cx={CX} cy={CY} r={R} fill="none" stroke="var(--bg-sunk)" strokeWidth={STROKE}/>
            {items.map((s, i) => {
              if (s.count === 0) return null;
              const len = (s.count / total) * C;
              const off = C - acc;
              acc += len;
              return (
                <circle key={i}
                  cx={CX} cy={CY} r={R} fill="none"
                  stroke={s.color} strokeWidth={STROKE}
                  strokeDasharray={`${len} ${C}`}
                  strokeDashoffset={off}
                  strokeLinecap="butt"
                />
              );
            })}
          </svg>
          <div style={{ position: "absolute", inset: 0, display: "grid", placeItems: "center", textAlign: "center" }}>
            <div>
              <div className="num" style={{ fontSize: 22, fontWeight: 600, lineHeight: 1 }}>{total}</div>
              <div style={{ fontSize: 10.5, color: "var(--ink-3)", marginTop: 2 }}>群</div>
            </div>
          </div>
        </div>
        <div style={{ flex: 1, display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px 12px" }}>
          {items.map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12 }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, background: s.color }}/>
              <span style={{ color: "var(--ink-2)" }}>{s.label}</span>
              <span className="num" style={{ color: "var(--ink-3)", marginLeft: "auto" }}>{s.count}</span>
            </div>
          ))}
        </div>
      </div>
    </Card>
  );
};

const SpeakerBars = ({ speakers }) => {
  const max = Math.max(...speakers.map(s => s.msgs));
  return (
    <Card style={{ padding: 18 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
        <div style={{ fontSize: 13, color: "var(--ink-3)" }}>员工发言分布 · 今日</div>
        <span style={{ fontSize: 11, color: "var(--ink-4)" }}>{speakers.length} 人</span>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {speakers.slice(0, 6).map((u, i) => (
          <div key={u.id} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12.5 }}>
            <div style={{ width: 70, color: "var(--ink-2)", display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ width: 6, height: 6, borderRadius: 999, background: u.color }}/>
              {u.name}
            </div>
            <div style={{ flex: 1, height: 8, background: "var(--bg-sunk)", borderRadius: 999, overflow: "hidden" }}>
              <div style={{
                width: `${(u.msgs / max) * 100}%`, height: "100%",
                background: u.color, borderRadius: 999,
                transition: "width 400ms",
              }}/>
            </div>
            <span className="num" style={{ width: 32, textAlign: "right", color: "var(--ink-3)", fontSize: 11.5 }}>
              {u.msgs}
            </span>
          </div>
        ))}
      </div>
    </Card>
  );
};

const AlertStrip = ({ stalled, onOpen }) => {
  if (!stalled || stalled.length === 0) return null;
  return (
    <div style={{
      background: "var(--amber-soft)",
      border: "1px solid oklch(0.85 0.08 75)",
      borderRadius: "var(--r-lg)",
      padding: "12px 16px",
      display: "flex", alignItems: "center", gap: 12,
    }}>
      <div style={{
        width: 32, height: 32, borderRadius: 10,
        background: "oklch(0.88 0.11 75)", color: "oklch(0.30 0.10 70)",
        display: "grid", placeItems: "center",
      }}>
        <Icon name="alert" size={16} strokeWidth={2}/>
      </div>
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: "oklch(0.30 0.10 70)" }}>
          {stalled.length} 个群超过 1 小时未回复客户
        </div>
        <div style={{ fontSize: 12, color: "oklch(0.42 0.09 70)", marginTop: 2 }}>
          {stalled.slice(0, 3).map(g => g.company).join("、")}{stalled.length > 3 ? ` 等 ${stalled.length} 个群` : ""}
        </div>
      </div>
      <Btn variant="outline" size="sm" onClick={() => onOpen && onOpen(stalled[0])}
        style={{ background: "white", borderColor: "oklch(0.82 0.09 75)" }}>
        查看详情 <Icon name="chevronRight" size={12}/>
      </Btn>
    </div>
  );
};

const BroadcastsCard = ({ items }) => {
  if (!items || items.length === 0) {
    return (
      <Card style={{ padding: 18 }}>
        <div style={{ fontSize: 13, color: "var(--ink-3)", marginBottom: 8 }}>近期群发</div>
        <div style={{ fontSize: 12, color: "var(--ink-3)" }}>暂无群发记录</div>
      </Card>
    );
  }

  const formatPct = (v) => `${(v * 100).toFixed(1)}%`;
  const heatColor = (rate) => {
    if (rate >= 0.6) return "oklch(0.60 0.12 155)";   // sage
    if (rate >= 0.3) return "oklch(0.62 0.13 75)";    // amber
    return "oklch(0.62 0.13 20)";                     // rose
  };

  return (
    <Card style={{ padding: 18 }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 14 }}>
        <div>
          <div style={{ fontSize: 13, color: "var(--ink-3)", marginBottom: 2 }}>近期群发</div>
          <div style={{ fontSize: 18, fontWeight: 600 }} className="num">
            {items.length} <span style={{ fontSize: 12, color: "var(--ink-3)", fontWeight: 400 }}>批</span>
          </div>
        </div>
        <div style={{ fontSize: 11.5, color: "var(--ink-3)" }}>
          已读率 / 回复率均已排除内部 tenant
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        <div style={{
          display: "grid",
          gridTemplateColumns: "1.6fr 90px 90px 110px 130px",
          gap: 8,
          padding: "6px 10px",
          fontSize: 11.5,
          color: "var(--ink-3)",
          background: "var(--bg-sunk)",
          borderRadius: 8,
        }}>
          <div>任务标题</div>
          <div style={{ textAlign: "right" }}>群数</div>
          <div style={{ textAlign: "right" }}>受众</div>
          <div style={{ textAlign: "right" }}>已读率</div>
          <div style={{ textAlign: "right" }}>回复率</div>
        </div>
        {items.map((b) => (
          <div key={b.batchId} style={{
            display: "grid",
            gridTemplateColumns: "1.6fr 90px 90px 110px 130px",
            gap: 8,
            padding: "8px 10px",
            fontSize: 12.5,
            alignItems: "center",
            borderBottom: "1px solid var(--line)",
          }}>
            <div style={{ minWidth: 0 }}>
              <div style={{
                fontWeight: 500,
                whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
              }} title={b.text}>
                {b.title}
              </div>
              <div style={{ fontSize: 11, color: "var(--ink-3)", marginTop: 2 }}>
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
          </div>
        ))}
      </div>
    </Card>
  );
};

const StatsOverview = ({ D, broadcasts, onAlertOpen }) => {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <AlertStrip stalled={D.stalled} onOpen={onAlertOpen} />

      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(4, minmax(0,1fr))",
        gap: 14,
      }}>
        <StatCard
          label="群聊总数" value={D.totalGroups} unit="个"
          icon={<Icon name="chat" size={14}/>} color="var(--accent-ink)" tone="accent"
          sparkValues={[38,39,40,40,41,41,42]}
        />
        <StatCard
          label="当日消息数" value={D.todayMsgs.toLocaleString()} delta={12}
          icon={<Icon name="send" size={14}/>} color="oklch(0.38 0.08 240)" tone="sky"
          sparkValues={D.hourlyMsgs.slice(8, 20)}
        />
        <StatCard
          label="活跃群" value={D.activeGroups} unit={`/${D.totalGroups}`} delta={-3}
          icon={<Icon name="zap" size={14}/>} color="oklch(0.35 0.08 155)" tone="sage"
          sparkValues={[28,32,30,35,34,36,D.activeGroups]}
        />
        <StatCard
          label="平均首响" value={D.avgResponseMin} unit="分钟" delta={-18}
          icon={<Icon name="clock" size={14}/>} color="oklch(0.40 0.09 300)" tone="violet"
          sparkValues={[6.1,5.4,5.8,4.9,4.6,4.3,4.2]}
        />
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: "1.4fr 1fr 1fr",
        gap: 14,
      }}>
        <HourlyChart values={D.hourlyMsgs} />
        <SentimentDonut items={D.sentimentBreakdown} />
        <SpeakerBars speakers={D.speakerDist} />
      </div>
    </div>
  );
};

Object.assign(window, { StatsOverview });
