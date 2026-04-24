// Shared UI atoms

const Avatar = ({ avatar, size = 36, ring = false, style }) => {
  const s = {
    width: size, height: size, borderRadius: size * 0.32,
    background: avatar.bg, color: avatar.fg,
    display: "grid", placeItems: "center",
    fontSize: size * 0.45, fontWeight: 600,
    flexShrink: 0,
    boxShadow: ring ? `0 0 0 2px var(--bg-elev), 0 0 0 3.5px ${avatar.ring}` : "none",
    fontFamily: "var(--font-cn)",
    ...style,
  };
  return <div style={s}>{avatar.initial}</div>;
};

const Pill = ({ children, tone = "neutral", style, icon }) => {
  const tones = {
    neutral: { bg: "var(--bg-sunk)", fg: "var(--ink-2)", bd: "var(--line)" },
    accent:  { bg: "var(--accent-soft)", fg: "var(--accent-ink)", bd: "transparent" },
    sage:    { bg: "var(--sage-soft)", fg: "oklch(0.35 0.06 155)", bd: "transparent" },
    amber:   { bg: "var(--amber-soft)", fg: "oklch(0.40 0.11 70)", bd: "transparent" },
    rose:    { bg: "var(--rose-soft)", fg: "oklch(0.40 0.11 20)", bd: "transparent" },
    sky:     { bg: "var(--sky-soft)",  fg: "oklch(0.38 0.08 240)", bd: "transparent" },
    violet:  { bg: "var(--violet-soft)", fg: "oklch(0.40 0.09 300)", bd: "transparent" },
    outline: { bg: "transparent", fg: "var(--ink-2)", bd: "var(--line-strong)" },
  };
  const t = tones[tone] || tones.neutral;
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 4,
      padding: "2px 8px", borderRadius: 999,
      background: t.bg, color: t.fg, border: `1px solid ${t.bd}`,
      fontSize: 11.5, fontWeight: 500, lineHeight: 1.4,
      whiteSpace: "nowrap",
      ...style,
    }}>
      {icon}{children}
    </span>
  );
};

const Btn = ({ children, variant = "ghost", size = "md", icon, iconRight, onClick, style, disabled, title }) => {
  const sizes = {
    sm: { padding: "5px 10px", fontSize: 12.5, radius: 8, gap: 5 },
    md: { padding: "8px 14px", fontSize: 13, radius: 10, gap: 6 },
    lg: { padding: "11px 18px", fontSize: 14, radius: 12, gap: 7 },
  };
  const variants = {
    primary: {
      background: "var(--accent)", color: "white", border: "1px solid transparent",
      boxShadow: "0 1px 2px oklch(0.3 0.02 60 / 0.2), inset 0 1px 0 oklch(1 0 0 / 0.15)",
    },
    soft: {
      background: "var(--accent-soft)", color: "var(--accent-ink)",
      border: "1px solid transparent",
    },
    ghost: {
      background: "transparent", color: "var(--ink-2)",
      border: "1px solid transparent",
    },
    outline: {
      background: "var(--bg-elev)", color: "var(--ink)",
      border: "1px solid var(--line-strong)",
    },
    subtle: {
      background: "var(--bg-sunk)", color: "var(--ink-2)",
      border: "1px solid transparent",
    },
  };
  const sz = sizes[size];
  const v = variants[variant];
  return (
    <button
      disabled={disabled}
      title={title}
      onClick={onClick}
      style={{
        display: "inline-flex", alignItems: "center", gap: sz.gap,
        padding: sz.padding, fontSize: sz.fontSize, borderRadius: sz.radius,
        fontWeight: 500, cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
        transition: "all 120ms ease",
        ...v,
        ...style,
      }}
      onMouseEnter={e => {
        if (disabled) return;
        if (variant === "primary") e.currentTarget.style.filter = "brightness(1.05)";
        else if (variant === "ghost") e.currentTarget.style.background = "var(--bg-sunk)";
        else if (variant === "outline") e.currentTarget.style.background = "var(--bg-sunk)";
        else if (variant === "soft") e.currentTarget.style.filter = "brightness(0.98)";
      }}
      onMouseLeave={e => {
        e.currentTarget.style.filter = "";
        e.currentTarget.style.background = v.background;
      }}
    >
      {icon}{children}{iconRight}
    </button>
  );
};

const Card = ({ children, style, as = "div", padded = true, ...rest }) => {
  const Tag = as;
  return (
    <Tag {...rest} style={{
      background: "var(--bg-elev)",
      borderRadius: "var(--r-lg)",
      border: "1px solid var(--line)",
      boxShadow: "var(--sh-1)",
      padding: padded ? 20 : 0,
      ...style,
    }}>
      {children}
    </Tag>
  );
};

// progress bar with gradient fill
const Progress = ({ value, total, color = "var(--accent)", height = 6, showLabel = false }) => {
  const pct = total > 0 ? Math.max(0, Math.min(1, value / total)) : 0;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{
        flex: 1, height, borderRadius: 999, background: "var(--bg-sunk)",
        overflow: "hidden", border: "1px solid var(--line)",
      }}>
        <div style={{
          width: `${pct * 100}%`, height: "100%", borderRadius: 999,
          background: color,
          transition: "width 400ms cubic-bezier(.2,.8,.2,1)",
        }}/>
      </div>
      {showLabel && (
        <span className="num" style={{ fontSize: 11.5, color: "var(--ink-3)", minWidth: 32, textAlign: "right" }}>
          {Math.round(pct * 100)}%
        </span>
      )}
    </div>
  );
};

// sparkline
const Sparkline = ({ values, color = "var(--accent)", height = 28, width = 80, fill = true }) => {
  if (!values || !values.length) return null;
  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const points = values.map((v, i) => {
    const x = i * step;
    const y = height - ((v - min) / range) * (height - 4) - 2;
    return [x, y];
  });
  const path = points.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(" ");
  const area = `${path} L ${width} ${height} L 0 ${height} Z`;
  return (
    <svg width={width} height={height} style={{ display: "block", overflow: "visible" }}>
      {fill && <path d={area} fill={color} fillOpacity="0.12" />}
      <path d={path} stroke={color} strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
};

// tiny tooltip
const Tip = ({ text, children }) => {
  const [shown, setShown] = React.useState(false);
  return (
    <span style={{ position: "relative", display: "inline-flex" }}
      onMouseEnter={() => setShown(true)} onMouseLeave={() => setShown(false)}>
      {children}
      {shown && (
        <span style={{
          position: "absolute", bottom: "calc(100% + 6px)", left: "50%",
          transform: "translateX(-50%)",
          background: "oklch(0.20 0.01 60)", color: "white", fontSize: 11,
          padding: "4px 8px", borderRadius: 6, whiteSpace: "nowrap",
          pointerEvents: "none", zIndex: 100,
        }}>{text}</span>
      )}
    </span>
  );
};

Object.assign(window, { Avatar, Pill, Btn, Card, Progress, Sparkline, Tip });
