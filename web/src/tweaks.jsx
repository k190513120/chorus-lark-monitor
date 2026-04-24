// Tweaks panel — style switching

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "tone": "warm",
  "radius": "default",
  "density": "comfortable",
  "accentHue": 45,
  "fontWeight": "regular"
}/*EDITMODE-END*/;

function applyTweaks(t) {
  const root = document.documentElement;
  root.dataset.tone = t.tone;
  root.dataset.radius = t.radius;
  root.dataset.density = t.density;
  // accent hue override
  root.style.setProperty("--accent",      `oklch(0.68 0.13 ${t.accentHue})`);
  root.style.setProperty("--accent-ink",  `oklch(0.38 0.11 ${t.accentHue})`);
  root.style.setProperty("--accent-soft", `oklch(0.95 0.03 ${t.accentHue})`);
}

const TweaksPanel = ({ tweaks, setTweaks, onClose }) => {
  const set = (patch) => {
    const next = { ...tweaks, ...patch };
    setTweaks(next);
    applyTweaks(next);
    window.parent.postMessage({ type: "__edit_mode_set_keys", edits: patch }, "*");
  };

  return (
    <div style={{
      position: "fixed", right: 20, bottom: 20, zIndex: 900,
      width: 280,
      background: "var(--bg-elev)",
      borderRadius: 16,
      border: "1px solid var(--line-strong)",
      boxShadow: "var(--sh-3)",
      overflow: "hidden",
    }}>
      <div style={{
        padding: "12px 16px", display: "flex", alignItems: "center", gap: 8,
        borderBottom: "1px solid var(--line)",
      }}>
        <Icon name="sliders" size={14}/>
        <span style={{ fontSize: 13, fontWeight: 600, flex: 1 }}>Tweaks</span>
        <button onClick={onClose} style={{ color: "var(--ink-3)", display: "grid", placeItems: "center", width: 22, height: 22, borderRadius: 6 }}>
          <Icon name="x" size={12}/>
        </button>
      </div>
      <div style={{ padding: 14, display: "flex", flexDirection: "column", gap: 14 }}>
        <TwField label="色调">
          <Seg options={[
            { k: "warm", label: "暖色" },
            { k: "cool", label: "冷色" },
          ]} value={tweaks.tone} onChange={v => set({ tone: v })}/>
        </TwField>
        <TwField label="强调色">
          <div style={{ display: "flex", gap: 6 }}>
            {[45, 25, 155, 245, 300].map(h => (
              <button key={h} onClick={() => set({ accentHue: h })}
                style={{
                  width: 26, height: 26, borderRadius: 999,
                  background: `oklch(0.68 0.13 ${h})`,
                  border: tweaks.accentHue === h ? "2px solid var(--ink)" : "2px solid transparent",
                  cursor: "pointer",
                }}/>
            ))}
          </div>
        </TwField>
        <TwField label="圆角">
          <Seg options={[
            { k: "tight", label: "紧凑" },
            { k: "default", label: "默认" },
            { k: "pillowy", label: "柔和" },
          ]} value={tweaks.radius} onChange={v => set({ radius: v })}/>
        </TwField>
        <TwField label="主题">
          <Seg options={[
            { k: "warm", label: "浅色" },
            { k: "dark", label: "深色" },
          ]} value={tweaks.tone === "dark" ? "dark" : "warm"} onChange={v => set({ tone: v === "dark" ? "dark" : "warm" })}/>
        </TwField>
      </div>
    </div>
  );
};

const TwField = ({ label, children }) => (
  <div>
    <div style={{ fontSize: 11, color: "var(--ink-3)", marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
    {children}
  </div>
);

const Seg = ({ options, value, onChange }) => (
  <div style={{
    display: "flex", gap: 2, padding: 2,
    background: "var(--bg-sunk)", borderRadius: 8,
    border: "1px solid var(--line)",
  }}>
    {options.map(o => (
      <button key={o.k} onClick={() => onChange(o.k)}
        style={{
          flex: 1, padding: "5px 0", fontSize: 12,
          borderRadius: 6,
          background: value === o.k ? "var(--bg-elev)" : "transparent",
          color: value === o.k ? "var(--ink)" : "var(--ink-3)",
          boxShadow: value === o.k ? "0 1px 2px oklch(0.2 0.02 60 / 0.08)" : "none",
          fontWeight: 500,
        }}>
        {o.label}
      </button>
    ))}
  </div>
);

// Apply tweaks on load (so saved defaults take effect on refresh)
if (typeof window !== "undefined") {
  try { applyTweaks(TWEAK_DEFAULTS); } catch (e) {}
}

Object.assign(window, { TweaksPanel, TWEAK_DEFAULTS, applyTweaks });
