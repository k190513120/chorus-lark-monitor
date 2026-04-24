// Tiny icon set - stroke based
const Icon = ({ name, size = 16, color = "currentColor", strokeWidth = 1.75, style }) => {
  const common = {
    width: size, height: size, viewBox: "0 0 24 24", fill: "none",
    stroke: color, strokeWidth, strokeLinecap: "round", strokeLinejoin: "round", style
  };
  switch (name) {
    case "search": return <svg {...common}><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>;
    case "bell":   return <svg {...common}><path d="M6 8a6 6 0 1 1 12 0c0 4 1.5 5 2 6H4c.5-1 2-2 2-6Z"/><path d="M10 19a2 2 0 0 0 4 0"/></svg>;
    case "send":   return <svg {...common}><path d="m3 11 18-8-7 18-2-8-9-2Z"/></svg>;
    case "chat":   return <svg {...common}><path d="M4 6a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-7l-5 4V6Z"/></svg>;
    case "users":  return <svg {...common}><circle cx="9" cy="8" r="3.5"/><path d="M3 19c.8-3 3-5 6-5s5.2 2 6 5"/><circle cx="17" cy="9" r="2.5"/><path d="M16 14c2.5.3 4 2 4.5 4"/></svg>;
    case "clock":  return <svg {...common}><circle cx="12" cy="12" r="8"/><path d="M12 8v4l3 2"/></svg>;
    case "spark":  return <svg {...common}><path d="M12 3v3M12 18v3M3 12h3M18 12h3M5.6 5.6l2.1 2.1M16.3 16.3l2.1 2.1M5.6 18.4l2.1-2.1M16.3 7.7l2.1-2.1"/></svg>;
    case "pin":    return <svg {...common}><path d="M14 3 21 10l-3 1-5 5 1 6-6-6-5 5"/></svg>;
    case "alert":  return <svg {...common}><path d="M12 3 2 20h20L12 3Z"/><path d="M12 10v4"/><circle cx="12" cy="17" r=".5" fill={color}/></svg>;
    case "check":  return <svg {...common}><path d="m5 12 5 5 9-11"/></svg>;
    case "check2": return <svg {...common}><path d="m4 12 4 4 7-9"/><path d="m11 16 2 2 7-9"/></svg>;
    case "x":      return <svg {...common}><path d="M5 5l14 14M19 5 5 19"/></svg>;
    case "plus":   return <svg {...common}><path d="M12 5v14M5 12h14"/></svg>;
    case "chevronDown": return <svg {...common}><path d="m6 9 6 6 6-6"/></svg>;
    case "chevronRight": return <svg {...common}><path d="m9 6 6 6-6 6"/></svg>;
    case "arrowUp":   return <svg {...common}><path d="M12 19V5M6 11l6-6 6 6"/></svg>;
    case "arrowDown": return <svg {...common}><path d="M12 5v14M6 13l6 6 6-6"/></svg>;
    case "filter": return <svg {...common}><path d="M3 5h18l-7 9v5l-4 2v-7L3 5Z"/></svg>;
    case "dot":    return <svg {...common}><circle cx="12" cy="12" r="3" fill={color} stroke="none"/></svg>;
    case "more":   return <svg {...common}><circle cx="5" cy="12" r="1" fill={color} stroke="none"/><circle cx="12" cy="12" r="1" fill={color} stroke="none"/><circle cx="19" cy="12" r="1" fill={color} stroke="none"/></svg>;
    case "smile":  return <svg {...common}><circle cx="12" cy="12" r="9"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><circle cx="9" cy="10" r=".5" fill={color}/><circle cx="15" cy="10" r=".5" fill={color}/></svg>;
    case "flag":   return <svg {...common}><path d="M4 21V4h13l-2 4 2 4H4"/></svg>;
    case "pie":    return <svg {...common}><path d="M12 3v9h9"/><path d="M21 12a9 9 0 1 1-9-9"/></svg>;
    case "trend":  return <svg {...common}><path d="M3 17 9 11l4 4 8-9"/><path d="M14 6h7v7"/></svg>;
    case "heart":  return <svg {...common}><path d="M12 20s-7-5-7-10a4 4 0 0 1 7-2 4 4 0 0 1 7 2c0 5-7 10-7 10Z"/></svg>;
    case "book":   return <svg {...common}><path d="M4 5a2 2 0 0 1 2-2h13v17H6a2 2 0 0 0-2 2V5Z"/><path d="M4 19h15"/></svg>;
    case "zap":    return <svg {...common}><path d="M13 2 4 14h7l-1 8 9-12h-7l1-8Z"/></svg>;
    case "eye":    return <svg {...common}><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z"/><circle cx="12" cy="12" r="3"/></svg>;
    case "sliders":return <svg {...common}><path d="M4 21V14M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3"/><circle cx="4" cy="12" r="2"/><circle cx="12" cy="10" r="2"/><circle cx="20" cy="14" r="2"/></svg>;
    case "lark":   return <svg {...common}><path d="M5 10c3-4 9-4 12 0M7 14c2-2.5 6-2.5 8 0M10 18h4"/></svg>;
    default: return null;
  }
};

window.Icon = Icon;
