export function compact(value: unknown, fallback = "n/a") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

export function numberText(value: unknown, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value);
  }
  return numeric.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: 0
  });
}

export function pctText(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return `${(numeric * 100).toFixed(1)}%`;
}

export function dateTime(value?: string | null) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

export function ageText(value?: string | null) {
  if (!value) {
    return "n/a";
  }
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts)) {
    return "n/a";
  }
  const seconds = Math.max(0, Math.round((Date.now() - ts) / 1000));
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }
  return `${Math.floor(minutes / 60)}h`;
}
