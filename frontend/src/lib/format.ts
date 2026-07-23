export function compact(value: unknown, fallback = "n/a"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  if (Array.isArray(value)) {
    if (!value.length) {
      return fallback;
    }
    const text: string = value
      .slice(0, 8)
      .map((item) => compact(item, fallback))
      .join(", ");
    return value.length > 8 ? `${text}, +${value.length - 8} more` : text;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? fallback : value.toISOString();
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).filter(
      ([, child]) => child !== null && child !== undefined && child !== ""
    );
    if (!entries.length) {
      return fallback;
    }
    const text: string = entries
      .slice(0, 8)
      .map(([key, child]) => `${key}: ${compact(child, fallback)}`)
      .join(", ");
    return entries.length > 8 ? `${text}, +${entries.length - 8} more` : text;
  }
  return String(value);
}

export function numberText(value: unknown, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return compact(value);
  }
  return numeric.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: 0
  });
}

export function shareValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return undefined;
  }
  return Math.abs(numeric) > 1 && Math.abs(numeric) <= 100 ? numeric / 100 : numeric;
}

export function pctText(value: unknown) {
  const numeric = shareValue(value);
  if (numeric === undefined) {
    return "n/a";
  }
  return `${(numeric * 100).toFixed(1)}%`;
}

export function decisionGradeCoverageText(value: string | number | null | undefined) {
  if (value === null || value === undefined) {
    return "pending — no evaluation denominator";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "pending — invalid coverage";
  }
  return `${numberText(numeric * 100, 2)}%`;
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
