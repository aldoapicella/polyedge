import { compact, dateTime, numberText, pctText } from "@/lib/format";
import type { ExecutionReport, MarketSummary, RuntimeEvent, Snapshot, TradeDecision } from "@/lib/types";
import type { CollapsedDecision, EventTab, ExecutionFilter, RecorderHealth, TimelineRow, Tone } from "./types";

export const EVENT_TABS: { value: EventTab; label: string }[] = [
  { value: "highlights", label: "Highlights" },
  { value: "orders", label: "Orders" },
  { value: "market", label: "Market Data" },
  { value: "errors", label: "Errors" },
  { value: "raw", label: "Raw" }
];

export const EXECUTION_FILTERS: { value: ExecutionFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "fills", label: "Fills" },
  { value: "resting", label: "Resting" },
  { value: "cancelled", label: "Cancelled" },
  { value: "errors", label: "Errors" }
];

export function timelineRows(events: RuntimeEvent[], active: MarketSummary | null | undefined, tab: EventTab, search: string) {
  const rows =
    tab === "market"
      ? coalescedBookRows(events, active)
      : events
          .map((event, index) => eventToRow(event, active, index, tab))
          .filter((row): row is TimelineRow => Boolean(row));
  const needle = search.trim().toLowerCase();
  if (!needle) {
    return rows;
  }
  return rows.filter((row) => `${row.title} ${row.message} ${row.raw.type}`.toLowerCase().includes(needle));
}

export function collapseDecisions(decisions: TradeDecision[]): CollapsedDecision[] {
  const output: CollapsedDecision[] = [];
  decisions
    .slice()
    .reverse()
    .forEach((decision, index) => {
      const action = decision.action.toUpperCase();
      const reason = decision.reason || "n/a";
      const last = output[output.length - 1];
      const canCollapse = action === "HOLD" && last?.action === "HOLD" && last.reason === reason;
      if (canCollapse) {
        last.count += 1;
        return;
      }
      output.push({
        key: `${decision.market_id}-${index}`,
        action,
        outcome: compact(decision.outcome).toUpperCase(),
        price: sharePriceText(decision.price),
        size: compact(decision.size),
        edge: sharePriceText(decision.expected_edge),
        reason,
        count: 1
      });
    });
  return output;
}

export function filterReports(reports: ExecutionReport[], filter: ExecutionFilter) {
  return reports
    .slice()
    .reverse()
    .filter((report) => {
      if (filter === "all") {
        return true;
      }
      if (filter === "fills") {
        return Number(report.filled_size) > 0 || report.status.includes("filled");
      }
      if (filter === "resting") {
        return report.status.includes("resting");
      }
      if (filter === "cancelled") {
        return report.status.includes("cancel");
      }
      return report.status.includes("error") || report.status.includes("reject") || report.status.includes("missing");
    });
}

export function recorderSummary(recorder?: Snapshot["status"]["recorder"]): RecorderHealth {
  if (!recorder) {
    return { healthy: false, queueSize: "n/a", droppedCount: "n/a", errorCount: "n/a" };
  }
  const recorders = Array.isArray(recorder.recorders) ? (recorder.recorders as Record<string, unknown>[]) : [recorder];
  const azure = recorders.find((item) => item.type === "azure_storage") ?? recorders[0] ?? {};
  const errorCount = Number(azure.error_count ?? 0);
  const droppedCount = Number(azure.dropped_count ?? 0);
  return {
    healthy: Boolean(azure.worker_alive ?? true) && errorCount === 0 && droppedCount === 0,
    queueSize: numberText(azure.queue_size, 0),
    droppedCount: numberText(azure.dropped_count, 0),
    errorCount: numberText(azure.error_count, 0)
  };
}

export function statusClass(status: string) {
  const tone = status.includes("error") || status.includes("reject") || status.includes("missing")
    ? "danger"
    : status.includes("filled")
      ? "good"
      : status.includes("cancel")
        ? "warn"
        : "neutral";
  return [
    "inline-flex rounded-sm border px-2 py-1 text-xs font-semibold",
    tone === "danger" ? "border-danger/25 bg-danger/10 text-danger" : "",
    tone === "good" ? "border-good/25 bg-good/10 text-good" : "",
    tone === "warn" ? "border-warn/25 bg-warn/10 text-warn" : "",
    tone === "neutral" ? "border-line bg-panel text-ink/70" : ""
  ].join(" ");
}

export function reportOutcome(report: ExecutionReport, active: MarketSummary | null | undefined) {
  const rawDecision = report.raw?.decision;
  if (rawDecision && typeof rawDecision === "object" && "outcome" in rawDecision) {
    const outcome = stringValue((rawDecision as Record<string, unknown>).outcome);
    if (outcome) {
      return outcome.toUpperCase();
    }
  }
  return tokenOutcome(report.token_id ?? undefined, active);
}

export function moneyText(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toLocaleString(undefined, {
    maximumFractionDigits: 2,
    minimumFractionDigits: 0
  })}`;
}

export function sharePriceText(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return numeric.toFixed(2);
}

export function distanceBps(referencePrice?: string, startPrice?: string | null) {
  const reference = Number(referencePrice);
  const start = Number(startPrice);
  if (!Number.isFinite(reference) || !Number.isFinite(start) || start <= 0) {
    return undefined;
  }
  return (reference / start - 1) * 10000;
}

export function bpsText(value?: number) {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  return `${value! >= 0 ? "+" : ""}${value!.toFixed(1)} bps`;
}

export function distanceTone(value?: number): Tone {
  if (!Number.isFinite(value) || Math.abs(value!) < 1) {
    return "neutral";
  }
  return value! > 0 ? "good" : "danger";
}

export function timeRemaining(endTs: string) {
  const seconds = Math.max(0, Math.round((new Date(endTs).getTime() - Date.now()) / 1000));
  if (!Number.isFinite(seconds)) {
    return "window n/a";
  }
  if (seconds < 60) {
    return `${seconds}s left`;
  }
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s left`;
}

export function windowMeta(active: MarketSummary) {
  return `${dateTime(active.start_ts)} -> ${dateTime(active.end_ts)}`;
}

export function toneText(tone: Tone) {
  return {
    neutral: "text-ink",
    good: "text-good",
    warn: "text-warn",
    danger: "text-danger"
  }[tone];
}

export function toneDot(tone: Tone) {
  return [
    "inline-block h-2 w-2 shrink-0 rounded-full",
    tone === "good" ? "bg-good" : "",
    tone === "warn" ? "bg-warn" : "",
    tone === "danger" ? "bg-danger" : "",
    tone === "neutral" ? "bg-ink/35" : ""
  ].join(" ");
}

function eventToRow(event: RuntimeEvent, active: MarketSummary | null | undefined, index: number, tab: EventTab): TimelineRow | null {
  const severity = eventSeverity(event);
  const action = stringValue(event.data.action)?.toLowerCase();
  const status = stringValue(event.data.status);
  const isOrder = ["decision", "execution_report", "paper_fill"].includes(event.type);
  const isError = severity === "danger" || event.type.includes("error");
  const isHighlight =
    ["market_start_price", "paper_fill", "paper_settlement", "kill_switch_changed", "control_state_changed", "risk_block", "feed_error"].includes(
      event.type
    ) ||
    (event.type === "decision" && ["place", "cancel_all"].includes(action ?? "")) ||
    (event.type === "execution_report" && (status?.includes("filled") || status?.includes("error")));

  if (tab === "highlights" && !isHighlight) {
    return null;
  }
  if (tab === "orders" && !isOrder) {
    return null;
  }
  if (tab === "errors" && !isError) {
    return null;
  }

  return {
    key: `${event.ts}-${event.type}-${index}`,
    tab,
    severity,
    ts: event.ts,
    title: eventTitle(event, active),
    message: eventMessage(event, active),
    raw: event
  };
}

function coalescedBookRows(events: RuntimeEvent[], active: MarketSummary | null | undefined) {
  const seen = new Set<string>();
  const rows: TimelineRow[] = [];
  events.forEach((event, index) => {
    if (event.type !== "book_update_summary") {
      return;
    }
    const outcome = tokenOutcome(stringValue(event.data.token_id), active);
    if (outcome === "n/a") {
      return;
    }
    const second = Math.floor(new Date(event.ts).getTime() / 1000);
    const key = `${outcome}-${second}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    const bid = bookPrice(event.data.best_bid);
    const ask = bookPrice(event.data.best_ask);
    rows.push({
      key: `${event.ts}-${index}`,
      tab: "market",
      severity: "neutral",
      ts: event.ts,
      title: `${outcome} book`,
      message: `bid ${sharePriceText(bid)} / ask ${sharePriceText(ask)} / spread ${spreadText(bid, ask)}`,
      raw: event
    });
  });
  return rows;
}

function eventTitle(event: RuntimeEvent, active: MarketSummary | null | undefined) {
  if (event.type === "decision") {
    const action = stringValue(event.data.action)?.toUpperCase() ?? "DECISION";
    const outcome = decisionOutcome(event.data, active);
    return `${action}${outcome !== "n/a" ? ` ${outcome}` : ""}`;
  }
  if (event.type === "execution_report" || event.type === "paper_fill") {
    const outcome = tokenOutcome(stringValue(event.data.token_id), active);
    return event.type === "paper_fill" ? `Paper maker fill ${outcome}` : `Execution ${compact(event.data.status)}`;
  }
  if (event.type === "reference_update") {
    return "Reference update";
  }
  if (event.type === "fair_value_update") {
    return "Fair value";
  }
  return titleCase(event.type.replaceAll("_", " "));
}

function eventMessage(event: RuntimeEvent, active: MarketSummary | null | undefined) {
  if (event.type === "decision") {
    const outcome = decisionOutcome(event.data, active);
    const price = sharePriceText(event.data.price);
    const size = compact(event.data.size);
    const edge = sharePriceText(event.data.expected_edge);
    return [
      outcome !== "n/a" ? outcome : null,
      price !== "n/a" ? `@ ${price}` : null,
      size !== "n/a" ? `size ${size}` : null,
      edge !== "n/a" ? `edge ${edge}` : null,
      stringValue(event.data.reason)
    ]
      .filter(Boolean)
      .join(" · ");
  }
  if (event.type === "paper_fill" || event.type === "execution_report") {
    const outcome = tokenOutcome(stringValue(event.data.token_id), active);
    return `${outcome} ${compact(event.data.filled_size, "0")} @ ${sharePriceText(event.data.avg_price)} · ${compact(event.data.status)}`;
  }
  if (event.type === "reference_update") {
    return `${stringValue(event.data.source) ?? "reference"} · $${numberText(event.data.price, 2)}`;
  }
  if (event.type === "fair_value_update") {
    return `q Up ${pctText(event.data.q_up)} / q Down ${pctText(event.data.q_down)}`;
  }
  if (event.type === "control_state_changed") {
    return Boolean(event.data.paused) ? `Paused · ${compact(event.data.pause_reason)}` : "Running";
  }
  return stringValue(event.data.message) ?? stringValue(event.data.reason) ?? compact(event.data.status, JSON.stringify(event.data).slice(0, 120));
}

function eventSeverity(event: RuntimeEvent): Tone {
  const status = stringValue(event.data.status)?.toLowerCase() ?? "";
  const reason = stringValue(event.data.reason)?.toLowerCase() ?? "";
  if (event.type.includes("error") || status.includes("error") || status.includes("reject") || status.includes("missing")) {
    return "danger";
  }
  if (event.type.includes("risk") || reason.includes("stale") || event.type.includes("heartbeat")) {
    return "warn";
  }
  if (event.type === "paper_fill" || status.includes("filled")) {
    return "good";
  }
  return "neutral";
}

function decisionOutcome(data: Record<string, unknown>, active: MarketSummary | null | undefined) {
  return stringValue(data.outcome)?.toUpperCase() ?? tokenOutcome(stringValue(data.token_id), active);
}

function tokenOutcome(tokenId: string | undefined | null, active: MarketSummary | null | undefined) {
  if (!tokenId || !active) {
    return "n/a";
  }
  if (tokenId === active.up_token_id) {
    return "UP";
  }
  if (tokenId === active.down_token_id) {
    return "DOWN";
  }
  return "n/a";
}

function bookPrice(value: unknown) {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  return finiteNumber((value as Record<string, unknown>).price);
}

function spreadText(bid?: number, ask?: number) {
  if (bid === undefined || ask === undefined) {
    return "n/a";
  }
  return sharePriceText(Math.max(0, ask - bid));
}

function finiteNumber(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : undefined;
}

function stringValue(value: unknown) {
  return typeof value === "string" ? value : undefined;
}

function titleCase(value: string) {
  return value.replace(/\b\w/g, (match) => match.toUpperCase());
}
