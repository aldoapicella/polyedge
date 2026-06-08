import type { RuntimeEvent } from "@/lib/types";

export const TIMELINE_LIMIT = 160;

export type Tone = "neutral" | "good" | "warn" | "danger";
export type EventTab = "highlights" | "orders" | "market" | "errors" | "raw";
export type ExecutionFilter = "all" | "fills" | "resting" | "cancelled" | "errors";

export type TimelineRow = {
  key: string;
  tab: EventTab;
  severity: Tone;
  ts: string;
  title: string;
  message: string;
  raw: RuntimeEvent;
};

export type CollapsedDecision = {
  key: string;
  action: string;
  outcome: string;
  price: string;
  size: string;
  edge: string;
  reason: string;
  count: number;
};

export type RecorderHealth = {
  healthy: boolean;
  queueSize: string;
  droppedCount: string;
  errorCount: string;
};
