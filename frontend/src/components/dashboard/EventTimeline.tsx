"use client";

import { PauseCircle, PlayCircle, Search } from "lucide-react";
import { useMemo, useState } from "react";
import { dateTime } from "@/lib/format";
import type { MarketSummary, RuntimeEvent } from "@/lib/types";
import { Button, EmptyState, Panel, PanelHeader } from "@/components/ui";
import { EVENT_TABS, timelineRows, toneDot } from "./model";
import { TIMELINE_LIMIT, type EventTab, type TimelineRow } from "./types";

export function EventTimeline({ events, active }: { events: RuntimeEvent[]; active?: MarketSummary | null }) {
  const [tab, setTab] = useState<EventTab>("highlights");
  const [paused, setPaused] = useState(false);
  const [clearedAt, setClearedAt] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const visibleEvents = useMemo(() => {
    if (!clearedAt) {
      return events;
    }
    const cutoff = new Date(clearedAt).getTime();
    return events.filter((event) => new Date(event.ts).getTime() > cutoff);
  }, [clearedAt, events]);
  const rows = useMemo(() => {
    if (paused) {
      return [];
    }
    return timelineRows(visibleEvents, active, tab, search).slice(0, TIMELINE_LIMIT);
  }, [active, paused, search, tab, visibleEvents]);

  return (
    <Panel className="min-w-0">
      <PanelHeader
        title="Event Timeline"
        meta={paused ? "paused" : `${rows.length} rows`}
        help="Live runtime events from the backend stream. Market Data coalesces book updates to one row per outcome per second."
      >
        <Button className="h-8 px-2 text-xs" onClick={() => setPaused((value) => !value)}>
          {paused ? <PlayCircle className="h-3.5 w-3.5" /> : <PauseCircle className="h-3.5 w-3.5" />}
          {paused ? "Resume" : "Pause"}
        </Button>
      </PanelHeader>
      <div className="space-y-3 p-3">
        <div className="flex flex-wrap gap-2">
          {EVENT_TABS.map((item) => (
            <button
              key={item.value}
              className={[
                "h-8 rounded-sm border px-3 text-xs font-semibold transition",
                tab === item.value ? "border-good bg-good text-white" : "border-line bg-white text-ink/65 hover:bg-panel"
              ].join(" ")}
              onClick={() => setTab(item.value)}
            >
              {item.label}
            </button>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <label className="flex h-9 min-w-[220px] flex-1 items-center gap-2 border border-line bg-white px-3 text-sm">
            <Search className="h-4 w-4 text-ink/40" />
            <input
              className="min-w-0 flex-1 border-0 bg-transparent text-sm outline-none"
              placeholder="Search events"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
          </label>
          <Button className="h-9 px-3" onClick={() => setClearedAt(new Date().toISOString())}>
            Clear
          </Button>
        </div>
        <div className="max-h-[460px] overflow-auto border border-line bg-panel">
          {paused ? (
            <EmptyState label="Timeline paused" />
          ) : rows.length ? (
            rows.map((row) => <TimelineItem key={row.key} row={row} showRaw={tab === "raw"} />)
          ) : (
            <EmptyState label="No matching events" />
          )}
        </div>
      </div>
    </Panel>
  );
}

function TimelineItem({ row, showRaw }: { row: TimelineRow; showRaw: boolean }) {
  return (
    <div className="border-b border-line bg-white px-3 py-2 last:border-b-0">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <span className={toneDot(row.severity)} aria-hidden />
          <span className="truncate text-sm font-semibold text-ink">{row.title}</span>
        </div>
        <span className="shrink-0 text-xs text-ink/45">{dateTime(row.ts)}</span>
      </div>
      <p className="mt-1 text-sm text-ink/65">{row.message}</p>
      {showRaw ? (
        <details className="mt-2">
          <summary className="cursor-pointer text-xs font-semibold text-ink/55">Raw JSON</summary>
          <pre className="mt-2 max-h-64 overflow-auto border border-line bg-panel p-2 text-xs text-ink/70">
            {JSON.stringify(row.raw.data, null, 2)}
          </pre>
        </details>
      ) : null}
    </div>
  );
}
