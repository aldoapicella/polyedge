"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  CircleDot,
  FileText,
  PauseCircle,
  PlayCircle,
  Power,
  Radio,
  RefreshCw,
  Search,
  ShieldAlert,
  TrendingUp,
  XCircle
} from "lucide-react";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { buildReport, getLatestReport, getSnapshot, pauseBot, resumeBot, setKillSwitch } from "@/lib/api";
import type { ExecutionReport, MarketSummary, RuntimeEvent, Snapshot, TradeDecision } from "@/lib/types";
import { ageText, compact, dateTime, numberText, pctText } from "@/lib/format";
import {
  buildMarketSeries,
  formatChartTime,
  MARKET_EVENT_BUFFER_LIMIT,
  type ChartPoint
} from "@/lib/charting";
import { Button, EmptyState, IconButton, InfoHint, Panel, PanelHeader, Pill } from "@/components/ui";

const TIMELINE_LIMIT = 160;

type Tone = "neutral" | "good" | "warn" | "danger";
type EventTab = "highlights" | "orders" | "market" | "errors" | "raw";
type ExecutionFilter = "all" | "fills" | "resting" | "cancelled" | "errors";

type TimelineRow = {
  key: string;
  tab: EventTab;
  severity: Tone;
  ts: string;
  title: string;
  message: string;
  raw: RuntimeEvent;
};

type CollapsedDecision = {
  key: string;
  action: string;
  outcome: string;
  price: string;
  size: string;
  edge: string;
  reason: string;
  count: number;
};

export function Dashboard() {
  const queryClient = useQueryClient();
  const snapshot = useQuery({
    queryKey: ["snapshot"],
    queryFn: getSnapshot,
    refetchInterval: 10000
  });
  const latestReport = useQuery({
    queryKey: ["reports", "latest"],
    queryFn: getLatestReport,
    retry: false,
    refetchInterval: 30000
  });
  const [eventTapeStore, setEventTapeStore] = useState<RuntimeEvent[]>([]);
  const eventBufferRef = useRef<RuntimeEvent[]>([]);
  const pendingSnapshotRef = useRef<Snapshot | null>(null);
  const pendingRefreshRef = useRef(false);

  useEffect(() => {
    const stream = new EventSource("/api/realtime");
    const flush = window.setInterval(() => {
      setEventTapeStore([...eventBufferRef.current]);
      if (pendingSnapshotRef.current) {
        queryClient.setQueryData(["snapshot"], pendingSnapshotRef.current);
        pendingSnapshotRef.current = null;
      }
      if (pendingRefreshRef.current) {
        queryClient.invalidateQueries({ queryKey: ["snapshot"] });
        pendingRefreshRef.current = false;
      }
    }, 1000);

    stream.onmessage = (message) => {
      const event = JSON.parse(message.data) as RuntimeEvent;
      eventBufferRef.current = [event, ...eventBufferRef.current].slice(0, MARKET_EVENT_BUFFER_LIMIT);
      if (event.type === "status_snapshot" || event.type === "ui_snapshot") {
        pendingSnapshotRef.current = event.data as unknown as Snapshot;
      }
      if (
        [
          "paper_fill",
          "paper_settlement",
          "kill_switch_changed",
          "control_state_changed",
          "config_changed",
          "report_job_update",
          "execution_report"
        ].includes(event.type)
      ) {
        pendingRefreshRef.current = true;
      }
    };
    stream.onerror = () => undefined;
    return () => {
      window.clearInterval(flush);
      stream.close();
    };
  }, [queryClient]);

  const snapshotStore = snapshot.data;
  const status = snapshotStore?.status;
  const active = snapshotStore?.current_market;
  const reference = status?.reference;
  const reportSummary = latestReport.data?.report?.summary;
  const killSwitchOn = Boolean(status?.kill_switch);
  const paused = Boolean(status?.control?.paused);
  const recorder = recorderSummary(status?.recorder);
  const seriesStore = useMemo(
    () => buildMarketSeries({ snapshot: snapshotStore, events: eventTapeStore }),
    [snapshotStore, eventTapeStore]
  );

  return (
    <div className="space-y-5">
      <DashboardHeader
        mode={status?.execution_mode}
        referenceFresh={!reference?.stale}
        recorderHealthy={recorder.healthy}
        onRefresh={() => queryClient.invalidateQueries({ queryKey: ["snapshot"] })}
      />

      <SystemHealthCards
        status={status}
        reportSummary={reportSummary}
        recorder={recorder}
        killSwitchOn={killSwitchOn}
        paused={paused}
      />

      <ControlPanel
        killSwitchOn={killSwitchOn}
        paused={paused}
        reportPending={latestReport.isFetching}
        onAfterAction={() => {
          queryClient.invalidateQueries({ queryKey: ["snapshot"] });
          queryClient.invalidateQueries({ queryKey: ["reports", "latest"] });
        }}
      />

      <div className="grid gap-5 xl:grid-cols-12">
        <ActiveMarketPanel
          active={active}
          referencePrice={reference?.price}
          referenceAge={ageText(reference?.local_ts)}
          isLoading={snapshot.isLoading}
        />
        <MarketMainChart points={seriesStore.marketChart} domain={seriesStore.domain} sampleCount={seriesStore.sampleCount} />
      </div>

      <TrendCharts points={seriesStore.marketChart} fills={seriesStore.fills} domain={seriesStore.domain} />

      <div className="grid gap-5 xl:grid-cols-12">
        <div className="xl:col-span-5">
          <DecisionTable decisions={snapshotStore?.latest_decisions ?? []} />
        </div>
        <div className="xl:col-span-7">
          <EventTimeline events={eventTapeStore} active={active} />
        </div>
      </div>

      <ExecutionReportTable reports={snapshotStore?.latest_execution_reports ?? []} active={active} />
    </div>
  );
}

function DashboardHeader({
  mode,
  referenceFresh,
  recorderHealthy,
  onRefresh
}: {
  mode?: string;
  referenceFresh: boolean;
  recorderHealthy: boolean;
  onRefresh: () => void;
}) {
  return (
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h1 className="text-2xl font-semibold text-ink">Operations Dashboard</h1>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Pill tone={mode === "live" ? "danger" : "good"}>{mode ?? "unknown"}</Pill>
        <Pill tone={referenceFresh ? "good" : "warn"}>{referenceFresh ? "reference fresh" : "reference stale"}</Pill>
        <Pill tone={recorderHealthy ? "good" : "danger"}>{recorderHealthy ? "recorder healthy" : "recorder issue"}</Pill>
        <IconButton label="Refresh snapshot" onClick={onRefresh}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>
    </div>
  );
}

function SystemHealthCards({
  status,
  reportSummary,
  recorder,
  killSwitchOn,
  paused
}: {
  status?: Snapshot["status"];
  reportSummary?: Record<string, unknown>;
  recorder: ReturnType<typeof recorderSummary>;
  killSwitchOn: boolean;
  paused: boolean;
}) {
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
      <MetricCard
        icon={<ShieldAlert className="h-4 w-4" />}
        label="Kill Switch"
        value={killSwitchOn ? "Active" : "Clear"}
        tone={killSwitchOn ? "danger" : "good"}
        sublabel="backend control file"
        help="Emergency control state. When active, the backend should refuse trading actions."
      />
      <MetricCard
        icon={paused ? <PauseCircle className="h-4 w-4" /> : <PlayCircle className="h-4 w-4" />}
        label="Loop State"
        value={paused ? "Paused" : "Running"}
        tone={paused ? "warn" : "good"}
        sublabel={status?.control?.pause_reason ?? "operator control"}
        help="Operator pause/resume state for the bot loop. This is separate from the kill switch."
      />
      <MetricCard
        icon={<Radio className="h-4 w-4" />}
        label="Reference"
        value={status?.reference ? `$${numberText(status.reference.price, 2)}` : "n/a"}
        tone={status?.reference?.stale ? "warn" : "good"}
        sublabel={`${ageText(status?.reference?.local_ts)} · ${compact(status?.reference?.source, "no source")}`}
        help="Latest reference price used to value the active market, with freshness shown in the subtitle."
      />
      <MetricCard
        icon={<CircleDot className="h-4 w-4" />}
        label="Open Orders"
        value={numberText(status?.tracked_open_orders, 0)}
        tone={(status?.tracked_open_orders ?? 0) > 0 ? "warn" : "neutral"}
        sublabel={`${numberText(status?.paper_fill?.paper_open_resting_orders, 0)} resting · ${numberText(
          status?.paper_fill?.paper_maker_fills,
          0
        )} fills`}
        help="Tracked open orders are known to the order manager. Resting paper orders are simulated maker quotes waiting for eligible fills."
      />
      <MetricCard
        icon={<TrendingUp className="h-4 w-4" />}
        label="Runtime PnL"
        value={moneyText(reportSummary?.actual_paper_net_pnl)}
        tone={Number(reportSummary?.actual_paper_net_pnl ?? 0) < 0 ? "danger" : "neutral"}
        sublabel={`Replay ${moneyText(reportSummary?.replay_estimate_net_pnl)}`}
        help="Runtime paper PnL comes from simulated fills during operation. Replay PnL is the offline estimate from report generation."
      />
      <MetricCard
        icon={<BarChart3 className="h-4 w-4" />}
        label="Recorder"
        value={recorder.healthy ? "Healthy" : "Issue"}
        tone={recorder.healthy ? "good" : "danger"}
        sublabel={`queue ${recorder.queueSize} · drops ${recorder.droppedCount}`}
        help="Recorder health reflects event persistence status. Queue and drops indicate whether events are backing up or being lost."
      />
    </div>
  );
}

function MetricCard({
  icon,
  label,
  value,
  sublabel,
  tone,
  help
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  sublabel: string;
  tone: Tone;
  help?: string;
}) {
  return (
    <Panel className="p-3">
      <div className="flex items-center gap-3">
        <span className="grid h-9 w-9 shrink-0 place-items-center border border-line bg-panel text-ink/70">{icon}</span>
        <div className="min-w-0">
          <div className="flex min-w-0 items-center gap-1">
            <span className="truncate text-[11px] font-semibold uppercase text-ink/50">{label}</span>
            {help ? <InfoHint label={help} /> : null}
          </div>
          <div className="mt-1 flex min-w-0 items-center gap-2">
            <span className="truncate text-lg font-semibold text-ink">{value}</span>
            <span className={toneDot(tone)} aria-hidden />
          </div>
          <div className="mt-1 truncate text-xs text-ink/55">{sublabel}</div>
        </div>
      </div>
    </Panel>
  );
}

function ControlPanel({
  killSwitchOn,
  paused,
  reportPending,
  onAfterAction
}: {
  killSwitchOn: boolean;
  paused: boolean;
  reportPending: boolean;
  onAfterAction: () => void;
}) {
  const [confirmOpen, setConfirmOpen] = useState<"kill-switch" | null>(null);
  const today = new Date().toISOString().slice(0, 10);
  const killSwitch = useMutation({
    mutationFn: () => setKillSwitch(!killSwitchOn, killSwitchOn ? "UI disabled kill switch" : "UI enabled kill switch"),
    onSuccess: () => {
      setConfirmOpen(null);
      onAfterAction();
    }
  });
  const pauseResume = useMutation({
    mutationFn: () => (paused ? resumeBot("operator resume") : pauseBot("operator pause")),
    onSuccess: onAfterAction
  });
  const reportBuild = useMutation({
    mutationFn: () => buildReport({ source: "azure", date: today, force: false }),
    onSuccess: onAfterAction
  });

  return (
    <Panel className="p-4">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2 className="text-sm font-semibold text-ink">Control Panel</h2>
          <p className="mt-1 text-xs text-ink/55">Operator actions are audited. Live gates remain backend-only.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button tone={paused ? "good" : "neutral"} disabled={pauseResume.isPending} onClick={() => pauseResume.mutate()}>
            {paused ? <PlayCircle className="h-4 w-4" /> : <PauseCircle className="h-4 w-4" />}
            {paused ? "Resume Bot" : "Pause Bot"}
          </Button>
          <Button tone={killSwitchOn ? "good" : "danger"} onClick={() => setConfirmOpen("kill-switch")}>
            <Power className="h-4 w-4" />
            {killSwitchOn ? "Disable Kill Switch" : "Enable Kill Switch"}
          </Button>
          <Button disabled={reportBuild.isPending || reportPending} onClick={() => reportBuild.mutate()}>
            <FileText className="h-4 w-4" />
            Build Report
          </Button>
        </div>
      </div>
      {pauseResume.error ? <p className="mt-3 text-xs text-danger">{pauseResume.error.message}</p> : null}
      {reportBuild.error ? <p className="mt-3 text-xs text-danger">{reportBuild.error.message}</p> : null}
      {confirmOpen ? (
        <div className="mt-4 border border-line bg-panel p-3">
          <p className="text-sm font-semibold text-ink">
            {killSwitchOn ? "Disable the kill switch?" : "Enable the kill switch?"}
          </p>
          <p className="mt-1 text-xs text-ink/60">This writes backend control state and creates an audit entry.</p>
          {killSwitch.error ? <p className="mt-2 text-xs text-danger">{killSwitch.error.message}</p> : null}
          <div className="mt-3 flex gap-2">
            <Button tone={killSwitchOn ? "good" : "danger"} disabled={killSwitch.isPending} onClick={() => killSwitch.mutate()}>
              Confirm
            </Button>
            <Button disabled={killSwitch.isPending} onClick={() => setConfirmOpen(null)}>
              Cancel
            </Button>
          </div>
        </div>
      ) : null}
    </Panel>
  );
}

function ActiveMarketPanel({
  active,
  referencePrice,
  referenceAge,
  isLoading
}: {
  active?: MarketSummary | null;
  referencePrice?: string;
  referenceAge: string;
  isLoading: boolean;
}) {
  const distance = distanceBps(referencePrice, active?.start_price);
  return (
    <Panel className="xl:col-span-4">
      <PanelHeader
        title="Active Market"
        meta={active ? windowMeta(active) : "No active market"}
        help="The current crypto Up/Down market window selected by discovery and used by the strategy."
      />
      {active ? (
        <div className="space-y-4 p-4">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <Pill tone={active.is_tradeable ? "good" : "warn"}>{active.status}</Pill>
              <Pill>{timeRemaining(active.end_ts)}</Pill>
            </div>
            <Link
              href={`/markets/${encodeURIComponent(active.market_id)}`}
              className="block text-base font-semibold leading-snug text-ink hover:underline"
            >
              {active.question}
            </Link>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
            <Field label="Start Price" value={`$${numberText(active.start_price, 2)}`} />
            <Field label="Chainlink" value={`$${numberText(referencePrice, 2)}`} sublabel={referenceAge} />
            <Field
              label="Distance"
              value={bpsText(distance)}
              tone={distanceTone(distance)}
              help="Reference price move from the market start price, measured in basis points."
            />
            <Field label="Market Status" value={active.is_tradeable ? "Tradeable" : active.status} />
            <Field label="q Up" value={pctText(active.fair_value?.q_up)} tone="good" help="Model-implied probability that the market resolves Up." />
            <Field label="q Down" value={pctText(active.fair_value?.q_down)} tone="danger" help="Model-implied probability that the market resolves Down." />
          </div>
        </div>
      ) : (
        <EmptyState label={isLoading ? "Loading snapshot" : "No active market in the current snapshot"} />
      )}
    </Panel>
  );
}

function Field({
  label,
  value,
  sublabel,
  tone = "neutral",
  help
}: {
  label: string;
  value: string;
  sublabel?: string;
  tone?: Tone;
  help?: string;
}) {
  return (
    <div className="border border-line bg-panel px-3 py-2">
      <div className="flex items-center gap-1 text-[11px] font-semibold uppercase text-ink/50">
        <span>{label}</span>
        {help ? <InfoHint label={help} /> : null}
      </div>
      <div className={["mt-1 truncate text-xl font-semibold", toneText(tone)].join(" ")}>{value}</div>
      {sublabel ? <div className="mt-1 truncate text-xs text-ink/50">{sublabel}</div> : null}
    </div>
  );
}

function MarketMainChart({ points, domain, sampleCount }: { points: ChartPoint[]; domain: [number, number]; sampleCount: number }) {
  return (
    <Panel className="xl:col-span-8">
      <PanelHeader
        title="Market Probability & Price"
        meta={`${points.length} visible · ${sampleCount} market-window samples`}
        help="Full active-market window. Lines update in place; the x-axis stays pinned to market start and end."
      >
        <div className="hidden flex-wrap gap-2 md:flex">
          <MiniLegend tone="good" label="q up" />
          <MiniLegend tone="danger" label="q down" />
          <MiniLegend tone="neutral" label="bid/ask" />
        </div>
      </PanelHeader>
      <div className="h-[400px] p-3">
        {points.length ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={points}>
              <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
              <XAxis
                dataKey="bucket"
                type="number"
                domain={domain}
                tick={{ fontSize: 11 }}
                minTickGap={28}
                tickFormatter={formatChartTime}
              />
              <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={36} />
              <Tooltip formatter={(value) => numberText(value, 3)} />
              <Legend />
              <Line type="monotone" dataKey="qUp" name="q Up" stroke="#18705b" dot={false} strokeWidth={2.4} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="qDown" name="q Down" stroke="#b3363a" dot={false} strokeWidth={2.4} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="upBid" name="UP bid" stroke="#2f7fcb" dot={false} strokeWidth={1.5} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="upAsk" name="UP ask" stroke="#74a8dd" dot={false} strokeWidth={1.5} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="downBid" name="DOWN bid" stroke="#a45d13" dot={false} strokeWidth={1.5} connectNulls isAnimationActive={false} />
              <Line type="monotone" dataKey="downAsk" name="DOWN ask" stroke="#d49a4e" dot={false} strokeWidth={1.5} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <EmptyState label="No probability or book samples yet" />
        )}
      </div>
    </Panel>
  );
}

function TrendCharts({ points, fills, domain }: { points: ChartPoint[]; fills: ChartPoint[]; domain: [number, number] }) {
  return (
    <div className="grid gap-5 xl:grid-cols-3">
      <ChartPanel title="Probability Trend" meta={`${points.length} buckets`} empty="No probability samples" help="Model probability and UP quote levels over the visible market window.">
        <LineChart data={points}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 3)} />
          <Line type="monotone" dataKey="qUp" name="q Up" stroke="#18705b" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="qDown" name="q Down" stroke="#b3363a" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="upBid" name="UP bid" stroke="#2f7fcb" dot={false} strokeWidth={1.3} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="upAsk" name="UP ask" stroke="#74a8dd" dot={false} strokeWidth={1.3} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel title="Reference Distance" meta="bps from start" empty="No reference samples" help="Reference price move from the market start price. Positive means above start, negative means below start.">
        <LineChart data={points.filter((point) => Number.isFinite(point.distanceBps))}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis tick={{ fontSize: 11 }} width={42} tickFormatter={(value) => `${value}`} />
          <Tooltip formatter={(value) => `${numberText(value, 1)} bps`} />
          <ReferenceLine y={0} stroke="#17201b" strokeOpacity={0.35} />
          <Line type="monotone" dataKey="distanceBps" name="distance" stroke="#18705b" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel title="Paper Fills" meta={`${fills.length} fills`} empty="No paper fills yet" help="Simulated paper maker fills plotted at fill price inside the market window.">
        <ScatterChart data={fills}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis dataKey="fillPrice" domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 2)} />
          <Scatter dataKey="fillPrice" name="fills" fill="#a45d13" isAnimationActive={false} />
        </ScatterChart>
      </ChartPanel>
    </div>
  );
}

function ChartPanel({
  title,
  meta,
  empty,
  help,
  children
}: {
  title: string;
  meta: string;
  empty: string;
  help?: string;
  children: React.ReactElement;
}) {
  const data = (children.props as { data?: unknown[] }).data ?? [];
  return (
    <Panel>
      <PanelHeader title={title} meta={meta} help={help} />
      <div className="h-64 p-3">
        {data.length ? <ResponsiveContainer width="100%" height="100%">{children}</ResponsiveContainer> : <EmptyState label={empty} />}
      </div>
    </Panel>
  );
}

function EventTimeline({ events, active }: { events: RuntimeEvent[]; active?: MarketSummary | null }) {
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
    <Panel>
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

function DecisionTable({ decisions }: { decisions: TradeDecision[] }) {
  const rows = useMemo(() => collapseDecisions(decisions).slice(0, 14), [decisions]);
  return (
    <Panel>
      <PanelHeader
        title="Decisions"
        meta={`${rows.length} grouped rows`}
        help="Recent strategy decisions. Consecutive HOLD rows with the same reason are collapsed to reduce noise."
      />
      <div className="overflow-auto">
        <table className="w-full min-w-[760px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-[11px] uppercase text-ink/50">
            <tr>
              <th className="px-3 py-2">Action</th>
              <th className="px-3 py-2">Outcome</th>
              <th className="px-3 py-2">Price</th>
              <th className="px-3 py-2">Size</th>
              <th className="px-3 py-2">Edge</th>
              <th className="px-3 py-2">Reason</th>
              <th className="px-3 py-2">Count</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((row) => (
                <tr key={row.key} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-semibold">{row.action}</td>
                  <td className="px-3 py-2">{row.outcome}</td>
                  <td className="px-3 py-2">{row.price}</td>
                  <td className="px-3 py-2">{row.size}</td>
                  <td className="px-3 py-2">{row.edge}</td>
                  <td className="max-w-[360px] truncate px-3 py-2 text-ink/65">{row.reason}</td>
                  <td className="px-3 py-2">{row.count > 1 ? `x${row.count}` : "1"}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={7}>
                  <EmptyState label="No decisions in snapshot" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function ExecutionReportTable({ reports, active }: { reports: ExecutionReport[]; active?: MarketSummary | null }) {
  const [filter, setFilter] = useState<ExecutionFilter>("all");
  const rows = useMemo(() => filterReports(reports, filter).slice(0, 40), [filter, reports]);
  return (
    <Panel>
      <PanelHeader
        title="Execution Reports"
        meta={`${rows.length} shown`}
        help="Recent paper/live execution adapter reports. In paper mode these are simulated execution outcomes."
      >
        <div className="flex flex-wrap gap-2">
          {EXECUTION_FILTERS.map((item) => (
            <button
              key={item.value}
              className={[
                "h-8 rounded-sm border px-3 text-xs font-semibold transition",
                filter === item.value ? "border-good bg-good text-white" : "border-line bg-white text-ink/65 hover:bg-panel"
              ].join(" ")}
              onClick={() => setFilter(item.value)}
            >
              {item.label}
            </button>
          ))}
        </div>
      </PanelHeader>
      <div className="overflow-auto">
        <table className="w-full min-w-[840px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-[11px] uppercase text-ink/50">
            <tr>
              <th className="px-3 py-2">Status</th>
              <th className="px-3 py-2">Outcome</th>
              <th className="px-3 py-2">Filled</th>
              <th className="px-3 py-2">Avg</th>
              <th className="px-3 py-2">Fee</th>
              <th className="px-3 py-2">Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((row, index) => (
                <tr key={`${row.order_id}-${index}`} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2">
                    <span className={statusClass(row.status)}>{row.status}</span>
                  </td>
                  <td className="px-3 py-2">{reportOutcome(row, active)}</td>
                  <td className="px-3 py-2">{compact(row.filled_size)}</td>
                  <td className="px-3 py-2">{sharePriceText(row.avg_price)}</td>
                  <td className="px-3 py-2">{compact(row.fee)}</td>
                  <td className="px-3 py-2 text-ink/60">{dateTime(row.local_ts)}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={6}>
                  <EmptyState label="No execution reports match this filter" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

const EVENT_TABS: { value: EventTab; label: string }[] = [
  { value: "highlights", label: "Highlights" },
  { value: "orders", label: "Orders" },
  { value: "market", label: "Market Data" },
  { value: "errors", label: "Errors" },
  { value: "raw", label: "Raw" }
];

const EXECUTION_FILTERS: { value: ExecutionFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "fills", label: "Fills" },
  { value: "resting", label: "Resting" },
  { value: "cancelled", label: "Cancelled" },
  { value: "errors", label: "Errors" }
];

function timelineRows(events: RuntimeEvent[], active: MarketSummary | null | undefined, tab: EventTab, search: string) {
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

function collapseDecisions(decisions: TradeDecision[]): CollapsedDecision[] {
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

function filterReports(reports: ExecutionReport[], filter: ExecutionFilter) {
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
    return [outcome !== "n/a" ? outcome : null, price !== "n/a" ? `@ ${price}` : null, size !== "n/a" ? `size ${size}` : null, edge !== "n/a" ? `edge ${edge}` : null, stringValue(event.data.reason)]
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

function recorderSummary(recorder?: Record<string, unknown> | null) {
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

function statusClass(status: string) {
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

function reportOutcome(report: ExecutionReport, active: MarketSummary | null | undefined) {
  const rawDecision = report.raw?.decision;
  if (rawDecision && typeof rawDecision === "object" && "outcome" in rawDecision) {
    const outcome = stringValue((rawDecision as Record<string, unknown>).outcome);
    if (outcome) {
      return outcome.toUpperCase();
    }
  }
  return tokenOutcome(report.token_id ?? undefined, active);
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

function moneyText(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toLocaleString(undefined, {
    maximumFractionDigits: 2,
    minimumFractionDigits: 0
  })}`;
}

function sharePriceText(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return numeric.toFixed(2);
}

function distanceBps(referencePrice?: string, startPrice?: string | null) {
  const reference = Number(referencePrice);
  const start = Number(startPrice);
  if (!Number.isFinite(reference) || !Number.isFinite(start) || start <= 0) {
    return undefined;
  }
  return ((reference / start) - 1) * 10000;
}

function bpsText(value?: number) {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  return `${value! >= 0 ? "+" : ""}${value!.toFixed(1)} bps`;
}

function distanceTone(value?: number): Tone {
  if (!Number.isFinite(value) || Math.abs(value!) < 1) {
    return "neutral";
  }
  return value! > 0 ? "good" : "danger";
}

function timeRemaining(endTs: string) {
  const seconds = Math.max(0, Math.round((new Date(endTs).getTime() - Date.now()) / 1000));
  if (!Number.isFinite(seconds)) {
    return "window n/a";
  }
  if (seconds < 60) {
    return `${seconds}s left`;
  }
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s left`;
}

function windowMeta(active: MarketSummary) {
  return `${dateTime(active.start_ts)} -> ${dateTime(active.end_ts)}`;
}

function titleCase(value: string) {
  return value.replace(/\b\w/g, (match) => match.toUpperCase());
}

function toneText(tone: Tone) {
  return {
    neutral: "text-ink",
    good: "text-good",
    warn: "text-warn",
    danger: "text-danger"
  }[tone];
}

function toneDot(tone: Tone) {
  return [
    "inline-block h-2 w-2 shrink-0 rounded-full",
    tone === "good" ? "bg-good" : "",
    tone === "warn" ? "bg-warn" : "",
    tone === "danger" ? "bg-danger" : "",
    tone === "neutral" ? "bg-ink/35" : ""
  ].join(" ");
}

function MiniLegend({ tone, label }: { tone: Tone; label: string }) {
  return (
    <span className="inline-flex items-center gap-1 text-xs text-ink/55">
      <span className={toneDot(tone)} aria-hidden />
      {label}
    </span>
  );
}
