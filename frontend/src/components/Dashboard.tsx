"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  CheckCircle2,
  CircleDot,
  PauseCircle,
  PlayCircle,
  Power,
  Radio,
  RefreshCw,
  ShieldAlert,
  TrendingUp
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import Link from "next/link";
import { getLatestReport, getSnapshot, pauseBot, resumeBot, setKillSwitch } from "@/lib/api";
import type { RuntimeEvent, Snapshot } from "@/lib/types";
import { ageText, compact, dateTime, numberText, pctText } from "@/lib/format";
import { Button, EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

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
  const [events, setEvents] = useState<RuntimeEvent[]>([]);

  useEffect(() => {
    const stream = new EventSource("/api/realtime");
    stream.onmessage = (message) => {
      const event = JSON.parse(message.data) as RuntimeEvent;
      setEvents((current) => [event, ...current].slice(0, 80));
      if (event.type === "status_snapshot") {
        queryClient.setQueryData(["snapshot"], event.data as Snapshot);
      }
      if (
        ["reference_update", "fair_value_update", "paper_fill", "paper_settlement", "kill_switch_changed", "control_state_changed", "config_changed"].includes(
          event.type
        )
      ) {
        queryClient.invalidateQueries({ queryKey: ["snapshot"] });
      }
    };
    stream.onerror = () => undefined;
    return () => stream.close();
  }, [queryClient]);

  const data = snapshot.data;
  const status = data?.status;
  const active = data?.current_market;
  const reportSummary = latestReport.data?.report?.summary;
  const qChart = useMemo(() => fairValueChartData(data, events), [data, events]);
  const referenceChart = useMemo(() => referenceChartData(data, events), [data, events]);
  const fillChart = useMemo(() => fillChartData(events), [events]);
  const killSwitchOn = Boolean(status?.kill_switch);
  const paused = Boolean(status?.control?.paused);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Operations Dashboard</h1>
        </div>
        <div className="flex items-center gap-2">
          <Pill tone={status?.execution_mode === "live" ? "danger" : "good"}>
            {status?.execution_mode ?? "unknown"}
          </Pill>
          <IconButton
            label="Refresh snapshot"
            onClick={() => queryClient.invalidateQueries({ queryKey: ["snapshot"] })}
          >
            <RefreshCw className="h-4 w-4" />
          </IconButton>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <MetricCard
          icon={<ShieldAlert className="h-4 w-4" />}
          label="Kill Switch"
          value={killSwitchOn ? "Enabled" : "Clear"}
          tone={killSwitchOn ? "danger" : "good"}
          sublabel="Control file state"
        />
        <MetricCard
          icon={paused ? <PauseCircle className="h-4 w-4" /> : <PlayCircle className="h-4 w-4" />}
          label="Loop State"
          value={paused ? "Paused" : "Running"}
          tone={paused ? "warn" : "good"}
          sublabel={status?.control?.pause_reason ?? "operator control"}
        />
        <MetricCard
          icon={<Radio className="h-4 w-4" />}
          label="Reference"
          value={status?.reference ? `$${numberText(status.reference.price, 2)}` : "n/a"}
          tone={status?.reference?.stale ? "warn" : "good"}
          sublabel={`${status?.reference?.source ?? "no source"} · ${ageText(status?.reference?.local_ts)}`}
        />
        <MetricCard
          icon={<CircleDot className="h-4 w-4" />}
          label="Open Orders"
          value={numberText(status?.tracked_open_orders, 0)}
          tone={(status?.tracked_open_orders ?? 0) > 0 ? "warn" : "neutral"}
          sublabel={`${numberText(status?.paper_fill?.paper_open_resting_orders, 0)} paper resting`}
        />
        <MetricCard
          icon={<TrendingUp className="h-4 w-4" />}
          label="Runtime PnL"
          value={compact(reportSummary?.actual_paper_net_pnl)}
          tone={Number(reportSummary?.actual_paper_net_pnl ?? 0) < 0 ? "danger" : "neutral"}
          sublabel={`Replay ${compact(reportSummary?.replay_estimate_net_pnl)}`}
        />
      </div>

      <div className="grid gap-5 xl:grid-cols-[1.25fr_0.75fr]">
        <Panel>
          <PanelHeader
            title="Active Market"
            meta={active ? `${dateTime(active.start_ts)} → ${dateTime(active.end_ts)}` : "No active market"}
          />
          {active ? (
            <div className="grid gap-4 p-4 lg:grid-cols-[1fr_320px]">
              <div className="min-w-0 space-y-4">
                <div>
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    <Pill tone={active.is_tradeable ? "good" : "warn"}>{active.status}</Pill>
                    <Pill>{active.market_id.slice(0, 12)}</Pill>
                  </div>
                  <Link href={`/markets/${encodeURIComponent(active.market_id)}`} className="break-words text-lg font-semibold text-ink hover:underline">
                    {active.question}
                  </Link>
                </div>

                <div className="grid gap-3 sm:grid-cols-3">
                  <Field label="Start Price" value={`$${numberText(active.start_price, 2)}`} />
                  <Field label="q Up" value={pctText(active.fair_value?.q_up)} />
                  <Field label="q Down" value={pctText(active.fair_value?.q_down)} />
                </div>

                <div className="h-64 border border-line bg-panel p-3">
                  {qChart.length ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={qChart}>
                        <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
                        <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={24} />
                        <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} />
                        <Tooltip />
                        <Area type="monotone" dataKey="qUp" stroke="#18705b" fill="#18705b" fillOpacity={0.14} />
                        <Area type="monotone" dataKey="qDown" stroke="#b3363a" fill="#b3363a" fillOpacity={0.1} />
                      </AreaChart>
                    </ResponsiveContainer>
                  ) : (
                    <EmptyState label="No fair-value samples yet" />
                  )}
                </div>
              </div>

              <OperatorControls enabled={killSwitchOn} paused={paused} />
            </div>
          ) : (
            <EmptyState label={snapshot.isLoading ? "Loading snapshot" : "No active market in the current snapshot"} />
          )}
        </Panel>

        <Panel>
          <PanelHeader title="Realtime Tape" meta={`${events.length} recent events`} />
          <div className="max-h-[476px] overflow-auto">
            {events.length ? (
              events.map((event, index) => (
                <div key={`${event.ts}-${index}`} className="border-b border-line px-4 py-3 last:border-b-0">
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate text-sm font-medium text-ink">{event.type}</span>
                    <span className="shrink-0 text-xs text-ink/45">{dateTime(event.ts)}</span>
                  </div>
                  <p className="mt-1 truncate text-xs text-ink/55">{eventSummary(event)}</p>
                </div>
              ))
            ) : (
              <EmptyState label="No live events yet" />
            )}
          </div>
        </Panel>
      </div>

      <RealtimeCharts qChart={qChart} referenceChart={referenceChart} fillChart={fillChart} />

      <div className="grid gap-5 xl:grid-cols-2">
        <RecentDecisions snapshot={data} />
        <RecentExecutions snapshot={data} />
      </div>
    </div>
  );
}

function OperatorControls({ enabled, paused }: { enabled: boolean; paused: boolean }) {
  const queryClient = useQueryClient();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const killSwitch = useMutation({
    mutationFn: () => setKillSwitch(!enabled, enabled ? "UI disabled kill switch" : "UI enabled kill switch"),
    onSuccess: () => {
      setConfirmOpen(false);
      queryClient.invalidateQueries({ queryKey: ["snapshot"] });
    }
  });
  const pauseResume = useMutation({
    mutationFn: () => paused ? resumeBot("operator resume") : pauseBot("operator pause"),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["snapshot"] })
  });

  return (
    <div className="border border-line bg-white p-4">
      <div className="flex items-start gap-3">
        <span className="grid h-10 w-10 shrink-0 place-items-center border border-line bg-panel">
          {enabled ? <AlertTriangle className="h-5 w-5 text-danger" /> : <CheckCircle2 className="h-5 w-5 text-good" />}
        </span>
        <div className="min-w-0">
          <h3 className="text-sm font-semibold text-ink">Kill Switch</h3>
          <p className="mt-1 text-sm text-ink/60">
            {enabled ? "Trading is blocked by the backend control file." : "No kill-switch file is active."}
          </p>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <Button tone={paused ? "good" : "neutral"} disabled={pauseResume.isPending} onClick={() => pauseResume.mutate()}>
          {paused ? <PlayCircle className="h-4 w-4" /> : <PauseCircle className="h-4 w-4" />}
          {paused ? "Resume" : "Pause"}
        </Button>
        <Button tone={enabled ? "good" : "danger"} onClick={() => setConfirmOpen(true)}>
          <Power className="h-4 w-4" />
          {enabled ? "Disable" : "Enable"}
        </Button>
      </div>
      {pauseResume.error ? <p className="mt-2 text-xs text-danger">{pauseResume.error.message}</p> : null}

      {confirmOpen ? (
        <div className="mt-4 border border-line bg-panel p-3">
          <p className="text-sm font-medium text-ink">{enabled ? "Disable kill switch?" : "Enable kill switch?"}</p>
          <p className="mt-1 text-xs text-ink/60">
            Action is audited. Live enablement remains backend-only.
          </p>
          {killSwitch.error ? <p className="mt-2 text-xs text-danger">{killSwitch.error.message}</p> : null}
          <div className="mt-3 flex gap-2">
            <Button
              tone={enabled ? "good" : "danger"}
              disabled={killSwitch.isPending}
              onClick={() => killSwitch.mutate()}
            >
              Confirm
            </Button>
            <Button disabled={killSwitch.isPending} onClick={() => setConfirmOpen(false)}>
              Cancel
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function MetricCard({
  icon,
  label,
  value,
  sublabel,
  tone
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  sublabel: string;
  tone: "neutral" | "good" | "warn" | "danger";
}) {
  return (
    <Panel className="p-4">
      <div className="flex items-center gap-3">
        <span className="grid h-9 w-9 shrink-0 place-items-center border border-line bg-panel text-ink/70">{icon}</span>
        <div className="min-w-0">
          <div className="truncate text-xs font-medium uppercase text-ink/50">{label}</div>
          <div className="mt-1 flex items-center gap-2">
            <span className="truncate text-lg font-semibold text-ink">{value}</span>
            <Pill tone={tone}>{tone}</Pill>
          </div>
          <div className="mt-1 truncate text-xs text-ink/55">{sublabel}</div>
        </div>
      </div>
    </Panel>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="border border-line bg-white px-3 py-2">
      <div className="text-xs text-ink/50">{label}</div>
      <div className="mt-1 truncate text-sm font-semibold text-ink">{value}</div>
    </div>
  );
}

function RealtimeCharts({
  qChart,
  referenceChart,
  fillChart
}: {
  qChart: { time: string; qUp: number; qDown: number }[];
  referenceChart: { time: string; price: number }[];
  fillChart: { time: string; fills: number }[];
}) {
  return (
    <div className="grid gap-5 xl:grid-cols-3">
      <ChartPanel title="Probability" empty="No probability samples">
        <AreaChart data={qChart}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={24} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} />
          <Tooltip />
          <Area type="monotone" dataKey="qUp" stroke="#18705b" fill="#18705b" fillOpacity={0.14} />
          <Area type="monotone" dataKey="qDown" stroke="#b3363a" fill="#b3363a" fillOpacity={0.1} />
        </AreaChart>
      </ChartPanel>
      <ChartPanel title="Reference Price" empty="No reference samples">
        <LineChart data={referenceChart}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={24} />
          <YAxis tick={{ fontSize: 11 }} domain={["dataMin", "dataMax"]} />
          <Tooltip />
          <Line type="monotone" dataKey="price" stroke="#18705b" dot={false} strokeWidth={2} />
        </LineChart>
      </ChartPanel>
      <ChartPanel title="Paper Fills" empty="No fill samples">
        <LineChart data={fillChart}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={24} />
          <YAxis allowDecimals={false} tick={{ fontSize: 11 }} />
          <Tooltip />
          <Line type="stepAfter" dataKey="fills" stroke="#a45d13" dot={false} strokeWidth={2} />
        </LineChart>
      </ChartPanel>
    </div>
  );
}

function ChartPanel({ title, empty, children }: { title: string; empty: string; children: React.ReactElement }) {
  const data = (children.props as { data?: unknown[] }).data ?? [];
  return (
    <Panel>
      <PanelHeader title={title} meta={`${data.length} samples`} />
      <div className="h-64 p-3">
        {data.length ? (
          <ResponsiveContainer width="100%" height="100%">{children}</ResponsiveContainer>
        ) : (
          <EmptyState label={empty} />
        )}
      </div>
    </Panel>
  );
}

function RecentDecisions({ snapshot }: { snapshot?: Snapshot }) {
  const rows = snapshot?.latest_decisions ?? [];
  return (
    <Panel>
      <PanelHeader title="Latest Decisions" meta={`${rows.length} shown`} />
      <div className="overflow-auto">
        <table className="w-full min-w-[680px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
            <tr>
              <th className="px-3 py-2">Action</th>
              <th className="px-3 py-2">Outcome</th>
              <th className="px-3 py-2">Price</th>
              <th className="px-3 py-2">Size</th>
              <th className="px-3 py-2">Reason</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.slice(-8).reverse().map((row, index) => (
                <tr key={`${row.market_id}-${index}`} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-medium">{row.action}</td>
                  <td className="px-3 py-2">{compact(row.outcome)}</td>
                  <td className="px-3 py-2">{compact(row.price)}</td>
                  <td className="px-3 py-2">{compact(row.size)}</td>
                  <td className="max-w-[360px] truncate px-3 py-2 text-ink/60">{row.reason}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={5}>
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

function RecentExecutions({ snapshot }: { snapshot?: Snapshot }) {
  const rows = snapshot?.latest_execution_reports ?? [];
  return (
    <Panel>
      <PanelHeader title="Latest Execution Reports" meta={`${rows.length} shown`} />
      <div className="overflow-auto">
        <table className="w-full min-w-[680px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
            <tr>
              <th className="px-3 py-2">Status</th>
              <th className="px-3 py-2">Filled</th>
              <th className="px-3 py-2">Avg</th>
              <th className="px-3 py-2">Fee</th>
              <th className="px-3 py-2">Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.slice(-8).reverse().map((row, index) => (
                <tr key={`${row.order_id}-${index}`} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-medium">{row.status}</td>
                  <td className="px-3 py-2">{compact(row.filled_size)}</td>
                  <td className="px-3 py-2">{compact(row.avg_price)}</td>
                  <td className="px-3 py-2">{compact(row.fee)}</td>
                  <td className="px-3 py-2 text-ink/60">{dateTime(row.local_ts)}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={5}>
                  <EmptyState label="No execution reports in snapshot" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function fairValueChartData(snapshot: Snapshot | undefined, events: RuntimeEvent[]) {
  const rows = events
    .filter((event) => event.type === "fair_value_update")
    .map((event) => ({
      time: new Date(event.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      qUp: Number(event.data.q_up),
      qDown: Number(event.data.q_down)
    }))
    .filter((event) => Number.isFinite(event.qUp) && Number.isFinite(event.qDown))
    .reverse();

  const fair = snapshot?.current_market?.fair_value;
  if (rows.length || !fair) {
    return rows;
  }
  return [
    {
      time: "now",
      qUp: Number(fair.q_up),
      qDown: Number(fair.q_down)
    }
  ];
}

function referenceChartData(snapshot: Snapshot | undefined, events: RuntimeEvent[]) {
  const rows = events
    .filter((event) => event.type === "reference_update")
    .map((event) => ({
      time: new Date(event.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      price: Number(event.data.price)
    }))
    .filter((event) => Number.isFinite(event.price))
    .reverse();
  if (rows.length || !snapshot?.status.reference) {
    return rows;
  }
  return [{ time: "now", price: Number(snapshot.status.reference.price) }].filter((row) => Number.isFinite(row.price));
}

function fillChartData(events: RuntimeEvent[]) {
  let fills = 0;
  return events
    .filter((event) => event.type === "paper_fill")
    .slice()
    .reverse()
    .map((event) => {
      fills += 1;
      return {
        time: new Date(event.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        fills
      };
    });
}

function eventSummary(event: RuntimeEvent) {
  const data = event.data;
  const marketId = typeof data.market_id === "string" ? data.market_id.slice(0, 12) : null;
  const status = typeof data.status === "string" ? data.status : null;
  const source = typeof data.source === "string" ? data.source : null;
  return [marketId, status, source].filter(Boolean).join(" · ") || JSON.stringify(data).slice(0, 120);
}
