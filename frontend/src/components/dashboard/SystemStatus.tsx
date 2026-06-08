import { BarChart3, CircleDot, PauseCircle, PlayCircle, Radio, RefreshCw, ShieldAlert, TrendingUp } from "lucide-react";
import { ageText, compact, numberText } from "@/lib/format";
import type { Snapshot } from "@/lib/types";
import { IconButton, InfoHint, Panel, Pill } from "@/components/ui";
import { moneyText, toneDot } from "./model";
import type { RecorderHealth, Tone } from "./types";

export function DashboardHeader({
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

export function SystemHealthCards({
  status,
  reportSummary,
  recorder,
  killSwitchOn,
  paused
}: {
  status?: Snapshot["status"];
  reportSummary?: Record<string, unknown>;
  recorder: RecorderHealth;
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
