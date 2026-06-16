import { BarChart3, CircleDot, PauseCircle, PlayCircle, Radio, RefreshCw, ShieldAlert, TrendingUp } from "lucide-react";
import { ageText, compact, numberText } from "@/lib/format";
import type { LabDataQuality, LabJob, MarketSummary, Snapshot, TradeDecision } from "@/lib/types";
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

export function CurrentVerdictBanner({
  status,
  active,
  recorder,
  dataQuality,
  jobs,
  latestDecision
}: {
  status?: Snapshot["status"];
  active?: MarketSummary | null;
  recorder: RecorderHealth;
  dataQuality?: LabDataQuality;
  jobs: LabJob[];
  latestDecision?: TradeDecision;
}) {
  const killSwitchOn = Boolean(status?.kill_switch);
  const paused = Boolean(status?.control?.paused);
  const systemTone: Tone = killSwitchOn || !recorder.healthy ? "danger" : paused || status?.reference?.stale ? "warn" : "good";
  const system = systemTone === "good" ? "HEALTHY" : systemTone === "danger" ? "BROKEN" : "DEGRADED";
  const dataStatus = dataVerdict(dataQuality, status?.reference?.stale, recorder);
  const trading = tradingVerdict(killSwitchOn, paused, status?.tracked_open_orders ?? 0, active, latestDecision);
  const research = jobs.some((job) => job.running) ? "VALIDATING_DYNAMIC_QUOTE_STYLE" : "COLLECTING_EVIDENCE";
  const nextAction = nextActionText(systemTone, dataStatus, trading, active);

  return (
    <Panel className={bannerClass(systemTone)}>
      <div className="grid gap-3 p-4 xl:grid-cols-[1.2fr_repeat(4,minmax(120px,1fr))_1.4fr]">
        <VerdictItem label="Mode" value={(status?.execution_mode ?? "unknown").toUpperCase()} tone={status?.execution_mode === "live" ? "danger" : "good"} />
        <VerdictItem label="System" value={system} tone={systemTone} />
        <VerdictItem label="Trading" value={trading} tone={trading === "QUOTING" ? "good" : trading.includes("PAUSED") ? "warn" : "neutral"} />
        <VerdictItem label="Data" value={dataStatus} tone={dataStatus === "CLEAN" ? "good" : dataStatus === "STALE" || dataStatus === "DEGRADED" ? "warn" : "neutral"} />
        <VerdictItem label="Research" value={research} tone="neutral" />
        <div className="min-w-0">
          <div className="text-[11px] font-semibold uppercase text-ink/50">Next Action</div>
          <div className="mt-1 text-sm font-semibold text-ink">{nextAction}</div>
        </div>
      </div>
    </Panel>
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
    <div className="grid gap-3 xl:grid-cols-4">
      <HealthCluster title="Safety">
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
      </HealthCluster>
      <HealthCluster title="Data">
        <MetricCard
          icon={<Radio className="h-4 w-4" />}
          label="Reference"
          value={status?.reference ? `$${numberText(status.reference.price, 2)}` : "waiting"}
          tone={status?.reference?.stale ? "warn" : status?.reference ? "good" : "neutral"}
          sublabel={`${ageText(status?.reference?.local_ts)} · ${compact(status?.reference?.source, "no source")}`}
          help="Latest reference price used to value the active market, with freshness shown in the subtitle."
        />
        <MetricCard
          icon={<BarChart3 className="h-4 w-4" />}
          label="Recorder"
          value={recorder.healthy ? "Healthy" : "Issue"}
          tone={recorder.healthy ? "good" : "danger"}
          sublabel={`queue ${recorder.queueSize} · drops ${recorder.droppedCount}`}
          help="Recorder health reflects event persistence status. Queue and drops indicate whether events are backing up or being lost."
        />
      </HealthCluster>
      <HealthCluster title="Market">
        <MetricCard
          icon={<CircleDot className="h-4 w-4" />}
          label="Markets"
          value={numberText(status?.markets, 0)}
          tone={(status?.tradeable_markets ?? 0) > 0 ? "good" : "neutral"}
          sublabel={`${numberText(status?.tradeable_markets, 0)} tradeable · ${numberText(status?.books, 0)} books`}
          help="Discovered markets and order books available to the runtime."
        />
        <MetricCard
          icon={<Radio className="h-4 w-4" />}
          label="Latest Decision"
          value={compact(status?.latest_decisions?.[0]?.action, "observing").toUpperCase()}
          tone={status?.latest_decisions?.[0]?.action === "place" ? "good" : "neutral"}
          sublabel={compact(status?.latest_decisions?.[0]?.reason, "no decision reason yet")}
          help="Most recent strategy decision from the runtime snapshot."
        />
      </HealthCluster>
      <HealthCluster title="Execution">
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
      </HealthCluster>
    </div>
  );
}

function VerdictItem({ label, value, tone }: { label: string; value: string; tone: Tone }) {
  return (
    <div className="min-w-0">
      <div className="text-[11px] font-semibold uppercase text-ink/50">{label}</div>
      <div className="mt-1 flex min-w-0 items-center gap-2">
        <span className={toneDot(tone)} aria-hidden />
        <span className="truncate text-sm font-semibold text-ink">{value}</span>
      </div>
    </div>
  );
}

function HealthCluster({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="border border-line bg-white shadow-hairline">
      <div className="border-b border-line px-3 py-2 text-xs font-semibold uppercase text-ink/50">{title}</div>
      <div className="divide-y divide-line">{children}</div>
    </section>
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
    <div className="p-3">
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
    </div>
  );
}

function dataVerdict(dataQuality: LabDataQuality | undefined, referenceStale: boolean | undefined, recorder: RecorderHealth) {
  const freshness = dataQuality?.freshness && typeof dataQuality.freshness === "object" ? dataQuality.freshness as Record<string, unknown> : null;
  const result = freshness?.result && typeof freshness.result === "object" ? freshness.result as Record<string, unknown> : freshness;
  const status = String(result?.status ?? "").toLowerCase();
  if (!recorder.healthy) {
    return "DEGRADED";
  }
  if (referenceStale || status.includes("stale")) {
    return "STALE";
  }
  if (status.includes("healthy") || status.includes("ok")) {
    return "CLEAN";
  }
  return "UNKNOWN";
}

function tradingVerdict(
  killSwitchOn: boolean,
  paused: boolean,
  openOrders: number,
  active?: MarketSummary | null,
  latestDecision?: TradeDecision
) {
  if (killSwitchOn) {
    return "PAUSED_BY_KILL_SWITCH";
  }
  if (paused) {
    return "PAUSED";
  }
  if (openOrders > 0 || latestDecision?.action?.toLowerCase() === "place") {
    return "QUOTING";
  }
  if (active) {
    return latestDecision?.action?.toLowerCase() === "hold" ? "NO_EDGE" : "OBSERVING";
  }
  return "COLLECTING_DATA";
}

function nextActionText(systemTone: Tone, dataStatus: string, trading: string, active?: MarketSummary | null) {
  if (systemTone === "danger") {
    return "Open Jobs and Data Quality before trusting new research output.";
  }
  if (dataStatus !== "CLEAN") {
    return "Run freshness and hourly quality checks, then inspect exclusions.";
  }
  if (!active) {
    return "Continue discovery until the next BTC 15m market becomes active.";
  }
  if (trading === "NO_EDGE" || trading === "OBSERVING") {
    return "Continue collecting clean paper evidence.";
  }
  return "Monitor paper fills, cancels, and recorder health.";
}

function bannerClass(tone: Tone) {
  if (tone === "danger") {
    return "border-danger/35 bg-danger/5";
  }
  if (tone === "warn") {
    return "border-warn/35 bg-warn/5";
  }
  return "border-good/25 bg-good/5";
}
