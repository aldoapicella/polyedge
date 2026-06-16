"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getLabDataQualityLatest, getLabJobs, getLatestReport, getMarketChart, getRecentEvents, getSnapshot } from "@/lib/api";
import { emptyMarketSeries, mergeRuntimeEventsIntoSeries } from "@/lib/charting";
import { ageText, dateTime, numberText } from "@/lib/format";
import { ActiveMarketPanel } from "@/components/dashboard/ActiveMarketPanel";
import { ControlPanel } from "@/components/dashboard/ControlPanel";
import { DecisionTable } from "@/components/dashboard/DecisionTable";
import { EventTimeline } from "@/components/dashboard/EventTimeline";
import { ExecutionReportTable } from "@/components/dashboard/ExecutionReportTable";
import { MarketMainChart, TrendCharts } from "@/components/dashboard/MarketCharts";
import { CurrentVerdictBanner, DashboardHeader, SystemHealthCards } from "@/components/dashboard/SystemStatus";
import { recorderSummary } from "@/components/dashboard/model";
import { useRealtimeSnapshot } from "@/components/dashboard/useRealtimeSnapshot";
import { Panel, PanelHeader, Pill } from "@/components/ui";
import type { RuntimeEvent } from "@/lib/types";

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
  const dataQuality = useQuery({
    queryKey: ["labs", "data-quality", "latest"],
    queryFn: getLabDataQualityLatest,
    retry: false,
    refetchInterval: 30000
  });
  const labJobs = useQuery({
    queryKey: ["labs", "jobs"],
    queryFn: getLabJobs,
    retry: false,
    refetchInterval: 30000
  });
  const eventTape = useRealtimeSnapshot(queryClient);

  const snapshotStore = snapshot.data;
  const status = snapshotStore?.status;
  const active = snapshotStore?.current_market;
  const reference = status?.reference;
  const reportSummary = latestReport.data?.report?.summary;
  const killSwitchOn = Boolean(status?.kill_switch);
  const paused = Boolean(status?.control?.paused);
  const recorder = recorderSummary(status?.recorder);
  const recentEvents = useQuery({
    queryKey: ["events", "recent", active?.market_id ?? "all"],
    queryFn: () => getRecentEvents({ marketId: active?.market_id, limit: 80 }),
    enabled: snapshot.isSuccess,
    retry: false,
    refetchInterval: 30000
  });
  const chartSeries = useQuery({
    queryKey: ["markets", "chart", active?.market_id ?? "none", "full"],
    queryFn: () => getMarketChart(active?.market_id ?? "", "full"),
    enabled: Boolean(active?.market_id),
    refetchInterval: 30000
  });
  const seriesStore = mergeRuntimeEventsIntoSeries(
    chartSeries.data ?? emptyMarketSeries(active),
    combinedEvents(recentEvents.data?.events ?? [], eventTape),
    active?.market_id,
    active,
    "full"
  );
  const timelineEvents = combinedEvents(recentEvents.data?.events ?? [], eventTape);

  return (
    <div className="space-y-5">
      <DashboardHeader
        mode={status?.execution_mode}
        referenceFresh={!reference?.stale}
        recorderHealthy={recorder.healthy}
        onRefresh={() => queryClient.invalidateQueries({ queryKey: ["snapshot"] })}
      />

      <CurrentVerdictBanner
        status={status}
        active={active}
        recorder={recorder}
        dataQuality={dataQuality.data}
        jobs={labJobs.data?.jobs ?? []}
        latestDecision={snapshotStore?.latest_decisions?.[0]}
      />

      <SystemHealthCards
        status={status}
        reportSummary={reportSummary}
        recorder={recorder}
        killSwitchOn={killSwitchOn}
        paused={paused}
      />

      <OperatorReadiness
        dataQuality={dataQuality.data}
        jobs={labJobs.data?.jobs ?? []}
        reportDate={latestReport.data?.report?.report_metadata?.date as string | undefined}
        chartSummary={seriesStore.summary}
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
          latestDecision={snapshotStore?.latest_decisions?.[0]}
        />
        <MarketMainChart
          points={seriesStore.marketChart}
          domain={seriesStore.domain}
          sampleCount={seriesStore.sampleCount}
          summary={seriesStore.summary}
          active={active}
          events={timelineEvents}
        />
      </div>

      <TrendCharts points={seriesStore.marketChart} fills={seriesStore.fills} domain={seriesStore.domain} summary={seriesStore.summary} />

      <div className="grid gap-5 xl:grid-cols-12">
        <div className="min-w-0 xl:col-span-5">
          <DecisionTable decisions={snapshotStore?.latest_decisions ?? []} />
        </div>
        <div className="min-w-0 xl:col-span-7">
          <EventTimeline events={timelineEvents} active={active} />
        </div>
      </div>

      <ExecutionReportTable reports={snapshotStore?.latest_execution_reports ?? []} active={active} />
    </div>
  );
}

function combinedEvents(seed: RuntimeEvent[], live: RuntimeEvent[]) {
  const events = new Map<string, RuntimeEvent>();
  for (const event of [...seed, ...live]) {
    events.set(`${event.type}:${event.ts}:${JSON.stringify(event.data).slice(0, 120)}`, event);
  }
  return [...events.values()].sort((left, right) => new Date(right.ts).getTime() - new Date(left.ts).getTime());
}

function OperatorReadiness({
  dataQuality,
  jobs,
  reportDate,
  chartSummary
}: {
  dataQuality: Awaited<ReturnType<typeof getLabDataQualityLatest>> | undefined;
  jobs: Awaited<ReturnType<typeof getLabJobs>>["jobs"];
  reportDate?: string;
  chartSummary: ReturnType<typeof mergeRuntimeEventsIntoSeries>["summary"];
}) {
  const freshness = dataQuality?.freshness && typeof dataQuality.freshness === "object" ? (dataQuality.freshness as Record<string, unknown>) : null;
  const result = freshness?.result && typeof freshness.result === "object" ? (freshness.result as Record<string, unknown>) : freshness;
  const freshnessStatus = String(result?.status ?? "unknown");
  const latestJob = jobs.find((job) => job.running) ?? jobs.find((job) => job.last_start) ?? jobs[0];
  const jobStatus = latestJob ? `${latestJob.job_id}: ${latestJob.status}` : "no jobs";
  return (
    <Panel>
      <PanelHeader title="Operator Readiness" meta="research evidence and chart coverage" />
      <div className="grid gap-3 p-4 md:grid-cols-4">
        <ReadinessMetric
          label="Data Quality"
          value={freshnessStatus}
          meta={result?.latest_blob_last_modified ? `latest blob ${ageText(String(result.latest_blob_last_modified))}` : "freshness snapshot"}
          tone={freshnessStatus === "healthy" ? "good" : "warn"}
        />
        <ReadinessMetric label="Daily Report" value={reportDate ?? "latest"} meta={reportDate ? dateTime(reportDate) : "available report"} />
        <ReadinessMetric
          label="Latest Job"
          value={jobStatus}
          meta={latestJob?.last_start ? dateTime(latestJob.last_start) : "defined in IaC"}
          tone={latestJob?.status?.toLowerCase().includes("fail") ? "danger" : latestJob?.running ? "good" : "neutral"}
        />
        <ReadinessMetric
          label="q Coverage"
          value={`${numberText(chartSummary.qSampleCount, 0)} q / ${numberText(chartSummary.bookSampleCount, 0)} book`}
          meta={chartSummary.firstQTs ? `first q ${dateTime(chartSummary.firstQTs)}` : "model-only q samples"}
          tone={(chartSummary.qSampleCount ?? 0) > 0 ? "good" : "warn"}
        />
      </div>
    </Panel>
  );
}

function ReadinessMetric({
  label,
  value,
  meta,
  tone = "neutral"
}: {
  label: string;
  value: string;
  meta: string;
  tone?: "neutral" | "good" | "warn" | "danger";
}) {
  return (
    <div className="min-w-0 border border-line bg-panel px-3 py-3">
      <div className="text-xs font-medium uppercase text-ink/50">{label}</div>
      <div className="mt-1 truncate text-sm font-semibold text-ink">{value}</div>
      <div className="mt-2 flex items-center gap-2">
        <Pill tone={tone}>{tone}</Pill>
        <span className="truncate text-xs text-ink/50">{meta}</span>
      </div>
    </div>
  );
}
