"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ExternalLink, Play, RefreshCw } from "lucide-react";
import { useState } from "react";
import { getJobDetail, getJobLogs, getJobs, startLabJob } from "@/lib/api";
import type { LabJob } from "@/lib/types";
import { compact, dateTime, numberText } from "@/lib/format";
import { Button, EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

type RunnableJob = Parameters<typeof startLabJob>[0];

const runnableJobs: { id: RunnableJob; label: string }[] = [
  { id: "freshness-check", label: "Freshness" },
  { id: "hourly-quality-audit", label: "Hourly Audit" },
  { id: "daily-research-report", label: "Daily Report" },
  { id: "prospective-validation", label: "Prospective" },
  { id: "compact-replay-index", label: "Replay Index" },
  { id: "chart-backfill", label: "Chart Backfill" },
  { id: "adx-ingestion", label: "ADX Ingestion" },
  { id: "manual-backfill", label: "Manual Backfill" }
];

export function JobMonitorPage() {
  const queryClient = useQueryClient();
  const [selectedJob, setSelectedJob] = useState<string | null>(null);
  const [backfillStart, setBackfillStart] = useState("");
  const [backfillEnd, setBackfillEnd] = useState("");
  const [backfillTask, setBackfillTask] = useState("all");
  const jobs = useQuery({ queryKey: ["jobs"], queryFn: getJobs, retry: false, refetchInterval: 30000 });
  const run = useMutation({
    mutationFn: (job: RunnableJob) => {
      if (job !== "manual-backfill" && job !== "backfill") {
        return startLabJob(job);
      }
      return startLabJob(job, { start: backfillStart, end: backfillEnd, task: backfillTask });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["jobs"] })
  });
  const backfillReady = Boolean(backfillStart && backfillEnd && backfillTask);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Job Monitor</h1>
        </div>
        <IconButton label="Refresh jobs" onClick={() => queryClient.invalidateQueries({ queryKey: ["jobs"] })}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <Panel>
        <PanelHeader title="Manual Actions" meta={run.data?.status ?? "ready"} />
        <div className="flex flex-wrap gap-2 p-4">
          {runnableJobs.map((job) => (
            <Button
              key={job.id}
              disabled={run.isPending || (job.id === "manual-backfill" && !backfillReady)}
              onClick={() => confirmAndRun(job.id, () => run.mutate(job.id))}
            >
              <Play className="h-4 w-4" />
              {job.label}
            </Button>
          ))}
        </div>
        <div className="grid gap-3 border-t border-line bg-panel px-4 py-3 md:grid-cols-[160px_160px_180px_1fr]">
          <label className="text-xs font-medium uppercase text-ink/55">
            Backfill Start
            <input
              type="date"
              className="mt-1 h-9 w-full border border-line bg-white px-2 text-sm normal-case text-ink"
              value={backfillStart}
              onChange={(event) => setBackfillStart(event.target.value)}
            />
          </label>
          <label className="text-xs font-medium uppercase text-ink/55">
            Backfill End
            <input
              type="date"
              className="mt-1 h-9 w-full border border-line bg-white px-2 text-sm normal-case text-ink"
              value={backfillEnd}
              onChange={(event) => setBackfillEnd(event.target.value)}
            />
          </label>
          <label className="text-xs font-medium uppercase text-ink/55">
            Backfill Task
            <select
              className="mt-1 h-9 w-full border border-line bg-white px-2 text-sm normal-case text-ink"
              value={backfillTask}
              onChange={(event) => setBackfillTask(event.target.value)}
            >
              <option value="all">all</option>
              <option value="audit">audit</option>
              <option value="daily-report">daily-report</option>
              <option value="replay-index">replay-index</option>
              <option value="prospective">prospective</option>
            </select>
          </label>
          <div className="self-end text-xs text-ink/55">
            Backfill is manual-only and requires explicit date bounds before it can start.
          </div>
        </div>
        {run.error ? <div className="border-t border-line px-4 py-3 text-sm text-danger">{run.error.message}</div> : null}
        {run.data ? <div className="border-t border-line px-4 py-3 text-sm text-ink/70">{run.data.job_name}: {run.data.status}</div> : null}
      </Panel>

      <Panel>
        <PanelHeader title="Job Executions" meta={`${jobs.data?.jobs.length ?? 0} jobs`} />
        <JobTable jobs={jobs.data?.jobs ?? []} loading={jobs.isLoading} onSelect={setSelectedJob} onRun={(job) => run.mutate(job)} running={run.isPending} />
      </Panel>

      {selectedJob ? <JobDetailPanel jobId={selectedJob} /> : null}
    </div>
  );
}

function JobTable({
  jobs,
  loading,
  onSelect,
  onRun,
  running
}: {
  jobs: LabJob[];
  loading: boolean;
  onSelect: (jobId: string) => void;
  onRun: (jobId: RunnableJob) => void;
  running: boolean;
}) {
  if (!jobs.length) {
    return <EmptyState label={loading ? "Loading jobs" : "No jobs found"} />;
  }
  return (
    <div className="overflow-auto">
      <table className="w-full min-w-[1040px] text-left text-sm">
        <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
          <tr>
            {["Job", "Trigger", "Status", "Last Start", "Last Finish", "Duration", "Exit", "Artifact", "Error", "Action"].map((header) => (
              <th key={header} className="px-3 py-2">{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.job_id} className="border-b border-line last:border-b-0 hover:bg-panel">
              <td className="px-3 py-2">
                <button className="text-left font-medium text-ink hover:underline" onClick={() => onSelect(job.job_id)}>{job.job_name}</button>
                <div className="font-mono text-xs text-ink/50">{job.job_id}</div>
              </td>
              <td className="px-3 py-2">{job.trigger ?? "n/a"}{job.cron ? <div className="font-mono text-xs text-ink/50">{job.cron}</div> : null}</td>
              <td className="px-3 py-2">
                <Pill tone={jobTone(job)}>{job.status}</Pill>
                {job.running ? <div className="mt-1 text-xs text-good">running</div> : null}
                {job.execution_name ? <div className="mt-1 font-mono text-xs text-ink/45">{job.execution_name}</div> : null}
              </td>
              <td className="px-3 py-2">{dateTime(job.last_start)}</td>
              <td className="px-3 py-2">{dateTime(job.last_finish)}</td>
              <td className="px-3 py-2">{numberText(job.duration)}</td>
              <td className="px-3 py-2">{numberText(job.exit_code)}</td>
              <td className="px-3 py-2 font-mono text-xs">{job.output_artifact ?? "reports/jobs/latest pending"}</td>
              <td className="px-3 py-2">{job.error ?? "none reported"}</td>
              <td className="px-3 py-2">
                <Button className="h-8 px-2 text-xs" disabled={running || job.job_id === "manual-backfill"} onClick={() => onRun(job.job_id as RunnableJob)}>
                  <Play className="h-3.5 w-3.5" />
                  Rerun
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function JobDetailPanel({ jobId }: { jobId: string }) {
  const detail = useQuery({ queryKey: ["jobs", jobId], queryFn: () => getJobDetail(jobId), retry: false });
  const logs = useQuery({ queryKey: ["jobs", jobId, "logs"], queryFn: () => getJobLogs(jobId), retry: false });
  const job = detail.data?.job;
  const artifacts = logs.data?.artifacts ?? [job?.output_artifact].filter((artifact): artifact is string => Boolean(artifact));
  return (
    <Panel>
      <PanelHeader title="Job Detail" meta={jobId} />
      {job ? (
        <div className="grid gap-5 p-4 xl:grid-cols-[1fr_1.4fr]">
          <div className="space-y-3">
            <DetailRow label="Status" value={job.status} />
            <DetailRow label="Trigger" value={`${job.trigger ?? "unknown"} ${job.cron ?? ""}`} />
            <DetailRow label="Last Start" value={dateTime(job.last_start)} />
            <DetailRow label="Last Finish" value={dateTime(job.last_finish)} />
            <DetailRow label="Duration" value={numberText(job.duration)} />
            <DetailRow label="Data Quality" value={compact(job.data_quality ?? "unknown")} />
            <DetailRow label="Safety" value={job.live_trading_enabled ? "live enabled" : "research-only, live disabled"} />
          </div>
          <div className="space-y-3">
            <Panel>
              <PanelHeader title="Logs" meta={logs.data?.logs.length ? `${logs.data.logs.length} lines` : "Azure Monitor"} />
              {logs.data?.logs.length ? (
                <pre className="max-h-72 overflow-auto p-3 text-xs text-ink/70">{logs.data.logs.join("\n")}</pre>
              ) : (
                <EmptyState label={logs.data?.detail ?? "No inline logs returned. Check Azure Monitor for execution logs."} />
              )}
            </Panel>
            <Panel>
              <PanelHeader title="Artifacts" meta={`${logs.data?.artifacts?.length ?? 0} linked`} />
              <div className="space-y-2 p-3">
                {artifacts.map((artifact) => (
                  <div key={artifact} className="flex items-center justify-between gap-3 border border-line bg-panel px-3 py-2">
                    <span className="truncate font-mono text-xs text-ink/70">{artifact}</span>
                    <ExternalLink className="h-4 w-4 shrink-0 text-ink/45" />
                  </div>
                ))}
              </div>
            </Panel>
          </div>
        </div>
      ) : (
        <EmptyState label={detail.isLoading ? "Loading job detail" : detail.error?.message ?? "Job detail unavailable"} />
      )}
    </Panel>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="grid grid-cols-[120px_1fr] gap-3 border-b border-line pb-2 text-sm last:border-b-0">
      <div className="text-xs font-semibold uppercase text-ink/50">{label}</div>
      <div className="min-w-0 truncate text-ink/75">{value}</div>
    </div>
  );
}

function confirmAndRun(job: string, run: () => void) {
  if (window.confirm(`Run ${job}?`)) {
    run();
  }
}

function jobTone(job: LabJob) {
  const status = job.status.toLowerCase();
  if (status.includes("failed") || status.includes("error")) {
    return "danger" as const;
  }
  if (job.running || status.includes("running") || status.includes("succeeded") || status.includes("start_requested")) {
    return "good" as const;
  }
  if (status.includes("defined")) {
    return "neutral" as const;
  }
  return "warn" as const;
}
