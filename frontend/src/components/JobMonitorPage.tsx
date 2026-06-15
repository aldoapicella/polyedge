"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Play, RefreshCw } from "lucide-react";
import { useState } from "react";
import { getLabJobs, startLabJob } from "@/lib/api";
import type { LabJob } from "@/lib/types";
import { dateTime, numberText } from "@/lib/format";
import { Button, EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

const runnableJobs: { id: "freshness-check" | "daily-report" | "prospective-validation" | "replay-index" | "backfill"; label: string }[] = [
  { id: "freshness-check", label: "Freshness" },
  { id: "daily-report", label: "Daily Report" },
  { id: "prospective-validation", label: "Prospective" },
  { id: "replay-index", label: "Replay Index" },
  { id: "backfill", label: "Backfill" }
];

export function JobMonitorPage() {
  const queryClient = useQueryClient();
  const [backfillStart, setBackfillStart] = useState("");
  const [backfillEnd, setBackfillEnd] = useState("");
  const [backfillTask, setBackfillTask] = useState("all");
  const jobs = useQuery({ queryKey: ["labs", "jobs"], queryFn: getLabJobs, retry: false, refetchInterval: 30000 });
  const run = useMutation({
    mutationFn: (job: (typeof runnableJobs)[number]["id"]) => {
      if (job !== "backfill") {
        return startLabJob(job);
      }
      return startLabJob(job, { start: backfillStart, end: backfillEnd, task: backfillTask });
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["labs", "jobs"] })
  });
  const backfillReady = Boolean(backfillStart && backfillEnd && backfillTask);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Job Monitor</h1>
        </div>
        <IconButton label="Refresh jobs" onClick={() => queryClient.invalidateQueries({ queryKey: ["labs", "jobs"] })}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <Panel>
        <PanelHeader title="Manual Actions" meta={run.data?.status ?? "ready"} />
        <div className="flex flex-wrap gap-2 p-4">
          {runnableJobs.map((job) => (
            <Button
              key={job.id}
              disabled={run.isPending || (job.id === "backfill" && !backfillReady)}
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
        <JobTable jobs={jobs.data?.jobs ?? []} loading={jobs.isLoading} />
      </Panel>
    </div>
  );
}

function JobTable({ jobs, loading }: { jobs: LabJob[]; loading: boolean }) {
  if (!jobs.length) {
    return <EmptyState label={loading ? "Loading jobs" : "No jobs found"} />;
  }
  return (
    <div className="overflow-auto">
      <table className="w-full min-w-[1040px] text-left text-sm">
        <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
          <tr>
            {["Job", "Trigger", "Status", "Last Start", "Last Finish", "Duration", "Exit", "Artifact", "Error"].map((header) => (
              <th key={header} className="px-3 py-2">{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.job_id} className="border-b border-line last:border-b-0">
              <td className="px-3 py-2">
                <div className="font-medium text-ink">{job.job_name}</div>
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
              <td className="px-3 py-2 font-mono text-xs">{job.output_artifact ?? "n/a"}</td>
              <td className="px-3 py-2">{job.error ?? "n/a"}</td>
            </tr>
          ))}
        </tbody>
      </table>
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
