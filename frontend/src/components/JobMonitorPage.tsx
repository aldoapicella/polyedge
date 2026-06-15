"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Play, RefreshCw } from "lucide-react";
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
  const jobs = useQuery({ queryKey: ["labs", "jobs"], queryFn: getLabJobs, retry: false, refetchInterval: 30000 });
  const run = useMutation({
    mutationFn: (job: (typeof runnableJobs)[number]["id"]) => startLabJob(job),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["labs", "jobs"] })
  });

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
            <Button key={job.id} disabled={run.isPending} onClick={() => confirmAndRun(job.id, () => run.mutate(job.id))}>
              <Play className="h-4 w-4" />
              {job.label}
            </Button>
          ))}
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
              <td className="px-3 py-2"><Pill tone={job.status.includes("failed") ? "danger" : job.status.includes("defined") ? "neutral" : "good"}>{job.status}</Pill></td>
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
