"use client";

import { useMutation } from "@tanstack/react-query";
import { FileText, PauseCircle, PlayCircle, Power } from "lucide-react";
import { useState } from "react";
import { buildReport, pauseBot, resumeBot, setKillSwitch } from "@/lib/api";
import { Button, Panel } from "@/components/ui";

export function ControlPanel({
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
