"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, History, LockKeyhole, RefreshCw, RotateCcw, Save, ShieldCheck } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { applyConfig, getConfig, getConfigHistory, rollbackConfig, validateConfig } from "@/lib/api";
import type { ConfigChange, RuntimeConfig, RuntimeConfigPatch } from "@/lib/types";
import { compact, dateTime } from "@/lib/format";
import { Button, EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

type EditableSection = "strategy" | "risk" | "paper";
type FormState = Record<EditableSection, Record<string, string>>;

export function ConfigPage() {
  const queryClient = useQueryClient();
  const config = useQuery({
    queryKey: ["config"],
    queryFn: getConfig
  });
  const history = useQuery({
    queryKey: ["config", "history"],
    queryFn: () => getConfigHistory(20),
    retry: false
  });
  const [form, setForm] = useState<FormState | null>(null);
  const [reason, setReason] = useState("paper config adjustment");

  useEffect(() => {
    if (config.data) {
      setForm(toForm(config.data));
    }
  }, [config.data]);

  const patch = useMemo(() => (config.data && form ? buildPatch(config.data, form) : {}), [config.data, form]);
  const hasChanges = Boolean(patch.strategy || patch.risk || patch.paper);
  const paperMode = config.data?.read_only.execution_mode === "paper";

  const validate = useMutation({
    mutationFn: () => validateConfig(patch)
  });
  const apply = useMutation({
    mutationFn: () => applyConfig(patch, reason),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["config"] });
      queryClient.invalidateQueries({ queryKey: ["config", "history"] });
      queryClient.invalidateQueries({ queryKey: ["snapshot"] });
    }
  });
  const rollback = useMutation({
    mutationFn: (version: string) => rollbackConfig(version, `rollback ${version}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["config"] });
      queryClient.invalidateQueries({ queryKey: ["config", "history"] });
      queryClient.invalidateQueries({ queryKey: ["snapshot"] });
    }
  });

  const changes = validate.data?.changes ?? diffFromPatch(config.data, patch);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Configuration</h1>
        </div>
        <IconButton label="Refresh config" onClick={() => config.refetch()}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      {config.data && form ? (
        <div className="grid gap-5 xl:grid-cols-[1fr_420px]">
          <div className="space-y-5">
            <ConfigSection title="Strategy Runtime" section="strategy" values={form.strategy} disabled={!paperMode} onChange={setField(setForm)} />
            <ConfigSection title="Risk Runtime" section="risk" values={form.risk} disabled={!paperMode} onChange={setField(setForm)} />
            <ConfigSection title="Paper Fill Runtime" section="paper" values={form.paper} disabled={!paperMode} onChange={setField(setForm)} />
          </div>

          <div className="space-y-5">
            <Panel>
              <PanelHeader title="Change Set" meta={paperMode ? "Paper mode" : "Locked outside paper mode"}>
                <Pill tone={paperMode ? "good" : "danger"}>{String(config.data.read_only.execution_mode)}</Pill>
              </PanelHeader>
              <div className="space-y-4 p-4">
                <label className="block">
                  <span className="text-xs font-medium text-ink/55">Reason</span>
                  <input
                    value={reason}
                    onChange={(event) => setReason(event.target.value)}
                    className="mt-1 h-10 w-full rounded-sm border border-line bg-white px-3 text-sm text-ink"
                  />
                </label>
                <DiffTable changes={changes} />
                {validate.data && !validate.data.valid ? (
                  <div className="border border-danger/30 bg-danger/10 p-3 text-sm text-danger">
                    {validate.data.issues.join("; ")}
                  </div>
                ) : null}
                {validate.error ? <p className="text-sm text-danger">{validate.error.message}</p> : null}
                {apply.error ? <p className="text-sm text-danger">{apply.error.message}</p> : null}
                <div className="flex flex-wrap gap-2">
                  <Button disabled={!hasChanges || validate.isPending} onClick={() => validate.mutate()}>
                    <CheckCircle2 className="h-4 w-4" />
                    Validate
                  </Button>
                  <Button tone="good" disabled={!paperMode || !hasChanges || apply.isPending} onClick={() => apply.mutate()}>
                    <Save className="h-4 w-4" />
                    Apply
                  </Button>
                </div>
              </div>
            </Panel>

            <Guardrails values={config.data.read_only} />

            <Panel>
              <PanelHeader title="Config History" meta={`${history.data?.history.length ?? 0} entries`}>
                <History className="h-4 w-4 text-ink/55" />
              </PanelHeader>
              <div className="max-h-[460px] overflow-auto">
                {history.data?.history.length ? (
                  history.data.history.map((entry) => (
                    <div key={entry.version} className="border-b border-line px-4 py-3 last:border-b-0">
                      <div className="flex items-center justify-between gap-2">
                        <span className="truncate text-sm font-medium text-ink">{entry.action}</span>
                        <span className="shrink-0 text-xs text-ink/50">{dateTime(entry.created_ts)}</span>
                      </div>
                      <p className="mt-1 truncate text-xs text-ink/55">{entry.reason ?? entry.version}</p>
                      <Button
                        className="mt-3"
                        disabled={!paperMode || rollback.isPending}
                        onClick={() => rollback.mutate(entry.version)}
                      >
                        <RotateCcw className="h-4 w-4" />
                        Rollback
                      </Button>
                    </div>
                  ))
                ) : (
                  <EmptyState label={history.isLoading ? "Loading history" : "No config history"} />
                )}
              </div>
            </Panel>
          </div>
        </div>
      ) : (
        <Panel>
          <EmptyState label={config.isLoading ? "Loading configuration" : config.error?.message ?? "Config unavailable"} />
        </Panel>
      )}
    </div>
  );
}

function ConfigSection({
  title,
  section,
  values,
  disabled,
  onChange
}: {
  title: string;
  section: EditableSection;
  values: Record<string, string>;
  disabled: boolean;
  onChange: (section: EditableSection, key: string, value: string) => void;
}) {
  return (
    <Panel>
      <PanelHeader title={title} meta={disabled ? "Locked" : "Editable"} />
      <div className="grid gap-3 p-4 md:grid-cols-2">
        {Object.entries(values).map(([key, value]) => (
          <label key={key} className="block">
            <span className="text-xs font-medium text-ink/55">{key}</span>
            {key === "paper_maker_fill_policy" ? (
              <select
                disabled={disabled}
                value={value}
                onChange={(event) => onChange(section, key, event.target.value)}
                className="mt-1 h-10 w-full rounded-sm border border-line bg-white px-3 text-sm text-ink disabled:bg-panel disabled:text-ink/55"
              >
                <option value="touch_after_quote_was_live">touch_after_quote_was_live</option>
                <option value="none">none</option>
              </select>
            ) : (
              <input
                disabled={disabled}
                value={value}
                onChange={(event) => onChange(section, key, event.target.value)}
                className="mt-1 h-10 w-full rounded-sm border border-line bg-white px-3 text-sm text-ink disabled:bg-panel disabled:text-ink/55"
              />
            )}
          </label>
        ))}
      </div>
    </Panel>
  );
}

function DiffTable({ changes }: { changes: ConfigChange[] }) {
  return (
    <div className="overflow-auto border border-line">
      <table className="w-full min-w-[360px] text-left text-sm">
        <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
          <tr>
            <th className="px-3 py-2">Field</th>
            <th className="px-3 py-2">Old</th>
            <th className="px-3 py-2">New</th>
          </tr>
        </thead>
        <tbody>
          {changes.length ? changes.map((change) => (
            <tr key={change.field} className="border-b border-line last:border-b-0">
              <td className="px-3 py-2 font-medium">{change.field}</td>
              <td className="px-3 py-2 text-ink/60">{compact(change.old)}</td>
              <td className="px-3 py-2 text-ink">{compact(change.new)}</td>
            </tr>
          )) : (
            <tr><td colSpan={3}><EmptyState label="No pending changes" /></td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function Guardrails({ values }: { values: Record<string, boolean | string> }) {
  return (
    <Panel>
      <PanelHeader title="Live Gates And Secrets" meta="Deployment-level only">
        <LockKeyhole className="h-4 w-4 text-ink/55" />
      </PanelHeader>
      <div className="divide-y divide-line">
        {Object.entries(values).map(([key, value]) => (
          <div key={key} className="flex items-center justify-between gap-3 px-4 py-3">
            <div className="flex min-w-0 items-center gap-2">
              <ShieldCheck className="h-4 w-4 shrink-0 text-ink/45" />
              <span className="truncate text-sm text-ink">{key}</span>
            </div>
            <Pill tone={pillTone(key, value)}>{String(value)}</Pill>
          </div>
        ))}
      </div>
    </Panel>
  );
}

function toForm(config: RuntimeConfig): FormState {
  return {
    strategy: stringifySection(config.strategy),
    risk: stringifySection(config.risk),
    paper: stringifySection(config.paper)
  };
}

function stringifySection(section: Record<string, string | number>) {
  return Object.fromEntries(Object.entries(section).map(([key, value]) => [key, String(value)]));
}

function setField(setForm: React.Dispatch<React.SetStateAction<FormState | null>>) {
  return (section: EditableSection, key: string, value: string) => {
    setForm((current) => current ? { ...current, [section]: { ...current[section], [key]: value } } : current);
  };
}

function buildPatch(current: RuntimeConfig, form: FormState): RuntimeConfigPatch {
  const patch: RuntimeConfigPatch = {};
  for (const section of ["strategy", "risk", "paper"] as const) {
    const changes: Record<string, string | number> = {};
    for (const [key, raw] of Object.entries(form[section])) {
      const oldValue = current[section][key];
      const nextValue = key === "paper_maker_fill_policy" ? raw : parseNumeric(raw);
      if (String(oldValue) !== String(nextValue)) {
        changes[key] = nextValue;
      }
    }
    if (Object.keys(changes).length) {
      patch[section] = changes;
    }
  }
  return patch;
}

function diffFromPatch(current: RuntimeConfig | undefined, patch: RuntimeConfigPatch): ConfigChange[] {
  if (!current) {
    return [];
  }
  const changes: ConfigChange[] = [];
  for (const section of ["strategy", "risk", "paper"] as const) {
    for (const [key, value] of Object.entries(patch[section] ?? {})) {
      changes.push({ field: `${section}.${key}`, old: current[section][key], new: value });
    }
  }
  return changes;
}

function parseNumeric(value: string) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? value : value;
}

function pillTone(key: string, value: boolean | string): "neutral" | "good" | "warn" | "danger" {
  if (key.includes("private_key") || key.includes("bearer_token") || key.includes("azure_storage")) {
    return value ? "good" : "warn";
  }
  if (key === "execution_mode") {
    return value === "live" ? "danger" : "good";
  }
  if (key.includes("allow_live") || key.includes("enable_taker") || key.includes("emergency")) {
    return value ? "danger" : "good";
  }
  return "neutral";
}
