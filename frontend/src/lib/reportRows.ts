import type { JsonRecord } from "@/lib/types";

export type ReportColumn = {
  key: string;
  label: string;
};

export const REGIME_PROFILE_ORDER = [
  "static",
  "dynamic_safety_only",
  "dynamic_quote_style",
  "full_deterministic_profile"
] as const;

export const REGIME_PROFILE_COLUMNS: ReportColumn[] = [
  { key: "profile", label: "Profile" },
  { key: "net_pnl", label: "Net PnL" },
  { key: "delta_vs_static", label: "Delta vs Static" },
  { key: "regime_frequency", label: "Regime Frequency" },
  { key: "regime_time_share", label: "Regime Time Share" },
  { key: "fills", label: "Fills" },
  { key: "cancels", label: "Cancels" },
  { key: "skipped_orders", label: "Skipped Orders" }
];

export const CALIBRATION_COLUMNS: ReportColumn[] = [
  { key: "q_bucket", label: "Q Bucket" },
  { key: "decision_count", label: "Decisions" },
  { key: "avg_q_up", label: "Avg q_up" },
  { key: "observed_up_frequency", label: "Observed Up" },
  { key: "calibration_error", label: "Calibration Error" },
  { key: "brier_score", label: "Brier Score" }
];

export const FILL_MODEL_COLUMNS: ReportColumn[] = [
  { key: "fill_model", label: "Fill Model" },
  { key: "net_pnl", label: "Net PnL" },
  { key: "max_drawdown", label: "Max Drawdown" },
  { key: "fills", label: "Fills" },
  { key: "fill_rate", label: "Fill Rate" },
  { key: "cancel_fill_ratio", label: "Cancel/Fill" },
  { key: "queue_proxy", label: "Queue Proxy" }
];

export const QUEUE_PROXY_COLUMNS: ReportColumn[] = [
  { key: "fill_model", label: "Fill Model" },
  { key: "queue_proxy_mode", label: "Mode" },
  { key: "queue_proxy_enabled", label: "Enabled" },
  { key: "queue_proxy_eligible_markets", label: "Eligible Markets" },
  { key: "queue_proxy_ineligible_markets", label: "Ineligible Markets" },
  { key: "queue_proxy_eligibility_rate", label: "Eligibility Rate" },
  { key: "queue_proxy_fills", label: "Queue Fills" },
  { key: "queue_proxy_fill_rate", label: "Fill Rate" },
  { key: "avg_size_ahead", label: "Avg Size Ahead" },
  { key: "ineligible_reasons", label: "Ineligible Reasons" }
];

export function selectRegimeProfileRows(report: unknown): JsonRecord[] {
  const comparisons = firstRecordArray(report, ["/result/comparisons", "/comparisons"]);
  const profiles = firstRecordArray(report, ["/result/profiles", "/profiles"]);
  if (!comparisons.length && !profiles.length) {
    return [];
  }

  const comparisonByProfile = rowsByProfile(comparisons);
  const profileByProfile = rowsByProfile(profiles);

  return REGIME_PROFILE_ORDER.map((profile) => {
    const comparison = comparisonByProfile.get(profile);
    const profileSummary = profileByProfile.get(profile);
    return {
      profile,
      net_pnl: firstDefined(comparison?.net_pnl, profileSummary?.net_pnl),
      delta_vs_static: firstDefined(comparison?.delta_vs_static, profile === "static" ? "0" : undefined),
      regime_frequency: firstDefined(comparison?.regime_frequency, profileSummary?.regime_frequency),
      regime_time_share: firstDefined(comparison?.regime_time_share, profileSummary?.regime_time_share),
      fills: profileSummary?.fills,
      cancels: profileSummary?.cancels,
      skipped_orders: firstDefined(
        profileSummary?.skipped_by_profile,
        profileSummary?.skipped_orders,
        comparison?.skipped_by_profile,
        comparison?.skipped_orders
      )
    };
  });
}

export function selectCalibrationBucketRows(report: unknown): JsonRecord[] {
  const qBuckets = firstRecordObject(report, ["/result/q_up_buckets", "/q_up_buckets"]);
  if (qBuckets) {
    return Object.entries(qBuckets).map<JsonRecord>(([bucket, value]) => ({
      q_bucket: bucket,
      ...pickCalibrationFields(asRecord(value))
    }));
  }

  const rows = firstRecordArray(report, [
    "/result/buckets",
    "/result/calibration_buckets",
    "/result/calibration/buckets",
    "/buckets",
    "/calibration_buckets"
  ]);
  return rows
    .filter((row) => row.market_id === undefined && row.market_slug === undefined)
    .map<JsonRecord>((row) => ({
      q_bucket: firstDefined(row.q_bucket, row.bucket, row.label),
      ...pickCalibrationFields(row)
    }))
    .filter((row) => row.q_bucket !== undefined || row.decision_count !== undefined);
}

export function selectFillModelSummaryRows(report: unknown): JsonRecord[] {
  const rows = firstRecordArray(report, [
    "/result/fill_models",
    "/result/fill_model_sensitivity",
    "/result/fill_model_results",
    "/result/results",
    "/fill_models",
    "/fill_model_sensitivity",
    "/fill_model_results"
  ]);

  return rows
    .filter((row) => row.market_id === undefined && row.market_slug === undefined)
    .map((row) => ({
      fill_model: firstDefined(row.fill_model, row.name),
      net_pnl: row.net_pnl,
      max_drawdown: row.max_drawdown,
      fills: row.fills,
      fill_rate: row.fill_rate,
      cancel_fill_ratio: row.cancel_fill_ratio,
      queue_proxy: firstDefined(row.queue_proxy, pointer(row, "/replay_metrics/queue_proxy/status"))
    }))
    .filter((row) => row.fill_model !== undefined);
}

export function selectQueueProxyRows(report: unknown): JsonRecord[] {
  return selectFillModelRawRows(report)
    .map((row) => {
      const queue = asRecord(pointer(row, "/replay_metrics/queue_proxy")) ?? asRecord(row.queue_proxy) ?? {};
      return {
        fill_model: firstDefined(row.fill_model, row.name),
        queue_proxy_mode: firstDefined(row.queue_proxy_mode, queue.queue_proxy_mode),
        queue_proxy_enabled: firstDefined(row.queue_proxy_enabled, queue.queue_proxy_enabled),
        queue_proxy_eligible_markets: firstDefined(row.queue_proxy_eligible_markets, queue.queue_proxy_eligible_markets),
        queue_proxy_ineligible_markets: firstDefined(row.queue_proxy_ineligible_markets, queue.queue_proxy_ineligible_markets),
        queue_proxy_eligibility_rate: firstDefined(row.queue_proxy_eligibility_rate, queue.queue_proxy_eligibility_rate),
        queue_proxy_fills: firstDefined(row.queue_proxy_fills, queue.queue_proxy_fills),
        queue_proxy_fill_rate: firstDefined(row.queue_proxy_fill_rate, queue.queue_proxy_fill_rate),
        avg_size_ahead: firstDefined(row.avg_size_ahead, queue.avg_size_ahead),
        ineligible_reasons: firstDefined(row.ineligible_reasons, queue.ineligible_reasons)
      };
    })
    .filter((row) => row.fill_model !== undefined);
}

function selectFillModelRawRows(report: unknown): JsonRecord[] {
  return firstRecordArray(report, [
    "/result/fill_models",
    "/result/fill_model_sensitivity",
    "/result/fill_model_results",
    "/result/results",
    "/fill_models",
    "/fill_model_sensitivity",
    "/fill_model_results"
  ]).filter((row) => row.market_id === undefined && row.market_slug === undefined);
}

function pickCalibrationFields(row: JsonRecord | null): JsonRecord {
  return {
    decision_count: row?.decision_count,
    avg_q_up: row?.avg_q_up,
    observed_up_frequency: row?.observed_up_frequency,
    calibration_error: row?.calibration_error,
    brier_score: row?.brier_score
  };
}

function rowsByProfile(rows: JsonRecord[]): Map<string, JsonRecord> {
  const byProfile = new Map<string, JsonRecord>();
  rows.forEach((row) => {
    const profile = normalizedProfileName(firstDefined(row.profile, row.name, row.candidate));
    if (profile) {
      byProfile.set(profile, row);
    }
  });
  return byProfile;
}

function normalizedProfileName(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  return value === "static_baseline" ? "static" : value;
}

function firstRecordArray(value: unknown, paths: string[]): JsonRecord[] {
  for (const root of candidateRoots(value)) {
    for (const path of paths) {
      const found = pointer(root, path);
      if (Array.isArray(found)) {
        return found.map(asRecord).filter((row): row is JsonRecord => Boolean(row));
      }
    }
  }
  return [];
}

function firstRecordObject(value: unknown, paths: string[]): JsonRecord | null {
  for (const root of candidateRoots(value)) {
    for (const path of paths) {
      const found = asRecord(pointer(root, path));
      if (found) {
        return found;
      }
    }
  }
  return null;
}

function candidateRoots(value: unknown): JsonRecord[] {
  if (Array.isArray(value)) {
    return value.map(asRecord).filter((row): row is JsonRecord => Boolean(row));
  }
  const record = asRecord(value);
  if (!record) {
    return [];
  }
  const nestedReport = asRecord(record.report);
  return nestedReport ? [nestedReport, record] : [record];
}

function pointer(record: JsonRecord | null | undefined, path: string): unknown {
  if (!record) {
    return undefined;
  }
  return path
    .split("/")
    .slice(1)
    .reduce<unknown>((current, key) => asRecord(current)?.[key], record);
}

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}

function firstDefined(...values: unknown[]) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}
