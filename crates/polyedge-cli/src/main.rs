use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use polyedge_api::{app_with_shutdown, benchmark_snapshot};
use polyedge_config::{embedded_git_sha, RuntimeRole, RuntimeSettings};
use polyedge_reporting::research::{
    advance_funded_ladder, advance_funded_manifest, expire_funded_manifest,
    initialize_funded_manifest_after_canary, load_default_exclusions, publish_daily_directory,
    publish_normalized_snapshot, restore_normalized_snapshot, run_audit, run_azure_freshness,
    run_backfill, run_baseline, run_begin_shadow_correction, run_build_cumulative_wallet_snapshot,
    run_build_markets, run_build_replay_index, run_calibration, run_chart_backfill,
    run_complete_shadow_correction, run_evaluate_profitability, run_execution_quality,
    run_final_report, run_loss_diagnostics, run_loss_regime_oos,
    run_materialize_projected_campaign, run_ml_calibrate, run_normalize, run_publish_projected_day,
    run_queue_audit, run_regimes, run_replay, run_sample_size, run_sweep, run_validate_prospective,
    stop_funded_manifest_from_stage_block, AdvanceFundedLadderOptions,
    AdvanceFundedManifestOptions, AuditOptions, AzureFreshnessOptions, BackfillOptions,
    BaselineOptions, BeginShadowCorrectionOptions, BuildMarketsOptions, CalibrationOptions,
    ChartBackfillOptions, CompleteShadowCorrectionOptions, CumulativeWalletSnapshotOptions,
    ExcludedTimeWindow, ExecutionQualityOptions, ExpireFundedManifestOptions, FillModel,
    FinalReportOptions, InitializeFundedManifestOptions, LossDiagnosticsOptions,
    LossRegimeOosOptions, MaterializeProjectedCampaignOptions, MlCalibrateOptions,
    NormalizeOptions, ProfitabilityEvaluationOptions, ProspectiveValidationOptions,
    PublishNormalizedSnapshotOptions, PublishProjectedDayOptions, QueueAuditOptions,
    RegimesOptions, ReplayIndexOptions, ReplayOptions, RestoreNormalizedSnapshotOptions,
    SampleSizeOptions, SettlementCarryOptions, StopFundedManifestFromStageBlockOptions,
    SweepOptions, WarningSeverity, DEFAULT_EXCLUSION_FILE, DEFAULT_FROZEN_CANDIDATES_FILE,
    DEFAULT_PROSPECTIVE_SINCE,
};
use polyedge_reporting::{
    build_pnl_report, run_backtest, BacktestConfig, ReplayBacktester, REPLAY_BUFFER_BYTES,
};
use polyedge_storage::{
    AzureBlobClient, AzureBlobItem, BlobLeaseAcquireResult, ImmutableBlobWrite,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command as ProcessCommand};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

const LEGACY_RING_BLOB_PREFIX: &str = "events-oci-dual";
const RING_QUARANTINE_BLOB_PREFIX: &str =
    "events-oci-quarantine-v1/invalid-recorder-sequence-proof";
const MAX_RING_QUARANTINE_SOURCE_BYTES: u64 = 512 * 1024 * 1024;
const QSET_V2_CONTAINER: &str = "polyedge-shadow-qset-events";
const QSET_V2_PREFIX: &str = "shadow-events/campaign-2026-08-22-qset-v2";
const QSET_V2_START: &str = "2026-08-22";
const QSET_V2_EXPECTED_DAY_BLOBS: usize = 24 * 60;
const QSET_V2_MAX_DAY_BLOBS: usize = 2_000;
const QSET_V2_MAX_DAY_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const QSET_V2_FREEZE_CONTAINER: &str = "polyedge-qset-control";
const QSET_V2_FREEZE_BLOB: &str = "reports/research/shadow/campaigns/campaign-2026-08-22-qset-v2/control/code-freeze/source-8017ed1d036ba502ae0596376e54d781af350307d71cb174e24e3f2fa16fd3e1.json";
const QSET_V2_FREEZE_SHA256: &str =
    "sha256:8017ed1d036ba502ae0596376e54d781af350307d71cb174e24e3f2fa16fd3e1";
const QSET_V2_MAX_FREEZE_BYTES: u64 = 1024 * 1024;
const QSET_V3_CONTAINER: &str = "polyedge-shadow-qset-v3-events";
const QSET_V3_PREFIX: &str = "shadow-events/campaign-2026-08-23-qset-v3";
const QSET_V3_DATES: [&str; 2] = ["2026-08-23", "2026-08-24"];
const QSET_V3_FREEZE_CONTAINER: &str = "polyedge-qset-v3-control";
const QSET_V4_CONTAINER: &str = "polyedge-shadow-qset-v4-events";
const QSET_V4_PREFIX: &str = "shadow-events/campaign-2026-08-24-qset-v4";
const QSET_V4_DATES: [&str; 2] = ["2026-08-24", "2026-08-25"];
const QSET_V4_FREEZE_CONTAINER: &str = "polyedge-qset-v4-control";

struct QsetSealConfig {
    name: &'static str,
    campaign_id: &'static str,
    container: &'static str,
    prefix: &'static str,
    dates: &'static [&'static str; 2],
    freeze_container: &'static str,
    freeze_blob_prefix: &'static str,
    validation_schema: &'static str,
    seal_schema: &'static str,
}

const QSET_V3_SEAL_CONFIG: QsetSealConfig = QsetSealConfig {
    name: "qset-v3",
    campaign_id: "campaign-2026-08-23-qset-v3",
    container: QSET_V3_CONTAINER,
    prefix: QSET_V3_PREFIX,
    dates: &QSET_V3_DATES,
    freeze_container: QSET_V3_FREEZE_CONTAINER,
    freeze_blob_prefix:
        "reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-",
    validation_schema: "polyedge.qset_v3_closed_day_validation.v1",
    seal_schema: "polyedge.qset_v3_closed_day_seal.v1",
};

const QSET_V4_SEAL_CONFIG: QsetSealConfig = QsetSealConfig {
    name: "qset-v4",
    campaign_id: "campaign-2026-08-24-qset-v4",
    container: QSET_V4_CONTAINER,
    prefix: QSET_V4_PREFIX,
    dates: &QSET_V4_DATES,
    freeze_container: QSET_V4_FREEZE_CONTAINER,
    freeze_blob_prefix:
        "reports/research/shadow/campaigns/campaign-2026-08-24-qset-v4/control/code-freeze/source-",
    validation_schema: "polyedge.qset_v4_closed_day_validation.v1",
    seal_schema: "polyedge.qset_v4_closed_day_seal.v1",
};

#[derive(Parser)]
#[command(name = "polyedge-rs")]
#[command(about = "PolyEdge Rust backend CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Api {
        #[arg(long, default_value = "127.0.0.1:8081")]
        bind: String,
    },
    Run {
        #[arg(long, default_value = "127.0.0.1:8081")]
        bind: String,
    },
    Discover,
    ConfirmSource,
    Backtest {
        #[arg(long)]
        path: PathBuf,
    },
    Report {
        #[arg(long)]
        prefix: PathBuf,
    },
    BenchIngest {
        #[arg(long, default_value_t = 100_000)]
        events: usize,
    },
    BenchReplay {
        #[arg(long)]
        path: PathBuf,
    },
    BenchAzureReplay {
        #[arg(long)]
        account: String,
        #[arg(long, default_value = "bot-events")]
        container: String,
        #[arg(long)]
        prefix: String,
        #[arg(long, default_value = "AZURE_STORAGE_SAS")]
        sas_env: String,
        #[arg(long)]
        max_blobs: Option<usize>,
        #[arg(long)]
        max_bytes: Option<u64>,
        #[arg(long, default_value_t = 8)]
        prefetch_blobs: usize,
    },
    /// Upload locally sealed recorder segments without listing Azure blobs.
    RingUpload {
        #[arg(long, default_value = "/srv/polyedge-ring")]
        root: PathBuf,
        #[arg(
            long,
            default_value = "events-oci-hot7-v1",
            env = "POLYEDGE_RING_BLOB_PREFIX"
        )]
        blob_prefix: String,
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(
            long,
            default_value = "bot-events",
            env = "AZURE_STORAGE_CONTAINER_NAME"
        )]
        container: String,
        #[arg(long, default_value_t = 48)]
        retention_hours: u64,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    /// Preserve and verify an approved pre-boundary recorder quarantine.
    RingQuarantineResolve {
        #[arg(long, default_value = "/srv/polyedge-ring")]
        root: PathBuf,
        #[arg(long)]
        receipt_id: String,
        #[arg(long)]
        formal_boundary_epoch: i64,
        #[arg(long)]
        approval_reference: String,
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(
            long,
            default_value = "bot-events",
            env = "AZURE_STORAGE_CONTAINER_NAME"
        )]
        container: String,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    /// Seal one closed UTC day from the isolated qset-v2 successor campaign.
    SealQsetV2Day {
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(long)]
        date: String,
        #[arg(long)]
        validate_only: bool,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    /// Seal one approved closed UTC day from the isolated qset-v3 campaign.
    SealQsetV3Day {
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(long)]
        date: String,
        /// Exact reviewed source-freeze blob in polyedge-qset-v3-control.
        #[arg(long)]
        source_freeze_blob: String,
        /// SHA-256 of --source-freeze-blob, prefixed with sha256:.
        #[arg(long)]
        source_freeze_sha256: String,
        #[arg(long)]
        validate_only: bool,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    /// Seal one approved closed UTC day from the isolated qset-v4 campaign.
    SealQsetV4Day {
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(long)]
        date: String,
        /// Exact reviewed source-freeze blob in polyedge-qset-v4-control.
        #[arg(long)]
        source_freeze_blob: String,
        /// SHA-256 of --source-freeze-blob, prefixed with sha256:.
        #[arg(long)]
        source_freeze_sha256: String,
        #[arg(long)]
        validate_only: bool,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    BenchApiSnapshot {
        #[arg(long, default_value_t = 10_000)]
        iterations: usize,
    },
    Research {
        #[command(subcommand)]
        command: ResearchCommand,
    },
}

#[derive(Subcommand)]
enum ResearchCommand {
    /// Serialize an entire research writer process with a finite Azure Blob
    /// lease. The child is killed if lease renewal is ever lost.
    WithAzureLease {
        #[arg(long)]
        account: String,
        #[arg(long)]
        container: String,
        #[arg(long)]
        blob: String,
        #[arg(long, default_value_t = 60)]
        lease_seconds: u32,
        #[arg(long, default_value_t = 20)]
        renew_seconds: u64,
        #[arg(long, default_value_t = 600)]
        wait_seconds: u64,
        #[arg(last = true, required = true, num_args = 1.., allow_hyphen_values = true)]
        command: Vec<String>,
    },
    BeginShadowCorrection {
        #[arg(long)]
        campaign_id: String,
        #[arg(long)]
        correction_id: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        through: String,
        #[arg(long)]
        reason: String,
        #[arg(
            long,
            default_value = "reports/research/shadow/corrections/active.json"
        )]
        out: PathBuf,
    },
    CompleteShadowCorrection {
        #[arg(long)]
        campaign_id: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        through: String,
        #[arg(
            long,
            default_value = "reports/research/shadow/corrections/active.json"
        )]
        out: PathBuf,
    },
    Audit {
        #[arg(long, default_value = "data/events.jsonl")]
        input: PathBuf,
        #[arg(long, default_value = "reports/research/data_audit.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/data_audit.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
        #[arg(long)]
        settlement_carry_input: Option<PathBuf>,
        #[arg(long)]
        settlement_carry_manifest: Option<PathBuf>,
        #[arg(long)]
        settlement_carry_campaign_id: Option<String>,
        #[arg(long)]
        settlement_carry_source_account: Option<String>,
        #[arg(long)]
        settlement_carry_source_container: Option<String>,
        #[arg(long)]
        market_day: Option<String>,
    },
    ExecutionQuality {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "reports/research/execution_quality.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/execution_quality.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    /// Build diagnostic-only, one-row-per-lifecycle facts from an explicit
    /// immutable normalized Protocol-v3 snapshot.
    LossDiagnostics {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        settlement_carry_input: Option<PathBuf>,
        #[arg(long)]
        settlement_carry_manifest: Option<PathBuf>,
        #[arg(long)]
        settlement_carry_campaign_id: Option<String>,
        #[arg(long)]
        settlement_carry_source_account: Option<String>,
        #[arg(long)]
        settlement_carry_source_container: Option<String>,
        #[arg(long)]
        market_day: Option<String>,
    },
    Normalize {
        #[arg(long, default_value = "data/events.jsonl")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/normalized")]
        out: PathBuf,
        #[arg(long, default_value = "jsonl-indexed")]
        format: String,
        #[arg(long, default_value_t = false, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
        overwrite: bool,
        /// Preserve decision-grade state and trades while sampling high-rate books.
        #[arg(long, default_value_t = false, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
        decision_grade_projection: bool,
    },
    /// Publish an already-produced projected UTC day as an immutable,
    /// content-addressed cache bundle. The manifest is written last.
    PublishProjectedDay {
        #[arg(long)]
        normalized: PathBuf,
        #[arg(long)]
        date: String,
        #[arg(long)]
        campaign_id: String,
        #[arg(long)]
        cache_root: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = true, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
        require_azure_source: bool,
        #[arg(long)]
        expected_source_container: Option<String>,
    },
    /// Verify and materialize sealed projected-day bundles through an explicit
    /// UTC cutoff. Open/current-day data is never included.
    MaterializeProjectedCampaign {
        #[arg(long)]
        since: String,
        #[arg(long)]
        through: String,
        #[arg(long)]
        campaign_id: String,
        #[arg(long)]
        cache_root: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, default_value_t = true, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
        require_azure_source: bool,
        #[arg(long)]
        expected_source_container: Option<String>,
    },
    QueueAudit {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        markets: PathBuf,
        #[arg(long, default_value = "reports/research/queue_evidence_audit.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/queue_evidence_audit.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    BuildMarkets {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/markets_summary.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
        #[arg(long)]
        settlement_carry_input: Option<PathBuf>,
        #[arg(long)]
        settlement_carry_manifest: Option<PathBuf>,
        #[arg(long)]
        settlement_carry_campaign_id: Option<String>,
        #[arg(long)]
        settlement_carry_source_account: Option<String>,
        #[arg(long)]
        settlement_carry_source_container: Option<String>,
        #[arg(long)]
        market_day: Option<String>,
    },
    Replay {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        markets: PathBuf,
        #[arg(long)]
        strategy_config: Option<PathBuf>,
        #[arg(long, default_value = "touch_after_250ms")]
        fill_model: String,
        #[arg(long, default_value = "reports/research/replay_touch_after_250ms.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/replay_touch_after_250ms.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    Baseline {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        markets: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/baseline_static_all_fill_models.json"
        )]
        out: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/baseline_static_all_fill_models.md"
        )]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    Regimes {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        markets: PathBuf,
        #[arg(long, default_value = "touch_after_250ms")]
        fill_model: String,
        #[arg(long)]
        profile_config: Option<PathBuf>,
        #[arg(long, default_value = "reports/research/regime_profiles.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/regime_profiles.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    Sweep {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        markets: PathBuf,
        #[arg(long)]
        search: Option<PathBuf>,
        #[arg(long, default_value = "walk_forward")]
        split: String,
        #[arg(long, default_value_t = 500)]
        max_experiments: usize,
        #[arg(long, default_value = "reports/research/parameter_sweep.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/parameter_sweep.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    LossRegimeOos {
        #[arg(long)]
        facts: PathBuf,
        #[arg(long)]
        queue_evidence: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        source_campaign_id: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        markdown: PathBuf,
    },
    Calibration {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/markets.json")]
        markets: PathBuf,
        #[arg(long, default_value = "reports/research/calibration.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/calibration.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    SampleSize {
        #[arg(
            long,
            default_value = "reports/research/baseline_static_all_fill_models.json"
        )]
        results: PathBuf,
        #[arg(long, default_value = "reports/research/sample_size.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/sample_size.md")]
        markdown: PathBuf,
    },
    Report {
        #[arg(long, default_value = "reports/research")]
        reports_dir: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/final_strategy_research_report.json"
        )]
        out: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/final_strategy_research_report.md"
        )]
        markdown: PathBuf,
    },
    MlCalibrate {
        #[arg(long, default_value = "reports/research/ml_calibrate.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/ml_calibrate.md")]
        markdown: PathBuf,
    },
    AzureFreshness {
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(
            long,
            default_value = "bot-events",
            env = "AZURE_STORAGE_CONTAINER_NAME"
        )]
        container: String,
        #[arg(long, default_value = "events/")]
        prefix: String,
        #[arg(long, default_value_t = 300)]
        max_age_seconds: u64,
        #[arg(long, default_value_t = 60)]
        expected_interval_seconds: u64,
        #[arg(long, default_value = "data_quality/freshness/latest.json")]
        out: PathBuf,
        #[arg(long = "sas-env")]
        sas_env: Option<String>,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    /// Publish a content-addressed immutable normalized day for reuse by later jobs.
    PublishNormalizedSnapshot {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        date: String,
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(
            long,
            default_value = "bot-events",
            env = "AZURE_STORAGE_CONTAINER_NAME"
        )]
        container: String,
        #[arg(long, default_value = "data/research/normalized/v1")]
        prefix: String,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    /// Restore and hash-verify the normalized day selected by its manifest-last pointer.
    RestoreNormalizedSnapshot {
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        date: String,
        #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
        account: String,
        #[arg(
            long,
            default_value = "bot-events",
            env = "AZURE_STORAGE_CONTAINER_NAME"
        )]
        container: String,
        #[arg(long, default_value = "data/research/normalized/v1")]
        prefix: String,
        #[arg(long, env = "AZURE_CLIENT_ID")]
        client_id: Option<String>,
    },
    ValidateProspective {
        #[arg(long, default_value = DEFAULT_PROSPECTIVE_SINCE)]
        since: String,
        #[arg(long, default_value = DEFAULT_FROZEN_CANDIDATES_FILE)]
        candidates: PathBuf,
        #[arg(long, default_value = "reports/research/daily")]
        reports_dir: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/prospective/prospective_validation.json"
        )]
        out: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/prospective/prospective_validation.md"
        )]
        markdown: PathBuf,
        /// Require a verified COMPLETE atomic daily bundle for this UTC date.
        /// If absent/incomplete, report waiting and preserve the prior output.
        #[arg(long)]
        expected_daily_date: Option<String>,
    },
    /// Atomically package and publish a generated UTC daily research directory.
    PublishDailyBundle {
        #[arg(long)]
        date: String,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        input_sha256: String,
        /// Runtime role whose continuous provenance must own the complete day.
        #[arg(long, default_value = "primary")]
        expected_runtime_role: String,
        #[arg(long)]
        source_dir: PathBuf,
        #[arg(long, default_value = "reports/research/daily")]
        output_root: PathBuf,
        #[arg(long)]
        data_audit: PathBuf,
    },
    /// Bind the cumulative campaign wallet replay to its normalized input and
    /// emit an artifact for inclusion in the immutable daily bundle.
    BuildCumulativeWallet {
        #[arg(long)]
        regimes: PathBuf,
        #[arg(long)]
        campaign_manifest: PathBuf,
        /// Immutable protocol-v3 campaign contract. Omit only for historical
        /// schema-v2 wallet reconstruction.
        #[arg(long)]
        campaign_contract: Option<PathBuf>,
        #[arg(long)]
        snapshot_date: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Advance the durable funded ladder from exact hash-bound prior state,
    /// observation and optional one-shot human stage grant.
    AdvanceFundedLadder {
        #[arg(long)]
        prior_state: PathBuf,
        #[arg(long)]
        prior_state_sha256: String,
        #[arg(long)]
        observation: PathBuf,
        #[arg(long)]
        observation_sha256: String,
        #[arg(long, requires = "grant_sha256")]
        grant: Option<PathBuf>,
        #[arg(long, requires = "grant")]
        grant_sha256: Option<String>,
        #[arg(long)]
        out: PathBuf,
    },
    /// Initialize checkpoint 1 from exact hash-bound, reconciled protocol-v3
    /// canary evidence and its already-consumed one-shot human grant.
    InitializeFundedManifest {
        #[arg(long)]
        shadow_manifest: PathBuf,
        #[arg(long)]
        shadow_manifest_sha256: String,
        #[arg(long)]
        canary_evidence: PathBuf,
        #[arg(long)]
        canary_evidence_blob_name: String,
        #[arg(long)]
        canary_evidence_sha256: String,
        #[arg(long)]
        human_grant_consumption: PathBuf,
        #[arg(long)]
        human_grant_consumption_sha256: String,
        #[arg(long)]
        terminal_evidence: PathBuf,
        #[arg(long)]
        terminal_evidence_blob_name: String,
        #[arg(long)]
        terminal_evidence_sha256: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Advance targets 5/25/100/200 in the canonical API-visible manifest.
    AdvanceFundedManifest {
        #[arg(long)]
        prior_manifest: PathBuf,
        #[arg(long)]
        prior_manifest_sha256: String,
        #[arg(long)]
        observation: PathBuf,
        #[arg(long)]
        observation_sha256: String,
        #[arg(long, requires = "grant_sha256")]
        grant: Option<PathBuf>,
        #[arg(long, requires = "grant")]
        grant_sha256: Option<String>,
        #[arg(long, requires_all = ["next_execution_model_blob_uri", "next_execution_model_sha256"])]
        next_execution_model: Option<PathBuf>,
        #[arg(long, requires_all = ["next_execution_model", "next_execution_model_sha256"])]
        next_execution_model_blob_uri: Option<String>,
        #[arg(long, requires_all = ["next_execution_model", "next_execution_model_blob_uri"])]
        next_execution_model_sha256: Option<String>,
        #[arg(long)]
        out: PathBuf,
    },
    /// Consume an immutable funded stage block and move the exact canonical
    /// campaign into absorbing stopped_no_go. This command never authorizes an order.
    StopFundedManifestFromStageBlock {
        #[arg(long)]
        prior_manifest: PathBuf,
        #[arg(long)]
        prior_manifest_sha256: String,
        #[arg(long)]
        stage_block: PathBuf,
        #[arg(long)]
        stage_block_sha256: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Move an exact expired active funded campaign into absorbing stopped_no_go.
    ExpireFundedManifest {
        #[arg(long)]
        prior_manifest: PathBuf,
        #[arg(long)]
        prior_manifest_sha256: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Evaluate shadow profitability and publish a fail-closed research
    /// manifest. This command never authorizes or arms funded execution.
    EvaluateProfitability {
        #[arg(long, default_value = "reports/research/shadow/daily")]
        daily_root: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/prospective/prospective_validation.json"
        )]
        prospective: PathBuf,
        #[arg(long, default_value = "research/configs/profitability_gate.yaml")]
        gate_config: PathBuf,
        #[arg(
            long,
            default_value = "reports/research/venue-probe/effective_queue_model.json"
        )]
        execution_model: PathBuf,
        #[arg(long, default_value = "reports/research/profitability/latest.json")]
        out: PathBuf,
    },
    BuildReplayIndex {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/replay-index/latest")]
        out: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    ChartBackfill {
        #[arg(long, default_value = "data/research/normalized")]
        input: PathBuf,
        #[arg(long, default_value = "reports/jobs/latest/chart-backfill.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/jobs/latest/chart-backfill.md")]
        markdown: PathBuf,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
    },
    Backfill {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long, default_value = "all")]
        task: String,
        #[arg(long = "exclude-file", default_value = DEFAULT_EXCLUSION_FILE)]
        exclude_file: PathBuf,
        #[arg(long = "exclude-window")]
        exclude_window: Vec<String>,
        #[arg(long, default_value = "reports/research/backfill/latest.json")]
        out: PathBuf,
        #[arg(long, default_value = "reports/research/backfill/latest.md")]
        markdown: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();
    let cli = Cli::parse();
    let settings = RuntimeSettings::from_env().context("loading runtime settings")?;
    if settings.live_requested() {
        match settings.validate_live_gates(false) {
            Ok(()) => bail!("Rust backend refuses live mode even when config gates pass."),
            Err(error) => bail!("Rust backend refuses live mode: {error}"),
        }
    }
    match cli.command {
        Command::Api { bind } | Command::Run { bind } => serve(settings, bind).await,
        Command::Discover => {
            let markets =
                polyedge_feeds::discover_markets(&settings).context("discovering markets")?;
            print_json(json!({
                "count": markets.len(),
                "markets": markets,
                "backend_impl": "rust",
                "runtime_role": settings.deploy.runtime_role.as_str(),
                "shadow_only": settings.deploy.runtime_role.is_shadow()
            }))
        }
        Command::ConfirmSource => print_json(confirm_source(&settings)?),
        Command::Backtest { path } => print_json(run_backtest(&path)?.as_value()),
        Command::Report { prefix } => print_json(build_pnl_report(&prefix)?),
        Command::BenchIngest { events } => print_json(bench_ingest(events)),
        Command::BenchReplay { path } => print_json(bench_replay(path)?),
        Command::BenchAzureReplay {
            account,
            container,
            prefix,
            sas_env,
            max_blobs,
            max_bytes,
            prefetch_blobs,
        } => print_json(bench_azure_replay(
            account,
            container,
            prefix,
            sas_env,
            max_blobs,
            max_bytes,
            prefetch_blobs,
        )?),
        Command::RingUpload {
            root,
            blob_prefix,
            account,
            container,
            retention_hours,
            client_id,
        } => print_json(run_ring_upload(
            &root,
            &blob_prefix,
            account,
            container,
            retention_hours,
            client_id,
        )?),
        Command::RingQuarantineResolve {
            root,
            receipt_id,
            formal_boundary_epoch,
            approval_reference,
            account,
            container,
            client_id,
        } => print_json(run_ring_quarantine_resolve(
            &root,
            &receipt_id,
            formal_boundary_epoch,
            &approval_reference,
            account,
            container,
            client_id,
        )?),
        Command::SealQsetV2Day {
            account,
            date,
            validate_only,
            client_id,
        } => print_json(run_seal_qset_v2_day(
            account,
            parse_date_arg(&date)?,
            validate_only,
            client_id,
        )?),
        Command::SealQsetV3Day {
            account,
            date,
            source_freeze_blob,
            source_freeze_sha256,
            validate_only,
            client_id,
        } => print_json(run_seal_qset_day(
            &QSET_V3_SEAL_CONFIG,
            account,
            parse_date_arg(&date)?,
            &source_freeze_blob,
            &source_freeze_sha256,
            validate_only,
            client_id,
        )?),
        Command::SealQsetV4Day {
            account,
            date,
            source_freeze_blob,
            source_freeze_sha256,
            validate_only,
            client_id,
        } => print_json(run_seal_qset_day(
            &QSET_V4_SEAL_CONFIG,
            account,
            parse_date_arg(&date)?,
            &source_freeze_blob,
            &source_freeze_sha256,
            validate_only,
            client_id,
        )?),
        Command::BenchApiSnapshot { iterations } => print_json(benchmark_snapshot(iterations)),
        Command::Research { command } => run_research_command(command),
    }
}

fn run_research_command(command: ResearchCommand) -> Result<()> {
    let value = match command {
        ResearchCommand::WithAzureLease {
            account,
            container,
            blob,
            lease_seconds,
            renew_seconds,
            wait_seconds,
            command,
        } => run_with_azure_lease(
            account,
            container,
            blob,
            lease_seconds,
            renew_seconds,
            wait_seconds,
            command,
        )?,
        ResearchCommand::BeginShadowCorrection {
            campaign_id,
            correction_id,
            from,
            through,
            reason,
            out,
        } => run_begin_shadow_correction(BeginShadowCorrectionOptions {
            campaign_id,
            correction_id,
            from: parse_date_arg(&from)?,
            through: parse_date_arg(&through)?,
            reason,
            out,
        })?,
        ResearchCommand::CompleteShadowCorrection {
            campaign_id,
            from,
            through,
            out,
        } => run_complete_shadow_correction(CompleteShadowCorrectionOptions {
            campaign_id,
            from: parse_date_arg(&from)?,
            through: parse_date_arg(&through)?,
            out,
        })?,
        ResearchCommand::Audit {
            input,
            out,
            markdown,
            exclude_file,
            exclude_window,
            settlement_carry_input,
            settlement_carry_manifest,
            settlement_carry_campaign_id,
            settlement_carry_source_account,
            settlement_carry_source_container,
            market_day,
        } => run_audit(AuditOptions {
            input,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
            settlement_carry: parse_settlement_carry(
                settlement_carry_input,
                settlement_carry_manifest,
                settlement_carry_campaign_id,
                settlement_carry_source_account,
                settlement_carry_source_container,
                market_day,
            )?,
        })?,
        ResearchCommand::ExecutionQuality {
            input,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_execution_quality(ExecutionQualityOptions {
            input,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::LossDiagnostics {
            input,
            out,
            settlement_carry_input,
            settlement_carry_manifest,
            settlement_carry_campaign_id,
            settlement_carry_source_account,
            settlement_carry_source_container,
            market_day,
        } => run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out,
            settlement_carry: parse_settlement_carry(
                settlement_carry_input,
                settlement_carry_manifest,
                settlement_carry_campaign_id,
                settlement_carry_source_account,
                settlement_carry_source_container,
                market_day,
            )?,
        })?,
        ResearchCommand::LossRegimeOos {
            facts,
            queue_evidence,
            config,
            source_campaign_id,
            out,
            markdown,
        } => run_loss_regime_oos(LossRegimeOosOptions {
            facts,
            queue_evidence,
            config,
            source_campaign_id,
            out,
            markdown,
        })?,
        ResearchCommand::Normalize {
            input,
            out,
            format,
            overwrite,
            decision_grade_projection,
        } => run_normalize(NormalizeOptions {
            input,
            out,
            format,
            overwrite,
            decision_grade_projection,
        })?,
        ResearchCommand::PublishProjectedDay {
            normalized,
            date,
            campaign_id,
            cache_root,
            out,
            require_azure_source,
            expected_source_container,
        } => run_publish_projected_day(PublishProjectedDayOptions {
            normalized,
            date: parse_date_arg(&date)?,
            campaign_id,
            cache_root,
            out,
            require_azure_source,
            expected_source_container,
        })?,
        ResearchCommand::MaterializeProjectedCampaign {
            since,
            through,
            campaign_id,
            cache_root,
            out,
            manifest,
            require_azure_source,
            expected_source_container,
        } => run_materialize_projected_campaign(MaterializeProjectedCampaignOptions {
            since: parse_date_arg(&since)?,
            through: parse_date_arg(&through)?,
            campaign_id,
            cache_root,
            out,
            manifest,
            require_azure_source,
            expected_source_container,
        })?,
        ResearchCommand::QueueAudit {
            input,
            markets,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_queue_audit(QueueAuditOptions {
            input,
            markets,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::BuildMarkets {
            input,
            out,
            markdown,
            exclude_file,
            exclude_window,
            settlement_carry_input,
            settlement_carry_manifest,
            settlement_carry_campaign_id,
            settlement_carry_source_account,
            settlement_carry_source_container,
            market_day,
        } => run_build_markets(BuildMarketsOptions {
            input,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
            settlement_carry: parse_settlement_carry(
                settlement_carry_input,
                settlement_carry_manifest,
                settlement_carry_campaign_id,
                settlement_carry_source_account,
                settlement_carry_source_container,
                market_day,
            )?,
        })?,
        ResearchCommand::Replay {
            input,
            markets,
            strategy_config,
            fill_model,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_replay(ReplayOptions {
            input,
            markets: Some(markets),
            strategy_config,
            fill_model: fill_model.parse::<FillModel>()?,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::Baseline {
            input,
            markets,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_baseline(BaselineOptions {
            input,
            markets: Some(markets),
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::Regimes {
            input,
            markets,
            fill_model,
            profile_config,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_regimes(RegimesOptions {
            input,
            markets: Some(markets),
            fill_model: fill_model.parse::<FillModel>()?,
            profile_config,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::Sweep {
            input,
            markets,
            search,
            split,
            max_experiments,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_sweep(SweepOptions {
            input,
            markets: Some(markets),
            search,
            split,
            max_experiments,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::Calibration {
            input,
            markets,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_calibration(CalibrationOptions {
            input,
            markets: Some(markets),
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::SampleSize {
            results,
            out,
            markdown,
        } => run_sample_size(SampleSizeOptions {
            results,
            out,
            markdown,
        })?,
        ResearchCommand::Report {
            reports_dir,
            out,
            markdown,
        } => run_final_report(FinalReportOptions {
            reports_dir,
            out,
            markdown,
        })?,
        ResearchCommand::MlCalibrate { out, markdown } => {
            run_ml_calibrate(MlCalibrateOptions { out, markdown })?
        }
        ResearchCommand::AzureFreshness {
            account,
            container,
            prefix,
            max_age_seconds,
            expected_interval_seconds,
            out,
            sas_env,
            client_id,
        } => run_azure_freshness(AzureFreshnessOptions {
            account,
            container,
            prefix,
            out,
            sas_env,
            client_id,
            generated_at: None,
            max_age_seconds,
            expected_interval_seconds,
        })?,
        ResearchCommand::PublishNormalizedSnapshot {
            input,
            date,
            account,
            container,
            prefix,
            client_id,
        } => publish_normalized_snapshot(PublishNormalizedSnapshotOptions {
            input,
            date: parse_date_arg(&date)?,
            account,
            container,
            prefix,
            client_id,
        })?,
        ResearchCommand::RestoreNormalizedSnapshot {
            out,
            date,
            account,
            container,
            prefix,
            client_id,
        } => restore_normalized_snapshot(RestoreNormalizedSnapshotOptions {
            out,
            date: parse_date_arg(&date)?,
            account,
            container,
            prefix,
            client_id,
        })?,
        ResearchCommand::ValidateProspective {
            since,
            candidates,
            reports_dir,
            out,
            markdown,
            expected_daily_date,
        } => run_validate_prospective(ProspectiveValidationOptions {
            since: parse_datetime_arg(&since)?,
            reports_dir,
            candidates,
            out,
            markdown,
            expected_daily_date: expected_daily_date
                .as_deref()
                .map(parse_date_arg)
                .transpose()?,
        })?,
        ResearchCommand::PublishDailyBundle {
            date,
            run_id,
            input_sha256,
            expected_runtime_role,
            source_dir,
            output_root,
            data_audit,
        } => serde_json::to_value(publish_daily_directory(
            parse_date_arg(&date)?,
            run_id,
            input_sha256,
            parse_runtime_role_arg(&expected_runtime_role)?,
            &source_dir,
            &output_root,
            &data_audit,
        )?)?,
        ResearchCommand::BuildCumulativeWallet {
            regimes,
            campaign_manifest,
            campaign_contract,
            snapshot_date,
            out,
        } => run_build_cumulative_wallet_snapshot(CumulativeWalletSnapshotOptions {
            regimes,
            campaign_manifest,
            campaign_contract,
            snapshot_date: parse_date_arg(&snapshot_date)?,
            out,
        })?,
        ResearchCommand::AdvanceFundedLadder {
            prior_state,
            prior_state_sha256,
            observation,
            observation_sha256,
            grant,
            grant_sha256,
            out,
        } => serde_json::to_value(advance_funded_ladder(AdvanceFundedLadderOptions {
            prior_state,
            prior_state_sha256,
            observation,
            observation_sha256,
            grant,
            grant_sha256,
            out,
            now: Utc::now(),
        })?)?,
        ResearchCommand::InitializeFundedManifest {
            shadow_manifest,
            shadow_manifest_sha256,
            canary_evidence,
            canary_evidence_blob_name,
            canary_evidence_sha256,
            human_grant_consumption,
            human_grant_consumption_sha256,
            terminal_evidence,
            terminal_evidence_blob_name,
            terminal_evidence_sha256,
            out,
        } => serde_json::to_value(initialize_funded_manifest_after_canary(
            InitializeFundedManifestOptions {
                shadow_manifest,
                shadow_manifest_sha256,
                canary_evidence,
                canary_evidence_blob_name,
                canary_evidence_sha256,
                human_grant_consumption,
                human_grant_consumption_sha256,
                terminal_evidence,
                terminal_evidence_blob_name,
                terminal_evidence_sha256,
                out,
                now: Utc::now(),
            },
        )?)?,
        ResearchCommand::AdvanceFundedManifest {
            prior_manifest,
            prior_manifest_sha256,
            observation,
            observation_sha256,
            grant,
            grant_sha256,
            next_execution_model,
            next_execution_model_blob_uri,
            next_execution_model_sha256,
            out,
        } => serde_json::to_value(advance_funded_manifest(AdvanceFundedManifestOptions {
            prior_manifest,
            prior_manifest_sha256,
            observation,
            observation_sha256,
            grant,
            grant_sha256,
            next_execution_model,
            next_execution_model_blob_uri,
            next_execution_model_sha256,
            out,
            now: Utc::now(),
        })?)?,
        ResearchCommand::StopFundedManifestFromStageBlock {
            prior_manifest,
            prior_manifest_sha256,
            stage_block,
            stage_block_sha256,
            out,
        } => serde_json::to_value(stop_funded_manifest_from_stage_block(
            StopFundedManifestFromStageBlockOptions {
                prior_manifest,
                prior_manifest_sha256,
                stage_block,
                stage_block_sha256,
                out,
                now: Utc::now(),
            },
        )?)?,
        ResearchCommand::ExpireFundedManifest {
            prior_manifest,
            prior_manifest_sha256,
            out,
        } => serde_json::to_value(expire_funded_manifest(ExpireFundedManifestOptions {
            prior_manifest,
            prior_manifest_sha256,
            out,
            now: Utc::now(),
        })?)?,
        ResearchCommand::EvaluateProfitability {
            daily_root,
            prospective,
            gate_config,
            execution_model,
            out,
        } => {
            let manifest = run_evaluate_profitability(ProfitabilityEvaluationOptions {
                daily_root,
                prospective,
                gate_config,
                execution_model,
                out,
                generated_at: None,
            })?;
            let metrics = &manifest.gate_metrics.metrics;
            let blocking_warnings = metrics
                .data_quality
                .warnings
                .iter()
                .filter(|warning| warning.severity == WarningSeverity::Blocking)
                .count();
            let phase = serde_json::to_value(manifest.phase)?
                .as_str()
                .unwrap_or("unknown")
                .to_owned();
            let authorization_flags = profitability_authorization_flags(
                manifest.gate_metrics.promotion_allowed,
                manifest.promotion_allowed,
            );
            eprintln!(
                "polyedge_profitability_summary phase={phase} clean_days={} settled_markets={} queue_conservative_net_pnl={} wallet_constrained_net_pnl={} pnl_ci_95_low={} positive_weekly_blocks={} decision_parity_rate={} markout_30s_ci_low={} decision_grade_coverage={} blocking_warnings={} missing_metrics={} {authorization_flags}",
                metrics.clean_days,
                metrics.settled_markets,
                metrics.queue_conservative_net_pnl,
                metrics.wallet_constrained_net_pnl,
                metrics.pnl_ci_95_low,
                metrics.consecutive_positive_weekly_blocks,
                metrics.decision_parity_rate,
                metrics.markout_30s_ci_low,
                metrics.data_quality.decision_grade_coverage,
                blocking_warnings,
                metrics.missing_metrics.join(","),
            );
            serde_json::to_value(manifest)?
        }
        ResearchCommand::BuildReplayIndex {
            input,
            out,
            exclude_file,
            exclude_window,
        } => run_build_replay_index(ReplayIndexOptions {
            input,
            out,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::ChartBackfill {
            input,
            out,
            markdown,
            exclude_file,
            exclude_window,
        } => run_chart_backfill(ChartBackfillOptions {
            input,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
        })?,
        ResearchCommand::Backfill {
            start,
            end,
            task,
            exclude_file,
            exclude_window,
            out,
            markdown,
        } => run_backfill(BackfillOptions {
            start,
            end,
            task,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
            out,
            markdown,
        })?,
    };
    print_json(value)
}

fn run_with_azure_lease(
    account: String,
    container: String,
    blob: String,
    lease_seconds: u32,
    renew_seconds: u64,
    wait_seconds: u64,
    command: Vec<String>,
) -> Result<serde_json::Value> {
    if account.trim().is_empty() || container.trim().is_empty() || blob.trim().is_empty() {
        bail!("Azure lease account, container, and blob are required");
    }
    if !(15..=60).contains(&lease_seconds) {
        bail!("lease-seconds must be between 15 and 60");
    }
    if renew_seconds == 0 || renew_seconds >= u64::from(lease_seconds) {
        bail!("renew-seconds must be positive and shorter than lease-seconds");
    }
    let executable = command
        .first()
        .filter(|value| !value.trim().is_empty())
        .context("with-azure-lease requires a child command after --")?;
    let client_id = std::env::var("AZURE_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let mut client = AzureBlobClient::with_managed_identity_for_lease(
        account.clone(),
        container.clone(),
        client_id,
    );
    let acquire_started = Instant::now();
    let lease_id = loop {
        match client
            .acquire_blob_lease(&blob, lease_seconds)
            .context("acquiring Azure campaign lease")?
        {
            BlobLeaseAcquireResult::Acquired(lease_id) => break lease_id,
            BlobLeaseAcquireResult::AlreadyLeased
                if acquire_started.elapsed() < StdDuration::from_secs(wait_seconds) =>
            {
                thread::sleep(StdDuration::from_secs(5));
            }
            BlobLeaseAcquireResult::AlreadyLeased => {
                bail!(
                    "Azure campaign lease remained held for {wait_seconds}s; refusing overlapping research writer"
                );
            }
        }
    };

    let mut child_command = ProcessCommand::new(executable);
    child_command
        .args(command.iter().skip(1))
        .env("POLYEDGE_CAMPAIGN_LEASE_ACTIVE", "true")
        .env("POLYEDGE_CAMPAIGN_LEASE_ID", &lease_id)
        .env("POLYEDGE_CAMPAIGN_LEASE_ACCOUNT", &account)
        .env("POLYEDGE_CAMPAIGN_LEASE_CONTAINER", &container)
        .env("POLYEDGE_CAMPAIGN_LEASE_BLOB", &blob);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        child_command.process_group(0);
    }
    let mut child = match child_command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = client.release_blob_lease(&blob, &lease_id);
            return Err(error).context("starting Azure-lease child command");
        }
    };

    let renew_interval = StdDuration::from_secs(renew_seconds);
    let mut last_renewed = Instant::now();
    let child_status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                terminate_lease_child_tree(&mut child);
                let _ = client.release_blob_lease(&blob, &lease_id);
                return Err(error).context(
                    "checking Azure-lease child; child was killed and lease release attempted",
                );
            }
        }
        if last_renewed.elapsed() >= renew_interval {
            let (renew_tx, renew_rx) = mpsc::sync_channel(1);
            let mut renew_client = client.clone();
            let renew_blob = blob.clone();
            let renew_lease_id = lease_id.clone();
            thread::spawn(move || {
                let _ = renew_tx.send(renew_client.renew_blob_lease(&renew_blob, &renew_lease_id));
            });
            let renewal_deadline = StdDuration::from_secs(
                u64::from(lease_seconds)
                    .saturating_sub(renew_seconds)
                    .min(10),
            );
            let renewal = renew_rx.recv_timeout(renewal_deadline);
            match renewal {
                Ok(Ok(true)) => last_renewed = Instant::now(),
                Ok(Ok(false)) => {
                    terminate_lease_child_tree(&mut child);
                    let _ = client.release_blob_lease(&blob, &lease_id);
                    bail!("Azure campaign lease was lost; child was killed before publication");
                }
                Ok(Err(error)) => {
                    terminate_lease_child_tree(&mut child);
                    let _ = client.release_blob_lease(&blob, &lease_id);
                    return Err(error).context(
                        "renewing Azure campaign lease; child was killed before publication",
                    );
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    terminate_lease_child_tree(&mut child);
                    let _ = client.release_blob_lease(&blob, &lease_id);
                    bail!(
                        "Azure campaign lease renewal exceeded its safety deadline; child was killed before lease expiry"
                    );
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    terminate_lease_child_tree(&mut child);
                    let _ = client.release_blob_lease(&blob, &lease_id);
                    bail!(
                        "Azure campaign lease renewal worker exited; child was killed before lease expiry"
                    );
                }
            }
        }
        thread::sleep(StdDuration::from_secs(1));
    };
    let released = client
        .release_blob_lease(&blob, &lease_id)
        .context("releasing Azure campaign lease")?;
    if !released {
        bail!("Azure campaign lease was no longer owned at child completion");
    }
    if !child_status.success() {
        bail!(
            "Azure-lease child command failed with status {}",
            child_status
        );
    }
    Ok(json!({
        "status": "completed",
        "lease": "released",
        "account": account,
        "container": container,
        "blob": blob,
        "child_status": child_status.code()
    }))
}

fn terminate_lease_child_tree(child: &mut Child) {
    #[cfg(unix)]
    {
        if let Ok(process_group) = i32::try_from(child.id()) {
            // SAFETY: the child was spawned into a process group whose PGID is
            // its PID. A negative PID targets only that group, never this
            // lease-wrapper process. ESRCH is harmless if it already exited.
            unsafe {
                libc::kill(-process_group, libc::SIGKILL);
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn run_seal_qset_v2_day(
    account: String,
    date: NaiveDate,
    validate_only: bool,
    client_id: Option<String>,
) -> Result<serde_json::Value> {
    let campaign_start = parse_date_arg(QSET_V2_START)?;
    let today = Utc::now().date_naive();
    if date < campaign_start || date >= today {
        bail!(
            "qset-v2 seal date must be on or after {campaign_start} and before {today}; received {date}"
        );
    }

    let mut freeze_client = AzureBlobClient::with_managed_identity(
        account.clone(),
        QSET_V2_FREEZE_CONTAINER,
        client_id.clone(),
    );
    let freeze_items = freeze_client
        .list_blobs_unfiltered(QSET_V2_FREEZE_BLOB, None, None)
        .context("listing the qset-v2 source-freeze artifact")?;
    let freeze_item = freeze_items
        .iter()
        .filter(|item| item.name == QSET_V2_FREEZE_BLOB)
        .collect::<Vec<_>>();
    if freeze_item.len() != 1
        || freeze_items.len() != 1
        || freeze_item[0].content_length == 0
        || freeze_item[0].content_length > QSET_V2_MAX_FREEZE_BYTES
    {
        bail!("qset-v2 source-freeze artifact binding is not unique and bounded");
    }
    let freeze_bytes = freeze_client
        .download_blob_bytes_exact_bounded(
            QSET_V2_FREEZE_BLOB,
            freeze_item[0].content_length,
            QSET_V2_MAX_FREEZE_BYTES,
        )
        .context("reading the qset-v2 source-freeze artifact")?;
    if sha256_prefixed(&freeze_bytes) != QSET_V2_FREEZE_SHA256 {
        bail!("qset-v2 source-freeze artifact hash disagrees with the approved binding");
    }

    let prefix = format!("{QSET_V2_PREFIX}/{}/", date.format("%Y/%m/%d"));
    let mut client =
        AzureBlobClient::with_managed_identity(account.clone(), QSET_V2_CONTAINER, client_id);
    let before = validate_qset_v2_inventory(
        client
            .list_blobs_unfiltered(&prefix, None, None)
            .context("listing the closed qset-v2 day before sealing")?,
        &prefix,
    )?;
    let source_inventory_sha256 = qset_v2_inventory_sha256(&before)?;
    if validate_only {
        let total_bytes = before.iter().try_fold(0_u64, |total, blob| {
            total
                .checked_add(blob.content_length)
                .context("qset-v2 day byte count overflow")
        })?;
        return Ok(json!({
            "schema": "polyedge.qset_v2_closed_day_validation.v1",
            "account": account,
            "container": QSET_V2_CONTAINER,
            "campaign_id": "campaign-2026-08-22-qset-v2",
            "date": date,
            "prefix": prefix,
            "blob_count": before.len(),
            "total_bytes": total_bytes,
            "all_sealed": before.iter().all(|blob| blob.sealed == Some(true)),
            "source_inventory_sha256": source_inventory_sha256,
            "source_freeze": {
                "container": QSET_V2_FREEZE_CONTAINER,
                "blob": QSET_V2_FREEZE_BLOB,
                "sha256": QSET_V2_FREEZE_SHA256,
                "verified": true
            }
        }));
    }
    for blob in before.iter().filter(|blob| blob.sealed == Some(false)) {
        client
            .seal_append_blob_if_match(&blob.name, &blob.etag)
            .with_context(|| format!("sealing closed qset-v2 blob {}", blob.name))?;
    }

    let after = validate_qset_v2_inventory(
        client
            .list_blobs_unfiltered(&prefix, None, None)
            .context("listing the closed qset-v2 day after sealing")?,
        &prefix,
    )?;
    if qset_v2_inventory_sha256(&after)? != source_inventory_sha256 {
        bail!("qset-v2 source inventory changed while the day was sealed");
    }
    if after.iter().any(|blob| blob.sealed != Some(true)) {
        bail!("qset-v2 day still contains unsealed append blobs");
    }
    let total_bytes = after.iter().try_fold(0_u64, |total, blob| {
        total
            .checked_add(blob.content_length)
            .context("qset-v2 day byte count overflow")
    })?;

    Ok(json!({
        "schema": "polyedge.qset_v2_closed_day_seal.v1",
        "account": account,
        "container": QSET_V2_CONTAINER,
        "campaign_id": "campaign-2026-08-22-qset-v2",
        "date": date,
        "prefix": prefix,
        "blob_count": after.len(),
        "total_bytes": total_bytes,
        "sealed_blob_count": after.len(),
        "all_sealed": true,
        "source_inventory_sha256": source_inventory_sha256,
        "source_freeze": {
            "container": QSET_V2_FREEZE_CONTAINER,
            "blob": QSET_V2_FREEZE_BLOB,
            "sha256": QSET_V2_FREEZE_SHA256,
            "verified": true
        }
    }))
}

fn run_seal_qset_day(
    config: &QsetSealConfig,
    account: String,
    date: NaiveDate,
    source_freeze_blob: &str,
    source_freeze_sha256: &str,
    validate_only: bool,
    client_id: Option<String>,
) -> Result<serde_json::Value> {
    validate_qset_seal_date(config, date, Utc::now().date_naive())?;
    if !valid_qset_source_freeze_binding(config, source_freeze_blob, source_freeze_sha256) {
        bail!("{} source-freeze binding is invalid", config.name);
    }

    let mut freeze_client = AzureBlobClient::with_managed_identity(
        account.clone(),
        config.freeze_container,
        client_id.clone(),
    );
    let freeze_items = freeze_client
        .list_blobs_unfiltered(source_freeze_blob, None, None)
        .with_context(|| format!("listing the {} source-freeze artifact", config.name))?;
    let freeze_item = freeze_items
        .iter()
        .filter(|item| item.name == source_freeze_blob)
        .collect::<Vec<_>>();
    if freeze_item.len() != 1
        || freeze_items.len() != 1
        || freeze_item[0].content_length == 0
        || freeze_item[0].content_length > QSET_V2_MAX_FREEZE_BYTES
    {
        bail!(
            "{} source-freeze artifact binding is not unique and bounded",
            config.name
        );
    }
    let freeze_bytes = freeze_client
        .download_blob_bytes_exact_bounded(
            source_freeze_blob,
            freeze_item[0].content_length,
            QSET_V2_MAX_FREEZE_BYTES,
        )
        .with_context(|| format!("reading the {} source-freeze artifact", config.name))?;
    if sha256_prefixed(&freeze_bytes) != source_freeze_sha256 {
        bail!(
            "{} source-freeze artifact hash disagrees with the approved binding",
            config.name
        );
    }

    let prefix = format!("{}/{}/", config.prefix, date.format("%Y/%m/%d"));
    let mut client =
        AzureBlobClient::with_managed_identity(account.clone(), config.container, client_id);
    let before = validate_qset_inventory(
        config,
        client
            .list_blobs_unfiltered(&prefix, None, None)
            .with_context(|| format!("listing the closed {} day before sealing", config.name))?,
        &prefix,
    )?;
    let source_inventory_sha256 = qset_v2_inventory_sha256(&before)?;
    if validate_only {
        return qset_closed_day_receipt(
            config,
            &account,
            date,
            &prefix,
            &before,
            &source_inventory_sha256,
            source_freeze_blob,
            source_freeze_sha256,
            false,
        );
    }
    for blob in before.iter().filter(|blob| blob.sealed == Some(false)) {
        client
            .seal_append_blob_if_match(&blob.name, &blob.etag)
            .with_context(|| format!("sealing closed {} blob {}", config.name, blob.name))?;
    }

    let after = validate_qset_inventory(
        config,
        client
            .list_blobs_unfiltered(&prefix, None, None)
            .with_context(|| format!("listing the closed {} day after sealing", config.name))?,
        &prefix,
    )?;
    if qset_v2_inventory_sha256(&after)? != source_inventory_sha256 {
        bail!(
            "{} source inventory changed while the day was sealed",
            config.name
        );
    }
    if after.iter().any(|blob| blob.sealed != Some(true)) {
        bail!("{} day still contains unsealed append blobs", config.name);
    }
    qset_closed_day_receipt(
        config,
        &account,
        date,
        &prefix,
        &after,
        &source_inventory_sha256,
        source_freeze_blob,
        source_freeze_sha256,
        true,
    )
}

fn validate_qset_seal_date(
    config: &QsetSealConfig,
    date: NaiveDate,
    today: NaiveDate,
) -> Result<()> {
    let date_value = date.format("%Y-%m-%d").to_string();
    if config.dates.iter().any(|allowed| *allowed == date_value) && date < today {
        return Ok(());
    }
    bail!(
        "{} seal date must be exactly {} or {} and before {today}; received {date}",
        config.name,
        config.dates[0],
        config.dates[1]
    );
}

fn qset_closed_day_receipt(
    config: &QsetSealConfig,
    account: &str,
    date: NaiveDate,
    prefix: &str,
    blobs: &[AzureBlobItem],
    source_inventory_sha256: &str,
    source_freeze_blob: &str,
    source_freeze_sha256: &str,
    sealed: bool,
) -> Result<serde_json::Value> {
    let total_bytes = blobs.iter().try_fold(0_u64, |total, blob| {
        total
            .checked_add(blob.content_length)
            .with_context(|| format!("{} day byte count overflow", config.name))
    })?;
    let mut receipt = json!({
        "schema": if sealed { config.seal_schema } else { config.validation_schema },
        "account": account,
        "container": config.container,
        "campaign_id": config.campaign_id,
        "date": date,
        "prefix": prefix,
        "blob_count": blobs.len(),
        "total_bytes": total_bytes,
        "all_sealed": sealed || blobs.iter().all(|blob| blob.sealed == Some(true)),
        "source_inventory_sha256": source_inventory_sha256,
        "source_freeze": {
            "container": config.freeze_container,
            "blob": source_freeze_blob,
            "sha256": source_freeze_sha256,
            "verified": true
        }
    });
    if sealed {
        receipt["sealed_blob_count"] = json!(blobs.len());
    }
    Ok(receipt)
}
fn valid_qset_source_freeze_binding(
    config: &QsetSealConfig,
    source_freeze_blob: &str,
    source_freeze_sha256: &str,
) -> bool {
    let Some(digest) = source_freeze_sha256.strip_prefix("sha256:") else {
        return false;
    };
    let Some(blob_digest) = source_freeze_blob
        .strip_prefix(config.freeze_blob_prefix)
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return false;
    };
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        && blob_digest == digest
}

fn validate_qset_inventory(
    config: &QsetSealConfig,
    blobs: Vec<AzureBlobItem>,
    prefix: &str,
) -> Result<Vec<AzureBlobItem>> {
    validate_qset_v2_inventory(blobs, prefix)
        .map_err(|error| anyhow::anyhow!("{} closed-day validation failed: {error:#}", config.name))
}

fn validate_qset_v2_inventory(
    mut blobs: Vec<AzureBlobItem>,
    prefix: &str,
) -> Result<Vec<AzureBlobItem>> {
    if blobs.is_empty() || blobs.len() > QSET_V2_MAX_DAY_BLOBS {
        bail!("qset-v2 closed day must contain between 1 and {QSET_V2_MAX_DAY_BLOBS} blobs");
    }
    blobs.sort_by(|left, right| left.name.cmp(&right.name));
    let mut names = HashSet::new();
    let mut total_bytes = 0_u64;
    for blob in &blobs {
        let tail = blob
            .name
            .strip_prefix(prefix)
            .context("qset-v2 blob escaped the exact closed-day prefix")?;
        let bytes = tail.as_bytes();
        if bytes.len() != 11
            || bytes[2] != b'/'
            || &bytes[5..] != b".jsonl"
            || !bytes[0..2].iter().all(u8::is_ascii_digit)
            || !bytes[3..5].iter().all(u8::is_ascii_digit)
        {
            bail!("qset-v2 blob name is not an exact HH/MM.jsonl minute path");
        }
        let hour = tail[0..2].parse::<u8>()?;
        let minute = tail[3..5].parse::<u8>()?;
        if hour > 23 || minute > 59 {
            bail!("qset-v2 blob name contains an invalid UTC minute");
        }
        if !names.insert(&blob.name)
            || blob.blob_type.as_deref() != Some("AppendBlob")
            || !matches!(blob.sealed, Some(true | false))
            || blob.etag.trim().is_empty()
            || blob.last_modified.is_none()
            || blob.content_length == 0
        {
            bail!("qset-v2 closed-day blob properties are incomplete or invalid");
        }
        total_bytes = total_bytes
            .checked_add(blob.content_length)
            .context("qset-v2 day byte count overflow")?;
        if total_bytes > QSET_V2_MAX_DAY_BYTES {
            bail!("qset-v2 closed day exceeds the 16 GiB evidence bound");
        }
    }
    if names.len() != QSET_V2_EXPECTED_DAY_BLOBS {
        bail!(
            "qset-v2 closed day must contain all {QSET_V2_EXPECTED_DAY_BLOBS} unique UTC minute blobs"
        );
    }
    Ok(blobs)
}

fn qset_v2_inventory_sha256(blobs: &[AzureBlobItem]) -> Result<String> {
    let mut bytes = Vec::new();
    for blob in blobs {
        serde_json::to_writer(
            &mut bytes,
            &json!({
                "name": blob.name,
                "content_md5": blob.content_md5,
                "blob_type": blob.blob_type,
                "content_length": blob.content_length,
            }),
        )?;
        bytes.push(b'\n');
    }
    Ok(sha256_prefixed(&bytes))
}
fn load_exclusions(path: PathBuf, values: Vec<String>) -> Result<Vec<ExcludedTimeWindow>> {
    let mut windows = load_default_exclusions(&path)
        .with_context(|| format!("loading exclusion registry {}", path.display()))?;
    windows.extend(parse_exclude_windows(values)?);
    Ok(windows)
}

fn parse_exclude_windows(values: Vec<String>) -> Result<Vec<ExcludedTimeWindow>> {
    values
        .into_iter()
        .map(|value| ExcludedTimeWindow::parse(&value).map_err(anyhow::Error::from))
        .collect()
}

fn parse_datetime_arg(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid RFC3339 timestamp: {value}"))?
        .with_timezone(&Utc))
}

fn parse_date_arg(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("invalid UTC date (expected YYYY-MM-DD): {value}"))
}

fn parse_settlement_carry(
    input: Option<PathBuf>,
    published_manifest: Option<PathBuf>,
    campaign_id: Option<String>,
    source_account: Option<String>,
    source_container: Option<String>,
    market_day: Option<String>,
) -> Result<Option<SettlementCarryOptions>> {
    match (
        input,
        published_manifest,
        campaign_id,
        source_account,
        source_container,
        market_day,
    ) {
        (None, None, None, None, None, None) => Ok(None),
        (
            Some(input),
            Some(published_manifest),
            Some(campaign_id),
            Some(source_account),
            Some(source_container),
            Some(market_day),
        ) => Ok(Some(SettlementCarryOptions {
            input,
            published_manifest,
            market_day: parse_date_arg(&market_day)?,
            campaign_id,
            source_account,
            source_container,
        })),
        _ => bail!(
            "--settlement-carry-input, --settlement-carry-manifest, \
             --settlement-carry-campaign-id, --settlement-carry-source-account, \
             --settlement-carry-source-container, and --market-day must be supplied together"
        ),
    }
}

fn parse_runtime_role_arg(value: &str) -> Result<RuntimeRole> {
    match value.trim().to_ascii_lowercase().as_str() {
        "primary" => Ok(RuntimeRole::Primary),
        "profitability_shadow" => Ok(RuntimeRole::ProfitabilityShadow),
        _ => {
            bail!("invalid expected runtime role {value}; expected primary or profitability_shadow")
        }
    }
}

fn confirm_source(settings: &RuntimeSettings) -> Result<serde_json::Value> {
    let markets = polyedge_feeds::discover_markets(settings).context("discovering markets")?;
    let symbol = settings.target.chainlink_symbol.to_ascii_lowercase();
    let asset = settings.target.asset.to_ascii_lowercase();
    let matched_markets = markets
        .iter()
        .filter(|market| {
            let description = market
                .description
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            description.contains("chainlink")
                && (description.contains(&symbol)
                    || description.contains(&symbol.replace("/", " / "))
                    || description.contains(&asset))
        })
        .map(|market| {
            json!({
                "market_id": market.market_id,
                "market_slug": market.market_slug,
                "event_slug": market.event_slug,
                "question": market.question,
                "start_ts": market.start_ts,
                "end_ts": market.end_ts,
                "resolution_source": market.resolution_source
            })
        })
        .collect::<Vec<_>>();
    let ok = !matched_markets.is_empty() && settings.target.enable_polymarket_rtds_chainlink;
    let message = if matched_markets.is_empty() {
        "No discovered market description mentioned the configured Chainlink source."
    } else {
        "Discovered market descriptions mention the configured Chainlink source."
    };
    Ok(json!({
        "ok": ok,
        "backend_impl": "rust",
        "runtime_role": settings.deploy.runtime_role.as_str(),
        "shadow_only": settings.deploy.runtime_role.is_shadow(),
        "target_asset": settings.target.asset,
        "target_horizon": settings.target.horizon,
        "configured_rtds_url": settings.target.polymarket_rtds_url,
        "configured_chainlink_symbol": settings.target.chainlink_symbol,
        "configured_resolution_source": settings.target.resolution_source,
        "discovered_markets": markets.len(),
        "matched_markets": matched_markets,
        "message": message
    }))
}

async fn serve(settings: RuntimeSettings, bind: String) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding Rust API to {bind}"))?;
    println!(
        "{}",
        json!({
            "backend_impl": "rust",
            "runtime_role": settings.deploy.runtime_role.as_str(),
            "shadow_only": settings.deploy.runtime_role.is_shadow(),
            "git_sha": embedded_git_sha().unwrap_or("unknown"),
            "execution_mode": "paper",
            "bind": bind
        })
    );
    let qset_v4_writer = settings.deploy.app_name == "polyedge-shadow-qset-v4";
    let (app, shutdown) = app_with_shutdown(settings);
    let (shutdown_result_tx, shutdown_result_rx) = tokio::sync::oneshot::channel();
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let result = shutdown_protocol(shutdown, qset_v4_writer).await;
            if let Err(error) = &result {
                eprintln!("Rust API shutdown did not fully drain: {error}");
            }
            let _ = shutdown_result_tx.send(result);
        })
        .await;
    match shutdown_result_rx.await {
        Ok(Ok(_)) => serve_result.context("serving Rust API"),
        Ok(Err(error)) => Err(anyhow::anyhow!("Rust API retirement failed: {error}")),
        Err(_) => serve_result.context("serving Rust API"),
    }
}

async fn shutdown_protocol(
    shutdown: polyedge_api::ApiShutdown,
    qset_v4_writer: bool,
) -> Result<(), String> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("installing SIGTERM handler");
        if !qset_v4_writer {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return shutdown.drain().await;
        }
        let mut prepare =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .expect("installing SIGUSR1 handler");
        let mut prepared = false;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => return require_qset_v4_prepared(prepared),
                _ = terminate.recv() => return require_qset_v4_prepared(prepared),
                _ = prepare.recv() => match shutdown.prepare_qset_v4_retirement().await {
                    Ok(receipt) => {
                        println!(
                            "{}",
                            serde_json::to_string(&receipt)
                                .map_err(|error| format!("serializing qset-v4 retirement receipt: {error}"))?
                        );
                        prepared = true;
                    }
                    Err(error) => {
                        eprintln!("qset-v4 prepare-retirement failed; writer remains fenced: {error}");
                    }
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("installing Ctrl-C handler");
        if qset_v4_writer {
            require_qset_v4_prepared(false)
        } else {
            shutdown.drain().await
        }
    }
}

fn require_qset_v4_prepared(prepared: bool) -> Result<(), String> {
    if prepared {
        Ok(())
    } else {
        Err("qset-v4 writer is not prepared for retirement; send SIGUSR1 and require a valid receipt before TERM".to_owned())
    }
}

fn bench_ingest(events: usize) -> serde_json::Value {
    let mut latencies_us = Vec::with_capacity(events);
    let start = Instant::now();
    let mut dropped = 0usize;
    for index in 0..events {
        let event_start = Instant::now();
        let payload = json!({
            "type": "reference",
            "sequence": index,
            "price": "100000",
            "backend_impl": "rust"
        });
        if payload.get("sequence").is_none() {
            dropped += 1;
        }
        latencies_us.push(event_start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let elapsed = start.elapsed();
    latencies_us.sort_by(|left, right| left.total_cmp(right));
    json!({
        "events": events,
        "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
        "events_per_second": if elapsed.as_secs_f64() == 0.0 { 0.0 } else { events as f64 / elapsed.as_secs_f64() },
        "p95_event_to_snapshot_latency_ms": percentile(&latencies_us, 0.95) / 1000.0,
        "p99_event_to_snapshot_latency_ms": percentile(&latencies_us, 0.99) / 1000.0,
        "recorder_drops": dropped,
        "memory_rss_mb": rss_mb()
    })
}

fn bench_replay(path: PathBuf) -> Result<serde_json::Value> {
    let start = Instant::now();
    let result = run_backtest(&path)?;
    let elapsed = start.elapsed();
    let bytes = std::fs::metadata(&path).map(|metadata| metadata.len()).ok();
    Ok(json!({
        "path": path.to_string_lossy(),
        "events": result.event_count,
        "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
        "events_per_second": if elapsed.as_secs_f64() == 0.0 { 0.0 } else { result.event_count as f64 / elapsed.as_secs_f64() },
        "bytes": bytes,
        "bytes_per_second": bytes.map(|value| if elapsed.as_secs_f64() == 0.0 { 0.0 } else { value as f64 / elapsed.as_secs_f64() }),
        "mib_per_second": bytes.map(|value| if elapsed.as_secs_f64() == 0.0 { 0.0 } else { value as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64() }),
        "filled_orders": result.filled_orders,
        "net_pnl": result.net_pnl,
        "memory_rss_mb": rss_mb()
    }))
}

fn run_ring_upload(
    root: &Path,
    blob_prefix: &str,
    account: String,
    container: String,
    retention_hours: u64,
    client_id: Option<String>,
) -> Result<serde_json::Value> {
    if blob_prefix.is_empty()
        || blob_prefix.starts_with('/')
        || blob_prefix.ends_with('/')
        || blob_prefix
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || !blob_prefix.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-/".contains(&byte)
        })
    {
        bail!("ring blob prefix is invalid");
    }
    if retention_hours < 48 {
        bail!("ring retention must be at least 48 hours");
    }
    let root = root
        .canonicalize()
        .with_context(|| format!("resolving ring root {}", root.display()))?;
    if !root.is_dir() {
        bail!("ring root is not a directory: {}", root.display());
    }
    let mut manifests = Vec::new();
    collect_ring_manifests(&root, &mut manifests)?;
    manifests.sort();

    let mut client = AzureBlobClient::with_managed_identity(account, container, client_id);
    let now = Utc::now().timestamp();
    let retention_seconds = i64::try_from(retention_hours)
        .ok()
        .and_then(|hours| hours.checked_mul(3600))
        .context("ring retention is too large")?;
    let mut uploaded = 0_u64;
    let mut verified = 0_u64;
    let mut deleted = 0_u64;
    let mut processed = 0_u64;

    for manifest_path in manifests {
        processed += 1;
        let manifest_bytes = fs::read(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)
            .with_context(|| format!("parsing {}", manifest_path.display()))?;
        let schema_version = manifest["schema_version"]
            .as_u64()
            .context("ring manifest schema_version must be an unsigned integer")?;
        if !matches!(schema_version, 1 | 2 | 3 | 4) {
            bail!(
                "unsupported ring manifest schema: {}",
                manifest_path.display()
            );
        }
        let source_relative = ring_relative_path(&manifest, "segment_path")?;
        let payload_relative = if schema_version >= 2 {
            if manifest["compression"].as_str() != Some("gzip") {
                bail!("ring manifest v2/v3 compression must equal gzip");
            }
            match schema_version {
                3 => validate_ring_manifest_v3_sequence(&manifest)?,
                4 => validate_ring_manifest_v4_runs(&manifest)?,
                _ => {}
            }
            ring_relative_path(&manifest, "archive_path")?
        } else {
            source_relative.clone()
        };
        let source_path = root.join(&source_relative);
        let payload_path = root.join(&payload_relative);
        let receipt_path = PathBuf::from(format!("{}.uploaded.json", manifest_path.display()));
        let accepted_prefix =
            accepted_ring_blob_prefix(&manifest, blob_prefix, receipt_path.exists());
        let blob_name = ring_blob_name(&manifest, accepted_prefix)?;
        if (schema_version == 1 && !blob_name.ends_with(".jsonl"))
            || (schema_version >= 2 && !blob_name.ends_with(".jsonl.gz"))
        {
            bail!("ring manifest schema and blob compression disagree");
        }
        let expected_sha = ring_sha256(&manifest, "sha256")?;
        let expected_bytes = manifest["bytes"]
            .as_u64()
            .context("ring manifest bytes must be an unsigned integer")?;
        let segment_end = manifest["segment_end_epoch"]
            .as_i64()
            .context("ring manifest segment_end_epoch must be an integer")?;
        let segment_start = manifest["segment_start_epoch"]
            .as_i64()
            .context("ring manifest segment_start_epoch must be an integer")?;
        validate_ring_identity(
            &source_relative,
            &payload_relative,
            &blob_name,
            accepted_prefix,
            segment_start,
            segment_end,
            now,
        )
        .with_context(|| {
            format!(
                "invalid ring manifest identity: {}",
                manifest_path.display()
            )
        })?;
        let manifest_sha = sha256_prefixed(&manifest_bytes);
        let manifest_blob = format!("{blob_name}.manifest.json");

        if receipt_path.exists() {
            let receipt_bytes = fs::read(&receipt_path)
                .with_context(|| format!("reading {}", receipt_path.display()))?;
            validate_ring_upload_receipt(&receipt_bytes, &manifest_sha, &blob_name, &manifest_blob)
                .with_context(|| format!("invalid ring receipt {}", receipt_path.display()))?;
        } else {
            match schema_version {
                3 => validate_ring_source_v3(&source_path, &manifest)?,
                4 => validate_ring_source_v4(&source_path, &manifest)?,
                _ => {}
            }
            if schema_version >= 2 {
                let source_bytes = fs::read(&source_path)
                    .with_context(|| format!("reading {}", source_path.display()))?;
                let expected_source_bytes = manifest["source_bytes"]
                    .as_u64()
                    .context("ring manifest source_bytes must be an unsigned integer")?;
                let expected_source_sha = ring_sha256(&manifest, "source_sha256")?;
                if source_bytes.len() as u64 != expected_source_bytes
                    || sha256_prefixed(&source_bytes) != expected_source_sha
                {
                    bail!("sealed ring source changed: {}", source_path.display());
                }
            }
            let payload_bytes = fs::read(&payload_path)
                .with_context(|| format!("reading {}", payload_path.display()))?;
            if payload_bytes.len() as u64 != expected_bytes
                || sha256_prefixed(&payload_bytes) != expected_sha
            {
                bail!("sealed ring payload changed: {}", payload_path.display());
            }
            match client
                .upload_hot_block_blob_bytes_if_absent(
                    &blob_name,
                    &payload_bytes,
                    if schema_version >= 2 {
                        "application/gzip"
                    } else {
                        "application/x-ndjson"
                    },
                )
                .with_context(|| format!("uploading {blob_name}"))?
            {
                ImmutableBlobWrite::Created => uploaded += 1,
                ImmutableBlobWrite::AlreadyExists => {
                    let remote = client
                        .download_blob_bytes(&blob_name)
                        .with_context(|| format!("verifying existing {blob_name}"))?;
                    if remote.len() as u64 != expected_bytes
                        || sha256_prefixed(&remote) != expected_sha
                    {
                        bail!("existing Azure ring segment disagrees: {blob_name}");
                    }
                }
            }
            match client
                .upload_block_blob_bytes_if_absent(
                    &manifest_blob,
                    &manifest_bytes,
                    "application/json",
                )
                .with_context(|| format!("uploading {manifest_blob}"))?
            {
                ImmutableBlobWrite::Created => {}
                ImmutableBlobWrite::AlreadyExists => {
                    if client.download_blob_bytes(&manifest_blob)? != manifest_bytes {
                        bail!("existing Azure ring manifest disagrees: {manifest_blob}");
                    }
                }
            }
            let receipt = serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "manifest_sha256": manifest_sha,
                "blob_name": blob_name,
                "manifest_blob_name": manifest_blob,
                "verified_ts": Utc::now().to_rfc3339(),
            }))?;
            write_new_file(&receipt_path, &receipt)?;
            verified += 1;
        }

        if now >= segment_end.saturating_add(retention_seconds) {
            if client.download_blob_bytes(&manifest_blob)? != manifest_bytes {
                bail!("remote manifest changed before local deletion: {manifest_blob}");
            }
            if payload_path.exists() {
                fs::remove_file(&payload_path)
                    .with_context(|| format!("deleting verified {}", payload_path.display()))?;
            }
            if source_path != payload_path && source_path.exists() {
                fs::remove_file(&source_path)
                    .with_context(|| format!("deleting verified {}", source_path.display()))?;
            }
            if manifest_path.exists() {
                fs::remove_file(&manifest_path)
                    .with_context(|| format!("deleting verified {}", manifest_path.display()))?;
            }
            deleted += 1;
        }
    }

    Ok(json!({
        "ok": true,
        "manifest_count": processed,
        "uploaded_segments": uploaded,
        "newly_verified_segments": verified,
        "deleted_local_segments": deleted,
        "retention_hours": retention_hours,
        "segment_access_tier": "Hot",
    }))
}

struct RingQuarantineResolution {
    source_path: PathBuf,
    receipt_bytes: Vec<u8>,
    resolution_bytes: Vec<u8>,
    source_sha256: String,
    source_bytes: u64,
    receipt_sha256: String,
    source_blob_name: String,
    receipt_blob_name: String,
    resolution_blob_name: String,
    final_directory: PathBuf,
}

fn run_ring_quarantine_resolve(
    root: &Path,
    receipt_id: &str,
    formal_boundary_epoch: i64,
    approval_reference: &str,
    account: String,
    container: String,
    client_id: Option<String>,
) -> Result<serde_json::Value> {
    let resolution = prepare_ring_quarantine_resolution(
        root,
        receipt_id,
        formal_boundary_epoch,
        approval_reference,
    )?;
    let staging = recover_ring_quarantine_staging(&resolution)?;
    let already_resolved = resolution.final_directory.exists();
    if already_resolved {
        validate_local_ring_quarantine_resolution(&resolution)?;
    }

    let mut client = AzureBlobClient::with_managed_identity_for_large_immutable_upload(
        account, container, client_id,
    );
    let source = read_ring_quarantine_source(&resolution.source_path, resolution.source_bytes)?;
    if source.len() as u64 != resolution.source_bytes
        || sha256_prefixed(&source) != resolution.source_sha256
    {
        bail!("quarantined source changed before upload");
    }
    let source_created = upload_verified_quarantine_blob(
        &mut client,
        &resolution.source_blob_name,
        &source,
        "application/x-ndjson",
        &resolution.source_sha256,
    )?;
    drop(source);
    let receipt_created = upload_verified_quarantine_blob(
        &mut client,
        &resolution.receipt_blob_name,
        &resolution.receipt_bytes,
        "application/json",
        &resolution.receipt_sha256,
    )?;
    let resolution_sha256 = sha256_prefixed(&resolution.resolution_bytes);
    // The resolution object is the remote commit marker and must stay last.
    let resolution_created = upload_verified_quarantine_blob(
        &mut client,
        &resolution.resolution_blob_name,
        &resolution.resolution_bytes,
        "application/json",
        &resolution_sha256,
    )?;

    if !already_resolved {
        publish_local_ring_quarantine_resolution(&resolution, &staging, &resolution_sha256)?;
    }
    Ok(json!({
        "ok": true,
        "receipt_id": receipt_id,
        "already_resolved": already_resolved,
        "source_blob_created": source_created,
        "receipt_blob_created": receipt_created,
        "resolution_blob_created": resolution_created,
        "remote_prefix": format!("{RING_QUARANTINE_BLOB_PREFIX}/{receipt_id}"),
    }))
}

fn prepare_ring_quarantine_resolution(
    root: &Path,
    receipt_id: &str,
    formal_boundary_epoch: i64,
    approval_reference: &str,
) -> Result<RingQuarantineResolution> {
    if receipt_id.len() != 64
        || !receipt_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("quarantine receipt ID must be a lowercase sha256 digest");
    }
    if approval_reference.trim() != approval_reference
        || approval_reference.is_empty()
        || approval_reference.len() > 256
        || approval_reference.chars().any(char::is_control)
    {
        bail!("approval reference must be a nonempty single-line value of at most 256 bytes");
    }
    if formal_boundary_epoch <= 0 || formal_boundary_epoch % 600 != 0 {
        bail!("quarantine boundary is invalid");
    }
    let root = root
        .canonicalize()
        .with_context(|| format!("resolving ring root {}", root.display()))?;
    let segments = root
        .join("segments")
        .canonicalize()
        .context("resolving segments")?;
    let quarantine_path = root.join("quarantine");
    let quarantine_metadata =
        fs::symlink_metadata(&quarantine_path).context("reading quarantine directory metadata")?;
    if quarantine_metadata.file_type().is_symlink() || !quarantine_metadata.is_dir() {
        bail!("quarantine path is not a regular directory");
    }
    let quarantine = quarantine_path
        .canonicalize()
        .context("resolving quarantine directory")?;
    if !quarantine.starts_with(&root) {
        bail!("quarantine directory escapes ring root");
    }
    let receipt_root = quarantine.join("recorder-sequence-proof-v1");
    let receipt_root_metadata = fs::symlink_metadata(&receipt_root)
        .context("reading quarantine receipt directory metadata")?;
    if receipt_root_metadata.file_type().is_symlink() || !receipt_root_metadata.is_dir() {
        bail!("quarantine receipt path is not a regular directory");
    }
    let receipt_root = receipt_root
        .canonicalize()
        .context("resolving quarantine receipt directory")?;
    if !receipt_root.starts_with(&quarantine) {
        bail!("quarantine receipt directory escapes quarantine root");
    }
    let receipt_path = receipt_root.join(format!("{receipt_id}.json"));
    require_regular_file(&receipt_path)?;
    let receipt_bytes =
        fs::read(&receipt_path).with_context(|| format!("reading {}", receipt_path.display()))?;
    let receipt: serde_json::Value =
        serde_json::from_slice(&receipt_bytes).context("quarantine receipt must be valid JSON")?;
    let receipt_object = receipt
        .as_object()
        .context("quarantine receipt must be a JSON object")?;
    let expected_fields = [
        "reason_code",
        "schema",
        "source_bytes",
        "source_lines",
        "source_segment_path",
        "source_sha256",
        "type",
    ];
    if receipt_object.len() != expected_fields.len()
        || !receipt_object
            .keys()
            .all(|key| expected_fields.contains(&key.as_str()))
        || receipt["schema"].as_str() != Some("polyedge_ring_quarantine.v1")
        || receipt["type"].as_str() != Some("quarantine_receipt")
        || receipt["reason_code"].as_str() != Some("invalid_recorder_sequence_proof")
    {
        bail!("quarantine receipt has an invalid schema");
    }
    let source_relative = receipt["source_segment_path"]
        .as_str()
        .context("quarantine receipt source path must be a string")?;
    let source_relative_path = Path::new(source_relative);
    if source_relative_path.is_absolute()
        || !source_relative.starts_with("segments/")
        || !source_relative.ends_with(".jsonl")
        || source_relative
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || b"._-/".contains(&byte)))
        || source_relative_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("quarantine receipt source path is invalid");
    }
    let source_sha256 = ring_sha256(&receipt, "source_sha256")?;
    let expected_id = format!(
        "{:x}",
        Sha256::digest(format!(
            "{}:{source_relative}:{source_sha256}",
            source_relative.len()
        ))
    );
    if expected_id != receipt_id {
        bail!("quarantine receipt content address disagrees");
    }
    let source_path = root.join(source_relative_path);
    require_regular_file(&source_path)?;
    let source_real = source_path
        .canonicalize()
        .with_context(|| format!("resolving {}", source_path.display()))?;
    if !source_real.starts_with(&segments) {
        bail!("quarantine source escapes segments");
    }
    let source_bytes = receipt["source_bytes"]
        .as_u64()
        .context("quarantine receipt source_bytes must be an unsigned integer")?;
    let source_lines = receipt["source_lines"]
        .as_u64()
        .context("quarantine receipt source_lines must be an unsigned integer")?;
    validate_ring_quarantine_source_size(source_bytes, fs::metadata(&source_path)?.len())?;
    let (actual_sha256, actual_bytes, actual_lines) = file_sha_bytes_lines(&source_path)?;
    if actual_sha256 != source_sha256
        || actual_bytes != source_bytes
        || actual_lines != source_lines
    {
        bail!("quarantined source disagrees with its receipt");
    }
    let start = source_path
        .file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<i64>().ok())
        .context("quarantine source filename must be an epoch")?;
    let hour = DateTime::<Utc>::from_timestamp(start, 0)
        .context("quarantine source epoch is outside the UTC timestamp range")?
        .format("%Y/%m/%d/%H")
        .to_string();
    if source_relative != format!("segments/{hour}/{start}.jsonl") {
        bail!("quarantine source path is not exactly bound to its UTC epoch");
    }
    let end = start
        .checked_add(600)
        .context("quarantine segment end overflows")?;
    if end > formal_boundary_epoch {
        bail!("quarantined segment is not wholly pre-boundary");
    }
    for sidecar in [
        PathBuf::from(format!("{}.manifest.json", source_path.display())),
        root.join(format!("archive/{hour}/{start}.jsonl.gz")),
        root.join(format!("archive/{hour}/{start}.jsonl.gz.manifest.json")),
    ] {
        if fs::symlink_metadata(&sidecar).is_ok() {
            bail!(
                "quarantined segment has a sealed sidecar: {}",
                sidecar.display()
            );
        }
    }

    let remote_prefix = format!("{RING_QUARANTINE_BLOB_PREFIX}/{receipt_id}");
    let source_blob_name = format!("{remote_prefix}/source.jsonl");
    let receipt_blob_name = format!("{remote_prefix}/quarantine-receipt.json");
    let resolution_blob_name = format!("{remote_prefix}/resolution.json");
    let receipt_sha256 = sha256_prefixed(&receipt_bytes);
    let mut resolution_bytes = serde_json::to_vec_pretty(&json!({
        "schema": "polyedge_ring_quarantine_resolution.v1",
        "type": "invalid_recorder_sequence_proof_resolution",
        "disposition": "preserved_historical_pre_boundary_non_parity",
        "active_ring": false,
        "parity_eligible": false,
        "retention_policy": "indefinite_outside_lifecycle",
        "receipt_id": receipt_id,
        "approval_reference": approval_reference,
        "formal_boundary_epoch": formal_boundary_epoch,
        "segment_start_epoch": start,
        "segment_end_epoch": end,
        "source_segment_path": source_relative,
        "source_sha256": source_sha256,
        "source_bytes": source_bytes,
        "source_lines": source_lines,
        "quarantine_receipt_sha256": receipt_sha256,
        "remote_prefix": remote_prefix,
        "source_blob_name": source_blob_name,
        "quarantine_receipt_blob_name": receipt_blob_name,
        "resolution_blob_name": resolution_blob_name,
    }))?;
    resolution_bytes.push(b'\n');
    Ok(RingQuarantineResolution {
        source_path,
        receipt_bytes,
        resolution_bytes,
        source_sha256,
        source_bytes,
        receipt_sha256,
        source_blob_name,
        receipt_blob_name,
        resolution_blob_name,
        final_directory: quarantine
            .join("resolved-recorder-sequence-proof-v1")
            .join(receipt_id),
    })
}

fn validate_ring_quarantine_source_size(receipt_bytes: u64, actual_bytes: u64) -> Result<()> {
    if receipt_bytes != actual_bytes || actual_bytes > MAX_RING_QUARANTINE_SOURCE_BYTES {
        bail!("quarantine source exceeds 512 MiB or disagrees with its receipt");
    }
    Ok(())
}

fn read_ring_quarantine_source(path: &Path, expected_bytes: u64) -> Result<Vec<u8>> {
    validate_ring_quarantine_source_size(expected_bytes, fs::metadata(path)?.len())?;
    let mut source = Vec::with_capacity(expected_bytes as usize);
    fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?
        .take(MAX_RING_QUARANTINE_SOURCE_BYTES + 1)
        .read_to_end(&mut source)
        .with_context(|| format!("reading {}", path.display()))?;
    validate_ring_quarantine_source_size(expected_bytes, source.len() as u64)?;
    Ok(source)
}

fn upload_verified_quarantine_blob(
    client: &mut AzureBlobClient,
    blob_name: &str,
    bytes: &[u8],
    content_type: &str,
    expected_sha256: &str,
) -> Result<bool> {
    let created = matches!(
        client
            .upload_block_blob_bytes_if_absent(blob_name, bytes, content_type)
            .with_context(|| format!("uploading {blob_name}"))?,
        ImmutableBlobWrite::Created
    );
    let remote = client
        .download_blob_bytes_exact_bounded(
            blob_name,
            bytes.len() as u64,
            MAX_RING_QUARANTINE_SOURCE_BYTES,
        )
        .with_context(|| format!("verifying {blob_name}"))?;
    if remote.len() != bytes.len() || sha256_prefixed(&remote) != expected_sha256 {
        bail!("remote quarantine object disagrees: {blob_name}");
    }
    Ok(created)
}

fn publish_local_ring_quarantine_resolution(
    resolution: &RingQuarantineResolution,
    staging: &Path,
    resolution_sha256: &str,
) -> Result<()> {
    let parent = resolution
        .final_directory
        .parent()
        .context("resolved quarantine directory has no parent")?;
    let receipt_id = resolution
        .final_directory
        .file_name()
        .and_then(|value| value.to_str())
        .context("resolved quarantine directory has an invalid receipt ID")?;
    let expected_staging = parent.join(format!(".{receipt_id}.staging"));
    if staging != expected_staging {
        bail!("resolved quarantine staging path is not exactly bound");
    }
    fs::create_dir(staging).with_context(|| format!("creating {}", staging.display()))?;
    set_mode(staging, 0o750)?;
    let result = (|| -> Result<()> {
        let source_copy = staging.join("source.jsonl");
        fs::copy(&resolution.source_path, &source_copy)
            .with_context(|| format!("copying {}", resolution.source_path.display()))?;
        set_mode(&source_copy, 0o640)?;
        require_evidence_file(&source_copy)?;
        let (copied_sha, copied_bytes, _) = file_sha_bytes_lines(&source_copy)?;
        if copied_sha != resolution.source_sha256 || copied_bytes != resolution.source_bytes {
            bail!("local quarantine source copy disagrees");
        }
        fs::File::open(&source_copy)?.sync_all()?;
        write_new_exact(
            &staging.join("quarantine-receipt.json"),
            &resolution.receipt_bytes,
        )?;
        write_new_exact(
            &staging.join("resolution.json"),
            &resolution.resolution_bytes,
        )?;
        let mut uploaded = serde_json::to_vec_pretty(&json!({
            "schema": "polyedge_ring_quarantine_resolution_upload.v1",
            "receipt_id": receipt_id,
            "source_blob_name": resolution.source_blob_name,
            "quarantine_receipt_blob_name": resolution.receipt_blob_name,
            "resolution_blob_name": resolution.resolution_blob_name,
            "source_sha256": resolution.source_sha256,
            "quarantine_receipt_sha256": resolution.receipt_sha256,
            "resolution_sha256": resolution_sha256,
            "verified_ts": Utc::now().to_rfc3339(),
        }))?;
        uploaded.push(b'\n');
        write_new_exact(&staging.join("resolution.uploaded.json"), &uploaded)?;
        fs::File::open(staging)?.sync_all()?;
        fs::rename(staging, &resolution.final_directory).with_context(|| {
            format!(
                "publishing resolved quarantine {}",
                resolution.final_directory.display()
            )
        })?;
        set_mode(&resolution.final_directory, 0o750)?;
        fs::File::open(parent)?.sync_all()?;
        validate_local_ring_quarantine_resolution(resolution)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(staging);
    }
    result
}

fn recover_ring_quarantine_staging(resolution: &RingQuarantineResolution) -> Result<PathBuf> {
    let parent = resolution
        .final_directory
        .parent()
        .context("resolved quarantine directory has no parent")?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            bail!("resolved quarantine parent is not a regular directory")
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        Err(error) => return Err(error.into()),
    }
    set_mode(parent, 0o750)?;
    let receipt_id = resolution
        .final_directory
        .file_name()
        .and_then(|value| value.to_str())
        .context("resolved quarantine directory has an invalid receipt ID")?;
    let staging = parent.join(format!(".{receipt_id}.staging"));
    match fs::symlink_metadata(&staging) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            bail!("stale quarantine staging entry is not a regular directory")
        }
        Ok(_) => {
            fs::remove_dir_all(&staging)
                .with_context(|| format!("removing stale {}", staging.display()))?;
            fs::File::open(parent)?.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(staging)
}

fn validate_local_ring_quarantine_resolution(resolution: &RingQuarantineResolution) -> Result<()> {
    require_directory_mode(&resolution.final_directory, 0o750)?;
    let mut names = fs::read_dir(&resolution.final_directory)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    let expected = [
        "quarantine-receipt.json",
        "resolution.json",
        "resolution.uploaded.json",
        "source.jsonl",
    ];
    if names.len() != expected.len()
        || names
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual != expected)
    {
        bail!("resolved quarantine bundle does not contain exactly four expected files");
    }
    let source = resolution.final_directory.join("source.jsonl");
    let receipt = resolution.final_directory.join("quarantine-receipt.json");
    let resolution_path = resolution.final_directory.join("resolution.json");
    let uploaded_path = resolution.final_directory.join("resolution.uploaded.json");
    for path in [&source, &receipt, &resolution_path, &uploaded_path] {
        require_evidence_file(path)?;
    }
    let (source_sha, source_bytes, _) = file_sha_bytes_lines(&source)?;
    if source_sha != resolution.source_sha256 || source_bytes != resolution.source_bytes {
        bail!("resolved quarantine source disagrees");
    }
    if fs::read(&receipt)? != resolution.receipt_bytes
        || fs::read(&resolution_path)? != resolution.resolution_bytes
    {
        bail!("resolved quarantine evidence disagrees");
    }
    let uploaded_bytes = fs::read(&uploaded_path)?;
    let uploaded: serde_json::Value = serde_json::from_slice(&uploaded_bytes)
        .context("resolved quarantine upload receipt must be valid JSON")?;
    let object = uploaded
        .as_object()
        .context("resolved quarantine upload receipt must be an object")?;
    let expected_fields = [
        "quarantine_receipt_blob_name",
        "quarantine_receipt_sha256",
        "receipt_id",
        "resolution_blob_name",
        "resolution_sha256",
        "schema",
        "source_blob_name",
        "source_sha256",
        "verified_ts",
    ];
    if object.len() != expected_fields.len()
        || !object
            .keys()
            .all(|key| expected_fields.contains(&key.as_str()))
        || uploaded["schema"].as_str() != Some("polyedge_ring_quarantine_resolution_upload.v1")
        || uploaded["receipt_id"].as_str()
            != resolution
                .final_directory
                .file_name()
                .and_then(|value| value.to_str())
        || uploaded["source_blob_name"].as_str() != Some(&resolution.source_blob_name)
        || uploaded["quarantine_receipt_blob_name"].as_str() != Some(&resolution.receipt_blob_name)
        || uploaded["resolution_blob_name"].as_str() != Some(&resolution.resolution_blob_name)
        || uploaded["source_sha256"].as_str() != Some(&resolution.source_sha256)
        || uploaded["quarantine_receipt_sha256"].as_str() != Some(&resolution.receipt_sha256)
        || uploaded["resolution_sha256"].as_str()
            != Some(&sha256_prefixed(&resolution.resolution_bytes))
    {
        bail!("resolved quarantine upload receipt disagrees");
    }
    let verified_ts = uploaded["verified_ts"]
        .as_str()
        .filter(|value| !value.is_empty())
        .context("resolved quarantine verified_ts must be nonempty")?;
    DateTime::parse_from_rfc3339(verified_ts)
        .context("resolved quarantine verified_ts must be RFC 3339")?;
    Ok(())
}

fn require_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("reading metadata for {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("path is not a regular file: {}", path.display());
    }
    Ok(())
}

fn require_evidence_file(path: &Path) -> Result<()> {
    require_regular_file(path)?;
    let mode = fs::symlink_metadata(path)?.permissions().mode() & 0o777;
    if mode != 0o640 {
        bail!("evidence file mode must be 0640: {}", path.display());
    }
    Ok(())
}

fn require_directory_mode(path: &Path, expected: u32) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("reading metadata for {}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o777 != expected
    {
        bail!("directory mode or type is invalid: {}", path.display());
    }
    Ok(())
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting mode on {}", path.display()))
}

fn file_sha_bytes_lines(path: &Path) -> Result<(String, u64, u64)> {
    let mut file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut lines = 0_u64;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("quarantine source size overflows")?;
        if bytes > MAX_RING_QUARANTINE_SOURCE_BYTES {
            bail!("quarantine source exceeds 512 MiB");
        }
        lines = lines
            .checked_add(buffer[..read].iter().filter(|byte| **byte == b'\n').count() as u64)
            .context("quarantine source line count overflows")?;
    }
    Ok((format!("sha256:{:x}", hasher.finalize()), bytes, lines))
}

fn write_new_exact(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o640)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(bytes)?;
    file.set_permissions(fs::Permissions::from_mode(0o640))?;
    file.sync_all()?;
    Ok(())
}

fn accepted_ring_blob_prefix<'a>(
    manifest: &serde_json::Value,
    current_prefix: &'a str,
    receipt_exists: bool,
) -> &'a str {
    if receipt_exists
        && manifest["blob_name"]
            .as_str()
            .is_some_and(|name| name.starts_with(&format!("{LEGACY_RING_BLOB_PREFIX}/")))
    {
        LEGACY_RING_BLOB_PREFIX
    } else {
        current_prefix
    }
}

fn validate_ring_upload_receipt(
    receipt_bytes: &[u8],
    manifest_sha256: &str,
    blob_name: &str,
    manifest_blob_name: &str,
) -> Result<()> {
    let receipt: serde_json::Value =
        serde_json::from_slice(receipt_bytes).context("ring receipt must be valid JSON")?;
    let receipt = receipt
        .as_object()
        .context("ring receipt must be a JSON object")?;
    if receipt.len() != 5
        || !receipt.keys().all(|key| {
            matches!(
                key.as_str(),
                "schema_version"
                    | "manifest_sha256"
                    | "blob_name"
                    | "manifest_blob_name"
                    | "verified_ts"
            )
        })
    {
        bail!("ring receipt must contain exactly the expected fields");
    }
    if receipt
        .get("schema_version")
        .and_then(|value| value.as_u64())
        != Some(1)
    {
        bail!("ring receipt schema_version must equal 1");
    }
    if receipt
        .get("manifest_sha256")
        .and_then(|value| value.as_str())
        != Some(manifest_sha256)
        || receipt.get("blob_name").and_then(|value| value.as_str()) != Some(blob_name)
        || receipt
            .get("manifest_blob_name")
            .and_then(|value| value.as_str())
            != Some(manifest_blob_name)
    {
        bail!("ring receipt identity disagrees with its manifest");
    }
    let verified_ts = receipt
        .get("verified_ts")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .context("ring receipt verified_ts must be a nonempty string")?;
    DateTime::parse_from_rfc3339(verified_ts)
        .context("ring receipt verified_ts must be an RFC 3339 timestamp")?;
    Ok(())
}

fn collect_ring_manifests(path: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("listing {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_ring_manifests(&path, output)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.ends_with(".jsonl.manifest.json") || name.ends_with(".jsonl.gz.manifest.json")
            })
        {
            output.push(path);
        }
    }
    Ok(())
}

fn ring_relative_path(manifest: &serde_json::Value, field: &str) -> Result<PathBuf> {
    let value = manifest[field]
        .as_str()
        .with_context(|| format!("ring manifest {field} must be a string"))?;
    let path = PathBuf::from(value);
    if path.is_absolute()
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        || !(value.ends_with(".jsonl") || value.ends_with(".jsonl.gz"))
    {
        bail!("ring manifest {field} must be a relative JSONL or JSONL.gz path without traversal");
    }
    Ok(path)
}

fn ring_blob_name(manifest: &serde_json::Value, blob_prefix: &str) -> Result<String> {
    let value = manifest["blob_name"]
        .as_str()
        .context("ring manifest blob_name must be a string")?;
    if !value.starts_with(&format!("{blob_prefix}/"))
        || value.starts_with('/')
        || !(value.ends_with(".jsonl") || value.ends_with(".jsonl.gz"))
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || value.chars().any(char::is_control)
    {
        bail!("ring manifest blob_name is invalid");
    }
    Ok(value.to_owned())
}

fn validate_ring_identity(
    source_path: &Path,
    payload_path: &Path,
    blob_name: &str,
    blob_prefix: &str,
    segment_start: i64,
    segment_end: i64,
    now: i64,
) -> Result<()> {
    let hour = DateTime::<Utc>::from_timestamp(segment_start, 0)
        .context("segment_start_epoch is outside the UTC timestamp range")?
        .format("%Y/%m/%d/%H")
        .to_string();
    let compressed = blob_name.ends_with(".jsonl.gz");
    let expected_source = format!("segments/{hour}/{segment_start}.jsonl");
    let expected_payload = if compressed {
        format!("archive/{hour}/{segment_start}.jsonl.gz")
    } else {
        expected_source.clone()
    };
    let expected_blob = if compressed {
        format!("{blob_prefix}/{hour}/{segment_start}.jsonl.gz")
    } else {
        format!("{blob_prefix}/{hour}/{segment_start}.jsonl")
    };
    if !(300..=900).contains(&segment_end.saturating_sub(segment_start))
        || segment_end > now
        || source_path != Path::new(&expected_source)
        || payload_path != Path::new(&expected_payload)
        || blob_name != expected_blob
    {
        bail!("segment path, blob name, or UTC interval is not exactly bound");
    }
    Ok(())
}

fn validate_ring_manifest_v3_sequence(manifest: &serde_json::Value) -> Result<()> {
    let instance_id = manifest["recorder_instance_id"]
        .as_str()
        .context("ring manifest v3 recorder_instance_id must be a string")?;
    if !is_canonical_uuid_v4(instance_id) {
        bail!("ring manifest v3 recorder_instance_id must be a lowercase UUID v4");
    }
    let first = manifest["recorder_first_sequence"]
        .as_u64()
        .filter(|sequence| *sequence >= 1)
        .context("ring manifest v3 recorder_first_sequence must be an unsigned integer >= 1")?;
    let last = manifest["recorder_last_sequence"]
        .as_u64()
        .context("ring manifest v3 recorder_last_sequence must be an unsigned integer")?;
    let count = manifest["recorder_event_count"]
        .as_u64()
        .filter(|count| *count >= 1)
        .context("ring manifest v3 recorder_event_count must be an unsigned integer >= 1")?;
    let lines = manifest["lines"]
        .as_u64()
        .context("ring manifest v3 lines must be an unsigned integer")?;
    if last < first
        || last.checked_sub(first).and_then(|span| span.checked_add(1)) != Some(count)
        || count != lines
    {
        bail!("ring manifest v3 recorder sequence evidence is inconsistent");
    }
    Ok(())
}

#[derive(Debug)]
struct RingRecorderRun {
    instance_id: String,
    first: u64,
    last: u64,
    count: u64,
}

fn ring_manifest_v4_runs(manifest: &serde_json::Value) -> Result<Vec<RingRecorderRun>> {
    if [
        "recorder_instance_id",
        "recorder_first_sequence",
        "recorder_last_sequence",
        "recorder_event_count",
    ]
    .iter()
    .any(|field| manifest.get(*field).is_some())
    {
        bail!("ring manifest v4 must use recorder_runs instead of v3 recorder fields");
    }
    let lines = manifest["lines"]
        .as_u64()
        .context("ring manifest v4 lines must be an unsigned integer")?;
    let values = manifest["recorder_runs"]
        .as_array()
        .filter(|runs| !runs.is_empty())
        .context("ring manifest v4 recorder_runs must be a nonempty array")?;
    let mut runs = Vec::with_capacity(values.len());
    let mut seen_instances = HashSet::new();
    for (index, value) in values.iter().enumerate() {
        let instance_id = value["recorder_instance_id"]
            .as_str()
            .filter(|value| is_canonical_uuid_v4(value))
            .context("ring manifest v4 recorder run instance ID must be a lowercase UUID v4")?
            .to_owned();
        let first = value["recorder_first_sequence"]
            .as_u64()
            .filter(|sequence| *sequence >= 1)
            .context(
                "ring manifest v4 recorder run first sequence must be an unsigned integer >= 1",
            )?;
        let last = value["recorder_last_sequence"]
            .as_u64()
            .context("ring manifest v4 recorder run last sequence must be an unsigned integer")?;
        let count = value["recorder_event_count"]
            .as_u64()
            .filter(|count| *count >= 1)
            .context(
                "ring manifest v4 recorder run event count must be an unsigned integer >= 1",
            )?;
        if last < first
            || last.checked_sub(first).and_then(|span| span.checked_add(1)) != Some(count)
            || (index > 0 && (first != 1 || !seen_instances.insert(instance_id.clone())))
        {
            bail!("ring manifest v4 recorder run evidence is inconsistent");
        }
        if index == 0 {
            seen_instances.insert(instance_id.clone());
        }
        runs.push(RingRecorderRun {
            instance_id,
            first,
            last,
            count,
        });
    }
    if runs
        .iter()
        .try_fold(0_u64, |total, run| total.checked_add(run.count))
        != Some(lines)
    {
        bail!("ring manifest v4 recorder run counts must exactly cover lines");
    }
    Ok(runs)
}

fn validate_ring_manifest_v4_runs(manifest: &serde_json::Value) -> Result<()> {
    ring_manifest_v4_runs(manifest).map(|_| ())
}

fn validate_ring_source_v3(source_path: &Path, manifest: &serde_json::Value) -> Result<()> {
    let instance_id = manifest["recorder_instance_id"]
        .as_str()
        .context("ring manifest v3 recorder_instance_id must be a string")?;
    let first = manifest["recorder_first_sequence"]
        .as_u64()
        .context("ring manifest v3 recorder_first_sequence must be an unsigned integer")?;
    let last = manifest["recorder_last_sequence"]
        .as_u64()
        .context("ring manifest v3 recorder_last_sequence must be an unsigned integer")?;
    let count = manifest["recorder_event_count"]
        .as_u64()
        .context("ring manifest v3 recorder_event_count must be an unsigned integer")?;
    let lines = manifest["lines"]
        .as_u64()
        .context("ring manifest v3 lines must be an unsigned integer")?;
    if !is_canonical_uuid_v4(instance_id) {
        bail!("ring manifest v3 recorder_instance_id must be a lowercase UUID v4");
    }
    if count == 0 || count != lines {
        bail!("ring manifest v3 recorder_event_count must equal nonzero lines");
    }

    let file = fs::File::open(source_path)
        .with_context(|| format!("opening sealed ring source {}", source_path.display()))?;
    let mut seen = 0_u64;
    for line in BufReader::new(file).lines() {
        let line =
            line.with_context(|| format!("reading sealed ring source {}", source_path.display()))?;
        if line.trim().is_empty() {
            bail!("sealed ring source contains a blank line");
        }
        seen = seen
            .checked_add(1)
            .context("sealed ring source line count overflow")?;
        if seen > count {
            bail!("sealed ring source contains more events than its manifest");
        }
        let expected_sequence = first
            .checked_add(seen - 1)
            .context("sealed ring source sequence overflows")?;
        let event: serde_json::Value =
            serde_json::from_str(&line).context("sealed ring source contains invalid JSON")?;
        let event_instance_id = event["recorder_instance_id"]
            .as_str()
            .filter(|value| is_canonical_uuid_v4(value))
            .context("sealed ring source recorder_instance_id is invalid")?;
        if event_instance_id != instance_id {
            bail!("sealed ring source recorder_instance_id disagrees with its manifest");
        }
        if event["recorder_sequence"].as_u64() != Some(expected_sequence) {
            bail!("sealed ring source recorder_sequence is not contiguous");
        }
    }
    if seen != count || first.checked_add(seen - 1) != Some(last) {
        bail!("sealed ring source sequence proof disagrees with its manifest");
    }
    Ok(())
}

fn validate_ring_source_v4(source_path: &Path, manifest: &serde_json::Value) -> Result<()> {
    let runs = ring_manifest_v4_runs(manifest)?;
    let file = fs::File::open(source_path)
        .with_context(|| format!("opening sealed ring source {}", source_path.display()))?;
    let mut run_index = 0_usize;
    let mut seen_in_run = 0_u64;
    for line in BufReader::new(file).lines() {
        let line =
            line.with_context(|| format!("reading sealed ring source {}", source_path.display()))?;
        if line.trim().is_empty() || run_index == runs.len() {
            bail!("sealed ring source contains a blank or excess event");
        }
        let event: serde_json::Value =
            serde_json::from_str(&line).context("sealed ring source contains invalid JSON")?;
        let run = &runs[run_index];
        let expected_sequence = run
            .first
            .checked_add(seen_in_run)
            .context("sealed ring source sequence overflows")?;
        if event["recorder_instance_id"].as_str() != Some(&run.instance_id)
            || event["recorder_sequence"].as_u64() != Some(expected_sequence)
        {
            bail!("sealed ring source does not exactly match its recorder runs");
        }
        seen_in_run += 1;
        if seen_in_run == run.count {
            if expected_sequence != run.last {
                bail!("sealed ring source recorder run disagrees with its manifest");
            }
            run_index += 1;
            seen_in_run = 0;
        }
    }
    if run_index != runs.len() || seen_in_run != 0 {
        bail!("sealed ring source recorder runs do not exactly cover its manifest");
    }
    Ok(())
}

fn is_canonical_uuid_v4(value: &str) -> bool {
    value.len() == 36
        && value.as_bytes()[8] == b'-'
        && value.as_bytes()[13] == b'-'
        && value.as_bytes()[18] == b'-'
        && value.as_bytes()[23] == b'-'
        && value.as_bytes()[14] == b'4'
        && matches!(value.as_bytes()[19], b'8' | b'9' | b'a' | b'b')
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                || byte.is_ascii_digit()
                || byte.is_ascii_lowercase() && byte.is_ascii_hexdigit()
        })
}

fn ring_sha256(manifest: &serde_json::Value, field: &str) -> Result<String> {
    let value = manifest[field]
        .as_str()
        .with_context(|| format!("ring manifest {field} must be a string"))?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("ring manifest {field} must be a lowercase sha256 digest");
    }
    Ok(value.to_owned())
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn bench_azure_replay(
    account: String,
    container: String,
    prefix: String,
    sas_env: String,
    max_blobs: Option<usize>,
    max_bytes: Option<u64>,
    prefetch_blobs: usize,
) -> Result<serde_json::Value> {
    let sas = std::env::var(&sas_env).with_context(|| {
        format!("{sas_env} must contain a read/list SAS token for the container")
    })?;
    let mut client = AzureBlobClient::new(&account, &container, sas);
    let list_start = Instant::now();
    let blobs = client
        .list_blobs(&prefix, max_blobs, max_bytes)
        .context("listing Azure blobs")?;
    let list_elapsed = list_start.elapsed();
    let listed_bytes = blobs.iter().map(|blob| blob.content_length).sum::<u64>();
    let replay_start = Instant::now();
    let mut backtester = ReplayBacktester::new(BacktestConfig::new(format!(
        "azure://{account}/{container}/{prefix}"
    )));
    let replayed_bytes =
        replay_prefetched_azure_blobs(client, blobs.clone(), prefetch_blobs, &mut backtester)?;
    let replay_elapsed = replay_start.elapsed();
    let result = backtester.finish();
    Ok(json!({
        "source": "azure_blob",
        "transport": "native_ureq_persistent_prefetch",
        "account": account,
        "container": container,
        "prefix": prefix,
        "listed_blobs": blobs.len(),
        "listed_bytes": listed_bytes,
        "listed_gib": listed_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
        "replayed_bytes": replayed_bytes,
        "replayed_gib": replayed_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
        "events": result.event_count,
        "elapsed_ms": replay_elapsed.as_secs_f64() * 1000.0,
        "events_per_second": if replay_elapsed.as_secs_f64() == 0.0 { 0.0 } else { result.event_count as f64 / replay_elapsed.as_secs_f64() },
        "bytes_per_second": if replay_elapsed.as_secs_f64() == 0.0 { 0.0 } else { replayed_bytes as f64 / replay_elapsed.as_secs_f64() },
        "mib_per_second": if replay_elapsed.as_secs_f64() == 0.0 { 0.0 } else { replayed_bytes as f64 / 1024.0 / 1024.0 / replay_elapsed.as_secs_f64() },
        "filled_orders": result.filled_orders,
        "net_pnl": result.net_pnl,
        "list_elapsed_ms": list_elapsed.as_secs_f64() * 1000.0,
        "prefetch_blobs": prefetch_blobs.max(1).min(blobs.len().max(1)),
        "memory_rss_mb": rss_mb()
    }))
}

#[derive(Debug)]
struct PrefetchedBlob {
    index: usize,
    blob: AzureBlobItem,
    bytes: Vec<u8>,
}

fn replay_prefetched_azure_blobs(
    client: AzureBlobClient,
    blobs: Vec<AzureBlobItem>,
    prefetch_blobs: usize,
    backtester: &mut ReplayBacktester,
) -> Result<u64> {
    if blobs.is_empty() {
        return Ok(0);
    }
    let total_blobs = blobs.len();
    let worker_count = prefetch_blobs.max(1).min(blobs.len());
    let (job_tx, job_rx) = mpsc::channel::<(usize, AzureBlobItem)>();
    let (result_tx, result_rx) = mpsc::sync_channel::<Result<PrefetchedBlob>>(worker_count);
    let job_rx = Arc::new(Mutex::new(job_rx));
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let mut worker_client = client.clone();
        let worker_job_rx = Arc::clone(&job_rx);
        let worker_result_tx = result_tx.clone();
        handles.push(thread::spawn(move || {
            while let Ok((index, blob)) = worker_job_rx
                .lock()
                .map_err(|_| ())
                .and_then(|receiver| receiver.recv().map_err(|_| ()))
            {
                let result = worker_client
                    .download_blob_bytes(&blob.name)
                    .with_context(|| format!("downloading {}", blob.name))
                    .map(|bytes| PrefetchedBlob { index, blob, bytes });
                if worker_result_tx.send(result).is_err() {
                    break;
                }
            }
        }));
    }
    drop(result_tx);

    let mut blob_iter = blobs.into_iter().enumerate();
    let mut pending = BTreeMap::new();
    let mut next_index = 0_usize;
    let mut in_flight = 0_usize;
    let mut replayed_bytes = 0_u64;

    fill_prefetch_window(
        &job_tx,
        &mut blob_iter,
        &pending,
        &mut in_flight,
        worker_count,
    )?;
    while next_index < total_blobs {
        let prefetched = result_rx
            .recv()
            .context("Azure blob download workers stopped before replay completed")??;
        in_flight = in_flight.saturating_sub(1);
        pending.insert(prefetched.index, prefetched);
        while let Some(prefetched) = pending.remove(&next_index) {
            let bytes_len = prefetched.bytes.len() as u64;
            backtester
                .run_reader(BufReader::with_capacity(
                    REPLAY_BUFFER_BYTES,
                    Cursor::new(prefetched.bytes),
                ))
                .with_context(|| format!("replaying {}", prefetched.blob.name))?;
            replayed_bytes += bytes_len;
            next_index += 1;
        }
        fill_prefetch_window(
            &job_tx,
            &mut blob_iter,
            &pending,
            &mut in_flight,
            worker_count,
        )?;
    }
    drop(job_tx);
    while let Ok(prefetched) = result_rx.try_recv() {
        let prefetched = prefetched?;
        pending.insert(prefetched.index, prefetched);
        while let Some(prefetched) = pending.remove(&next_index) {
            let bytes_len = prefetched.bytes.len() as u64;
            backtester
                .run_reader(BufReader::with_capacity(
                    REPLAY_BUFFER_BYTES,
                    Cursor::new(prefetched.bytes),
                ))
                .with_context(|| format!("replaying {}", prefetched.blob.name))?;
            replayed_bytes += bytes_len;
            next_index += 1;
        }
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("Azure blob download worker panicked"))?;
    }
    if !pending.is_empty() {
        bail!("Azure blob prefetch completed with unreplayed out-of-order blobs");
    }
    Ok(replayed_bytes)
}

fn fill_prefetch_window<I>(
    job_tx: &mpsc::Sender<(usize, AzureBlobItem)>,
    blob_iter: &mut I,
    pending: &BTreeMap<usize, PrefetchedBlob>,
    in_flight: &mut usize,
    worker_count: usize,
) -> Result<()>
where
    I: Iterator<Item = (usize, AzureBlobItem)>,
{
    while *in_flight + pending.len() < worker_count {
        let Some((index, blob)) = blob_iter.next() else {
            break;
        };
        job_tx
            .send((index, blob))
            .context("queueing Azure blob download job")?;
        *in_flight += 1;
    }
    Ok(())
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let index = ((sorted_values.len() - 1) as f64 * percentile).round() as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

fn rss_mb() -> Option<f64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<f64>().ok()?;
    Some(pages * 4096.0 / 1024.0 / 1024.0)
}

fn print_json(value: serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn profitability_authorization_flags(
    shadow_gate_passed: bool,
    execution_promotion_allowed: bool,
) -> String {
    format!(
        "shadow_gate_passed={shadow_gate_passed} execution_promotion_allowed={execution_promotion_allowed}"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        accepted_ring_blob_prefix, prepare_ring_quarantine_resolution,
        profitability_authorization_flags, publish_local_ring_quarantine_resolution,
        qset_v2_inventory_sha256, recover_ring_quarantine_staging, require_qset_v4_prepared,
        ring_blob_name, ring_relative_path, ring_sha256, sha256_prefixed,
        terminate_lease_child_tree, validate_local_ring_quarantine_resolution,
        validate_qset_v2_inventory, validate_ring_identity, validate_ring_manifest_v3_sequence,
        validate_ring_manifest_v4_runs, validate_ring_quarantine_source_size,
        validate_ring_source_v3, validate_ring_source_v4, validate_ring_upload_receipt,
        AzureBlobItem, Cli, Command, Path, PathBuf, ResearchCommand,
        MAX_RING_QUARANTINE_SOURCE_BYTES, RING_QUARANTINE_BLOB_PREFIX,
    };
    use clap::Parser;
    use serde_json::json;
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn qset_v4_term_requires_a_successful_prepare_receipt() {
        assert!(require_qset_v4_prepared(false).is_err());
        assert!(require_qset_v4_prepared(true).is_ok());
    }

    // Clap builds the full nested command tree on the stack; use the same
    // 8 MiB stack as the production Linux main thread, not a 2 MiB test worker.
    fn try_parse_cli<I, T>(args: I) -> std::result::Result<Cli, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString>,
    {
        let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
        std::thread::Builder::new()
            .name("cli-parse-test".to_owned())
            .stack_size(8 * 1024 * 1024)
            .spawn(move || Cli::try_parse_from(args))
            .unwrap()
            .join()
            .unwrap()
    }
    #[test]
    fn qset_v2_sealer_is_exact_closed_day_only() {
        let prefix = "shadow-events/campaign-2026-08-22-qset-v2/2026/08/22/";
        let blobs = (0_u8..24)
            .flat_map(|hour| (0_u8..60).map(move |minute| (hour, minute)))
            .map(|(hour, minute)| AzureBlobItem {
                name: format!("{prefix}{hour:02}/{minute:02}.jsonl"),
                etag: format!("\"etag-{hour}-{minute}\""),
                version_id: None,
                is_current_version: None,
                content_md5: None,
                blob_type: Some("AppendBlob".to_owned()),
                sealed: Some(false),
                content_length: 1,
                last_modified: Some(chrono::Utc::now()),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            validate_qset_v2_inventory(blobs.clone(), prefix)
                .unwrap()
                .len(),
            1_440
        );

        let source_digest = qset_v2_inventory_sha256(&blobs).unwrap();
        let mut sealed = blobs.clone();
        for blob in &mut sealed {
            blob.etag.push_str("-sealed");
            blob.last_modified = Some(chrono::Utc::now() + chrono::Duration::seconds(1));
            blob.sealed = Some(true);
        }
        assert_eq!(qset_v2_inventory_sha256(&sealed).unwrap(), source_digest);
        sealed[0].content_length += 1;
        assert_ne!(qset_v2_inventory_sha256(&sealed).unwrap(), source_digest);

        let mut invalid = blobs.clone();
        invalid[0].name =
            "shadow-events/campaign-2026-07-28-qset-v1/2026/08/22/00/00.jsonl".to_owned();
        assert!(validate_qset_v2_inventory(invalid, prefix).is_err());
        let mut invalid = blobs.clone();
        invalid[0].sealed = None;
        assert!(validate_qset_v2_inventory(invalid, prefix).is_err());
        assert!(validate_qset_v2_inventory(blobs[..1_439].to_vec(), prefix).is_err());

        let cli = try_parse_cli([
            "polyedge-rs",
            "seal-qset-v2-day",
            "--account",
            "storage",
            "--date",
            "2026-08-22",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::SealQsetV2Day { .. }));
        assert!(try_parse_cli([
            "polyedge-rs",
            "seal-qset-v2-day",
            "--account",
            "storage",
            "--date",
            "2026-08-22",
            "--container",
            "wrong",
        ])
        .is_err());
    }

    #[test]
    fn qset_v3_sealer_is_exact_closed_day_only() {
        let prefix = "shadow-events/campaign-2026-08-23-qset-v3/2026/08/23/";
        let blobs = (0_u8..24)
            .flat_map(|hour| (0_u8..60).map(move |minute| (hour, minute)))
            .map(|(hour, minute)| AzureBlobItem {
                name: format!("{prefix}{hour:02}/{minute:02}.jsonl"),
                etag: format!("\"etag-{hour}-{minute}\""),
                version_id: None,
                is_current_version: None,
                content_md5: None,
                blob_type: Some("AppendBlob".to_owned()),
                sealed: Some(false),
                content_length: 1,
                last_modified: Some(chrono::Utc::now()),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            super::validate_qset_inventory(&super::QSET_V3_SEAL_CONFIG, blobs.clone(), prefix)
                .unwrap()
                .len(),
            1_440
        );
        let source_digest = qset_v2_inventory_sha256(&blobs).unwrap();
        let mut sealed = blobs.clone();
        sealed[0].etag.push_str("-sealed");
        sealed[0].sealed = Some(true);
        assert_eq!(qset_v2_inventory_sha256(&sealed).unwrap(), source_digest);
        assert!(super::validate_qset_inventory(
            &super::QSET_V3_SEAL_CONFIG,
            blobs[..1_439].to_vec(),
            prefix
        )
        .is_err());
        assert_eq!(super::QSET_V3_DATES, ["2026-08-23", "2026-08-24"]);

        let freeze_sha256 =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let freeze_blob =
            "reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
        assert!(super::valid_qset_source_freeze_binding(
            &super::QSET_V3_SEAL_CONFIG,
            freeze_blob,
            freeze_sha256
        ));
        assert!(!super::valid_qset_source_freeze_binding(&super::QSET_V3_SEAL_CONFIG,
            "reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json",
            freeze_sha256
        ));
        assert!(!super::valid_qset_source_freeze_binding(&super::QSET_V3_SEAL_CONFIG,
            "reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json?x=1",
            freeze_sha256
        ));

        let cli = try_parse_cli([
            "polyedge-rs",
            "seal-qset-v3-day",
            "--account",
            "storage",
            "--date",
            "2026-08-23",
            "--source-freeze-blob",
            "reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
            "--source-freeze-sha256",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::SealQsetV3Day { .. }));
        assert!(try_parse_cli([
            "polyedge-rs",
            "seal-qset-v3-day",
            "--account",
            "storage",
            "--date",
            "2026-08-23",
        ])
        .is_err());
    }

    #[test]
    fn qset_v4_sealer_contract_is_exact() {
        const SHA: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const V4_BLOB: &str =
            "reports/research/shadow/campaigns/campaign-2026-08-24-qset-v4/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
        const V3_BLOB: &str =
            "reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
        const WRONG_HASH_BLOB: &str =
            "reports/research/shadow/campaigns/campaign-2026-08-24-qset-v4/control/code-freeze/source-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json";

        assert!(super::valid_qset_source_freeze_binding(
            &super::QSET_V4_SEAL_CONFIG,
            V4_BLOB,
            SHA
        ));
        for (config, blob) in [
            (&super::QSET_V4_SEAL_CONFIG, V3_BLOB),
            (&super::QSET_V3_SEAL_CONFIG, V4_BLOB),
            (&super::QSET_V4_SEAL_CONFIG, WRONG_HASH_BLOB),
        ] {
            assert!(!super::valid_qset_source_freeze_binding(config, blob, SHA));
        }

        let after_both = chrono::NaiveDate::from_ymd_opt(2026, 8, 26).unwrap();
        for (day, valid) in [(23, false), (24, true), (25, true)] {
            let date = chrono::NaiveDate::from_ymd_opt(2026, 8, day).unwrap();
            assert_eq!(
                super::validate_qset_seal_date(&super::QSET_V4_SEAL_CONFIG, date, after_both)
                    .is_ok(),
                valid
            );
        }
        let second_date = chrono::NaiveDate::from_ymd_opt(2026, 8, 25).unwrap();
        assert!(super::validate_qset_seal_date(
            &super::QSET_V4_SEAL_CONFIG,
            second_date,
            second_date
        )
        .is_err());

        let cli = try_parse_cli([
            "polyedge-rs",
            "seal-qset-v4-day",
            "--account",
            "storage",
            "--date",
            "2026-08-24",
            "--source-freeze-blob",
            V4_BLOB,
            "--source-freeze-sha256",
            SHA,
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::SealQsetV4Day { .. }));
        assert!(try_parse_cli([
            "polyedge-rs",
            "seal-qset-v4-day",
            "--account",
            "storage",
            "--date",
            "2026-08-24",
        ])
        .is_err());

        let date = chrono::NaiveDate::from_ymd_opt(2026, 8, 24).unwrap();
        let prefix = "shadow-events/campaign-2026-08-24-qset-v4/2026/08/24/";
        let blobs = [AzureBlobItem {
            name: format!("{prefix}00/00.jsonl"),
            etag: "\"etag\"".to_owned(),
            version_id: None,
            is_current_version: None,
            content_md5: None,
            blob_type: Some("AppendBlob".to_owned()),
            sealed: Some(true),
            content_length: 7,
            last_modified: None,
        }];
        let receipt = |sealed| {
            super::qset_closed_day_receipt(
                &super::QSET_V4_SEAL_CONFIG,
                "storage",
                date,
                prefix,
                &blobs,
                "sha256:inventory",
                V4_BLOB,
                SHA,
                sealed,
            )
            .unwrap()
        };
        let validation = receipt(false);
        assert_eq!(
            validation["schema"],
            "polyedge.qset_v4_closed_day_validation.v1"
        );
        assert_eq!(validation["campaign_id"], "campaign-2026-08-24-qset-v4");
        assert_eq!(validation["container"], "polyedge-shadow-qset-v4-events");
        assert_eq!(validation["prefix"], prefix);
        assert_eq!(validation["blob_count"], 1);
        assert_eq!(validation["total_bytes"], 7);
        assert_eq!(validation["source_inventory_sha256"], "sha256:inventory");
        assert_eq!(
            validation["source_freeze"],
            json!({
                "container": "polyedge-qset-v4-control",
                "blob": V4_BLOB,
                "sha256": SHA,
                "verified": true
            })
        );
        assert!(validation.get("generated_ts").is_none());
        assert!(validation.get("sealed_at").is_none());
        assert!(validation.get("sealed_blob_count").is_none());

        let seal = receipt(true);
        assert_eq!(seal["schema"], "polyedge.qset_v4_closed_day_seal.v1");
        assert_eq!(seal["sealed_blob_count"], 1);
        assert!(seal.get("generated_ts").is_none());
        assert!(seal.get("sealed_at").is_none());
    }
    fn quarantine_fixture() -> (PathBuf, String) {
        let root = std::env::temp_dir().join(format!(
            "polyedge-ring-quarantine-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_relative = "segments/2026/08/16/17/1786900200.jsonl";
        let source = root.join(source_relative);
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::create_dir_all(root.join("archive/2026/08/16/17")).unwrap();
        fs::create_dir_all(root.join("quarantine/recorder-sequence-proof-v1")).unwrap();
        fs::write(
            &source,
            b"{\"recorder_instance_id\":\"7c66d77b-a911-4f9b-95f2-98ca9395255e\",\"recorder_sequence\":1}\n{\"recorder_instance_id\":\"7c66d77b-a911-4f9b-95f2-98ca9395255e\",\"recorder_sequence\":3}\n",
        )
        .unwrap();
        let source_bytes = fs::read(&source).unwrap();
        let source_sha = sha256_prefixed(&source_bytes);
        let identity = format!("{}:{source_relative}:{source_sha}", source_relative.len());
        let receipt_id = sha256_prefixed(identity.as_bytes())[7..].to_owned();
        let receipt = json!({
            "schema": "polyedge_ring_quarantine.v1",
            "type": "quarantine_receipt",
            "source_segment_path": source_relative,
            "source_sha256": source_sha,
            "source_bytes": source_bytes.len(),
            "source_lines": 2,
            "reason_code": "invalid_recorder_sequence_proof",
        });
        fs::write(
            root.join(format!(
                "quarantine/recorder-sequence-proof-v1/{receipt_id}.json"
            )),
            format!("{}\n", serde_json::to_string(&receipt).unwrap()),
        )
        .unwrap();
        (root, receipt_id)
    }

    #[test]
    fn ring_quarantine_resolution_is_fixed_pre_boundary_and_idempotent() {
        let (root, receipt_id) = quarantine_fixture();
        let resolution = prepare_ring_quarantine_resolution(
            &root,
            &receipt_id,
            1786924800,
            "approved-change-123",
        )
        .unwrap();
        assert_eq!(
            resolution.source_blob_name,
            format!("{RING_QUARANTINE_BLOB_PREFIX}/{receipt_id}/source.jsonl")
        );
        let resolution_json: serde_json::Value =
            serde_json::from_slice(&resolution.resolution_bytes).unwrap();
        assert_eq!(resolution_json["active_ring"], false);
        assert_eq!(resolution_json["parity_eligible"], false);
        assert_eq!(
            resolution_json["retention_policy"],
            "indefinite_outside_lifecycle"
        );
        let resolution_sha = sha256_prefixed(&resolution.resolution_bytes);
        let staging = recover_ring_quarantine_staging(&resolution).unwrap();
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("partial"), b"interrupted").unwrap();
        let staging = recover_ring_quarantine_staging(&resolution).unwrap();
        assert!(!staging.exists());
        publish_local_ring_quarantine_resolution(&resolution, &staging, &resolution_sha).unwrap();
        validate_local_ring_quarantine_resolution(&resolution).unwrap();
        assert_eq!(
            fs::metadata(&resolution.final_directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
        for name in [
            "source.jsonl",
            "quarantine-receipt.json",
            "resolution.json",
            "resolution.uploaded.json",
        ] {
            assert_eq!(
                fs::metadata(resolution.final_directory.join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o640
            );
        }
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("partial"), b"interrupted").unwrap();
        recover_ring_quarantine_staging(&resolution).unwrap();
        assert!(!staging.exists());
        validate_local_ring_quarantine_resolution(&resolution).unwrap();
        symlink("/tmp", &staging).unwrap();
        assert!(recover_ring_quarantine_staging(&resolution).is_err());
        assert!(staging.symlink_metadata().unwrap().file_type().is_symlink());
        fs::remove_file(&staging).unwrap();
        assert!(prepare_ring_quarantine_resolution(
            &root,
            &receipt_id,
            1786924800,
            "approved-change-123",
        )
        .is_ok());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ring_quarantine_resolution_fails_closed_on_boundary_tamper_and_partial_bundle() {
        let (root, receipt_id) = quarantine_fixture();
        assert!(prepare_ring_quarantine_resolution(
            &root,
            &receipt_id,
            1786900200,
            "approved-change-123",
        )
        .is_err());
        let resolution = prepare_ring_quarantine_resolution(
            &root,
            &receipt_id,
            1786924800,
            "approved-change-123",
        )
        .unwrap();
        let resolution_sha = sha256_prefixed(&resolution.resolution_bytes);
        let staging = recover_ring_quarantine_staging(&resolution).unwrap();
        publish_local_ring_quarantine_resolution(&resolution, &staging, &resolution_sha).unwrap();
        fs::write(
            resolution.final_directory.join("source.jsonl"),
            b"tampered\n",
        )
        .unwrap();
        assert!(validate_local_ring_quarantine_resolution(&resolution).is_err());
        fs::copy(
            &resolution.source_path,
            resolution.final_directory.join("source.jsonl"),
        )
        .unwrap();
        fs::set_permissions(
            resolution.final_directory.join("source.jsonl"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(validate_local_ring_quarantine_resolution(&resolution).is_err());
        fs::set_permissions(
            resolution.final_directory.join("source.jsonl"),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
        fs::remove_file(resolution.final_directory.join("resolution.uploaded.json")).unwrap();
        assert!(validate_local_ring_quarantine_resolution(&resolution).is_err());
        fs::write(&resolution.source_path, b"tampered\n").unwrap();
        assert!(prepare_ring_quarantine_resolution(
            &root,
            &receipt_id,
            1786924800,
            "approved-change-123",
        )
        .is_err());
        fs::remove_dir_all(root).unwrap();

        let (root, receipt_id) = quarantine_fixture();
        let receipt_root = root.join("quarantine/recorder-sequence-proof-v1");
        let actual = root.join("quarantine/actual-receipts");
        fs::rename(&receipt_root, &actual).unwrap();
        symlink(&actual, &receipt_root).unwrap();
        assert!(prepare_ring_quarantine_resolution(
            &root,
            &receipt_id,
            1786924800,
            "approved-change-123",
        )
        .is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ring_quarantine_resolution_cli_requires_identity_boundary_and_approval() {
        let cli = try_parse_cli([
            "polyedge-rs",
            "ring-quarantine-resolve",
            "--root",
            "/srv/polyedge-ring",
            "--receipt-id",
            &"a".repeat(64),
            "--formal-boundary-epoch",
            "1786924800",
            "--approval-reference",
            "approved-change-123",
            "--account",
            "storage",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::RingQuarantineResolve { .. }));
        assert!(try_parse_cli([
            "polyedge-rs",
            "ring-quarantine-resolve",
            "--receipt-id",
            &"a".repeat(64),
            "--formal-boundary-epoch",
            "1786924800",
            "--account",
            "storage",
        ])
        .is_err());
        assert!(try_parse_cli([
            "polyedge-rs",
            "ring-quarantine-resolve",
            "--receipt-id",
            &"a".repeat(64),
            "--formal-boundary-epoch",
            "1786924800",
            "--approval-reference",
            "approved-change-123",
            "--segment-seconds",
            "300",
            "--account",
            "storage",
        ])
        .is_err());
    }

    #[test]
    fn ring_quarantine_source_size_accepts_boundary_and_rejects_oversize() {
        assert!(validate_ring_quarantine_source_size(
            MAX_RING_QUARANTINE_SOURCE_BYTES,
            MAX_RING_QUARANTINE_SOURCE_BYTES,
        )
        .is_ok());
        assert!(validate_ring_quarantine_source_size(
            MAX_RING_QUARANTINE_SOURCE_BYTES + 1,
            MAX_RING_QUARANTINE_SOURCE_BYTES + 1,
        )
        .is_err());
        assert!(validate_ring_quarantine_source_size(1, 2).is_err());
    }

    #[test]
    fn ring_manifest_v3_requires_contiguous_lowercase_uuid_v4_sequences() {
        let manifest = json!({
            "recorder_instance_id": "7c66d77b-a911-4f9b-95f2-98ca9395255e",
            "recorder_first_sequence": 41,
            "recorder_last_sequence": 42,
            "recorder_event_count": 2,
            "lines": 2,
        });
        assert!(validate_ring_manifest_v3_sequence(&manifest).is_ok());
        let mut invalid = manifest.clone();
        invalid["recorder_instance_id"] = json!("7C66D77B-A911-4F9B-95F2-98CA9395255E");
        assert!(validate_ring_manifest_v3_sequence(&invalid).is_err());
        invalid = manifest.clone();
        invalid["recorder_last_sequence"] = json!(43);
        assert!(validate_ring_manifest_v3_sequence(&invalid).is_err());
    }

    #[test]
    fn ring_manifest_v4_exactly_covers_restart_runs() {
        let first = "7c66d77b-a911-4f9b-95f2-98ca9395255e";
        let second = "8c66d77b-a911-4f9b-95f2-98ca9395255e";
        let manifest = json!({
            "lines": 4,
            "recorder_runs": [
                {"recorder_instance_id": first, "recorder_first_sequence": 41, "recorder_last_sequence": 42, "recorder_event_count": 2},
                {"recorder_instance_id": second, "recorder_first_sequence": 1, "recorder_last_sequence": 2, "recorder_event_count": 2}
            ]
        });
        assert!(validate_ring_manifest_v4_runs(&manifest).is_ok());
        let mut invalid = manifest.clone();
        invalid["recorder_runs"][1]["recorder_first_sequence"] = json!(2);
        assert!(validate_ring_manifest_v4_runs(&invalid).is_err());
        invalid = manifest.clone();
        invalid["recorder_runs"][1]["recorder_instance_id"] = json!(first);
        assert!(validate_ring_manifest_v4_runs(&invalid).is_err());
    }

    #[test]
    fn ring_manifest_v4_source_envelopes_exactly_match_runs() {
        let path = std::env::temp_dir().join(format!(
            "polyedge-ring-v4-source-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let first = "7c66d77b-a911-4f9b-95f2-98ca9395255e";
        let second = "8c66d77b-a911-4f9b-95f2-98ca9395255e";
        let manifest = json!({
            "lines": 3,
            "recorder_runs": [
                {"recorder_instance_id": first, "recorder_first_sequence": 41, "recorder_last_sequence": 42, "recorder_event_count": 2},
                {"recorder_instance_id": second, "recorder_first_sequence": 1, "recorder_last_sequence": 1, "recorder_event_count": 1}
            ]
        });
        fs::write(&path, format!(
            "{{\"recorder_instance_id\":\"{first}\",\"recorder_sequence\":41}}\n{{\"recorder_instance_id\":\"{first}\",\"recorder_sequence\":42}}\n{{\"recorder_instance_id\":\"{second}\",\"recorder_sequence\":1}}\n"
        )).unwrap();
        assert!(validate_ring_source_v4(&path, &manifest).is_ok());
        fs::write(&path, format!(
            "{{\"recorder_instance_id\":\"{first}\",\"recorder_sequence\":41}}\n{{\"recorder_instance_id\":\"{second}\",\"recorder_sequence\":1}}\n{{\"recorder_instance_id\":\"{first}\",\"recorder_sequence\":42}}\n"
        )).unwrap();
        assert!(validate_ring_source_v4(&path, &manifest).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ring_manifest_v3_source_envelopes_must_match_sequence_proof() {
        let path = std::env::temp_dir().join(format!(
            "polyedge-ring-source-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let instance_id = "7c66d77b-a911-4f9b-95f2-98ca9395255e";
        let manifest = json!({
            "recorder_instance_id": instance_id,
            "recorder_first_sequence": 1,
            "recorder_last_sequence": 2,
            "recorder_event_count": 2,
            "lines": 2,
        });
        fs::write(
            &path,
            format!(
                "{{\"recorder_instance_id\":\"{instance_id}\",\"recorder_sequence\":1}}\n{{\"recorder_instance_id\":\"{instance_id}\",\"recorder_sequence\":2}}\n"
            ),
        )
        .unwrap();
        assert!(validate_ring_source_v3(&path, &manifest).is_ok());

        fs::write(
            &path,
            format!(
                "{{\"recorder_instance_id\":\"{instance_id}\",\"recorder_sequence\":1}}\n{{\"recorder_instance_id\":\"{instance_id}\",\"recorder_sequence\":3}}\n"
            ),
        )
        .unwrap();
        assert!(validate_ring_source_v3(&path, &manifest).is_err());

        fs::write(
            &path,
            "{\"recorder_instance_id\":\"7C66D77B-A911-4F9B-95F2-98CA9395255E\",\"recorder_sequence\":1}\n{\"recorder_instance_id\":\"7c66d77b-a911-4f9b-95f2-98ca9395255e\",\"recorder_sequence\":2}\n",
        )
        .unwrap();
        assert!(validate_ring_source_v3(&path, &manifest).is_err());

        fs::write(
            &path,
            "{\"recorder_instance_id\":\"7c66d77b-a911-4f9b-95f2-98ca9395255e\",\"recorder_sequence\":1}\n{\"recorder_instance_id\":\"7c66d77b-a911-4f9b-85f2-98ca9395255e\",\"recorder_sequence\":2}\n",
        )
        .unwrap();
        assert!(validate_ring_source_v3(&path, &manifest).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ring_manifest_paths_and_hashes_fail_closed() {
        let manifest = json!({
            "segment_path": "segments/2026/08/05/22/1785969000.jsonl",
            "blob_name": "events-oci-dual/2026/08/05/22/1785969000.jsonl",
            "sha256": format!("sha256:{}", "a".repeat(64)),
        });
        assert_eq!(
            ring_relative_path(&manifest, "segment_path").unwrap(),
            PathBuf::from("segments/2026/08/05/22/1785969000.jsonl")
        );
        assert!(ring_blob_name(&manifest, "events-oci-dual").is_ok());
        assert!(ring_sha256(&manifest, "sha256").is_ok());

        let mut invalid = manifest.clone();
        invalid["segment_path"] = json!("../events.jsonl");
        assert!(ring_relative_path(&invalid, "segment_path").is_err());
        invalid = manifest.clone();
        invalid["blob_name"] = json!("/events.jsonl");
        assert!(ring_blob_name(&invalid, "events-oci-dual").is_err());
        invalid = manifest.clone();
        invalid["blob_name"] = json!("other-prefix/2026/08/05/22/1785969000.jsonl");
        assert!(ring_blob_name(&invalid, "events-oci-dual").is_err());
        invalid = manifest.clone();
        invalid["sha256"] = json!(format!("sha256:{}", "A".repeat(64)));
        assert!(ring_sha256(&invalid, "sha256").is_err());

        assert!(validate_ring_identity(
            Path::new("segments/2026/08/05/22/1785969000.jsonl"),
            Path::new("segments/2026/08/05/22/1785969000.jsonl"),
            "events-oci-dual/2026/08/05/22/1785969000.jsonl",
            "events-oci-dual",
            1785969000,
            1785969600,
            1785969700,
        )
        .is_ok());
        assert!(validate_ring_identity(
            Path::new("segments/2026/08/05/22/1785969000.jsonl"),
            Path::new("segments/2026/08/05/22/1785969000.jsonl"),
            "other-prefix/2026/08/05/22/1785969000.jsonl",
            "events-oci-dual",
            1785969000,
            1785969600,
            1785969700,
        )
        .is_err());

        let compressed = json!({
            "segment_path": "segments/2026/08/05/22/1785969000.jsonl",
            "archive_path": "archive/2026/08/05/22/1785969000.jsonl.gz",
            "blob_name": "events-oci-hot7-v1/2026/08/05/22/1785969000.jsonl.gz",
        });
        assert!(ring_relative_path(&compressed, "archive_path").is_ok());
        assert!(ring_blob_name(&compressed, "events-oci-hot7-v1").is_ok());
        assert!(validate_ring_identity(
            Path::new("segments/2026/08/05/22/1785969000.jsonl"),
            Path::new("archive/2026/08/05/22/1785969000.jsonl.gz"),
            "events-oci-hot7-v1/2026/08/05/22/1785969000.jsonl.gz",
            "events-oci-hot7-v1",
            1785969000,
            1785969600,
            1785969700,
        )
        .is_ok());

        assert_eq!(
            accepted_ring_blob_prefix(&manifest, "events-oci-hot7-v1", true),
            "events-oci-dual"
        );
        assert_eq!(
            accepted_ring_blob_prefix(&manifest, "events-oci-hot7-v1", false),
            "events-oci-hot7-v1"
        );
    }

    #[test]
    fn ring_upload_receipt_accepts_exact_bound_identity() {
        let manifest_sha = format!("sha256:{}", "a".repeat(64));
        let blob_name = "events-oci-hot7-v1/2026/08/05/22/1785969000.jsonl.gz";
        let manifest_blob = format!("{blob_name}.manifest.json");
        let receipt = serde_json::to_vec(&json!({
            "schema_version": 1,
            "manifest_sha256": manifest_sha,
            "blob_name": blob_name,
            "manifest_blob_name": manifest_blob,
            "verified_ts": "2026-08-12T00:00:00Z",
        }))
        .unwrap();

        assert!(validate_ring_upload_receipt(
            &receipt,
            &format!("sha256:{}", "a".repeat(64)),
            blob_name,
            &format!("{blob_name}.manifest.json"),
        )
        .is_ok());
    }

    #[test]
    fn ring_upload_receipt_rejects_malformed_or_mismatched_fields() {
        let manifest_sha = format!("sha256:{}", "a".repeat(64));
        let blob_name = "events-oci-hot7-v1/2026/08/05/22/1785969000.jsonl.gz";
        let manifest_blob = format!("{blob_name}.manifest.json");
        let valid = json!({
            "schema_version": 1,
            "manifest_sha256": manifest_sha,
            "blob_name": blob_name,
            "manifest_blob_name": manifest_blob,
            "verified_ts": "2026-08-12T00:00:00Z",
        });
        assert!(
            validate_ring_upload_receipt(b"{", &manifest_sha, blob_name, &manifest_blob).is_err()
        );

        let mut invalid_receipts = vec![json!([])];
        for (field, value) in [
            ("schema_version", json!(2)),
            ("schema_version", json!("1")),
            ("manifest_sha256", json!("sha256:wrong")),
            ("manifest_sha256", json!(1)),
            ("blob_name", json!("events-oci-hot7-v1/wrong.jsonl.gz")),
            ("blob_name", json!(1)),
            ("manifest_blob_name", json!("wrong.manifest.json")),
            ("manifest_blob_name", json!(1)),
            ("verified_ts", json!("")),
            ("verified_ts", json!("not-a-timestamp")),
            ("verified_ts", json!(1)),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            invalid_receipts.push(invalid);
        }
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove("verified_ts");
        invalid_receipts.push(missing);
        let mut extra = valid.clone();
        extra["unexpected"] = json!(true);
        invalid_receipts.push(extra);

        for receipt in invalid_receipts {
            assert!(
                validate_ring_upload_receipt(
                    &serde_json::to_vec(&receipt).unwrap(),
                    &manifest_sha,
                    blob_name,
                    &manifest_blob,
                )
                .is_err(),
                "accepted invalid receipt: {receipt}"
            );
        }
    }

    #[test]
    fn loss_diagnostics_cli_requires_explicit_snapshot_and_output_directory() {
        let cli = try_parse_cli([
            "polyedge-rs",
            "research",
            "loss-diagnostics",
            "--input",
            "immutable-v3-snapshot",
            "--out",
            "loss-diagnostics-out",
        ])
        .expect("parse loss diagnostics command");
        let Command::Research {
            command: ResearchCommand::LossDiagnostics { input, out, .. },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(input, PathBuf::from("immutable-v3-snapshot"));
        assert_eq!(out, PathBuf::from("loss-diagnostics-out"));
        assert!(try_parse_cli(["polyedge-rs", "research", "loss-diagnostics"]).is_err());
    }

    #[test]
    fn loss_regime_oos_cli_requires_explicit_isolated_evidence_inputs() {
        let cli = try_parse_cli([
            "polyedge-rs",
            "research",
            "loss-regime-oos",
            "--facts",
            "loss-diagnostics",
            "--queue-evidence",
            "baseline.json",
            "--config",
            "research/configs/experiments/loss-regime-oos-v2-2026-07-23.yaml",
            "--source-campaign-id",
            "campaign-2026-07-23",
            "--out",
            "reports/research/experiments/experiment-loss-regime-oos-v2-2026-07-23/report.json",
            "--markdown",
            "reports/research/experiments/experiment-loss-regime-oos-v2-2026-07-23/report.md",
        ])
        .expect("parse loss regime OOS command");
        let Command::Research {
            command:
                ResearchCommand::LossRegimeOos {
                    facts,
                    queue_evidence,
                    config,
                    source_campaign_id,
                    out,
                    markdown,
                },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(facts, PathBuf::from("loss-diagnostics"));
        assert_eq!(queue_evidence, PathBuf::from("baseline.json"));
        assert_eq!(source_campaign_id, "campaign-2026-07-23");
        assert!(config.ends_with("loss-regime-oos-v2-2026-07-23.yaml"));
        assert!(out.starts_with("reports/research/experiments"));
        assert!(markdown.starts_with("reports/research/experiments"));
        assert!(try_parse_cli(["polyedge-rs", "research", "loss-regime-oos"]).is_err());
    }

    #[test]
    fn publish_daily_bundle_cli_binds_explicit_shadow_runtime_role() {
        let cli = try_parse_cli([
            "polyedge-rs",
            "research",
            "publish-daily-bundle",
            "--date",
            "2026-07-14",
            "--run-id",
            "shadow-2026-07-14",
            "--input-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--expected-runtime-role",
            "profitability_shadow",
            "--source-dir",
            "staging",
            "--data-audit",
            "staging/data_audit.json",
        ])
        .expect("parse shadow daily command");
        let Command::Research {
            command:
                ResearchCommand::PublishDailyBundle {
                    expected_runtime_role,
                    ..
                },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(expected_runtime_role, "profitability_shadow");
    }

    #[test]
    fn profitability_log_distinguishes_shadow_evidence_from_execution_authorization() {
        assert_eq!(
            profitability_authorization_flags(true, false),
            "shadow_gate_passed=true execution_promotion_allowed=false"
        );
    }

    #[test]
    fn azure_lease_cli_preserves_the_exact_child_command() {
        let cli = try_parse_cli([
            "polyedge-rs",
            "research",
            "with-azure-lease",
            "--account",
            "storage",
            "--container",
            "research",
            "--blob",
            "campaign/control/replay.lock",
            "--",
            "/bin/sh",
            "/app/research/run_shadow_daily.sh",
            "--test-child-argument",
        ])
        .expect("parse Azure lease wrapper");
        let Command::Research {
            command:
                ResearchCommand::WithAzureLease {
                    lease_seconds,
                    renew_seconds,
                    command,
                    ..
                },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(lease_seconds, 60);
        assert_eq!(renew_seconds, 20);
        assert_eq!(
            command,
            [
                "/bin/sh",
                "/app/research/run_shadow_daily.sh",
                "--test-child-argument"
            ]
        );
    }

    #[test]
    fn normalized_snapshot_cli_requires_explicit_date_and_path() {
        let publish = try_parse_cli([
            "polyedge-rs",
            "research",
            "publish-normalized-snapshot",
            "--input",
            "normalized",
            "--date",
            "2026-07-30",
            "--account",
            "storage",
            "--container",
            "events",
        ])
        .expect("parse normalized snapshot publisher");
        let Command::Research {
            command:
                ResearchCommand::PublishNormalizedSnapshot {
                    input,
                    date,
                    prefix,
                    ..
                },
        } = publish.command
        else {
            panic!("unexpected publisher command");
        };
        assert_eq!(input, PathBuf::from("normalized"));
        assert_eq!(date, "2026-07-30");
        assert_eq!(prefix, "data/research/normalized/v1");

        let restore = try_parse_cli([
            "polyedge-rs",
            "research",
            "restore-normalized-snapshot",
            "--out",
            "restored",
            "--date",
            "2026-07-30",
            "--account",
            "storage",
            "--container",
            "events",
        ])
        .expect("parse normalized snapshot restore");
        let Command::Research {
            command: ResearchCommand::RestoreNormalizedSnapshot { out, date, .. },
        } = restore.command
        else {
            panic!("unexpected restore command");
        };
        assert_eq!(out, PathBuf::from("restored"));
        assert_eq!(date, "2026-07-30");

        assert!(try_parse_cli([
            "polyedge-rs",
            "research",
            "publish-normalized-snapshot",
            "--account",
            "storage",
        ])
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn lease_tree_termination_kills_the_entire_process_group() {
        use std::os::unix::process::CommandExt;
        use std::process::Command;
        use std::thread;
        use std::time::Duration;

        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 300 & wait"]).process_group(0);
        let mut child = command.spawn().expect("spawn lease child process group");
        let process_group = i32::try_from(child.id()).expect("child PID fits i32");
        thread::sleep(Duration::from_millis(50));
        terminate_lease_child_tree(&mut child);

        for _ in 0..50 {
            // SAFETY: signal 0 only checks whether the dedicated child process
            // group still exists; it sends no signal.
            let result = unsafe { libc::kill(-process_group, 0) };
            if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("lease child process group survived watchdog termination");
    }
}
