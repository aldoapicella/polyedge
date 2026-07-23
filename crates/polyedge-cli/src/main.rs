use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use polyedge_api::{app, benchmark_snapshot};
use polyedge_config::{embedded_git_sha, RuntimeRole, RuntimeSettings};
use polyedge_reporting::research::{
    advance_funded_ladder, advance_funded_manifest, expire_funded_manifest,
    initialize_funded_manifest_after_canary, load_default_exclusions, publish_daily_directory,
    run_audit, run_azure_freshness, run_backfill, run_baseline, run_begin_shadow_correction,
    run_build_cumulative_wallet_snapshot, run_build_markets, run_build_replay_index,
    run_calibration, run_chart_backfill, run_complete_shadow_correction,
    run_evaluate_profitability, run_execution_quality, run_final_report, run_loss_diagnostics,
    run_loss_regime_oos, run_materialize_projected_campaign, run_ml_calibrate, run_normalize,
    run_publish_projected_day, run_queue_audit, run_regimes, run_replay, run_sample_size,
    run_sweep, run_validate_prospective, stop_funded_manifest_from_stage_block,
    AdvanceFundedLadderOptions, AdvanceFundedManifestOptions, AuditOptions, AzureFreshnessOptions,
    BackfillOptions, BaselineOptions, BeginShadowCorrectionOptions, BuildMarketsOptions,
    CalibrationOptions, ChartBackfillOptions, CompleteShadowCorrectionOptions,
    CumulativeWalletSnapshotOptions, ExcludedTimeWindow, ExecutionQualityOptions,
    ExpireFundedManifestOptions, FillModel, FinalReportOptions, InitializeFundedManifestOptions,
    LossDiagnosticsOptions, LossRegimeOosOptions, MaterializeProjectedCampaignOptions,
    MlCalibrateOptions, NormalizeOptions, ProfitabilityEvaluationOptions,
    ProspectiveValidationOptions, PublishProjectedDayOptions, QueueAuditOptions, RegimesOptions,
    ReplayIndexOptions, ReplayOptions, SampleSizeOptions, StopFundedManifestFromStageBlockOptions,
    SweepOptions, WarningSeverity, DEFAULT_EXCLUSION_FILE, DEFAULT_FROZEN_CANDIDATES_FILE,
    DEFAULT_PROSPECTIVE_SINCE,
};
use polyedge_reporting::{
    build_pnl_report, run_backtest, BacktestConfig, ReplayBacktester, REPLAY_BUFFER_BYTES,
};
use polyedge_storage::{AzureBlobClient, AzureBlobItem, BlobLeaseAcquireResult};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{BufReader, Cursor};
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

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
        #[arg(long, default_value = "data_quality/freshness/latest.json")]
        out: PathBuf,
        #[arg(long = "sas-env")]
        sas_env: Option<String>,
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
        } => run_audit(AuditOptions {
            input,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
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
        ResearchCommand::LossDiagnostics { input, out } => {
            run_loss_diagnostics(LossDiagnosticsOptions { input, out })?
        }
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
        } => run_build_markets(BuildMarketsOptions {
            input,
            out,
            markdown,
            exclude_windows: load_exclusions(exclude_file, exclude_window)?,
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
    axum::serve(listener, app(settings))
        .await
        .context("serving Rust API")
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
        profitability_authorization_flags, terminate_lease_child_tree, Cli, Command, PathBuf,
        ResearchCommand,
    };
    use clap::Parser;

    #[test]
    fn loss_diagnostics_cli_requires_explicit_snapshot_and_output_directory() {
        let cli = Cli::try_parse_from([
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
            command: ResearchCommand::LossDiagnostics { input, out },
        } = cli.command
        else {
            panic!("unexpected command");
        };
        assert_eq!(input, PathBuf::from("immutable-v3-snapshot"));
        assert_eq!(out, PathBuf::from("loss-diagnostics-out"));
        assert!(Cli::try_parse_from(["polyedge-rs", "research", "loss-diagnostics"]).is_err());
    }

    #[test]
    fn loss_regime_oos_cli_requires_explicit_isolated_evidence_inputs() {
        let cli = Cli::try_parse_from([
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
        assert!(Cli::try_parse_from(["polyedge-rs", "research", "loss-regime-oos"]).is_err());
    }

    #[test]
    fn publish_daily_bundle_cli_binds_explicit_shadow_runtime_role() {
        let cli = Cli::try_parse_from([
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
        let cli = Cli::try_parse_from([
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
