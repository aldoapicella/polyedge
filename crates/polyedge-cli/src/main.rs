use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use polyedge_api::{app, benchmark_snapshot};
use polyedge_config::RuntimeSettings;
use polyedge_reporting::research::{
    advance_funded_ladder, advance_funded_manifest, expire_funded_manifest,
    initialize_funded_manifest_after_canary, load_default_exclusions, publish_daily_directory,
    run_audit, run_azure_freshness, run_backfill, run_baseline,
    run_build_cumulative_wallet_snapshot, run_build_markets, run_build_replay_index,
    run_calibration, run_chart_backfill, run_evaluate_profitability, run_execution_quality,
    run_final_report, run_ml_calibrate, run_normalize, run_queue_audit, run_regimes, run_replay,
    run_sample_size, run_sweep, run_validate_prospective, stop_funded_manifest_from_stage_block,
    AdvanceFundedLadderOptions, AdvanceFundedManifestOptions, AuditOptions, AzureFreshnessOptions,
    BackfillOptions, BaselineOptions, BuildMarketsOptions, CalibrationOptions,
    ChartBackfillOptions, CumulativeWalletSnapshotOptions, ExcludedTimeWindow,
    ExecutionQualityOptions, ExpireFundedManifestOptions, FillModel, FinalReportOptions,
    InitializeFundedManifestOptions, MlCalibrateOptions, NormalizeOptions,
    ProfitabilityEvaluationOptions, ProspectiveValidationOptions, QueueAuditOptions,
    RegimesOptions, ReplayIndexOptions, ReplayOptions, SampleSizeOptions,
    StopFundedManifestFromStageBlockOptions, SweepOptions, DEFAULT_EXCLUSION_FILE,
    DEFAULT_FROZEN_CANDIDATES_FILE, DEFAULT_PROSPECTIVE_SINCE,
};
use polyedge_reporting::{
    build_pnl_report, run_backtest, BacktestConfig, ReplayBacktester, REPLAY_BUFFER_BYTES,
};
use polyedge_storage::{AzureBlobClient, AzureBlobItem};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{BufReader, Cursor};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;

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
    Normalize {
        #[arg(long, default_value = "data/events.jsonl")]
        input: PathBuf,
        #[arg(long, default_value = "data/research/normalized")]
        out: PathBuf,
        #[arg(long, default_value = "jsonl-indexed")]
        format: String,
        #[arg(long, default_value_t = false, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
        overwrite: bool,
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
        normalized_manifest: PathBuf,
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
                "shadow_only": false
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
        ResearchCommand::Normalize {
            input,
            out,
            format,
            overwrite,
        } => run_normalize(NormalizeOptions {
            input,
            out,
            format,
            overwrite,
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
            source_dir,
            output_root,
            data_audit,
        } => serde_json::to_value(publish_daily_directory(
            parse_date_arg(&date)?,
            run_id,
            input_sha256,
            &source_dir,
            &output_root,
            &data_audit,
        )?)?,
        ResearchCommand::BuildCumulativeWallet {
            regimes,
            normalized_manifest,
            snapshot_date,
            out,
        } => run_build_cumulative_wallet_snapshot(CumulativeWalletSnapshotOptions {
            regimes,
            normalized_manifest,
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
        } => serde_json::to_value(run_evaluate_profitability(
            ProfitabilityEvaluationOptions {
                daily_root,
                prospective,
                gate_config,
                execution_model,
                out,
                generated_at: None,
            },
        )?)?,
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
        "shadow_only": false,
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
            "shadow_only": false,
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
