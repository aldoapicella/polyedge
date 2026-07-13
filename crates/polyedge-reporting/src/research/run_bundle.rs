use super::ResearchError;
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use polyedge_config::RuntimeRole;
use polyedge_engine::FrozenStrategyMode;
use polyedge_storage::AzureBlobClient;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const WARNING_REGISTRY_VERSION: &str = "research-data-quality-v1";
pub const DEFAULT_PROFITABILITY_LATEST: &str = "reports/research/profitability/latest.json";
const DAILY_PROVENANCE_CUTOFF: &str = "2026-07-12";
const MANIFEST_FILE: &str = "run_manifest.json";
const LATEST_FILE: &str = "latest.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunStatus {
    Staging,
    Complete,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningSeverity {
    Informational,
    Blocking,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarningClassification {
    pub message: String,
    pub rule_id: String,
    pub severity: WarningSeverity,
    pub known: bool,
}

/// Classifies research warnings against a versioned, fail-closed registry.
/// Unknown warnings are deliberately blocking until a reviewed rule is added.
pub fn classify_warning(message: impl Into<String>) -> WarningClassification {
    let message = message.into();
    let (rule_id, severity, known) = if message.ends_with("out-of-order timestamps")
        || message.starts_with("out-of-order timestamp in ")
    {
        ("event_time_reordered", WarningSeverity::Informational, true)
    } else if message.starts_with("azure input listed ") {
        (
            "azure_input_inventory",
            WarningSeverity::Informational,
            true,
        )
    } else if message.starts_with("0 events skipped by ")
        && message.ends_with("excluded event-time window(s)")
    {
        (
            "exclusion_window_noop",
            WarningSeverity::Informational,
            true,
        )
    } else if message.starts_with("daily capture window incomplete for ") {
        (
            "daily_capture_window_incomplete",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily capture gap exceeds 300000ms for ") {
        (
            "daily_capture_gap_exceeds_5m",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily capture gap evidence missing for ") {
        (
            "daily_capture_gap_evidence_missing",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily runtime provenance missing for ") {
        (
            "daily_runtime_provenance_missing",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily runtime provenance invalid for ") {
        (
            "daily_runtime_provenance_invalid",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily runtime provenance window incomplete for ") {
        (
            "daily_runtime_provenance_window_incomplete",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily runtime provenance gap exceeds 300000ms for ") {
        (
            "daily_runtime_provenance_gap_exceeds_5m",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily runtime provenance identity changed for ") {
        (
            "daily_runtime_provenance_identity_changed",
            WarningSeverity::Blocking,
            true,
        )
    } else if message.starts_with("daily runtime provenance reporter mismatch for ") {
        (
            "daily_runtime_provenance_reporter_mismatch",
            WarningSeverity::Blocking,
            true,
        )
    } else {
        ("unknown_warning", WarningSeverity::Blocking, false)
    };
    WarningClassification {
        message,
        rule_id: rule_id.to_owned(),
        severity,
        known,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataQualitySummary {
    pub registry_version: String,
    pub total_events: u64,
    pub decision_grade_coverage: Decimal,
    pub fatal_issues: Vec<String>,
    pub warnings: Vec<WarningClassification>,
    #[serde(default)]
    pub out_of_order_events: u64,
    #[serde(default)]
    pub event_time_ordering_restored: bool,
}

impl DataQualitySummary {
    pub fn new(
        total_events: u64,
        decision_grade_coverage: Decimal,
        fatal_issues: Vec<String>,
        warnings: impl IntoIterator<Item = String>,
    ) -> Self {
        let out_of_order_events = warnings.into_iter().collect::<Vec<_>>();
        let measured_out_of_order = out_of_order_events
            .iter()
            .filter(|warning| warning.ends_with("out-of-order timestamps"))
            .filter_map(|warning| warning.split_whitespace().next()?.parse::<u64>().ok())
            .sum();
        Self {
            registry_version: WARNING_REGISTRY_VERSION.to_owned(),
            total_events,
            decision_grade_coverage,
            fatal_issues,
            warnings: out_of_order_events
                .into_iter()
                .map(classify_warning)
                .collect(),
            out_of_order_events: measured_out_of_order,
            event_time_ordering_restored: measured_out_of_order == 0,
        }
    }

    pub fn promotion_allowed(&self) -> bool {
        self.total_events > 0
            && self.decision_grade_coverage >= Decimal::new(95, 2)
            && self.fatal_issues.is_empty()
            && self.event_time_ordering_restored
            && Decimal::from(self.out_of_order_events) / Decimal::from(self.total_events)
                <= Decimal::new(1, 4)
            && self
                .warnings
                .iter()
                .all(|warning| warning.severity == WarningSeverity::Informational)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunArtifact {
    pub name: String,
    pub relative_path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DailyRunManifest {
    pub schema_version: u32,
    #[serde(default)]
    pub git_sha: Option<String>,
    #[serde(default)]
    pub runtime_role: Option<RuntimeRole>,
    pub date: NaiveDate,
    pub run_id: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub input_sha256: String,
    pub status: RunStatus,
    pub artifacts: BTreeMap<String, RunArtifact>,
    pub data_quality: DataQualitySummary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LatestRunPointer {
    pub schema_version: u32,
    pub date: NaiveDate,
    pub run_id: String,
    pub manifest_path: String,
    pub manifest_sha256: String,
    pub promoted_at: DateTime<Utc>,
}

/// A manifest-first staging run which becomes immutable once `complete` succeeds.
/// Artifacts live under `<root>/<date>/runs/<run_id>/`; latest pointers are only
/// changed after the complete manifest and all artifact hashes are verified.
pub struct AtomicDailyRun {
    root: PathBuf,
    run_dir: PathBuf,
    manifest: DailyRunManifest,
    finalized: bool,
}

impl AtomicDailyRun {
    pub fn begin(
        root: impl Into<PathBuf>,
        date: NaiveDate,
        run_id: impl Into<String>,
        input_sha256: impl Into<String>,
        data_quality: DataQualitySummary,
    ) -> Result<Self, ResearchError> {
        Self::begin_with_runtime_role(
            root,
            date,
            run_id,
            input_sha256,
            data_quality,
            RuntimeRole::Primary,
        )
    }

    pub fn begin_with_runtime_role(
        root: impl Into<PathBuf>,
        date: NaiveDate,
        run_id: impl Into<String>,
        input_sha256: impl Into<String>,
        data_quality: DataQualitySummary,
        runtime_role: RuntimeRole,
    ) -> Result<Self, ResearchError> {
        let root = root.into();
        let run_id = validate_component("run_id", run_id.into())?;
        let input_sha256 = input_sha256.into();
        validate_sha256("input_sha256", &input_sha256)?;
        let git_sha = super::git_sha().ok_or_else(|| {
            ResearchError::InvalidInput(
                "daily run requires an exact 40-character Git SHA".to_owned(),
            )
        })?;
        let run_dir = root
            .join(date.format("%Y-%m-%d").to_string())
            .join("runs")
            .join(&run_id);
        fs::create_dir_all(run_dir.parent().expect("run directory has parent"))?;
        fs::create_dir(&run_dir).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                ResearchError::InvalidInput(format!(
                    "daily run {run_id} already exists; completed and staged run ids are immutable"
                ))
            } else {
                ResearchError::Io(error)
            }
        })?;
        let manifest = DailyRunManifest {
            schema_version: 2,
            git_sha: Some(git_sha),
            runtime_role: Some(runtime_role),
            date,
            run_id,
            created_at: Utc::now(),
            completed_at: None,
            input_sha256,
            status: RunStatus::Staging,
            artifacts: BTreeMap::new(),
            data_quality,
        };
        write_new_json(&run_dir.join(MANIFEST_FILE), &manifest)?;
        Ok(Self {
            root,
            run_dir,
            manifest,
            finalized: false,
        })
    }

    pub fn write_artifact(
        &mut self,
        name: impl Into<String>,
        relative_path: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<RunArtifact, ResearchError> {
        if self.finalized {
            return Err(ResearchError::InvalidInput(
                "cannot add an artifact to a completed daily run".to_owned(),
            ));
        }
        let name = validate_component("artifact name", name.into())?;
        if self.manifest.artifacts.contains_key(&name) {
            return Err(ResearchError::InvalidInput(format!(
                "artifact {name} already exists in this run"
            )));
        }
        let relative_path = validate_relative_path(relative_path.as_ref())?;
        let destination = self.run_dir.join(&relative_path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        super::maybe_publish_research_artifact(&destination)?;
        let artifact = RunArtifact {
            name: name.clone(),
            relative_path: path_string(&relative_path),
            sha256: sha256_bytes(bytes),
            bytes: bytes.len() as u64,
        };
        self.manifest.artifacts.insert(name, artifact.clone());
        replace_json(&self.run_dir.join(MANIFEST_FILE), &self.manifest)?;
        Ok(artifact)
    }

    pub fn complete(mut self) -> Result<LatestRunPointer, ResearchError> {
        if self.manifest.artifacts.is_empty() {
            return Err(ResearchError::InvalidInput(
                "cannot complete a daily run without artifacts".to_owned(),
            ));
        }
        verify_artifacts(&self.run_dir, &self.manifest)?;
        self.manifest.status = RunStatus::Complete;
        self.manifest.completed_at = Some(Utc::now());
        replace_json(&self.run_dir.join(MANIFEST_FILE), &self.manifest)?;
        let manifest_bytes = fs::read(self.run_dir.join(MANIFEST_FILE))?;
        let date_dir = self
            .root
            .join(self.manifest.date.format("%Y-%m-%d").to_string());
        let pointer = LatestRunPointer {
            schema_version: 1,
            date: self.manifest.date,
            run_id: self.manifest.run_id.clone(),
            manifest_path: path_string(
                &Path::new("runs")
                    .join(&self.manifest.run_id)
                    .join(MANIFEST_FILE),
            ),
            manifest_sha256: sha256_bytes(&manifest_bytes),
            promoted_at: Utc::now(),
        };
        // Per-date first, then global. A crash can leave the global pointer old,
        // but neither pointer can ever reference a partial bundle.
        replace_json(&date_dir.join(LATEST_FILE), &pointer)?;
        let global_pointer = LatestRunPointer {
            manifest_path: path_string(
                &Path::new(&self.manifest.date.format("%Y-%m-%d").to_string())
                    .join(&pointer.manifest_path),
            ),
            ..pointer.clone()
        };
        replace_json(&self.root.join(LATEST_FILE), &global_pointer)?;
        self.finalized = true;
        Ok(pointer)
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublishedDailyBundle {
    pub bundle_dir: PathBuf,
    pub manifest: DailyRunManifest,
    pub latest: LatestRunPointer,
}

/// Packages an already-generated daily directory into the immutable run
/// protocol. The required primary artifacts are verified before any run is
/// created, all JSON/Markdown artifacts are copied and hashed, and Blob upload
/// follows the same artifact publisher used by the rest of research reporting.
pub fn publish_daily_directory(
    date: NaiveDate,
    run_id: impl Into<String>,
    input_sha256: impl Into<String>,
    expected_runtime_role: RuntimeRole,
    source_dir: &Path,
    output_root: &Path,
    data_audit_path: &Path,
) -> Result<PublishedDailyBundle, ResearchError> {
    if !source_dir.is_dir() {
        return Err(ResearchError::InvalidInput(format!(
            "daily source directory does not exist: {}",
            source_dir.display()
        )));
    }
    for required in [
        &["baseline.json", "baseline_static_all_fill_models.json"][..],
        &["regimes.json", "regime_profiles.json"][..],
        &["final_report.json", "final_strategy_research_report.json"][..],
        &["execution_quality.json"][..],
    ] {
        if !required.iter().any(|name| source_dir.join(name).is_file()) {
            return Err(ResearchError::InvalidInput(format!(
                "daily source is missing required artifact (one of: {})",
                required.join(", ")
            )));
        }
    }
    if !data_audit_path.is_file() {
        return Err(ResearchError::InvalidInput(format!(
            "data audit does not exist: {}",
            data_audit_path.display()
        )));
    }
    let mut artifacts = collect_publishable_files(source_dir)?;
    artifacts.retain(|(relative, _)| relative != Path::new("data_audit.json"));
    artifacts.push((
        PathBuf::from("data_audit.json"),
        data_audit_path.to_path_buf(),
    ));
    artifacts.sort_by(|left, right| left.0.cmp(&right.0));
    let audit_value: serde_json::Value = read_json(data_audit_path)?;
    let quality = quality_from_audit_for_date(&audit_value, date, &expected_runtime_role);
    let mut run = AtomicDailyRun::begin_with_runtime_role(
        output_root,
        date,
        run_id,
        input_sha256,
        quality,
        expected_runtime_role,
    )?;
    for (relative, source) in artifacts {
        let bytes = fs::read(&source)?;
        let name = path_string(&relative)
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>();
        run.write_artifact(name, &relative, &bytes)?;
    }
    let bundle_dir = run.run_dir().to_path_buf();
    let latest = run.complete()?;
    let dependency = inspect_daily_dependency(output_root, date)?;
    let DailyDependency::Ready { manifest, .. } = dependency else {
        return Err(ResearchError::InvalidInput(
            "completed daily bundle failed final dependency verification".to_owned(),
        ));
    };
    Ok(PublishedDailyBundle {
        bundle_dir,
        manifest: *manifest,
        latest,
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DailyDependency {
    Ready {
        date: NaiveDate,
        run_id: String,
        bundle_dir: PathBuf,
        manifest: Box<DailyRunManifest>,
    },
    WaitingForDependency {
        date: NaiveDate,
        reason: String,
    },
}

pub fn inspect_daily_dependency(
    root: &Path,
    expected_date: NaiveDate,
) -> Result<DailyDependency, ResearchError> {
    let date_dir = root.join(expected_date.format("%Y-%m-%d").to_string());
    let pointer_path = date_dir.join(LATEST_FILE);
    if !pointer_path.is_file() {
        return Ok(waiting(expected_date, "latest_pointer_absent"));
    }
    let pointer: LatestRunPointer = read_json(&pointer_path)?;
    if pointer.date != expected_date {
        return Ok(waiting(expected_date, "latest_pointer_date_mismatch"));
    }
    let manifest_path = date_dir.join(&pointer.manifest_path);
    if !manifest_path.is_file() {
        return Ok(waiting(expected_date, "manifest_absent"));
    }
    let manifest_bytes = fs::read(&manifest_path)?;
    if sha256_bytes(&manifest_bytes) != pointer.manifest_sha256 {
        return Ok(waiting(expected_date, "manifest_hash_mismatch"));
    }
    let manifest: DailyRunManifest = serde_json::from_slice(&manifest_bytes)?;
    if daily_provenance_required(expected_date) && manifest.schema_version != 2 {
        return Ok(waiting(expected_date, "manifest_schema_downgrade"));
    }
    if manifest.schema_version == 2 && manifest.runtime_role.is_none() {
        return Ok(waiting(expected_date, "manifest_runtime_role_missing"));
    }
    if manifest.schema_version == 2
        && !manifest
            .git_sha
            .as_deref()
            .is_some_and(polyedge_config::is_full_git_sha)
    {
        return Ok(waiting(expected_date, "manifest_git_sha_invalid"));
    }
    if manifest.status != RunStatus::Complete {
        return Ok(waiting(expected_date, "manifest_incomplete"));
    }
    if manifest.date != expected_date || manifest.run_id != pointer.run_id {
        return Ok(waiting(expected_date, "manifest_identity_mismatch"));
    }
    let bundle_dir = manifest_path
        .parent()
        .expect("manifest path has parent")
        .to_path_buf();
    if verify_artifacts(&bundle_dir, &manifest).is_err() {
        return Ok(waiting(expected_date, "artifact_verification_failed"));
    }
    Ok(DailyDependency::Ready {
        date: expected_date,
        run_id: manifest.run_id.clone(),
        bundle_dir,
        manifest: Box::new(manifest),
    })
}

fn waiting(date: NaiveDate, reason: &str) -> DailyDependency {
    DailyDependency::WaitingForDependency {
        date,
        reason: reason.to_owned(),
    }
}

pub(super) fn quality_from_audit(audit: &serde_json::Value) -> DataQualitySummary {
    let result = audit.get("result").unwrap_or(audit);
    let total_events = result
        .get("total_events")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let explicit_coverage = result
        .get("decision_grade_coverage")
        .and_then(decimal_from_json);
    let start_capture = result
        .get("start_price_capture_rate")
        .and_then(decimal_from_json);
    let settlement = result.get("settlement_rate").and_then(decimal_from_json);
    let coverage = explicit_coverage
        .or_else(|| match (start_capture, settlement) {
            (Some(left), Some(right)) => Some(left.min(right)),
            _ => None,
        })
        .unwrap_or(Decimal::ZERO);
    let fatal_issues = result
        .get("fatal_data_quality_issues")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(value_as_message)
        .collect::<Vec<_>>();
    let mut warnings = Vec::new();
    for value in [result.get("warnings"), audit.get("warnings")]
        .into_iter()
        .flatten()
    {
        warnings.extend(
            value
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(value_as_message),
        );
    }
    warnings.sort();
    warnings.dedup();
    let mut quality = DataQualitySummary::new(total_events, coverage, fatal_issues, warnings);
    quality.out_of_order_events = result
        .pointer("/stream_ordering/out_of_order_timestamps")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            result
                .get("out_of_order_timestamps")
                .and_then(serde_json::Value::as_u64)
        })
        .unwrap_or(quality.out_of_order_events);
    // Promotion consumes an audit of the normalized stream. A raw stream with
    // any inversion is not considered restored merely because the warning is
    // known; the normalized audit must measure zero inversions (or explicitly
    // attest restoration in a future schema).
    quality.event_time_ordering_restored = result
        .get("event_time_ordering_restored")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(quality.out_of_order_events == 0);
    quality
}

fn quality_from_audit_for_date(
    audit: &serde_json::Value,
    date: NaiveDate,
    expected_runtime_role: &RuntimeRole,
) -> DataQualitySummary {
    let mut quality = quality_from_audit(audit);
    let result = audit.get("result").unwrap_or(audit);
    let day_start = DateTime::<Utc>::from_naive_utc_and_offset(
        date.and_hms_opt(0, 0, 0).expect("midnight is valid"),
        Utc,
    );
    let day_end = day_start + Duration::days(1);
    let first = result
        .get("first_event_timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_utc_timestamp);
    let last = result
        .get("last_event_timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_utc_timestamp);
    let observed_hours = result
        .get("event_count_by_hour")
        .and_then(serde_json::Value::as_object)
        .map(|hours| {
            (0..24)
                .filter(|hour| {
                    hours
                        .get(&format!("{}T{hour:02}", date.format("%Y-%m-%d")))
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|count| count > 0)
                })
                .count()
        })
        .unwrap_or_default();
    let boundary_tolerance = Duration::minutes(5);
    let full_window = first
        .is_some_and(|value| value >= day_start && value <= day_start + boundary_tolerance)
        && last.is_some_and(|value| value >= day_end - boundary_tolerance && value < day_end)
        && observed_hours == 24;
    if !full_window {
        quality.warnings.push(classify_warning(format!(
            "daily capture window incomplete for {date}: first={} last={} observed_hours={observed_hours}/24",
            first
                .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
                .unwrap_or_else(|| "missing".to_owned()),
            last.map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
                .unwrap_or_else(|| "missing".to_owned())
        )));
    }
    let gap_evidence = result
        .get("largest_time_gaps")
        .and_then(serde_json::Value::as_array)
        .map(|gaps| {
            gaps.iter()
                .filter_map(|gap| gap.get("gap_ms").and_then(serde_json::Value::as_u64))
                .collect::<Vec<_>>()
        })
        .filter(|gaps| !gaps.is_empty());
    if let Some(gaps) = gap_evidence {
        let max_gap_ms = gaps.into_iter().max().unwrap_or_default();
        if max_gap_ms > 300_000 {
            quality.warnings.push(classify_warning(format!(
                "daily capture gap exceeds 300000ms for {date}: max_gap_ms={max_gap_ms}"
            )));
        }
    } else {
        quality.warnings.push(classify_warning(format!(
            "daily capture gap evidence missing for {date}"
        )));
    }
    validate_daily_runtime_provenance(result, date, expected_runtime_role, &mut quality);
    quality
}

fn validate_daily_runtime_provenance(
    result: &serde_json::Value,
    date: NaiveDate,
    expected_runtime_role: &RuntimeRole,
    quality: &mut DataQualitySummary,
) {
    let Some(provenance) = result
        .get("runtime_provenance")
        .and_then(serde_json::Value::as_object)
    else {
        quality.warnings.push(classify_warning(format!(
            "daily runtime provenance missing for {date}"
        )));
        return;
    };
    let observations = provenance
        .get("observations")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let valid_observations = provenance
        .get("valid_observations")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let invalid_observations = provenance
        .get("invalid_observations")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let identities = provenance
        .get("identities")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let invalid_reasons = provenance
        .get("invalid_reasons")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    let identity_errors = identities
        .iter()
        .flat_map(|identity| match expected_runtime_role {
            RuntimeRole::Primary => primary_runtime_provenance_errors(identity),
            RuntimeRole::ProfitabilityShadow => shadow_runtime_provenance_errors(identity),
        })
        .collect::<Vec<_>>();
    if observations == 0
        || valid_observations == 0
        || invalid_observations > 0
        || !invalid_reasons.is_empty()
        || !identity_errors.is_empty()
    {
        quality.warnings.push(classify_warning(format!(
            "daily runtime provenance invalid for {date}: observations={observations} valid={valid_observations} invalid={invalid_observations} reasons={} identity_errors={}",
            invalid_reasons.join("|"),
            identity_errors.join("|")
        )));
    }
    if identities.len() != 1 {
        quality.warnings.push(classify_warning(format!(
            "daily runtime provenance identity changed for {date}: distinct_identities={}",
            identities.len()
        )));
    }

    let day_start = DateTime::<Utc>::from_naive_utc_and_offset(
        date.and_hms_opt(0, 0, 0).expect("midnight is valid"),
        Utc,
    );
    let day_end = day_start + Duration::days(1);
    let first = provenance
        .get("first_timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_utc_timestamp);
    let last = provenance
        .get("last_timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_utc_timestamp);
    let boundary_tolerance = Duration::minutes(5);
    let full_window = first
        .is_some_and(|value| value >= day_start && value <= day_start + boundary_tolerance)
        && last.is_some_and(|value| value >= day_end - boundary_tolerance && value < day_end);
    if !full_window {
        quality.warnings.push(classify_warning(format!(
            "daily runtime provenance window incomplete for {date}: first={} last={}",
            first
                .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
                .unwrap_or_else(|| "missing".to_owned()),
            last.map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
                .unwrap_or_else(|| "missing".to_owned())
        )));
    }
    let max_gap_ms = provenance
        .get("max_gap_ms")
        .and_then(serde_json::Value::as_u64);
    if max_gap_ms.is_none_or(|value| value > 300_000) {
        quality.warnings.push(classify_warning(format!(
            "daily runtime provenance gap exceeds 300000ms for {date}: max_gap_ms={}",
            max_gap_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "missing".to_owned())
        )));
    }
    if let (Some(reporter_git_sha), Some(identity)) = (super::git_sha(), identities.first()) {
        if identity.get("git_sha").and_then(serde_json::Value::as_str)
            != Some(reporter_git_sha.as_str())
        {
            quality.warnings.push(classify_warning(format!(
                "daily runtime provenance reporter mismatch for {date}: runtime={} reporter={reporter_git_sha}",
                identity
                    .get("git_sha")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("missing")
            )));
        }
    } else {
        quality.warnings.push(classify_warning(format!(
            "daily runtime provenance reporter mismatch for {date}: runtime_or_reporter_git_sha_missing"
        )));
    }
}

pub(super) fn runtime_provenance_common_errors(payload: &serde_json::Value) -> Vec<String> {
    let mut errors = Vec::new();
    if payload
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
    {
        errors.push("/schema_version must equal 1".to_owned());
    }
    require_provenance_text(payload, "/backend_impl", "rust", &mut errors);
    for pointer in [
        "/app_name",
        "/runtime_role",
        "/execution_mode",
        "/paper_maker_fill_policy",
        "/storage_container",
        "/event_blob_prefix",
    ] {
        if payload
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .is_none_or(str::is_empty)
        {
            errors.push(format!("{pointer} must be non-empty"));
        }
    }
    for pointer in [
        "/shadow_only",
        "/allow_live",
        "/enable_taker_orders",
        "/allow_emergency_account_cancel",
        "/adaptive_regime_enabled",
        "/publish_strategy_canary_intents",
        "/research_only",
    ] {
        if payload
            .pointer(pointer)
            .and_then(serde_json::Value::as_bool)
            .is_none()
        {
            errors.push(format!("{pointer} must be boolean"));
        }
    }
    if payload
        .get("storage_account")
        .and_then(serde_json::Value::as_str)
        .is_none_or(str::is_empty)
    {
        errors.push("/storage_account must be non-empty".to_owned());
    }
    if payload
        .get("git_sha")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| !polyedge_config::is_full_git_sha(value))
    {
        errors.push("/git_sha must be a canonical full commit ID".to_owned());
    }
    if payload
        .pointer("/runtime_config_hash")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| !is_prefixed_sha256(value))
    {
        errors.push("/runtime_config_hash must be a canonical sha256 digest".to_owned());
    }
    errors
}

pub(super) fn shadow_runtime_provenance_errors(payload: &serde_json::Value) -> Vec<String> {
    let expected_candidate = FrozenStrategyMode::DynamicQuoteStyle.candidate();
    let mut errors = runtime_provenance_common_errors(payload);
    require_provenance_text(payload, "/app_name", "polyedge-shadow-neu", &mut errors);
    require_provenance_text(
        payload,
        "/runtime_role",
        "profitability_shadow",
        &mut errors,
    );
    require_provenance_bool(payload, "/shadow_only", true, &mut errors);
    require_provenance_text(payload, "/execution_mode", "paper", &mut errors);
    require_provenance_bool(payload, "/allow_live", false, &mut errors);
    require_provenance_bool(payload, "/enable_taker_orders", false, &mut errors);
    require_provenance_bool(
        payload,
        "/allow_emergency_account_cancel",
        false,
        &mut errors,
    );
    require_provenance_text(payload, "/paper_maker_fill_policy", "none", &mut errors);
    require_provenance_bool(payload, "/adaptive_regime_enabled", true, &mut errors);
    require_provenance_text(
        payload,
        "/adaptive_regime_mode",
        "dynamic_quote_style",
        &mut errors,
    );
    require_provenance_bool(
        payload,
        "/publish_strategy_canary_intents",
        true,
        &mut errors,
    );
    require_provenance_bool(payload, "/research_only", true, &mut errors);
    require_provenance_text(
        payload,
        "/candidate/name",
        &expected_candidate.name,
        &mut errors,
    );
    require_provenance_text(
        payload,
        "/candidate/version",
        &expected_candidate.version,
        &mut errors,
    );
    require_provenance_text(
        payload,
        "/candidate/config_hash",
        &expected_candidate.config_hash,
        &mut errors,
    );
    require_provenance_text(
        payload,
        "/storage_container",
        "polyedge-shadow-events",
        &mut errors,
    );
    if payload
        .get("event_blob_prefix")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| !value.starts_with("shadow-events/"))
    {
        errors.push("/event_blob_prefix must start with shadow-events/".to_owned());
    }
    if payload
        .pointer("/execution_model/sha256")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| !is_prefixed_sha256(value))
    {
        errors.push("/execution_model/sha256 must be a canonical sha256 digest".to_owned());
    }
    for pointer in ["/execution_model/version", "/execution_model/blob_uri"] {
        if payload
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .is_none_or(str::is_empty)
        {
            errors.push(format!("{pointer} must be non-empty"));
        }
    }
    errors
}

fn primary_runtime_provenance_errors(payload: &serde_json::Value) -> Vec<String> {
    let mut errors = runtime_provenance_common_errors(payload);
    require_provenance_text(payload, "/app_name", "polyedge", &mut errors);
    require_provenance_text(payload, "/runtime_role", "primary", &mut errors);
    require_provenance_bool(payload, "/shadow_only", false, &mut errors);
    require_provenance_text(payload, "/execution_mode", "paper", &mut errors);
    require_provenance_bool(payload, "/allow_live", false, &mut errors);
    require_provenance_bool(payload, "/enable_taker_orders", false, &mut errors);
    require_provenance_bool(
        payload,
        "/allow_emergency_account_cancel",
        false,
        &mut errors,
    );
    require_provenance_text(
        payload,
        "/paper_maker_fill_policy",
        "touch_after_quote_was_live",
        &mut errors,
    );
    require_provenance_bool(payload, "/adaptive_regime_enabled", false, &mut errors);
    require_provenance_text(payload, "/adaptive_regime_mode", "paper_only", &mut errors);
    require_provenance_bool(
        payload,
        "/publish_strategy_canary_intents",
        false,
        &mut errors,
    );
    require_provenance_bool(payload, "/research_only", true, &mut errors);
    require_provenance_text(payload, "/storage_container", "bot-events", &mut errors);
    require_provenance_text(payload, "/event_blob_prefix", "events", &mut errors);
    if !payload
        .get("candidate")
        .is_some_and(serde_json::Value::is_null)
    {
        errors.push("/candidate must be null for the primary paper profile".to_owned());
    }
    errors
}

fn require_provenance_text(
    payload: &serde_json::Value,
    pointer: &str,
    expected: &str,
    errors: &mut Vec<String>,
) {
    if payload.pointer(pointer).and_then(serde_json::Value::as_str) != Some(expected) {
        errors.push(format!("{pointer} must equal {expected}"));
    }
}

fn require_provenance_bool(
    payload: &serde_json::Value,
    pointer: &str,
    expected: bool,
    errors: &mut Vec<String>,
) {
    if payload
        .pointer(pointer)
        .and_then(serde_json::Value::as_bool)
        != Some(expected)
    {
        errors.push(format!("{pointer} must equal {expected}"));
    }
}

fn is_prefixed_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn daily_provenance_required(date: NaiveDate) -> bool {
    let cutoff = NaiveDate::parse_from_str(DAILY_PROVENANCE_CUTOFF, "%Y-%m-%d")
        .expect("daily provenance cutoff is valid");
    date >= cutoff
}

fn parse_utc_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn collect_publishable_files(root: &Path) -> Result<Vec<(PathBuf, PathBuf)>, ResearchError> {
    fn visit(
        root: &Path,
        current: &Path,
        files: &mut Vec<(PathBuf, PathBuf)>,
    ) -> Result<(), ResearchError> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                visit(root, &path, files)?;
                continue;
            }
            let relative = path.strip_prefix(root).map_err(|_| {
                ResearchError::InvalidInput("daily source traversal escaped root".to_owned())
            })?;
            let extension = path.extension().and_then(|value| value.to_str());
            if matches!(extension, Some("json" | "md"))
                && !matches!(
                    relative.file_name().and_then(|value| value.to_str()),
                    Some(MANIFEST_FILE | LATEST_FILE)
                )
            {
                files.push((relative.to_path_buf(), path));
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

fn value_as_message(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| (!value.is_null()).then(|| value.to_string()))
}

fn decimal_from_json(value: &serde_json::Value) -> Option<Decimal> {
    if let Some(value) = value.as_str() {
        return value.parse().ok();
    }
    value
        .as_i64()
        .map(Decimal::from)
        .or_else(|| value.as_u64().map(Decimal::from))
        .or_else(|| value.as_f64().and_then(Decimal::from_f64))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Passed,
    Collecting,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GateOutcome {
    pub gate: String,
    pub status: GateStatus,
    pub actual: String,
    pub required: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionPhase {
    Frozen,
    RiskRepair,
    ShadowCollecting,
    ShadowPassed,
    EvidenceCollecting,
    CanaryReady,
    LimitedLive,
    ProfitableGo,
    StoppedNoGo,
}

pub const FUNDED_LADDER_TARGETS: [u32; 5] = [1, 5, 25, 100, 200];
pub const MIN_FUNDED_MARKOUT_SAMPLE_SIZE: u32 = 10;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedLadderMetrics {
    pub observed_calendar_days: u32,
    pub cumulative_eligible_orders: u32,
    pub cumulative_funded_orders: u32,
    pub cumulative_net_pnl: Decimal,
    pub cumulative_max_drawdown: Decimal,
    pub mean_net_markout_30s: Decimal,
    pub net_markout_30s_lower_95: Decimal,
    pub markout_sample_size: u32,
    pub data_quality_passed: bool,
    pub unresolved_exposure: Decimal,
}

impl Default for FundedLadderMetrics {
    fn default() -> Self {
        Self {
            observed_calendar_days: 0,
            cumulative_eligible_orders: 0,
            cumulative_funded_orders: 0,
            cumulative_net_pnl: Decimal::ZERO,
            cumulative_max_drawdown: Decimal::ZERO,
            mean_net_markout_30s: Decimal::ZERO,
            net_markout_30s_lower_95: Decimal::ZERO,
            markout_sample_size: 0,
            data_quality_passed: false,
            unresolved_exposure: Decimal::ZERO,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FundedStageGrantV1 {
    pub schema_version: String,
    pub grant_id: String,
    pub source_state_sha256: String,
    pub candidate: CandidateIdentity,
    pub stage_target_orders: u32,
    pub single_use: bool,
    pub authorized_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImmutableArtifactBindingV1 {
    pub blob_name: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueModelTransitionV1 {
    pub schema_version: String,
    pub binding: ExecutionModelBinding,
    pub generated_at: DateTime<Utc>,
    pub training_cutoff: DateTime<Utc>,
    pub training_dataset_sha256: String,
    pub training_checkpoint_sha256: String,
    pub model_quality_passed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedHoldoutEvaluationV1 {
    pub schema_version: String,
    pub exact_order_count: u32,
    pub label_sample_size: u32,
    pub filled_order_count: u32,
    pub non_filled_order_count: u32,
    pub brier_score: Decimal,
    pub naive_base_rate_brier_score: Decimal,
    pub brier_improvement_fraction: Decimal,
    pub expected_calibration_error: Decimal,
    pub markout_sample_size: u32,
    pub mean_net_markout_30s: Decimal,
    pub net_markout_30s_lower_95: Decimal,
    pub holdout_net_pnl: Decimal,
    pub holdout_max_drawdown: Decimal,
    pub mean_holdout_net_pnl_per_order: Decimal,
    pub holdout_net_pnl_per_order_lower_95: Decimal,
    pub passed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedCheckpointEvidenceV1 {
    pub schema_version: String,
    pub evidence_protocol_version: u32,
    pub candidate: CandidateIdentity,
    pub source_state_sha256: String,
    pub stage_target_orders: u32,
    pub exact_eligible_order_count: u32,
    pub exact_funded_order_count: u32,
    pub observed_calendar_days: u32,
    pub cumulative_net_pnl: Decimal,
    pub cumulative_max_drawdown: Decimal,
    pub mean_net_markout_30s: Decimal,
    pub net_markout_30s_lower_95: Decimal,
    pub markout_sample_size: u32,
    pub data_quality_passed: bool,
    pub unresolved_exposure: Decimal,
    pub lifecycle_reconciled: bool,
    pub protocol_v3_order_artifacts: Vec<ImmutableArtifactBindingV1>,
    pub terminal_risk_portfolio_artifacts: Vec<ImmutableArtifactBindingV1>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedProtocolV3OrderEvidence {
    pub run_id: String,
    pub probe_id: String,
    pub order_id: String,
    pub started_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub campaign_starting_equity: Decimal,
    pub net_external_cash_flows: Decimal,
    pub ending_equity: Decimal,
    pub cumulative_net_pnl: Decimal,
    pub terminal_drawdown: Decimal,
    pub net_markout_30s: Option<Decimal>,
}

impl FundedCheckpointEvidenceV1 {
    fn validated_metrics(
        &self,
        state: &FundedLadderStateV1,
    ) -> Result<FundedLadderMetrics, ResearchError> {
        if self.schema_version != "funded_checkpoint_evidence_v1"
            || self.evidence_protocol_version != 3
            || self.candidate != state.candidate
            || self.source_state_sha256 != state.state_sha256()?
            || self.stage_target_orders != state.active_target_orders
            || self.exact_funded_order_count != state.active_target_orders
            || self.exact_eligible_order_count != state.active_target_orders
            || self.protocol_v3_order_artifacts.len() != state.active_target_orders as usize
            || self.terminal_risk_portfolio_artifacts.len() != state.active_target_orders as usize
            || !self.lifecycle_reconciled
        {
            return Err(ResearchError::InvalidInput(
                "canonical funded advancement requires exact immutable protocol-v3 checkpoint evidence and terminal risk/portfolio binding"
                    .to_owned(),
            ));
        }
        let mut identities = std::collections::BTreeSet::new();
        let mut orders = Vec::new();
        for (summary_binding, terminal_binding) in self
            .protocol_v3_order_artifacts
            .iter()
            .zip(&self.terminal_risk_portfolio_artifacts)
        {
            let summary = load_bound_artifact(summary_binding)?;
            let terminal = load_bound_artifact(terminal_binding)?;
            let order = validate_protocol_v3_order_evidence(
                &self.candidate,
                &summary,
                &terminal,
                terminal_binding,
            )?;
            if !identities.insert((
                order.run_id.clone(),
                order.probe_id.clone(),
                order.order_id.clone(),
            )) {
                return Err(ResearchError::InvalidInput(
                    "funded checkpoint contains a duplicated run/probe/order identity".to_owned(),
                ));
            }
            orders.push(order);
        }
        orders.sort_by_key(|order| order.observed_at);
        let baseline = orders
            .first()
            .map(|order| order.campaign_starting_equity)
            .ok_or_else(|| ResearchError::InvalidInput("funded checkpoint is empty".to_owned()))?;
        if orders
            .iter()
            .any(|order| order.campaign_starting_equity != baseline)
        {
            return Err(ResearchError::InvalidInput(
                "terminal artifacts disagree on the immutable campaign baseline".to_owned(),
            ));
        }
        let mut peak = baseline;
        let mut derived_drawdown = Decimal::ZERO;
        for order in &orders {
            let adjusted_equity = order.ending_equity - order.net_external_cash_flows;
            peak = peak.max(adjusted_equity);
            derived_drawdown = derived_drawdown.max(peak - adjusted_equity);
        }
        let latest = orders.last().expect("non-empty checked above");
        let derived_pnl = latest.cumulative_net_pnl.round_dp(12);
        derived_drawdown = derived_drawdown.round_dp(12);
        let first_started = orders.iter().map(|order| order.started_at).min().unwrap();
        let days = (latest.observed_at.date_naive() - first_started.date_naive()).num_days() + 1;
        let observed_days = u32::try_from(days).unwrap_or(u32::MAX);
        let markouts = orders
            .iter()
            .filter_map(|order| order.net_markout_30s)
            .collect::<Vec<_>>();
        let markout_sample_size = markouts.len() as u32;
        let mean_markout = if markouts.is_empty() {
            Decimal::ZERO
        } else {
            markouts.iter().copied().sum::<Decimal>() / Decimal::from(markouts.len() as u32)
        }
        .round_dp(12);
        let lower_95 = if markouts.len() < 2 {
            Decimal::ZERO
        } else {
            let mean = mean_markout.to_f64().unwrap_or_default();
            let variance = markouts
                .iter()
                .map(|value| {
                    let delta = value.to_f64().unwrap_or_default() - mean;
                    delta * delta
                })
                .sum::<f64>()
                / (markouts.len() - 1) as f64;
            Decimal::from_f64(mean - 1.96 * (variance / markouts.len() as f64).sqrt())
                .unwrap_or_default()
                .round_dp(12)
        };
        if derived_pnl != self.cumulative_net_pnl
            || derived_drawdown != self.cumulative_max_drawdown
            || observed_days != self.observed_calendar_days
            || mean_markout != self.mean_net_markout_30s
            || lower_95 != self.net_markout_30s_lower_95
            || markout_sample_size != self.markout_sample_size
            || !self.data_quality_passed
            || self.unresolved_exposure != Decimal::ZERO
        {
            return Err(ResearchError::InvalidInput(
                "checkpoint rollup claims do not equal independently derived immutable evidence"
                    .to_owned(),
            ));
        }
        Ok(FundedLadderMetrics {
            observed_calendar_days: observed_days,
            cumulative_eligible_orders: identities.len() as u32,
            cumulative_funded_orders: identities.len() as u32,
            cumulative_net_pnl: derived_pnl,
            cumulative_max_drawdown: derived_drawdown,
            mean_net_markout_30s: mean_markout,
            net_markout_30s_lower_95: lower_95,
            markout_sample_size,
            data_quality_passed: true,
            unresolved_exposure: Decimal::ZERO,
        })
    }
}

pub fn validate_protocol_v3_order_evidence(
    candidate: &CandidateIdentity,
    summary: &serde_json::Value,
    terminal: &serde_json::Value,
    terminal_binding: &ImmutableArtifactBindingV1,
) -> Result<ValidatedProtocolV3OrderEvidence, ResearchError> {
    let fail = |message: &str| {
        ResearchError::InvalidInput(format!("protocol-v3 order evidence rejected: {message}"))
    };
    if summary["schema_version"].as_u64() != Some(3)
        || summary["evidence_protocol_version"].as_u64() != Some(3)
        || summary["status"].as_str() != Some("completed")
        || summary["order_submission_attempted"].as_bool() != Some(true)
        || summary["order_submitted"].as_bool() != Some(true)
        || summary["submitted_order_count"].as_u64() != Some(1)
        || summary["completed_probe_count"].as_u64() != Some(1)
    {
        return Err(fail(
            "summary is not exactly one completed submitted protocol-v3 order",
        ));
    }
    if summary["candidate"]["name"].as_str() != Some(candidate.name.as_str())
        || summary["candidate"]["candidate_version"].as_str()
            != Some(candidate.candidate_version.as_str())
        || summary["candidate"]["config_hash"].as_str() != Some(candidate.config_hash.as_str())
    {
        return Err(fail("candidate identity does not match"));
    }
    let probes = summary["probes"]
        .as_array()
        .ok_or_else(|| fail("probes array is missing"))?;
    if probes.len() != 1 || probes[0]["order_submitted"].as_bool() != Some(true) {
        return Err(fail("exactly one submitted probe is required"));
    }
    let probe = &probes[0];
    let lifecycle = &probe["lifecycle"];
    if lifecycle["reconciliation_complete"].as_bool() != Some(true)
        || lifecycle["zero_open_orders_confirmed"].as_bool() != Some(true)
        || lifecycle["data_gap_detected"].as_bool() != Some(false)
        || lifecycle["cancellation_failure"].as_bool() != Some(false)
    {
        return Err(fail(
            "lifecycle is not reconciled, globally zero-open, and data-gap free",
        ));
    }
    let observations = probe["model_observations"]
        .as_array()
        .ok_or_else(|| fail("model observations are missing"))?;
    if !observations.iter().any(|row| {
        row["eligible"].as_bool() == Some(true)
            && row["quality_eligible"].as_bool() == Some(true)
            && row["reconciliation_complete"].as_bool() == Some(true)
            && row["zero_open_orders_confirmed"].as_bool() == Some(true)
    }) {
        return Err(fail("no eligible reconciled model observation exists"));
    }
    let started_at = DateTime::parse_from_rfc3339(
        summary["started_ts"]
            .as_str()
            .ok_or_else(|| fail("summary started_ts is missing"))?,
    )
    .map_err(|_| fail("summary started_ts is invalid"))?
    .with_timezone(&Utc);
    let model = &summary["prediction_model"];
    normalize_sha256(
        model["sha256"]
            .as_str()
            .ok_or_else(|| fail("prediction model SHA-256 is missing"))?,
    )?;
    let model_uri = model["blob_uri"].as_str().unwrap_or_default();
    let model_path = model_uri.strip_prefix("azure://").unwrap_or_default();
    let mut model_parts = model_path.splitn(3, '/');
    let model_account = model_parts.next().unwrap_or_default();
    let model_container = model_parts.next().unwrap_or_default();
    let model_blob = model_parts.next().unwrap_or_default();
    if model_account.is_empty()
        || model_container.is_empty()
        || model_blob.is_empty()
        || model_blob.contains("..")
        || model["container_name"].as_str() != Some(model_container)
        || model["blob_name"].as_str() != Some(model_blob)
        || model["model_version"].as_str().is_none_or(str::is_empty)
    {
        return Err(fail("prediction model exact artifact lineage is missing"));
    }
    let model_generated_at = DateTime::parse_from_rfc3339(
        model["generated_at"]
            .as_str()
            .ok_or_else(|| fail("prediction model generated_at is missing"))?,
    )
    .map_err(|_| fail("prediction model generated_at is invalid"))?
    .with_timezone(&Utc);
    if model_generated_at >= started_at {
        return Err(fail(
            "prediction model was not an immutable temporal prior to this order",
        ));
    }

    let run_id = required_json_text(summary, "/run_id")?;
    let probe_id = required_json_text(probe, "/probe_id")?;
    let order_id = required_json_text(lifecycle, "/order_id")?;
    let matched = json_decimal(&lifecycle["actual_matched_size"])?;
    if matched < Decimal::ZERO {
        return Err(fail("matched size is negative"));
    }
    let related_ids = lifecycle["related_trade_ids"]
        .as_array()
        .ok_or_else(|| fail("related trade IDs are missing"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| fail("related trade ID is empty"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let unique_related = related_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if unique_related.len() != related_ids.len() {
        return Err(fail("related trade IDs are duplicated"));
    }
    let markouts = probe["markouts"]
        .as_array()
        .ok_or_else(|| fail("markouts are missing"))?;
    if matched == Decimal::ZERO && (!related_ids.is_empty() || !markouts.is_empty()) {
        return Err(fail("a no-fill order contains fill or markout claims"));
    }
    if matched > Decimal::ZERO
        && (related_ids.is_empty() || markouts.len() != related_ids.len() * 3)
    {
        return Err(fail("a fill requires exactly one 1/5/30 markout triplet"));
    }
    let mut weighted_markout = Decimal::ZERO;
    let mut weighted_size = Decimal::ZERO;
    for fill_id in &related_ids {
        for horizon in [1_u64, 5, 30] {
            let rows = markouts
                .iter()
                .filter(|row| {
                    row["fill_id"].as_str() == Some(*fill_id)
                        && row["horizon_seconds"].as_u64() == Some(horizon)
                })
                .collect::<Vec<_>>();
            if rows.len() != 1 {
                return Err(fail(
                    "each authenticated fill requires exactly one 1/5/30-second markout",
                ));
            }
            let row = rows[0];
            let delay = row["observation_delay_ms"]
                .as_i64()
                .ok_or_else(|| fail("markout delay is missing"))?;
            let fill_size = json_decimal(&row["fill_size"])?;
            let _ = json_decimal(&row["midpoint"])?;
            let _ = json_decimal(&row["executable_price"])?;
            let executable_markout = json_decimal(&row["executable_markout_per_share"])?;
            if !(0..=2_000).contains(&delay) || fill_size <= Decimal::ZERO {
                return Err(fail("markout values are incomplete or untimely"));
            }
            if horizon == 30 {
                weighted_markout += executable_markout * fill_size;
                weighted_size += fill_size;
            }
        }
    }
    if matched > Decimal::ZERO && (weighted_size - matched).abs() > Decimal::new(1, 8) {
        return Err(fail(
            "30-second markout fill sizes do not reconcile to matched size",
        ));
    }
    let net_markout_30s = if weighted_size > Decimal::ZERO {
        Some(weighted_markout / weighted_size)
    } else {
        None
    };

    if terminal["schema"].as_str() != Some("polyedge.canary_terminal_risk_portfolio.v1")
        || terminal["producer"].as_str() != Some("polyedge_node_authenticated_risk_terminal")
        || terminal["run_id"].as_str() != Some(run_id.as_str())
        || terminal["probe_id"].as_str() != Some(probe_id.as_str())
        || terminal["order_id"].as_str() != Some(order_id.as_str())
        || terminal["settlement_verified"].as_bool() != Some(true)
        || terminal["portfolio_reconciled"].as_bool() != Some(true)
        || terminal["zero_open_orders_confirmed"].as_bool() != Some(true)
        || json_decimal(&terminal["unresolved_exposure"])? != Decimal::ZERO
        || json_decimal(&terminal["reconciliation_discrepancy"])? > Decimal::new(1, 2)
    {
        return Err(fail(
            "terminal risk/portfolio evidence is invalid or mismatched",
        ));
    }
    let source = terminal["source"].as_str().unwrap_or_default();
    let transaction_hash = terminal["settlement_transaction_hash"]
        .as_str()
        .unwrap_or_default();
    if (matched == Decimal::ZERO && source != "authenticated_no_fill")
        || (matched > Decimal::ZERO
            && (source != "polymarket_data_api_plus_onchain_redemption"
                || transaction_hash.is_empty()))
    {
        return Err(fail("terminal evidence source does not match fill state"));
    }
    if matched == Decimal::ZERO {
        let provenance = &summary["provenance"];
        if provenance["terminal_evidence_blob_name"].as_str()
            != Some(terminal_binding.blob_name.as_str())
            || normalize_sha256(
                provenance["terminal_evidence_sha256"]
                    .as_str()
                    .ok_or_else(|| fail("no-fill terminal evidence SHA-256 is missing"))?,
            )? != normalize_sha256(&terminal_binding.sha256)?
        {
            return Err(fail(
                "no-fill summary does not cross-link its exact terminal artifact",
            ));
        }
    }
    let baseline = json_decimal(&terminal["campaign_starting_equity"])?;
    let cash_flows = json_decimal(&terminal["net_external_cash_flows"])?;
    let liquid = json_decimal(&terminal["liquid_collateral"])?;
    let positions = json_decimal(&terminal["summed_position_value"])?;
    let ending = json_decimal(&terminal["cash_flow_adjusted_ending_equity"])?;
    let stated_discrepancy = json_decimal(&terminal["reconciliation_discrepancy"])?;
    let calculated_discrepancy = (liquid + positions - ending).abs();
    if calculated_discrepancy > Decimal::new(1, 2)
        || (calculated_discrepancy - stated_discrepancy).abs() > Decimal::new(1, 2)
    {
        return Err(fail(
            "terminal equity components do not reconcile within $0.01",
        ));
    }
    let maximum = json_decimal(&terminal["maximum_observed_equity"])?;
    let minimum = json_decimal(&terminal["minimum_observed_equity"])?;
    if maximum < minimum {
        return Err(fail("terminal equity extrema are invalid"));
    }
    let observed_at = DateTime::parse_from_rfc3339(
        terminal["observed_at"]
            .as_str()
            .ok_or_else(|| fail("terminal observed_at is missing"))?,
    )
    .map_err(|_| fail("terminal observed_at is invalid"))?
    .with_timezone(&Utc);
    if observed_at < started_at {
        return Err(fail("terminal evidence predates the order"));
    }
    Ok(ValidatedProtocolV3OrderEvidence {
        run_id,
        probe_id,
        order_id,
        started_at,
        observed_at,
        campaign_starting_equity: baseline,
        net_external_cash_flows: cash_flows,
        ending_equity: ending,
        cumulative_net_pnl: ending - baseline - cash_flows,
        terminal_drawdown: maximum - minimum,
        net_markout_30s,
    })
}

fn load_bound_artifact(
    binding: &ImmutableArtifactBindingV1,
) -> Result<serde_json::Value, ResearchError> {
    let bytes = if Path::new(&binding.blob_name).is_file() {
        fs::read(&binding.blob_name)?
    } else {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            ResearchError::InvalidInput(
                "bound evidence is not local and Azure storage is unconfigured".to_owned(),
            )
        })?;
        let container = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
            .unwrap_or_else(|_| "bot-events".to_owned());
        let client_id = std::env::var("AZURE_CLIENT_ID").ok();
        let mut client = AzureBlobClient::with_managed_identity(account, container, client_id);
        client
            .download_blob_bytes(&binding.blob_name)
            .map_err(|error| {
                ResearchError::Azure(format!(
                    "reading bound funded evidence {}: {error}",
                    binding.blob_name
                ))
            })?
    };
    verify_exact_hash("bound funded evidence", &bytes, &binding.sha256)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn parse_azure_artifact_uri(uri: &str) -> Result<(String, String, String), ResearchError> {
    let rest = uri.strip_prefix("azure://").ok_or_else(|| {
        ResearchError::InvalidInput("artifact URI must start with azure://".to_owned())
    })?;
    let (account, tail) = rest.split_once('/').ok_or_else(|| {
        ResearchError::InvalidInput(
            "artifact URI must contain account, container, and blob".to_owned(),
        )
    })?;
    let (container, blob) = tail.split_once('/').ok_or_else(|| {
        ResearchError::InvalidInput(
            "artifact URI must contain account, container, and blob".to_owned(),
        )
    })?;
    if account.is_empty()
        || container.is_empty()
        || blob.is_empty()
        || account.contains(['/', '\\'])
        || container.contains(['/', '\\'])
        || blob.starts_with('/')
        || blob
            .split('/')
            .any(|segment| segment.is_empty() || segment == "..")
    {
        return Err(ResearchError::InvalidInput(
            "artifact Azure account/container/blob path is unsafe".to_owned(),
        ));
    }
    Ok((account.to_owned(), container.to_owned(), blob.to_owned()))
}

fn read_exact_artifact(
    path: &Path,
    expected_sha256: &str,
    label: &str,
) -> Result<Vec<u8>, ResearchError> {
    let text = path.to_string_lossy();
    let bytes = if text.starts_with("azure://") {
        let (uri_account, container, blob_name) = parse_azure_artifact_uri(&text)?;
        let configured_account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            ResearchError::InvalidInput(format!(
                "{label} is remote and AZURE_STORAGE_ACCOUNT_NAME is missing"
            ))
        })?;
        if uri_account != configured_account {
            return Err(ResearchError::InvalidInput(format!(
                "{label} Azure account does not match configured storage"
            )));
        }
        let client_id = std::env::var("AZURE_CLIENT_ID").ok();
        let mut client = AzureBlobClient::with_managed_identity(uri_account, container, client_id);
        client.download_blob_bytes(&blob_name).map_err(|error| {
            ResearchError::Azure(format!("reading exact {label} {blob_name}: {error}"))
        })?
    } else {
        fs::read(path)?
    };
    verify_exact_hash(label, &bytes, expected_sha256)?;
    Ok(bytes)
}

fn required_json_text(value: &serde_json::Value, pointer: &str) -> Result<String, ResearchError> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ResearchError::InvalidInput(format!("evidence is missing {pointer}")))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedLadderStateV1 {
    pub schema_version: String,
    pub campaign_id: String,
    pub candidate: CandidateIdentity,
    pub phase: PromotionPhase,
    pub stage_targets: Vec<u32>,
    pub active_stage_index: usize,
    pub active_target_orders: u32,
    pub completed_checkpoints: Vec<u32>,
    pub metrics: FundedLadderMetrics,
    pub maximum_calendar_days: u32,
    pub maximum_funded_orders: u32,
    pub maximum_drawdown: Decimal,
    pub human_grant_required: bool,
    pub stage_authorized: bool,
    pub consumed_grant_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_1_protocol_v3_artifact: Option<ImmutableArtifactBindingV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_1_terminal_artifact: Option<ImmutableArtifactBindingV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verified_terminal_artifact: Option<ImmutableArtifactBindingV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_model_transition: Option<QueueModelTransitionV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holdout_evaluation: Option<FundedHoldoutEvaluationV1>,
    pub terminal: bool,
    pub promotion_allowed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl FundedLadderStateV1 {
    pub fn new(candidate: CandidateIdentity, now: DateTime<Utc>) -> Result<Self, ResearchError> {
        validate_candidate(&candidate)?;
        Ok(Self {
            schema_version: "funded_ladder_state_v1".to_owned(),
            campaign_id: format!(
                "{}-funded-ladder-{}",
                candidate.candidate_version,
                now.format("%Y%m%dT%H%M%S%.fZ")
            ),
            candidate,
            phase: PromotionPhase::EvidenceCollecting,
            stage_targets: FUNDED_LADDER_TARGETS.to_vec(),
            active_stage_index: 0,
            active_target_orders: FUNDED_LADDER_TARGETS[0],
            completed_checkpoints: Vec::new(),
            metrics: FundedLadderMetrics::default(),
            maximum_calendar_days: 60,
            maximum_funded_orders: 200,
            maximum_drawdown: Decimal::ONE,
            human_grant_required: true,
            stage_authorized: false,
            consumed_grant_ids: Vec::new(),
            checkpoint_1_protocol_v3_artifact: None,
            checkpoint_1_terminal_artifact: None,
            last_verified_terminal_artifact: None,
            queue_model_transition: None,
            holdout_evaluation: None,
            terminal: false,
            promotion_allowed: false,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn state_sha256(&self) -> Result<String, ResearchError> {
        let bytes = serde_json::to_vec(self)?;
        Ok(format!("sha256:{}", sha256_bytes(&bytes)))
    }

    pub fn initialize_after_canary(
        candidate: CandidateIdentity,
        metrics: FundedLadderMetrics,
        human_grant_id: &str,
        protocol_v3_artifact: ImmutableArtifactBindingV1,
        terminal_artifact: ImmutableArtifactBindingV1,
        now: DateTime<Utc>,
    ) -> Result<Self, ResearchError> {
        if metrics.cumulative_eligible_orders != 1
            || metrics.cumulative_funded_orders != 1
            || human_grant_id.trim().is_empty()
        {
            return Err(ResearchError::InvalidInput(
                "checkpoint 1 requires exactly one reconciled protocol-v3 canary and consumed human grant evidence"
                    .to_owned(),
            ));
        }
        let mut state = Self::new(candidate, now)?;
        state.stage_authorized = true;
        state.human_grant_required = false;
        state.consumed_grant_ids.push(human_grant_id.to_owned());
        validate_sha256("checkpoint 1 terminal artifact", &terminal_artifact.sha256)?;
        validate_sha256(
            "checkpoint 1 protocol-v3 artifact",
            &protocol_v3_artifact.sha256,
        )?;
        if terminal_artifact.blob_name.trim().is_empty()
            || protocol_v3_artifact.blob_name.trim().is_empty()
        {
            return Err(ResearchError::InvalidInput(
                "checkpoint 1 terminal artifact blob name is required".to_owned(),
            ));
        }
        state.checkpoint_1_protocol_v3_artifact = Some(protocol_v3_artifact);
        state.checkpoint_1_terminal_artifact = Some(terminal_artifact.clone());
        state.transition_with_evidence(metrics, None, Some(terminal_artifact), None, None, now)
    }

    pub fn transition(
        &self,
        observation: FundedLadderMetrics,
        grant: Option<&FundedStageGrantV1>,
        now: DateTime<Utc>,
    ) -> Result<Self, ResearchError> {
        self.transition_with_evidence(
            observation,
            grant,
            self.last_verified_terminal_artifact.clone(),
            None,
            None,
            now,
        )
    }

    fn transition_with_evidence(
        &self,
        observation: FundedLadderMetrics,
        grant: Option<&FundedStageGrantV1>,
        last_terminal_artifact: Option<ImmutableArtifactBindingV1>,
        queue_model_transition: Option<QueueModelTransitionV1>,
        holdout_evaluation: Option<FundedHoldoutEvaluationV1>,
        now: DateTime<Utc>,
    ) -> Result<Self, ResearchError> {
        self.validate()?;
        if self.terminal {
            return Ok(self.clone());
        }
        if now < self.updated_at
            || observation.observed_calendar_days < self.metrics.observed_calendar_days
            || observation.cumulative_eligible_orders < self.metrics.cumulative_eligible_orders
            || observation.cumulative_funded_orders < self.metrics.cumulative_funded_orders
            || observation.cumulative_funded_orders > observation.cumulative_eligible_orders
            || observation.cumulative_funded_orders > self.active_target_orders
            || observation.cumulative_max_drawdown < self.metrics.cumulative_max_drawdown
            || observation.unresolved_exposure < Decimal::ZERO
        {
            return Err(ResearchError::InvalidInput(
                "funded ladder observation regressed or skipped its active checkpoint".to_owned(),
            ));
        }
        let mut next = self.clone();
        if let Some(grant) = grant {
            next.validate_and_consume_grant(grant, now)?;
        }
        if observation.cumulative_funded_orders > self.metrics.cumulative_funded_orders
            && !next.stage_authorized
        {
            return Err(ResearchError::InvalidInput(
                "funded ladder cannot record a new funded order without the exact stage grant"
                    .to_owned(),
            ));
        }
        next.metrics = observation;
        if next.metrics.cumulative_funded_orders > self.metrics.cumulative_funded_orders {
            let terminal = last_terminal_artifact.ok_or_else(|| {
                ResearchError::InvalidInput(
                    "new funded evidence requires its exact latest terminal artifact binding"
                        .to_owned(),
                )
            })?;
            validate_sha256("latest funded terminal artifact", &terminal.sha256)?;
            if terminal.blob_name.trim().is_empty() {
                return Err(ResearchError::InvalidInput(
                    "latest funded terminal artifact blob name is required".to_owned(),
                ));
            }
            next.last_verified_terminal_artifact = Some(terminal);
        }
        next.updated_at = now;
        if next.metrics.observed_calendar_days >= next.maximum_calendar_days {
            next.stop_terminal();
            return Ok(next);
        }
        if next.metrics.cumulative_funded_orders == next.active_target_orders {
            if next.active_target_orders == 100 {
                next.queue_model_transition = Some(queue_model_transition.ok_or_else(|| {
                    ResearchError::InvalidInput(
                        "checkpoint 100 requires an explicit immutable queue-model transition"
                            .to_owned(),
                    )
                })?);
            } else if queue_model_transition.is_some() {
                return Err(ResearchError::InvalidInput(
                    "queue-model transition is accepted only at checkpoint 100".to_owned(),
                ));
            }
            if next.active_target_orders == 200 {
                next.holdout_evaluation = Some(holdout_evaluation.ok_or_else(|| {
                    ResearchError::InvalidInput(
                        "checkpoint 200 requires exact orders 101-200 holdout evaluation"
                            .to_owned(),
                    )
                })?);
            } else if holdout_evaluation.is_some() {
                return Err(ResearchError::InvalidInput(
                    "holdout evaluation is accepted only at checkpoint 200".to_owned(),
                ));
            }
            let required_markout_samples = next
                .active_target_orders
                .min(MIN_FUNDED_MARKOUT_SAMPLE_SIZE);
            let markout_gate_passes = next.metrics.markout_sample_size >= required_markout_samples
                && if next.active_target_orders < 25 {
                    next.metrics.mean_net_markout_30s > Decimal::ZERO
                } else {
                    next.metrics.net_markout_30s_lower_95 > Decimal::ZERO
                };
            let gates_pass = next.metrics.cumulative_net_pnl > Decimal::ZERO
                && next.metrics.cumulative_max_drawdown <= next.maximum_drawdown
                && markout_gate_passes
                && next.metrics.data_quality_passed
                && next.metrics.unresolved_exposure == Decimal::ZERO
                && (next.active_target_orders != 100
                    || next
                        .queue_model_transition
                        .as_ref()
                        .is_some_and(|transition| transition.model_quality_passed))
                && (next.active_target_orders != 200
                    || next
                        .holdout_evaluation
                        .as_ref()
                        .is_some_and(|holdout| holdout.passed));
            if !gates_pass {
                next.stop_terminal();
            } else if next.active_target_orders == next.maximum_funded_orders {
                next.phase = PromotionPhase::ProfitableGo;
                next.terminal = true;
                next.human_grant_required = false;
                next.stage_authorized = false;
            } else {
                next.completed_checkpoints.push(next.active_target_orders);
                next.active_stage_index += 1;
                next.active_target_orders = next.stage_targets[next.active_stage_index];
                next.phase = PromotionPhase::LimitedLive;
                next.human_grant_required = true;
                next.stage_authorized = false;
            }
        }
        // The state artifact is evidence/control state, never executable
        // authorization. Each funded stage still needs its exact consumed grant.
        next.promotion_allowed = false;
        next.validate()?;
        Ok(next)
    }

    fn validate_and_consume_grant(
        &mut self,
        grant: &FundedStageGrantV1,
        now: DateTime<Utc>,
    ) -> Result<(), ResearchError> {
        if self.stage_authorized
            || !self.human_grant_required
            || grant.schema_version != "funded_stage_grant_v1"
            || !grant.single_use
            || grant.candidate != self.candidate
            || grant.stage_target_orders != self.active_target_orders
            || grant.source_state_sha256 != self.state_sha256()?
            || grant.authorized_at > now
            || grant.expires_at <= now
            || grant.expires_at <= grant.authorized_at
            || grant
                .expires_at
                .signed_duration_since(grant.authorized_at)
                .num_minutes()
                > 5
            || grant.grant_id.is_empty()
            || self.consumed_grant_ids.contains(&grant.grant_id)
        {
            return Err(ResearchError::InvalidInput(
                "funded ladder stage grant is invalid, stale, reused, or not exactly state-bound"
                    .to_owned(),
            ));
        }
        self.consumed_grant_ids.push(grant.grant_id.clone());
        self.stage_authorized = true;
        self.human_grant_required = false;
        Ok(())
    }

    fn stop_terminal(&mut self) {
        self.phase = PromotionPhase::StoppedNoGo;
        self.terminal = true;
        self.human_grant_required = false;
        self.stage_authorized = false;
        self.promotion_allowed = false;
    }

    pub fn validate(&self) -> Result<(), ResearchError> {
        validate_candidate(&self.candidate)?;
        let expected_consumed_grants = self.active_stage_index + usize::from(self.stage_authorized);
        let grant_count_valid = if self.terminal {
            self.consumed_grant_ids.len() == self.active_stage_index
                || self.consumed_grant_ids.len() == self.active_stage_index + 1
        } else {
            self.consumed_grant_ids.len() == expected_consumed_grants
        };
        let grants_consistent = grant_count_valid
            && self
                .consumed_grant_ids
                .iter()
                .all(|grant| !grant.trim().is_empty())
            && self
                .consumed_grant_ids
                .iter()
                .enumerate()
                .all(|(index, grant)| !self.consumed_grant_ids[..index].contains(grant));
        let terminal_binding_valid = self.metrics.cumulative_funded_orders == 0
            || self
                .last_verified_terminal_artifact
                .as_ref()
                .is_some_and(|binding| {
                    !binding.blob_name.trim().is_empty()
                        && validate_sha256("last verified terminal artifact", &binding.sha256)
                            .is_ok()
                });
        let checkpoint_1_bindings_valid = self.metrics.cumulative_funded_orders == 0
            || self
                .checkpoint_1_protocol_v3_artifact
                .as_ref()
                .zip(self.checkpoint_1_terminal_artifact.as_ref())
                .is_some_and(|(summary, terminal)| {
                    !summary.blob_name.trim().is_empty()
                        && !terminal.blob_name.trim().is_empty()
                        && validate_sha256("checkpoint 1 summary", &summary.sha256).is_ok()
                        && validate_sha256("checkpoint 1 terminal", &terminal.sha256).is_ok()
                });
        let queue_transition_valid = if self.active_stage_index >= 4
            || (self.terminal && self.active_target_orders == 100)
        {
            self.queue_model_transition
                .as_ref()
                .is_some_and(|transition| {
                    transition.schema_version == "queue_model_transition_v1"
                        && transition.binding.model_version == "queue-calibration-v1"
                        && validate_execution_model_binding(&transition.binding).is_ok()
                        && validate_sha256(
                            "queue model training dataset",
                            &transition.training_dataset_sha256,
                        )
                        .is_ok()
                        && validate_sha256(
                            "queue model training checkpoint",
                            &transition.training_checkpoint_sha256,
                        )
                        .is_ok()
                        && transition.training_cutoff < transition.generated_at
                })
        } else {
            self.queue_model_transition.is_none()
        };
        let holdout_passed = self.holdout_evaluation.as_ref().is_some_and(|holdout| {
            holdout.schema_version == "funded_holdout_evaluation_v1"
                && holdout.exact_order_count == 100
                && holdout.label_sample_size >= 100
                && holdout.filled_order_count >= 10
                && holdout.non_filled_order_count >= 10
                && holdout.filled_order_count + holdout.non_filled_order_count == 100
                && holdout.brier_improvement_fraction >= Decimal::new(5, 2)
                && holdout.expected_calibration_error <= Decimal::new(10, 2)
                && holdout.markout_sample_size >= 10
                && holdout.markout_sample_size == holdout.filled_order_count
                && holdout.mean_net_markout_30s > Decimal::ZERO
                && holdout.net_markout_30s_lower_95 > Decimal::ZERO
                && holdout.holdout_net_pnl > Decimal::ZERO
                && holdout.mean_holdout_net_pnl_per_order
                    == (holdout.holdout_net_pnl / Decimal::from(100_u32)).round_dp(12)
                && holdout.holdout_net_pnl_per_order_lower_95
                    <= holdout.mean_holdout_net_pnl_per_order
                && holdout.holdout_max_drawdown >= Decimal::ZERO
                && holdout.holdout_max_drawdown <= self.maximum_drawdown
                && holdout.holdout_net_pnl_per_order_lower_95 > Decimal::ZERO
                && holdout.passed
        });
        let profitable_go_valid = self.phase != PromotionPhase::ProfitableGo
            || (self.terminal
                && self.active_stage_index == self.stage_targets.len() - 1
                && self.active_target_orders == 200
                && self.completed_checkpoints == vec![1, 5, 25, 100]
                && self.metrics.cumulative_funded_orders == 200
                && self.metrics.cumulative_eligible_orders == 200
                && self.metrics.cumulative_net_pnl > Decimal::ZERO
                && self.metrics.mean_net_markout_30s > Decimal::ZERO
                && self.metrics.net_markout_30s_lower_95 > Decimal::ZERO
                && self.metrics.markout_sample_size >= MIN_FUNDED_MARKOUT_SAMPLE_SIZE
                && self.metrics.data_quality_passed
                && self.metrics.cumulative_max_drawdown <= self.maximum_drawdown
                && self.metrics.unresolved_exposure == Decimal::ZERO
                && queue_transition_valid
                && self
                    .queue_model_transition
                    .as_ref()
                    .is_some_and(|transition| transition.model_quality_passed)
                && holdout_passed
                && self.consumed_grant_ids.len() == self.stage_targets.len()
                && !self.stage_authorized
                && !self.human_grant_required);
        let active_phase_valid = self.terminal
            || (self.phase
                == if self.active_stage_index == 0 {
                    PromotionPhase::EvidenceCollecting
                } else {
                    PromotionPhase::LimitedLive
                }
                && self.human_grant_required != self.stage_authorized);
        let valid = self.schema_version == "funded_ladder_state_v1"
            && self.stage_targets == FUNDED_LADDER_TARGETS
            && self.active_stage_index < self.stage_targets.len()
            && self.active_target_orders == self.stage_targets[self.active_stage_index]
            && self.maximum_calendar_days == 60
            && self.maximum_funded_orders == 200
            && !self.promotion_allowed
            && self.metrics.cumulative_funded_orders <= self.active_target_orders
            && self.metrics.cumulative_funded_orders <= self.metrics.cumulative_eligible_orders
            && self.completed_checkpoints == self.stage_targets[..self.active_stage_index].to_vec()
            && grants_consistent
            && terminal_binding_valid
            && checkpoint_1_bindings_valid
            && queue_transition_valid
            && profitable_go_valid
            && active_phase_valid
            && (!self.terminal
                || matches!(
                    self.phase,
                    PromotionPhase::ProfitableGo | PromotionPhase::StoppedNoGo
                ));
        if valid {
            Ok(())
        } else {
            Err(ResearchError::InvalidInput(
                "funded ladder state is internally inconsistent".to_owned(),
            ))
        }
    }
}

/// Atomically persists the ladder control state. Terminal states are
/// absorbing, and only same-stage or single-checkpoint transitions are
/// accepted, so replayed writers cannot skip or resurrect a campaign.
pub fn write_funded_ladder_state(
    path: &Path,
    proposed: &FundedLadderStateV1,
) -> Result<FundedLadderStateV1, ResearchError> {
    proposed.validate()?;
    if path.is_file() {
        let existing: FundedLadderStateV1 = read_json(path)?;
        existing.validate()?;
        if existing.terminal {
            return Ok(existing);
        }
        let valid_transition = proposed.campaign_id == existing.campaign_id
            && proposed.candidate == existing.candidate
            && (proposed.active_stage_index == existing.active_stage_index
                || proposed.active_stage_index == existing.active_stage_index + 1)
            && proposed.metrics.observed_calendar_days >= existing.metrics.observed_calendar_days
            && proposed.metrics.cumulative_eligible_orders
                >= existing.metrics.cumulative_eligible_orders
            && proposed.metrics.cumulative_funded_orders
                >= existing.metrics.cumulative_funded_orders
            && proposed
                .consumed_grant_ids
                .starts_with(&existing.consumed_grant_ids)
            && proposed.updated_at >= existing.updated_at;
        if !valid_transition {
            return Err(ResearchError::InvalidInput(
                "funded ladder durable transition regressed, skipped a checkpoint, or replayed state"
                    .to_owned(),
            ));
        }
    }
    replace_json(path, proposed)?;
    Ok(proposed.clone())
}

#[derive(Clone, Debug)]
pub struct AdvanceFundedLadderOptions {
    pub prior_state: PathBuf,
    pub prior_state_sha256: String,
    pub observation: PathBuf,
    pub observation_sha256: String,
    pub grant: Option<PathBuf>,
    pub grant_sha256: Option<String>,
    pub out: PathBuf,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedLadderTransitionResult {
    pub schema_version: String,
    pub prior_state_sha256: String,
    pub observation_sha256: String,
    pub grant_sha256: Option<String>,
    pub resulting_state_sha256: String,
    pub state: FundedLadderStateV1,
}

pub fn advance_funded_ladder(
    _options: AdvanceFundedLadderOptions,
) -> Result<FundedLadderTransitionResult, ResearchError> {
    Err(ResearchError::InvalidInput(
        "standalone funded metrics advancement is disabled; use advance-funded-manifest with controller-produced immutable protocol-v3 checkpoint evidence"
            .to_owned(),
    ))
}

#[derive(Clone, Debug)]
pub struct AdvanceFundedManifestOptions {
    pub prior_manifest: PathBuf,
    pub prior_manifest_sha256: String,
    pub observation: PathBuf,
    pub observation_sha256: String,
    pub grant: Option<PathBuf>,
    pub grant_sha256: Option<String>,
    pub next_execution_model: Option<PathBuf>,
    pub next_execution_model_blob_uri: Option<String>,
    pub next_execution_model_sha256: Option<String>,
    pub out: PathBuf,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct StopFundedManifestFromStageBlockOptions {
    pub prior_manifest: PathBuf,
    pub prior_manifest_sha256: String,
    pub stage_block: PathBuf,
    pub stage_block_sha256: String,
    pub out: PathBuf,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ExpireFundedManifestOptions {
    pub prior_manifest: PathBuf,
    pub prior_manifest_sha256: String,
    pub out: PathBuf,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedStageBlockV1 {
    pub schema: String,
    pub grant_id: String,
    pub campaign_id: String,
    pub campaign_control_id: String,
    pub candidate: CandidateIdentity,
    pub stage_target_orders: u32,
    pub source_manifest_sha256: String,
    pub source_state_sha256: String,
    pub decision_id: String,
    pub child_run_id: Option<String>,
    pub reason: String,
    pub blocked_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct InitializeFundedManifestOptions {
    pub shadow_manifest: PathBuf,
    pub shadow_manifest_sha256: String,
    pub canary_evidence: PathBuf,
    pub canary_evidence_blob_name: String,
    pub canary_evidence_sha256: String,
    pub human_grant_consumption: PathBuf,
    pub human_grant_consumption_sha256: String,
    pub terminal_evidence: PathBuf,
    pub terminal_evidence_blob_name: String,
    pub terminal_evidence_sha256: String,
    pub out: PathBuf,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedManifestTransitionResult {
    pub schema_version: String,
    pub prior_manifest_sha256: String,
    pub observation_sha256: String,
    pub grant_sha256: Option<String>,
    pub resulting_manifest_sha256: String,
    pub manifest: PromotionManifestV1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedStageBlockTransitionResult {
    pub schema_version: String,
    pub prior_manifest_sha256: String,
    pub stage_block_sha256: String,
    pub resulting_manifest_sha256: String,
    pub manifest: PromotionManifestV1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FundedExpirationTransitionResult {
    pub schema_version: String,
    pub prior_manifest_sha256: String,
    pub resulting_manifest_sha256: String,
    pub manifest: PromotionManifestV1,
}

/// Expire an exact active funded campaign into absorbing stopped_no_go. This
/// credential-free transition has no evidence, grant, or order inputs.
pub fn expire_funded_manifest(
    options: ExpireFundedManifestOptions,
) -> Result<FundedExpirationTransitionResult, ResearchError> {
    let prior_bytes = read_exact_artifact(
        &options.prior_manifest,
        &options.prior_manifest_sha256,
        "prior promotion manifest",
    )?;
    let mut manifest: PromotionManifestV1 = serde_json::from_slice(&prior_bytes)?;
    let ladder = manifest.funded_ladder.as_ref().ok_or_else(|| {
        ResearchError::InvalidInput(
            "expiration transition requires canonical funded ladder state".to_owned(),
        )
    })?;
    ladder.validate()?;
    if ladder.terminal
        || !matches!(
            manifest.phase,
            PromotionPhase::EvidenceCollecting | PromotionPhase::LimitedLive
        )
        || manifest.phase != ladder.phase
        || manifest.candidate != ladder.candidate
        || manifest.promotion_allowed
        || !manifest.human_authorization_required
        || options.now < manifest.expires_at
        || options.now < ladder.updated_at
    {
        return Err(ResearchError::InvalidInput(
            "funded expiration requires an exact expired active non-executable campaign".to_owned(),
        ));
    }
    let mut stopped = ladder.clone();
    stopped.stop_terminal();
    stopped.updated_at = options.now;
    stopped.validate()?;
    manifest.phase = PromotionPhase::StoppedNoGo;
    manifest.funded_ladder = Some(stopped);
    manifest.promotion_allowed = false;
    write_promotion_manifest(&options.out, &manifest)?;
    let bytes = fs::read(&options.out)?;
    Ok(FundedExpirationTransitionResult {
        schema_version: "funded_expiration_transition_v1".to_owned(),
        prior_manifest_sha256: normalize_sha256(&options.prior_manifest_sha256)?,
        resulting_manifest_sha256: format!("sha256:{}", sha256_bytes(&bytes)),
        manifest,
    })
}

/// Consume an immutable, exact-hash funded stage block and force the canonical
/// campaign into its absorbing non-executable terminal state. This transition
/// has no grant or order-authorizing input and can only remove authorization.
pub fn stop_funded_manifest_from_stage_block(
    options: StopFundedManifestFromStageBlockOptions,
) -> Result<FundedStageBlockTransitionResult, ResearchError> {
    let prior_bytes = read_exact_artifact(
        &options.prior_manifest,
        &options.prior_manifest_sha256,
        "prior promotion manifest",
    )?;
    let block_bytes = read_exact_artifact(
        &options.stage_block,
        &options.stage_block_sha256,
        "funded stage block",
    )?;
    let mut manifest: PromotionManifestV1 = serde_json::from_slice(&prior_bytes)?;
    #[derive(Deserialize)]
    struct ExactPriorState {
        funded_ladder: Box<serde_json::value::RawValue>,
    }
    let exact_prior: ExactPriorState = serde_json::from_slice(&prior_bytes)?;
    let exact_state_hash = format!(
        "sha256:{}",
        sha256_bytes(&compact_json_tokens(
            exact_prior.funded_ladder.get().as_bytes()
        ))
    );
    let block: FundedStageBlockV1 = serde_json::from_slice(&block_bytes)?;
    let ladder = manifest.funded_ladder.as_ref().ok_or_else(|| {
        ResearchError::InvalidInput(
            "stage-block transition requires canonical funded ladder state".to_owned(),
        )
    })?;
    ladder.validate()?;
    let prior_hash = normalize_sha256(&options.prior_manifest_sha256)?;
    let block_hash = normalize_sha256(&options.stage_block_sha256)?;
    let expected_campaign_control_id = sha256_bytes(ladder.campaign_id.as_bytes());
    let valid = block.schema == "polyedge.funded_stage_block.v1"
        && !ladder.terminal
        && manifest.phase == PromotionPhase::LimitedLive
        && manifest.phase == ladder.phase
        && !manifest.promotion_allowed
        && manifest.human_authorization_required
        && ladder.stage_authorized
        && !ladder.human_grant_required
        && block.campaign_id == ladder.campaign_id
        && block.campaign_control_id == expected_campaign_control_id
        && block.candidate == manifest.candidate
        && block.candidate == ladder.candidate
        && block.stage_target_orders == ladder.active_target_orders
        && ladder.consumed_grant_ids.last() == Some(&block.grant_id)
        && normalize_sha256(&block.source_manifest_sha256)? == prior_hash
        && normalize_sha256(&block.source_state_sha256)? == exact_state_hash
        && !block.decision_id.trim().is_empty()
        && !block.reason.trim().is_empty()
        && block
            .child_run_id
            .as_ref()
            .is_none_or(|run_id| !run_id.trim().is_empty())
        && block.blocked_at >= ladder.updated_at
        && block.blocked_at <= options.now;
    if !valid {
        return Err(ResearchError::InvalidInput(
            "funded stage block is not exact campaign/candidate/state-bound active-stage evidence"
                .to_owned(),
        ));
    }
    let mut stopped = ladder.clone();
    stopped.stop_terminal();
    stopped.updated_at = options.now;
    stopped.validate()?;
    manifest.phase = PromotionPhase::StoppedNoGo;
    manifest.funded_ladder = Some(stopped);
    manifest.promotion_allowed = false;
    manifest.created_at = options.now;
    if manifest.expires_at <= options.now {
        return Err(ResearchError::InvalidInput(
            "cannot publish a stage-block transition for an expired canonical campaign".to_owned(),
        ));
    }
    write_promotion_manifest(&options.out, &manifest)?;
    let bytes = fs::read(&options.out)?;
    Ok(FundedStageBlockTransitionResult {
        schema_version: "funded_stage_block_transition_v1".to_owned(),
        prior_manifest_sha256: prior_hash,
        stage_block_sha256: block_hash,
        resulting_manifest_sha256: format!("sha256:{}", sha256_bytes(&bytes)),
        manifest,
    })
}

/// `JSON.stringify` and pretty JSON preserve object-key/token order but differ
/// in insignificant whitespace. Compact the exact raw state tokens so Rust can
/// validate the Node controller's hash without reserializing timestamps.
fn compact_json_tokens(bytes: &[u8]) -> Vec<u8> {
    let mut compact = Vec::with_capacity(bytes.len());
    let mut in_string = false;
    let mut escaped = false;
    for &byte in bytes {
        if in_string {
            compact.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
            compact.push(byte);
        } else if !byte.is_ascii_whitespace() {
            compact.push(byte);
        }
    }
    compact
}

pub fn initialize_funded_manifest_after_canary(
    options: InitializeFundedManifestOptions,
) -> Result<FundedManifestTransitionResult, ResearchError> {
    let manifest_bytes = read_exact_artifact(
        &options.shadow_manifest,
        &options.shadow_manifest_sha256,
        "passed-shadow promotion manifest",
    )?;
    let evidence_bytes = read_exact_artifact(
        &options.canary_evidence,
        &options.canary_evidence_sha256,
        "protocol-v3 canary checkpoint evidence",
    )?;
    let consumption_bytes = read_exact_artifact(
        &options.human_grant_consumption,
        &options.human_grant_consumption_sha256,
        "consumed canary human grant",
    )?;
    let terminal_bytes = read_exact_artifact(
        &options.terminal_evidence,
        &options.terminal_evidence_sha256,
        "terminal canary risk/portfolio evidence",
    )?;
    let mut manifest: PromotionManifestV1 = serde_json::from_slice(&manifest_bytes)?;
    if manifest.phase != PromotionPhase::ShadowPassed
        || manifest.gate_metrics.phase != PromotionPhase::ShadowPassed
        || !manifest.gate_metrics.promotion_allowed
        || manifest.promotion_allowed
        || manifest.funded_ladder.is_some()
    {
        return Err(ResearchError::InvalidInput(
            "funded ladder can only initialize once from a non-executable passed-shadow manifest"
                .to_owned(),
        ));
    }
    let evidence: serde_json::Value = serde_json::from_slice(&evidence_bytes)?;
    let consumption: serde_json::Value = serde_json::from_slice(&consumption_bytes)?;
    let terminal: serde_json::Value = serde_json::from_slice(&terminal_bytes)?;
    let terminal_binding = ImmutableArtifactBindingV1 {
        blob_name: options.terminal_evidence_blob_name.clone(),
        sha256: options.terminal_evidence_sha256.clone(),
    };
    let protocol_v3_binding = ImmutableArtifactBindingV1 {
        blob_name: options.canary_evidence_blob_name.clone(),
        sha256: options.canary_evidence_sha256.clone(),
    };
    let (metrics, human_grant_id) = derive_canary_checkpoint_metrics(
        &manifest.candidate,
        &manifest.execution_model,
        &evidence,
        &consumption,
        &options.human_grant_consumption_sha256,
        &terminal,
        &terminal_binding,
    )?;
    let ladder = FundedLadderStateV1::initialize_after_canary(
        manifest.candidate.clone(),
        metrics,
        &human_grant_id,
        protocol_v3_binding,
        terminal_binding,
        options.now,
    )?;
    if manifest.expires_at <= options.now {
        return Err(ResearchError::InvalidInput(
            "cannot initialize funded ladder from an expired shadow manifest".to_owned(),
        ));
    }
    manifest.phase = ladder.phase;
    manifest.funded_ladder = Some(ladder);
    manifest.created_at = options.now;
    // The funded canonical control copy is independently durable for the
    // bounded campaign; the read-only latest shadow gate is rechecked by the
    // controller before every order.
    manifest.expires_at = options.now + Duration::days(60);
    write_promotion_manifest(&options.out, &manifest)?;
    let bytes = fs::read(&options.out)?;
    Ok(FundedManifestTransitionResult {
        schema_version: "funded_manifest_transition_v1".to_owned(),
        prior_manifest_sha256: normalize_sha256(&options.shadow_manifest_sha256)?,
        observation_sha256: normalize_sha256(&options.canary_evidence_sha256)?,
        grant_sha256: Some(normalize_sha256(&options.human_grant_consumption_sha256)?),
        resulting_manifest_sha256: format!("sha256:{}", sha256_bytes(&bytes)),
        manifest,
    })
}

fn derive_canary_checkpoint_metrics(
    candidate: &CandidateIdentity,
    execution_model: &ExecutionModelBinding,
    summary: &serde_json::Value,
    consumption: &serde_json::Value,
    consumption_sha256: &str,
    terminal: &serde_json::Value,
    terminal_binding: &ImmutableArtifactBindingV1,
) -> Result<(FundedLadderMetrics, String), ResearchError> {
    let fail = |message: &str| {
        ResearchError::InvalidInput(format!("protocol-v3 canary provenance rejected: {message}"))
    };
    let validated =
        validate_protocol_v3_order_evidence(candidate, summary, terminal, terminal_binding)?;
    if execution_model.model_version != "conservative-execution-prior-v1"
        || summary["prediction_model"]["blob_uri"].as_str()
            != Some(execution_model.blob_uri.as_str())
        || summary["prediction_model"]["sha256"].as_str() != Some(execution_model.sha256.as_str())
        || summary["prediction_model"]["model_version"].as_str()
            != Some(execution_model.model_version.as_str())
    {
        return Err(fail(
            "checkpoint 1 must use the exact passed-shadow conservative prior artifact",
        ));
    }
    if summary["schema_version"].as_u64() != Some(3)
        || summary["evidence_protocol_version"].as_u64() != Some(3)
        || summary["status"].as_str() != Some("completed")
        || summary["order_submission_attempted"].as_bool() != Some(true)
        || summary["order_submitted"].as_bool() != Some(true)
        || summary["submitted_order_count"].as_u64() != Some(1)
        || summary["completed_probe_count"].as_u64() != Some(1)
    {
        return Err(fail(
            "summary is not exactly one completed submitted protocol-v3 canary",
        ));
    }
    if summary["candidate"]["name"].as_str() != Some(candidate.name.as_str())
        || summary["candidate"]["candidate_version"].as_str()
            != Some(candidate.candidate_version.as_str())
        || summary["candidate"]["config_hash"].as_str() != Some(candidate.config_hash.as_str())
    {
        return Err(fail(
            "candidate identity does not match the passed-shadow manifest",
        ));
    }
    let probes = summary["probes"]
        .as_array()
        .ok_or_else(|| fail("probes array is missing"))?;
    if probes.len() != 1 || probes[0]["order_submitted"].as_bool() != Some(true) {
        return Err(fail("exactly one submitted probe is required"));
    }
    let probe = &probes[0];
    let lifecycle = &probe["lifecycle"];
    if lifecycle["reconciliation_complete"].as_bool() != Some(true)
        || lifecycle["zero_open_orders_confirmed"].as_bool() != Some(true)
        || lifecycle["data_gap_detected"].as_bool() != Some(false)
        || lifecycle["cancellation_failure"].as_bool() != Some(false)
    {
        return Err(fail(
            "lifecycle is not reconciled, globally zero-open, and data-gap free",
        ));
    }
    let observations = probe["model_observations"]
        .as_array()
        .ok_or_else(|| fail("model_observations are missing"))?;
    if !observations.iter().any(|row| {
        row["eligible"].as_bool() == Some(true)
            && row["quality_eligible"].as_bool() == Some(true)
            && row["reconciliation_complete"].as_bool() == Some(true)
            && row["zero_open_orders_confirmed"].as_bool() == Some(true)
    }) {
        return Err(fail("no eligible reconciled model observation exists"));
    }
    let related_ids = lifecycle["related_trade_ids"]
        .as_array()
        .ok_or_else(|| fail("related_trade_ids are missing"))?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    let matched = json_decimal(&lifecycle["actual_matched_size"])?;
    let markouts = probe["markouts"]
        .as_array()
        .ok_or_else(|| fail("top-level markouts are missing"))?;
    if matched > Decimal::ZERO && related_ids.is_empty() {
        return Err(fail(
            "a filled canary requires authenticated related trade IDs",
        ));
    }
    for fill_id in &related_ids {
        for horizon in [1_u64, 5, 30] {
            let row = markouts.iter().find(|row| {
                row["fill_id"].as_str() == Some(*fill_id)
                    && row["horizon_seconds"].as_u64() == Some(horizon)
            });
            let Some(row) = row else {
                return Err(fail(
                    "every fill requires one 1/5/30-second markout triplet",
                ));
            };
            let delay = row["observation_delay_ms"]
                .as_i64()
                .ok_or_else(|| fail("markout delay is missing"))?;
            if !(0..=2_000).contains(&delay)
                || json_decimal(&row["fill_size"])? <= Decimal::ZERO
                || json_decimal(&row["midpoint"]).is_err()
                || json_decimal(&row["executable_price"]).is_err()
                || json_decimal(&row["executable_markout_per_share"]).is_err()
            {
                return Err(fail("markout values are incomplete or untimely"));
            }
        }
    }

    let provenance = &summary["provenance"];
    let exact_consumption_hash = normalize_sha256(consumption_sha256)?;
    if consumption["schema"].as_str() != Some("polyedge.strategy_canary_human_grant_consumption.v1")
        || consumption["grant_id"].as_str() != provenance["human_grant_id"].as_str()
        || consumption["consumption_blob_name"].as_str()
            != provenance["human_grant_consumption_blob_name"].as_str()
        || normalize_sha256(
            provenance["human_grant_consumption_sha256"]
                .as_str()
                .ok_or_else(|| fail("consumption SHA-256 is missing"))?,
        )? != exact_consumption_hash
        || consumption["selected_intent_blob_name"].as_str()
            != provenance["intent_blob_name"].as_str()
        || consumption["selected_intent_container_name"].as_str()
            != provenance["intent_container_name"].as_str()
        || consumption["selected_intent_sha256"].as_str() != provenance["intent_sha256"].as_str()
        || consumption["promotion_manifest_blob_name"].as_str()
            != provenance["promotion_manifest_blob_name"].as_str()
        || consumption["promotion_manifest_container_name"].as_str()
            != provenance["promotion_manifest_container_name"].as_str()
        || consumption["promotion_manifest_sha256"].as_str()
            != provenance["promotion_manifest_sha256"].as_str()
        || consumption["decision_id"].as_str() != provenance["decision_id"].as_str()
    {
        return Err(fail(
            "consumed human grant does not exactly bind the canary artifacts",
        ));
    }
    normalize_sha256(
        provenance["authorization_sha256"]
            .as_str()
            .ok_or_else(|| fail("authorization SHA-256 is missing"))?,
    )?;

    if terminal["schema"].as_str() != Some("polyedge.canary_terminal_risk_portfolio.v1")
        || terminal["producer"].as_str() != Some("polyedge_node_authenticated_risk_terminal")
        || terminal["run_id"].as_str() != summary["run_id"].as_str()
        || terminal["probe_id"].as_str() != probe["probe_id"].as_str()
        || terminal["order_id"].as_str() != lifecycle["order_id"].as_str()
        || terminal["settlement_verified"].as_bool() != Some(true)
        || terminal["portfolio_reconciled"].as_bool() != Some(true)
        || terminal["zero_open_orders_confirmed"].as_bool() != Some(true)
        || json_decimal(&terminal["unresolved_exposure"])? != Decimal::ZERO
        || json_decimal(&terminal["reconciliation_discrepancy"])? > Decimal::new(1, 2)
    {
        return Err(fail(
            "terminal risk/portfolio evidence is missing, unresolved, or mismatched",
        ));
    }
    let source = terminal["source"].as_str().unwrap_or_default();
    if source != "authenticated_no_fill"
        && (source != "polymarket_data_api_plus_onchain_redemption"
            || terminal["settlement_transaction_hash"]
                .as_str()
                .is_none_or(str::is_empty))
    {
        return Err(fail("terminal evidence source is not trusted"));
    }
    if source == "authenticated_no_fill" && matched != Decimal::ZERO {
        return Err(fail(
            "authenticated_no_fill terminal evidence cannot settle a fill",
        ));
    }
    let _baseline = json_decimal(&terminal["campaign_starting_equity"])?;
    let _cash_flows = json_decimal(&terminal["net_external_cash_flows"])?;
    let liquid = json_decimal(&terminal["liquid_collateral"])?;
    let positions = json_decimal(&terminal["summed_position_value"])?;
    let ending = json_decimal(&terminal["cash_flow_adjusted_ending_equity"])?;
    if (liquid + positions - ending).abs() > Decimal::new(1, 2) {
        return Err(fail("terminal equity components differ by more than $0.01"));
    }
    let maximum = json_decimal(&terminal["maximum_observed_equity"])?;
    let minimum = json_decimal(&terminal["minimum_observed_equity"])?;
    if maximum < minimum {
        return Err(fail("terminal equity extrema are invalid"));
    }
    let _legacy_single_order_markout_check = related_ids
        .iter()
        .filter_map(|fill_id| {
            markouts.iter().find(|row| {
                row["fill_id"].as_str() == Some(*fill_id)
                    && row["horizon_seconds"].as_u64() == Some(30)
            })
        })
        .map(|row| json_decimal(&row["executable_markout_per_share"]))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .min()
        .unwrap_or(Decimal::ZERO);
    let started = DateTime::parse_from_rfc3339(
        summary["started_ts"]
            .as_str()
            .ok_or_else(|| fail("summary started_ts is missing"))?,
    )
    .map_err(|_| fail("summary started_ts is invalid"))?;
    let observed = DateTime::parse_from_rfc3339(
        terminal["observed_at"]
            .as_str()
            .ok_or_else(|| fail("terminal observed_at is missing"))?,
    )
    .map_err(|_| fail("terminal observed_at is invalid"))?;
    if observed < started {
        return Err(fail("terminal evidence predates the canary"));
    }
    let days = (observed.date_naive() - started.date_naive()).num_days() + 1;
    Ok((
        FundedLadderMetrics {
            observed_calendar_days: u32::try_from(days).unwrap_or(u32::MAX),
            cumulative_eligible_orders: 1,
            cumulative_funded_orders: 1,
            cumulative_net_pnl: validated.cumulative_net_pnl,
            cumulative_max_drawdown: validated.terminal_drawdown,
            mean_net_markout_30s: validated.net_markout_30s.unwrap_or(Decimal::ZERO),
            net_markout_30s_lower_95: Decimal::ZERO,
            markout_sample_size: u32::from(validated.net_markout_30s.is_some()),
            data_quality_passed: true,
            unresolved_exposure: Decimal::ZERO,
        },
        consumption["grant_id"]
            .as_str()
            .ok_or_else(|| fail("consumed grant id is missing"))?
            .to_owned(),
    ))
}

fn json_decimal(value: &serde_json::Value) -> Result<Decimal, ResearchError> {
    let text = value
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| value.as_number().map(ToString::to_string))
        .ok_or_else(|| {
            ResearchError::InvalidInput("required decimal evidence is missing".to_owned())
        })?;
    text.parse::<Decimal>()
        .map_err(|_| ResearchError::InvalidInput(format!("invalid decimal evidence value: {text}")))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct QueueModelTrainingOrderV1 {
    run_id: String,
    probe_id: String,
    order_id: String,
    observed_at: String,
    summary_blob_name: String,
    summary_sha256: String,
}

fn validate_queue_model_transition(
    path: &Path,
    blob_uri: &str,
    expected_sha256: &str,
    observation_sha256: &str,
    checkpoint: &FundedCheckpointEvidenceV1,
) -> Result<QueueModelTransitionV1, ResearchError> {
    let bytes = read_exact_artifact(path, expected_sha256, "checkpoint-100 queue model")?;
    let model: serde_json::Value = serde_json::from_slice(&bytes)?;
    let normalized_model_hash = normalize_sha256(expected_sha256)?;
    let model_hex = normalized_model_hash.trim_start_matches("sha256:");
    let candidate: CandidateIdentity = serde_json::from_value(model["candidate"].clone())?;
    if model["schema"].as_str() != Some("polyedge.execution_queue_model.v1")
        || model["model_version"].as_str() != Some("queue-calibration-v1")
        || model["status"].as_str() != Some("trained_research_only")
        || model["evidence_protocol_version"].as_u64() != Some(3)
        || model["sample_size"].as_u64() != Some(100)
        || model["positive_fills"].as_u64().unwrap_or_default() < 10
        || model["negative_non_fills"].as_u64().unwrap_or_default() < 10
        || candidate != checkpoint.candidate
        || !blob_uri.starts_with("azure://")
        || !blob_uri.ends_with(&format!("/{model_hex}.json"))
    {
        return Err(ResearchError::InvalidInput(
            "checkpoint 100 queue model is not the exact calibrated content-addressed artifact"
                .to_owned(),
        ));
    }
    let checkpoint_hash = normalize_sha256(
        model["training_checkpoint"]["sha256"]
            .as_str()
            .ok_or_else(|| {
                ResearchError::InvalidInput(
                    "queue model training checkpoint SHA-256 is missing".to_owned(),
                )
            })?,
    )?;
    if checkpoint_hash != normalize_sha256(observation_sha256)? {
        return Err(ResearchError::InvalidInput(
            "queue model was not trained from this exact checkpoint-100 evidence artifact"
                .to_owned(),
        ));
    }
    let mut expected_orders = Vec::with_capacity(100);
    for binding in &checkpoint.protocol_v3_order_artifacts {
        let summary = load_bound_artifact(binding)?;
        let probe = summary["probes"]
            .as_array()
            .and_then(|probes| probes.first())
            .ok_or_else(|| {
                ResearchError::InvalidInput("queue training summary probe is missing".to_owned())
            })?;
        expected_orders.push(QueueModelTrainingOrderV1 {
            run_id: required_json_text(&summary, "/run_id")?,
            probe_id: required_json_text(probe, "/probe_id")?,
            order_id: required_json_text(&probe["lifecycle"], "/order_id")?,
            observed_at: required_json_text(probe, "/finished_ts")?,
            summary_blob_name: binding.blob_name.clone(),
            summary_sha256: normalize_sha256(&binding.sha256)?,
        });
    }
    expected_orders.sort_by(|left, right| {
        left.observed_at
            .cmp(&right.observed_at)
            .then_with(|| left.run_id.cmp(&right.run_id))
            .then_with(|| left.probe_id.cmp(&right.probe_id))
    });
    let actual_orders: Vec<QueueModelTrainingOrderV1> =
        serde_json::from_value(model["training_dataset"]["orders"].clone())?;
    let stated_dataset_hash = normalize_sha256(
        model["training_dataset"]["sha256"]
            .as_str()
            .ok_or_else(|| {
                ResearchError::InvalidInput("queue model dataset SHA-256 is missing".to_owned())
            })?,
    )?;
    let calculated_dataset_hash = format!(
        "sha256:{}",
        sha256_bytes(&serde_json::to_vec(&actual_orders)?)
    );
    if model["training_dataset"]["exact_order_count"].as_u64() != Some(100)
        || actual_orders != expected_orders
        || calculated_dataset_hash != stated_dataset_hash
    {
        return Err(ResearchError::InvalidInput(
            "queue model training dataset does not exactly bind orders 1-100 and their summary hashes"
                .to_owned(),
        ));
    }
    let generated_at =
        DateTime::parse_from_rfc3339(model["generated_at"].as_str().ok_or_else(|| {
            ResearchError::InvalidInput("model generated_at is missing".to_owned())
        })?)
        .map_err(|_| ResearchError::InvalidInput("model generated_at is invalid".to_owned()))?
        .with_timezone(&Utc);
    let training_cutoff =
        DateTime::parse_from_rfc3339(model["training_cutoff"].as_str().ok_or_else(|| {
            ResearchError::InvalidInput("model training_cutoff is missing".to_owned())
        })?)
        .map_err(|_| ResearchError::InvalidInput("model training_cutoff is invalid".to_owned()))?
        .with_timezone(&Utc);
    let latest_terminal = checkpoint
        .terminal_risk_portfolio_artifacts
        .iter()
        .map(load_bound_artifact)
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .filter_map(|terminal| terminal["observed_at"].as_str())
        .filter_map(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .max()
        .ok_or_else(|| {
            ResearchError::InvalidInput("checkpoint-100 terminal cutoff is missing".to_owned())
        })?;
    if training_cutoff > latest_terminal || training_cutoff >= generated_at {
        return Err(ResearchError::InvalidInput(
            "queue model temporal cutoff is after training evidence or generation time".to_owned(),
        ));
    }
    let binding = ExecutionModelBinding {
        blob_uri: blob_uri.to_owned(),
        sha256: normalized_model_hash,
        model_version: "queue-calibration-v1".to_owned(),
    };
    validate_execution_model_binding(&binding)?;
    let model_quality_passed = model["promotion_ready"].as_bool() == Some(true)
        && json_decimal(&model["brier_improvement_fraction"])? >= Decimal::new(5, 2)
        && json_decimal(&model["expected_calibration_error"])? <= Decimal::new(10, 2)
        && json_decimal(&model["net_executable_markout_30s_lower_confidence_bound_95"])?
            > Decimal::ZERO;
    Ok(QueueModelTransitionV1 {
        schema_version: "queue_model_transition_v1".to_owned(),
        binding,
        generated_at,
        training_cutoff,
        training_dataset_sha256: stated_dataset_hash,
        training_checkpoint_sha256: checkpoint_hash,
        model_quality_passed,
    })
}

fn load_execution_model_artifact(
    binding: &ExecutionModelBinding,
) -> Result<serde_json::Value, ResearchError> {
    let bytes = if Path::new(&binding.blob_uri).is_file() {
        fs::read(&binding.blob_uri)?
    } else if let Some(rest) = binding.blob_uri.strip_prefix("azure://") {
        let (uri_account, tail) = rest.split_once('/').ok_or_else(|| {
            ResearchError::InvalidInput("execution model Azure URI is invalid".to_owned())
        })?;
        let (container, blob_name) = tail.split_once('/').ok_or_else(|| {
            ResearchError::InvalidInput("execution model Azure URI is invalid".to_owned())
        })?;
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            ResearchError::InvalidInput(
                "execution model is remote and Azure storage is unconfigured".to_owned(),
            )
        })?;
        if uri_account != account {
            return Err(ResearchError::InvalidInput(
                "execution model Azure URI account does not match configured storage".to_owned(),
            ));
        }
        let client_id = std::env::var("AZURE_CLIENT_ID").ok();
        let mut client =
            AzureBlobClient::with_managed_identity(account, container.to_owned(), client_id);
        client.download_blob_bytes(blob_name).map_err(|error| {
            ResearchError::Azure(format!(
                "reading bound execution model {blob_name}: {error}"
            ))
        })?
    } else {
        return Err(ResearchError::InvalidInput(
            "execution model binding is neither a local artifact nor an Azure URI".to_owned(),
        ));
    };
    verify_exact_hash("bound execution model", &bytes, &binding.sha256)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn derive_funded_holdout_evaluation(
    checkpoint: &FundedCheckpointEvidenceV1,
    transition: &QueueModelTransitionV1,
    maximum_drawdown: Decimal,
) -> Result<FundedHoldoutEvaluationV1, ResearchError> {
    let model = load_execution_model_artifact(&transition.binding)?;
    let weights = model["weights"]
        .as_array()
        .ok_or_else(|| ResearchError::InvalidInput("queue model weights are missing".to_owned()))?
        .iter()
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                ResearchError::InvalidInput("queue model weight is invalid".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let means = model["normalization"]["means"]
        .as_array()
        .ok_or_else(|| ResearchError::InvalidInput("queue model means are missing".to_owned()))?
        .iter()
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                ResearchError::InvalidInput("queue model mean is invalid".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let scales = model["normalization"]["scales"]
        .as_array()
        .ok_or_else(|| ResearchError::InvalidInput("queue model scales are missing".to_owned()))?
        .iter()
        .map(|value| {
            value.as_f64().filter(|value| *value > 0.0).ok_or_else(|| {
                ResearchError::InvalidInput("queue model scale is invalid".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if weights.len() != 10 || means.len() != 10 || scales.len() != 10 {
        return Err(ResearchError::InvalidInput(
            "queue model feature dimensions are not the frozen 10-feature contract".to_owned(),
        ));
    }
    let mut orders = Vec::with_capacity(200);
    for (summary_binding, terminal_binding) in checkpoint
        .protocol_v3_order_artifacts
        .iter()
        .zip(&checkpoint.terminal_risk_portfolio_artifacts)
    {
        let summary = load_bound_artifact(summary_binding)?;
        let terminal = load_bound_artifact(terminal_binding)?;
        let validated = validate_protocol_v3_order_evidence(
            &checkpoint.candidate,
            &summary,
            &terminal,
            terminal_binding,
        )?;
        orders.push((validated.started_at, summary, validated));
    }
    orders.sort_by_key(|(started, _, _)| *started);
    if orders.len() != 200 {
        return Err(ResearchError::InvalidInput(
            "final holdout requires exactly 200 chronological funded orders".to_owned(),
        ));
    }
    let order_100_pnl = orders[99].2.cumulative_net_pnl;
    let holdout_net_pnl = (orders[199].2.cumulative_net_pnl - order_100_pnl).round_dp(12);
    let mut prior_pnl = order_100_pnl;
    let mut holdout_peak = order_100_pnl;
    let mut holdout_max_drawdown = Decimal::ZERO;
    let mut holdout_order_pnls = Vec::with_capacity(100);
    for (_, _, validated) in orders.iter().skip(100) {
        let cumulative = validated.cumulative_net_pnl;
        holdout_order_pnls.push((cumulative - prior_pnl).round_dp(12));
        prior_pnl = cumulative;
        holdout_peak = holdout_peak.max(cumulative);
        holdout_max_drawdown = holdout_max_drawdown.max(holdout_peak - cumulative);
    }
    holdout_max_drawdown = holdout_max_drawdown.round_dp(12);
    let mean_holdout_net_pnl_per_order =
        (holdout_order_pnls.iter().copied().sum::<Decimal>() / Decimal::from(100_u32)).round_dp(12);
    let holdout_net_pnl_per_order_lower_95 = decimal_lower_95(&holdout_order_pnls).round_dp(12);
    let mut predictions = Vec::new();
    let mut actuals = Vec::new();
    let mut naive = Vec::new();
    let mut markouts = Vec::new();
    let mut filled_orders = 0_u32;
    for (started_at, summary, validated) in orders.into_iter().skip(100) {
        let prediction_model = &summary["prediction_model"];
        if prediction_model["blob_uri"].as_str() != Some(transition.binding.blob_uri.as_str())
            || normalize_sha256(prediction_model["sha256"].as_str().ok_or_else(|| {
                ResearchError::InvalidInput(
                    "holdout prediction model SHA-256 is missing".to_owned(),
                )
            })?)?
                != transition.binding.sha256
            || prediction_model["model_version"].as_str() != Some("queue-calibration-v1")
            || started_at <= transition.generated_at
        {
            return Err(ResearchError::InvalidInput(
                "orders 101-200 were not predicted by the exact frozen checkpoint-100 model"
                    .to_owned(),
            ));
        }
        let probe = summary["probes"]
            .as_array()
            .and_then(|probes| probes.first())
            .ok_or_else(|| ResearchError::InvalidInput("holdout probe is missing".to_owned()))?;
        if let Some(net_markout_30s) = validated.net_markout_30s {
            filled_orders += 1;
            markouts.push(net_markout_30s);
        }
        for row in probe["model_observations"].as_array().ok_or_else(|| {
            ResearchError::InvalidInput("holdout model observations are missing".to_owned())
        })? {
            if row["eligible"].as_bool() != Some(true)
                || row["quality_eligible"].as_bool() != Some(true)
            {
                return Err(ResearchError::InvalidInput(
                    "orders 101-200 contain an ineligible model label".to_owned(),
                ));
            }
            let horizon = row["horizon_seconds"].as_u64().ok_or_else(|| {
                ResearchError::InvalidInput("holdout horizon is missing".to_owned())
            })?;
            let raw = [
                1.0,
                (1.0 + json_f64(row, "inferred_size_ahead")?).ln(),
                json_f64(row, "spread")?,
                json_f64(row, "order_price")?,
                json_f64(row, "order_size")?,
                (1.0 + json_f64(row, "time_to_expiry_seconds")?).ln(),
                (1.0 + json_f64(row, "pre_send_trade_size")?).ln(),
                json_f64(row, "pre_send_depth_changes")?,
                json_f64(row, "pre_send_volatility")?,
                horizon as f64,
            ];
            let linear = weights
                .iter()
                .enumerate()
                .map(|(index, weight)| {
                    weight
                        * if index == 0 {
                            1.0
                        } else {
                            (raw[index] - means[index]) / scales[index]
                        }
                })
                .sum::<f64>();
            predictions.push(1.0 / (1.0 + (-linear).exp()));
            actuals.push(if row["filled"].as_bool() == Some(true) {
                1.0
            } else {
                0.0
            });
            naive.push(
                model["training_horizon_base_rates"][horizon.to_string()]
                    .as_f64()
                    .ok_or_else(|| {
                        ResearchError::InvalidInput(
                            "training horizon base rate is missing".to_owned(),
                        )
                    })?,
            );
        }
    }
    let sample_size = predictions.len();
    if sample_size == 0 {
        return Err(ResearchError::InvalidInput(
            "holdout has no model labels".to_owned(),
        ));
    }
    let brier = predictions
        .iter()
        .zip(&actuals)
        .map(|(prediction, actual)| (prediction - actual).powi(2))
        .sum::<f64>()
        / sample_size as f64;
    let naive_brier = naive
        .iter()
        .zip(&actuals)
        .map(|(prediction, actual)| (prediction - actual).powi(2))
        .sum::<f64>()
        / sample_size as f64;
    let improvement = if naive_brier > 0.0 {
        (naive_brier - brier) / naive_brier
    } else {
        0.0
    };
    let ece = (0..10)
        .map(|bin| {
            let low = bin as f64 / 10.0;
            let high = (bin + 1) as f64 / 10.0;
            let indexes = predictions
                .iter()
                .enumerate()
                .filter(|(_, prediction)| {
                    **prediction >= low
                        && if bin == 9 {
                            **prediction <= high
                        } else {
                            **prediction < high
                        }
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if indexes.is_empty() {
                0.0
            } else {
                let predicted = indexes.iter().map(|index| predictions[*index]).sum::<f64>()
                    / indexes.len() as f64;
                let observed =
                    indexes.iter().map(|index| actuals[*index]).sum::<f64>() / indexes.len() as f64;
                indexes.len() as f64 / sample_size as f64 * (predicted - observed).abs()
            }
        })
        .sum::<f64>();
    let mean_markout = if markouts.is_empty() {
        Decimal::ZERO
    } else {
        markouts.iter().copied().sum::<Decimal>() / Decimal::from(markouts.len() as u32)
    };
    let lower_95 = decimal_lower_95(&markouts);
    let non_filled_orders = 100_u32.saturating_sub(filled_orders);
    let brier_decimal = Decimal::from_f64(brier).unwrap_or_default();
    let naive_decimal = Decimal::from_f64(naive_brier).unwrap_or_default();
    let improvement_decimal = Decimal::from_f64(improvement).unwrap_or_default();
    let ece_decimal = Decimal::from_f64(ece).unwrap_or(Decimal::ONE);
    let passed = filled_orders >= 10
        && non_filled_orders >= 10
        && improvement_decimal >= Decimal::new(5, 2)
        && ece_decimal <= Decimal::new(10, 2)
        && markouts.len() >= 10
        && mean_markout > Decimal::ZERO
        && lower_95 > Decimal::ZERO
        && holdout_net_pnl > Decimal::ZERO
        && holdout_max_drawdown <= maximum_drawdown
        && holdout_net_pnl_per_order_lower_95 > Decimal::ZERO;
    Ok(FundedHoldoutEvaluationV1 {
        schema_version: "funded_holdout_evaluation_v1".to_owned(),
        exact_order_count: 100,
        label_sample_size: sample_size as u32,
        filled_order_count: filled_orders,
        non_filled_order_count: non_filled_orders,
        brier_score: brier_decimal,
        naive_base_rate_brier_score: naive_decimal,
        brier_improvement_fraction: improvement_decimal,
        expected_calibration_error: ece_decimal,
        markout_sample_size: markouts.len() as u32,
        mean_net_markout_30s: mean_markout,
        net_markout_30s_lower_95: lower_95,
        holdout_net_pnl,
        holdout_max_drawdown,
        mean_holdout_net_pnl_per_order,
        holdout_net_pnl_per_order_lower_95,
        passed,
    })
}

fn json_f64(value: &serde_json::Value, field: &str) -> Result<f64, ResearchError> {
    value[field]
        .as_f64()
        .or_else(|| value[field].as_str().and_then(|text| text.parse().ok()))
        .filter(|number| number.is_finite() && *number >= 0.0)
        .ok_or_else(|| ResearchError::InvalidInput(format!("holdout feature {field} is invalid")))
}

fn decimal_lower_95(values: &[Decimal]) -> Decimal {
    if values.len() < 2 {
        return Decimal::ZERO;
    }
    let mean = values.iter().copied().sum::<Decimal>() / Decimal::from(values.len() as u32);
    let mean_f64 = mean.to_f64().unwrap_or_default();
    let variance = values
        .iter()
        .map(|value| {
            let delta = value.to_f64().unwrap_or_default() - mean_f64;
            delta * delta
        })
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Decimal::from_f64(mean_f64 - 1.96 * (variance / values.len() as f64).sqrt()).unwrap_or_default()
}

pub fn advance_funded_manifest(
    options: AdvanceFundedManifestOptions,
) -> Result<FundedManifestTransitionResult, ResearchError> {
    let prior_bytes = read_exact_artifact(
        &options.prior_manifest,
        &options.prior_manifest_sha256,
        "prior promotion manifest",
    )?;
    let observation_bytes = read_exact_artifact(
        &options.observation,
        &options.observation_sha256,
        "funded ladder observation",
    )?;
    let mut manifest: PromotionManifestV1 = serde_json::from_slice(&prior_bytes)?;
    let checkpoint: FundedCheckpointEvidenceV1 = serde_json::from_slice(&observation_bytes)?;
    let (grant, grant_hash) = match (&options.grant, &options.grant_sha256) {
        (Some(path), Some(expected)) => {
            let bytes = read_exact_artifact(path, expected, "funded ladder grant")?;
            (
                Some(serde_json::from_slice::<FundedStageGrantV1>(&bytes)?),
                Some(expected.clone()),
            )
        }
        (None, None) => (None, None),
        _ => {
            return Err(ResearchError::InvalidInput(
                "funded ladder grant path and hash must be provided together".to_owned(),
            ))
        }
    };
    let ladder = manifest.funded_ladder.as_ref().ok_or_else(|| {
        ResearchError::InvalidInput(
            "promotion manifest has no passed-shadow funded ladder state".to_owned(),
        )
    })?;
    let queue_model_transition = match (
        &options.next_execution_model,
        &options.next_execution_model_blob_uri,
        &options.next_execution_model_sha256,
    ) {
        (Some(path), Some(blob_uri), Some(expected_hash)) if ladder.active_target_orders == 100 => {
            Some(validate_queue_model_transition(
                path,
                blob_uri,
                expected_hash,
                &options.observation_sha256,
                &checkpoint,
            )?)
        }
        (None, None, None) if ladder.active_target_orders != 100 => None,
        (None, None, None) => {
            return Err(ResearchError::InvalidInput(
                "checkpoint 100 requires exact next execution model path, Azure URI, and SHA-256"
                    .to_owned(),
            ))
        }
        _ => {
            return Err(ResearchError::InvalidInput(
                "next execution model triple is incomplete or supplied outside checkpoint 100"
                    .to_owned(),
            ))
        }
    };
    let holdout_evaluation = if ladder.active_target_orders == 200 {
        Some(derive_funded_holdout_evaluation(
            &checkpoint,
            ladder.queue_model_transition.as_ref().ok_or_else(|| {
                ResearchError::InvalidInput(
                    "checkpoint 200 has no canonical checkpoint-100 queue model transition"
                        .to_owned(),
                )
            })?,
            ladder.maximum_drawdown,
        )?)
    } else {
        None
    };
    let observation = checkpoint.validated_metrics(ladder)?;
    let last_terminal = checkpoint
        .terminal_risk_portfolio_artifacts
        .last()
        .cloned()
        .ok_or_else(|| {
            ResearchError::InvalidInput(
                "canonical checkpoint has no latest terminal artifact".to_owned(),
            )
        })?;
    let next = ladder.transition_with_evidence(
        observation,
        grant.as_ref(),
        Some(last_terminal),
        queue_model_transition,
        holdout_evaluation,
        options.now,
    )?;
    if ladder.active_target_orders == 100 && next.active_target_orders == 200 {
        manifest.execution_model = next
            .queue_model_transition
            .as_ref()
            .expect("checkpoint-100 transition validated")
            .binding
            .clone();
    }
    manifest.phase = next.phase;
    manifest.funded_ladder = Some(next);
    manifest.created_at = options.now;
    if manifest.expires_at <= options.now {
        return Err(ResearchError::InvalidInput(
            "cannot advance an expired canonical promotion manifest".to_owned(),
        ));
    }
    write_promotion_manifest(&options.out, &manifest)?;
    let bytes = fs::read(&options.out)?;
    Ok(FundedManifestTransitionResult {
        schema_version: "funded_manifest_transition_v1".to_owned(),
        prior_manifest_sha256: normalize_sha256(&options.prior_manifest_sha256)?,
        observation_sha256: normalize_sha256(&options.observation_sha256)?,
        grant_sha256: grant_hash.map(|hash| normalize_sha256(&hash)).transpose()?,
        resulting_manifest_sha256: format!("sha256:{}", sha256_bytes(&bytes)),
        manifest,
    })
}

fn verify_exact_hash(label: &str, bytes: &[u8], expected: &str) -> Result<(), ResearchError> {
    let expected = normalize_sha256(expected)?;
    let actual = format!("sha256:{}", sha256_bytes(bytes));
    if actual == expected {
        Ok(())
    } else {
        Err(ResearchError::InvalidInput(format!(
            "{label} SHA-256 mismatch"
        )))
    }
}

fn normalize_sha256(value: &str) -> Result<String, ResearchError> {
    let hex = value.strip_prefix("sha256:").unwrap_or(value);
    if hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(format!("sha256:{}", hex.to_ascii_lowercase()))
    } else {
        Err(ResearchError::InvalidInput(
            "expected a strict SHA-256 value".to_owned(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateIdentity {
    pub name: String,
    pub candidate_version: String,
    pub config_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfitabilityMetrics {
    pub observed_calendar_days: u32,
    pub clean_days: u32,
    pub settled_markets: u64,
    pub wallet_constrained: bool,
    pub queue_conservative: bool,
    pub wallet_constrained_net_pnl: Decimal,
    pub wallet_constrained_ending_equity: Decimal,
    pub queue_conservative_net_pnl: Decimal,
    pub pnl_ci_95_low: Decimal,
    pub consecutive_positive_weekly_blocks: u32,
    pub max_drawdown: Decimal,
    pub drawdown_limit: Decimal,
    pub markout_30s_ci_low: Decimal,
    pub replay_runtime_parity: bool,
    pub decision_parity_rate: Decimal,
    pub execution_model_protocol_version: u32,
    pub execution_model_eligible_orders: u64,
    pub execution_model_filled_orders: u64,
    pub execution_model_non_filled_orders: u64,
    pub execution_model_brier_improvement: Decimal,
    pub execution_model_expected_calibration_error: Decimal,
    pub execution_model_promotion_ready: bool,
    pub execution_model_markout_30s_lower_95: Decimal,
    pub data_quality: DataQualitySummary,
    pub missing_metrics: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromotionThresholds {
    pub required_clean_days: u32,
    pub maximum_extension_days: u32,
    pub required_settled_markets: u64,
    pub maximum_extension_markets: u64,
    pub required_positive_weekly_blocks: u32,
    pub minimum_decision_parity_rate: Decimal,
    pub minimum_decision_grade_coverage: Decimal,
    pub maximum_modeled_drawdown: Decimal,
    pub maximum_out_of_order_event_rate: Decimal,
    pub execution_model_protocol_version: u32,
    pub minimum_execution_model_eligible_orders: u64,
    pub minimum_execution_model_filled_orders: u64,
    pub minimum_execution_model_non_filled_orders: u64,
    pub minimum_brier_improvement_over_base_rate: Decimal,
    pub maximum_expected_calibration_error: Decimal,
}

impl Default for PromotionThresholds {
    fn default() -> Self {
        Self {
            required_clean_days: 30,
            maximum_extension_days: 60,
            required_settled_markets: 1_000,
            maximum_extension_markets: 2_000,
            required_positive_weekly_blocks: 4,
            minimum_decision_parity_rate: Decimal::ONE,
            minimum_decision_grade_coverage: Decimal::new(95, 2),
            maximum_modeled_drawdown: Decimal::ONE,
            maximum_out_of_order_event_rate: Decimal::new(1, 4),
            execution_model_protocol_version: 3,
            minimum_execution_model_eligible_orders: 100,
            minimum_execution_model_filled_orders: 10,
            minimum_execution_model_non_filled_orders: 10,
            minimum_brier_improvement_over_base_rate: Decimal::new(5, 2),
            maximum_expected_calibration_error: Decimal::new(10, 2),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionModelBinding {
    pub blob_uri: String,
    pub sha256: String,
    pub model_version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromotionEvaluation {
    pub schema_version: u32,
    pub phase: PromotionPhase,
    pub promotion_allowed: bool,
    pub gates: Vec<GateOutcome>,
    pub metrics: ProfitabilityMetrics,
}

/// Durable, fail-closed handoff between research evidence and any later
/// execution control plane. Creating a manifest never authorizes trading;
/// a separate human-controlled system must deliberately set and audit that.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromotionManifestV1 {
    pub schema_version: String,
    pub candidate: CandidateIdentity,
    pub phase: PromotionPhase,
    pub gate_metrics: PromotionEvaluation,
    pub artifact_uris: BTreeMap<String, String>,
    pub execution_model: ExecutionModelBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funded_ladder: Option<FundedLadderStateV1>,
    pub human_authorization_required: bool,
    pub promotion_allowed: bool,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl PromotionManifestV1 {
    pub fn new(
        candidate: CandidateIdentity,
        gate_metrics: PromotionEvaluation,
        artifact_uris: BTreeMap<String, String>,
        execution_model: ExecutionModelBinding,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, ResearchError> {
        validate_candidate(&candidate)?;
        validate_execution_model_binding(&execution_model)?;
        if expires_at <= created_at {
            return Err(ResearchError::InvalidInput(
                "promotion manifest expiry must be after creation".to_owned(),
            ));
        }
        Ok(Self {
            schema_version: "promotion_manifest_v1".to_owned(),
            candidate,
            phase: gate_metrics.phase,
            gate_metrics,
            artifact_uris,
            execution_model,
            funded_ladder: None,
            human_authorization_required: true,
            promotion_allowed: false,
            created_at,
            expires_at,
        })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

/// Atomically writes a profitability snapshot. Callers should pass
/// `DEFAULT_PROFITABILITY_LATEST` for the canonical dashboard/API hook.
/// Invalid, expired, or internally inconsistent manifests never replace the
/// prior latest file.
pub fn write_promotion_manifest(
    path: &Path,
    manifest: &PromotionManifestV1,
) -> Result<(), ResearchError> {
    if path.is_file() {
        let existing: PromotionManifestV1 = read_json(path)?;
        if existing.phase == PromotionPhase::StoppedNoGo
            || existing
                .funded_ladder
                .as_ref()
                .is_some_and(|ladder| ladder.terminal)
        {
            return Ok(());
        }
    }
    validate_candidate(&manifest.candidate)?;
    validate_execution_model_binding(&manifest.execution_model)?;
    if manifest.schema_version != "promotion_manifest_v1" {
        return Err(ResearchError::InvalidInput(
            "unsupported promotion manifest schema version".to_owned(),
        ));
    }
    if let Some(ladder) = &manifest.funded_ladder {
        ladder.validate()?;
        if ladder.candidate != manifest.candidate
            || manifest.phase != ladder.phase
            || manifest.gate_metrics.phase != PromotionPhase::ShadowPassed
            || !manifest.gate_metrics.promotion_allowed
        {
            return Err(ResearchError::InvalidInput(
                "funded ladder, top-level phase, candidate, and passed shadow gates are inconsistent"
                    .to_owned(),
            ));
        }
    } else if manifest.phase != manifest.gate_metrics.phase {
        return Err(ResearchError::InvalidInput(
            "promotion phase must match gate evaluation phase before funded ladder creation"
                .to_owned(),
        ));
    }
    let terminal_expiry = manifest.phase == PromotionPhase::StoppedNoGo
        && manifest
            .funded_ladder
            .as_ref()
            .is_some_and(|ladder| ladder.terminal && ladder.phase == PromotionPhase::StoppedNoGo);
    if manifest.expires_at <= manifest.created_at
        || (manifest.is_expired(Utc::now()) && !terminal_expiry)
    {
        return Err(ResearchError::InvalidInput(
            "promotion manifest is expired or has an invalid validity window".to_owned(),
        ));
    }
    if !manifest.human_authorization_required {
        return Err(ResearchError::InvalidInput(
            "promotion manifests must require human authorization".to_owned(),
        ));
    }
    if manifest.promotion_allowed
        && (!manifest.gate_metrics.promotion_allowed
            || !matches!(
                manifest.phase,
                PromotionPhase::CanaryReady
                    | PromotionPhase::LimitedLive
                    | PromotionPhase::ProfitableGo
            ))
    {
        return Err(ResearchError::InvalidInput(
            "promotion cannot be allowed before all gates pass and an authorized phase is reached"
                .to_owned(),
        ));
    }
    replace_json(path, manifest)
}

impl PromotionEvaluation {
    pub fn evaluate_shadow(metrics: ProfitabilityMetrics) -> Self {
        Self::evaluate_shadow_with_thresholds(metrics, &PromotionThresholds::default())
    }

    pub fn evaluate_shadow_with_thresholds(
        metrics: ProfitabilityMetrics,
        thresholds: &PromotionThresholds,
    ) -> Self {
        let mut gates = vec![
            minimum_gate(
                "clean_days",
                metrics.clean_days,
                thresholds.required_clean_days,
            ),
            minimum_gate(
                "settled_markets",
                metrics.settled_markets,
                thresholds.required_settled_markets,
            ),
            bool_gate("wallet_constrained", metrics.wallet_constrained),
            bool_gate("queue_conservative", metrics.queue_conservative),
            positive_gate(
                "wallet_constrained_net_pnl",
                metrics.wallet_constrained_net_pnl,
            ),
            positive_gate(
                "queue_conservative_net_pnl",
                metrics.queue_conservative_net_pnl,
            ),
            positive_gate("pnl_ci_95_low", metrics.pnl_ci_95_low),
            minimum_gate(
                "consecutive_positive_weekly_blocks",
                metrics.consecutive_positive_weekly_blocks,
                thresholds.required_positive_weekly_blocks,
            ),
            maximum_gate(
                "max_drawdown",
                metrics.max_drawdown,
                metrics
                    .drawdown_limit
                    .min(thresholds.maximum_modeled_drawdown),
            ),
            positive_gate("markout_30s_ci_low", metrics.markout_30s_ci_low),
            minimum_gate(
                "decision_parity_rate",
                metrics.decision_parity_rate,
                thresholds.minimum_decision_parity_rate,
            ),
            bool_gate("replay_runtime_parity", metrics.replay_runtime_parity),
        ];
        gates.push(GateOutcome {
            gate: "required_metrics_present".to_owned(),
            status: if metrics.missing_metrics.is_empty() {
                GateStatus::Passed
            } else {
                GateStatus::Failed
            },
            actual: if metrics.missing_metrics.is_empty() {
                "all_present".to_owned()
            } else {
                metrics.missing_metrics.join(",")
            },
            required: "no missing metrics".to_owned(),
        });
        let data_quality_passes = metrics.data_quality.total_events > 0
            && metrics.data_quality.decision_grade_coverage
                >= thresholds.minimum_decision_grade_coverage
            && metrics.data_quality.fatal_issues.is_empty()
            && metrics.data_quality.event_time_ordering_restored
            && Decimal::from(metrics.data_quality.out_of_order_events)
                / Decimal::from(metrics.data_quality.total_events)
                <= thresholds.maximum_out_of_order_event_rate
            && metrics
                .data_quality
                .warnings
                .iter()
                .all(|warning| warning.severity == WarningSeverity::Informational);
        gates.push(GateOutcome {
            gate: "data_quality".to_owned(),
            status: if data_quality_passes {
                GateStatus::Passed
            } else {
                GateStatus::Failed
            },
            actual: format!(
                "coverage={}, fatal={}, blocking_warnings={}, out_of_order_rate={}, ordering_restored={}",
                metrics.data_quality.decision_grade_coverage,
                metrics.data_quality.fatal_issues.len(),
                metrics
                    .data_quality
                    .warnings
                    .iter()
                    .filter(|warning| warning.severity == WarningSeverity::Blocking)
                    .count(),
                Decimal::from(metrics.data_quality.out_of_order_events)
                    / Decimal::from(metrics.data_quality.total_events.max(1)),
                metrics.data_quality.event_time_ordering_restored
            ),
            required: format!(
                "coverage>=0.95, zero fatal issues, zero blocking warnings, restored ordering, out-of-order rate<={}",
                thresholds.maximum_out_of_order_event_rate
            ),
        });
        let promotion_allowed = gates.iter().all(|gate| gate.status == GateStatus::Passed);
        let extension_exhausted = metrics.observed_calendar_days
            >= thresholds.maximum_extension_days
            || metrics.settled_markets >= thresholds.maximum_extension_markets;
        let phase = if promotion_allowed {
            PromotionPhase::ShadowPassed
        } else if extension_exhausted {
            PromotionPhase::StoppedNoGo
        } else {
            PromotionPhase::ShadowCollecting
        };
        Self {
            schema_version: 1,
            phase,
            promotion_allowed,
            gates,
            metrics,
        }
    }
}

fn minimum_gate<T>(gate: &str, actual: T, required: T) -> GateOutcome
where
    T: Copy + PartialOrd + std::fmt::Display,
{
    GateOutcome {
        gate: gate.to_owned(),
        status: if actual >= required {
            GateStatus::Passed
        } else {
            GateStatus::Collecting
        },
        actual: actual.to_string(),
        required: format!(">={required}"),
    }
}

fn bool_gate(gate: &str, actual: bool) -> GateOutcome {
    GateOutcome {
        gate: gate.to_owned(),
        status: if actual {
            GateStatus::Passed
        } else {
            GateStatus::Failed
        },
        actual: actual.to_string(),
        required: "true".to_owned(),
    }
}

fn positive_gate(gate: &str, actual: Decimal) -> GateOutcome {
    GateOutcome {
        gate: gate.to_owned(),
        status: if actual > Decimal::ZERO {
            GateStatus::Passed
        } else {
            GateStatus::Failed
        },
        actual: actual.to_string(),
        required: ">0".to_owned(),
    }
}

fn maximum_gate(gate: &str, actual: Decimal, required: Decimal) -> GateOutcome {
    GateOutcome {
        gate: gate.to_owned(),
        status: if actual <= required {
            GateStatus::Passed
        } else {
            GateStatus::Failed
        },
        actual: actual.to_string(),
        required: format!("<={required}"),
    }
}

fn validate_component(field: &str, value: String) -> Result<String, ResearchError> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(ResearchError::InvalidInput(format!(
            "{field} must contain only ASCII letters, numbers, '-' or '_'"
        )));
    }
    Ok(value)
}

fn validate_candidate(candidate: &CandidateIdentity) -> Result<(), ResearchError> {
    if candidate.name.trim().is_empty()
        || candidate.candidate_version.trim().is_empty()
        || candidate.config_hash.trim().is_empty()
    {
        return Err(ResearchError::InvalidInput(
            "promotion candidate name, version, and config hash are required".to_owned(),
        ));
    }
    validate_sha256("promotion candidate config_hash", &candidate.config_hash)?;
    Ok(())
}

fn validate_execution_model_binding(binding: &ExecutionModelBinding) -> Result<(), ResearchError> {
    if binding.blob_uri.trim().is_empty() || binding.model_version.trim().is_empty() {
        return Err(ResearchError::InvalidInput(
            "execution model blob URI and model version are required".to_owned(),
        ));
    }
    validate_sha256("execution model sha256", &binding.sha256)
}

fn validate_relative_path(path: &Path) -> Result<PathBuf, ResearchError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
        || path.file_name().and_then(|name| name.to_str()) == Some(MANIFEST_FILE)
    {
        return Err(ResearchError::InvalidInput(format!(
            "artifact path must be relative, contained by the run, and not {MANIFEST_FILE}"
        )));
    }
    Ok(path.to_path_buf())
}

fn validate_sha256(field: &str, value: &str) -> Result<(), ResearchError> {
    let digest = value.strip_prefix("sha256:").unwrap_or(value);
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ResearchError::InvalidInput(format!(
            "{field} must be a lowercase 64-character SHA-256 digest, optionally prefixed by sha256:"
        )));
    }
    Ok(())
}

fn verify_artifacts(run_dir: &Path, manifest: &DailyRunManifest) -> Result<(), ResearchError> {
    for artifact in manifest.artifacts.values() {
        let path = run_dir.join(&artifact.relative_path);
        let bytes = fs::read(&path)?;
        if bytes.len() as u64 != artifact.bytes || sha256_bytes(&bytes) != artifact.sha256 {
            return Err(ResearchError::InvalidInput(format!(
                "artifact {} failed size or SHA-256 verification",
                artifact.name
            )));
        }
    }
    Ok(())
}

fn write_new_json<T: Serialize>(path: &Path, value: &T) -> Result<(), ResearchError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    super::maybe_publish_research_artifact(path)?;
    Ok(())
}

fn replace_json<T: Serialize>(path: &Path, value: &T) -> Result<(), ResearchError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!(
        "tmp-{}",
        Utc::now()
            .to_rfc3339_opts(SecondsFormat::Nanos, true)
            .replace([':', '.'], "-")
    ));
    let bytes = serde_json::to_vec_pretty(value)?;
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    fs::rename(&temporary, path)?;
    super::maybe_publish_research_artifact(path)?;
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, ResearchError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
