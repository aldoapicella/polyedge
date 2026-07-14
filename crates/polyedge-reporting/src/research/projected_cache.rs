use super::*;
use chrono::NaiveDate;
const PROJECTED_DAY_SCHEMA_VERSION: u32 = 2;
const PROJECTED_CAMPAIGN_SCHEMA_VERSION: u32 = 2;
const PROJECTED_CACHE_DOMAIN: &str = "polyedge.projected-day-cache.v2";
const PROJECTED_CAMPAIGN_DOMAIN: &str = "polyedge.projected-campaign-chain.v2";
pub const PROJECTED_CAMPAIGN_INDEX_FILE: &str = "campaign_index.json";
const PROJECTED_DAY_MANIFEST_FILE: &str = "projected_day_manifest.json";
const SHADOW_CORRECTION_SCHEMA_VERSION: u32 = 1;
const SHADOW_CORRECTION_ROOT: &str = "reports/research/shadow/corrections";
const SHADOW_CORRECTION_ACTIVE: &str = "active.json";
const SHADOW_CORRECTION_RUNS: &str = "runs";

#[derive(Clone, Debug)]
pub struct PublishProjectedDayOptions {
    pub normalized: PathBuf,
    pub date: NaiveDate,
    pub campaign_id: String,
    pub cache_root: String,
    pub out: PathBuf,
    pub require_azure_source: bool,
    pub expected_source_container: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MaterializeProjectedCampaignOptions {
    pub since: NaiveDate,
    pub through: NaiveDate,
    pub campaign_id: String,
    pub cache_root: String,
    pub out: PathBuf,
    pub manifest: PathBuf,
    pub require_azure_source: bool,
    pub expected_source_container: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BeginShadowCorrectionOptions {
    pub campaign_id: String,
    pub correction_id: String,
    pub from: NaiveDate,
    pub through: NaiveDate,
    pub reason: String,
    pub out: PathBuf,
}

#[derive(Clone, Debug)]
pub struct CompleteShadowCorrectionOptions {
    pub campaign_id: String,
    pub from: NaiveDate,
    pub through: NaiveDate,
    pub out: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShadowCorrectionState {
    pub schema_version: u32,
    pub campaign_id: String,
    pub correction_id: String,
    pub from: NaiveDate,
    pub through: NaiveDate,
    pub reason: String,
    pub status: String,
    pub builder_git_sha: Option<String>,
    pub started_at: String,
    pub completed_at: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ShadowCorrectionPointer {
    schema_version: u32,
    state_sha256: String,
    state_path: String,
    state: ShadowCorrectionState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedFileBinding {
    pub logical_name: String,
    pub relative_path: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedDayCanonical {
    pub domain: String,
    pub schema_version: u32,
    pub campaign_id: String,
    pub builder_git_sha: Option<String>,
    pub date: NaiveDate,
    pub event_time_start: String,
    pub event_time_end_exclusive: String,
    pub format: String,
    pub decision_grade_projection: bool,
    pub events: u64,
    pub input_events: u64,
    pub malformed_lines: u64,
    pub raw_source_inventory: RawSourceInventory,
    pub first_recorded_ts: String,
    pub last_recorded_ts: String,
    pub event_counts: BTreeMap<String, u64>,
    pub files: Vec<ProjectedFileBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedDayManifest {
    pub schema_version: u32,
    pub canonical_sha256: String,
    pub canonical: ProjectedDayCanonical,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ProjectedDayPointer {
    schema_version: u32,
    date: NaiveDate,
    canonical_sha256: String,
    manifest_path: String,
    manifest_sha256: String,
    #[serde(default)]
    supersedes_pointer_sha256: Option<String>,
}

#[derive(Clone, Debug)]
struct ProjectedDayPointerSnapshot {
    date: NaiveDate,
    path: String,
    bytes: Vec<u8>,
    pointer: ProjectedDayPointer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedCampaignSegment {
    pub date: NaiveDate,
    pub relative_path: String,
    pub day_canonical_sha256: String,
    pub day_manifest_sha256: String,
    pub raw_source_inventory_sha256: String,
    pub raw_source_kind: String,
    pub parent_chain_sha256: Option<String>,
    pub chain_sha256: String,
    pub events: u64,
    pub first_recorded_ts: String,
    pub last_recorded_ts: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectedCampaignIndex {
    pub schema_version: u32,
    pub campaign_id: String,
    pub since: NaiveDate,
    pub through: NaiveDate,
    pub cutoff_exclusive: String,
    pub source_policy: String,
    pub source_container: Option<String>,
    pub canonical_sha256: String,
    pub total_events: u64,
    pub segments: Vec<ProjectedCampaignSegment>,
}

pub fn run_publish_projected_day(
    options: PublishProjectedDayOptions,
) -> Result<Value, ResearchError> {
    ensure_sealed_utc_day(options.date, "projected cache publication date")?;
    validate_campaign_id(&options.campaign_id)?;
    let normalize_manifest_path = options.normalized.join("events_manifest.json");
    let normalize_manifest_bytes = fs::read(&normalize_manifest_path)?;
    let normalize_manifest: Value = serde_json::from_slice(&normalize_manifest_bytes)?;
    let canonical = build_day_canonical(
        &options.normalized,
        options.date,
        &options.campaign_id,
        &normalize_manifest,
    )?;
    validate_projected_day_source(
        &canonical,
        options.date,
        options.require_azure_source,
        options.expected_source_container.as_deref(),
    )?;
    let canonical_sha256 = sha256_prefixed(&canonical_bytes(&canonical)?);
    let manifest = ProjectedDayManifest {
        schema_version: PROJECTED_DAY_SCHEMA_VERSION,
        canonical_sha256: canonical_sha256.clone(),
        canonical,
    };
    validate_day_manifest(&manifest, options.date, &options.campaign_id)?;

    let mut store = ProjectedCacheStore::open(&options.cache_root)?;
    let run_prefix = format!(
        "days/{}/runs/{}/",
        options.date.format("%Y-%m-%d"),
        canonical_sha256.trim_start_matches("sha256:")
    );
    for file in &manifest.canonical.files {
        let bytes = fs::read(options.normalized.join(&file.relative_path))?;
        verify_binding_bytes(file, &bytes)?;
        store.put_immutable_verified(&format!("{run_prefix}{}", file.relative_path), &bytes)?;
    }

    // Manifest-last publication keeps partially uploaded runs invisible.
    let manifest_path = format!("{run_prefix}manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_bytes = with_trailing_newline(manifest_bytes);
    store.put_immutable_verified(&manifest_path, &manifest_bytes)?;
    let pointer_path = format!("days/{}/latest.json", options.date.format("%Y-%m-%d"));
    let prior_pointer_bytes = store.read_optional(&pointer_path)?;
    let prior_pointer_sha256 = prior_pointer_bytes.as_deref().map(sha256_prefixed);
    if let Some(prior_bytes) = &prior_pointer_bytes {
        let prior: ProjectedDayPointer = serde_json::from_slice(prior_bytes)?;
        if prior.date == options.date
            && prior.canonical_sha256 == canonical_sha256
            && prior.manifest_path == manifest_path
            && prior.manifest_sha256 == sha256_prefixed(&manifest_bytes)
        {
            write_pretty_json(&options.out, &manifest)?;
            return serde_json::to_value(&manifest).map_err(ResearchError::Json);
        }
    }
    let pointer = ProjectedDayPointer {
        schema_version: PROJECTED_DAY_SCHEMA_VERSION,
        date: options.date,
        canonical_sha256,
        manifest_path,
        manifest_sha256: sha256_prefixed(&manifest_bytes),
        supersedes_pointer_sha256: prior_pointer_sha256.clone(),
    };
    store.put_pointer_cas(
        &pointer_path,
        &with_trailing_newline(serde_json::to_vec_pretty(&pointer)?),
        prior_pointer_sha256.as_deref(),
    )?;
    write_pretty_json(&options.out, &manifest)?;
    serde_json::to_value(&manifest).map_err(ResearchError::Json)
}

pub fn run_materialize_projected_campaign(
    options: MaterializeProjectedCampaignOptions,
) -> Result<Value, ResearchError> {
    ensure_sealed_utc_day(options.through, "projected campaign through date")?;
    validate_campaign_id(&options.campaign_id)?;
    if options.since > options.through {
        return Err(ResearchError::InvalidInput(
            "projected campaign since date must not follow through date".to_owned(),
        ));
    }
    let mut store = ProjectedCacheStore::open(&options.cache_root)?;
    let pointer_snapshots = read_pointer_snapshots(&mut store, options.since, options.through)?;
    let staging = sibling_staging_path(&options.out);
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;

    let materialized = (|| {
        let mut segments = Vec::new();
        let mut parent_chain_sha256 = None::<String>;
        let mut total_events = 0_u64;
        for snapshot in &pointer_snapshots {
            let date = snapshot.date;
            let pointer = &snapshot.pointer;
            let manifest_bytes = store.read(&pointer.manifest_path)?;
            if sha256_prefixed(&manifest_bytes) != pointer.manifest_sha256 {
                return Err(ResearchError::InvalidInput(format!(
                    "projected cache manifest hash mismatch for {date}"
                )));
            }
            let manifest: ProjectedDayManifest = serde_json::from_slice(&manifest_bytes)?;
            validate_day_manifest(&manifest, date, &options.campaign_id)?;
            validate_projected_day_source(
                &manifest.canonical,
                date,
                options.require_azure_source,
                options.expected_source_container.as_deref(),
            )?;
            if manifest.canonical_sha256 != pointer.canonical_sha256 {
                return Err(ResearchError::InvalidInput(format!(
                    "projected cache canonical pointer mismatch for {date}"
                )));
            }

            let segment_relative = format!("segments/{}", date.format("%Y-%m-%d"));
            let segment_dir = staging.join(&segment_relative);
            fs::create_dir_all(&segment_dir)?;
            let run_prefix = pointer
                .manifest_path
                .strip_suffix("manifest.json")
                .ok_or_else(|| {
                    ResearchError::InvalidInput(format!(
                        "projected cache manifest path is invalid for {date}"
                    ))
                })?;
            for file in &manifest.canonical.files {
                let bytes = store.read(&format!("{run_prefix}{}", file.relative_path))?;
                verify_binding_bytes(file, &bytes)?;
                let destination = segment_dir.join(&file.relative_path);
                if let Some(parent) = destination.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(destination, bytes)?;
            }
            fs::write(
                segment_dir.join(PROJECTED_DAY_MANIFEST_FILE),
                &manifest_bytes,
            )?;
            write_pretty_json(
                &segment_dir.join("events_manifest.json"),
                &json!({
                    "schema_version": PROJECTED_DAY_SCHEMA_VERSION,
                    "canonical_sha256": manifest.canonical_sha256,
                    "format": manifest.canonical.format,
                    "decision_grade_projection": manifest.canonical.decision_grade_projection,
                    "events": manifest.canonical.events,
                    "raw_source_inventory_sha256": manifest.canonical.raw_source_inventory.canonical_sha256,
                    "first_recorded_ts": manifest.canonical.first_recorded_ts,
                    "last_recorded_ts": manifest.canonical.last_recorded_ts
                }),
            )?;

            let chain_sha256 = campaign_chain_hash(
                parent_chain_sha256.as_deref(),
                date,
                &manifest.canonical_sha256,
            );
            total_events = total_events
                .checked_add(manifest.canonical.events)
                .ok_or_else(|| {
                    ResearchError::InvalidInput("campaign event count overflow".to_owned())
                })?;
            segments.push(ProjectedCampaignSegment {
                date,
                relative_path: segment_relative,
                day_canonical_sha256: manifest.canonical_sha256,
                day_manifest_sha256: pointer.manifest_sha256.clone(),
                raw_source_inventory_sha256: manifest
                    .canonical
                    .raw_source_inventory
                    .canonical_sha256,
                raw_source_kind: manifest
                    .canonical
                    .raw_source_inventory
                    .canonical
                    .source_kind,
                parent_chain_sha256: parent_chain_sha256.clone(),
                chain_sha256: chain_sha256.clone(),
                events: manifest.canonical.events,
                first_recorded_ts: manifest.canonical.first_recorded_ts,
                last_recorded_ts: manifest.canonical.last_recorded_ts,
            });
            parent_chain_sha256 = Some(chain_sha256);
        }

        let index = ProjectedCampaignIndex {
            schema_version: PROJECTED_CAMPAIGN_SCHEMA_VERSION,
            campaign_id: options.campaign_id.clone(),
            since: options.since,
            through: options.through,
            cutoff_exclusive: day_end(options.through),
            source_policy: if options.require_azure_source {
                "exact_azure_blob_inventory_v1".to_owned()
            } else {
                "local_test_inventory_v1".to_owned()
            },
            source_container: options.expected_source_container.clone(),
            canonical_sha256: parent_chain_sha256.ok_or_else(|| {
                ResearchError::InvalidInput("projected campaign contains no segments".to_owned())
            })?,
            total_events,
            segments,
        };
        validate_campaign_index(&index, Some(&staging))?;
        verify_pointer_snapshots_current(&mut store, &pointer_snapshots)?;
        write_pretty_json(&staging.join(PROJECTED_CAMPAIGN_INDEX_FILE), &index)?;
        Ok::<ProjectedCampaignIndex, ResearchError>(index)
    })();

    let index = match materialized {
        Ok(index) => index,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
    };
    if options.out.exists() {
        fs::remove_dir_all(&options.out)?;
    }
    fs::rename(&staging, &options.out)?;
    write_pretty_json(&options.manifest, &index)?;
    serde_json::to_value(&index).map_err(ResearchError::Json)
}

pub fn run_begin_shadow_correction(
    options: BeginShadowCorrectionOptions,
) -> Result<Value, ResearchError> {
    let mut store = shadow_correction_store()?;
    begin_shadow_correction_with_store(options, &mut store)
}

fn begin_shadow_correction_with_store(
    options: BeginShadowCorrectionOptions,
    store: &mut ProjectedCacheStore,
) -> Result<Value, ResearchError> {
    validate_campaign_id(&options.campaign_id)?;
    validate_campaign_id(&options.correction_id)?;
    ensure_sealed_utc_day(options.through, "shadow correction through date")?;
    if options.from > options.through
        || options.reason.trim().is_empty()
        || options.reason.len() > 256
    {
        return Err(ResearchError::InvalidInput(
            "shadow correction range or reason is invalid".to_owned(),
        ));
    }
    let prior_bytes = store.read_optional(SHADOW_CORRECTION_ACTIVE)?;
    let prior_sha256 = prior_bytes.as_deref().map(sha256_prefixed);
    if let Some(prior_bytes) = &prior_bytes {
        let prior: ShadowCorrectionPointer = serde_json::from_slice(prior_bytes)?;
        validate_shadow_correction_pointer(&prior)?;
        if matches!(prior.state.status.as_str(), "in_progress" | "failed") {
            if prior.state.campaign_id == options.campaign_id
                && prior.state.from == options.from
                && prior.state.through == options.through
            {
                write_pretty_json(&options.out, &prior)?;
                return serde_json::to_value(prior).map_err(ResearchError::Json);
            }
            return Err(ResearchError::InvalidInput(format!(
                "shadow correction {} is still {}; resume or resolve it before starting another range",
                prior.state.correction_id, prior.state.status
            )));
        }
    }
    let state = ShadowCorrectionState {
        schema_version: SHADOW_CORRECTION_SCHEMA_VERSION,
        campaign_id: options.campaign_id,
        correction_id: options.correction_id,
        from: options.from,
        through: options.through,
        reason: options.reason,
        status: "in_progress".to_owned(),
        builder_git_sha: git_sha(),
        started_at: now_ts(),
        completed_at: None,
    };
    let pointer = correction_pointer(state)?;
    let state_bytes = with_trailing_newline(serde_json::to_vec_pretty(&pointer.state)?);
    store.put_immutable_verified(&pointer.state_path, &state_bytes)?;
    let pointer_bytes = with_trailing_newline(serde_json::to_vec_pretty(&pointer)?);
    store.put_pointer_cas(
        SHADOW_CORRECTION_ACTIVE,
        &pointer_bytes,
        prior_sha256.as_deref(),
    )?;
    write_pretty_json(&options.out, &pointer)?;
    serde_json::to_value(pointer).map_err(ResearchError::Json)
}

pub fn run_complete_shadow_correction(
    options: CompleteShadowCorrectionOptions,
) -> Result<Value, ResearchError> {
    let mut store = shadow_correction_store()?;
    complete_shadow_correction_with_store(options, &mut store)
}

fn complete_shadow_correction_with_store(
    options: CompleteShadowCorrectionOptions,
    store: &mut ProjectedCacheStore,
) -> Result<Value, ResearchError> {
    validate_campaign_id(&options.campaign_id)?;
    ensure_sealed_utc_day(options.through, "shadow correction through date")?;
    let prior_bytes = store
        .read_optional(SHADOW_CORRECTION_ACTIVE)?
        .ok_or_else(|| {
            ResearchError::InvalidInput(
                "shadow correction cannot complete without an active state".to_owned(),
            )
        })?;
    let prior_sha256 = sha256_prefixed(&prior_bytes);
    let prior: ShadowCorrectionPointer = serde_json::from_slice(&prior_bytes)?;
    validate_shadow_correction_pointer(&prior)?;
    if prior.state.campaign_id != options.campaign_id
        || prior.state.from != options.from
        || prior.state.through != options.through
        || prior.state.status != "in_progress"
    {
        return Err(ResearchError::InvalidInput(
            "shadow correction completion does not match the active in-progress range".to_owned(),
        ));
    }
    let mut state = prior.state;
    state.status = "complete".to_owned();
    state.completed_at = Some(now_ts());
    let pointer = correction_pointer(state)?;
    let state_bytes = with_trailing_newline(serde_json::to_vec_pretty(&pointer.state)?);
    store.put_immutable_verified(&pointer.state_path, &state_bytes)?;
    let pointer_bytes = with_trailing_newline(serde_json::to_vec_pretty(&pointer)?);
    store.put_pointer_cas(
        SHADOW_CORRECTION_ACTIVE,
        &pointer_bytes,
        Some(&prior_sha256),
    )?;
    write_pretty_json(&options.out, &pointer)?;
    serde_json::to_value(pointer).map_err(ResearchError::Json)
}

pub fn read_shadow_correction_state() -> Result<Option<ShadowCorrectionState>, ResearchError> {
    let mut store = shadow_correction_store()?;
    read_shadow_correction_state_from_store(&mut store)
}

fn read_shadow_correction_state_from_store(
    store: &mut ProjectedCacheStore,
) -> Result<Option<ShadowCorrectionState>, ResearchError> {
    let Some(bytes) = store.read_optional(SHADOW_CORRECTION_ACTIVE)? else {
        return Ok(None);
    };
    let pointer: ShadowCorrectionPointer = serde_json::from_slice(&bytes)?;
    validate_shadow_correction_pointer(&pointer)?;
    let state_bytes = store.read(&pointer.state_path)?;
    if sha256_prefixed(&canonical_bytes(&pointer.state)?) != pointer.state_sha256
        || serde_json::from_slice::<ShadowCorrectionState>(&state_bytes)? != pointer.state
    {
        return Err(ResearchError::InvalidInput(
            "shadow correction immutable state does not match its active pointer".to_owned(),
        ));
    }
    Ok(Some(pointer.state))
}

fn shadow_correction_store() -> Result<ProjectedCacheStore, ResearchError> {
    let root = match std::env::var("AZURE_STORAGE_ACCOUNT_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(account) => {
            let container = std::env::var("AZURE_RESEARCH_STORAGE_CONTAINER_NAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    std::env::var("AZURE_STORAGE_CONTAINER_NAME")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                })
                .ok_or_else(|| {
                    ResearchError::InvalidInput(
                        "AZURE_RESEARCH_STORAGE_CONTAINER_NAME or AZURE_STORAGE_CONTAINER_NAME is required for the shadow correction journal".to_owned(),
                    )
                })?;
            format!("azure://{account}/{container}/{SHADOW_CORRECTION_ROOT}")
        }
        None => SHADOW_CORRECTION_ROOT.to_owned(),
    };
    ProjectedCacheStore::open(&root)
}

fn correction_pointer(
    state: ShadowCorrectionState,
) -> Result<ShadowCorrectionPointer, ResearchError> {
    let state_bytes = canonical_bytes(&state)?;
    let state_sha256 = sha256_prefixed(&state_bytes);
    let pointer = ShadowCorrectionPointer {
        schema_version: SHADOW_CORRECTION_SCHEMA_VERSION,
        state_path: format!(
            "{SHADOW_CORRECTION_RUNS}/{}.json",
            state_sha256.trim_start_matches("sha256:")
        ),
        state_sha256,
        state,
    };
    validate_shadow_correction_pointer(&pointer)?;
    Ok(pointer)
}

fn validate_shadow_correction_pointer(
    pointer: &ShadowCorrectionPointer,
) -> Result<(), ResearchError> {
    if pointer.schema_version != SHADOW_CORRECTION_SCHEMA_VERSION
        || pointer.state.schema_version != SHADOW_CORRECTION_SCHEMA_VERSION
        || !matches!(
            pointer.state.status.as_str(),
            "in_progress" | "failed" | "complete"
        )
        || pointer.state.from > pointer.state.through
        || pointer.state.reason.trim().is_empty()
        || !valid_sha256(&pointer.state_sha256)
    {
        return Err(ResearchError::InvalidInput(
            "shadow correction pointer is invalid".to_owned(),
        ));
    }
    validate_campaign_id(&pointer.state.campaign_id)?;
    validate_campaign_id(&pointer.state.correction_id)?;
    validate_relative_cache_path(&pointer.state_path)?;
    let expected = sha256_prefixed(&canonical_bytes(&pointer.state)?);
    if pointer.state_sha256 != expected
        || pointer.state_path
            != format!(
                "{SHADOW_CORRECTION_RUNS}/{}.json",
                expected.trim_start_matches("sha256:")
            )
    {
        return Err(ResearchError::InvalidInput(
            "shadow correction state hash or path mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn read_pointer_snapshots(
    store: &mut ProjectedCacheStore,
    since: NaiveDate,
    through: NaiveDate,
) -> Result<Vec<ProjectedDayPointerSnapshot>, ResearchError> {
    let mut snapshots = Vec::new();
    let mut date = since;
    loop {
        let path = format!("days/{}/latest.json", date.format("%Y-%m-%d"));
        let bytes = store.read(&path).map_err(|error| {
            ResearchError::InvalidInput(format!(
                "projected cache is missing a complete day for {date}: {error}"
            ))
        })?;
        let pointer: ProjectedDayPointer = serde_json::from_slice(&bytes)?;
        if pointer.date != date || pointer.schema_version != PROJECTED_DAY_SCHEMA_VERSION {
            return Err(ResearchError::InvalidInput(format!(
                "projected cache pointer identity mismatch for {date}"
            )));
        }
        if pointer
            .supersedes_pointer_sha256
            .as_deref()
            .is_some_and(|value| !valid_sha256(value))
        {
            return Err(ResearchError::InvalidInput(format!(
                "projected cache correction lineage is invalid for {date}"
            )));
        }
        snapshots.push(ProjectedDayPointerSnapshot {
            date,
            path,
            bytes,
            pointer,
        });
        if date == through {
            break;
        }
        date = date.succ_opt().ok_or_else(|| {
            ResearchError::InvalidInput("projected campaign date overflow".to_owned())
        })?;
    }
    Ok(snapshots)
}

fn verify_pointer_snapshots_current(
    store: &mut ProjectedCacheStore,
    snapshots: &[ProjectedDayPointerSnapshot],
) -> Result<(), ResearchError> {
    for snapshot in snapshots {
        let current = store.read(&snapshot.path)?;
        if current != snapshot.bytes {
            return Err(ResearchError::InvalidInput(format!(
                "projected cache changed during campaign materialization at {}; retry under a stable campaign lease",
                snapshot.date
            )));
        }
    }
    Ok(())
}

pub(crate) fn load_campaign_index(
    root: &Path,
) -> Result<Option<ProjectedCampaignIndex>, ResearchError> {
    let path = root.join(PROJECTED_CAMPAIGN_INDEX_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let index: ProjectedCampaignIndex = serde_json::from_slice(&fs::read(path)?)?;
    validate_campaign_index(&index, Some(root))?;
    Ok(Some(index))
}

pub fn read_verified_campaign_index(path: &Path) -> Result<ProjectedCampaignIndex, ResearchError> {
    let index: ProjectedCampaignIndex = serde_json::from_slice(&fs::read(path)?)?;
    validate_campaign_index(&index, None)?;
    Ok(index)
}

fn build_day_canonical(
    normalized: &Path,
    date: NaiveDate,
    campaign_id: &str,
    manifest: &Value,
) -> Result<ProjectedDayCanonical, ResearchError> {
    if manifest["format"].as_str() != Some("jsonl-indexed-gzip-sharded")
        || manifest["decision_grade_projection"].as_bool() != Some(true)
    {
        return Err(ResearchError::InvalidInput(
            "projected cache requires decision-grade jsonl-indexed-gzip-sharded normalization"
                .to_owned(),
        ));
    }
    let events = required_u64(manifest, "events")?;
    if events == 0 {
        return Err(ResearchError::InvalidInput(
            "projected cache refuses an empty normalized day".to_owned(),
        ));
    }
    let first_recorded_ts = required_text(manifest, "first_recorded_ts")?;
    let last_recorded_ts = required_text(manifest, "last_recorded_ts")?;
    validate_day_bounds(date, &first_recorded_ts, &last_recorded_ts)?;
    let event_counts = value_u64_map(&manifest["event_counts"])?;
    let files = normalized_file_bindings(normalized, manifest)?;
    let file_rows = files.iter().map(|file| file.rows).sum::<u64>();
    if file_rows != events {
        return Err(ResearchError::InvalidInput(format!(
            "projected normalized row total {file_rows} does not match manifest events {events}"
        )));
    }
    Ok(ProjectedDayCanonical {
        domain: PROJECTED_CACHE_DOMAIN.to_owned(),
        schema_version: PROJECTED_DAY_SCHEMA_VERSION,
        campaign_id: campaign_id.to_owned(),
        builder_git_sha: git_sha(),
        date,
        event_time_start: day_start(date),
        event_time_end_exclusive: day_end(date),
        format: "jsonl-indexed-gzip-sharded".to_owned(),
        decision_grade_projection: true,
        events,
        input_events: required_u64(manifest, "input_events")?,
        malformed_lines: manifest["malformed_lines"].as_u64().unwrap_or(0),
        raw_source_inventory: serde_json::from_value(
            manifest
                .get("raw_source_inventory")
                .cloned()
                .ok_or_else(|| {
                    ResearchError::InvalidInput(
                        "normalized manifest is missing raw_source_inventory".to_owned(),
                    )
                })?,
        )?,
        first_recorded_ts,
        last_recorded_ts,
        event_counts,
        files,
    })
}

fn normalized_file_bindings(
    root: &Path,
    manifest: &Value,
) -> Result<Vec<ProjectedFileBinding>, ResearchError> {
    let files = manifest["files"].as_object().ok_or_else(|| {
        ResearchError::InvalidInput("normalized manifest is missing files".to_owned())
    })?;
    let mut bindings = Vec::new();
    for (logical_name, value) in files {
        if logical_name == "events" || value.is_null() {
            continue;
        }
        let original_path = value["path"].as_str().ok_or_else(|| {
            ResearchError::InvalidInput(format!(
                "normalized manifest file {logical_name} is missing path"
            ))
        })?;
        let file_name = Path::new(original_path)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                ResearchError::InvalidInput(format!(
                    "normalized manifest file {logical_name} has an invalid path"
                ))
            })?;
        if !file_name.ends_with(".jsonl.gz") {
            return Err(ResearchError::InvalidInput(format!(
                "projected cache file {file_name} is not a gzip JSONL shard"
            )));
        }
        let bytes = fs::read(root.join(file_name))?;
        bindings.push(ProjectedFileBinding {
            logical_name: logical_name.clone(),
            relative_path: file_name.to_owned(),
            rows: value["rows"].as_u64().unwrap_or(0),
            bytes: bytes.len() as u64,
            sha256: sha256_prefixed(&bytes),
        });
    }
    bindings.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if bindings.is_empty() {
        return Err(ResearchError::InvalidInput(
            "projected cache found no normalized shards".to_owned(),
        ));
    }
    Ok(bindings)
}

fn validate_day_manifest(
    manifest: &ProjectedDayManifest,
    expected_date: NaiveDate,
    expected_campaign: &str,
) -> Result<(), ResearchError> {
    if manifest.schema_version != PROJECTED_DAY_SCHEMA_VERSION
        || manifest.canonical.schema_version != PROJECTED_DAY_SCHEMA_VERSION
        || manifest.canonical.domain != PROJECTED_CACHE_DOMAIN
        || manifest.canonical.date != expected_date
        || manifest.canonical.campaign_id != expected_campaign
        || !manifest.canonical.decision_grade_projection
        || manifest.canonical.events == 0
        || manifest.canonical.files.is_empty()
    {
        return Err(ResearchError::InvalidInput(format!(
            "projected day manifest identity or schema is invalid for {expected_date}"
        )));
    }
    validate_day_bounds(
        expected_date,
        &manifest.canonical.first_recorded_ts,
        &manifest.canonical.last_recorded_ts,
    )?;
    validate_raw_source_inventory(&manifest.canonical.raw_source_inventory)?;
    let expected = sha256_prefixed(&canonical_bytes(&manifest.canonical)?);
    if expected != manifest.canonical_sha256 {
        return Err(ResearchError::InvalidInput(format!(
            "projected day canonical hash mismatch for {expected_date}"
        )));
    }
    let mut paths = BTreeSet::new();
    for file in &manifest.canonical.files {
        validate_relative_cache_path(&file.relative_path)?;
        if !paths.insert(file.relative_path.clone()) || !valid_sha256(&file.sha256) {
            return Err(ResearchError::InvalidInput(format!(
                "projected day file bindings are invalid for {expected_date}"
            )));
        }
    }
    Ok(())
}

fn validate_projected_day_source(
    canonical: &ProjectedDayCanonical,
    expected_date: NaiveDate,
    require_azure_source: bool,
    expected_source_container: Option<&str>,
) -> Result<(), ResearchError> {
    if !require_azure_source {
        return Ok(());
    }
    let inventory = &canonical.raw_source_inventory.canonical;
    let expected_suffix = format!("{}/", expected_date.format("%Y/%m/%d"));
    let expected_campaign_component = format!("/{}/", canonical.campaign_id);
    let expected_source_container = expected_source_container
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ResearchError::InvalidInput(
                "exact Azure projected source policy requires expected_source_container".to_owned(),
            )
        })?;
    if inventory.source_kind != "azure_blob"
        || inventory.account.as_deref().is_none_or(str::is_empty)
        || inventory.container.as_deref().is_none_or(str::is_empty)
        || inventory.container.as_deref() != Some(expected_source_container)
        || !inventory.exhaustive_listing
        || inventory.max_blobs.is_some()
        || inventory.max_bytes.is_some()
        || !inventory.prefix.ends_with(&expected_suffix)
        || !format!("/{}", inventory.prefix).contains(&expected_campaign_component)
    {
        return Err(ResearchError::InvalidInput(format!(
            "projected day {expected_date} requires an exhaustive Azure raw-source inventory bound to its exact UTC prefix"
        )));
    }
    if inventory.canonical_blob_names_invalid_for_prefix() {
        return Err(ResearchError::InvalidInput(format!(
            "projected day {expected_date} contains a raw blob outside its exact Azure day prefix"
        )));
    }
    Ok(())
}

impl RawSourceInventoryCanonical {
    fn canonical_blob_names_invalid_for_prefix(&self) -> bool {
        self.blobs.iter().any(|blob| {
            !blob.name.starts_with(&self.prefix)
                || !blob.name.ends_with(".jsonl")
                || blob.blob_type.as_deref() != Some("AppendBlob")
                || blob.sealed.is_none()
                || blob
                    .last_modified
                    .as_deref()
                    .is_none_or(|value| chrono::DateTime::parse_from_rfc3339(value).is_err())
        })
    }
}

fn validate_campaign_index(
    index: &ProjectedCampaignIndex,
    materialized_root: Option<&Path>,
) -> Result<(), ResearchError> {
    ensure_sealed_utc_day(index.through, "projected campaign through date")?;
    validate_campaign_id(&index.campaign_id)?;
    if index.schema_version != PROJECTED_CAMPAIGN_SCHEMA_VERSION
        || index.since > index.through
        || index.cutoff_exclusive != day_end(index.through)
        || index.segments.is_empty()
        || !matches!(
            index.source_policy.as_str(),
            "exact_azure_blob_inventory_v1" | "local_test_inventory_v1"
        )
        || (index.source_policy == "exact_azure_blob_inventory_v1"
            && index.source_container.as_deref().is_none_or(str::is_empty))
        || (index.source_policy == "local_test_inventory_v1" && index.source_container.is_some())
    {
        return Err(ResearchError::InvalidInput(
            "projected campaign index schema, range, or cutoff is invalid".to_owned(),
        ));
    }
    let expected_len = index
        .through
        .signed_duration_since(index.since)
        .num_days()
        .checked_add(1)
        .and_then(|days| usize::try_from(days).ok())
        .ok_or_else(|| ResearchError::InvalidInput("campaign date range overflow".to_owned()))?;
    if index.segments.len() != expected_len {
        return Err(ResearchError::InvalidInput(
            "projected campaign index has a date gap or duplicate".to_owned(),
        ));
    }
    let mut expected_date = index.since;
    let mut parent = None::<String>;
    let mut total_events = 0_u64;
    for segment in &index.segments {
        if segment.date != expected_date || segment.parent_chain_sha256 != parent {
            return Err(ResearchError::InvalidInput(format!(
                "projected campaign chain discontinuity at {}",
                segment.date
            )));
        }
        validate_day_bounds(
            segment.date,
            &segment.first_recorded_ts,
            &segment.last_recorded_ts,
        )?;
        let expected_chain = campaign_chain_hash(
            parent.as_deref(),
            segment.date,
            &segment.day_canonical_sha256,
        );
        if segment.chain_sha256 != expected_chain
            || !valid_sha256(&segment.day_manifest_sha256)
            || !valid_sha256(&segment.raw_source_inventory_sha256)
            || (index.source_policy == "exact_azure_blob_inventory_v1"
                && segment.raw_source_kind != "azure_blob")
            || (index.source_policy == "local_test_inventory_v1"
                && segment.raw_source_kind != "local_files")
            || segment.events == 0
        {
            return Err(ResearchError::InvalidInput(format!(
                "projected campaign segment binding is invalid at {}",
                segment.date
            )));
        }
        validate_relative_cache_path(&segment.relative_path)?;
        if let Some(root) = materialized_root {
            let segment_dir = root.join(&segment.relative_path);
            if !segment_dir.is_dir() || !segment_dir.join("events_manifest.json").is_file() {
                return Err(ResearchError::InvalidInput(format!(
                    "materialized projected segment is incomplete at {}",
                    segment.date
                )));
            }
            let day_manifest_bytes = fs::read(segment_dir.join(PROJECTED_DAY_MANIFEST_FILE))?;
            if sha256_prefixed(&day_manifest_bytes) != segment.day_manifest_sha256 {
                return Err(ResearchError::InvalidInput(format!(
                    "materialized projected manifest hash mismatch at {}",
                    segment.date
                )));
            }
            let day_manifest: ProjectedDayManifest = serde_json::from_slice(&day_manifest_bytes)?;
            validate_day_manifest(&day_manifest, segment.date, &index.campaign_id)?;
            validate_projected_day_source(
                &day_manifest.canonical,
                segment.date,
                index.source_policy == "exact_azure_blob_inventory_v1",
                index.source_container.as_deref(),
            )?;
            if day_manifest.canonical_sha256 != segment.day_canonical_sha256
                || day_manifest.canonical.events != segment.events
                || day_manifest.canonical.raw_source_inventory.canonical_sha256
                    != segment.raw_source_inventory_sha256
                || day_manifest
                    .canonical
                    .raw_source_inventory
                    .canonical
                    .source_kind
                    != segment.raw_source_kind
                || day_manifest.canonical.first_recorded_ts != segment.first_recorded_ts
                || day_manifest.canonical.last_recorded_ts != segment.last_recorded_ts
            {
                return Err(ResearchError::InvalidInput(format!(
                    "materialized projected segment binding mismatch at {}",
                    segment.date
                )));
            }
            for file in &day_manifest.canonical.files {
                let bytes = fs::read(segment_dir.join(&file.relative_path))?;
                verify_binding_bytes(file, &bytes)?;
            }
        }
        total_events = total_events.checked_add(segment.events).ok_or_else(|| {
            ResearchError::InvalidInput("campaign event count overflow".to_owned())
        })?;
        parent = Some(expected_chain);
        if expected_date != index.through {
            expected_date = expected_date
                .succ_opt()
                .ok_or_else(|| ResearchError::InvalidInput("campaign date overflow".to_owned()))?;
        }
    }
    if expected_date != index.through
        || index.total_events != total_events
        || parent.as_deref() != Some(index.canonical_sha256.as_str())
    {
        return Err(ResearchError::InvalidInput(
            "projected campaign terminal hash or event total is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_day_bounds(date: NaiveDate, first: &str, last: &str) -> Result<(), ResearchError> {
    let first = DateTime::parse_from_rfc3339(first)
        .map_err(|_| ResearchError::InvalidInput(format!("invalid first timestamp for {date}")))?
        .with_timezone(&Utc);
    let last = DateTime::parse_from_rfc3339(last)
        .map_err(|_| ResearchError::InvalidInput(format!("invalid last timestamp for {date}")))?
        .with_timezone(&Utc);
    let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let end = date
        .succ_opt()
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    if first < start || first >= end || last < first || last >= end {
        return Err(ResearchError::InvalidInput(format!(
            "projected day timestamps escape the sealed UTC partition for {date}"
        )));
    }
    Ok(())
}

fn ensure_sealed_utc_day(date: NaiveDate, field: &str) -> Result<(), ResearchError> {
    let today = Utc::now().date_naive();
    if date >= today {
        return Err(ResearchError::InvalidInput(format!(
            "{field} must be a sealed UTC day before {today}; received {date}"
        )));
    }
    Ok(())
}

fn campaign_chain_hash(parent: Option<&str>, date: NaiveDate, day_sha256: &str) -> String {
    let value = json!({
        "domain": PROJECTED_CAMPAIGN_DOMAIN,
        "parent_sha256": parent,
        "date": date,
        "day_canonical_sha256": day_sha256
    });
    sha256_prefixed(&serde_json::to_vec(&value).expect("campaign chain value serializes"))
}

fn verify_binding_bytes(binding: &ProjectedFileBinding, bytes: &[u8]) -> Result<(), ResearchError> {
    if bytes.len() as u64 != binding.bytes || sha256_prefixed(bytes) != binding.sha256 {
        return Err(ResearchError::InvalidInput(format!(
            "projected shard {} failed size or SHA-256 verification",
            binding.relative_path
        )));
    }
    Ok(())
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ResearchError> {
    serde_json::to_vec(value).map_err(ResearchError::Json)
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn valid_sha256(value: &str) -> bool {
    let digest = value.strip_prefix("sha256:").unwrap_or(value);
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_campaign_id(value: &str) -> Result<(), ResearchError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ResearchError::InvalidInput(
            "campaign id must contain only ASCII letters, numbers, '-' or '_'".to_owned(),
        ));
    }
    Ok(())
}

fn validate_relative_cache_path(value: &str) -> Result<(), ResearchError> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(ResearchError::InvalidInput(
            "projected cache path is not safely relative".to_owned(),
        ));
    }
    Ok(())
}

fn required_text(value: &Value, key: &str) -> Result<String, ResearchError> {
    value[key]
        .as_str()
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ResearchError::InvalidInput(format!("normalized manifest is missing {key}")))
}

fn required_u64(value: &Value, key: &str) -> Result<u64, ResearchError> {
    value[key]
        .as_u64()
        .ok_or_else(|| ResearchError::InvalidInput(format!("normalized manifest is missing {key}")))
}

fn value_u64_map(value: &Value) -> Result<BTreeMap<String, u64>, ResearchError> {
    let object = value.as_object().ok_or_else(|| {
        ResearchError::InvalidInput("normalized manifest event_counts is invalid".to_owned())
    })?;
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_u64()
                .map(|count| (key.clone(), count))
                .ok_or_else(|| {
                    ResearchError::InvalidInput("normalized event count is invalid".to_owned())
                })
        })
        .collect()
}

fn day_start(date: NaiveDate) -> String {
    date.and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn day_end(date: NaiveDate) -> String {
    day_start(date.succ_opt().expect("valid campaign date successor"))
}

fn sibling_staging_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("campaign");
    path.with_file_name(format!(".{name}.staging-{}", std::process::id()))
}

fn with_trailing_newline(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(b'\n');
    bytes
}

fn write_pretty_json<T: Serialize>(path: &Path, value: &T) -> Result<(), ResearchError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        with_trailing_newline(serde_json::to_vec_pretty(value)?),
    )?;
    Ok(())
}

enum ProjectedCacheStore {
    Local {
        root: PathBuf,
    },
    Azure {
        client: AzureBlobClient,
        prefix: String,
    },
}

impl ProjectedCacheStore {
    fn open(root: &str) -> Result<Self, ResearchError> {
        if root.starts_with("azure://") {
            let (account, container, prefix) = parse_azure_artifact_uri(root)?;
            let client_id = std::env::var("AZURE_CLIENT_ID")
                .ok()
                .filter(|value| !value.trim().is_empty());
            return Ok(Self::Azure {
                client: AzureBlobClient::with_managed_identity(account, container, client_id),
                prefix: prefix.trim_matches('/').to_owned(),
            });
        }
        Ok(Self::Local {
            root: PathBuf::from(root),
        })
    }

    fn read(&mut self, relative: &str) -> Result<Vec<u8>, ResearchError> {
        validate_relative_cache_path(relative)?;
        match self {
            Self::Local { root } => fs::read(root.join(relative)).map_err(ResearchError::Io),
            Self::Azure { client, prefix } => client
                .download_blob_bytes(&format!("{prefix}/{relative}"))
                .map_err(|error| ResearchError::Azure(error.to_string())),
        }
    }

    fn read_optional(&mut self, relative: &str) -> Result<Option<Vec<u8>>, ResearchError> {
        validate_relative_cache_path(relative)?;
        match self {
            Self::Local { root } => match fs::read(root.join(relative)) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(ResearchError::Io(error)),
            },
            Self::Azure { client, prefix } => {
                match client.download_blob_bytes(&format!("{prefix}/{relative}")) {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(AzureBlobError::HttpStatus(404)) => Ok(None),
                    Err(error) => Err(ResearchError::Azure(error.to_string())),
                }
            }
        }
    }

    fn put_immutable_verified(
        &mut self,
        relative: &str,
        bytes: &[u8],
    ) -> Result<(), ResearchError> {
        validate_relative_cache_path(relative)?;
        match self {
            Self::Local { root } => {
                let path = root.join(relative);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                match OpenOptions::new().write(true).create_new(true).open(&path) {
                    Ok(mut file) => {
                        file.write_all(bytes)?;
                        file.sync_all()?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if fs::read(&path)? != bytes {
                            return Err(ResearchError::InvalidInput(format!(
                                "immutable projected cache collision at {relative}"
                            )));
                        }
                    }
                    Err(error) => return Err(ResearchError::Io(error)),
                }
                Ok(())
            }
            Self::Azure { client, prefix } => {
                let name = format!("{prefix}/{relative}");
                match client
                    .upload_block_blob_bytes_if_absent(&name, bytes, content_type(relative))
                    .map_err(|error| ResearchError::Azure(error.to_string()))?
                {
                    ImmutableBlobWrite::Created => Ok(()),
                    ImmutableBlobWrite::AlreadyExists => {
                        let existing = client
                            .download_blob_bytes(&name)
                            .map_err(|error| ResearchError::Azure(error.to_string()))?;
                        if existing == bytes {
                            Ok(())
                        } else {
                            Err(ResearchError::InvalidInput(format!(
                                "immutable projected cache collision at {relative}"
                            )))
                        }
                    }
                }
            }
        }
    }

    fn put_pointer_cas(
        &mut self,
        relative: &str,
        bytes: &[u8],
        expected_prior_sha256: Option<&str>,
    ) -> Result<(), ResearchError> {
        validate_relative_cache_path(relative)?;
        match self {
            Self::Local { root } => {
                let path = root.join(relative);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let lock = path.with_extension("cas-lock");
                let lock_file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock)
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::AlreadyExists {
                            ResearchError::InvalidInput(format!(
                                "projected cache pointer compare-and-swap is already in progress at {relative}"
                            ))
                        } else {
                            ResearchError::Io(error)
                        }
                    })?;
                let result = (|| {
                    let current = if path.is_file() {
                        Some(fs::read(&path)?)
                    } else {
                        None
                    };
                    if current.as_deref().map(sha256_prefixed).as_deref() != expected_prior_sha256 {
                        return Err(ResearchError::InvalidInput(format!(
                            "projected cache pointer expected-prior mismatch at {relative}"
                        )));
                    }
                    if current.as_deref() == Some(bytes) {
                        return Ok(());
                    }
                    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
                    fs::write(&temporary, bytes)?;
                    fs::rename(temporary, &path)?;
                    Ok::<(), ResearchError>(())
                })();
                drop(lock_file);
                let _ = fs::remove_file(lock);
                result
            }
            Self::Azure { client, prefix } => {
                let name = format!("{prefix}/{relative}");
                let prior = match client.download_blob_bytes_with_etag(&name) {
                    Ok(prior) => Some(prior),
                    Err(AzureBlobError::HttpStatus(404)) => None,
                    Err(error) => return Err(ResearchError::Azure(error.to_string())),
                };
                if prior
                    .as_ref()
                    .map(|prior| sha256_prefixed(&prior.bytes))
                    .as_deref()
                    != expected_prior_sha256
                {
                    return Err(ResearchError::InvalidInput(format!(
                        "projected cache pointer expected-prior mismatch at {relative}"
                    )));
                }
                if prior.as_ref().is_some_and(|prior| prior.bytes == bytes) {
                    return Ok(());
                }
                if let Some(prior) = prior {
                    let updated = client
                        .upload_block_blob_bytes_if_match(
                            &name,
                            bytes,
                            content_type(relative),
                            &prior.etag,
                        )
                        .map_err(|error| ResearchError::Azure(error.to_string()))?;
                    if updated {
                        return Ok(());
                    }
                } else {
                    match client
                        .upload_block_blob_bytes_if_absent(&name, bytes, content_type(relative))
                        .map_err(|error| ResearchError::Azure(error.to_string()))?
                    {
                        ImmutableBlobWrite::Created => return Ok(()),
                        ImmutableBlobWrite::AlreadyExists => {}
                    }
                }
                let winner = client
                    .download_blob_bytes(&name)
                    .map_err(|error| ResearchError::Azure(error.to_string()))?;
                if winner == bytes {
                    Ok(())
                } else {
                    Err(ResearchError::InvalidInput(format!(
                        "projected cache pointer compare-and-swap conflict at {relative}"
                    )))
                }
            }
        }
    }
}

fn content_type(path: &str) -> &'static str {
    if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".gz") {
        "application/gzip"
    } else {
        "application/octet-stream"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_test_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("polyedge-{label}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn shadow_correction_journal_blocks_overlap_and_is_resumable() {
        let root = temporary_test_root("shadow-correction");
        let output = root.join("active-output.json");
        let mut store = ProjectedCacheStore::open(root.to_str().unwrap()).unwrap();
        let begin = BeginShadowCorrectionOptions {
            campaign_id: "campaign-2026-07-12".to_owned(),
            correction_id: "correction-july-13".to_owned(),
            from: NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
            through: NaiveDate::from_ymd_opt(2026, 7, 13).unwrap(),
            reason: "schema migration".to_owned(),
            out: output.clone(),
        };
        begin_shadow_correction_with_store(begin.clone(), &mut store).unwrap();
        let active = read_shadow_correction_state_from_store(&mut store)
            .unwrap()
            .unwrap();
        assert_eq!(active.status, "in_progress");
        assert_eq!(active.correction_id, "correction-july-13");

        let resumed = begin_shadow_correction_with_store(begin, &mut store).unwrap();
        assert_eq!(resumed["state"]["correction_id"], "correction-july-13");

        let conflicting = BeginShadowCorrectionOptions {
            campaign_id: "campaign-2026-07-12".to_owned(),
            correction_id: "correction-july-11".to_owned(),
            from: NaiveDate::from_ymd_opt(2026, 7, 11).unwrap(),
            through: NaiveDate::from_ymd_opt(2026, 7, 13).unwrap(),
            reason: "overlap".to_owned(),
            out: output.clone(),
        };
        assert!(begin_shadow_correction_with_store(conflicting, &mut store).is_err());

        complete_shadow_correction_with_store(
            CompleteShadowCorrectionOptions {
                campaign_id: "campaign-2026-07-12".to_owned(),
                from: NaiveDate::from_ymd_opt(2026, 7, 12).unwrap(),
                through: NaiveDate::from_ymd_opt(2026, 7, 13).unwrap(),
                out: output,
            },
            &mut store,
        )
        .unwrap();
        let complete = read_shadow_correction_state_from_store(&mut store)
            .unwrap()
            .unwrap();
        assert_eq!(complete.status, "complete");
        assert!(complete.completed_at.is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn campaign_index_rejects_gap_and_bad_parent() {
        let first = ProjectedCampaignSegment {
            date: NaiveDate::from_ymd_opt(2026, 7, 10).unwrap(),
            relative_path: "segments/2026-07-10".to_owned(),
            day_canonical_sha256: format!("sha256:{}", "a".repeat(64)),
            day_manifest_sha256: format!("sha256:{}", "b".repeat(64)),
            raw_source_inventory_sha256: format!("sha256:{}", "c".repeat(64)),
            raw_source_kind: "local_files".to_owned(),
            parent_chain_sha256: None,
            chain_sha256: String::new(),
            events: 1,
            first_recorded_ts: "2026-07-10T00:00:00Z".to_owned(),
            last_recorded_ts: "2026-07-10T23:59:59Z".to_owned(),
        };
        let mut first = first;
        first.chain_sha256 = campaign_chain_hash(None, first.date, &first.day_canonical_sha256);
        let mut second = first.clone();
        second.date = NaiveDate::from_ymd_opt(2026, 7, 12).unwrap();
        second.relative_path = "segments/2026-07-12".to_owned();
        second.first_recorded_ts = "2026-07-12T00:00:00Z".to_owned();
        second.last_recorded_ts = "2026-07-12T23:59:59Z".to_owned();
        second.parent_chain_sha256 = Some(first.chain_sha256.clone());
        second.chain_sha256 = campaign_chain_hash(
            second.parent_chain_sha256.as_deref(),
            second.date,
            &second.day_canonical_sha256,
        );
        let index = ProjectedCampaignIndex {
            schema_version: PROJECTED_CAMPAIGN_SCHEMA_VERSION,
            campaign_id: "campaign-2026-07-12".to_owned(),
            since: first.date,
            through: second.date,
            cutoff_exclusive: "2026-07-13T00:00:00Z".to_owned(),
            source_policy: "local_test_inventory_v1".to_owned(),
            source_container: None,
            canonical_sha256: second.chain_sha256.clone(),
            total_events: 2,
            segments: vec![first, second],
        };
        assert!(validate_campaign_index(&index, None).is_err());
    }

    #[test]
    fn day_bounds_reject_current_or_next_day_events() {
        let date = NaiveDate::from_ymd_opt(2026, 7, 13).unwrap();
        assert!(validate_day_bounds(date, "2026-07-13T00:00:00Z", "2026-07-13T23:59:59Z").is_ok());
        assert!(validate_day_bounds(date, "2026-07-13T00:00:00Z", "2026-07-14T00:00:00Z").is_err());
    }

    #[test]
    fn direct_cache_calls_reject_the_open_utc_day_before_io() {
        let today = Utc::now().date_naive();
        let missing = std::env::temp_dir().join("polyedge-open-day-must-not-be-read");
        let publish = run_publish_projected_day(PublishProjectedDayOptions {
            normalized: missing.clone(),
            date: today,
            campaign_id: "campaign-open-day-rejection".to_owned(),
            cache_root: missing.to_string_lossy().into_owned(),
            out: missing.join("publish.json"),
            require_azure_source: false,
            expected_source_container: None,
        })
        .unwrap_err();
        assert!(publish.to_string().contains("must be a sealed UTC day"));

        let materialize = run_materialize_projected_campaign(MaterializeProjectedCampaignOptions {
            since: today,
            through: today,
            campaign_id: "campaign-open-day-rejection".to_owned(),
            cache_root: missing.to_string_lossy().into_owned(),
            out: missing.join("campaign"),
            manifest: missing.join("campaign.json"),
            require_azure_source: false,
            expected_source_container: None,
        })
        .unwrap_err();
        assert!(materialize.to_string().contains("must be a sealed UTC day"));
    }

    #[test]
    fn local_pointer_compare_and_swap_fails_closed_on_concurrent_lock() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-projected-pointer-cas-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("days/2026-07-12")).unwrap();
        fs::write(
            root.join("days/2026-07-12/latest.cas-lock"),
            b"concurrent writer",
        )
        .unwrap();
        let mut store = ProjectedCacheStore::open(&root.to_string_lossy()).unwrap();
        let error = store
            .put_pointer_cas("days/2026-07-12/latest.json", b"{}\n", None)
            .unwrap_err();
        assert!(error.to_string().contains("already in progress"));
        assert!(!root.join("days/2026-07-12/latest.json").exists());
    }

    #[test]
    fn local_pointer_compare_and_swap_rejects_wrong_expected_prior() {
        let root = temporary_test_root("pointer-expected-prior");
        let relative = "days/2026-07-10/latest.json";
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"prior\n").unwrap();
        let mut store = ProjectedCacheStore::open(root.to_str().unwrap()).unwrap();

        let wrong_prior = format!("sha256:{}", "0".repeat(64));
        let error = store
            .put_pointer_cas(relative, b"next\n", Some(&wrong_prior))
            .unwrap_err();
        assert!(error.to_string().contains("expected-prior mismatch"));
        assert_eq!(fs::read(&path).unwrap(), b"prior\n");
        assert!(!path.with_extension("cas-lock").exists());

        let exact_prior = sha256_prefixed(b"prior\n");
        store
            .put_pointer_cas(relative, b"next\n", Some(&exact_prior))
            .unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"next\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mixed_pointer_and_legacy_campaign_schemas_are_rejected() {
        let root = temporary_test_root("mixed-pointer-schema");
        let first_date = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let second_date = NaiveDate::from_ymd_opt(2026, 7, 11).unwrap();
        for (date, schema_version) in [
            (first_date, PROJECTED_DAY_SCHEMA_VERSION),
            (second_date, PROJECTED_DAY_SCHEMA_VERSION - 1),
        ] {
            let relative = format!("days/{date}/latest.json");
            let path = root.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let pointer = ProjectedDayPointer {
                schema_version,
                date,
                canonical_sha256: format!("sha256:{}", "a".repeat(64)),
                manifest_path: format!("days/{date}/runs/test/manifest.json"),
                manifest_sha256: format!("sha256:{}", "b".repeat(64)),
                supersedes_pointer_sha256: None,
            };
            fs::write(
                path,
                with_trailing_newline(serde_json::to_vec(&pointer).unwrap()),
            )
            .unwrap();
        }
        let mut store = ProjectedCacheStore::open(root.to_str().unwrap()).unwrap();
        let error = read_pointer_snapshots(&mut store, first_date, second_date).unwrap_err();
        assert!(error.to_string().contains("pointer identity mismatch"));

        let segment = ProjectedCampaignSegment {
            date: first_date,
            relative_path: "segments/2026-07-10".to_owned(),
            day_canonical_sha256: format!("sha256:{}", "a".repeat(64)),
            day_manifest_sha256: format!("sha256:{}", "b".repeat(64)),
            raw_source_inventory_sha256: format!("sha256:{}", "c".repeat(64)),
            raw_source_kind: "local_files".to_owned(),
            parent_chain_sha256: None,
            chain_sha256: campaign_chain_hash(
                None,
                first_date,
                &format!("sha256:{}", "a".repeat(64)),
            ),
            events: 1,
            first_recorded_ts: "2026-07-10T00:00:00Z".to_owned(),
            last_recorded_ts: "2026-07-10T23:59:59Z".to_owned(),
        };
        let legacy_index = ProjectedCampaignIndex {
            schema_version: PROJECTED_CAMPAIGN_SCHEMA_VERSION - 1,
            campaign_id: "campaign-2026-07-12".to_owned(),
            since: first_date,
            through: first_date,
            cutoff_exclusive: "2026-07-11T00:00:00Z".to_owned(),
            source_policy: "local_test_inventory_v1".to_owned(),
            source_container: None,
            canonical_sha256: segment.chain_sha256.clone(),
            total_events: 1,
            segments: vec![segment],
        };
        assert!(validate_campaign_index(&legacy_index, None).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn raw_inventory_field_mutation_changes_identity_and_requires_rehash() {
        let binding = RawSourceBlobBinding {
            ordinal: 0,
            name: "shadow-events/campaign-2026-07-12/2026/07/10/00/00.jsonl".to_owned(),
            etag: Some("source-etag-a".to_owned()),
            version_id: None,
            content_md5: None,
            blob_type: Some("AppendBlob".to_owned()),
            sealed: Some(true),
            content_length: 4,
            last_modified: Some("2026-07-10T00:00:00Z".to_owned()),
            sha256: sha256_prefixed(b"data"),
        };
        let inventory = super::super::build_raw_source_inventory(
            "azure_blob",
            Some("stpolyedge".to_owned()),
            Some("polyedge-shadow-events".to_owned()),
            "shadow-events/campaign-2026-07-12/2026/07/10/".to_owned(),
            None,
            None,
            vec![binding],
        )
        .unwrap();
        let original_identity = inventory.canonical_sha256.clone();
        let mut mutated = inventory;
        mutated.canonical.blobs[0].etag = Some("source-etag-b".to_owned());

        let stale_hash_error = validate_raw_source_inventory(&mutated).unwrap_err();
        assert!(stale_hash_error
            .to_string()
            .contains("canonical SHA-256 mismatch"));
        mutated.canonical_sha256 = sha256_prefixed(&canonical_bytes(&mutated.canonical).unwrap());
        assert_ne!(mutated.canonical_sha256, original_identity);
        validate_raw_source_inventory(&mutated).unwrap();
    }

    #[test]
    fn pointer_snapshot_mutation_is_detected_before_campaign_publication() {
        let root = temporary_test_root("pointer-snapshot-mutation");
        let date = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let relative = format!("days/{date}/latest.json");
        let path = root.join(&relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut pointer = ProjectedDayPointer {
            schema_version: PROJECTED_DAY_SCHEMA_VERSION,
            date,
            canonical_sha256: format!("sha256:{}", "a".repeat(64)),
            manifest_path: format!("days/{date}/runs/a/manifest.json"),
            manifest_sha256: format!("sha256:{}", "b".repeat(64)),
            supersedes_pointer_sha256: None,
        };
        fs::write(
            &path,
            with_trailing_newline(serde_json::to_vec_pretty(&pointer).unwrap()),
        )
        .unwrap();
        let mut store = ProjectedCacheStore::open(root.to_str().unwrap()).unwrap();
        let snapshots = read_pointer_snapshots(&mut store, date, date).unwrap();

        pointer.canonical_sha256 = format!("sha256:{}", "c".repeat(64));
        fs::write(
            &path,
            with_trailing_newline(serde_json::to_vec_pretty(&pointer).unwrap()),
        )
        .unwrap();
        let error = verify_pointer_snapshots_current(&mut store, &snapshots).unwrap_err();
        assert!(error
            .to_string()
            .contains("changed during campaign materialization"));
        fs::remove_dir_all(root).unwrap();
    }
}
