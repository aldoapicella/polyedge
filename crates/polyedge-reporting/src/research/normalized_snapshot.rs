use super::{validate_raw_source_inventory, RawSourceInventory, ResearchError};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use polyedge_config::embedded_git_sha;
use polyedge_storage::{AzureBlobClient, AzureBlobError, ImmutableBlobWrite};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

const SNAPSHOT_DOMAIN: &str = "polyedge.normalized-snapshot.v1";
const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_SNAPSHOT_PREFIX: &str = "data/research/normalized/v1";

#[derive(Clone, Debug)]
pub struct PublishNormalizedSnapshotOptions {
    pub input: PathBuf,
    pub date: NaiveDate,
    pub account: String,
    pub container: String,
    pub prefix: String,
    pub client_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RestoreNormalizedSnapshotOptions {
    pub out: PathBuf,
    pub date: NaiveDate,
    pub account: String,
    pub container: String,
    pub prefix: String,
    pub client_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedSnapshotFileV1 {
    pub path: String,
    pub content_length: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedSnapshotManifestV1 {
    pub domain: String,
    pub schema_version: u32,
    pub date: NaiveDate,
    pub snapshot_sha256: String,
    pub source_inventory_sha256: String,
    pub git_sha: String,
    pub format: String,
    pub events: u64,
    pub raw_source_inventory: RawSourceInventory,
    pub files: Vec<NormalizedSnapshotFileV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedSnapshotPointerV1 {
    pub domain: String,
    pub schema_version: u32,
    pub date: NaiveDate,
    pub snapshot_sha256: String,
    pub manifest_blob: String,
}

#[derive(Serialize)]
struct SnapshotIdentity<'a> {
    domain: &'a str,
    schema_version: u32,
    date: NaiveDate,
    source_inventory_sha256: &'a str,
    git_sha: &'a str,
    format: &'a str,
    events: u64,
    files: &'a [NormalizedSnapshotFileV1],
}

pub fn publish_normalized_snapshot(
    options: PublishNormalizedSnapshotOptions,
) -> Result<Value, ResearchError> {
    let local_only = super::research_artifact_publication_disabled();
    let (manifest, local_files) = build_manifest(&options.input, options.date, !local_only)?;
    if local_only {
        return Ok(json!({
            "status": "local_only",
            "date": options.date,
            "snapshot_sha256": manifest.snapshot_sha256,
            "source_inventory_sha256": manifest.source_inventory_sha256,
            "file_count": manifest.files.len(),
            "content_length": manifest.files.iter().map(|file| file.content_length).sum::<u64>(),
        }));
    }
    validate_location(&options.account, &options.container, &options.prefix)?;
    let mut client = AzureBlobClient::with_managed_identity(
        &options.account,
        &options.container,
        options.client_id.clone(),
    );
    let snapshot_segment = manifest.snapshot_sha256.trim_start_matches("sha256:");
    let snapshot_root = format!(
        "{}/{}/{snapshot_segment}",
        normalize_prefix(&options.prefix),
        options.date
    );

    for (file, local_path) in manifest.files.iter().zip(local_files.iter()) {
        let bytes = fs::read(local_path)?;
        let blob_name = format!("{snapshot_root}/files/{}", file.path);
        publish_immutable(&mut client, &blob_name, &bytes, content_type(local_path))?;
    }

    let manifest_blob = format!("{snapshot_root}/manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    publish_immutable(
        &mut client,
        &manifest_blob,
        &manifest_bytes,
        "application/json",
    )?;

    let pointer = NormalizedSnapshotPointerV1 {
        domain: SNAPSHOT_DOMAIN.to_owned(),
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        date: options.date,
        snapshot_sha256: manifest.snapshot_sha256.clone(),
        manifest_blob: manifest_blob.clone(),
    };
    let pointer_blob = pointer_blob_name(&options.prefix, options.date);
    client
        .upload_block_blob_bytes(
            &pointer_blob,
            &serde_json::to_vec_pretty(&pointer)?,
            "application/json",
        )
        .map_err(|error| ResearchError::Azure(error.to_string()))?;

    Ok(json!({
        "status": "published",
        "date": options.date,
        "snapshot_sha256": manifest.snapshot_sha256,
        "source_inventory_sha256": manifest.source_inventory_sha256,
        "manifest_blob": manifest_blob,
        "pointer_blob": pointer_blob,
        "file_count": manifest.files.len(),
        "content_length": manifest.files.iter().map(|file| file.content_length).sum::<u64>(),
    }))
}

pub fn restore_normalized_snapshot(
    options: RestoreNormalizedSnapshotOptions,
) -> Result<Value, ResearchError> {
    validate_location(&options.account, &options.container, &options.prefix)?;
    let mut snapshot_client = AzureBlobClient::with_managed_identity(
        &options.account,
        &options.container,
        options.client_id.clone(),
    );
    let pointer_blob = pointer_blob_name(&options.prefix, options.date);
    let pointer_bytes = snapshot_client
        .download_blob_bytes(&pointer_blob)
        .map_err(|error| snapshot_read_error(&pointer_blob, error))?;
    let pointer: NormalizedSnapshotPointerV1 = serde_json::from_slice(&pointer_bytes)?;
    validate_pointer(&pointer, options.date, &options.prefix)?;

    let manifest_bytes = snapshot_client
        .download_blob_bytes(&pointer.manifest_blob)
        .map_err(|error| snapshot_read_error(&pointer.manifest_blob, error))?;
    let manifest: NormalizedSnapshotManifestV1 = serde_json::from_slice(&manifest_bytes)?;
    validate_manifest(&manifest, &pointer, options.date)?;
    validate_live_source_inventory(&manifest.raw_source_inventory, options.client_id.clone())?;

    let snapshot_root = pointer
        .manifest_blob
        .strip_suffix("/manifest.json")
        .ok_or_else(|| {
            ResearchError::InvalidInput("snapshot manifest blob is unsafe".to_owned())
        })?;
    let temp = restore_temp_path(&options.out)?;
    if temp.exists() {
        fs::remove_dir_all(&temp)?;
    }
    fs::create_dir_all(&temp)?;

    let restore_result = (|| {
        for file in &manifest.files {
            let relative = safe_relative_path(&file.path)?;
            let blob_name = format!("{snapshot_root}/files/{}", file.path);
            let bytes = snapshot_client
                .download_blob_bytes(&blob_name)
                .map_err(|error| snapshot_read_error(&blob_name, error))?;
            verify_file_bytes(file, &bytes)?;
            let destination = temp.join(relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(destination, bytes)?;
        }
        Ok::<(), ResearchError>(())
    })();
    if let Err(error) = restore_result {
        let _ = fs::remove_dir_all(&temp);
        return Err(error);
    }
    replace_directory(&temp, &options.out)?;

    Ok(json!({
        "status": "restored",
        "date": options.date,
        "snapshot_sha256": manifest.snapshot_sha256,
        "source_inventory_sha256": manifest.source_inventory_sha256,
        "manifest_blob": pointer.manifest_blob,
        "file_count": manifest.files.len(),
        "content_length": manifest.files.iter().map(|file| file.content_length).sum::<u64>(),
    }))
}

fn build_manifest(
    input: &Path,
    date: NaiveDate,
    require_azure_source: bool,
) -> Result<(NormalizedSnapshotManifestV1, Vec<PathBuf>), ResearchError> {
    if !input.is_dir() {
        return Err(ResearchError::InvalidInput(format!(
            "normalized snapshot input is not a directory: {}",
            input.display()
        )));
    }
    let events_manifest_path = input.join("events_manifest.json");
    let events_manifest: Value = serde_json::from_slice(&fs::read(&events_manifest_path)?)?;
    let raw_source_inventory: RawSourceInventory = serde_json::from_value(
        events_manifest
            .get("raw_source_inventory")
            .cloned()
            .ok_or_else(|| {
                ResearchError::InvalidInput(
                    "normalized events manifest is missing raw_source_inventory".to_owned(),
                )
            })?,
    )?;
    validate_raw_source_inventory(&raw_source_inventory)?;
    if !raw_source_inventory.canonical.exhaustive_listing {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot requires an exhaustive raw-source inventory".to_owned(),
        ));
    }
    if require_azure_source && raw_source_inventory.canonical.source_kind != "azure_blob" {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot publication requires an Azure raw-source inventory".to_owned(),
        ));
    }
    let format = events_manifest
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ResearchError::InvalidInput("normalized events manifest is missing format".to_owned())
        })?
        .to_owned();
    let events = events_manifest
        .get("events")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ResearchError::InvalidInput("normalized events manifest is missing events".to_owned())
        })?;
    let git_sha = current_git_sha();
    let local_files = collect_files(input)?;
    let files = local_files
        .iter()
        .map(|path| {
            let relative = path.strip_prefix(input).map_err(|_| {
                ResearchError::InvalidInput("normalized snapshot path escaped input".to_owned())
            })?;
            let relative = relative_path_string(relative)?;
            let bytes = fs::read(path)?;
            Ok(NormalizedSnapshotFileV1 {
                path: relative,
                content_length: bytes.len() as u64,
                sha256: sha256_prefixed(&bytes),
            })
        })
        .collect::<Result<Vec<_>, ResearchError>>()?;
    if files.is_empty() || !files.iter().any(|file| file.path == "events_manifest.json") {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot is missing events_manifest.json".to_owned(),
        ));
    }
    let identity = SnapshotIdentity {
        domain: SNAPSHOT_DOMAIN,
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        date,
        source_inventory_sha256: &raw_source_inventory.canonical_sha256,
        git_sha: &git_sha,
        format: &format,
        events,
        files: &files,
    };
    let snapshot_sha256 = sha256_prefixed(&serde_json::to_vec(&identity)?);
    Ok((
        NormalizedSnapshotManifestV1 {
            domain: SNAPSHOT_DOMAIN.to_owned(),
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            date,
            snapshot_sha256,
            source_inventory_sha256: raw_source_inventory.canonical_sha256.clone(),
            git_sha,
            format,
            events,
            raw_source_inventory,
            files,
        },
        local_files,
    ))
}

fn validate_live_source_inventory(
    inventory: &RawSourceInventory,
    client_id: Option<String>,
) -> Result<(), ResearchError> {
    validate_raw_source_inventory(inventory)?;
    let account = inventory.canonical.account.as_deref().ok_or_else(|| {
        ResearchError::InvalidInput("snapshot source account is missing".to_owned())
    })?;
    let container = inventory.canonical.container.as_deref().ok_or_else(|| {
        ResearchError::InvalidInput("snapshot source container is missing".to_owned())
    })?;
    if inventory.canonical.source_kind != "azure_blob"
        || !inventory.canonical.exhaustive_listing
        || inventory.canonical.max_blobs.is_some()
        || inventory.canonical.max_bytes.is_some()
    {
        return Err(ResearchError::InvalidInput(
            "snapshot source inventory is not exhaustive Azure Blob evidence".to_owned(),
        ));
    }
    let mut client = AzureBlobClient::with_managed_identity(account, container, client_id);
    let mut live = client
        .list_blobs_unfiltered(&inventory.canonical.prefix, None, None)
        .map_err(|error| ResearchError::Azure(error.to_string()))?;
    live.sort_by(|left, right| left.name.cmp(&right.name));
    if live.len() != inventory.canonical.blobs.len() {
        return Err(ResearchError::InvalidInput(
            "raw source blob count changed after normalized snapshot publication".to_owned(),
        ));
    }
    for (expected, actual) in inventory.canonical.blobs.iter().zip(live.iter()) {
        let actual_last_modified = actual.last_modified.map(canonical_last_modified);
        if expected.name != actual.name
            || expected.etag.as_deref() != Some(actual.etag.as_str())
            || expected.version_id != actual.version_id
            || expected.content_md5 != actual.content_md5
            || expected.blob_type != actual.blob_type
            || expected.sealed != actual.sealed
            || expected.content_length != actual.content_length
            || expected.last_modified != actual_last_modified
        {
            return Err(ResearchError::InvalidInput(format!(
                "raw source blob changed after normalized snapshot publication: {}",
                expected.name
            )));
        }
    }
    Ok(())
}

fn validate_pointer(
    pointer: &NormalizedSnapshotPointerV1,
    date: NaiveDate,
    prefix: &str,
) -> Result<(), ResearchError> {
    let expected_manifest = format!(
        "{}/{date}/{}/manifest.json",
        normalize_prefix(prefix),
        pointer.snapshot_sha256.trim_start_matches("sha256:")
    );
    if pointer.domain != SNAPSHOT_DOMAIN
        || pointer.schema_version != SNAPSHOT_SCHEMA_VERSION
        || pointer.date != date
        || !is_sha256(&pointer.snapshot_sha256)
        || pointer.manifest_blob != expected_manifest
    {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot pointer is invalid or outside its date prefix".to_owned(),
        ));
    }
    Ok(())
}

fn validate_manifest(
    manifest: &NormalizedSnapshotManifestV1,
    pointer: &NormalizedSnapshotPointerV1,
    date: NaiveDate,
) -> Result<(), ResearchError> {
    validate_raw_source_inventory(&manifest.raw_source_inventory)?;
    if manifest.domain != SNAPSHOT_DOMAIN
        || manifest.schema_version != SNAPSHOT_SCHEMA_VERSION
        || manifest.date != date
        || manifest.snapshot_sha256 != pointer.snapshot_sha256
        || manifest.source_inventory_sha256 != manifest.raw_source_inventory.canonical_sha256
        || !is_sha256(&manifest.snapshot_sha256)
        || manifest.files.is_empty()
    {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot manifest failed identity validation".to_owned(),
        ));
    }
    let current_sha = current_git_sha();
    if current_sha != "unknown" && manifest.git_sha != "unknown" && manifest.git_sha != current_sha
    {
        return Err(ResearchError::InvalidInput(format!(
            "normalized snapshot git SHA {} does not match current image {}",
            manifest.git_sha, current_sha
        )));
    }
    let identity = SnapshotIdentity {
        domain: &manifest.domain,
        schema_version: manifest.schema_version,
        date: manifest.date,
        source_inventory_sha256: &manifest.source_inventory_sha256,
        git_sha: &manifest.git_sha,
        format: &manifest.format,
        events: manifest.events,
        files: &manifest.files,
    };
    if sha256_prefixed(&serde_json::to_vec(&identity)?) != manifest.snapshot_sha256 {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot manifest content hash does not match".to_owned(),
        ));
    }
    let mut paths = BTreeSet::new();
    for file in &manifest.files {
        safe_relative_path(&file.path)?;
        if !paths.insert(file.path.as_str()) || !is_sha256(&file.sha256) {
            return Err(ResearchError::InvalidInput(format!(
                "normalized snapshot file path or hash is invalid: {}",
                file.path
            )));
        }
    }
    if !paths.contains("events_manifest.json") {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot manifest omits events_manifest.json".to_owned(),
        ));
    }
    Ok(())
}

fn publish_immutable(
    client: &mut AzureBlobClient,
    blob_name: &str,
    bytes: &[u8],
    content_type: &str,
) -> Result<(), ResearchError> {
    match client
        .upload_block_blob_bytes_if_absent(blob_name, bytes, content_type)
        .map_err(|error| ResearchError::Azure(error.to_string()))?
    {
        ImmutableBlobWrite::Created => Ok(()),
        ImmutableBlobWrite::AlreadyExists => {
            let existing = client
                .download_blob_bytes(blob_name)
                .map_err(|error| ResearchError::Azure(error.to_string()))?;
            if existing != bytes {
                return Err(ResearchError::InvalidInput(format!(
                    "immutable normalized snapshot blob already exists with different bytes: {blob_name}"
                )));
            }
            Ok(())
        }
    }
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, ResearchError> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(ResearchError::InvalidInput(format!(
                    "normalized snapshot refuses symbolic link {}",
                    entry.path().display()
                )));
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort_by(|left, right| {
        left.strip_prefix(root)
            .unwrap_or(left)
            .cmp(right.strip_prefix(root).unwrap_or(right))
    });
    Ok(files)
}

fn verify_file_bytes(file: &NormalizedSnapshotFileV1, bytes: &[u8]) -> Result<(), ResearchError> {
    if bytes.len() as u64 != file.content_length || sha256_prefixed(bytes) != file.sha256 {
        return Err(ResearchError::InvalidInput(format!(
            "normalized snapshot file failed hash validation: {}",
            file.path
        )));
    }
    Ok(())
}

fn replace_directory(temp: &Path, destination: &Path) -> Result<(), ResearchError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let backup = destination.with_extension(format!("backup-{}", std::process::id()));
    if backup.exists() {
        fs::remove_dir_all(&backup)?;
    }
    if destination.exists() {
        fs::rename(destination, &backup)?;
    }
    if let Err(error) = fs::rename(temp, destination) {
        if backup.exists() {
            let _ = fs::rename(&backup, destination);
        }
        return Err(error.into());
    }
    if backup.exists() {
        fs::remove_dir_all(backup)?;
    }
    Ok(())
}

fn restore_temp_path(out: &Path) -> Result<PathBuf, ResearchError> {
    let name = out
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            ResearchError::InvalidInput("normalized snapshot output path is invalid".to_owned())
        })?;
    let parent = out.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent.join(format!(".{name}.restore-{}", std::process::id())))
}

fn safe_relative_path(value: &str) -> Result<PathBuf, ResearchError> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ResearchError::InvalidInput(format!(
            "normalized snapshot contains unsafe relative path: {value}"
        )));
    }
    Ok(path.to_path_buf())
}

fn relative_path_string(path: &Path) -> Result<String, ResearchError> {
    let value = path.to_string_lossy().replace('\\', "/");
    safe_relative_path(&value)?;
    Ok(value)
}

fn validate_location(account: &str, container: &str, prefix: &str) -> Result<(), ResearchError> {
    if account.trim().is_empty()
        || container.trim().is_empty()
        || normalize_prefix(prefix).is_empty()
    {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot account, container, and prefix are required".to_owned(),
        ));
    }
    if prefix.contains("..") || prefix.starts_with('/') {
        return Err(ResearchError::InvalidInput(
            "normalized snapshot prefix is unsafe".to_owned(),
        ));
    }
    Ok(())
}

fn pointer_blob_name(prefix: &str, date: NaiveDate) -> String {
    format!("{}/{date}/latest.json", normalize_prefix(prefix))
}

fn normalize_prefix(prefix: &str) -> String {
    let value = prefix.trim_matches('/').trim();
    if value.is_empty() {
        DEFAULT_SNAPSHOT_PREFIX.to_owned()
    } else {
        value.to_owned()
    }
}

fn current_git_sha() -> String {
    embedded_git_sha()
        .map(ToOwned::to_owned)
        .or_else(|| {
            std::env::var("GIT_SHA")
                .ok()
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| {
                    value.len() == 40
                        && value.chars().all(|character| character.is_ascii_hexdigit())
                })
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

fn canonical_last_modified(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn snapshot_read_error(blob: &str, error: AzureBlobError) -> ResearchError {
    match error {
        AzureBlobError::HttpStatus(404) => {
            ResearchError::InvalidInput(format!("normalized snapshot blob does not exist: {blob}"))
        }
        other => ResearchError::Azure(other.to_string()),
    }
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|value| value.to_str()) {
        Some("json") => "application/json",
        Some("gz") => "application/gzip",
        _ => "application/octet-stream",
    }
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hash| {
        hash.len() == 64 && hash.chars().all(|character| character.is_ascii_hexdigit())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research::{RawSourceBlobBinding, RawSourceInventoryCanonical};

    #[test]
    fn rejects_snapshot_path_traversal() {
        assert!(safe_relative_path("events_manifest.json").is_ok());
        assert!(safe_relative_path("../events_manifest.json").is_err());
        assert!(safe_relative_path("/tmp/events_manifest.json").is_err());
    }

    #[test]
    fn pointer_must_match_its_content_addressed_manifest_path() {
        let date = NaiveDate::from_ymd_opt(2026, 7, 30).unwrap();
        let snapshot_sha256 = sha256_prefixed(b"snapshot");
        let segment = snapshot_sha256.trim_start_matches("sha256:").to_owned();
        let mut pointer = NormalizedSnapshotPointerV1 {
            domain: SNAPSHOT_DOMAIN.to_owned(),
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            date,
            snapshot_sha256,
            manifest_blob: format!("{DEFAULT_SNAPSHOT_PREFIX}/{date}/{segment}/manifest.json"),
        };
        assert!(validate_pointer(&pointer, date, DEFAULT_SNAPSHOT_PREFIX).is_ok());
        pointer.manifest_blob = format!("{DEFAULT_SNAPSHOT_PREFIX}/{date}/other/manifest.json");
        assert!(validate_pointer(&pointer, date, DEFAULT_SNAPSHOT_PREFIX).is_err());
    }

    #[test]
    fn live_inventory_timestamp_matches_the_normalizer_precision() {
        let value = DateTime::parse_from_rfc3339("2026-07-31T00:00:00.987Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(canonical_last_modified(value), "2026-07-31T00:00:00Z");
    }

    #[test]
    fn snapshot_manifest_identity_is_deterministic() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-normalized-snapshot-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let canonical = RawSourceInventoryCanonical {
            domain: super::super::RAW_SOURCE_INVENTORY_DOMAIN.to_owned(),
            schema_version: super::super::RAW_SOURCE_INVENTORY_SCHEMA_VERSION,
            source_kind: "azure_blob".to_owned(),
            account: Some("storage".to_owned()),
            container: Some("events".to_owned()),
            prefix: "events/2026/07/30/".to_owned(),
            max_blobs: None,
            max_bytes: None,
            ordering: "blob_name_ascii_ascending".to_owned(),
            exhaustive_listing: true,
            blob_count: 1,
            total_bytes: 2,
            blobs: vec![RawSourceBlobBinding {
                ordinal: 0,
                name: "events/2026/07/30/00.jsonl".to_owned(),
                etag: Some("etag".to_owned()),
                version_id: None,
                content_md5: None,
                blob_type: Some("AppendBlob".to_owned()),
                sealed: Some(false),
                content_length: 2,
                last_modified: Some("2026-07-31T00:00:00.000Z".to_owned()),
                sha256: sha256_prefixed(b"{}"),
            }],
        };
        let inventory = RawSourceInventory {
            schema_version: super::super::RAW_SOURCE_INVENTORY_SCHEMA_VERSION,
            canonical_sha256: sha256_prefixed(&serde_json::to_vec(&canonical).unwrap()),
            canonical,
        };
        fs::write(root.join("events.jsonl.gz"), b"gzip").unwrap();
        fs::write(
            root.join("events_manifest.json"),
            serde_json::to_vec(&json!({
                "format": "jsonl-indexed-gzip",
                "events": 1,
                "raw_source_inventory": inventory.clone(),
            }))
            .unwrap(),
        )
        .unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 7, 30).unwrap();
        let first = build_manifest(&root, date, true).unwrap().0;
        let second = build_manifest(&root, date, true).unwrap().0;
        assert_eq!(first, second);
        assert!(is_sha256(&first.snapshot_sha256));

        let mut local_inventory = inventory;
        local_inventory.canonical.source_kind = "local_files".to_owned();
        local_inventory.canonical.account = None;
        local_inventory.canonical.container = None;
        local_inventory.canonical.blobs[0].etag = None;
        local_inventory.canonical.blobs[0].blob_type = Some("LocalFile".to_owned());
        local_inventory.canonical_sha256 =
            sha256_prefixed(&serde_json::to_vec(&local_inventory.canonical).unwrap());
        fs::write(
            root.join("events_manifest.json"),
            serde_json::to_vec(&json!({
                "format": "jsonl-indexed-gzip",
                "events": 1,
                "raw_source_inventory": local_inventory,
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(build_manifest(&root, date, false).is_ok());
        assert!(build_manifest(&root, date, true).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
