use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS, NON_ALPHANUMERIC};
use polyedge_domain::RuntimeEvent;
use quick_xml::events::Event;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{env, thread};
use thiserror::Error;
use tracing::warn;

const AZURE_BLOB_API_VERSION: &str = "2023-11-03";
const AZURE_BLOB_MAX_ATTEMPTS: usize = 5;
const AZURE_LEASE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const AZURE_LEASE_READ_TIMEOUT: Duration = Duration::from_secs(5);
const AZURE_LEASE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const AZURE_APPEND_BLOCK_TARGET_BYTES: usize = 4 * 1024 * 1024;
const AZURE_APPEND_HANDOFF_WINDOW: Duration = Duration::from_secs(120);
const AZURE_APPEND_RECONCILE_MAX_BYTES: u64 = 128 * 1024 * 1024;
const AZURE_TABLE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const AZURE_TABLE_READ_TIMEOUT: Duration = Duration::from_secs(8);
const AZURE_TABLE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const AZURE_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";
const AZURE_CLIENT_SECRET_MAX_BYTES: u64 = 16 * 1024;
const AZURE_CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
const AZURE_ARC_TOKEN_ROOT: &str = "/var/opt/azcmagent/tokens";
const AZURE_ARC_CHALLENGE_MAX_BYTES: u64 = 4 * 1024;
type AzureTableContinuation = Option<(String, String)>;
type AzureTablePage = (Vec<Value>, AzureTableContinuation);
type HmacSha256 = Hmac<Sha256>;
const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Azure Blob error: {0}")]
    AzureBlob(#[from] AzureBlobError),
    #[error("{0} is not implemented in the Rust backend yet")]
    Unsupported(&'static str),
    #[error("invalid recorder binding: {0}")]
    InvalidRecorderBinding(&'static str),
    #[error("local JSONL recorder has an unterminated tail")]
    UnterminatedJsonlTail,
    #[error("local JSONL recorder append was short: wrote {actual} of {expected} bytes")]
    ShortJsonlWrite { expected: usize, actual: usize },
    #[error("local JSONL recorder pending append diverged from disk")]
    PendingJsonlAppendDiverged,
    #[error("local JSONL recorder already has a pending append")]
    PendingJsonlAppend,
}

pub trait EventRecorder {
    fn record(&mut self, event: &RuntimeEvent) -> Result<(), StorageError>;

    fn record_batch(&mut self, events: &[RuntimeEvent]) -> Result<(), StorageError> {
        for event in events {
            self.record(event)?;
        }
        Ok(())
    }

    /// Records an event that crossed the runtime persistence boundary. The
    /// default keeps existing recorders source-compatible; production JSONL
    /// recorders override this to retain the binding in their envelope.
    fn record_recorded_batch(
        &mut self,
        events: &[RecordedRuntimeEvent],
    ) -> Result<(), StorageError> {
        for event in events {
            self.record(event.event())?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Additive recorder-bound metadata. `RuntimeEvent` deliberately remains the
/// event-bus and payload contract; this is only the persistence envelope.
#[derive(Clone, Debug)]
pub struct RecordedRuntimeEvent {
    event: RuntimeEvent,
    recorder_instance_id: String,
    recorder_sequence: u64,
}

impl RecordedRuntimeEvent {
    pub fn bound(
        event: RuntimeEvent,
        recorder_instance_id: impl Into<String>,
        recorder_sequence: u64,
    ) -> Self {
        Self {
            event,
            recorder_instance_id: recorder_instance_id.into(),
            recorder_sequence,
        }
    }

    pub fn event(&self) -> &RuntimeEvent {
        &self.event
    }

    pub fn recorder_sequence(&self) -> u64 {
        self.recorder_sequence
    }
}

/// Canonical JSON shared by producers and independent evidence validators.
/// Object keys are sorted recursively; arrays retain order.
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| {
                        format!(
                            "{}:{}",
                            serde_json::to_string(key).expect("JSON key serializes"),
                            canonical_json(value)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        _ => serde_json::to_string(value).expect("JSON value serializes"),
    }
}

pub fn canonical_json_sha256(value: &Value) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(canonical_json(value).as_bytes())
    )
}

/// Materialize the exact JSON representation that will cross the recorder
/// boundary before computing any content binding over it.
pub fn wire_normalized_json<T: Serialize>(value: &T) -> Result<Value, serde_json::Error> {
    serde_json::from_slice(&serde_json::to_vec(value)?)
}

#[derive(Clone, Debug)]
pub struct JsonlRecorder {
    path: PathBuf,
    segment_seconds: Option<i64>,
    pending_append: Option<PendingJsonlAppend>,
}

#[derive(Clone, Debug)]
struct PendingJsonlAppend {
    path: PathBuf,
    original_offset: Option<u64>,
    bytes: Vec<u8>,
}

impl JsonlRecorder {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            segment_seconds: None,
            pending_append: None,
        }
    }

    pub fn segmented(path: impl Into<PathBuf>, segment_seconds: u64) -> Self {
        assert!(segment_seconds > 0 && segment_seconds <= i64::MAX as u64);
        Self {
            path: path.into(),
            segment_seconds: Some(segment_seconds as i64),
            pending_append: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn active_path(&self, now: DateTime<Utc>) -> PathBuf {
        let Some(segment_seconds) = self.segment_seconds else {
            return self.path.clone();
        };
        let start = now.timestamp().div_euclid(segment_seconds) * segment_seconds;
        self.path
            .join(now.format("%Y/%m/%d/%H").to_string())
            .join(format!("{start}.jsonl"))
    }

    fn append_lines(&mut self, lines: &[Vec<u8>]) -> Result<(), StorageError> {
        if self.pending_append.is_some() {
            return Err(StorageError::PendingJsonlAppend);
        }
        let path = self.active_path(Utc::now());
        let bytes = lines.concat();
        self.pending_append = Some(PendingJsonlAppend {
            path,
            original_offset: None,
            bytes,
        });
        self.resume_pending_append()
    }

    fn resume_pending_append(&mut self) -> Result<(), StorageError> {
        let Some(pending) = self.pending_append.clone() else {
            return Ok(());
        };
        if let Some(parent) = pending.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&pending.path)?;
        let length = file.metadata()?.len();
        let original_offset = match pending.original_offset {
            Some(offset) => offset,
            None => {
                if length > 0 {
                    let mut tail = [0_u8; 1];
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::FileExt;
                        file.read_exact_at(&mut tail, length - 1)?;
                    }
                    #[cfg(not(unix))]
                    {
                        use std::io::{Seek, SeekFrom};
                        file.seek(SeekFrom::Start(length - 1))?;
                        file.read_exact(&mut tail)?;
                    }
                    if tail[0] != b'\n' {
                        return Err(StorageError::UnterminatedJsonlTail);
                    }
                }
                self.pending_append
                    .as_mut()
                    .expect("pending append remains while staged")
                    .original_offset = Some(length);
                length
            }
        };
        let expected_end = original_offset
            .checked_add(
                u64::try_from(pending.bytes.len())
                    .map_err(|_| StorageError::PendingJsonlAppendDiverged)?,
            )
            .ok_or(StorageError::PendingJsonlAppendDiverged)?;
        if length < original_offset || length > expected_end {
            return Err(StorageError::PendingJsonlAppendDiverged);
        }
        let present = (length - original_offset) as usize;
        if present > 0 {
            let mut existing = vec![0_u8; present];
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.read_exact_at(&mut existing, original_offset)?;
            }
            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom};
                file.seek(SeekFrom::Start(original_offset))?;
                file.read_exact(&mut existing)?;
            }
            if existing != pending.bytes[..present] {
                return Err(StorageError::PendingJsonlAppendDiverged);
            }
        }
        if present < pending.bytes.len() {
            file.write_all(&pending.bytes[present..])?;
        }
        file.sync_all()?;
        self.pending_append = None;
        Ok(())
    }
}

impl EventRecorder for JsonlRecorder {
    fn record(&mut self, event: &RuntimeEvent) -> Result<(), StorageError> {
        self.record_batch(std::slice::from_ref(event))
    }

    fn record_batch(&mut self, events: &[RuntimeEvent]) -> Result<(), StorageError> {
        if events.is_empty() {
            return Ok(());
        }
        let lines = events
            .iter()
            .map(jsonl_event_line)
            .collect::<Result<Vec<_>, _>>()?;
        self.append_lines(&lines)
    }

    fn record_recorded_batch(
        &mut self,
        events: &[RecordedRuntimeEvent],
    ) -> Result<(), StorageError> {
        if events.is_empty() {
            return Ok(());
        }
        let lines = events
            .iter()
            .map(jsonl_recorded_event_line)
            .collect::<Result<Vec<_>, _>>()?;
        self.append_lines(&lines)
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.resume_pending_append()
    }
}

#[derive(Clone, Debug, Default)]
pub struct AzureBlobRecorder;

impl EventRecorder for AzureBlobRecorder {
    fn record(&mut self, _event: &RuntimeEvent) -> Result<(), StorageError> {
        Err(StorageError::Unsupported("Azure Blob recorder"))
    }
}

#[derive(Clone)]
pub struct AzureAppendBlobRecorder {
    account: String,
    container: String,
    event_blob_prefix: String,
    event_blob_prefix_after_cutover: Option<String>,
    event_blob_prefix_cutover_utc: Option<DateTime<Utc>>,
    agent: ureq::Agent,
    token: ManagedIdentityToken,
    known_append_positions: BTreeMap<String, u64>,
    pending_append_blocks: BufferedAppendBlocks,
    handoff_unconditional_until: Instant,
    handoff_ambiguous_starts: BTreeMap<String, u64>,
}

impl AzureAppendBlobRecorder {
    pub fn new(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: Option<String>,
    ) -> Self {
        Self::new_with_prefix(account, container, client_id, "events")
    }

    pub fn new_with_prefix(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: Option<String>,
        event_blob_prefix: impl Into<String>,
    ) -> Self {
        Self::new_with_prefix_cutover(
            account,
            container,
            client_id,
            event_blob_prefix,
            None::<String>,
            None,
        )
    }

    pub fn new_with_prefix_cutover(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: Option<String>,
        event_blob_prefix: impl Into<String>,
        event_blob_prefix_after_cutover: Option<impl Into<String>>,
        event_blob_prefix_cutover_utc: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            event_blob_prefix: normalize_blob_prefix(event_blob_prefix.into()),
            event_blob_prefix_after_cutover: event_blob_prefix_after_cutover
                .map(Into::into)
                .map(normalize_blob_prefix),
            event_blob_prefix_cutover_utc,
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(30))
                .timeout_write(Duration::from_secs(30))
                .build(),
            token: ManagedIdentityToken::new(client_id),
            known_append_positions: BTreeMap::new(),
            pending_append_blocks: BufferedAppendBlocks::new(AZURE_APPEND_BLOCK_TARGET_BYTES),
            handoff_unconditional_until: Instant::now() + AZURE_APPEND_HANDOFF_WINDOW,
            handoff_ambiguous_starts: BTreeMap::new(),
        }
    }

    fn append_block(&mut self, blob_name: &str, block: &[u8]) -> Result<(), AzureBlobError> {
        if Instant::now() < self.handoff_unconditional_until
            || self.handoff_ambiguous_starts.contains_key(blob_name)
        {
            return self.append_block_during_handoff(blob_name, block);
        }
        self.ensure_append_blob(blob_name)?;
        // The shadow recorder is deployed as one replica and is the sole writer
        // for its minute prefix. Binding every append to the last observed byte
        // offset makes an ambiguous response safely reconcilable without
        // appending the same block twice.
        let expected_position = *self.known_append_positions.get(blob_name).ok_or_else(|| {
            AzureBlobError::AppendPosition(format!("missing known append position for {blob_name}"))
        })?;
        let url = self.blob_url(blob_name, Some("comp=appendblock"));
        let token = self.token.access_token(&self.agent)?;
        let result = self
            .agent
            .put(&url)
            .set("authorization", &format!("Bearer {token}"))
            .set("x-ms-version", AZURE_BLOB_API_VERSION)
            .set("x-ms-date", &rfc1123_now())
            .set(
                "x-ms-blob-condition-appendpos",
                &expected_position.to_string(),
            )
            .set("content-type", "application/octet-stream")
            .send_bytes(block);
        match result {
            Ok(_) => {
                self.known_append_positions.insert(
                    blob_name.to_owned(),
                    expected_position.saturating_add(block.len() as u64),
                );
                Ok(())
            }
            Err(ureq::Error::Status(status, _)) if status == 412 => self
                .reconcile_ambiguous_append(
                    blob_name,
                    expected_position,
                    block,
                    AzureBlobError::HttpStatus(status),
                ),
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => self.reconcile_ambiguous_append(
                blob_name,
                expected_position,
                block,
                AzureBlobError::Transport(error.to_string()),
            ),
        }
    }

    fn append_block_during_handoff(
        &mut self,
        blob_name: &str,
        block: &[u8],
    ) -> Result<(), AzureBlobError> {
        self.ensure_append_blob(blob_name)?;
        let expected_position = self.append_blob_position(blob_name)?.ok_or_else(|| {
            AzureBlobError::AppendPosition(format!(
                "append blob disappeared before handoff append for {blob_name}"
            ))
        })?;
        if let Some(ambiguous_start) = self.handoff_ambiguous_starts.get(blob_name).copied() {
            let range_outcome = if expected_position > ambiguous_start {
                self.append_range_contains_block(
                    blob_name,
                    ambiguous_start,
                    expected_position,
                    block,
                )
                .map(|present| {
                    if present {
                        HandoffRangeOutcome::ExactBlockPresent
                    } else {
                        HandoffRangeOutcome::ExactBlockAbsent
                    }
                })
            } else {
                Ok(HandoffRangeOutcome::ExactBlockAbsent)
            };
            if resolve_handoff_ambiguity(
                &mut self.handoff_ambiguous_starts,
                blob_name,
                expected_position,
                range_outcome,
            )? {
                self.known_append_positions
                    .insert(blob_name.to_owned(), expected_position);
                return Ok(());
            }
        }
        self.known_append_positions
            .insert(blob_name.to_owned(), expected_position);
        // Retain this exact boundary until either the append returns success or
        // a later range read proves the canonical block absent/present. If the
        // reconciliation read itself fails, the next retry must search from
        // this boundary before it is allowed to append.
        self.handoff_ambiguous_starts
            .insert(blob_name.to_owned(), expected_position);
        let url = self.blob_url(blob_name, Some("comp=appendblock"));
        let token = self.token.access_token(&self.agent)?;
        let result = self
            .agent
            .put(&url)
            .set("authorization", &format!("Bearer {token}"))
            .set("x-ms-version", AZURE_BLOB_API_VERSION)
            .set("x-ms-date", &rfc1123_now())
            .set("content-type", "application/octet-stream")
            .send_bytes(block);
        match result {
            Ok(_) => {
                self.handoff_ambiguous_starts.remove(blob_name);
                self.known_append_positions.insert(
                    blob_name.to_owned(),
                    expected_position.saturating_add(block.len() as u64),
                );
                Ok(())
            }
            Err(ureq::Error::Status(status, _)) if is_retryable_azure_status(status) => self
                .reconcile_ambiguous_append(
                    blob_name,
                    expected_position,
                    block,
                    AzureBlobError::HttpStatus(status),
                ),
            Err(ureq::Error::Status(status, _)) => {
                self.handoff_ambiguous_starts.remove(blob_name);
                Err(AzureBlobError::HttpStatus(status))
            }
            Err(ureq::Error::Transport(error)) => self.reconcile_ambiguous_append(
                blob_name,
                expected_position,
                block,
                AzureBlobError::Transport(error.to_string()),
            ),
        }
    }

    fn ensure_append_blob(&mut self, blob_name: &str) -> Result<(), AzureBlobError> {
        if self.known_append_positions.contains_key(blob_name) {
            return Ok(());
        }
        if let Some(position) = self.append_blob_position(blob_name)? {
            self.known_append_positions
                .insert(blob_name.to_owned(), position);
            return Ok(());
        }
        let url = self.blob_url(blob_name, None);
        let token = self.token.access_token(&self.agent)?;
        match self
            .agent
            .put(&url)
            .set("authorization", &format!("Bearer {token}"))
            .set("x-ms-version", AZURE_BLOB_API_VERSION)
            .set("x-ms-date", &rfc1123_now())
            .set("x-ms-blob-type", "AppendBlob")
            .send_bytes(&[])
        {
            Ok(_) | Err(ureq::Error::Status(409, _)) => {
                let position = self.append_blob_position(blob_name)?.unwrap_or(0);
                self.known_append_positions
                    .insert(blob_name.to_owned(), position);
                Ok(())
            }
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => Err(AzureBlobError::Transport(error.to_string())),
        }
    }

    fn append_blob_position(&mut self, blob_name: &str) -> Result<Option<u64>, AzureBlobError> {
        let url = self.blob_url(blob_name, None);
        let token = self.token.access_token(&self.agent)?;
        match self
            .agent
            .head(&url)
            .set("authorization", &format!("Bearer {token}"))
            .set("x-ms-version", AZURE_BLOB_API_VERSION)
            .set("x-ms-date", &rfc1123_now())
            .call()
        {
            Ok(response) => response
                .header("content-length")
                .ok_or_else(|| {
                    AzureBlobError::AppendPosition(format!(
                        "Azure append blob HEAD omitted content-length for {blob_name}"
                    ))
                })?
                .parse::<u64>()
                .map(Some)
                .map_err(|error| {
                    AzureBlobError::AppendPosition(format!(
                        "invalid Azure append blob content-length for {blob_name}: {error}"
                    ))
                }),
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => Err(AzureBlobError::Transport(error.to_string())),
        }
    }

    fn reconcile_ambiguous_append(
        &mut self,
        blob_name: &str,
        expected_position: u64,
        block: &[u8],
        original_error: AzureBlobError,
    ) -> Result<(), AzureBlobError> {
        let observed_position = self.append_blob_position(blob_name)?.ok_or_else(|| {
            AzureBlobError::AppendPosition(format!(
                "append blob disappeared while reconciling {blob_name}"
            ))
        })?;
        if observed_position < expected_position {
            return Err(AzureBlobError::AppendPosition(format!(
                "append position moved backwards for {blob_name}: expected at least {expected_position}, observed {observed_position}"
            )));
        }
        if observed_position == expected_position {
            self.known_append_positions
                .insert(blob_name.to_owned(), observed_position);
            return Err(original_error);
        }

        // A revision handoff can advance the same minute blob between our
        // HEAD and conditional append. Read only the advanced byte range: if
        // our exact canonical block is already present, the response was
        // ambiguous but committed; otherwise the precondition rejected it and
        // the next retry must append at the newly observed end.
        let already_committed = self.append_range_contains_block(
            blob_name,
            expected_position,
            observed_position,
            block,
        )?;
        self.known_append_positions
            .insert(blob_name.to_owned(), observed_position);
        if already_committed {
            self.handoff_ambiguous_starts.remove(blob_name);
            Ok(())
        } else {
            Err(original_error)
        }
    }

    fn append_range_contains_block(
        &mut self,
        blob_name: &str,
        start: u64,
        end_exclusive: u64,
        block: &[u8],
    ) -> Result<bool, AzureBlobError> {
        let range_len = end_exclusive.saturating_sub(start);
        if range_len > AZURE_APPEND_RECONCILE_MAX_BYTES {
            return Err(AzureBlobError::AppendPosition(format!(
                "append reconciliation range for {blob_name} is {range_len} bytes, above the {} byte safety limit",
                AZURE_APPEND_RECONCILE_MAX_BYTES
            )));
        }
        let url = self.blob_url(blob_name, None);
        let token = self.token.access_token(&self.agent)?;
        let response = self
            .agent
            .get(&url)
            .set("authorization", &format!("Bearer {token}"))
            .set("x-ms-version", AZURE_BLOB_API_VERSION)
            .set("x-ms-date", &rfc1123_now())
            .set(
                "range",
                &format!("bytes={start}-{}", end_exclusive.saturating_sub(1)),
            )
            .call()
            .map_err(|error| match error {
                ureq::Error::Status(status, _) => AzureBlobError::HttpStatus(status),
                ureq::Error::Transport(error) => AzureBlobError::Transport(error.to_string()),
            })?;
        let mut bytes = Vec::with_capacity(range_len as usize);
        response
            .into_reader()
            .take(range_len.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(AzureBlobError::Io)?;
        if bytes.len() as u64 != range_len {
            return Err(AzureBlobError::AppendPosition(format!(
                "append reconciliation range for {blob_name} returned {} bytes, expected {range_len}",
                bytes.len()
            )));
        }
        Ok(byte_range_contains_block(&bytes, block))
    }

    fn blob_url(&self, blob_name: &str, query: Option<&str>) -> String {
        let mut url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(blob_name)
        );
        if let Some(query) = query {
            url.push('?');
            url.push_str(query);
        }
        url
    }

    fn append_ready_blocks<I>(&mut self, blocks: I) -> Result<(), AzureBlobError>
    where
        I: IntoIterator<Item = (String, Vec<u8>)>,
    {
        let mut blocks = blocks.into_iter();
        while let Some((blob_name, block)) = blocks.next() {
            if let Err(error) = self.append_block(&blob_name, &block) {
                let mut retry_blocks = vec![(blob_name, block)];
                retry_blocks.extend(blocks);
                self.pending_append_blocks
                    .prepend_retry_blocks(retry_blocks);
                return Err(error);
            }
        }
        Ok(())
    }
}

fn byte_range_contains_block(bytes: &[u8], block: &[u8]) -> bool {
    if block.is_empty() || block.len() > bytes.len() {
        return false;
    }
    let final_start = bytes.len() - block.len();
    let mut offset = 0;
    while offset <= final_start {
        let Some(relative) = bytes[offset..=final_start]
            .iter()
            .position(|byte| *byte == block[0])
        else {
            return false;
        };
        let start = offset + relative;
        if bytes[start..start + block.len()] == *block {
            return true;
        }
        offset = start + 1;
    }
    false
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandoffRangeOutcome {
    ExactBlockPresent,
    ExactBlockAbsent,
}

fn resolve_handoff_ambiguity(
    ambiguous_starts: &mut BTreeMap<String, u64>,
    blob_name: &str,
    observed_position: u64,
    range_outcome: Result<HandoffRangeOutcome, AzureBlobError>,
) -> Result<bool, AzureBlobError> {
    let Some(ambiguous_start) = ambiguous_starts.get(blob_name).copied() else {
        return Ok(false);
    };
    if observed_position < ambiguous_start {
        return Err(AzureBlobError::AppendPosition(format!(
            "append position moved backwards for {blob_name}: ambiguous handoff started at {ambiguous_start}, observed {observed_position}"
        )));
    }
    let outcome = range_outcome?;
    if outcome == HandoffRangeOutcome::ExactBlockPresent {
        ambiguous_starts.remove(blob_name);
        Ok(true)
    } else {
        ambiguous_starts.insert(blob_name.to_owned(), observed_position);
        Ok(false)
    }
}

impl EventRecorder for AzureAppendBlobRecorder {
    fn record(&mut self, event: &RuntimeEvent) -> Result<(), StorageError> {
        self.record_batch(std::slice::from_ref(event))
    }

    fn record_batch(&mut self, events: &[RuntimeEvent]) -> Result<(), StorageError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut ready_blocks = Vec::new();
        for event in events {
            ready_blocks.extend(
                self.pending_append_blocks
                    .push_line(&self.event_blob_name(event), jsonl_event_line(event)?),
            );
        }
        let mut blocks = self.pending_append_blocks.take_retry_blocks();
        blocks.extend(ready_blocks);
        self.append_ready_blocks(blocks)?;
        Ok(())
    }

    fn record_recorded_batch(
        &mut self,
        events: &[RecordedRuntimeEvent],
    ) -> Result<(), StorageError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut ready_blocks = Vec::new();
        for event in events {
            ready_blocks.extend(self.pending_append_blocks.push_line(
                &self.event_blob_name(event.event()),
                jsonl_recorded_event_line(event)?,
            ));
        }
        let mut blocks = self.pending_append_blocks.take_retry_blocks();
        blocks.extend(ready_blocks);
        self.append_ready_blocks(blocks)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        let mut blocks = self.pending_append_blocks.take_retry_blocks();
        blocks.extend(self.pending_append_blocks.drain());
        self.append_ready_blocks(blocks)?;
        Ok(())
    }
}

impl AzureAppendBlobRecorder {
    fn event_blob_name(&self, event: &RuntimeEvent) -> String {
        let prefix = match (
            self.event_blob_prefix_after_cutover.as_deref(),
            self.event_blob_prefix_cutover_utc,
        ) {
            (Some(prefix), Some(cutover)) if event.ts >= cutover => prefix,
            _ => &self.event_blob_prefix,
        };
        event_blob_name(prefix, event)
    }
}

impl Drop for AzureAppendBlobRecorder {
    fn drop(&mut self) {
        if self.pending_append_blocks.pending_bytes() == 0 {
            return;
        }
        if let Err(error) = self.flush() {
            warn!("failed to flush Azure append blob recorder on drop: {error}");
        }
    }
}

#[derive(Clone, Debug)]
struct BufferedAppendBlocks {
    max_bytes: usize,
    pending: BTreeMap<String, Vec<u8>>,
    pending_line_sha256: BTreeMap<String, BTreeSet<[u8; 32]>>,
    retry_blocks: VecDeque<(String, Vec<u8>)>,
    retry_line_sha256: BTreeMap<String, BTreeSet<[u8; 32]>>,
}

impl BufferedAppendBlocks {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            pending: BTreeMap::new(),
            pending_line_sha256: BTreeMap::new(),
            retry_blocks: VecDeque::new(),
            retry_line_sha256: BTreeMap::new(),
        }
    }

    fn push_line(&mut self, blob_name: &str, line: Vec<u8>) -> Vec<(String, Vec<u8>)> {
        if line.is_empty() {
            return Vec::new();
        }
        let line_sha256: [u8; 32] = Sha256::digest(&line).into();
        if self
            .pending_line_sha256
            .get(blob_name)
            .is_some_and(|hashes| hashes.contains(&line_sha256))
            || self
                .retry_line_sha256
                .get(blob_name)
                .is_some_and(|hashes| hashes.contains(&line_sha256))
        {
            return Vec::new();
        }
        let blob_name = blob_name.to_owned();
        let mut ready = Vec::new();

        if line.len() >= self.max_bytes {
            self.push_pending_if_any(&blob_name, &mut ready);
            ready.push((blob_name, line));
            return ready;
        }

        let pending_len = self.pending.get(&blob_name).map_or(0, Vec::len);
        if pending_len > 0 && pending_len + line.len() > self.max_bytes {
            self.push_pending_if_any(&blob_name, &mut ready);
        }

        let current = self.pending.entry(blob_name.clone()).or_default();
        current.extend(line);
        self.pending_line_sha256
            .entry(blob_name.clone())
            .or_default()
            .insert(line_sha256);
        if current.len() >= self.max_bytes {
            self.push_pending_if_any(&blob_name, &mut ready);
        }
        ready
    }

    fn drain(&mut self) -> Vec<(String, Vec<u8>)> {
        self.pending_line_sha256.clear();
        std::mem::take(&mut self.pending)
            .into_iter()
            .filter(|(_, chunk)| !chunk.is_empty())
            .collect()
    }

    fn prepend_retry_blocks<I>(&mut self, blocks: I)
    where
        I: IntoIterator<Item = (String, Vec<u8>)>,
    {
        let blocks: Vec<_> = blocks
            .into_iter()
            .filter(|(_, block)| !block.is_empty())
            .collect();
        for (blob_name, block) in &blocks {
            for line in block.split_inclusive(|byte| *byte == b'\n') {
                if !line.is_empty() {
                    self.retry_line_sha256
                        .entry(blob_name.clone())
                        .or_default()
                        .insert(Sha256::digest(line).into());
                }
            }
        }
        for block in blocks.into_iter().rev() {
            self.retry_blocks.push_front(block);
        }
    }

    fn take_retry_blocks(&mut self) -> Vec<(String, Vec<u8>)> {
        self.retry_line_sha256.clear();
        self.retry_blocks.drain(..).collect()
    }

    fn pending_bytes(&self) -> usize {
        self.pending.values().map(Vec::len).sum::<usize>()
            + self
                .retry_blocks
                .iter()
                .map(|(_, block)| block.len())
                .sum::<usize>()
    }

    fn push_pending_if_any(&mut self, blob_name: &str, ready: &mut Vec<(String, Vec<u8>)>) {
        self.pending_line_sha256.remove(blob_name);
        if let Some(chunk) = self.pending.remove(blob_name) {
            if !chunk.is_empty() {
                ready.push((blob_name.to_owned(), chunk));
            }
        }
    }
}

fn normalize_blob_prefix(prefix: String) -> String {
    let prefix = prefix.trim().trim_matches('/');
    if prefix.is_empty() {
        "events".to_owned()
    } else {
        prefix.to_owned()
    }
}

fn event_blob_name(prefix: &str, event: &RuntimeEvent) -> String {
    format!("{prefix}/{}.jsonl", event.ts.format("%Y/%m/%d/%H/%M"))
}

fn jsonl_event_line(event: &RuntimeEvent) -> Result<Vec<u8>, StorageError> {
    let envelope = jsonl_event_envelope(event);
    let mut line = serde_json::to_vec(&envelope)?;
    line.push(b'\n');
    Ok(line)
}

fn jsonl_recorded_event_line(event: &RecordedRuntimeEvent) -> Result<Vec<u8>, StorageError> {
    let mut line = serde_json::to_vec(&jsonl_recorded_event_envelope(event)?)?;
    line.push(b'\n');
    Ok(line)
}

#[derive(Debug, Error)]
pub enum AzureBlobError {
    #[error("Azure Blob HTTP status {0}")]
    HttpStatus(u16),
    #[error("Azure Blob HTTP status {status}: {detail}")]
    HttpStatusDetail { status: u16, detail: String },
    #[error("managed identity token is unavailable: {0}")]
    ManagedIdentity(String),
    #[error("external Azure identity token is unavailable: {0}")]
    ExternalIdentity(String),
    #[error("Azure Blob HTTP transport error: {0}")]
    Transport(String),
    #[error("Azure append position error: {0}")]
    AppendPosition(String),
    #[error("invalid Azure Storage account key: {0}")]
    InvalidStorageKey(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("response body was not UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("XML parse error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("failed to parse Azure blob list XML: {0}")]
    XmlMessage(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImmutableBlobWrite {
    Created,
    AlreadyExists,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlobLeaseAcquireResult {
    Acquired(String),
    AlreadyLeased,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedBlobBytes {
    pub bytes: Vec<u8>,
    pub etag: String,
    pub version_id: Option<String>,
    pub content_md5: Option<String>,
    pub blob_type: Option<String>,
    pub sealed: Option<bool>,
}

impl AzureBlobError {
    fn is_retryable(&self) -> bool {
        match self {
            AzureBlobError::HttpStatus(status) => is_retryable_azure_status(*status),
            AzureBlobError::HttpStatusDetail { status, .. } => is_retryable_azure_status(*status),
            AzureBlobError::Transport(_) | AzureBlobError::Io(_) => true,
            _ => false,
        }
    }
}

fn safe_azure_response_header(response: &ureq::Response, name: &str) -> String {
    response
        .header(name)
        .filter(|value| {
            value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
        .unwrap_or("absent")
        .to_owned()
}

#[derive(Clone, Debug, Default)]
struct ManagedIdentityToken {
    client_id: Option<String>,
    resource: String,
    access_token: Option<String>,
    expires_on_epoch: Option<i64>,
    cached_auth: Option<AzureTokenAuth>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AzureTokenAuth {
    ManagedIdentity,
    ClientSecretFile(ExternalAzureAuth),
    FederatedTokenFile(ExternalAzureAuth),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExternalAzureAuth {
    tenant_id: String,
    client_id: String,
    credential_file: PathBuf,
}

impl ManagedIdentityToken {
    fn new(client_id: Option<String>) -> Self {
        Self::for_resource(client_id, "https://storage.azure.com/")
    }

    fn for_resource(client_id: Option<String>, resource: impl Into<String>) -> Self {
        Self {
            client_id,
            resource: resource.into(),
            access_token: None,
            expires_on_epoch: None,
            cached_auth: None,
        }
    }

    fn access_token(&mut self, agent: &ureq::Agent) -> Result<String, AzureBlobError> {
        let auth = token_auth_from_env(self.client_id.as_deref())?;
        let now = Utc::now().timestamp();
        if cache_is_valid(self.cached_auth.as_ref(), &auth, self.expires_on_epoch, now) {
            if let Some(token) = &self.access_token {
                return Ok(token.clone());
            }
        }
        let payload = match &auth {
            AzureTokenAuth::ManagedIdentity => {
                fetch_managed_identity_token(agent, self.client_id.as_deref(), &self.resource)?
            }
            AzureTokenAuth::ClientSecretFile(config) => {
                fetch_client_secret_token(agent, config, &self.resource)?
            }
            AzureTokenAuth::FederatedTokenFile(config) => {
                fetch_federated_identity_token(agent, config, &self.resource)?
            }
        };
        let token = payload
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| match &auth {
                AzureTokenAuth::ManagedIdentity => {
                    AzureBlobError::ManagedIdentity("missing access_token".to_owned())
                }
                AzureTokenAuth::ClientSecretFile(_) => {
                    AzureBlobError::ExternalIdentity("missing access_token".to_owned())
                }
                AzureTokenAuth::FederatedTokenFile(_) => {
                    AzureBlobError::ExternalIdentity("missing access_token".to_owned())
                }
            })?
            .to_owned();
        let expires_on = match &auth {
            AzureTokenAuth::ManagedIdentity => payload
                .get("expires_on")
                .and_then(parse_expires_on)
                .unwrap_or(now + 300),
            AzureTokenAuth::ClientSecretFile(_) | AzureTokenAuth::FederatedTokenFile(_) => payload
                .get("expires_in")
                .and_then(parse_expires_on)
                .filter(|seconds| *seconds > 0)
                .map(|seconds| now.saturating_add(seconds))
                .unwrap_or(now + 300),
        };
        self.access_token = Some(token.clone());
        self.expires_on_epoch = Some(expires_on);
        self.cached_auth = Some(auth);
        Ok(token)
    }
}

fn cache_is_valid(
    cached_auth: Option<&AzureTokenAuth>,
    auth: &AzureTokenAuth,
    expires_on_epoch: Option<i64>,
    now: i64,
) -> bool {
    cached_auth == Some(auth) && expires_on_epoch.is_some_and(|expires_on| expires_on - now > 120)
}

fn token_auth_from_env(expected_client_id: Option<&str>) -> Result<AzureTokenAuth, AzureBlobError> {
    select_token_auth(
        env::var_os("AZURE_TENANT_ID"),
        env::var_os("AZURE_CLIENT_ID"),
        env::var_os("AZURE_CLIENT_SECRET_FILE"),
        env::var_os("AZURE_FEDERATED_TOKEN_FILE"),
        expected_client_id,
    )
}

fn select_token_auth(
    tenant_id: Option<OsString>,
    client_id: Option<OsString>,
    secret_file: Option<OsString>,
    federated_token_file: Option<OsString>,
    expected_client_id: Option<&str>,
) -> Result<AzureTokenAuth, AzureBlobError> {
    // AZURE_CLIENT_ID alone selects a user-assigned managed identity today.
    if tenant_id.is_none() && secret_file.is_none() && federated_token_file.is_none() {
        return Ok(AzureTokenAuth::ManagedIdentity);
    }
    if secret_file.is_some() && federated_token_file.is_some() {
        return Err(AzureBlobError::ExternalIdentity(
            "AZURE_CLIENT_SECRET_FILE and AZURE_FEDERATED_TOKEN_FILE cannot both be set".to_owned(),
        ));
    }
    let (credential_file, credential_name, federated) = match (secret_file, federated_token_file) {
        (Some(file), None) => (file, "AZURE_CLIENT_SECRET_FILE", false),
        (None, Some(file)) => (file, "AZURE_FEDERATED_TOKEN_FILE", true),
        (None, None) => {
            return Err(AzureBlobError::ExternalIdentity(
                "AZURE_TENANT_ID, AZURE_CLIENT_ID, and exactly one credential file must be set"
                    .to_owned(),
            ));
        }
        (Some(_), Some(_)) => unreachable!("both credential files are rejected above"),
    };
    if tenant_id.is_none() || client_id.is_none() {
        return Err(AzureBlobError::ExternalIdentity(
            "AZURE_TENANT_ID, AZURE_CLIENT_ID, and exactly one credential file must be set"
                .to_owned(),
        ));
    }

    let tenant_id = unicode_config("AZURE_TENANT_ID", tenant_id.unwrap())?;
    let client_id = unicode_config("AZURE_CLIENT_ID", client_id.unwrap())?;
    let credential_file = PathBuf::from(credential_file);
    if !is_guid(&tenant_id) || !is_guid(&client_id) {
        return Err(AzureBlobError::ExternalIdentity(
            "AZURE_TENANT_ID and AZURE_CLIENT_ID must be GUIDs".to_owned(),
        ));
    }
    if expected_client_id.is_some_and(|expected| expected != client_id.as_str()) {
        return Err(AzureBlobError::ExternalIdentity(
            "AZURE_CLIENT_ID changed after the Azure client was initialized".to_owned(),
        ));
    }
    if !credential_file.is_absolute() {
        return Err(AzureBlobError::ExternalIdentity(format!(
            "{credential_name} must be an absolute path"
        )));
    }

    let config = ExternalAzureAuth {
        tenant_id,
        client_id,
        credential_file,
    };
    Ok(if federated {
        AzureTokenAuth::FederatedTokenFile(config)
    } else {
        AzureTokenAuth::ClientSecretFile(config)
    })
}

fn unicode_config(name: &str, value: OsString) -> Result<String, AzureBlobError> {
    value
        .into_string()
        .map_err(|_| AzureBlobError::ExternalIdentity(format!("{name} must contain valid Unicode")))
}

fn is_guid(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

fn fetch_client_secret_token(
    agent: &ureq::Agent,
    config: &ExternalAzureAuth,
    resource: &str,
) -> Result<Value, AzureBlobError> {
    let secret = read_credential_file(&config.credential_file, "AZURE_CLIENT_SECRET_FILE")?;
    let (url, body) = external_token_request(config, resource, &secret);
    fetch_external_identity_token(agent, &url, &body)
}

fn fetch_federated_identity_token(
    agent: &ureq::Agent,
    config: &ExternalAzureAuth,
    resource: &str,
) -> Result<Value, AzureBlobError> {
    let assertion = read_federated_token_file(&config.credential_file)?;
    let (url, body) = federated_token_request(config, resource, &assertion);
    fetch_external_identity_token(agent, &url, &body)
}

fn fetch_external_identity_token(
    agent: &ureq::Agent,
    url: &str,
    body: &str,
) -> Result<Value, AzureBlobError> {
    let response = agent
        .post(url)
        .set("Content-Type", "application/x-www-form-urlencoded")
        .set("Accept", "application/json")
        .send_string(&body)
        .map_err(external_identity_error)?;
    let text = response.into_string().map_err(|error| {
        AzureBlobError::ExternalIdentity(format!("token response could not be read: {error}"))
    })?;
    serde_json::from_str(&text).map_err(|error| {
        AzureBlobError::ExternalIdentity(format!("token response was not JSON: {error}"))
    })
}

fn external_token_request(
    config: &ExternalAzureAuth,
    resource: &str,
    secret: &str,
) -> (String, String) {
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        AZURE_AUTHORITY_HOST, config.tenant_id
    );
    let scope = format!("{}/.default", resource.trim_end_matches('/'));
    let encode = |value: &str| utf8_percent_encode(value, NON_ALPHANUMERIC).to_string();
    let body = format!(
        "client_id={}&client_secret={}&scope={}&grant_type=client_credentials",
        encode(&config.client_id),
        encode(secret),
        encode(&scope)
    );
    (url, body)
}

fn federated_token_request(
    config: &ExternalAzureAuth,
    resource: &str,
    assertion: &str,
) -> (String, String) {
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        AZURE_AUTHORITY_HOST, config.tenant_id
    );
    let scope = format!("{}/.default", resource.trim_end_matches('/'));
    let encode = |value: &str| utf8_percent_encode(value, NON_ALPHANUMERIC).to_string();
    let body = format!(
        "client_id={}&scope={}&grant_type=client_credentials&client_assertion_type={}&client_assertion={}",
        encode(&config.client_id),
        encode(&scope),
        encode(AZURE_CLIENT_ASSERTION_TYPE),
        encode(assertion),
    );
    (url, body)
}

fn read_credential_file(path: &Path, name: &str) -> Result<String, AzureBlobError> {
    validate_credential_file_path(path, name)?;
    let path_metadata =
        fs::symlink_metadata(path).map_err(|error| credential_file_error(name, error))?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(AzureBlobError::ExternalIdentity(format!(
            "{name} must be a regular file, not a symlink"
        )));
    }
    validate_credential_file_metadata(&path_metadata, name)?;
    let file = File::open(path).map_err(|error| credential_file_error(name, error))?;
    let opened_metadata = file
        .metadata()
        .map_err(|error| credential_file_error(name, error))?;
    validate_credential_file_metadata(&opened_metadata, name)?;
    validate_same_credential_file(&path_metadata, &opened_metadata, name)?;

    let mut bytes = Vec::new();
    file.take(AZURE_CLIENT_SECRET_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| credential_file_error(name, error))?;
    if bytes.len() as u64 > AZURE_CLIENT_SECRET_MAX_BYTES {
        return Err(AzureBlobError::ExternalIdentity(format!(
            "{name} exceeds 16 KiB"
        )));
    }
    let value = String::from_utf8(bytes)
        .map_err(|_| AzureBlobError::ExternalIdentity(format!("{name} is not UTF-8")))?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.is_empty() || value.bytes().any(|byte| matches!(byte, b'\r' | b'\n' | 0)) {
        return Err(AzureBlobError::ExternalIdentity(format!(
            "{name} is empty or contains an invalid control character"
        )));
    }
    Ok(value.to_owned())
}

fn validate_credential_file_path(path: &Path, name: &str) -> Result<(), AzureBlobError> {
    if path
        .as_os_str()
        .to_string_lossy()
        .split(['/', '\\'])
        .any(|component| matches!(component, "." | ".."))
    {
        return Err(AzureBlobError::ExternalIdentity(format!(
            "{name} must not contain traversal components"
        )));
    }
    for ancestor in path.ancestors().skip(1) {
        let metadata =
            fs::symlink_metadata(ancestor).map_err(|error| credential_file_error(name, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(AzureBlobError::ExternalIdentity(format!(
                "{name} must not have a symlinked or non-directory ancestor"
            )));
        }
    }
    Ok(())
}

fn read_federated_token_file(path: &Path) -> Result<String, AzureBlobError> {
    let assertion = read_credential_file(path, "AZURE_FEDERATED_TOKEN_FILE")?;
    if assertion.split('.').count() != 3
        || assertion.split('.').any(|segment| {
            segment.is_empty()
                || segment.len() % 4 == 1
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
    {
        return Err(AzureBlobError::ExternalIdentity(
            "AZURE_FEDERATED_TOKEN_FILE must be an unpadded three-segment base64url JWT".to_owned(),
        ));
    }
    Ok(assertion)
}

fn validate_credential_file_metadata(
    metadata: &fs::Metadata,
    name: &str,
) -> Result<(), AzureBlobError> {
    if metadata.len() == 0 || metadata.len() > AZURE_CLIENT_SECRET_MAX_BYTES {
        return Err(AzureBlobError::ExternalIdentity(format!(
            "{name} is empty or exceeds 16 KiB"
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(AzureBlobError::ExternalIdentity(format!(
                "{name} must have no group/other permissions or hard links"
            )));
        }
    }
    Ok(())
}

fn validate_same_credential_file(
    path_metadata: &fs::Metadata,
    opened_metadata: &fs::Metadata,
    name: &str,
) -> Result<(), AzureBlobError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_metadata.dev() != opened_metadata.dev()
            || path_metadata.ino() != opened_metadata.ino()
        {
            return Err(AzureBlobError::ExternalIdentity(format!(
                "{name} changed while it was opened"
            )));
        }
    }
    Ok(())
}

fn credential_file_error(name: &str, error: std::io::Error) -> AzureBlobError {
    AzureBlobError::ExternalIdentity(format!("{name} cannot be read: {error}"))
}

fn external_identity_error(error: ureq::Error) -> AzureBlobError {
    match error {
        ureq::Error::Status(status, _) => {
            AzureBlobError::ExternalIdentity(format!("HTTP {status}"))
        }
        ureq::Error::Transport(error) => AzureBlobError::ExternalIdentity(error.to_string()),
    }
}

fn fetch_managed_identity_token(
    agent: &ureq::Agent,
    client_id: Option<&str>,
    resource: &str,
) -> Result<Value, AzureBlobError> {
    let resource = utf8_percent_encode(resource, NON_ALPHANUMERIC).to_string();
    if let Ok(endpoint) = env::var("IDENTITY_ENDPOINT") {
        let identity_header = env::var("IDENTITY_HEADER").ok();
        let api_version = if identity_header.is_some() {
            "2019-08-01"
        } else {
            "2019-11-01"
        };
        let mut url = format!("{endpoint}?api-version={api_version}&resource={resource}");
        if let Some(client_id) = client_id {
            url.push_str("&client_id=");
            url.push_str(&utf8_percent_encode(client_id, NON_ALPHANUMERIC).to_string());
        }
        let mut request = agent.get(&url).set("Metadata", "true");
        if let Some(header) = identity_header {
            request = request.set("X-IDENTITY-HEADER", &header);
            return parse_json_response(request.call().map_err(identity_error)?);
        }
        return match request.call() {
            Ok(response) => parse_json_response(response),
            Err(ureq::Error::Status(401, response)) => {
                let challenge = read_arc_identity_challenge(&response)?;
                let response = agent
                    .get(&url)
                    .set("Metadata", "true")
                    .set("Authorization", &format!("Basic {challenge}"))
                    .call()
                    .map_err(identity_error)?;
                parse_json_response(response)
            }
            Err(error) => Err(identity_error(error)),
        };
    }

    let mut url = format!(
        "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={resource}"
    );
    if let Some(client_id) = client_id {
        url.push_str("&client_id=");
        url.push_str(&utf8_percent_encode(client_id, NON_ALPHANUMERIC).to_string());
    }
    let response = agent
        .get(&url)
        .set("Metadata", "true")
        .call()
        .map_err(identity_error)?;
    parse_json_response(response)
}

fn read_arc_identity_challenge(response: &ureq::Response) -> Result<String, AzureBlobError> {
    let header = response.header("WWW-Authenticate").ok_or_else(|| {
        AzureBlobError::ManagedIdentity("Azure Arc challenge header is missing".to_owned())
    })?;
    let path = arc_identity_challenge_path(header)?;
    let metadata = fs::symlink_metadata(&path).map_err(|error| {
        AzureBlobError::ManagedIdentity(format!("Azure Arc challenge cannot be read: {error}"))
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > AZURE_ARC_CHALLENGE_MAX_BYTES
    {
        return Err(AzureBlobError::ManagedIdentity(
            "Azure Arc challenge must be a small regular file".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| {
            file.take(AZURE_ARC_CHALLENGE_MAX_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|error| {
            AzureBlobError::ManagedIdentity(format!("Azure Arc challenge cannot be read: {error}"))
        })?;
    if bytes.len() as u64 > AZURE_ARC_CHALLENGE_MAX_BYTES {
        return Err(AzureBlobError::ManagedIdentity(
            "Azure Arc challenge is too large".to_owned(),
        ));
    }
    let value = String::from_utf8(bytes).map_err(|_| {
        AzureBlobError::ManagedIdentity("Azure Arc challenge is not UTF-8".to_owned())
    })?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.is_empty() || value.bytes().any(|byte| matches!(byte, b'\r' | b'\n' | 0)) {
        return Err(AzureBlobError::ManagedIdentity(
            "Azure Arc challenge is empty or invalid".to_owned(),
        ));
    }
    Ok(value.to_owned())
}

fn arc_identity_challenge_path(header: &str) -> Result<PathBuf, AzureBlobError> {
    let value = header
        .strip_prefix("Basic realm=")
        .map(str::trim)
        .map(|value| value.trim_matches('"'))
        .ok_or_else(|| {
            AzureBlobError::ManagedIdentity("Azure Arc challenge header is invalid".to_owned())
        })?;
    let path = PathBuf::from(value);
    if path.parent() != Some(Path::new(AZURE_ARC_TOKEN_ROOT)) || path.file_name().is_none() {
        return Err(AzureBlobError::ManagedIdentity(
            "Azure Arc challenge path is outside the agent token directory".to_owned(),
        ));
    }
    Ok(path)
}

fn identity_error(error: ureq::Error) -> AzureBlobError {
    match error {
        ureq::Error::Status(status, response) => {
            let body = response.into_string().unwrap_or_default();
            AzureBlobError::ManagedIdentity(format!("HTTP {status}: {body}"))
        }
        ureq::Error::Transport(error) => AzureBlobError::ManagedIdentity(error.to_string()),
    }
}

fn parse_json_response(response: ureq::Response) -> Result<Value, AzureBlobError> {
    let text = response
        .into_string()
        .map_err(|error| AzureBlobError::ManagedIdentity(error.to_string()))?;
    serde_json::from_str(&text).map_err(|error| AzureBlobError::ManagedIdentity(error.to_string()))
}

fn parse_expires_on(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn jsonl_event_envelope(event: &RuntimeEvent) -> Value {
    json!({
        "recorded_ts": event.ts,
        "event_type": event.event_type,
        "payload": event.data
    })
}

fn jsonl_recorded_event_envelope(event: &RecordedRuntimeEvent) -> Result<Value, StorageError> {
    if event.recorder_instance_id.is_empty() {
        return Err(StorageError::InvalidRecorderBinding(
            "recorder_instance_id is empty",
        ));
    }
    if event.recorder_sequence == 0 {
        return Err(StorageError::InvalidRecorderBinding(
            "recorder_sequence is zero",
        ));
    }
    let mut envelope = jsonl_event_envelope(event.event());
    envelope["recorder_instance_id"] = json!(event.recorder_instance_id);
    envelope["recorder_sequence"] = json!(event.recorder_sequence);
    Ok(envelope)
}

#[derive(Clone)]
pub struct AzureServiceBusSender {
    namespace: String,
    queue: String,
    token: ManagedIdentityToken,
    agent: ureq::Agent,
}

impl AzureServiceBusSender {
    pub fn with_managed_identity(
        namespace: impl Into<String>,
        queue: impl Into<String>,
        client_id: Option<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            queue: queue.into(),
            token: ManagedIdentityToken::for_resource(client_id, "https://servicebus.azure.net/"),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(5))
                .timeout_read(Duration::from_secs(10))
                .timeout_write(Duration::from_secs(10))
                .build(),
        }
    }

    pub fn send_json(
        &mut self,
        message_id: &str,
        ttl_seconds: u64,
        value: &Value,
    ) -> Result<(), AzureBlobError> {
        if self.namespace.trim().is_empty()
            || self.queue.trim().is_empty()
            || message_id.trim().is_empty()
            || ttl_seconds == 0
        {
            return Err(AzureBlobError::Transport(
                "Service Bus sender binding is incomplete".to_owned(),
            ));
        }
        let body = serde_json::to_vec(value)?;
        let queue = utf8_percent_encode(self.queue.trim(), NON_ALPHANUMERIC).to_string();
        let url = format!(
            "https://{}.servicebus.windows.net/{queue}/messages",
            self.namespace.trim()
        );
        let broker_properties = serde_json::to_string(&serde_json::json!({
            "MessageId": message_id,
            "TimeToLive": ttl_seconds
        }))?;
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let access_token = self.token.access_token(&self.agent)?;
            let response = self
                .agent
                .post(&url)
                .set("authorization", &format!("Bearer {access_token}"))
                .set("content-type", "application/json; charset=utf-8")
                .set("brokerproperties", &broker_properties)
                .send_bytes(&body);
            match response {
                Ok(_) => return Ok(()),
                Err(ureq::Error::Status(status, _))
                    if is_retryable_azure_status(status)
                        && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS =>
                {
                    thread::sleep(retry_delay(attempt));
                }
                Err(ureq::Error::Status(status, _)) => {
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(ureq::Error::Transport(error)) if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                    tracing::warn!(
                        attempt = attempt + 1,
                        error = %error,
                        "retrying managed-identity Service Bus send"
                    );
                }
                Err(ureq::Error::Transport(error)) => {
                    return Err(AzureBlobError::Transport(error.to_string()));
                }
            }
        }
        unreachable!("Service Bus retry loop always returns");
    }
}

#[derive(Clone)]
pub struct HttpJsonQueueSender {
    url: String,
    agent: ureq::Agent,
}

impl HttpJsonQueueSender {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(5))
                .timeout_read(Duration::from_secs(10))
                .timeout_write(Duration::from_secs(10))
                .build(),
        }
    }

    pub fn send_json(
        &self,
        message_id: &str,
        ttl_seconds: u64,
        value: &Value,
    ) -> Result<(), AzureBlobError> {
        if self.url.trim().is_empty() || message_id.trim().is_empty() || ttl_seconds == 0 {
            return Err(AzureBlobError::Transport(
                "HTTP queue sender binding is incomplete".to_owned(),
            ));
        }
        let body = serde_json::to_vec(value)?;
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let response = self
                .agent
                .post(self.url.trim())
                .set("content-type", "application/json; charset=utf-8")
                .set("x-polyedge-message-id", message_id)
                .set("x-polyedge-ttl-seconds", &ttl_seconds.to_string())
                .send_bytes(&body);
            match response {
                Ok(_) => return Ok(()),
                Err(ureq::Error::Status(status, _))
                    if is_retryable_azure_status(status)
                        && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS =>
                {
                    thread::sleep(retry_delay(attempt));
                }
                Err(ureq::Error::Status(status, _)) => {
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(ureq::Error::Transport(error)) if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                    tracing::warn!(
                        attempt = attempt + 1,
                        error = %error,
                        "retrying OCI queue bridge send"
                    );
                }
                Err(ureq::Error::Transport(error)) => {
                    return Err(AzureBlobError::Transport(error.to_string()));
                }
            }
        }
        unreachable!("HTTP queue retry loop always returns");
    }
}

fn rfc1123_now() -> String {
    Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AzureBlobItem {
    pub name: String,
    /// Strong validator returned by Azure for the exact listed blob version.
    /// This is kept together with size and modification time so callers can
    /// fail closed if a sealed source blob changes while it is being read.
    pub etag: String,
    pub version_id: Option<String>,
    pub is_current_version: Option<bool>,
    pub content_md5: Option<String>,
    pub blob_type: Option<String>,
    pub sealed: Option<bool>,
    pub content_length: u64,
    pub last_modified: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct AzureBlobClient {
    account: String,
    container: String,
    auth: AzureBlobAuth,
    agent: ureq::Agent,
}

#[derive(Clone)]
enum AzureBlobAuth {
    Sas(String),
    ManagedIdentity(ManagedIdentityToken),
}

impl AzureBlobClient {
    pub fn new(
        account: impl Into<String>,
        container: impl Into<String>,
        sas: impl Into<String>,
    ) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            auth: AzureBlobAuth::Sas(sas.into()),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(120))
                .timeout_write(Duration::from_secs(30))
                .build(),
        }
    }

    pub fn with_managed_identity(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: Option<String>,
    ) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            auth: AzureBlobAuth::ManagedIdentity(ManagedIdentityToken::new(client_id)),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(120))
                .timeout_write(Duration::from_secs(30))
                .build(),
        }
    }

    /// Build a managed-identity client for a single large immutable object and
    /// its mandatory read-back verification.
    pub fn with_managed_identity_for_large_immutable_upload(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: Option<String>,
    ) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            auth: AzureBlobAuth::ManagedIdentity(ManagedIdentityToken::new(client_id)),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(7200))
                .timeout_write(Duration::from_secs(7200))
                .build(),
        }
    }

    /// Build a client whose individual network operations are short enough for
    /// a finite Azure lease watchdog. Lease renewal is additionally single-shot
    /// so a transient outage cannot keep a protected child alive past expiry.
    pub fn with_managed_identity_for_lease(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: Option<String>,
    ) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            auth: AzureBlobAuth::ManagedIdentity(ManagedIdentityToken::new(client_id)),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(AZURE_LEASE_CONNECT_TIMEOUT)
                .timeout_read(AZURE_LEASE_READ_TIMEOUT)
                .timeout_write(AZURE_LEASE_WRITE_TIMEOUT)
                .build(),
        }
    }

    pub fn list_blobs(
        &mut self,
        prefix: &str,
        max_blobs: Option<usize>,
        max_bytes: Option<u64>,
    ) -> Result<Vec<AzureBlobItem>, AzureBlobError> {
        self.list_blobs_filtered(prefix, Some(&[".jsonl"]), max_blobs, max_bytes)
    }

    pub fn list_blobs_by_suffixes(
        &mut self,
        prefix: &str,
        suffixes: &[&str],
        max_blobs: Option<usize>,
        max_bytes: Option<u64>,
    ) -> Result<Vec<AzureBlobItem>, AzureBlobError> {
        self.list_blobs_filtered(prefix, Some(suffixes), max_blobs, max_bytes)
    }

    pub fn list_blobs_unfiltered(
        &mut self,
        prefix: &str,
        max_blobs: Option<usize>,
        max_bytes: Option<u64>,
    ) -> Result<Vec<AzureBlobItem>, AzureBlobError> {
        self.list_blobs_filtered(prefix, None, max_blobs, max_bytes)
    }

    fn list_blobs_filtered(
        &mut self,
        prefix: &str,
        suffixes: Option<&[&str]>,
        max_blobs: Option<usize>,
        max_bytes: Option<u64>,
    ) -> Result<Vec<AzureBlobItem>, AzureBlobError> {
        let mut marker = String::new();
        let mut blobs = Vec::new();
        let mut selected_bytes = 0_u64;
        loop {
            let mut url = format!(
                "https://{}.blob.core.windows.net/{}?restype=container&comp=list&maxresults=5000&prefix={}",
                self.account,
                self.container,
                utf8_percent_encode(prefix, NON_ALPHANUMERIC)
            );
            if !marker.is_empty() {
                url.push_str("&marker=");
                url.push_str(&utf8_percent_encode(&marker, NON_ALPHANUMERIC).to_string());
            }
            let text = self.get_text(&url)?;
            let page = parse_blob_list(&text)?;
            for blob in page.blobs {
                if suffixes.is_some_and(|suffixes| {
                    !suffixes.iter().any(|suffix| blob.name.ends_with(suffix))
                }) {
                    continue;
                }
                if max_blobs.is_some_and(|limit| blobs.len() >= limit) {
                    return Ok(blobs);
                }
                if max_bytes.is_some_and(|limit| {
                    !blobs.is_empty() && selected_bytes + blob.content_length > limit
                }) {
                    return Ok(blobs);
                }
                selected_bytes += blob.content_length;
                blobs.push(blob);
            }
            marker = page.next_marker;
            if marker.is_empty() {
                return Ok(blobs);
            }
        }
    }

    pub fn download_blob_bytes(&mut self, name: &str) -> Result<Vec<u8>, AzureBlobError> {
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        self.get_bytes_with_retry(&url)
    }

    /// Downloads an exact immutable object without allowing a corrupt remote
    /// response to allocate beyond the caller's evidence bound.
    pub fn download_blob_bytes_exact_bounded(
        &mut self,
        name: &str,
        expected_bytes: u64,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, AzureBlobError> {
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        self.read_response_bytes_exact_bounded(&url, expected_bytes, maximum_bytes)
    }

    /// Reads the exact blob bytes together with the Azure ETag needed for a
    /// subsequent compare-and-swap update.
    pub fn download_blob_bytes_with_etag(
        &mut self,
        name: &str,
    ) -> Result<VersionedBlobBytes, AzureBlobError> {
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        let response = self.get_response(&url)?;
        read_versioned_blob_response(response)
    }

    /// Reads a blob only if it is still the exact ETag returned by the prior
    /// exhaustive listing. A 412 response proves the source changed before it
    /// could be admitted into a sealed-day inventory.
    pub fn download_blob_bytes_if_match(
        &mut self,
        name: &str,
        expected_etag: &str,
    ) -> Result<VersionedBlobBytes, AzureBlobError> {
        if expected_etag.trim().is_empty()
            || expected_etag
                .chars()
                .any(|character| character.is_control())
        {
            return Err(AzureBlobError::Transport(
                "conditional Azure Blob read requires a valid ETag".to_owned(),
            ));
        }
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let result = self
                .get_response_if_match(&url, expected_etag)
                .and_then(read_versioned_blob_response);
            match result {
                Ok(versioned) => return Ok(versioned),
                Err(error) if error.is_retryable() && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure Blob conditional read retry loop always returns")
    }

    pub fn upload_block_blob_bytes(
        &mut self,
        name: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(), AzureBlobError> {
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        self.put_block_blob_with_retry(&url, bytes, content_type)
    }

    /// Creates a block blob exactly once. An existing name is reported without
    /// replacing its bytes, which makes the caller's artifact path immutable.
    pub fn upload_block_blob_bytes_if_absent(
        &mut self,
        name: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<ImmutableBlobWrite, AzureBlobError> {
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        self.put_block_blob_if_absent(&url, bytes, content_type, None)
    }

    /// Creates an immutable block blob directly in Azure's Hot tier.
    pub fn upload_hot_block_blob_bytes_if_absent(
        &mut self,
        name: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<ImmutableBlobWrite, AzureBlobError> {
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        self.put_block_blob_if_absent(&url, bytes, content_type, Some("Hot"))
    }

    /// Replaces a block blob only while it still has `expected_etag`.
    /// Returns `false` for an Azure 409/412 precondition conflict; callers can
    /// then re-read the pointer and fail closed or recognize an idempotent win.
    pub fn upload_block_blob_bytes_if_match(
        &mut self,
        name: &str,
        bytes: &[u8],
        content_type: &str,
        expected_etag: &str,
    ) -> Result<bool, AzureBlobError> {
        if expected_etag.trim().is_empty()
            || expected_etag
                .chars()
                .any(|character| character.is_control())
        {
            return Err(AzureBlobError::Transport(
                "conditional Azure Blob upload requires a valid ETag".to_owned(),
            ));
        }
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            match self.put_block_blob_if_match(&url, bytes, content_type, expected_etag) {
                Ok(updated) => return Ok(updated),
                Err(error) if error.is_retryable() && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure Blob conditional upload retry loop always returns");
    }

    /// Permanently seals the exact append-blob generation identified by a
    /// prior authenticated list or HEAD response.
    pub fn seal_append_blob_if_match(
        &mut self,
        name: &str,
        expected_etag: &str,
    ) -> Result<(), AzureBlobError> {
        if expected_etag.trim().is_empty()
            || expected_etag
                .chars()
                .any(|character| character.is_control())
        {
            return Err(AzureBlobError::Transport(
                "conditional Azure append blob seal requires a valid ETag".to_owned(),
            ));
        }
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        self.seal_append_blob_at_url(&url, expected_etag)
    }

    /// Acquires a finite Azure lease on a dedicated block blob. The lock blob
    /// is created once if it does not yet exist. Finite leases expire after a
    /// crashed worker stops renewing, preventing a permanent campaign lock.
    pub fn acquire_blob_lease(
        &mut self,
        name: &str,
        duration_seconds: u32,
    ) -> Result<BlobLeaseAcquireResult, AzureBlobError> {
        if !(15..=60).contains(&duration_seconds) {
            return Err(AzureBlobError::Transport(
                "Azure finite lease duration must be between 15 and 60 seconds".to_owned(),
            ));
        }
        let _ = self.upload_block_blob_bytes_if_absent(
            name,
            b"polyedge campaign lease\n",
            "text/plain",
        )?;
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}?comp=lease",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        match self.lease_request(&url, "acquire", None, Some(duration_seconds)) {
            Ok(response) => response
                .header("x-ms-lease-id")
                .filter(|value| !value.trim().is_empty())
                .map(|value| BlobLeaseAcquireResult::Acquired(value.to_owned()))
                .ok_or_else(|| {
                    AzureBlobError::Transport(
                        "Azure lease acquire response omitted x-ms-lease-id".to_owned(),
                    )
                }),
            Err(AzureBlobError::HttpStatus(409 | 412)) => Ok(BlobLeaseAcquireResult::AlreadyLeased),
            Err(error) => Err(error),
        }
    }

    pub fn renew_blob_lease(&mut self, name: &str, lease_id: &str) -> Result<bool, AzureBlobError> {
        validate_lease_id(lease_id)?;
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}?comp=lease",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        match self.lease_request_once(&url, "renew", Some(lease_id), None) {
            Ok(_) => Ok(true),
            Err(AzureBlobError::HttpStatus(409 | 412)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn release_blob_lease(
        &mut self,
        name: &str,
        lease_id: &str,
    ) -> Result<bool, AzureBlobError> {
        validate_lease_id(lease_id)?;
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}?comp=lease",
            self.account,
            self.container,
            encode_blob_path(name)
        );
        match self.lease_request(&url, "release", Some(lease_id), None) {
            Ok(_) => Ok(true),
            Err(AzureBlobError::HttpStatus(409 | 412)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn get_text(&mut self, url: &str) -> Result<String, AzureBlobError> {
        Ok(String::from_utf8(self.get_bytes_with_retry(url)?)?)
    }

    fn lease_request(
        &mut self,
        url: &str,
        action: &str,
        lease_id: Option<&str>,
        duration_seconds: Option<u32>,
    ) -> Result<ureq::Response, AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            match self.lease_request_once(url, action, lease_id, duration_seconds) {
                Ok(response) => return Ok(response),
                Err(AzureBlobError::HttpStatus(status)) => {
                    if is_retryable_azure_status(status) && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS {
                        thread::sleep(retry_delay(attempt));
                        continue;
                    }
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(AzureBlobError::Transport(message)) => {
                    if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS {
                        thread::sleep(retry_delay(attempt));
                        continue;
                    }
                    return Err(AzureBlobError::Transport(message));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure lease retry loop always returns");
    }

    fn seal_append_blob_at_url(
        &mut self,
        url: &str,
        expected_etag: &str,
    ) -> Result<(), AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            match self.seal_append_blob_once(url, expected_etag) {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retryable() && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure append blob seal retry loop always returns");
    }

    fn seal_append_blob_once(
        &mut self,
        url: &str,
        expected_etag: &str,
    ) -> Result<(), AzureBlobError> {
        let date = rfc1123_now();
        let seal_url = format!("{url}?comp=seal");
        let request = match &mut self.auth {
            AzureBlobAuth::Sas(sas) => self.agent.put(&append_sas(&seal_url, sas)),
            AzureBlobAuth::ManagedIdentity(token) => {
                let access_token = token.access_token(&self.agent)?;
                self.agent
                    .put(&seal_url)
                    .set("authorization", &format!("Bearer {access_token}"))
            }
        }
        .set("x-ms-version", AZURE_BLOB_API_VERSION)
        .set("x-ms-date", &date)
        .set("if-match", expected_etag)
        .set("content-length", "0");
        match request.call() {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(status, response)) => {
                let error_code = safe_azure_response_header(&response, "x-ms-error-code");
                let request_id = safe_azure_response_header(&response, "x-ms-request-id");
                let detail =
                    format!("seal PUT x-ms-error-code={error_code} x-ms-request-id={request_id}");
                if status == 409 && error_code == "BlobAlreadySealed" {
                    return match self.append_blob_is_sealed_if_match(url, expected_etag) {
                        Ok(true) => Ok(()),
                        Ok(false) => Err(AzureBlobError::HttpStatusDetail {
                            status,
                            detail: format!("{detail}; HEAD did not prove the listed sealed blob"),
                        }),
                        Err(error) => Err(error),
                    };
                }
                Err(AzureBlobError::HttpStatusDetail { status, detail })
            }
            Err(ureq::Error::Transport(_)) => Err(AzureBlobError::Transport(
                "Azure append blob seal request failed".to_owned(),
            )),
        }
    }

    fn append_blob_is_sealed_if_match(
        &mut self,
        url: &str,
        expected_etag: &str,
    ) -> Result<bool, AzureBlobError> {
        let date = rfc1123_now();
        let request = match &mut self.auth {
            AzureBlobAuth::Sas(sas) => self.agent.head(&append_sas(url, sas)),
            AzureBlobAuth::ManagedIdentity(token) => {
                let access_token = token.access_token(&self.agent)?;
                self.agent
                    .head(url)
                    .set("authorization", &format!("Bearer {access_token}"))
            }
        }
        .set("x-ms-version", AZURE_BLOB_API_VERSION)
        .set("x-ms-date", &date)
        .set("if-match", expected_etag);
        match request.call() {
            Ok(response) => Ok(response.header("etag") == Some(expected_etag)
                && response.header("x-ms-blob-type") == Some("AppendBlob")
                && response
                    .header("x-ms-blob-sealed")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true"))),
            Err(ureq::Error::Status(status, response)) => Err(AzureBlobError::HttpStatusDetail {
                status,
                detail: format!(
                    "seal verification HEAD x-ms-error-code={} x-ms-request-id={}",
                    safe_azure_response_header(&response, "x-ms-error-code"),
                    safe_azure_response_header(&response, "x-ms-request-id")
                ),
            }),
            Err(ureq::Error::Transport(_)) => Err(AzureBlobError::Transport(
                "Azure append blob seal verification request failed".to_owned(),
            )),
        }
    }

    fn lease_request_once(
        &mut self,
        url: &str,
        action: &str,
        lease_id: Option<&str>,
        duration_seconds: Option<u32>,
    ) -> Result<ureq::Response, AzureBlobError> {
        let date = rfc1123_now();
        let mut request = match &mut self.auth {
            AzureBlobAuth::Sas(sas) => self.agent.put(&append_sas(url, sas)),
            AzureBlobAuth::ManagedIdentity(token) => {
                let access_token = token.access_token(&self.agent)?;
                self.agent
                    .put(url)
                    .set("authorization", &format!("Bearer {access_token}"))
            }
        };
        request = request
            .set("x-ms-version", AZURE_BLOB_API_VERSION)
            .set("x-ms-date", &date)
            .set("x-ms-lease-action", action)
            .set("content-length", "0");
        if let Some(lease_id) = lease_id {
            request = request.set("x-ms-lease-id", lease_id);
        }
        if let Some(duration) = duration_seconds {
            request = request.set("x-ms-lease-duration", &duration.to_string());
        }
        match request.call() {
            Ok(response) => Ok(response),
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => Err(AzureBlobError::Transport(error.to_string())),
        }
    }

    fn get_bytes_with_retry(&mut self, url: &str) -> Result<Vec<u8>, AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let result = self.read_response_bytes(url);
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(error) if error.is_retryable() && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure Blob byte retry loop always returns");
    }

    fn read_response_bytes(&mut self, url: &str) -> Result<Vec<u8>, AzureBlobError> {
        let response = self.get_response(url)?;
        let mut bytes = Vec::new();
        response.into_reader().read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn read_response_bytes_exact_bounded(
        &mut self,
        url: &str,
        expected_bytes: u64,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, AzureBlobError> {
        if expected_bytes > maximum_bytes {
            return Err(AzureBlobError::Transport(
                "bounded Azure Blob download expectation exceeds its limit".to_owned(),
            ));
        }
        let response = self.get_response(url)?;
        if let Some(content_length) = response.header("content-length") {
            let advertised = content_length.parse::<u64>().map_err(|_| {
                AzureBlobError::Transport(
                    "bounded Azure Blob download has an invalid Content-Length".to_owned(),
                )
            })?;
            if advertised != expected_bytes || advertised > maximum_bytes {
                return Err(AzureBlobError::Transport(
                    "bounded Azure Blob download Content-Length disagrees".to_owned(),
                ));
            }
        }
        let limit = expected_bytes.checked_add(1).ok_or_else(|| {
            AzureBlobError::Transport("bounded Azure Blob download length overflows".to_owned())
        })?;
        let mut bytes = Vec::with_capacity(expected_bytes as usize);
        response.into_reader().take(limit).read_to_end(&mut bytes)?;
        if bytes.len() as u64 != expected_bytes {
            return Err(AzureBlobError::Transport(
                "bounded Azure Blob download body length disagrees".to_owned(),
            ));
        }
        Ok(bytes)
    }

    fn get_response(&mut self, url: &str) -> Result<ureq::Response, AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let date = rfc1123_now();
            let response = match &mut self.auth {
                AzureBlobAuth::Sas(sas) => self
                    .agent
                    .get(&append_sas(url, sas))
                    .set("x-ms-version", AZURE_BLOB_API_VERSION)
                    .call(),
                AzureBlobAuth::ManagedIdentity(token) => {
                    let access_token = token.access_token(&self.agent)?;
                    self.agent
                        .get(url)
                        .set("authorization", &format!("Bearer {access_token}"))
                        .set("x-ms-version", AZURE_BLOB_API_VERSION)
                        .set("x-ms-date", &date)
                        .call()
                }
            };
            match response {
                Ok(response) => return Ok(response),
                Err(ureq::Error::Status(status, response)) => {
                    if is_retryable_azure_status(status) && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS {
                        thread::sleep(retry_delay(attempt));
                        continue;
                    }
                    if status == 403 {
                        return Err(AzureBlobError::HttpStatusDetail {
                            status,
                            detail: format!(
                                "GET x-ms-error-code={} x-ms-request-id={}",
                                safe_azure_response_header(&response, "x-ms-error-code"),
                                safe_azure_response_header(&response, "x-ms-request-id")
                            ),
                        });
                    }
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(ureq::Error::Transport(error)) => {
                    let message = error.to_string();
                    if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS {
                        thread::sleep(retry_delay(attempt));
                        continue;
                    }
                    return Err(AzureBlobError::Transport(message));
                }
            }
        }
        unreachable!("Azure Blob retry loop always returns");
    }

    fn get_response_if_match(
        &mut self,
        url: &str,
        expected_etag: &str,
    ) -> Result<ureq::Response, AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let date = rfc1123_now();
            let response = match &mut self.auth {
                AzureBlobAuth::Sas(sas) => self
                    .agent
                    .get(&append_sas(url, sas))
                    .set("x-ms-version", AZURE_BLOB_API_VERSION)
                    .set("if-match", expected_etag)
                    .call(),
                AzureBlobAuth::ManagedIdentity(token) => {
                    let access_token = token.access_token(&self.agent)?;
                    self.agent
                        .get(url)
                        .set("authorization", &format!("Bearer {access_token}"))
                        .set("x-ms-version", AZURE_BLOB_API_VERSION)
                        .set("x-ms-date", &date)
                        .set("if-match", expected_etag)
                        .call()
                }
            };
            match response {
                Ok(response) => return Ok(response),
                Err(ureq::Error::Status(status, _)) => {
                    if is_retryable_azure_status(status) && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS {
                        thread::sleep(retry_delay(attempt));
                        continue;
                    }
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(ureq::Error::Transport(error)) => {
                    let message = error.to_string();
                    if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS {
                        thread::sleep(retry_delay(attempt));
                        continue;
                    }
                    return Err(AzureBlobError::Transport(message));
                }
            }
        }
        unreachable!("Azure Blob conditional read retry loop always returns");
    }

    fn put_block_blob_with_retry(
        &mut self,
        url: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(), AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let result = self.put_block_blob(url, bytes, content_type);
            match result {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retryable() && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure Blob upload retry loop always returns");
    }

    fn put_block_blob(
        &mut self,
        url: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(), AzureBlobError> {
        let date = rfc1123_now();
        let response = match &mut self.auth {
            AzureBlobAuth::Sas(sas) => self
                .agent
                .put(&append_sas(url, sas))
                .set("x-ms-version", AZURE_BLOB_API_VERSION)
                .set("x-ms-date", &date)
                .set("x-ms-blob-type", "BlockBlob")
                .set("content-type", content_type)
                .send_bytes(bytes),
            AzureBlobAuth::ManagedIdentity(token) => {
                let access_token = token.access_token(&self.agent)?;
                self.agent
                    .put(url)
                    .set("authorization", &format!("Bearer {access_token}"))
                    .set("x-ms-version", AZURE_BLOB_API_VERSION)
                    .set("x-ms-date", &date)
                    .set("x-ms-blob-type", "BlockBlob")
                    .set("content-type", content_type)
                    .send_bytes(bytes)
            }
        };
        match response {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => Err(AzureBlobError::Transport(error.to_string())),
        }
    }

    fn put_block_blob_if_absent(
        &mut self,
        url: &str,
        bytes: &[u8],
        content_type: &str,
        access_tier: Option<&str>,
    ) -> Result<ImmutableBlobWrite, AzureBlobError> {
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            match self.put_block_blob_if_absent_once(url, bytes, content_type, access_tier) {
                Ok(result) => return Ok(result),
                Err(error) if error.is_retryable() && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("Azure Blob immutable upload retry loop always returns");
    }

    fn put_block_blob_if_absent_once(
        &mut self,
        url: &str,
        bytes: &[u8],
        content_type: &str,
        access_tier: Option<&str>,
    ) -> Result<ImmutableBlobWrite, AzureBlobError> {
        let date = rfc1123_now();
        let response = match &mut self.auth {
            AzureBlobAuth::Sas(sas) => {
                let request = self
                    .agent
                    .put(&append_sas(url, sas))
                    .set("x-ms-version", AZURE_BLOB_API_VERSION)
                    .set("x-ms-date", &date)
                    .set("x-ms-blob-type", "BlockBlob")
                    .set("content-type", content_type)
                    .set("if-none-match", "*");
                let request = match access_tier {
                    Some(tier) => request.set("x-ms-access-tier", tier),
                    None => request,
                };
                request.send_bytes(bytes)
            }
            AzureBlobAuth::ManagedIdentity(token) => {
                let access_token = token.access_token(&self.agent)?;
                let request = self
                    .agent
                    .put(url)
                    .set("authorization", &format!("Bearer {access_token}"))
                    .set("x-ms-version", AZURE_BLOB_API_VERSION)
                    .set("x-ms-date", &date)
                    .set("x-ms-blob-type", "BlockBlob")
                    .set("content-type", content_type)
                    .set("if-none-match", "*");
                let request = match access_tier {
                    Some(tier) => request.set("x-ms-access-tier", tier),
                    None => request,
                };
                request.send_bytes(bytes)
            }
        };
        match response {
            Ok(_) => Ok(ImmutableBlobWrite::Created),
            Err(ureq::Error::Status(409 | 412, _)) => Ok(ImmutableBlobWrite::AlreadyExists),
            Err(ureq::Error::Status(403, response)) => {
                let error_code = safe_azure_response_header(&response, "x-ms-error-code");
                let request_id = safe_azure_response_header(&response, "x-ms-request-id");
                match self.get_response(url) {
                    Ok(_) => Ok(ImmutableBlobWrite::AlreadyExists),
                    Err(error) => {
                        let follow_up = match error {
                            AzureBlobError::HttpStatus(status) => format!("HTTP {status}"),
                            AzureBlobError::HttpStatusDetail { status, .. } => {
                                format!("HTTP {status}")
                            }
                            _ => "non-HTTP failure".to_owned(),
                        };
                        Err(AzureBlobError::HttpStatusDetail {
                            status: 403,
                            detail: format!(
                                "immutable PUT x-ms-error-code={error_code} \
                                 x-ms-request-id={request_id}; follow-up GET {follow_up}"
                            ),
                        })
                    }
                }
            }
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => Err(AzureBlobError::Transport(error.to_string())),
        }
    }

    fn put_block_blob_if_match(
        &mut self,
        url: &str,
        bytes: &[u8],
        content_type: &str,
        expected_etag: &str,
    ) -> Result<bool, AzureBlobError> {
        let date = rfc1123_now();
        let response = match &mut self.auth {
            AzureBlobAuth::Sas(sas) => self
                .agent
                .put(&append_sas(url, sas))
                .set("x-ms-version", AZURE_BLOB_API_VERSION)
                .set("x-ms-date", &date)
                .set("x-ms-blob-type", "BlockBlob")
                .set("content-type", content_type)
                .set("if-match", expected_etag)
                .send_bytes(bytes),
            AzureBlobAuth::ManagedIdentity(token) => {
                let access_token = token.access_token(&self.agent)?;
                self.agent
                    .put(url)
                    .set("authorization", &format!("Bearer {access_token}"))
                    .set("x-ms-version", AZURE_BLOB_API_VERSION)
                    .set("x-ms-date", &date)
                    .set("x-ms-blob-type", "BlockBlob")
                    .set("content-type", content_type)
                    .set("if-match", expected_etag)
                    .send_bytes(bytes)
            }
        };
        match response {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(409 | 412, _)) => Ok(false),
            Err(ureq::Error::Status(status, _)) => Err(AzureBlobError::HttpStatus(status)),
            Err(ureq::Error::Transport(error)) => Err(AzureBlobError::Transport(error.to_string())),
        }
    }
}

#[derive(Clone)]
pub struct AzureTableClient {
    account: String,
    agent: ureq::Agent,
    auth: AzureTableAuth,
}

#[derive(Clone)]
enum AzureTableAuth {
    ManagedIdentity(ManagedIdentityToken),
    SharedKey(String),
}

impl AzureTableClient {
    pub fn new(account: impl Into<String>, client_id: Option<String>) -> Self {
        Self {
            account: account.into(),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(AZURE_TABLE_CONNECT_TIMEOUT)
                .timeout_read(AZURE_TABLE_READ_TIMEOUT)
                .timeout_write(AZURE_TABLE_WRITE_TIMEOUT)
                .build(),
            auth: AzureTableAuth::ManagedIdentity(ManagedIdentityToken::new(client_id)),
        }
    }

    pub fn with_account_key(account: impl Into<String>, account_key: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(AZURE_TABLE_CONNECT_TIMEOUT)
                .timeout_read(AZURE_TABLE_READ_TIMEOUT)
                .timeout_write(AZURE_TABLE_WRITE_TIMEOUT)
                .build(),
            auth: AzureTableAuth::SharedKey(account_key.into()),
        }
    }

    pub fn query_entities(
        &mut self,
        table: &str,
        filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Value>, AzureBlobError> {
        let mut entities = Vec::new();
        let mut continuation: Option<(String, String)> = None;
        while entities.len() < limit {
            let top = (limit - entities.len()).min(1000);
            let (page, next) = self.query_page(table, filter, top, continuation.as_ref())?;
            if page.is_empty() {
                break;
            }
            entities.extend(page);
            continuation = next;
            if continuation.is_none() {
                break;
            }
        }
        Ok(entities)
    }

    pub fn insert_or_merge_entity(
        &mut self,
        table: &str,
        entity: &Value,
    ) -> Result<(), AzureBlobError> {
        let partition_key = entity
            .get("PartitionKey")
            .and_then(Value::as_str)
            .ok_or_else(|| AzureBlobError::Transport("missing Table PartitionKey".to_owned()))?;
        let row_key = entity
            .get("RowKey")
            .and_then(Value::as_str)
            .ok_or_else(|| AzureBlobError::Transport("missing Table RowKey".to_owned()))?;
        let resource_path = table_entity_path(table, partition_key, row_key);
        let url = format!(
            "https://{}.table.core.windows.net/{}",
            self.account, resource_path
        );
        self.send_table_entity("MERGE", &url, &resource_path, entity)
    }

    fn query_page(
        &mut self,
        table: &str,
        filter: Option<&str>,
        top: usize,
        continuation: Option<&(String, String)>,
    ) -> Result<AzureTablePage, AzureBlobError> {
        let mut params = vec![("$top".to_owned(), top.to_string())];
        if let Some(filter) = filter {
            params.push(("$filter".to_owned(), filter.to_owned()));
        }
        if let Some((partition_key, row_key)) = continuation {
            params.push(("NextPartitionKey".to_owned(), partition_key.clone()));
            params.push(("NextRowKey".to_owned(), row_key.clone()));
        }
        let query = params
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    utf8_percent_encode(key, NON_ALPHANUMERIC),
                    utf8_percent_encode(value, NON_ALPHANUMERIC)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        let url = format!(
            "https://{}.table.core.windows.net/{}()?{}",
            self.account, table, query
        );
        let resource_path = format!("{table}()");
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let date = rfc1123_now();
            let authorization = self.table_authorization(&resource_path, &date)?;
            let response = self
                .agent
                .get(&url)
                .set("authorization", &authorization)
                .set("x-ms-version", AZURE_BLOB_API_VERSION)
                .set("x-ms-date", &date)
                .set("Date", &date)
                .set("Accept", "application/json;odata=nometadata")
                .set("DataServiceVersion", "3.0;NetFx")
                .set("MaxDataServiceVersion", "3.0;NetFx")
                .call();
            match response {
                Ok(response) => return parse_table_response(response),
                Err(ureq::Error::Status(status, _))
                    if is_retryable_azure_status(status)
                        && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS =>
                {
                    thread::sleep(retry_delay(attempt));
                }
                Err(ureq::Error::Status(status, _)) => {
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(ureq::Error::Transport(error)) if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                    let _ = error;
                }
                Err(ureq::Error::Transport(error)) => {
                    return Err(AzureBlobError::Transport(error.to_string()));
                }
            }
        }
        unreachable!("Azure Table retry loop always returns");
    }

    fn send_table_entity(
        &mut self,
        method: &str,
        url: &str,
        resource_path: &str,
        entity: &Value,
    ) -> Result<(), AzureBlobError> {
        let body = serde_json::to_string(entity)?;
        for attempt in 0..AZURE_BLOB_MAX_ATTEMPTS {
            let date = rfc1123_now();
            let authorization = self.table_authorization(resource_path, &date)?;
            let request = self
                .agent
                .request(method, url)
                .set("authorization", &authorization)
                .set("x-ms-version", AZURE_BLOB_API_VERSION)
                .set("x-ms-date", &date)
                .set("Date", &date)
                .set("Accept", "application/json;odata=nometadata")
                .set("Content-Type", "application/json")
                .set("DataServiceVersion", "3.0;NetFx")
                .set("MaxDataServiceVersion", "3.0;NetFx")
                .set("Prefer", "return-no-content");
            match request.send_string(&body) {
                Ok(_) => return Ok(()),
                Err(ureq::Error::Status(status, _))
                    if is_retryable_azure_status(status)
                        && attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS =>
                {
                    thread::sleep(retry_delay(attempt));
                }
                Err(ureq::Error::Status(status, _)) => {
                    return Err(AzureBlobError::HttpStatus(status));
                }
                Err(ureq::Error::Transport(error)) if attempt + 1 < AZURE_BLOB_MAX_ATTEMPTS => {
                    thread::sleep(retry_delay(attempt));
                    let _ = error;
                }
                Err(ureq::Error::Transport(error)) => {
                    return Err(AzureBlobError::Transport(error.to_string()));
                }
            }
        }
        unreachable!("Azure Table entity retry loop always returns");
    }

    fn table_authorization(
        &mut self,
        resource_path: &str,
        date: &str,
    ) -> Result<String, AzureBlobError> {
        match &mut self.auth {
            AzureTableAuth::ManagedIdentity(token) => {
                Ok(format!("Bearer {}", token.access_token(&self.agent)?))
            }
            AzureTableAuth::SharedKey(account_key) => {
                shared_key_lite_header(&self.account, account_key, resource_path, date)
            }
        }
    }
}

fn shared_key_lite_header(
    account: &str,
    account_key: &str,
    resource_path: &str,
    date: &str,
) -> Result<String, AzureBlobError> {
    let key = general_purpose::STANDARD
        .decode(account_key.trim())
        .map_err(|error| AzureBlobError::InvalidStorageKey(error.to_string()))?;
    let string_to_sign = format!("{date}\n/{account}/{resource_path}");
    let mut mac = HmacSha256::new_from_slice(&key)
        .map_err(|error| AzureBlobError::InvalidStorageKey(error.to_string()))?;
    mac.update(string_to_sign.as_bytes());
    let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    Ok(format!("SharedKeyLite {account}:{signature}"))
}

fn table_entity_path(table: &str, partition_key: &str, row_key: &str) -> String {
    format!(
        "{}(PartitionKey='{}',RowKey='{}')",
        table,
        odata_key(partition_key),
        odata_key(row_key)
    )
}

fn odata_key(value: &str) -> String {
    value.replace('\'', "''")
}

fn parse_table_response(response: ureq::Response) -> Result<AzureTablePage, AzureBlobError> {
    let next_partition_key = response
        .header("x-ms-continuation-NextPartitionKey")
        .map(str::to_owned);
    let next_row_key = response
        .header("x-ms-continuation-NextRowKey")
        .map(str::to_owned);
    let text = response.into_string()?;
    let payload: Value = serde_json::from_str(&text)?;
    let entities = payload
        .get("value")
        .or_else(|| payload.get("items"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let continuation = match (next_partition_key, next_row_key) {
        (Some(partition_key), Some(row_key)) => Some((partition_key, row_key)),
        _ => None,
    };
    Ok((entities, continuation))
}

fn is_retryable_azure_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(250 * 2_u64.pow(attempt.min(4) as u32))
}

fn validate_lease_id(lease_id: &str) -> Result<(), AzureBlobError> {
    if lease_id.trim().is_empty()
        || lease_id.len() > 128
        || lease_id.chars().any(|character| character.is_control())
    {
        return Err(AzureBlobError::Transport(
            "Azure lease ID is empty or invalid".to_owned(),
        ));
    }
    Ok(())
}

fn read_versioned_blob_response(
    response: ureq::Response,
) -> Result<VersionedBlobBytes, AzureBlobError> {
    let etag = response
        .header("etag")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AzureBlobError::Transport("Azure Blob response omitted ETag".to_owned()))?
        .to_owned();
    let version_id = response
        .header("x-ms-version-id")
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned);
    let content_md5 = response
        .header("content-md5")
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned);
    let blob_type = response
        .header("x-ms-blob-type")
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned);
    let reported_sealed = response
        .header("x-ms-blob-sealed")
        .and_then(|value| value.parse::<bool>().ok());
    // Azure omits x-ms-blob-sealed for an unsealed append blob. Normalize
    // that documented wire representation so downstream evidence records the
    // physical state explicitly instead of confusing false with unknown.
    let sealed = if blob_type.as_deref() == Some("AppendBlob") {
        Some(reported_sealed.unwrap_or(false))
    } else {
        reported_sealed
    };
    let mut bytes = Vec::new();
    response.into_reader().read_to_end(&mut bytes)?;
    Ok(VersionedBlobBytes {
        bytes,
        etag,
        version_id,
        content_md5,
        blob_type,
        sealed,
    })
}

#[derive(Default)]
struct BlobListPage {
    blobs: Vec<AzureBlobItem>,
    next_marker: String,
}

fn parse_blob_list(xml: &str) -> Result<BlobListPage, AzureBlobError> {
    let mut reader = quick_xml::Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut page = BlobListPage::default();
    let mut current_tag = String::new();
    let mut in_blob = false;
    let mut name = String::new();
    let mut etag = String::new();
    let mut version_id = None;
    let mut is_current_version = None;
    let mut content_md5 = None;
    let mut blob_type = None;
    let mut sealed = None;
    let mut content_length = 0_u64;
    let mut last_modified = None;
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) => {
                current_tag = String::from_utf8_lossy(event.name().as_ref()).into_owned();
                if current_tag == "Blob" {
                    in_blob = true;
                    name.clear();
                    etag.clear();
                    version_id = None;
                    is_current_version = None;
                    content_md5 = None;
                    blob_type = None;
                    sealed = None;
                    content_length = 0;
                    last_modified = None;
                }
            }
            Event::End(event) => {
                let tag = String::from_utf8_lossy(event.name().as_ref()).into_owned();
                if tag == "Blob" && in_blob {
                    if name.is_empty() || etag.trim().is_empty() {
                        return Err(AzureBlobError::XmlMessage(
                            "Azure blob listing omitted Name or ETag".to_owned(),
                        ));
                    }
                    // List Blobs returns Sealed only when an append blob is
                    // sealed. Its absence on AppendBlob therefore means false,
                    // not an unavailable physical-seal state.
                    let normalized_sealed = if blob_type.as_deref() == Some("AppendBlob") {
                        Some(sealed.unwrap_or(false))
                    } else {
                        sealed
                    };
                    page.blobs.push(AzureBlobItem {
                        name: name.clone(),
                        etag: etag.clone(),
                        version_id: version_id.clone(),
                        is_current_version,
                        content_md5: content_md5.clone(),
                        blob_type: blob_type.clone(),
                        sealed: normalized_sealed,
                        content_length,
                        last_modified,
                    });
                    in_blob = false;
                }
                current_tag.clear();
            }
            Event::Text(event) => {
                let text = event
                    .unescape()
                    .map_err(|error| AzureBlobError::XmlMessage(error.to_string()))?
                    .into_owned();
                if in_blob && current_tag == "Name" {
                    name = text;
                } else if in_blob && current_tag == "Etag" {
                    etag = text;
                } else if in_blob && current_tag == "VersionId" {
                    version_id = (!text.is_empty()).then_some(text);
                } else if in_blob && current_tag == "IsCurrentVersion" {
                    is_current_version = text.parse::<bool>().ok();
                } else if in_blob && current_tag == "Content-MD5" {
                    content_md5 = (!text.is_empty()).then_some(text);
                } else if in_blob && current_tag == "BlobType" {
                    blob_type = (!text.is_empty()).then_some(text);
                } else if in_blob && current_tag == "Sealed" {
                    sealed = text.parse::<bool>().ok();
                } else if in_blob && current_tag == "Content-Length" {
                    content_length = text.parse().unwrap_or(0);
                } else if in_blob && current_tag == "Last-Modified" {
                    last_modified = DateTime::parse_from_rfc2822(&text)
                        .ok()
                        .map(|timestamp| timestamp.with_timezone(&Utc));
                } else if !in_blob && current_tag == "NextMarker" {
                    page.next_marker = text;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(page)
}

fn append_sas(url: &str, sas: &str) -> String {
    let trimmed = sas.trim_start_matches('?');
    if url.contains('?') {
        format!("{url}&{trimmed}")
    } else {
        format!("{url}?{trimmed}")
    }
}

fn encode_blob_path(name: &str) -> String {
    name.split('/')
        .map(|segment| utf8_percent_encode(segment, PATH_SEGMENT_ENCODE_SET).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    pub version: String,
    pub category: String,
    pub action: String,
    pub actor: Option<String>,
    pub source: String,
    pub reason: Option<String>,
    pub created_ts: DateTime<Utc>,
    pub before: Value,
    pub after: Value,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryAuditLog {
    entries: Vec<AuditEntry>,
}

impl InMemoryAuditLog {
    pub fn record(
        &mut self,
        category: impl Into<String>,
        action: impl Into<String>,
        before: Value,
        after: Value,
    ) -> AuditEntry {
        let entry = AuditEntry {
            version: format!("rust-{}", self.entries.len() + 1),
            category: category.into(),
            action: action.into(),
            actor: None,
            source: "api".to_owned(),
            reason: None,
            created_ts: Utc::now(),
            before,
            after,
            metadata: Value::Null,
        };
        self.entries.push(entry.clone());
        entry
    }

    pub fn history(&self, limit: usize) -> Vec<AuditEntry> {
        self.entries.iter().rev().take(limit).cloned().collect()
    }
}

#[derive(Clone, Debug)]
pub struct LocalReportStore {
    root: PathBuf,
}

impl LocalReportStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn write_latest(&self, payload: &Value) -> Result<PathBuf, StorageError> {
        fs::create_dir_all(&self.root)?;
        let path = self.root.join("latest-report.json");
        fs::write(&path, serde_json::to_vec_pretty(payload)?)?;
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyedge_domain::RuntimeEvent;
    use serde_json::json;
    use std::io::{BufRead, BufReader};
    use std::net::TcpStream;

    const TEST_TENANT_ID: &str = "11111111-2222-3333-4444-555555555555";
    const TEST_CLIENT_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    #[test]
    fn large_immutable_upload_client_keeps_managed_identity_binding() {
        let client = AzureBlobClient::with_managed_identity_for_large_immutable_upload(
            "account",
            "container",
            Some(TEST_CLIENT_ID.to_owned()),
        );
        assert_eq!(client.account, "account");
        assert_eq!(client.container, "container");
        assert!(matches!(client.auth, AzureBlobAuth::ManagedIdentity(_)));
    }

    #[test]
    fn bounded_blob_download_rejects_oversized_header_and_body_without_leaking_body() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_http_request(&stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nsecret-body"
            )
            .unwrap();

            let (mut stream, _) = listener.accept().unwrap();
            read_http_request(&stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nsecret-body"
            )
            .unwrap();
        });

        let mut client = AzureBlobClient::new("unused", "unused", "test=sas");
        let url = format!("http://{address}/bounded");
        for error in [
            client
                .read_response_bytes_exact_bounded(&url, 4, 8)
                .unwrap_err(),
            client
                .read_response_bytes_exact_bounded(&url, 4, 8)
                .unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains("bounded Azure Blob download"));
            assert!(!message.contains("secret-body"));
        }
        server.join().unwrap();
    }

    #[test]
    fn http_queue_sender_preserves_message_binding() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_http_request(&stream);
            assert!(request.starts_with("POST /v1/messages HTTP/1.1\r\n"));
            assert!(request
                .to_ascii_lowercase()
                .contains("x-polyedge-message-id: decision-1\r\n"));
            assert!(request
                .to_ascii_lowercase()
                .contains("x-polyedge-ttl-seconds: 3600\r\n"));
            write!(
                stream,
                "HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });

        HttpJsonQueueSender::new(format!("http://{address}/v1/messages"))
            .send_json("decision-1", 3600, &json!({"decision_id": "decision-1"}))
            .unwrap();
        server.join().unwrap();
    }

    fn read_http_request(stream: &TcpStream) -> String {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            request.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        let content_length = request
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
            .unwrap_or(0);
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        request
    }

    fn external_auth_options(
        tenant_id: Option<&str>,
        client_id: Option<&str>,
        secret_file: Option<&str>,
        federated_token_file: Option<&str>,
    ) -> (
        Option<OsString>,
        Option<OsString>,
        Option<OsString>,
        Option<OsString>,
    ) {
        (
            tenant_id.map(OsString::from),
            client_id.map(OsString::from),
            secret_file.map(OsString::from),
            federated_token_file.map(OsString::from),
        )
    }

    #[test]
    fn external_auth_selection_is_explicit_and_fail_closed() {
        assert_eq!(
            select_token_auth(None, None, None, None, Some(TEST_CLIENT_ID)).unwrap(),
            AzureTokenAuth::ManagedIdentity
        );
        assert_eq!(
            select_token_auth(
                None,
                Some(OsString::from(TEST_CLIENT_ID)),
                None,
                None,
                Some(TEST_CLIENT_ID),
            )
            .unwrap(),
            AzureTokenAuth::ManagedIdentity
        );

        let (tenant, client, secret_file, federated_token_file) = external_auth_options(
            Some(TEST_TENANT_ID),
            Some(TEST_CLIENT_ID),
            Some("/run/credentials/polyedge/azure-client-secret"),
            None,
        );
        assert!(matches!(
            select_token_auth(
                tenant,
                client,
                secret_file,
                federated_token_file,
                Some(TEST_CLIENT_ID)
            )
            .unwrap(),
            AzureTokenAuth::ClientSecretFile(_)
        ));

        let (tenant, client, secret_file, federated_token_file) = external_auth_options(
            Some(TEST_TENANT_ID),
            Some(TEST_CLIENT_ID),
            None,
            Some("/run/credentials/polyedge/federated-token"),
        );
        assert!(matches!(
            select_token_auth(tenant, client, secret_file, federated_token_file, None).unwrap(),
            AzureTokenAuth::FederatedTokenFile(_)
        ));

        let (tenant, client, secret_file, federated_token_file) =
            external_auth_options(Some(TEST_TENANT_ID), Some(TEST_CLIENT_ID), None, None);
        assert!(
            select_token_auth(tenant, client, secret_file, federated_token_file, None).is_err()
        );

        let (tenant, client, secret_file, federated_token_file) = external_auth_options(
            Some(TEST_TENANT_ID),
            Some(TEST_CLIENT_ID),
            Some("/run/credentials/polyedge/azure-client-secret"),
            Some("/run/credentials/polyedge/federated-token"),
        );
        assert!(
            select_token_auth(tenant, client, secret_file, federated_token_file, None).is_err()
        );

        let (tenant, client, secret_file, federated_token_file) = external_auth_options(
            Some("not-a-guid"),
            Some(TEST_CLIENT_ID),
            Some("/run/credentials/polyedge/azure-client-secret"),
            None,
        );
        assert!(
            select_token_auth(tenant, client, secret_file, federated_token_file, None).is_err()
        );

        let (tenant, client, secret_file, federated_token_file) = external_auth_options(
            Some(TEST_TENANT_ID),
            Some(TEST_CLIENT_ID),
            None,
            Some("relative-federated-token"),
        );
        assert!(
            select_token_auth(tenant, client, secret_file, federated_token_file, None).is_err()
        );
    }

    #[test]
    fn external_token_request_uses_v2_scope_and_form_encoding() {
        let config = ExternalAzureAuth {
            tenant_id: TEST_TENANT_ID.to_owned(),
            client_id: TEST_CLIENT_ID.to_owned(),
            credential_file: PathBuf::from("/unused"),
        };
        let (url, body) =
            external_token_request(&config, "https://storage.azure.com/", "test@ secret");
        assert_eq!(
            url,
            format!("{AZURE_AUTHORITY_HOST}/{TEST_TENANT_ID}/oauth2/v2.0/token")
        );
        assert!(body.contains("scope=https%3A%2F%2Fstorage%2Eazure%2Ecom%2F%2Edefault"));
        assert!(body.contains("grant_type=client_credentials"));
        assert!(!body.contains("test@ secret"));
    }

    #[test]
    fn federated_token_request_uses_exact_v2_assertion_form_without_secret() {
        let config = ExternalAzureAuth {
            tenant_id: TEST_TENANT_ID.to_owned(),
            client_id: TEST_CLIENT_ID.to_owned(),
            credential_file: PathBuf::from("/unused"),
        };
        let assertion = "header.payload.signature";
        let (url, body) = federated_token_request(&config, "https://storage.azure.com/", assertion);
        assert_eq!(
            url,
            format!("{AZURE_AUTHORITY_HOST}/{TEST_TENANT_ID}/oauth2/v2.0/token")
        );
        assert_eq!(
            body,
            format!(
                "client_id={}&scope=https%3A%2F%2Fstorage%2Eazure%2Ecom%2F%2Edefault&grant_type=client_credentials&client_assertion_type={}&client_assertion=header%2Epayload%2Esignature",
                utf8_percent_encode(TEST_CLIENT_ID, NON_ALPHANUMERIC),
                utf8_percent_encode(AZURE_CLIENT_ASSERTION_TYPE, NON_ALPHANUMERIC),
            )
        );
        assert!(!body.contains("client_secret"));
    }

    #[test]
    fn access_token_cache_requires_same_auth_and_more_than_120_seconds() {
        let auth = AzureTokenAuth::FederatedTokenFile(ExternalAzureAuth {
            tenant_id: TEST_TENANT_ID.to_owned(),
            client_id: TEST_CLIENT_ID.to_owned(),
            credential_file: PathBuf::from("/run/credentials/token-a"),
        });
        let different_path = AzureTokenAuth::FederatedTokenFile(ExternalAzureAuth {
            tenant_id: TEST_TENANT_ID.to_owned(),
            client_id: TEST_CLIENT_ID.to_owned(),
            credential_file: PathBuf::from("/run/credentials/token-b"),
        });
        assert!(cache_is_valid(Some(&auth), &auth, Some(1_121), 1_000));
        assert!(!cache_is_valid(Some(&auth), &auth, Some(1_120), 1_000));
        assert!(!cache_is_valid(
            Some(&auth),
            &different_path,
            Some(1_121),
            1_000
        ));
        assert!(!cache_is_valid(None, &auth, Some(1_121), 1_000));
    }

    #[test]
    fn arc_challenge_header_is_restricted_to_the_agent_token_directory() {
        assert_eq!(
            arc_identity_challenge_path("Basic realm=/var/opt/azcmagent/tokens/test-only.key")
                .unwrap(),
            PathBuf::from("/var/opt/azcmagent/tokens/test-only.key")
        );
        assert!(arc_identity_challenge_path(
            "Basic realm=\"/var/opt/azcmagent/tokens/quoted.key\""
        )
        .is_ok());
        assert!(arc_identity_challenge_path("Basic realm=/etc/shadow").is_err());
        assert!(arc_identity_challenge_path(
            "Basic realm=/var/opt/azcmagent/tokens/nested/challenge.key"
        )
        .is_err());
        assert!(arc_identity_challenge_path("Bearer token").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn client_secret_file_rejects_empty_large_linked_and_open_permissions() {
        use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};

        let dir = std::env::temp_dir().join(format!(
            "polyedge-external-auth-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        fs::create_dir_all(&dir).unwrap();
        let secure = dir.join("secure");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&secure)
            .unwrap();
        file.write_all(b"test-only-value\n").unwrap();
        drop(file);
        assert_eq!(
            read_credential_file(&secure, "AZURE_CLIENT_SECRET_FILE").unwrap(),
            "test-only-value"
        );

        let empty = dir.join("empty");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&empty)
            .unwrap();
        assert!(read_credential_file(&empty, "AZURE_CLIENT_SECRET_FILE").is_err());

        let large = dir.join("large");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&large)
            .unwrap();
        file.write_all(&vec![b'x'; AZURE_CLIENT_SECRET_MAX_BYTES as usize + 1])
            .unwrap();
        drop(file);
        assert!(read_credential_file(&large, "AZURE_CLIENT_SECRET_FILE").is_err());

        let open = dir.join("open");
        fs::write(&open, b"test-only-value").unwrap();
        fs::set_permissions(&open, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_credential_file(&open, "AZURE_CLIENT_SECRET_FILE").is_err());

        let linked = dir.join("linked");
        symlink(&secure, &linked).unwrap();
        assert!(read_credential_file(&linked, "AZURE_CLIENT_SECRET_FILE").is_err());

        let nested = dir.join("nested");
        fs::create_dir(&nested).unwrap();
        assert!(read_credential_file(
            &nested.join("..").join("secure"),
            "AZURE_CLIENT_SECRET_FILE"
        )
        .is_err());
        assert!(
            read_credential_file(&dir.join(".").join("secure"), "AZURE_CLIENT_SECRET_FILE")
                .is_err()
        );

        let linked_parent = dir.join("linked-parent");
        symlink(&dir, &linked_parent).unwrap();
        assert!(
            read_credential_file(&linked_parent.join("secure"), "AZURE_CLIENT_SECRET_FILE")
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn federated_token_file_rejects_invalid_files_and_rereads_atomic_replacement() {
        use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};

        let dir = std::env::temp_dir().join(format!(
            "polyedge-federated-auth-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        fs::create_dir_all(&dir).unwrap();
        let token = dir.join("token");
        let write_secure = |path: &Path, value: &[u8]| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path)
                .unwrap();
            file.write_all(value).unwrap();
        };
        write_secure(&token, b"b2xk.cGF5bG9hZA.c2lnbmF0dXJl\n");
        assert_eq!(
            read_federated_token_file(&token).unwrap(),
            "b2xk.cGF5bG9hZA.c2lnbmF0dXJl"
        );

        let replacement = dir.join("token.next");
        write_secure(&replacement, b"bmV3.cGF5bG9hZA.c2lnbmF0dXJl\n");
        fs::rename(&replacement, &token).unwrap();
        assert_eq!(
            read_federated_token_file(&token).unwrap(),
            "bmV3.cGF5bG9hZA.c2lnbmF0dXJl"
        );

        let empty = dir.join("empty");
        write_secure(&empty, b"");
        assert!(read_credential_file(&empty, "AZURE_FEDERATED_TOKEN_FILE").is_err());

        for (name, malformed) in [
            ("two-segments", b"aGVhZGVy.cGF5bG9hZA".as_slice()),
            ("empty-segment", b"aGVhZGVy..c2lnbmF0dXJl".as_slice()),
            ("padding", b"aGVhZGVy=.cGF5bG9hZA.c2lnbmF0dXJl".as_slice()),
            (
                "non-base64url",
                b"aGVhZGVy+.cGF5bG9hZA.c2lnbmF0dXJl".as_slice(),
            ),
        ] {
            let path = dir.join(name);
            write_secure(&path, malformed);
            let error = read_federated_token_file(&path).unwrap_err().to_string();
            assert!(!error.contains(std::str::from_utf8(malformed).unwrap()));
        }

        let control = dir.join("control");
        write_secure(&control, b"bad\nassertion");
        let error = read_credential_file(&control, "AZURE_FEDERATED_TOKEN_FILE")
            .unwrap_err()
            .to_string();
        assert!(!error.contains("bad\nassertion"));

        let open = dir.join("open");
        fs::write(&open, b"assertion").unwrap();
        fs::set_permissions(&open, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_credential_file(&open, "AZURE_FEDERATED_TOKEN_FILE").is_err());

        let linked = dir.join("linked");
        symlink(&token, &linked).unwrap();
        assert!(read_credential_file(&linked, "AZURE_FEDERATED_TOKEN_FILE").is_err());

        let nested = dir.join("nested");
        fs::create_dir(&nested).unwrap();
        assert!(read_federated_token_file(&nested.join("..").join("token")).is_err());
        assert!(read_federated_token_file(&dir.join(".").join("token")).is_err());

        let linked_parent = dir.join("linked-parent");
        symlink(&dir, &linked_parent).unwrap();
        assert!(read_federated_token_file(&linked_parent.join("token")).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn event_blob_prefix_is_configurable_and_defaults_to_events() {
        let event = RuntimeEvent {
            event_type: "decision".to_owned(),
            ts: DateTime::parse_from_rfc3339("2026-07-12T12:34:00Z")
                .unwrap()
                .with_timezone(&Utc),
            data: Value::Null,
        };
        assert_eq!(
            event_blob_name("events", &event),
            "events/2026/07/12/12/34.jsonl"
        );
        assert_eq!(
            event_blob_name("shadow-events", &event),
            "shadow-events/2026/07/12/12/34.jsonl"
        );
        assert_eq!(
            normalize_blob_prefix(" /shadow-events/ ".to_owned()),
            "shadow-events"
        );
        assert_eq!(normalize_blob_prefix("///".to_owned()), "events");
    }

    #[test]
    fn append_blob_prefix_cutover_routes_by_event_timestamp() {
        let cutover = DateTime::parse_from_rfc3339("2026-07-22T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let recorder = AzureAppendBlobRecorder::new_with_prefix_cutover(
            "account",
            "container",
            None,
            "shadow-events/old",
            Some("shadow-events/new"),
            Some(cutover),
        );
        let event = |ts: &str| RuntimeEvent {
            event_type: "decision".to_owned(),
            ts: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            data: Value::Null,
        };
        assert_eq!(
            recorder.event_blob_name(&event("2026-07-21T23:59:59.999Z")),
            "shadow-events/old/2026/07/21/23/59.jsonl"
        );
        assert_eq!(
            recorder.event_blob_name(&event("2026-07-22T00:00:00Z")),
            "shadow-events/new/2026/07/22/00/00.jsonl"
        );
        assert_eq!(
            recorder.event_blob_name(&event("2026-07-22T00:00:00.001Z")),
            "shadow-events/new/2026/07/22/00/00.jsonl"
        );
    }

    #[test]
    fn buffered_append_blocks_preserves_lines_and_caps_chunks() {
        let events: Vec<_> = (0..5)
            .map(|index| RuntimeEvent {
                event_type: "book".to_owned(),
                ts: Utc::now(),
                data: json!({
                    "index": index,
                    "padding": "x".repeat(60)
                }),
            })
            .collect();
        let mut buffer = BufferedAppendBlocks::new(220);
        let mut chunks = Vec::new();
        for event in &events {
            chunks.extend(buffer.push_line(
                "events/2026/06/14/12/events.jsonl",
                jsonl_event_line(event).unwrap(),
            ));
        }
        chunks.extend(buffer.drain());

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|(_, chunk)| chunk.len() <= 220));
        let joined: Vec<_> = chunks.into_iter().flat_map(|(_, chunk)| chunk).collect();
        let lines = String::from_utf8(joined).unwrap();
        assert_eq!(lines.lines().count(), events.len());
        for line in lines.lines() {
            let value: Value = serde_json::from_str(line).unwrap();
            assert_eq!(value["event_type"], "book");
        }
    }

    #[test]
    fn buffered_append_blocks_keeps_blobs_separate_until_flush() {
        let mut buffer = BufferedAppendBlocks::new(1_024);
        assert!(buffer
            .push_line("events/2026/06/14/12/events.jsonl", b"{\"a\":1}\n".to_vec())
            .is_empty());
        assert!(buffer
            .push_line("events/2026/06/14/13/events.jsonl", b"{\"b\":2}\n".to_vec())
            .is_empty());

        let chunks = buffer.drain();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].0, "events/2026/06/14/12/events.jsonl");
        assert_eq!(chunks[0].1, b"{\"a\":1}\n");
        assert_eq!(chunks[1].0, "events/2026/06/14/13/events.jsonl");
        assert_eq!(chunks[1].1, b"{\"b\":2}\n");
    }

    #[test]
    fn buffered_append_blocks_freezes_failed_block_before_pending_data() {
        let mut buffer = BufferedAppendBlocks::new(1_024);
        buffer.prepend_retry_blocks([(
            "events/2026/06/14/12/events.jsonl".to_owned(),
            b"failed\n".to_vec(),
        )]);
        assert!(buffer
            .push_line("events/2026/06/14/12/events.jsonl", b"pending\n".to_vec())
            .is_empty());

        let retry = buffer.take_retry_blocks();
        let pending = buffer.drain();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].1, b"failed\n");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1, b"pending\n");
    }

    #[test]
    fn buffered_append_blocks_deduplicates_an_exact_retried_line() {
        let blob = "events/2026/07/22/02/17.jsonl";
        let line = b"{\"event_type\":\"strategy_decision_batch\"}\n".to_vec();
        let mut buffer = BufferedAppendBlocks::new(1_024);
        buffer.prepend_retry_blocks([(blob.to_owned(), line.clone())]);

        assert!(buffer.push_line(blob, line.clone()).is_empty());
        let retry = buffer.take_retry_blocks();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].1, line);
        assert!(buffer.drain().is_empty());
    }

    #[test]
    fn append_reconciliation_finds_only_the_exact_pending_block() {
        assert!(byte_range_contains_block(b"before-BLOCK-after", b"BLOCK"));
        assert!(byte_range_contains_block(b"BLOCK", b"BLOCK"));
        assert!(!byte_range_contains_block(b"before-BLOC-after", b"BLOCK"));
        assert!(!byte_range_contains_block(b"", b"BLOCK"));
        assert!(!byte_range_contains_block(b"BLOCK", b""));
    }

    #[test]
    fn handoff_ambiguity_boundary_survives_reconciliation_read_failure() {
        let mut starts = BTreeMap::new();
        starts.insert("events/minute.jsonl".to_owned(), 100_u64);

        let error = resolve_handoff_ambiguity(
            &mut starts,
            "events/minute.jsonl",
            140,
            Err(AzureBlobError::Transport(
                "injected reconciliation GET failure".to_owned(),
            )),
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected reconciliation"));
        assert_eq!(starts.get("events/minute.jsonl"), Some(&100));

        let already_committed = resolve_handoff_ambiguity(
            &mut starts,
            "events/minute.jsonl",
            180,
            Ok(HandoffRangeOutcome::ExactBlockPresent),
        )
        .unwrap();
        assert!(already_committed);
        assert!(!starts.contains_key("events/minute.jsonl"));
    }

    #[test]
    fn conditional_seal_is_authenticated_idempotent_only_after_head_and_redacts_failures() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            (0..4)
                .map(|index| {
                    let (mut stream, _) = listener.accept().unwrap();
                    let request = read_http_request(&stream);
                    let lower = request.to_ascii_lowercase();
                    let expected_method = if index == 2 { "HEAD " } else { "PUT " };
                    let expected_etag = if matches!(index, 1 | 2) {
                        "if-match: \"etag-sealed\""
                    } else {
                        "if-match: \"etag-open\""
                    };
                    let valid_request = request.starts_with(expected_method)
                        && lower.contains(&format!("x-ms-version: {AZURE_BLOB_API_VERSION}"))
                        && lower.contains("x-ms-date:")
                        && lower.contains(expected_etag)
                        && if index == 2 {
                            request.starts_with("HEAD /minute.jsonl?sig=test-sas HTTP/1.1")
                        } else {
                            request.starts_with(
                                "PUT /minute.jsonl?comp=seal&sig=test-sas HTTP/1.1",
                            ) && lower.contains("content-length: 0")
                        };
                    let (status, headers, body) = match index {
                        0 => ("200 OK", "", ""),
                        1 => (
                            "409 Conflict",
                            "x-ms-error-code: BlobAlreadySealed\r\n",
                            "",
                        ),
                        2 => (
                            "200 OK",
                            "etag: \"etag-sealed\"\r\nx-ms-blob-type: AppendBlob\r\nx-ms-blob-sealed: true\r\n",
                            "",
                        ),
                        _ => (
                            "403 Forbidden",
                            "x-ms-error-code: AuthorizationPermissionMismatch\r\nx-ms-request-id: 11111111-2222-3333-4444-555555555555\r\n",
                            "secret-body",
                        ),
                    };
                    write!(
                        stream,
                        "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                    valid_request
                })
                .collect::<Vec<_>>()
        });

        let mut client = AzureBlobClient::new("unused", "unused", "sig=test-sas");
        let url = format!("http://{address}/minute.jsonl");
        client
            .seal_append_blob_at_url(&url, "\"etag-open\"")
            .unwrap();
        client
            .seal_append_blob_at_url(&url, "\"etag-sealed\"")
            .unwrap();
        let error = client
            .seal_append_blob_at_url(&url, "\"etag-open\"")
            .unwrap_err();
        let message = error.to_string();

        assert!(server.join().unwrap().into_iter().all(|valid| valid));
        assert!(message.contains("HTTP status 403"));
        assert!(message.contains("x-ms-error-code=AuthorizationPermissionMismatch"));
        assert!(message.contains("x-ms-request-id=11111111-2222-3333-4444-555555555555"));
        assert!(!message.contains("secret-body"));
    }

    #[test]
    fn immutable_blob_put_retries_with_the_create_only_precondition() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            ["500 Internal Server Error", "412 Precondition Failed"]
                .into_iter()
                .map(|status| {
                    let (mut stream, _) = listener.accept().unwrap();
                    let request = read_http_request(&stream);
                    write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    request
                })
                .collect::<Vec<_>>()
        });

        let mut client = AzureBlobClient::new("unused", "unused", "test=sas");
        let result = client
            .put_block_blob_if_absent(
                &format!("http://{address}/immutable"),
                b"intent",
                "application/json",
                None,
            )
            .unwrap();

        assert_eq!(result, ImmutableBlobWrite::AlreadyExists);
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.to_ascii_lowercase().contains("if-none-match: *")));
    }

    #[test]
    fn immutable_blob_put_accepts_forbidden_create_only_after_existing_blob_is_readable() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            ["403 Forbidden", "200 OK"]
                .into_iter()
                .map(|status| {
                    let (mut stream, _) = listener.accept().unwrap();
                    let request = read_http_request(&stream);
                    write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    request
                })
                .collect::<Vec<_>>()
        });

        let mut client = AzureBlobClient::new("unused", "unused", "test=sas");
        let result = client
            .put_block_blob_if_absent(
                &format!("http://{address}/immutable"),
                b"intent",
                "application/json",
                None,
            )
            .unwrap();

        assert_eq!(result, ImmutableBlobWrite::AlreadyExists);
        let requests = server.join().unwrap();
        assert!(requests[0].starts_with("PUT "));
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("if-none-match: *"));
        assert!(requests[1].starts_with("GET "));
    }

    #[test]
    fn immutable_blob_existing_read_retains_safe_forbidden_headers() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for status in ["412 Precondition Failed", "403 Forbidden"] {
                let (mut stream, _) = listener.accept().unwrap();
                read_http_request(&stream);
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\n{}Content-Length: 11\r\n\
                     Connection: close\r\n\r\nsecret-body",
                    if status.starts_with("403") {
                        "x-ms-error-code: AuthorizationPermissionMismatch\r\n\
                         x-ms-request-id: 11111111-2222-3333-4444-555555555555\r\n"
                    } else {
                        ""
                    }
                )
                .unwrap();
            }
        });

        let mut client = AzureBlobClient::new("unused", "unused", "test=sas");
        let url = format!("http://{address}/immutable");
        let result = client
            .put_block_blob_if_absent(&url, b"intent", "application/json", None)
            .unwrap();
        assert_eq!(result, ImmutableBlobWrite::AlreadyExists);
        let error = client.get_bytes_with_retry(&url).unwrap_err();

        let message = error.to_string();
        assert!(message.contains("x-ms-error-code=AuthorizationPermissionMismatch"));
        assert!(message.contains("x-ms-request-id=11111111-2222-3333-4444-555555555555"));
        assert!(message.contains("GET x-ms-error-code"));
        assert!(!message.contains("secret-body"));
        server.join().unwrap();
    }

    #[test]
    fn ambiguous_committed_block_is_not_coalesced_with_later_same_blob_data() {
        let blob = "events/minute.jsonl";
        let block_b = b"B\n".to_vec();
        let line_c = b"C\n".to_vec();
        let mut remote = block_b.clone();
        let mut starts = BTreeMap::new();
        starts.insert(blob.to_owned(), 100_u64);
        let mut buffer = BufferedAppendBlocks::new(1_024);

        let error = resolve_handoff_ambiguity(
            &mut starts,
            blob,
            102,
            Err(AzureBlobError::Transport(
                "injected reconciliation GET failure".to_owned(),
            )),
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected reconciliation"));
        buffer.prepend_retry_blocks([(blob.to_owned(), block_b.clone())]);

        assert!(buffer.push_line(blob, line_c.clone()).is_empty());
        let retry = buffer.take_retry_blocks();
        assert_eq!(retry, vec![(blob.to_owned(), block_b.clone())]);
        assert!(byte_range_contains_block(&remote, &retry[0].1));

        let already_committed = resolve_handoff_ambiguity(
            &mut starts,
            blob,
            102,
            Ok(HandoffRangeOutcome::ExactBlockPresent),
        )
        .unwrap();
        assert!(already_committed);
        let pending = buffer.drain();
        assert_eq!(pending, vec![(blob.to_owned(), line_c)]);
        remote.extend(&pending[0].1);
        assert_eq!(remote, b"B\nC\n");
        assert!(!starts.contains_key(blob));
    }

    #[test]
    fn bound_jsonl_envelopes_match_for_local_and_azure_recorders() {
        let event = RecordedRuntimeEvent::bound(
            RuntimeEvent {
                event_type: "book".to_owned(),
                ts: Utc::now(),
                data: json!({"token_id": "token"}),
            },
            "7c66d77b-a911-4f9b-95f2-98ca9395255e",
            42,
        );
        let envelope = jsonl_recorded_event_envelope(&event).unwrap();
        let azure_line: Value =
            serde_json::from_slice(&jsonl_recorded_event_line(&event).unwrap()).unwrap();
        assert_eq!(azure_line, envelope);
        assert_eq!(
            envelope["recorder_instance_id"],
            "7c66d77b-a911-4f9b-95f2-98ca9395255e"
        );
        assert_eq!(envelope["recorder_sequence"], 42);
    }

    #[test]
    fn jsonl_recorder_preserves_wire_normalized_content_bindings() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-wire-binding-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        let payload = wire_normalized_json(&json!({
            "unicode": "λ🧪",
            "negative_zero": -0.0,
            "subnormal": f64::from_bits(1),
            "large": 1.7976931348623157e308_f64,
            "small": 2.2250738585072014e-308_f64,
            "nested": {"z": [3, 2, 1], "a": {"escaped": "line\nfeed"}}
        }))
        .unwrap();
        let expected_sha256 = canonical_json_sha256(&payload);
        let event = RuntimeEvent {
            event_type: "strategy_decision_batch".to_owned(),
            ts: Utc::now(),
            data: payload,
        };
        JsonlRecorder::new(&path).record(&event).unwrap();
        let line = fs::read_to_string(&path).unwrap();
        let recorded: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(canonical_json_sha256(&recorded["payload"]), expected_sha256);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn jsonl_recorder_rejects_unterminated_tail_without_changing_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-jsonl-tail-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.jsonl");
        let partial = br#"{"partial":"#;
        fs::write(&path, partial).unwrap();
        let event = RecordedRuntimeEvent::bound(
            RuntimeEvent {
                event_type: "runtime_provenance".to_owned(),
                ts: Utc::now(),
                data: json!({"git_sha": "test"}),
            },
            "7c66d77b-a911-4f9b-95f2-98ca9395255e",
            1,
        );

        let error = JsonlRecorder::new(&path)
            .record_recorded_batch(&[event])
            .unwrap_err();

        assert!(matches!(error, StorageError::UnterminatedJsonlTail));
        assert_eq!(fs::read(&path).unwrap(), partial);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn segmented_jsonl_recorder_uses_stable_ten_minute_paths() {
        let recorder = JsonlRecorder::segmented("/srv/polyedge-ring/segments", 600);
        let first = DateTime::parse_from_rfc3339("2026-08-05T22:34:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let last = DateTime::parse_from_rfc3339("2026-08-05T22:39:59Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(recorder.active_path(first), recorder.active_path(last));
        assert_eq!(
            recorder.active_path(first),
            PathBuf::from("/srv/polyedge-ring/segments/2026/08/05/22/1785969000.jsonl")
        );
    }

    fn segmented_pending_append(
        root: &Path,
        original: &[u8],
        observed: &[u8],
        bytes: &[u8],
    ) -> (JsonlRecorder, PathBuf) {
        let path = root.join("2026/08/05/22/1785969000.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut existing = original.to_vec();
        existing.extend_from_slice(observed);
        fs::write(&path, existing).unwrap();
        let mut recorder = JsonlRecorder::segmented(root, 600);
        recorder.pending_append = Some(PendingJsonlAppend {
            path: path.clone(),
            original_offset: Some(original.len() as u64),
            bytes: bytes.to_vec(),
        });
        (recorder, path)
    }

    #[test]
    fn pending_segmented_jsonl_append_resumes_an_exact_prefix() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-jsonl-prefix-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let (mut recorder, path) = segmented_pending_append(&root, b"first\n", b"next", b"next\n");

        recorder.flush().unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"first\nnext\n");
        assert!(recorder.pending_append.is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_segmented_jsonl_append_resumes_a_mid_line_prefix() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-jsonl-mid-line-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let bytes = b"{\"event_type\":\"book\"}\n";
        let (mut recorder, path) =
            segmented_pending_append(&root, b"first\n", b"{\"event_type\":", bytes);

        recorder.flush().unwrap();

        assert_eq!(
            fs::read(&path).unwrap(),
            b"first\n{\"event_type\":\"book\"}\n"
        );
        assert!(recorder.pending_append.is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_segmented_jsonl_append_recognizes_a_full_synced_write() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-jsonl-full-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let (mut recorder, path) =
            segmented_pending_append(&root, b"first\n", b"next\n", b"next\n");

        recorder.flush().unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"first\nnext\n");
        assert!(recorder.pending_append.is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_segmented_jsonl_append_rejects_divergence_without_mutation() {
        let root = std::env::temp_dir().join(format!(
            "polyedge-jsonl-divergence-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let existing = b"first\nnope\n";
        let (mut recorder, path) =
            segmented_pending_append(&root, b"first\n", b"nope\n", b"next\n");

        assert!(matches!(
            recorder.flush(),
            Err(StorageError::PendingJsonlAppendDiverged)
        ));

        assert_eq!(fs::read(&path).unwrap(), existing);
        assert!(recorder.pending_append.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn blob_listing_requires_and_preserves_strong_identity_metadata() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Blobs>
    <Blob>
      <Name>shadow-events/campaign-2026-07-12/2026/07/13/00/00.jsonl</Name>
      <Properties>
        <Last-Modified>Mon, 13 Jul 2026 00:01:00 GMT</Last-Modified>
        <Etag>&quot;0x8DB123&quot;</Etag>
        <Content-MD5>YWJjZA==</Content-MD5>
        <Content-Length>123</Content-Length>
        <BlobType>AppendBlob</BlobType>
        <Sealed>true</Sealed>
      </Properties>
      <VersionId>2026-07-13T00:01:00.0000000Z</VersionId>
      <IsCurrentVersion>true</IsCurrentVersion>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>"#;
        let page = parse_blob_list(xml).unwrap();
        assert_eq!(page.blobs.len(), 1);
        assert_eq!(page.blobs[0].etag, "\"0x8DB123\"");
        assert_eq!(page.blobs[0].content_length, 123);
        assert_eq!(page.blobs[0].content_md5.as_deref(), Some("YWJjZA=="));
        assert_eq!(page.blobs[0].blob_type.as_deref(), Some("AppendBlob"));
        assert_eq!(page.blobs[0].sealed, Some(true));
        assert_eq!(
            page.blobs[0].version_id.as_deref(),
            Some("2026-07-13T00:01:00.0000000Z")
        );
        assert_eq!(page.blobs[0].is_current_version, Some(true));
        assert_eq!(
            page.blobs[0].name,
            "shadow-events/campaign-2026-07-12/2026/07/13/00/00.jsonl"
        );

        let missing_etag = xml.replace("<Etag>&quot;0x8DB123&quot;</Etag>", "<Etag></Etag>");
        assert!(parse_blob_list(&missing_etag).is_err());

        let unsealed = xml.replace("        <Sealed>true</Sealed>\n", "");
        let unsealed_page = parse_blob_list(&unsealed).unwrap();
        assert_eq!(
            unsealed_page.blobs[0].blob_type.as_deref(),
            Some("AppendBlob")
        );
        assert_eq!(unsealed_page.blobs[0].sealed, Some(false));
        assert!(unsealed_page.blobs[0].last_modified.is_some());
    }
}
