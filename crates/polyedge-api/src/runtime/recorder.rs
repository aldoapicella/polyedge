use polyedge_config::RuntimeSettings;
use polyedge_storage::{
    AzureAppendBlobRecorder, EventRecorder, JsonlRecorder, RecordedRuntimeEvent,
};
use serde_json::{json, Value};
use std::env;
use std::path::PathBuf;

pub(super) struct RuntimeRecorder {
    recorders: Vec<Box<dyn EventRecorder + Send>>,
    authoritative_remote: bool,
    error_count: usize,
    dropped_count: usize,
    last_error: Option<String>,
}

impl RuntimeRecorder {
    pub(super) fn new(settings: &RuntimeSettings) -> Self {
        let mut recorders: Vec<Box<dyn EventRecorder + Send>> = Vec::new();
        let authoritative_remote = settings.azure.storage_account_name.is_some();
        let local_jsonl_enabled = local_jsonl_recorder_enabled(
            authoritative_remote,
            env::var("LOCAL_JSONL_RECORDER_ENABLED").ok().as_deref(),
        );
        if local_jsonl_enabled {
            let path = env::var("RECORDER_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("data/events.jsonl"));
            let recorder = match env::var("RECORDER_SEGMENT_SECONDS") {
                Ok(value) => {
                    let seconds = value.parse::<u64>().unwrap_or_else(|_| {
                        panic!("RECORDER_SEGMENT_SECONDS must be an integer from 300 through 900")
                    });
                    assert!(
                        (300..=900).contains(&seconds),
                        "RECORDER_SEGMENT_SECONDS must be from 300 through 900"
                    );
                    JsonlRecorder::segmented(path, seconds)
                }
                Err(_) => JsonlRecorder::new(path),
            };
            recorders.push(Box::new(recorder));
        }
        if let Some(account) = settings.azure.storage_account_name.as_deref() {
            let client_id = env::var("AZURE_CLIENT_ID").ok();
            recorders.push(Box::new(AzureAppendBlobRecorder::new_with_prefix_cutover(
                account,
                settings.azure.storage_container_name.clone(),
                client_id,
                settings.azure.event_blob_prefix.clone(),
                settings.azure.event_blob_prefix_after_cutover.clone(),
                settings.azure.event_blob_prefix_cutover_utc,
            )));
        }
        Self {
            recorders,
            authoritative_remote,
            error_count: 0,
            dropped_count: 0,
            last_error: None,
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_path(path: PathBuf) -> Self {
        Self {
            recorders: vec![Box::new(JsonlRecorder::new(path))],
            authoritative_remote: false,
            error_count: 0,
            dropped_count: 0,
            last_error: None,
        }
    }

    #[cfg(test)]
    pub(super) fn new_for_test_recorder(
        recorder: Box<dyn EventRecorder + Send>,
        authoritative_remote: bool,
    ) -> Self {
        Self {
            recorders: vec![recorder],
            authoritative_remote,
            error_count: 0,
            dropped_count: 0,
            last_error: None,
        }
    }

    pub(super) fn has_authoritative_remote(&self) -> bool {
        self.authoritative_remote
    }

    pub(super) fn record_recorded_batch(
        &mut self,
        events: &[RecordedRuntimeEvent],
    ) -> Result<(), String> {
        let mut last_error = None;
        for recorder in &mut self.recorders {
            if let Err(error) = recorder.record_recorded_batch(events) {
                self.error_count += 1;
                last_error = Some(error.to_string());
            }
        }
        if let Some(error) = last_error {
            self.last_error = Some(error.clone());
            Err(error)
        } else {
            Ok(())
        }
    }

    pub(super) fn flush(&mut self) -> Result<(), String> {
        let mut last_error = None;
        for recorder in &mut self.recorders {
            if let Err(error) = recorder.flush() {
                self.error_count += 1;
                last_error = Some(error.to_string());
            }
        }
        if let Some(error) = last_error {
            self.last_error = Some(error.clone());
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Resume a previously staged authoritative append without re-recording
    /// the logical events. Local JSONL recorders do not buffer failed writes,
    /// so they cannot acknowledge a retry-only durable request.
    pub(super) fn retry_pending(&mut self) -> Result<(), String> {
        if !self.authoritative_remote {
            return Err(
                "runtime recorder has no authoritative remote pending append to retry".to_owned(),
            );
        }
        self.flush()
    }

    pub(super) fn status(&self, busy: bool) -> Value {
        json!({
            "type": "composite",
            "recorders": self.recorders.len(),
            "error_count": self.error_count,
            "dropped_count": self.dropped_count,
            "last_error": self.last_error,
            "busy": busy
        })
    }

    pub(super) fn busy_status() -> Value {
        json!({
            "type": "composite",
            "recorders": Value::Null,
            "error_count": Value::Null,
            "dropped_count": Value::Null,
            "last_error": Value::Null,
            "busy": true
        })
    }
}

fn local_jsonl_recorder_enabled(authoritative_remote: bool, configured: Option<&str>) -> bool {
    if !authoritative_remote {
        return true;
    }
    !matches!(
        configured
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("false" | "0" | "no")
    )
}

#[cfg(test)]
mod tests {
    use super::local_jsonl_recorder_enabled;

    #[test]
    fn local_jsonl_can_only_be_disabled_when_remote_storage_is_authoritative() {
        assert!(!local_jsonl_recorder_enabled(true, Some("false")));
        assert!(!local_jsonl_recorder_enabled(true, Some(" 0 ")));
        assert!(local_jsonl_recorder_enabled(true, None));
        assert!(local_jsonl_recorder_enabled(true, Some("true")));
        assert!(local_jsonl_recorder_enabled(false, Some("false")));
    }
}
