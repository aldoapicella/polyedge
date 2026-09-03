mod chart;
mod chart_history;
mod execution_intent;
mod execution_quality;
mod recorder;
mod reference;
mod view;

use chart::chart_sample_from_data;
use chart_history::{point_bucket_ms, should_persist, spawn_persist, ChartPersistenceSample};
use chrono::{DateTime, Utc};
use execution_intent::{
    build_execution_intent_with_model, resolve_execution_model, resolve_local_execution_model,
    IntentExecutionModel, IntentPublisher, IntentPublisherConfig, IntentPublisherPreparation,
};
use execution_quality::{deterministic_probe, ExecutionQualityTracker};
use polyedge_config::{embedded_git_sha, ExecutionMode, RuntimeSettings};
use polyedge_domain::{
    BookState, DecisionAction, ExecutionReport, FairValue, MarketId, MarketSpec, ReferencePrice,
    RuntimeEvent, TokenId, TradeDecision,
};
use polyedge_engine::{
    decision_config_projection_v1, evaluate_decision_pipeline_v3, final_decision_evidence_v1,
    DecisionPipelineInputV3, FrozenStrategyMode, LogReturnFairValueModel, MarketStartEvidenceV1,
    OrderManager, PaperFillEngine, RegimeBookSnapshot, RegimeClassifier, RegimeFeatureInput,
    RegimeReferencePoint, RestingMakerOrder, RiskManager, StrategyDecisionMetadata,
};
use polyedge_execution::{ExecutionClient, PaperExecutionClient};
use polyedge_feeds::{self, ClobGenerationLease, ClobResyncBarrier, FeedEvent, FeedName};
use polyedge_storage::{canonical_json_sha256, wire_normalized_json, RecordedRuntimeEvent};
use recorder::RuntimeRecorder;
use reference::ReferenceAggregator;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{
    broadcast, mpsc, oneshot, Mutex, OwnedMutexGuard, OwnedSemaphorePermit, RwLock, Semaphore,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const RECENT_LIMIT: usize = 1_000;
const HISTORY_LIMIT: usize = 500;
const CHART_HISTORY_LIMIT: usize = 2_000;
const RECORDER_BATCH_LIMIT: usize = 500;
const RECORDER_QUEUE_CAPACITY: usize = 1_000;
const RECORDER_FLUSH_INTERVAL: Duration = Duration::from_secs(10);
const RECORDER_RETRY_DELAY: Duration = Duration::from_millis(250);
const SHUTDOWN_TERMINATION_AUDIT_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(not(test))]
const RECORDER_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const RECORDER_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const RECORDER_COMPLETED_DURABLE_BATCH_LIMIT: usize = 10_000;
const REQUIRED_RECORDER_ATTEMPTS: usize = 3;
const STARTUP_PROVENANCE_ATTEMPTS: usize = 5;
const RUNTIME_PROVENANCE_INTERVAL: Duration = Duration::from_secs(60);
const EXACT_REFERENCE_HISTORY_LIMIT: usize = 1_200;
const PENDING_SETTLEMENT_RETENTION_SECONDS: i64 = 6 * 60 * 60;
const ESSENTIAL_FEED_MAX_AGE_SECONDS: i64 = 5 * 60;
const QSET_V4_APP_NAME: &str = "polyedge-shadow-qset-v4";
const QSET_V4_CAMPAIGN_ID: &str = "campaign-2026-08-24-qset-v4";
const QSET_V4_RAW_CONTAINER: &str = "polyedge-shadow-qset-v4-events";
const QSET_V5_APP_NAME: &str = "polyedge-shadow-qset-v5";
const QSET_V5_CAMPAIGN_ID: &str = "campaign-2026-08-26-qset-v5";
const QSET_V5_RAW_CONTAINER: &str = "polyedge-shadow-qset-v5-events";
const QSET_V6_APP_NAME: &str = "polyedge-shadow-qset-v6";
const QSET_V6_CAMPAIGN_ID: &str = "campaign-2026-09-01-qset-v6";
const QSET_V6_RAW_CONTAINER: &str = "polyedge-shadow-qset-v6-events";
const QSET_V7_APP_NAME: &str = "polyedge-shadow-qset-v7";
const QSET_V7_CAMPAIGN_ID: &str = "campaign-2026-09-02-qset-v7";
const QSET_V7_RAW_CONTAINER: &str = "polyedge-shadow-qset-v7-events";

#[derive(Clone, Debug, Serialize)]
pub struct QsetV4WriterRetirementReceipt {
    pub schema: &'static str,
    pub status: &'static str,
    pub retired_at: DateTime<Utc>,
    pub campaign_id: &'static str,
    pub app_name: String,
    pub image_digest: String,
    pub source_revision: String,
    pub recorder_instance_id: String,
    pub final_assigned_sequence: u64,
    pub final_enqueued_sequence: u64,
    pub final_enqueued_total: u64,
    pub final_persisted_sequence: u64,
    pub final_persisted_total: u64,
    pub final_queued: usize,
    pub flush_success: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct QsetV5WriterRetirementReceipt {
    pub schema: &'static str,
    pub status: &'static str,
    pub retired_at: DateTime<Utc>,
    pub campaign_id: &'static str,
    pub app_name: String,
    pub image_digest: String,
    pub source_revision: String,
    pub recorder_instance_id: String,
    pub final_assigned_sequence: u64,
    pub final_enqueued_sequence: u64,
    pub final_enqueued_total: u64,
    pub final_persisted_sequence: u64,
    pub final_persisted_total: u64,
    pub final_queued: usize,
    pub flush_success: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct QsetV6WriterRetirementReceipt {
    pub schema: &'static str,
    pub status: &'static str,
    pub retired_at: DateTime<Utc>,
    pub campaign_id: &'static str,
    pub app_name: String,
    pub image_digest: String,
    pub source_revision: String,
    pub recorder_instance_id: String,
    pub final_assigned_sequence: u64,
    pub final_enqueued_sequence: u64,
    pub final_enqueued_total: u64,
    pub final_persisted_sequence: u64,
    pub final_persisted_total: u64,
    pub final_queued: usize,
    pub flush_success: bool,
}
pub type QsetV7WriterRetirementReceipt = QsetV6WriterRetirementReceipt;

#[derive(Clone)]
pub struct RuntimeController {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    settings: RuntimeSettings,
    data: RwLock<RuntimeData>,
    engine: Mutex<RuntimeEngine>,
    /// Serializes every mutation that can invalidate a decision snapshot with
    /// the final durable compare-and-apply section.
    decision_gate: Arc<Mutex<()>>,
    recorder: Arc<StdMutex<RuntimeRecorder>>,
    recorder_enqueue_gate: StdMutex<()>,
    recorder_tx: std_mpsc::Sender<RecorderRequest>,
    recorder_admission: Arc<Semaphore>,
    recorder_metrics: Arc<RecorderMetrics>,
    persistence_filter: StdMutex<PersistenceFilter>,
    intent_publisher: Option<IntentPublisher>,
    broadcaster: broadcast::Sender<RuntimeEvent>,
    started: AtomicBool,
    shutting_down: AtomicBool,
    termination_audit_complete: AtomicBool,
    shutdown_gate: Mutex<()>,
    feed_task: StdMutex<Option<JoinHandle<()>>>,
    background_tasks: StdMutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug)]
struct RecorderMetrics {
    recorder_instance_id: String,
    last_assigned_sequence: AtomicU64,
    last_enqueued_sequence: AtomicU64,
    last_persisted_sequence: AtomicU64,
    queued: AtomicUsize,
    enqueued_total: AtomicU64,
    persisted_total: AtomicU64,
    filtered_total: AtomicU64,
    failed_total: AtomicU64,
    recovered_total: AtomicU64,
    unrecovered_durable_events: AtomicUsize,
    flush_failed_total: AtomicU64,
    flush_recovered_total: AtomicU64,
    flush_unrecovered: AtomicBool,
    unrecovered_batches: StdMutex<BTreeMap<String, usize>>,
    batches_total: AtomicU64,
    last_batch_size: AtomicUsize,
}

#[derive(Clone, Debug)]
struct DecisionBatchBinding {
    batch_id: String,
    output_index: usize,
    decision_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecisionStateGeneration {
    data: u64,
    engine: u64,
}

#[derive(Clone, Debug)]
struct PreparedDecision {
    decision: TradeDecision,
    metadata: Option<StrategyDecisionMetadata>,
    unbound_payload: Value,
    binding: DecisionBatchBinding,
    payload: Value,
}

#[derive(Clone, Debug)]
struct AppliedDecisionOutput {
    application: Value,
    reports: Vec<ExecutionReport>,
}

#[derive(Clone, Debug)]
struct PendingPaperSettlement {
    journal_id: String,
    events: Vec<RuntimeEvent>,
}

#[derive(Clone, Debug)]
struct PendingDecisionApplication {
    batch_id: String,
    events: Vec<RuntimeEvent>,
    reports: Vec<ExecutionReport>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingApplicationRetry {
    NotPending,
    Retained,
    Committed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingSettlementRetry {
    NotPending,
    Retained,
    Committed,
}

struct RecorderRequest {
    events: Vec<RecordedRuntimeEvent>,
    durable_batch_key: Option<String>,
    logical_event_count: usize,
    durable_ack: Option<oneshot::Sender<Result<(), String>>>,
    _admission_permit: Option<OwnedSemaphorePermit>,
}

impl RecorderRequest {
    fn best_effort(event: RecordedRuntimeEvent) -> Self {
        Self {
            events: vec![event],
            durable_batch_key: None,
            logical_event_count: 1,
            durable_ack: None,
            _admission_permit: None,
        }
    }

    fn admitted_best_effort(event: RecordedRuntimeEvent, permit: OwnedSemaphorePermit) -> Self {
        let mut request = Self::best_effort(event);
        request._admission_permit = Some(permit);
        request
    }

    fn durable(
        events: Vec<RecordedRuntimeEvent>,
        durable_ack: oneshot::Sender<Result<(), String>>,
    ) -> Self {
        let logical_event_count = events.len();
        let durable_batch_key = required_recorder_batch_key(
            &events
                .iter()
                .map(|event| event.event().clone())
                .collect::<Vec<_>>(),
        );
        Self {
            events,
            durable_batch_key: Some(durable_batch_key),
            logical_event_count,
            durable_ack: Some(durable_ack),
            _admission_permit: None,
        }
    }

    fn admitted_durable(
        events: Vec<RecordedRuntimeEvent>,
        durable_ack: oneshot::Sender<Result<(), String>>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        let mut request = Self::durable(events, durable_ack);
        request._admission_permit = Some(permit);
        request
    }
}

#[derive(Default)]
struct RecorderDurabilityState {
    pending_batch_key: Option<String>,
    completed_batch_keys: BTreeSet<String>,
    completed_batch_order: VecDeque<String>,
}

impl RecorderDurabilityState {
    fn remember_completed(&mut self, batch_key: String) {
        if self.completed_batch_keys.insert(batch_key.clone()) {
            self.completed_batch_order.push_back(batch_key);
        }
        while self.completed_batch_order.len() > RECORDER_COMPLETED_DURABLE_BATCH_LIMIT {
            if let Some(expired) = self.completed_batch_order.pop_front() {
                self.completed_batch_keys.remove(&expired);
            }
        }
    }
}

impl Default for RecorderMetrics {
    fn default() -> Self {
        Self {
            recorder_instance_id: Uuid::new_v4().to_string(),
            last_assigned_sequence: AtomicU64::new(0),
            last_enqueued_sequence: AtomicU64::new(0),
            last_persisted_sequence: AtomicU64::new(0),
            queued: AtomicUsize::new(0),
            enqueued_total: AtomicU64::new(0),
            persisted_total: AtomicU64::new(0),
            filtered_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
            recovered_total: AtomicU64::new(0),
            unrecovered_durable_events: AtomicUsize::new(0),
            flush_failed_total: AtomicU64::new(0),
            flush_recovered_total: AtomicU64::new(0),
            flush_unrecovered: AtomicBool::new(false),
            unrecovered_batches: StdMutex::new(BTreeMap::new()),
            batches_total: AtomicU64::new(0),
            last_batch_size: AtomicUsize::new(0),
        }
    }
}

impl RecorderMetrics {
    fn bind(&self, event: RuntimeEvent) -> Result<RecordedRuntimeEvent, String> {
        let previous = self
            .last_assigned_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| "runtime recorder sequence exhausted".to_owned())?;
        Ok(RecordedRuntimeEvent::bound(
            event,
            self.recorder_instance_id.clone(),
            previous + 1,
        ))
    }

    fn rollback_bound_tail(&self, events: &[RecordedRuntimeEvent]) -> bool {
        let Some(last_sequence) = events.last().map(RecordedRuntimeEvent::recorder_sequence) else {
            return true;
        };
        let Ok(event_count) = u64::try_from(events.len()) else {
            return false;
        };
        let Some(previous_sequence) = last_sequence.checked_sub(event_count) else {
            return false;
        };
        if events
            .iter()
            .enumerate()
            .any(|(index, event)| event.recorder_sequence() != previous_sequence + index as u64 + 1)
        {
            return false;
        }
        self.last_assigned_sequence
            .compare_exchange(
                last_sequence,
                previous_sequence,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    fn snapshot(&self) -> Value {
        json!({
            "recorder_instance_id": self.recorder_instance_id,
            "last_assigned_sequence": self.last_assigned_sequence.load(Ordering::Relaxed),
            "last_enqueued_sequence": self.last_enqueued_sequence.load(Ordering::Relaxed),
            "last_persisted_sequence": self.last_persisted_sequence.load(Ordering::Relaxed),
            "queued": self.queued.load(Ordering::Relaxed),
            "enqueued_total": self.enqueued_total.load(Ordering::Relaxed),
            "persisted_total": self.persisted_total.load(Ordering::Relaxed),
            "filtered_total": self.filtered_total.load(Ordering::Relaxed),
            "failed_total": self.failed_total.load(Ordering::Relaxed),
            "recovered_total": self.recovered_total.load(Ordering::Relaxed),
            "unrecovered_durable_events": self.unrecovered_durable_events.load(Ordering::Relaxed),
            "flush_failed_total": self.flush_failed_total.load(Ordering::Relaxed),
            "flush_recovered_total": self.flush_recovered_total.load(Ordering::Relaxed),
            "flush_unrecovered": self.flush_unrecovered.load(Ordering::Relaxed),
            "batches_total": self.batches_total.load(Ordering::Relaxed),
            "last_batch_size": self.last_batch_size.load(Ordering::Relaxed)
        })
    }

    fn mark_persisted(&self, event_count: usize, last_sequence: u64) {
        self.persisted_total
            .fetch_add(event_count as u64, Ordering::Release);
        self.last_persisted_sequence
            .store(last_sequence, Ordering::Release);
    }

    fn mark_enqueued(&self, last_sequence: u64) {
        self.last_enqueued_sequence
            .store(last_sequence, Ordering::Release);
    }

    fn mark_durable_batch_unrecovered(&self, events: &[RuntimeEvent]) {
        let key = required_recorder_batch_key(events);
        let Ok(mut batches) = self.unrecovered_batches.lock() else {
            self.unrecovered_durable_events
                .store(usize::MAX, Ordering::Relaxed);
            return;
        };
        batches.entry(key).or_insert(events.len());
        self.unrecovered_durable_events
            .store(batches.values().copied().sum::<usize>(), Ordering::Relaxed);
    }

    fn mark_durable_batch_recovered(&self, events: &[RuntimeEvent]) -> bool {
        let key = required_recorder_batch_key(events);
        let Ok(mut batches) = self.unrecovered_batches.lock() else {
            self.unrecovered_durable_events
                .store(usize::MAX, Ordering::Relaxed);
            return false;
        };
        let recovered = batches.remove(&key).is_some();
        self.unrecovered_durable_events
            .store(batches.values().copied().sum::<usize>(), Ordering::Relaxed);
        if recovered {
            self.recovered_total
                .fetch_add(events.len() as u64, Ordering::Relaxed);
        }
        recovered
    }
}

#[derive(Debug, Default)]
struct PersistenceFilter {
    last_bucket_by_stream_and_token: BTreeMap<String, i64>,
}

impl PersistenceFilter {
    fn should_persist(
        &mut self,
        settings: &RuntimeSettings,
        event_type: &str,
        data: &Value,
        timestamp: DateTime<Utc>,
        force: bool,
    ) -> bool {
        if force
            || !settings.deploy.runtime_role.is_shadow()
            || !settings.azure.compact_shadow_recording
        {
            return true;
        }
        if event_type == "raw_market_event" {
            let kind = data
                .get("event_type")
                .or_else(|| data.get("type"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if matches!(kind.as_str(), "last_trade_price" | "last_trade" | "trade") {
                return true;
            }
            if matches!(
                kind.as_str(),
                "price_change" | "pricechange" | "level_change" | "best_bid_ask" | "bestbidask"
            ) {
                return self.should_sample(settings, "level", data, timestamp);
            }
            return false;
        }
        if event_type != "book" {
            return true;
        }
        self.should_sample(settings, "book", data, timestamp)
    }

    fn should_sample(
        &mut self,
        settings: &RuntimeSettings,
        family: &str,
        data: &Value,
        timestamp: DateTime<Utc>,
    ) -> bool {
        let token = data
            .get("token_id")
            .or_else(|| data.get("asset_id"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let key = format!("{family}:{token}");
        let interval = i64::try_from(settings.azure.shadow_book_sample_ms).unwrap_or(i64::MAX);
        let bucket = timestamp.timestamp_millis().div_euclid(interval.max(1));
        match self.last_bucket_by_stream_and_token.get(&key) {
            Some(previous) if *previous >= bucket => false,
            _ => {
                self.last_bucket_by_stream_and_token.insert(key, bucket);
                true
            }
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeData {
    decision_generation: u64,
    started_at: DateTime<Utc>,
    paused: bool,
    pause_reason: Option<String>,
    paused_at: Option<DateTime<Utc>>,
    kill_switch: bool,
    clob_generation: Option<u64>,
    clob_pending_generation: Option<u64>,
    clob_terminal_generation: u64,
    clob_lease: Option<ClobGenerationLease>,
    clob_last_sequence: u64,
    clob_tokens: BTreeSet<TokenId>,
    clob_pending_tokens: BTreeSet<TokenId>,
    markets: BTreeMap<MarketId, MarketSpec>,
    books: BTreeMap<TokenId, BookState>,
    reference: Option<ReferencePrice>,
    exact_references: VecDeque<ReferencePrice>,
    market_start_references: BTreeMap<MarketId, ReferencePrice>,
    market_start_evidence_durable: BTreeSet<MarketId>,
    pending_market_start_events: BTreeMap<MarketId, RuntimeEvent>,
    fair_values: BTreeMap<MarketId, Value>,
    chart_samples: BTreeMap<MarketId, VecDeque<Value>>,
    chart_last_persisted_ms: BTreeMap<MarketId, i64>,
    decisions: VecDeque<TradeDecision>,
    execution_reports: VecDeque<ExecutionReport>,
    recent_events: VecDeque<RuntimeEvent>,
    settled_markets: Vec<MarketId>,
    funded_warmup_market_id: Option<MarketId>,
    feed_status: BTreeMap<String, Value>,
    feed_events: usize,
    runtime_events: usize,
    drop_counts: BTreeMap<String, usize>,
}

struct RuntimeEngine {
    decision_generation: u64,
    fair_model: LogReturnFairValueModel,
    risk: RiskManager,
    order_manager: OrderManager,
    execution: PaperExecutionClient,
    paper_fill_engine: PaperFillEngine,
    execution_quality: ExecutionQualityTracker,
    reference_aggregator: ReferenceAggregator,
    last_volatility_update_key: Option<(String, DateTime<Utc>, Decimal)>,
    regime_classifiers: BTreeMap<MarketId, RegimeClassifier>,
    pending_settlements: BTreeMap<MarketId, PendingPaperSettlement>,
    pending_decision_application: Option<PendingDecisionApplication>,
}

fn select_funded_warmup_market<'a>(
    markets: impl Iterator<Item = &'a MarketSpec>,
    now: DateTime<Utc>,
    minimum_seconds_to_expiry: i64,
) -> Option<&'a MarketSpec> {
    let minimum_seconds_to_expiry = minimum_seconds_to_expiry.max(0);
    markets
        .filter(|market| (market.end_ts - now).num_seconds() >= minimum_seconds_to_expiry)
        .min_by_key(|market| market.end_ts)
}

impl RuntimeController {
    pub fn new(settings: RuntimeSettings) -> Self {
        let recorder = RuntimeRecorder::new(&settings);
        Self::new_with_recorder(settings, recorder)
    }

    fn new_with_recorder(settings: RuntimeSettings, recorder: RuntimeRecorder) -> Self {
        Self::new_with_recorder_capacity(settings, recorder, RECORDER_QUEUE_CAPACITY)
    }

    #[cfg(test)]
    fn new_with_recorder_and_capacity(
        settings: RuntimeSettings,
        recorder: RuntimeRecorder,
        capacity: usize,
    ) -> Self {
        Self::new_with_recorder_capacity(settings, recorder, capacity)
    }

    fn new_with_recorder_capacity(
        settings: RuntimeSettings,
        recorder: RuntimeRecorder,
        recorder_queue_capacity: usize,
    ) -> Self {
        let (broadcaster, _) = broadcast::channel(1_000);
        let intent_publisher =
            IntentPublisherConfig::optional_connect(&settings).unwrap_or_else(|error| {
                panic!(
                    "refusing runtime startup with invalid pointer-only intent preflight: {error}"
                )
            });
        let data = RuntimeData {
            decision_generation: 0,
            started_at: Utc::now(),
            paused: false,
            pause_reason: None,
            paused_at: None,
            kill_switch: false,
            clob_generation: None,
            clob_pending_generation: None,
            clob_terminal_generation: 0,
            clob_lease: None,
            clob_last_sequence: 0,
            clob_tokens: BTreeSet::new(),
            clob_pending_tokens: BTreeSet::new(),
            markets: BTreeMap::new(),
            books: BTreeMap::new(),
            reference: None,
            exact_references: VecDeque::new(),
            market_start_references: BTreeMap::new(),
            market_start_evidence_durable: BTreeSet::new(),
            pending_market_start_events: BTreeMap::new(),
            fair_values: BTreeMap::new(),
            chart_samples: BTreeMap::new(),
            chart_last_persisted_ms: BTreeMap::new(),
            decisions: VecDeque::new(),
            execution_reports: VecDeque::new(),
            recent_events: VecDeque::new(),
            settled_markets: Vec::new(),
            funded_warmup_market_id: None,
            feed_status: BTreeMap::new(),
            feed_events: 0,
            runtime_events: 0,
            drop_counts: BTreeMap::new(),
        };
        let engine = RuntimeEngine {
            decision_generation: 0,
            fair_model: LogReturnFairValueModel::new(settings.clone()),
            risk: RiskManager::new(settings.clone()),
            order_manager: OrderManager::new(),
            execution: PaperExecutionClient::new(),
            paper_fill_engine: PaperFillEngine::new(settings.clone()),
            execution_quality: ExecutionQualityTracker::default(),
            reference_aggregator: ReferenceAggregator::default(),
            last_volatility_update_key: None,
            regime_classifiers: BTreeMap::new(),
            pending_settlements: BTreeMap::new(),
            pending_decision_application: None,
        };
        let recorder = Arc::new(StdMutex::new(recorder));
        let recorder_metrics = Arc::new(RecorderMetrics::default());
        let (recorder_tx, recorder_rx) = std_mpsc::channel();
        let recorder_admission = Arc::new(Semaphore::new(recorder_queue_capacity));
        spawn_recorder_worker(
            Arc::clone(&recorder),
            recorder_rx,
            Arc::clone(&recorder_metrics),
        );
        Self {
            inner: Arc::new(RuntimeInner {
                settings,
                data: RwLock::new(data),
                engine: Mutex::new(engine),
                decision_gate: Arc::new(Mutex::new(())),
                recorder,
                recorder_enqueue_gate: StdMutex::new(()),
                recorder_tx,
                recorder_admission,
                recorder_metrics,
                persistence_filter: StdMutex::new(PersistenceFilter::default()),
                intent_publisher,
                broadcaster,
                started: AtomicBool::new(false),
                shutting_down: AtomicBool::new(false),
                termination_audit_complete: AtomicBool::new(false),
                shutdown_gate: Mutex::new(()),
                feed_task: StdMutex::new(None),
                background_tasks: StdMutex::new(Vec::new()),
            }),
        }
    }

    pub async fn run_execution_quality_probe(&self) -> Value {
        let events = deterministic_probe(Utc::now());
        let summary = events
            .iter()
            .find(|event| event.event_type == "execution_quality_probe_completed")
            .map(|event| event.payload.clone())
            .unwrap_or_else(|| {
                json!({
                    "status": "fail",
                    "detail": "deterministic probe did not produce a completion event",
                    "venue_contacted": false,
                    "live_order_placed": false,
                    "research_only": true
                })
            });
        for event in events {
            self.record_event(event.event_type, event.payload, None, None)
                .await;
        }
        summary
    }

    pub fn start_if_configured(&self) {
        if !self.inner.settings.deploy.run_bot_on_startup {
            return;
        }
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let provenance = runtime_provenance(&self.inner.settings).unwrap_or_else(|error| {
            panic!("refusing runtime startup without exact provenance: {error}")
        });
        self.persist_startup_provenance(provenance)
            .unwrap_or_else(|error| {
                panic!("refusing runtime startup because provenance was not persisted: {error}")
            });
        let (sender, receiver) = mpsc::channel(10_000);
        let feed_task = self.spawn_feed_event_loop(receiver);
        let mut background_tasks = vec![
            self.spawn_discovery_loop(),
            self.spawn_strategy_loop(),
            self.spawn_runtime_telemetry_loop(),
            self.spawn_runtime_provenance_loop(),
            self.spawn_market_feed_loop(sender.clone()),
            self.spawn_chainlink_http_loop(sender.clone()),
        ];
        if self.inner.settings.target.enable_polymarket_rtds_chainlink {
            background_tasks
                .push(self.spawn_rtds_loop(sender.clone(), FeedName::PolymarketRtdsChainlink));
        }
        if self.inner.settings.target.enable_polymarket_rtds_binance {
            background_tasks
                .push(self.spawn_rtds_loop(sender.clone(), FeedName::PolymarketRtdsBinance));
        }
        if self.inner.settings.target.enable_direct_binance_book_ticker {
            background_tasks.push(self.spawn_binance_loop(sender));
        } else {
            info!("Direct Binance bookTicker feed disabled by configuration");
        }
        *self
            .inner
            .feed_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(feed_task);
        self.inner
            .background_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(background_tasks);
        info!("Rust PolyEdge runtime started in paper mode");
    }

    fn runtime_tasks_running(&self) -> bool {
        let Ok(feed_task) = self.inner.feed_task.lock() else {
            return false;
        };
        let Ok(background_tasks) = self.inner.background_tasks.lock() else {
            return false;
        };
        feed_task.as_ref().is_some_and(|task| !task.is_finished())
            && !background_tasks.is_empty()
            && background_tasks.iter().all(|task| !task.is_finished())
    }

    async fn acquire_recorder_admission(&self) -> Result<OwnedSemaphorePermit, String> {
        if self.inner.shutting_down.load(Ordering::SeqCst) {
            return Err("runtime recorder is shutting down".to_owned());
        }
        let permit = Arc::clone(&self.inner.recorder_admission)
            .acquire_owned()
            .await
            .map_err(|_| "runtime recorder is shutting down".to_owned())?;
        if self.inner.shutting_down.load(Ordering::SeqCst) {
            drop(permit);
            return Err("runtime recorder is shutting down".to_owned());
        }
        Ok(permit)
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        let _shutdown_guard = self.inner.shutdown_gate.lock().await;
        self.inner.shutting_down.store(true, Ordering::SeqCst);
        self.inner.recorder_admission.close();
        let mut shutdown_error = if self
            .inner
            .termination_audit_complete
            .load(Ordering::Acquire)
        {
            None
        } else {
            match tokio::time::timeout(
                SHUTDOWN_TERMINATION_AUDIT_TIMEOUT,
                self.terminate_all_clob_generations("runtime shutdown", true),
            )
            .await
            {
                Ok(()) => {
                    self.inner
                        .termination_audit_complete
                        .store(true, Ordering::Release);
                    None
                }
                Err(_) => Some("runtime shutdown CLOB termination audit timed out".to_owned()),
            }
        };
        let background_tasks = self
            .inner
            .background_tasks
            .lock()
            .map_err(|error| format!("runtime background task lock poisoned: {error}"))?
            .drain(..)
            .collect::<Vec<_>>();
        for task in &background_tasks {
            task.abort();
        }
        for task in background_tasks {
            let _ = task.await;
        }

        let feed_task = self
            .inner
            .feed_task
            .lock()
            .map_err(|error| format!("runtime feed task lock poisoned: {error}"))?
            .take();
        if let Some(mut feed_task) = feed_task {
            match tokio::time::timeout(Duration::from_secs(10), &mut feed_task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    shutdown_error.get_or_insert(format!("runtime feed drain failed: {error}"));
                }
                Err(_) => {
                    feed_task.abort();
                    let _ = feed_task.await;
                    shutdown_error.get_or_insert_with(|| "runtime feed drain timed out".to_owned());
                }
            }
        }

        let deadline = Instant::now() + RECORDER_SHUTDOWN_DRAIN_TIMEOUT;
        while self.inner.recorder_metrics.queued.load(Ordering::Relaxed) != 0 {
            if Instant::now() >= deadline {
                shutdown_error.get_or_insert_with(|| "runtime recorder drain timed out".to_owned());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let flush_result = if self.inner.recorder_metrics.queued.load(Ordering::Acquire) == 0 {
            recorder_flush_result(&self.inner.recorder, &self.inner.recorder_metrics, false)
        } else {
            Ok(())
        };
        if let Some(error) = shutdown_error {
            return Err(error);
        }
        flush_result
    }

    pub async fn prepare_qset_v4_retirement(
        &self,
    ) -> Result<QsetV4WriterRetirementReceipt, String> {
        if self.inner.settings.deploy.app_name != QSET_V4_APP_NAME
            || self.inner.settings.azure.storage_container_name != QSET_V4_RAW_CONTAINER
        {
            return Err(
                "qset-v4 writer retirement requires the exact app and raw container".to_owned(),
            );
        }
        self.shutdown().await?;
        self.qset_v4_retirement_receipt(qset_v4_image_digest()?, qset_v4_source_revision()?)
    }

    fn qset_v4_retirement_receipt(
        &self,
        image_digest: String,
        source_revision: String,
    ) -> Result<QsetV4WriterRetirementReceipt, String> {
        let metrics = &self.inner.recorder_metrics;
        let final_assigned_sequence = metrics.last_assigned_sequence.load(Ordering::Acquire);
        let final_enqueued_sequence = metrics.last_enqueued_sequence.load(Ordering::Acquire);
        let final_enqueued_total = metrics.enqueued_total.load(Ordering::Acquire);
        let final_persisted_sequence = metrics.last_persisted_sequence.load(Ordering::Acquire);
        let final_persisted_total = metrics.persisted_total.load(Ordering::Acquire);
        let final_queued = metrics.queued.load(Ordering::Acquire);
        if final_queued != 0
            || final_assigned_sequence != final_persisted_sequence
            || final_assigned_sequence != final_enqueued_sequence
            || final_assigned_sequence != final_enqueued_total
            || final_enqueued_total != final_persisted_total
            || metrics.unrecovered_durable_events.load(Ordering::Acquire) != 0
            || metrics.flush_unrecovered.load(Ordering::Acquire)
        {
            return Err(
                "qset-v4 writer retirement recorder waterline is not fully durable".to_owned(),
            );
        }
        Ok(QsetV4WriterRetirementReceipt {
            schema: "polyedge.qset_v4_writer_retirement_receipt.v1",
            status: "prepared_for_retirement",
            retired_at: Utc::now(),
            campaign_id: QSET_V4_CAMPAIGN_ID,
            app_name: self.inner.settings.deploy.app_name.clone(),
            image_digest,
            source_revision,
            recorder_instance_id: metrics.recorder_instance_id.clone(),
            final_assigned_sequence,
            final_enqueued_sequence,
            final_enqueued_total,
            final_persisted_sequence,
            final_persisted_total,
            final_queued,
            flush_success: true,
        })
    }

    pub async fn prepare_qset_v5_retirement(
        &self,
    ) -> Result<QsetV5WriterRetirementReceipt, String> {
        if self.inner.settings.deploy.app_name != QSET_V5_APP_NAME
            || self.inner.settings.azure.storage_container_name != QSET_V5_RAW_CONTAINER
        {
            return Err(
                "qset-v5 writer retirement requires the exact app and raw container".to_owned(),
            );
        }
        self.shutdown().await?;
        self.qset_v5_retirement_receipt(qset_v5_image_digest()?, qset_v5_source_revision()?)
    }

    fn qset_v5_retirement_receipt(
        &self,
        image_digest: String,
        source_revision: String,
    ) -> Result<QsetV5WriterRetirementReceipt, String> {
        let metrics = &self.inner.recorder_metrics;
        let final_assigned_sequence = metrics.last_assigned_sequence.load(Ordering::Acquire);
        let final_enqueued_sequence = metrics.last_enqueued_sequence.load(Ordering::Acquire);
        let final_enqueued_total = metrics.enqueued_total.load(Ordering::Acquire);
        let final_persisted_sequence = metrics.last_persisted_sequence.load(Ordering::Acquire);
        let final_persisted_total = metrics.persisted_total.load(Ordering::Acquire);
        let final_queued = metrics.queued.load(Ordering::Acquire);
        if final_queued != 0
            || final_assigned_sequence != final_persisted_sequence
            || final_assigned_sequence != final_enqueued_sequence
            || final_assigned_sequence != final_enqueued_total
            || final_enqueued_total != final_persisted_total
            || metrics.unrecovered_durable_events.load(Ordering::Acquire) != 0
            || metrics.flush_unrecovered.load(Ordering::Acquire)
        {
            return Err(
                "qset-v5 writer retirement recorder waterline is not fully durable".to_owned(),
            );
        }
        Ok(QsetV5WriterRetirementReceipt {
            schema: "polyedge.qset_v5_writer_retirement_receipt.v1",
            status: "prepared_for_retirement",
            retired_at: Utc::now(),
            campaign_id: QSET_V5_CAMPAIGN_ID,
            app_name: self.inner.settings.deploy.app_name.clone(),
            image_digest,
            source_revision,
            recorder_instance_id: metrics.recorder_instance_id.clone(),
            final_assigned_sequence,
            final_enqueued_sequence,
            final_enqueued_total,
            final_persisted_sequence,
            final_persisted_total,
            final_queued,
            flush_success: true,
        })
    }

    pub async fn prepare_qset_v6_retirement(
        &self,
    ) -> Result<QsetV6WriterRetirementReceipt, String> {
        if self.inner.settings.deploy.app_name != QSET_V6_APP_NAME
            || self.inner.settings.azure.storage_container_name != QSET_V6_RAW_CONTAINER
        {
            return Err(
                "qset-v6 writer retirement requires the exact app and raw container".to_owned(),
            );
        }
        self.shutdown().await?;
        self.qset_v6_retirement_receipt(qset_v6_image_digest()?, qset_v6_source_revision()?)
    }

    fn qset_v6_retirement_receipt(
        &self,
        image_digest: String,
        source_revision: String,
    ) -> Result<QsetV6WriterRetirementReceipt, String> {
        let metrics = &self.inner.recorder_metrics;
        let final_assigned_sequence = metrics.last_assigned_sequence.load(Ordering::Acquire);
        let final_enqueued_sequence = metrics.last_enqueued_sequence.load(Ordering::Acquire);
        let final_enqueued_total = metrics.enqueued_total.load(Ordering::Acquire);
        let final_persisted_sequence = metrics.last_persisted_sequence.load(Ordering::Acquire);
        let final_persisted_total = metrics.persisted_total.load(Ordering::Acquire);
        let final_queued = metrics.queued.load(Ordering::Acquire);
        if final_queued != 0
            || final_assigned_sequence != final_persisted_sequence
            || final_assigned_sequence != final_enqueued_sequence
            || final_assigned_sequence != final_enqueued_total
            || final_enqueued_total != final_persisted_total
            || metrics.unrecovered_durable_events.load(Ordering::Acquire) != 0
            || metrics.flush_unrecovered.load(Ordering::Acquire)
        {
            return Err(
                "qset-v6 writer retirement recorder waterline is not fully durable".to_owned(),
            );
        }
        Ok(QsetV6WriterRetirementReceipt {
            schema: "polyedge.qset_v6_writer_retirement_receipt.v1",
            status: "prepared_for_retirement",
            retired_at: Utc::now(),
            campaign_id: QSET_V6_CAMPAIGN_ID,
            app_name: self.inner.settings.deploy.app_name.clone(),
            image_digest,
            source_revision,
            recorder_instance_id: metrics.recorder_instance_id.clone(),
            final_assigned_sequence,
            final_enqueued_sequence,
            final_enqueued_total,
            final_persisted_sequence,
            final_persisted_total,
            final_queued,
            flush_success: true,
        })
    }

    pub async fn prepare_qset_v7_retirement(
        &self,
    ) -> Result<QsetV7WriterRetirementReceipt, String> {
        if self.inner.settings.deploy.app_name != QSET_V7_APP_NAME
            || self.inner.settings.azure.storage_container_name != QSET_V7_RAW_CONTAINER
        {
            return Err(
                "qset-v7 writer retirement requires the exact app and raw container".to_owned(),
            );
        }
        self.shutdown().await?;
        self.qset_v7_retirement_receipt(qset_v7_image_digest()?, qset_v7_source_revision()?)
    }

    fn qset_v7_retirement_receipt(
        &self,
        image_digest: String,
        source_revision: String,
    ) -> Result<QsetV7WriterRetirementReceipt, String> {
        let metrics = &self.inner.recorder_metrics;
        let final_assigned_sequence = metrics.last_assigned_sequence.load(Ordering::Acquire);
        let final_enqueued_sequence = metrics.last_enqueued_sequence.load(Ordering::Acquire);
        let final_enqueued_total = metrics.enqueued_total.load(Ordering::Acquire);
        let final_persisted_sequence = metrics.last_persisted_sequence.load(Ordering::Acquire);
        let final_persisted_total = metrics.persisted_total.load(Ordering::Acquire);
        let final_queued = metrics.queued.load(Ordering::Acquire);
        if final_queued != 0
            || final_assigned_sequence != final_persisted_sequence
            || final_assigned_sequence != final_enqueued_sequence
            || final_assigned_sequence != final_enqueued_total
            || final_enqueued_total != final_persisted_total
            || metrics.unrecovered_durable_events.load(Ordering::Acquire) != 0
            || metrics.flush_unrecovered.load(Ordering::Acquire)
        {
            return Err(
                "qset-v7 writer retirement recorder waterline is not fully durable".to_owned(),
            );
        }
        Ok(QsetV7WriterRetirementReceipt {
            schema: "polyedge.qset_v7_writer_retirement_receipt.v1",
            status: "prepared_for_retirement",
            retired_at: Utc::now(),
            campaign_id: QSET_V7_CAMPAIGN_ID,
            app_name: self.inner.settings.deploy.app_name.clone(),
            image_digest,
            source_revision,
            recorder_instance_id: metrics.recorder_instance_id.clone(),
            final_assigned_sequence,
            final_enqueued_sequence,
            final_enqueued_total,
            final_persisted_sequence,
            final_persisted_total,
            final_queued,
            flush_success: true,
        })
    }

    fn persist_startup_provenance(&self, payload: Value) -> Result<(), String> {
        let event = RuntimeEvent {
            event_type: "runtime_provenance".to_owned(),
            ts: Utc::now(),
            data: payload,
        };
        self.inner
            .recorder_metrics
            .enqueued_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .recorder_metrics
            .batches_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .recorder_metrics
            .last_batch_size
            .store(1, Ordering::Relaxed);
        let recorded_event = {
            let _enqueue_gate = self
                .inner
                .recorder_enqueue_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.inner.recorder_metrics.bind(event.clone())?
        };
        self.inner
            .recorder_metrics
            .mark_enqueued(recorded_event.recorder_sequence());
        let mut staged = false;
        let mut last_error = None;
        let mut result = Err("startup provenance persistence was not attempted".to_owned());
        for attempt in 1..=STARTUP_PROVENANCE_ATTEMPTS {
            result = self
                .inner
                .recorder
                .lock()
                .map_err(|error| format!("runtime recorder lock poisoned: {error}"))
                .and_then(|mut recorder| {
                    if staged {
                        return recorder.retry_pending();
                    }
                    staged = true;
                    recorder.record_recorded_batch(std::slice::from_ref(&recorded_event))?;
                    recorder.flush()
                });
            if result.is_ok() {
                break;
            }
            last_error = result.as_ref().err().cloned();
            self.inner
                .recorder_metrics
                .failed_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                attempt,
                max_attempts = STARTUP_PROVENANCE_ATTEMPTS,
                "startup provenance durable persistence failed; retrying"
            );
            if attempt < STARTUP_PROVENANCE_ATTEMPTS {
                std::thread::sleep(Duration::from_millis(250));
            }
        }
        match result {
            Ok(()) => {
                self.inner
                    .recorder_metrics
                    .mark_persisted(1, recorded_event.recorder_sequence());
                if let Ok(mut state) = self.inner.data.try_write() {
                    state.runtime_events += 1;
                    state.recent_events.push_back(event.clone());
                    truncate(&mut state.recent_events, RECENT_LIMIT);
                }
                let _ = self.inner.broadcaster.send(event);
                Ok(())
            }
            Err(error) => Err(last_error.unwrap_or(error)),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.inner.broadcaster.subscribe()
    }

    pub async fn pause(&self, reason: Option<String>) -> Value {
        {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
            data.paused = true;
            data.paused_at = Some(Utc::now());
            data.pause_reason = reason.clone();
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        self.cancel_active_markets(reason.unwrap_or_else(|| "operator pause".to_owned()))
            .await;
        json!({
            "control": self.control_status().await,
            "audit_version": format!("rust-control-{}", Utc::now().timestamp_micros())
        })
    }

    pub async fn resume(&self, _reason: Option<String>) -> Value {
        {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
            data.paused = false;
            data.paused_at = None;
            data.pause_reason = None;
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        self.publish_only("control_state_changed", self.control_status().await)
            .await;
        json!({
            "control": self.control_status().await,
            "audit_version": format!("rust-control-{}", Utc::now().timestamp_micros())
        })
    }

    pub async fn set_kill_switch(&self, enabled: bool, reason: Option<String>) -> Value {
        {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
            data.kill_switch = enabled;
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        self.record_event(
            "control_state_changed",
            json!({"kill_switch": enabled, "reason": reason}),
            None,
            None,
        )
        .await;
        json!({
            "enabled": enabled,
            "audit_version": format!("rust-kill-switch-{}", Utc::now().timestamp_micros())
        })
    }

    async fn control_status(&self) -> Value {
        let data = self.inner.data.read().await;
        json!({
            "paused": data.paused,
            "paused_at": data.paused_at,
            "pause_reason": data.pause_reason
        })
    }

    fn spawn_feed_event_loop(&self, mut receiver: mpsc::Receiver<FeedEvent>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                runtime.handle_feed_event(event).await;
            }
        })
    }

    fn spawn_discovery_loop(&self) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.set_feed_status("Discovery", "starting", None).await;
            loop {
                let settings = runtime.inner.settings.clone();
                let result = tokio::task::spawn_blocking(move || {
                    polyedge_feeds::discover_markets(&settings)
                })
                .await;
                match result {
                    Ok(Ok(markets)) => {
                        runtime.replace_markets(markets).await;
                        runtime.set_feed_status("Discovery", "ok", None).await;
                    }
                    Ok(Err(error)) => {
                        runtime
                            .feed_error(FeedName::Discovery, error.to_string())
                            .await;
                    }
                    Err(error) => {
                        runtime
                            .feed_error(FeedName::Discovery, error.to_string())
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_secs_f64(
                    runtime
                        .inner
                        .settings
                        .target
                        .discovery_interval_seconds
                        .max(2.0),
                ))
                .await;
            }
        })
    }

    fn spawn_strategy_loop(&self) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                runtime.evaluate_once().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    }

    fn spawn_runtime_telemetry_loop(&self) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await;
            loop {
                interval.tick().await;
                let status = runtime.status().await;
                info!(
                    "{}",
                    json!({
                        "event": "runtime_health",
                        "execution_mode": status["execution_mode"],
                        "uptime_seconds": status["uptime"],
                        "markets": status["markets"],
                        "books": status["books"],
                        "recorder_queued": status["recorder_metrics"]["queued"],
                        "recorder_failed_total": status["recorder_metrics"]["failed_total"],
                        "recorder_recovered_total": status["recorder_metrics"]["recovered_total"],
                        "recorder_unrecovered_durable_events": status["recorder_metrics"]["unrecovered_durable_events"],
                        "recorder_flush_failed_total": status["recorder_metrics"]["flush_failed_total"],
                        "recorder_flush_recovered_total": status["recorder_metrics"]["flush_recovered_total"],
                        "recorder_flush_unrecovered": status["recorder_metrics"]["flush_unrecovered"],
                        "recorder_dropped_count": status["recorder_status"]["dropped_count"],
                        "recorder_error_count": status["recorder_status"]["error_count"],
                        "runtime_loop": status["task_health"]["runtime_loop"],
                        "feeds": status["task_health"]["feeds"]
                    })
                );
            }
        })
    }

    fn spawn_runtime_provenance_loop(&self) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RUNTIME_PROVENANCE_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                let mut provenance = runtime_provenance(&runtime.inner.settings)
                    .expect("startup already validated exact runtime provenance");
                let data = runtime.inner.data.read().await;
                provenance["essential_feed_health"] = json!({
                    "summary": feed_summary(&data, &runtime.inner.settings),
                    "feed_status": {
                        "Discovery": data.feed_status.get("Discovery").cloned().unwrap_or(Value::Null),
                        "PolymarketClobMarket": data.feed_status.get("PolymarketClobMarket").cloned().unwrap_or(Value::Null),
                        "PolymarketRtdsChainlink": data.feed_status.get("PolymarketRtdsChainlink").cloned().unwrap_or(Value::Null),
                        "PolymarketRtdsBinance": data.feed_status.get("PolymarketRtdsBinance").cloned().unwrap_or(Value::Null)
                    }
                });
                drop(data);
                runtime
                    .record_event("runtime_provenance", provenance, None, None)
                    .await;
            }
        })
    }

    fn spawn_market_feed_loop(&self, sender: mpsc::Sender<FeedEvent>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let mut generation = 0_u64;
            loop {
                let token_ids = runtime.market_token_ids().await;
                if token_ids.is_empty() {
                    runtime
                        .set_feed_status("PolymarketClobMarket", "waiting_for_markets", None)
                        .await;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                runtime.mark_market_feed_connecting().await;
                generation = generation.wrapping_add(1);
                let lease = runtime.begin_clob_generation(generation, &token_ids).await;
                let subscribed_tokens = token_ids.clone();
                let feed = polyedge_feeds::run_market_feed_generation_with_lease(
                    runtime.inner.settings.clone(),
                    token_ids,
                    generation,
                    lease,
                    sender.clone(),
                );
                tokio::pin!(feed);
                let mut refresh = tokio::time::interval(Duration::from_secs(2));
                loop {
                    tokio::select! {
                        result = &mut feed => {
                            runtime.terminate_clob_generation(generation, "market feed generation ended").await;
                            match result {
                                Ok(()) => {
                                    runtime
                                        .record_feed_disconnect(
                                            &[FeedName::PolymarketClobMarket],
                                            "market feed ended without a close error",
                                        )
                                        .await;
                                }
                                Err(error) => {
                                    runtime
                                        .feed_error(FeedName::PolymarketClobMarket, error.to_string())
                                        .await;
                                }
                            }
                            break;
                        }
                        _ = refresh.tick() => {
                            if runtime.market_token_ids().await != subscribed_tokens {
                                runtime.terminate_clob_generation(generation, "market token set changed").await;
                                break;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
    }

    fn spawn_rtds_loop(&self, sender: mpsc::Sender<FeedEvent>, source: FeedName) -> JoinHandle<()> {
        let runtime = self.clone();
        let settings = rtds_source_settings(&self.inner.settings, &source);
        tokio::spawn(async move {
            loop {
                runtime
                    .set_feed_status(&format!("{source:?}"), "connecting", None)
                    .await;
                match polyedge_feeds::run_rtds_feed(settings.clone(), sender.clone()).await {
                    Ok(()) => {
                        runtime
                            .record_feed_disconnect(
                                &[source.clone()],
                                "RTDS feed ended without a close error",
                            )
                            .await;
                    }
                    Err(error) => {
                        runtime.feed_error(source.clone(), error.to_string()).await;
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
    }

    fn spawn_chainlink_http_loop(&self, sender: mpsc::Sender<FeedEvent>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                let settings = runtime.inner.settings.clone();
                if settings.target.chainlink_reference_url.is_none() {
                    runtime
                        .set_feed_status("chainlink_http", "disabled", None)
                        .await;
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
                let result = tokio::task::spawn_blocking(move || {
                    polyedge_feeds::fetch_chainlink_reference(&settings)
                })
                .await;
                match result {
                    Ok(Ok(Some(reference))) => {
                        let _ = sender.send(FeedEvent::Reference(reference)).await;
                        runtime.set_feed_status("chainlink_http", "ok", None).await;
                    }
                    Ok(Ok(None)) => {
                        runtime
                            .set_feed_status("chainlink_http", "no_data", None)
                            .await
                    }
                    Ok(Err(error)) => {
                        runtime
                            .feed_error(FeedName::ChainlinkHttp, error.to_string())
                            .await
                    }
                    Err(error) => {
                        runtime
                            .feed_error(FeedName::ChainlinkHttp, error.to_string())
                            .await
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    }

    fn spawn_binance_loop(&self, sender: mpsc::Sender<FeedEvent>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                runtime
                    .set_feed_status("binance_book_ticker", "connecting", None)
                    .await;
                match polyedge_feeds::run_binance_book_ticker_feed(
                    runtime.inner.settings.clone(),
                    sender.clone(),
                )
                .await
                {
                    Ok(()) => {
                        runtime
                            .set_feed_status("binance_book_ticker", "disconnected", None)
                            .await;
                    }
                    Err(error) => {
                        runtime
                            .feed_error(FeedName::BinanceBookTicker, error.to_string())
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
    }

    async fn handle_feed_event(&self, event: FeedEvent) {
        {
            let mut data = self.inner.data.write().await;
            data.feed_events += 1;
        }
        match event {
            FeedEvent::Reference(reference) => self.handle_reference(reference).await,
            FeedEvent::RawMarketEvent(event) => {
                self.set_feed_status_at("PolymarketClobMarket", "ok", None, event.recorded_ts)
                    .await;
                self.handle_raw_market_event(event, None).await;
            }
            FeedEvent::Book(book) => {
                self.set_feed_status_at("PolymarketClobMarket", "ok", None, book.local_ts)
                    .await;
                self.handle_book(book, None).await;
            }
            FeedEvent::BookInvalidated(token_id) => self.invalidate_book(token_id, None).await,
            FeedEvent::ClobResyncBarrier(barrier) => self.handle_clob_resync_barrier(barrier).await,
            FeedEvent::ClobRawMarketEvent {
                generation,
                sequence,
                event,
            } => {
                self.handle_raw_market_event(event, Some((generation, sequence)))
                    .await
            }
            FeedEvent::ClobBook {
                generation,
                sequence,
                book,
            } => self.handle_book(book, Some((generation, sequence))).await,
            FeedEvent::ClobBookInvalidated {
                generation,
                sequence,
                token_id,
            } => {
                self.invalidate_book(token_id, Some((generation, sequence)))
                    .await
            }
            FeedEvent::Error {
                source,
                message,
                ts,
            } => self.feed_error_at(source, message, ts).await,
            FeedEvent::Heartbeat { source, ts } => {
                self.set_feed_status_at(&format!("{source:?}"), "ok", None, ts)
                    .await;
            }
        }
    }

    async fn replace_markets(&self, markets: Vec<MarketSpec>) {
        let next_tokens = markets
            .iter()
            .flat_map(|market| [market.up_token_id.clone(), market.down_token_id.clone()])
            .collect::<BTreeSet<_>>();
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut data = self.inner.data.write().await;
        let existing = data.markets.clone();
        let now = Utc::now();
        let mut final_tokens = existing
            .values()
            .filter(|market| {
                !data.settled_markets.contains(&market.market_id)
                    && now.signed_duration_since(market.end_ts).num_seconds()
                        <= PENDING_SETTLEMENT_RETENTION_SECONDS
            })
            .flat_map(|market| [market.up_token_id.clone(), market.down_token_id.clone()])
            .collect::<BTreeSet<_>>();
        final_tokens.extend(next_tokens);
        let current_tokens = if data.clob_pending_generation.is_some() {
            &data.clob_pending_tokens
        } else {
            &data.clob_tokens
        };
        let revoked_generation = (current_tokens != &final_tokens)
            .then(|| Self::revoke_clob_generation_locked(&mut data))
            .flatten();
        if let Some(generation) = revoked_generation {
            drop(data);
            let _ = self
                .record_required_events(vec![(
                    "clob_resync_aborted".to_owned(),
                    json!({
                        "generation": generation,
                        "reason": "market token set changed",
                        "fail_closed": true,
                        "ready": false
                    }),
                )])
                .await;
            data = self.inner.data.write().await;
        }
        let settled = data.settled_markets.clone();
        data.markets = existing
            .values()
            .filter(|market| {
                !settled.contains(&market.market_id)
                    && now.signed_duration_since(market.end_ts).num_seconds()
                        <= PENDING_SETTLEMENT_RETENTION_SECONDS
            })
            .cloned()
            .map(|market| (market.market_id.clone(), market))
            .collect();
        for mut market in markets {
            let mut recovered_start = None;
            if let Some(reference) = data.market_start_references.get(&market.market_id).cloned() {
                market = market.with_start_price(reference.price);
            } else if market.start_price.is_none() {
                if let Some(prior) = existing.get(&market.market_id) {
                    if let Some(start_price) = prior.start_price {
                        market = market.with_start_price(start_price);
                    }
                }
            }
            if !data.market_start_references.contains_key(&market.market_id) {
                let grace_millis = (self.inner.settings.target.start_price_capture_grace_seconds
                    * 1_000.0)
                    .round() as i64;
                if let Some(reference) = data
                    .exact_references
                    .iter()
                    .filter(|reference| {
                        reference.source_ts >= market.start_ts
                            && reference.source_ts
                                <= market.start_ts
                                    + chrono::Duration::milliseconds(grace_millis.max(0))
                    })
                    .min_by_key(|reference| reference.source_ts)
                    .cloned()
                {
                    market = market.with_start_price(reference.price);
                    data.market_start_references
                        .insert(market.market_id.clone(), reference.clone());
                    recovered_start = Some((
                        market.market_id.clone(),
                        json!({
                            "schema_version": 1,
                            "schema": "polyedge.market_start_price.v1",
                            "market_id": market.market_id,
                            "market_slug": market.market_slug,
                            "market_start_ts": market.start_ts,
                            "market_end_ts": market.end_ts,
                            "start_price": reference.price.to_string(),
                            "reference_source": reference.source,
                            "reference_source_ts": reference.source_ts,
                            "reference_exact_resolution_source": true,
                            "reference_stale": false,
                            "capture_method": "exact_reference_history_after_discovery"
                        }),
                    ));
                }
            }
            let payload = serde_json::to_value(&market).unwrap_or(Value::Null);
            data.markets.insert(market.market_id.clone(), market);
            drop(data);
            self.record_event("market", payload, Some("market_discovered"), None)
                .await;
            if let Some((market_id, recovered_start)) = recovered_start {
                let mut state = self.inner.data.write().await;
                state
                    .pending_market_start_events
                    .entry(market_id)
                    .or_insert_with(|| RuntimeEvent {
                        event_type: "market_start_price".to_owned(),
                        ts: Utc::now(),
                        data: recovered_start,
                    });
            }
            data = self.inner.data.write().await;
        }
        let warmup_market = select_funded_warmup_market(
            data.markets.values(),
            now,
            self.inner
                .settings
                .azure
                .strategy_intent_min_seconds_to_expiry,
        )
        .filter(|market| data.funded_warmup_market_id.as_ref() != Some(&market.market_id))
        .cloned();
        data.decision_generation = data.decision_generation.wrapping_add(1);
        drop(data);
        drop(_decision_guard);
        self.retry_pending_market_start_events().await;
        if let Some(market) = warmup_market {
            if self.maybe_publish_market_warmup(market.clone()).await {
                self.inner.data.write().await.funded_warmup_market_id = Some(market.market_id);
            }
        }
    }

    async fn maybe_publish_market_warmup(&self, market: MarketSpec) -> bool {
        if !self.inner.settings.azure.strategy_intent_operator_direct {
            return false;
        }
        let pointer_only_preflight = self
            .inner
            .intent_publisher
            .as_ref()
            .is_some_and(IntentPublisher::is_pointer_only_preflight);
        let publisher_runtime = self.clone();
        let publish_market = market.clone();
        let result = tokio::task::spawn_blocking(move || {
            let publisher = publisher_runtime
                .inner
                .intent_publisher
                .as_ref()
                .ok_or_else(|| "persistent intent publisher is unavailable".to_owned())?;
            publisher.warm_market(&publish_market)
        })
        .await
        .map_err(|error| format!("market warmup task failed: {error}"))
        .and_then(|result| result);
        match result {
            Ok(IntentPublisherPreparation::PointerOnly) if pointer_only_preflight => true,
            Ok(IntentPublisherPreparation::WarmupSent) => {
                info!(
                    market_id = %market.market_id,
                    condition_id = %market.condition_id,
                    "funded market warmup sent"
                );
                self.record_event(
                    "funded_market_warmup_sent",
                    json!({
                        "market_id": market.market_id,
                        "condition_id": market.condition_id,
                        "token_ids": [market.up_token_id, market.down_token_id],
                        "market_end_ts": market.end_ts,
                        "executable": false
                    }),
                    None,
                    None,
                )
                .await;
                true
            }
            Ok(IntentPublisherPreparation::NotRequired) => false,
            Ok(IntentPublisherPreparation::PointerOnly) => false,
            Err(reason) => {
                warn!(
                    market_id = %market.market_id,
                    reason = %reason,
                    "funded market warmup not sent"
                );
                self.record_event(
                    "funded_market_warmup_not_sent",
                    json!({
                        "market_id": market.market_id,
                        "condition_id": market.condition_id,
                        "reason": reason,
                        "fail_closed": true,
                        "executable": false
                    }),
                    None,
                    None,
                )
                .await;
                false
            }
        }
    }

    async fn handle_reference(&self, reference: ReferencePrice) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut engine = self.inner.engine.lock().await;
        let composite = engine
            .reference_aggregator
            .update(reference, &self.inner.settings);
        if composite.exact_resolution_source {
            let key = (
                composite.source.clone(),
                composite.source_ts,
                composite.price,
            );
            if engine.last_volatility_update_key.as_ref() != Some(&key) {
                engine.fair_model.update_volatility(&composite);
                engine.last_volatility_update_key = Some(key);
            }
        }
        {
            let mut data = self.inner.data.write().await;
            data.reference = Some(composite.clone());
            if composite.exact_resolution_source && !composite.stale {
                let duplicate = data.exact_references.back().is_some_and(|reference| {
                    reference.source == composite.source
                        && reference.source_ts == composite.source_ts
                        && reference.price == composite.price
                });
                if !duplicate {
                    data.exact_references.push_back(composite.clone());
                    truncate(&mut data.exact_references, EXACT_REFERENCE_HISTORY_LIMIT);
                }
            }
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        engine.decision_generation = engine.decision_generation.wrapping_add(1);
        drop(engine);
        drop(_decision_guard);
        self.capture_market_start_prices(&composite).await;
        self.settle_finished_markets(&composite).await;
        self.record_event("reference", &composite, Some("reference_update"), None)
            .await;
    }

    async fn handle_book(&self, book: BookState, clob_sequence: Option<(u64, u64)>) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let (market, quality_events) = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
            if !self.accept_clob_sequence(&mut data, clob_sequence, book.local_ts) {
                return;
            }
            data.books.insert(book.token_id.clone(), book.clone());
            data.decision_generation = data.decision_generation.wrapping_add(1);
            let market = markets_by_token_from_data(&data)
                .get(&book.token_id)
                .cloned();
            drop(data);
            let mut engine = self.inner.engine.lock().await;
            let events = engine.execution_quality.observe_book(&book);
            engine.decision_generation = engine.decision_generation.wrapping_add(1);
            (market, events)
        };
        let publish_payload = book_summary(&book, market.as_ref());
        let recorded_book = compact_recorded_book(&book);
        self.record_event(
            "book",
            recorded_book,
            Some("book_update_summary"),
            Some(publish_payload),
        )
        .await;
        if let Some(market) = market {
            self.push_market_chart_sample(&market.market_id).await;
        }
        if !quality_events.is_empty() {
            self.force_record_book(&book).await;
        }
        for event in quality_events {
            self.record_event(event.event_type, event.payload, None, None)
                .await;
        }
        self.handle_paper_fills(&book, clob_sequence.map(|(generation, _)| generation))
            .await;
    }

    async fn invalidate_book(&self, token_id: TokenId, clob_sequence: Option<(u64, u64)>) {
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut data = self.inner.data.write().await;
        if !self.accept_clob_sequence(&mut data, clob_sequence, Utc::now()) {
            return;
        }
        data.books.remove(&token_id);
        data.decision_generation = data.decision_generation.wrapping_add(1);
        drop(data);
        let mut engine = self.inner.engine.lock().await;
        engine.decision_generation = engine.decision_generation.wrapping_add(1);
    }

    async fn handle_raw_market_event(
        &self,
        event: polyedge_feeds::MarketChannelEvent,
        clob_sequence: Option<(u64, u64)>,
    ) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let quality_events = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            {
                let mut data = self.inner.data.write().await;
                if !self.accept_clob_sequence(&mut data, clob_sequence, event.recorded_ts) {
                    return;
                }
            }
            let mut engine = self.inner.engine.lock().await;
            let events = engine.execution_quality.observe_market_event(&event);
            engine.decision_generation = engine.decision_generation.wrapping_add(1);
            events
        };
        let mut payload = serde_json::to_value(&event).unwrap_or(Value::Null);
        let token_id = event.token_id.as_deref().or(event.asset_id.as_deref());
        if let Some(token_id) = token_id {
            let token = TokenId::new(token_id.to_owned());
            let market = {
                let data = self.inner.data.read().await;
                markets_by_token_from_data(&data).get(&token).cloned()
            };
            if let (Some(market), Value::Object(map)) = (market, &mut payload) {
                map.entry("market_id".to_owned())
                    .or_insert_with(|| json!(market.market_id));
                map.entry("condition_id".to_owned())
                    .or_insert_with(|| json!(market.condition_id));
                if token == market.up_token_id {
                    map.entry("outcome".to_owned())
                        .or_insert_with(|| json!("up"));
                } else if token == market.down_token_id {
                    map.entry("outcome".to_owned())
                        .or_insert_with(|| json!("down"));
                }
            }
        }
        self.record_event("raw_market_event", payload, None, None)
            .await;
        for quality_event in quality_events {
            self.record_event(quality_event.event_type, quality_event.payload, None, None)
                .await;
        }
    }

    async fn handle_paper_fills(&self, book: &BookState, clob_generation: Option<u64>) {
        let markets_by_token = {
            let data = self.inner.data.read().await;
            markets_by_token_from_data(&data)
        };
        let reports = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            if let Some(generation) = clob_generation {
                let data = self.inner.data.read().await;
                if data.clob_generation != Some(generation)
                    || data
                        .clob_lease
                        .as_ref()
                        .is_none_or(ClobGenerationLease::is_terminal)
                {
                    return;
                }
            }
            let mut engine = self.inner.engine.lock().await;
            let resting: Vec<_> = engine
                .execution
                .resting_for_token(&book.token_id)
                .into_iter()
                .map(|resting| RestingMakerOrder {
                    order_id: resting.order_id,
                    decision: resting.decision,
                    report: resting.report,
                })
                .collect();
            let tracked = engine.order_manager.open_order_ids();
            let candidate_reports = engine.paper_fill_engine.on_book(
                book,
                &markets_by_token,
                &resting,
                &tracked,
                Utc::now(),
            );
            let mut filled = Vec::new();
            for report in candidate_reports {
                let Some(order_id) = report.order_id.clone() else {
                    continue;
                };
                let avg_price = report.avg_price.unwrap_or(Decimal::ZERO);
                if let Some(mut actual) =
                    engine
                        .execution
                        .fill_maker_order(&order_id, avg_price, report.local_ts)
                {
                    actual.status = "paper_filled_maker".to_owned();
                    engine.order_manager.on_fill(&actual);
                    engine.risk.open_order_count = engine.order_manager.open_order_count();
                    engine.risk.on_execution_report(&actual);
                    filled.push(actual);
                }
            }
            if !filled.is_empty() {
                engine.decision_generation = engine.decision_generation.wrapping_add(1);
            }
            filled
        };
        if !reports.is_empty() {
            self.force_record_book(book).await;
        }
        for report in reports {
            self.record_execution_report(report, true).await;
        }
    }

    async fn execute_paper_decision(
        &self,
        decision: &TradeDecision,
        books: &BTreeMap<TokenId, BookState>,
    ) -> Vec<ExecutionReport> {
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut engine = self.inner.engine.lock().await;
        let result = self
            .execute_paper_decision_with_engine(&mut engine, decision, books)
            .await;
        match result {
            Ok(reports) => {
                if matches!(
                    decision.action,
                    DecisionAction::Place | DecisionAction::CancelAll
                ) {
                    engine.decision_generation = engine.decision_generation.wrapping_add(1);
                }
                reports
            }
            Err(error) => {
                error!("paper execution failed: {error}");
                Vec::new()
            }
        }
    }

    async fn execute_paper_decision_with_engine(
        &self,
        engine: &mut RuntimeEngine,
        decision: &TradeDecision,
        books: &BTreeMap<TokenId, BookState>,
    ) -> Result<Vec<ExecutionReport>, String> {
        let cancel_requested_ts = Utc::now();
        let result = if decision.action == DecisionAction::CancelAll {
            engine.execution.cancel_all(Some(&decision.market_id)).await
        } else {
            engine
                .execution
                .submit(decision)
                .await
                .map(|report| vec![report])
        };
        let mut reports = result.map_err(|error| error.to_string())?;
        for report in &mut reports {
            if decision.action == DecisionAction::CancelAll {
                report.raw.insert(
                    "cancel_requested_ts".to_owned(),
                    json!(cancel_requested_ts.to_rfc3339()),
                );
            }
            if report.status == "paper_resting" {
                let book = decision
                    .token_id
                    .as_ref()
                    .and_then(|token_id| books.get(token_id));
                if let Some(registration) = engine.execution_quality.register_order(
                    decision,
                    report,
                    book,
                    self.inner.settings.paper.order_live_after_ms,
                ) {
                    report
                        .raw
                        .insert("execution_quality".to_owned(), registration);
                }
            }
            engine.order_manager.on_execution_report(decision, report);
            engine.risk.on_execution_report(report);
        }
        engine.risk.open_order_count = engine.order_manager.open_order_count();
        Ok(reports)
    }

    async fn evaluate_once(&self) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let (
            reference,
            references,
            markets,
            books,
            paused,
            kill_switch,
            data_generation,
            clob_generation,
        ) = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let data = self.inner.data.read().await;
            if data.clob_generation.is_none()
                || data
                    .clob_lease
                    .as_ref()
                    .is_none_or(ClobGenerationLease::is_terminal)
            {
                return;
            }
            (
                data.reference.clone(),
                data.exact_references.clone(),
                active_markets(&data)
                    .into_iter()
                    .filter_map(|market| {
                        if !data
                            .market_start_evidence_durable
                            .contains(&market.market_id)
                        {
                            return None;
                        }
                        let start_reference =
                            data.market_start_references.get(&market.market_id)?;
                        let evidence =
                            market_start_evidence(market, start_reference, &self.inner.settings)?;
                        Some((market.clone(), evidence))
                    })
                    .collect::<Vec<_>>(),
                data.books.clone(),
                data.paused,
                data.kill_switch,
                data.decision_generation,
                data.clob_generation.expect("checked above"),
            )
        };
        let Some(reference) = reference else {
            return;
        };
        if paused {
            return;
        }
        for (market, market_start_evidence) in markets {
            let market_books = books_for_market(&market, &books);
            let (
                prepared_decisions,
                strategy_evidence,
                strategy_batch,
                batch_id,
                fair_value,
                decision_ts,
                classifier_after,
                decision_state_generation,
            ) = {
                let _decision_guard = self.inner.decision_gate.lock().await;
                let observed_data_generation = {
                    let data = self.inner.data.read().await;
                    data.decision_generation
                };
                if observed_data_generation != data_generation {
                    continue;
                }
                let engine = self.inner.engine.lock().await;
                let decision_state_generation = DecisionStateGeneration {
                    data: data_generation,
                    engine: engine.decision_generation,
                };
                let now = Utc::now();
                let Some(fair_value) = engine
                    .fair_model
                    .compute(&market, &reference, now, None, None)
                else {
                    continue;
                };
                {
                    let mut data = self.inner.data.write().await;
                    data.fair_values.insert(
                        market.market_id.clone(),
                        serde_json::to_value(&fair_value).unwrap_or(Value::Null),
                    );
                }
                self.push_market_chart_sample(&market.market_id).await;
                self.record_event("fair_value", &fair_value, Some("fair_value_update"), None)
                    .await;
                let adaptive_mode = configured_adaptive_mode(&self.inner.settings);
                let decision_config_sha256 =
                    decision_config_sha256(&self.inner.settings, adaptive_mode);
                let classifier_before = adaptive_mode.map(|_| {
                    engine
                        .regime_classifiers
                        .get(&market.market_id)
                        .map(RegimeClassifier::snapshot)
                        .unwrap_or_else(|| RegimeClassifier::default().snapshot())
                });
                let regime_feature_input = runtime_regime_feature_input(
                    &market,
                    &fair_value,
                    &reference,
                    &references,
                    &market_books,
                    now,
                    engine.order_manager.open_order_count(),
                    &self.inner.settings,
                );
                let pipeline_input = DecisionPipelineInputV3 {
                    schema_version: 3,
                    settings: secret_safe_pipeline_settings(&self.inner.settings),
                    market: market.clone(),
                    market_start_evidence: market_start_evidence.clone(),
                    fair_value: fair_value.clone(),
                    reference: reference.clone(),
                    books: market_books.clone(),
                    decision_ts: now,
                    kill_switch_enabled: kill_switch,
                    adaptive_mode,
                    regime_feature_input,
                    classifier_before,
                    risk_before: engine.risk.snapshot(),
                    order_manager_before: engine.order_manager.snapshot(),
                };
                let pipeline_input_value = match wire_normalized_json(&pipeline_input) {
                    Ok(value @ Value::Object(_)) => value,
                    Ok(_) => {
                        error!("wire-normalized decision pipeline input was not an object");
                        continue;
                    }
                    Err(error) => {
                        error!("decision pipeline input wire normalization failed: {error}");
                        continue;
                    }
                };
                let pipeline_input_sha256 = value_sha256(&pipeline_input_value);
                let Some(market_start_evidence_value) =
                    pipeline_input_value.get("market_start_evidence")
                else {
                    error!("wire-normalized pipeline input omitted market start evidence");
                    continue;
                };
                let market_start_evidence_sha256 = value_sha256(market_start_evidence_value);
                let batch_id = decision_batch_id_v4(&pipeline_input_sha256);
                let pipeline_output = evaluate_decision_pipeline_v3(&pipeline_input);
                let pipeline_output_value = match wire_normalized_json(&pipeline_output) {
                    Ok(value @ Value::Object(_)) => value,
                    Ok(_) => {
                        error!("wire-normalized decision pipeline output was not an object");
                        continue;
                    }
                    Err(error) => {
                        error!("decision pipeline output wire normalization failed: {error}");
                        continue;
                    }
                };
                let pipeline_output_sha256 = value_sha256(&pipeline_output_value);
                let classifier_after = pipeline_output.classifier_after.clone();
                let features = pipeline_input.regime_feature_input.clone().build();
                let mut strategy_evidence = Vec::new();
                for evaluated in &pipeline_output.strategy_evaluations {
                    strategy_evidence.push(json!({
                        "schema_version": 1,
                        "decision_batch_schema_version": 4,
                        "strategy_batch_id": batch_id.clone(),
                        "evaluation_index": evaluated.evaluation_index,
                        "market_id": market.market_id.clone(),
                        "decision_ts": now,
                        "mode": adaptive_mode,
                        "strategy_config": pipeline_input.settings.strategy.clone(),
                        "raw_decision": pipeline_output.raw_decisions.get(evaluated.evaluation_index),
                        "quote_context": evaluated.quote_context.clone(),
                        "features": features.clone(),
                        "classifier_before": evaluated.classifier_before.clone(),
                        "classifier_after": evaluated.classifier_after.clone(),
                        "evaluated_decision": evaluated.evaluated_decision.clone(),
                        "cancel_existing": evaluated.cancel_existing,
                        "strategy_metadata": evaluated.metadata.clone()
                    }));
                }
                let decisions = &pipeline_output.final_decisions;
                let Some(final_decision_evidence) = final_decision_evidence_v1(&pipeline_output)
                    .filter(|evidence| evidence.len() == decisions.len())
                else {
                    error!("final decision evidence did not bind one-to-one with pipeline output");
                    continue;
                };
                let prepared_decisions = decisions
                    .iter()
                    .zip(final_decision_evidence)
                    .enumerate()
                    .map(|(output_index, (decision, evidence))| {
                        let metadata = evidence.strategy_metadata;
                        let unbound_payload = wire_normalized_json(&evidence.payload)?;
                        let binding = DecisionBatchBinding {
                            batch_id: batch_id.clone(),
                            output_index,
                            decision_sha256: value_sha256(&unbound_payload),
                        };
                        let recorded_payload =
                            bind_decision_event_payload(&unbound_payload, &binding);
                        Ok::<PreparedDecision, serde_json::Error>(PreparedDecision {
                            decision: decision.clone(),
                            metadata,
                            unbound_payload,
                            binding,
                            payload: recorded_payload,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>();
                let prepared_decisions = match prepared_decisions {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        error!("final decision evidence wire normalization failed: {error}");
                        continue;
                    }
                };
                let final_decisions = prepared_decisions
                    .iter()
                    .map(|prepared| {
                        json!({
                            "output_index": prepared.binding.output_index,
                            "decision_sha256": prepared.binding.decision_sha256,
                            "decision": prepared.unbound_payload
                        })
                    })
                    .collect::<Vec<_>>();
                let strategy_batch = json!({
                    "schema_version": 4,
                    "schema": "polyedge.strategy_decision_batch.v4",
                    "parity_scope": "full_decision_pipeline_recomputation",
                    "batch_id": batch_id.clone(),
                    "market_id": market.market_id.clone(),
                    "decision_ts": now,
                    "candidate": adaptive_mode.map(FrozenStrategyMode::candidate),
                    "decision_config_schema": "polyedge.decision_config.v1",
                    "decision_config_sha256": decision_config_sha256,
                    "market_start_evidence_sha256": market_start_evidence_sha256,
                    "pipeline_input_sha256": pipeline_input_sha256,
                    "pipeline_output_sha256": pipeline_output_sha256,
                    "pipeline_input": pipeline_input_value,
                    "pipeline_output": pipeline_output_value,
                    "bound_final_decisions": final_decisions
                });
                let strategy_batch = match wire_normalized_json(&strategy_batch) {
                    Ok(value @ Value::Object(_)) => value,
                    Ok(_) => {
                        error!("wire-normalized strategy batch was not an object");
                        continue;
                    }
                    Err(error) => {
                        error!("strategy batch wire normalization failed: {error}");
                        continue;
                    }
                };
                if let Err(reason) = validate_decision_batch_content_bindings(&strategy_batch) {
                    error!(reason, "strategy batch failed content-binding self-check");
                    continue;
                }
                (
                    prepared_decisions,
                    strategy_evidence,
                    strategy_batch,
                    batch_id,
                    fair_value,
                    now,
                    classifier_after,
                    decision_state_generation,
                )
            };

            let mut required_events = vec![("strategy_decision_batch".to_owned(), strategy_batch)];
            required_events.extend(
                strategy_evidence
                    .into_iter()
                    .map(|evidence| ("strategy_evaluation".to_owned(), evidence)),
            );
            let mut recorded_book_tokens = BTreeSet::new();
            for prepared in &prepared_decisions {
                if let Some(token_id) = prepared.decision.token_id.as_ref() {
                    if recorded_book_tokens.insert(token_id.clone()) {
                        if let Some(book) = market_books.get(token_id) {
                            required_events.push((
                                "book".to_owned(),
                                serde_json::to_value(compact_recorded_book(book))
                                    .unwrap_or(Value::Null),
                            ));
                        }
                    }
                }
            }
            required_events.extend(
                prepared_decisions
                    .iter()
                    .map(|prepared| ("decision".to_owned(), prepared.payload.clone())),
            );
            let decision_guard = Arc::clone(&self.inner.decision_gate).lock_owned().await;
            {
                let data = self.inner.data.read().await;
                if data.clob_generation != Some(clob_generation)
                    || data
                        .clob_lease
                        .as_ref()
                        .is_none_or(ClobGenerationLease::is_terminal)
                {
                    continue;
                }
            }
            let observed_data_generation = {
                let data = self.inner.data.read().await;
                data.decision_generation
            };
            let mut apply_engine = self.inner.engine.lock().await;
            if let Some(observed_generation) = stale_decision_state_generation(
                observed_data_generation,
                &apply_engine,
                decision_state_generation,
            ) {
                drop(apply_engine);
                drop(decision_guard);
                self.record_event(
                    "strategy_decision_batch_stale",
                    json!({
                        "batch_id": batch_id,
                        "market_id": market.market_id,
                        "evaluated_data_generation": decision_state_generation.data,
                        "evaluated_engine_generation": decision_state_generation.engine,
                        "observed_data_generation": observed_generation.data,
                        "observed_engine_generation": observed_generation.engine,
                        "decisions_executed": false,
                        "reason": "decision-relevant data or engine state changed before durable compare-and-apply"
                    }),
                    None,
                    None,
                )
                .await;
                continue;
            }
            if !self.record_required_events(required_events).await {
                drop(apply_engine);
                drop(decision_guard);
                self.record_event(
                    "strategy_decision_batch_rejected",
                    json!({
                        "batch_id": batch_id,
                        "market_id": market.market_id,
                        "reason": "required decision evidence was not durably appended and flushed",
                        "decisions_executed": false
                    }),
                    None,
                    None,
                )
                .await;
                continue;
            }

            let classifier_changed = classifier_after.is_some();
            if let Some(classifier_after) = classifier_after {
                apply_engine.regime_classifiers.insert(
                    market.market_id.clone(),
                    RegimeClassifier::from_snapshot(classifier_after),
                );
            }
            let mut applied_outputs = Vec::with_capacity(prepared_decisions.len());
            let mut engine_changed = classifier_changed;
            for prepared in &prepared_decisions {
                let applied = if matches!(
                    prepared.decision.action,
                    DecisionAction::Place | DecisionAction::CancelAll
                ) {
                    engine_changed = true;
                    match self
                        .execute_paper_decision_with_engine(
                            &mut apply_engine,
                            &prepared.decision,
                            &market_books,
                        )
                        .await
                    {
                        Ok(reports) => bind_applied_decision_output(prepared, reports),
                        Err(error) => {
                            error!(
                                batch_id = prepared.binding.batch_id,
                                output_index = prepared.binding.output_index,
                                "paper decision output was not applied: {error}"
                            );
                            None
                        }
                    }
                } else {
                    None
                };
                applied_outputs.push(applied);
            }
            if engine_changed {
                apply_engine.decision_generation = apply_engine.decision_generation.wrapping_add(1);
            }
            drop(apply_engine);

            let mut applied_events = Vec::new();
            for applied in applied_outputs.iter().flatten() {
                applied_events.push((
                    "paper_decision_output_applied".to_owned(),
                    applied.application.clone(),
                ));
                for report in &applied.reports {
                    applied_events.push((
                        "execution_report".to_owned(),
                        serde_json::to_value(report).unwrap_or(Value::Null),
                    ));
                    if let Some(registration) = report.raw.get("execution_quality") {
                        applied_events.push((
                            "paper_order_queue_registration".to_owned(),
                            registration.clone(),
                        ));
                    }
                }
            }
            let applied_events = required_runtime_events(applied_events, Utc::now());
            let pending_application =
                (!applied_events.is_empty()).then(|| PendingDecisionApplication {
                    batch_id: batch_id.clone(),
                    events: applied_events.clone(),
                    reports: applied_outputs
                        .iter()
                        .flatten()
                        .flat_map(|applied| applied.reports.iter().cloned())
                        .collect(),
                });
            let application_evidence_durable = self
                .record_required_runtime_events(applied_events, false)
                .await;
            if !application_evidence_durable {
                if let Some(pending) = pending_application {
                    let mut engine = self.inner.engine.lock().await;
                    engine.pending_decision_application = Some(pending);
                }
            }
            let local_execution_model = self
                .inner
                .settings
                .azure
                .publish_strategy_canary_intents
                .then(|| resolve_local_execution_model(&self.inner.settings))
                .flatten();
            let publication_guard = match local_execution_model {
                Some(_) => Some(Arc::new(decision_guard)),
                None => {
                    drop(decision_guard);
                    None
                }
            };

            let mut persisted_reports = Vec::new();
            for (prepared, applied) in prepared_decisions.into_iter().zip(applied_outputs) {
                let decision = prepared.decision;
                let metadata = prepared.metadata;
                self.accept_durable_decision(decision.clone()).await;
                if decision.action == DecisionAction::Place
                    && application_evidence_durable
                    && applied.is_some()
                {
                    self.maybe_publish_execution_intent(
                        &market,
                        &fair_value,
                        &reference,
                        &market_books,
                        &decision,
                        metadata.as_ref(),
                        decision_ts,
                        clob_generation,
                        local_execution_model
                            .as_ref()
                            .zip(publication_guard.as_ref())
                            .map(|(model, guard)| (model.clone(), Arc::clone(guard))),
                    )
                    .await;
                }
                if application_evidence_durable {
                    if let Some(applied) = applied {
                        persisted_reports.extend(applied.reports);
                    }
                }
            }
            drop(publication_guard);
            for report in persisted_reports {
                self.accept_persisted_execution_report(report, false).await;
            }
        }
    }

    async fn push_decision(&self, decision: TradeDecision) {
        self.push_decision_with_metadata(decision, None).await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn maybe_publish_execution_intent(
        &self,
        market: &MarketSpec,
        fair_value: &FairValue,
        reference: &ReferencePrice,
        books: &BTreeMap<TokenId, BookState>,
        decision: &TradeDecision,
        metadata: Option<&StrategyDecisionMetadata>,
        decision_ts: DateTime<Utc>,
        clob_generation: u64,
        local_publication: Option<(
            Result<IntentExecutionModel, String>,
            Arc<OwnedMutexGuard<()>>,
        )>,
    ) {
        if !self.inner.settings.azure.publish_strategy_canary_intents {
            return;
        }
        let Some(metadata) = metadata else {
            self.record_event(
                "execution_intent_not_published",
                json!({
                    "market_id": market.market_id,
                    "reason": "shared frozen strategy metadata is missing",
                    "fail_closed": true
                }),
                None,
                None,
            )
            .await;
            return;
        };
        let Some(token_id) = decision.token_id.as_ref() else {
            self.record_event(
                "execution_intent_not_published",
                json!({
                    "market_id": market.market_id,
                    "candidate_version": metadata.candidate.version,
                    "reason": "decision token_id is missing",
                    "fail_closed": true
                }),
                None,
                None,
            )
            .await;
            return;
        };
        let Some(book) = books.get(token_id) else {
            self.record_event(
                "execution_intent_not_published",
                json!({
                    "market_id": market.market_id,
                    "token_id": token_id,
                    "candidate_version": metadata.candidate.version,
                    "reason": "captured token book is missing",
                    "fail_closed": true
                }),
                None,
                None,
            )
            .await;
            return;
        };
        // Azure's credential and blob clients are synchronous. Keep canonical
        // model control reads off the runtime/feed task so a transient storage
        // delay cannot stall recording or market-data processing.
        let (execution_model_result, decision_guard) = match local_publication {
            Some((model, guard)) => (model, Some(guard)),
            None => {
                let model_settings = self.inner.settings.clone();
                let model = tokio::task::spawn_blocking(move || {
                    resolve_execution_model(&model_settings, decision_ts)
                })
                .await
                .map_err(|error| format!("execution-model control task failed: {error}"))
                .and_then(|result| result);
                (model, None)
            }
        };
        let execution_model = match execution_model_result {
            Ok(model) => model,
            Err(reason) => {
                warn!(
                    market_id = %market.market_id,
                    candidate_version = %metadata.candidate.version,
                    reason = %reason,
                    "execution intent not published"
                );
                self.record_event(
                    "execution_intent_not_published",
                    json!({
                        "market_id": market.market_id,
                        "condition_id": market.condition_id,
                        "token_id": token_id,
                        "candidate_version": metadata.candidate.version,
                        "reason": reason,
                        "fail_closed": true
                    }),
                    None,
                    None,
                )
                .await;
                return;
            }
        };
        let intent = match build_execution_intent_with_model(
            &self.inner.settings,
            market,
            fair_value,
            reference,
            book,
            decision,
            metadata,
            decision_ts,
            &execution_model,
        ) {
            Ok(intent) => intent,
            Err(reason) => {
                warn!(
                    market_id = %market.market_id,
                    candidate_version = %metadata.candidate.version,
                    reason = %reason,
                    "execution intent not published"
                );
                self.record_event(
                    "execution_intent_not_published",
                    json!({
                        "market_id": market.market_id,
                        "condition_id": market.condition_id,
                        "token_id": token_id,
                        "candidate_version": metadata.candidate.version,
                        "reason": reason,
                        "fail_closed": true
                    }),
                    None,
                    None,
                )
                .await;
                return;
            }
        };
        let runtime = self.clone();
        let publication_token_id = token_id.clone();
        let publication_book = book.clone();
        let wait_for_local_publication = decision_guard.is_some();
        let join_decision_id = intent.decision_id.clone();
        let join_market_id = intent.market_id.clone();
        let publication = tokio::spawn(async move {
            let publish_intent = intent.clone();
            let publisher_runtime = runtime.clone();
            let publish = move || {
                let publisher = publisher_runtime
                    .inner
                    .intent_publisher
                    .as_ref()
                    .ok_or_else(|| "persistent intent publisher is unavailable".to_owned())?;
                publisher.publish(&publish_intent)
            };
            let result = if let Some(_decision_guard) = decision_guard {
                runtime
                    .execute_live_clob_publication_while_gated(
                        clob_generation,
                        &publication_token_id,
                        &publication_book,
                        publish,
                    )
                    .await
            } else {
                runtime
                    .execute_live_clob_publication(
                        clob_generation,
                        &publication_token_id,
                        &publication_book,
                        publish,
                    )
                    .await
            };
            match result {
                Ok(None) => {
                    runtime
                        .record_event(
                            "execution_intent_not_published",
                            json!({
                                "decision_id": intent.decision_id,
                                "market_id": intent.market_id,
                                "reason": "CLOB generation changed or is not ready",
                                "fail_closed": true,
                                "order_submission_attempted": false
                            }),
                            None,
                            None,
                        )
                        .await;
                }
                Ok(Some(published)) => {
                    info!(
                        decision_id = %intent.decision_id,
                        market_id = %intent.market_id,
                        market_end_ts = ?intent.market_end_ts,
                        notional = %intent.notional,
                        blob_name = %published.blob_name,
                        blob_commit_elapsed_ms = published.blob_commit_elapsed_ms,
                        queue_send_elapsed_ms = ?published.queue_send_elapsed_ms,
                        pointer_only_preflight = published.pointer_only_preflight,
                        "execution intent published"
                    );
                    runtime
                        .record_event(
                            "execution_intent_published",
                            json!({
                                "decision_id": intent.decision_id,
                                "market_id": intent.market_id,
                                "condition_id": intent.condition_id,
                                "token_id": intent.token_id,
                                "candidate_version": intent.candidate_version,
                                "blob_name": published.blob_name,
                                "artifact_sha256": published.artifact_sha256,
                                "queue_handoff_sent": published.queue_handoff_sent,
                                "pointer_only_preflight": published.pointer_only_preflight,
                                "intent_created_wall_ts": intent.decision_ts,
                                "blob_commit_wall_ts": published.blob_commit_wall_ts,
                                "blob_commit_elapsed_ms": published.blob_commit_elapsed_ms,
                                "queue_send_wall_ts": published.queue_send_wall_ts,
                                "queue_send_elapsed_ms": published.queue_send_elapsed_ms,
                                "valid_until": intent.valid_until,
                                "order_submission_attempted": false,
                                "credential_free": true
                            }),
                            None,
                            None,
                        )
                        .await;
                }
                Err(reason) => {
                    warn!(
                        decision_id = %intent.decision_id,
                        market_id = %intent.market_id,
                        reason = %reason,
                        "execution intent publication failed"
                    );
                    runtime
                        .record_event(
                            "execution_intent_not_published",
                            json!({
                                "decision_id": intent.decision_id,
                                "market_id": intent.market_id,
                                "reason": reason,
                                "fail_closed": true,
                                "order_submission_attempted": false
                            }),
                            None,
                            None,
                        )
                        .await;
                }
            }
        });
        if wait_for_local_publication {
            if let Err(error) = publication.await {
                self.record_event(
                    "execution_intent_not_published",
                    json!({
                        "decision_id": join_decision_id,
                        "market_id": join_market_id,
                        "reason": format!("local publication task failed: {error}"),
                        "fail_closed": true,
                        "order_submission_attempted": false
                    }),
                    None,
                    None,
                )
                .await;
            }
        }
    }

    // The decision gate is the linearization point for every external intent
    // commit.  Tests inject a blocking closure here; production supplies the
    // real persistent publisher from maybe_publish_execution_intent above.
    async fn execute_live_clob_publication<T, F>(
        &self,
        clob_generation: u64,
        token_id: &TokenId,
        expected_book: &BookState,
        publish: F,
    ) -> Result<Option<T>, String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, String> + Send + 'static,
    {
        let decision_guard = self.inner.decision_gate.lock().await;
        let result = self
            .execute_live_clob_publication_while_gated(
                clob_generation,
                token_id,
                expected_book,
                publish,
            )
            .await;
        drop(decision_guard);
        result
    }

    async fn execute_live_clob_publication_while_gated<T, F>(
        &self,
        clob_generation: u64,
        token_id: &TokenId,
        expected_book: &BookState,
        publish: F,
    ) -> Result<Option<T>, String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, String> + Send + 'static,
    {
        let data = self.inner.data.read().await;
        let live = data.clob_generation == Some(clob_generation)
            && data
                .clob_lease
                .as_ref()
                .is_some_and(|lease| !lease.is_terminal())
            && data.books.get(token_id) == Some(expected_book);
        drop(data);
        if !live {
            return Ok(None);
        }
        let result = tokio::task::spawn_blocking(publish)
            .await
            .map_err(|error| format!("publisher task failed: {error}"))?;
        result.map(Some)
    }

    async fn push_decision_with_metadata(
        &self,
        decision: TradeDecision,
        metadata: Option<StrategyDecisionMetadata>,
    ) {
        self.push_decision_with_metadata_and_binding(decision, metadata, None)
            .await;
    }

    async fn push_decision_with_metadata_and_binding(
        &self,
        decision: TradeDecision,
        metadata: Option<StrategyDecisionMetadata>,
        binding: Option<DecisionBatchBinding>,
    ) {
        {
            let mut data = self.inner.data.write().await;
            data.decisions.push_back(decision.clone());
            truncate(&mut data.decisions, HISTORY_LIMIT);
        }
        self.record_pre_decision_book(&decision).await;
        let payload = decision_event_payload(&decision, metadata.as_ref(), binding.as_ref());
        self.record_event("decision", payload, None, None).await;
    }

    async fn accept_durable_decision(&self, decision: TradeDecision) {
        let mut data = self.inner.data.write().await;
        data.decisions.push_back(decision);
        truncate(&mut data.decisions, HISTORY_LIMIT);
    }

    async fn record_execution_report(&self, report: ExecutionReport, publish_fill: bool) {
        let quality_events = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut engine = self.inner.engine.lock().await;
            let events = engine.execution_quality.observe_execution_report(&report);
            engine.decision_generation = engine.decision_generation.wrapping_add(1);
            events
        };
        {
            let mut data = self.inner.data.write().await;
            data.execution_reports.push_back(report.clone());
            truncate(&mut data.execution_reports, HISTORY_LIMIT);
        }
        self.record_event("execution_report", &report, None, None)
            .await;
        if let Some(registration) = report.raw.get("execution_quality") {
            self.record_event("paper_order_queue_registration", registration, None, None)
                .await;
        }
        for event in quality_events {
            self.record_event(event.event_type, event.payload, None, None)
                .await;
        }
        self.push_market_chart_sample(&report.market_id).await;
        if publish_fill && report.status == "paper_filled_maker" {
            self.publish_only("paper_fill", &report).await;
        }
    }

    async fn accept_persisted_execution_report(&self, report: ExecutionReport, publish_fill: bool) {
        let quality_events = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut engine = self.inner.engine.lock().await;
            let events = engine.execution_quality.observe_execution_report(&report);
            engine.decision_generation = engine.decision_generation.wrapping_add(1);
            events
        };
        {
            let mut data = self.inner.data.write().await;
            data.execution_reports.push_back(report.clone());
            truncate(&mut data.execution_reports, HISTORY_LIMIT);
        }
        for event in quality_events {
            self.record_event(event.event_type, event.payload, None, None)
                .await;
        }
        self.push_market_chart_sample(&report.market_id).await;
        if publish_fill && report.status == "paper_filled_maker" {
            self.publish_only("paper_fill", &report).await;
        }
    }

    async fn push_market_chart_sample(&self, market_id: &MarketId) {
        let persistence = {
            let mut data = self.inner.data.write().await;
            let Some(market) = data.markets.get(market_id).cloned() else {
                return;
            };
            let point = chart_sample_from_data(&market, &data, Utc::now());
            let bucket_ms = point_bucket_ms(&point);
            let sample_count = {
                let samples = data.chart_samples.entry(market_id.clone()).or_default();
                samples.push_back(point.clone());
                truncate(samples, CHART_HISTORY_LIMIT);
                samples.len()
            };
            match bucket_ms {
                Some(bucket_ms)
                    if should_persist(
                        data.chart_last_persisted_ms.get(market_id).copied(),
                        bucket_ms,
                        self.inner.settings.azure.chart_persist_interval_ms as i64,
                    ) =>
                {
                    data.chart_last_persisted_ms
                        .insert(market_id.clone(), bucket_ms);
                    Some(ChartPersistenceSample::new(market, point, sample_count))
                }
                _ => None,
            }
        };
        if let Some(sample) = persistence {
            spawn_persist(self.inner.settings.clone(), sample);
        };
    }

    async fn capture_market_start_prices(&self, reference: &ReferencePrice) {
        self.retry_pending_market_start_events().await;
        if reference.stale || !reference.exact_resolution_source {
            return;
        }
        let grace = self.inner.settings.target.start_price_capture_grace_seconds;
        let mut updates = Vec::new();
        {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
            let captured_markets = data
                .market_start_evidence_durable
                .iter()
                .chain(data.pending_market_start_events.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for market in data.markets.values_mut() {
                if captured_markets.contains(&market.market_id) {
                    continue;
                }
                let seconds_after_start = reference
                    .source_ts
                    .signed_duration_since(market.start_ts)
                    .num_microseconds()
                    .map_or(-1.0, |micros| micros as f64 / 1_000_000.0);
                if seconds_after_start >= 0.0 && seconds_after_start <= grace {
                    let replaced_unverified_start_price = market
                        .start_price
                        .filter(|price| *price != reference.price)
                        .map(|price| price.to_string());
                    *market = market.clone().with_start_price(reference.price);
                    updates.push((
                        market.market_id.clone(),
                        json!({
                            "schema_version": 1,
                            "schema": "polyedge.market_start_price.v1",
                            "market_id": market.market_id,
                            "market_slug": market.market_slug,
                            "market_start_ts": market.start_ts,
                            "market_end_ts": market.end_ts,
                            "start_price": reference.price.to_string(),
                            "reference_source": reference.source,
                            "reference_source_ts": reference.source_ts,
                            "reference_exact_resolution_source": true,
                            "reference_stale": false,
                            "capture_method": "exact_reference_boundary",
                            "replaced_unverified_start_price": replaced_unverified_start_price
                        }),
                    ));
                }
            }
            for (market_id, _) in &updates {
                data.market_start_references
                    .insert(market_id.clone(), reference.clone());
            }
            for (market_id, update) in &updates {
                data.pending_market_start_events
                    .entry(market_id.clone())
                    .or_insert_with(|| RuntimeEvent {
                        event_type: "market_start_price".to_owned(),
                        ts: Utc::now(),
                        data: update.clone(),
                    });
            }
            if !updates.is_empty() {
                data.decision_generation = data.decision_generation.wrapping_add(1);
            }
        }
        self.retry_pending_market_start_events().await;
    }

    async fn retry_pending_market_start_events(&self) {
        let pending = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let data = self.inner.data.read().await;
            data.pending_market_start_events.clone()
        };
        for (market_id, event) in pending {
            if !self
                .record_required_runtime_events(vec![event.clone()], false)
                .await
            {
                continue;
            }
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
            if data.pending_market_start_events.get(&market_id) != Some(&event) {
                continue;
            }
            data.pending_market_start_events.remove(&market_id);
            data.market_start_evidence_durable.insert(market_id.clone());
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
    }

    async fn retry_pending_settlement(&self, market_id: &MarketId) -> PendingSettlementRetry {
        let decision_guard = self.inner.decision_gate.lock().await;
        let pending = {
            let engine = self.inner.engine.lock().await;
            let Some(pending) = engine.pending_settlements.get(market_id).cloned() else {
                return PendingSettlementRetry::NotPending;
            };
            pending
        };
        if !self
            .record_required_runtime_events(pending.events.clone(), false)
            .await
        {
            warn!(
                market_id = %market_id,
                settlement_journal_id = %pending.journal_id,
                "paper settlement retained for retry because durable persistence failed"
            );
            return PendingSettlementRetry::Retained;
        }
        let mut engine = self.inner.engine.lock().await;
        if engine
            .pending_settlements
            .get(market_id)
            .is_none_or(|current| current.journal_id != pending.journal_id)
        {
            warn!(
                market_id = %market_id,
                settlement_journal_id = %pending.journal_id,
                "paper settlement journal changed while durable evidence was persisted"
            );
            return PendingSettlementRetry::Retained;
        }
        engine.order_manager.clear_market(market_id);
        engine.execution.clear_market(market_id);
        engine.execution_quality.clear_market(market_id);
        engine.risk.clear_market(market_id);
        engine.pending_settlements.remove(market_id);
        engine.risk.open_order_count = engine.order_manager.open_order_count();
        engine.decision_generation = engine.decision_generation.wrapping_add(1);
        drop(engine);
        drop(decision_guard);
        let mut data = self.inner.data.write().await;
        if !data.settled_markets.contains(market_id) {
            data.settled_markets.push(market_id.clone());
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        PendingSettlementRetry::Committed
    }

    async fn retry_pending_decision_application(&self) -> PendingApplicationRetry {
        let decision_guard = self.inner.decision_gate.lock().await;
        let pending = {
            let engine = self.inner.engine.lock().await;
            let Some(pending) = engine.pending_decision_application.clone() else {
                return PendingApplicationRetry::NotPending;
            };
            pending
        };
        if !self
            .record_required_runtime_events(pending.events.clone(), false)
            .await
        {
            warn!(
                batch_id = %pending.batch_id,
                "paper decision application journal retained for retry because durable persistence failed"
            );
            return PendingApplicationRetry::Retained;
        }
        let mut engine = self.inner.engine.lock().await;
        if engine
            .pending_decision_application
            .as_ref()
            .is_none_or(|current| current.batch_id != pending.batch_id)
        {
            warn!(
                batch_id = %pending.batch_id,
                "paper decision application changed while durable evidence was persisted"
            );
            return PendingApplicationRetry::Retained;
        }
        engine.pending_decision_application = None;
        engine.decision_generation = engine.decision_generation.wrapping_add(1);
        drop(engine);
        drop(decision_guard);
        for report in pending.reports {
            self.accept_persisted_execution_report(report, false).await;
        }
        PendingApplicationRetry::Committed
    }

    async fn settle_finished_markets(&self, reference: &ReferencePrice) {
        let pending_ids = {
            let engine = self.inner.engine.lock().await;
            engine
                .pending_settlements
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut retained_pending = BTreeSet::new();
        for market_id in pending_ids {
            if self.retry_pending_settlement(&market_id).await == PendingSettlementRetry::Retained {
                retained_pending.insert(market_id);
            }
        }
        let markets = {
            let data = self.inner.data.read().await;
            data.markets.values().cloned().collect::<Vec<_>>()
        };
        for market in markets {
            if retained_pending.contains(&market.market_id) {
                continue;
            }
            if reference.stale || !reference.exact_resolution_source {
                continue;
            }
            let settlement_deadline = market.end_ts + chrono::Duration::seconds(15);
            if market.start_price.is_none()
                || reference.source_ts < market.end_ts
                || reference.source_ts > settlement_deadline
            {
                continue;
            }
            let start_reference = {
                let data = self.inner.data.read().await;
                if data.settled_markets.contains(&market.market_id) {
                    continue;
                }
                data.market_start_references.get(&market.market_id).cloned()
            };
            let Some(start_reference) = start_reference else {
                self.record_event(
                    "paper_settlement_rejected",
                    json!({
                        "market_id": market.market_id,
                        "reason": "exact non-stale start reference evidence is unavailable",
                        "state_cleared": false,
                        "research_only": true
                    }),
                    None,
                    None,
                )
                .await;
                continue;
            };
            if start_reference.stale
                || !start_reference.exact_resolution_source
                || market.start_price != Some(start_reference.price)
            {
                continue;
            }
            let start_price = market.start_price.unwrap_or(Decimal::ZERO);
            let winning_outcome = if reference.price >= start_price {
                "up"
            } else {
                "down"
            };
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut engine = self.inner.engine.lock().await;
            let mut risk_preview = engine.risk.clone();
            let cleared_position = risk_preview.clear_market(&market.market_id);
            let mut quality_preview = engine.execution_quality.clone();
            let missing_markouts = quality_preview.clear_market(&market.market_id);
            let journal_id = paper_settlement_journal_id(&market, &start_reference, reference);
            let mut unbound_events = missing_markouts
                .into_iter()
                .map(|event| (event.event_type.to_owned(), event.payload))
                .collect::<Vec<_>>();
            unbound_events.push((
                "paper_settlement".to_owned(),
                json!({
                    "market_id": market.market_id,
                    "market_slug": market.market_slug,
                    "start_ts": market.start_ts,
                    "end_ts": market.end_ts,
                    "start_price": start_price.to_string(),
                    "start_reference_source": start_reference.source,
                    "start_reference_source_ts": start_reference.source_ts,
                    "start_reference_exact_resolution_source": true,
                    "start_reference_stale": false,
                    "final_price": reference.price.to_string(),
                    "winning_outcome": winning_outcome,
                    "final_reference_source": reference.source,
                    "final_reference_source_ts": reference.source_ts,
                    "final_reference_exact_resolution_source": true,
                    "final_reference_stale": false,
                    "reference_source": reference.source,
                    "reference_source_ts": reference.source_ts,
                    "cleared_position": cleared_position.to_string()
                }),
            ));
            let events = required_runtime_events(
                finalize_settlement_journal(&journal_id, unbound_events),
                Utc::now(),
            );
            engine.pending_settlements.insert(
                market.market_id.clone(),
                PendingPaperSettlement { journal_id, events },
            );
            engine.decision_generation = engine.decision_generation.wrapping_add(1);
            drop(engine);
            drop(_decision_guard);
            let _ = self.retry_pending_settlement(&market.market_id).await;
        }
    }

    async fn cancel_active_markets(&self, reason: String) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let markets = {
            let data = self.inner.data.read().await;
            active_markets(&data)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        };
        for market in markets {
            let decision = TradeDecision {
                action: DecisionAction::CancelAll,
                market_id: market.market_id.clone(),
                condition_id: Some(market.condition_id.clone()),
                token_id: None,
                outcome: None,
                side: None,
                price: None,
                size: None,
                quote_amount: None,
                order_kind: None,
                reason: reason.clone(),
                ttl_ms: None,
                expected_edge: None,
                post_only: false,
                tick_size: None,
                neg_risk: false,
            };
            self.push_decision(decision.clone()).await;
            let books = {
                let data = self.inner.data.read().await;
                data.books.clone()
            };
            for report in self.execute_paper_decision(&decision, &books).await {
                self.record_execution_report(report, false).await;
            }
        }
    }

    async fn record_event<P>(
        &self,
        event_type: &str,
        payload: P,
        publish_type: Option<&str>,
        publish_payload: Option<Value>,
    ) where
        P: Serialize,
    {
        let _ = self
            .record_event_inner(event_type, payload, publish_type, publish_payload, false)
            .await;
    }

    async fn record_required_events(&self, entries: Vec<(String, Value)>) -> bool {
        self.record_required_runtime_events(required_runtime_events(entries, Utc::now()), false)
            .await
    }

    async fn record_required_events_during_shutdown(&self, entries: Vec<(String, Value)>) -> bool {
        self.record_required_runtime_events(required_runtime_events(entries, Utc::now()), true)
            .await
    }

    async fn record_required_runtime_events(
        &self,
        events: Vec<RuntimeEvent>,
        allow_during_shutdown: bool,
    ) -> bool {
        if events.is_empty() {
            return true;
        }
        match self
            .persist_required_batch(events.clone(), allow_during_shutdown)
            .await
        {
            Ok(()) => {
                if self
                    .inner
                    .recorder_metrics
                    .mark_durable_batch_recovered(&events)
                {
                    info!(
                        event_count = events.len(),
                        "required runtime evidence recovered after recorder retry"
                    );
                }
                self.accept_durable_events(&events).await;
                true
            }
            Err(error) => {
                self.inner
                    .recorder_metrics
                    .mark_durable_batch_unrecovered(&events);
                let mut state = self.inner.data.write().await;
                *state
                    .drop_counts
                    .entry("required_recorder_write_error".to_owned())
                    .or_insert(0) += events.len();
                warn!(
                    event_count = events.len(),
                    error, "required runtime evidence exhausted durable recorder retries"
                );
                false
            }
        }
    }

    async fn persist_required_batch(
        &self,
        events: Vec<RuntimeEvent>,
        allow_during_shutdown: bool,
    ) -> Result<(), String> {
        let event_count = events.len();
        let (ack_tx, ack_rx) = oneshot::channel();
        let permit = if allow_during_shutdown {
            None
        } else {
            Some(self.acquire_recorder_admission().await?)
        };
        // Keep sequence allocation and FIFO queue admission inseparable: a
        // later caller must not send N+1 before this caller sends N.
        let queue_failed = {
            let _enqueue_gate = self
                .inner
                .recorder_enqueue_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !allow_during_shutdown && self.inner.shutting_down.load(Ordering::SeqCst) {
                return Err("runtime recorder is shutting down".to_owned());
            }
            let events = events
                .into_iter()
                .map(|event| self.inner.recorder_metrics.bind(event))
                .collect::<Result<Vec<_>, _>>()?;
            let last_sequence = events
                .last()
                .map(RecordedRuntimeEvent::recorder_sequence)
                .unwrap_or_default();
            self.inner
                .recorder_metrics
                .queued
                .fetch_add(event_count, Ordering::Relaxed);
            self.inner
                .recorder_metrics
                .enqueued_total
                .fetch_add(event_count as u64, Ordering::Relaxed);
            let request = match permit {
                Some(permit) => RecorderRequest::admitted_durable(events, ack_tx, permit),
                None => RecorderRequest::durable(events, ack_tx),
            };
            match self.inner.recorder_tx.send(request) {
                Ok(()) => {
                    self.inner.recorder_metrics.mark_enqueued(last_sequence);
                    false
                }
                Err(error) => {
                    if !self
                        .inner
                        .recorder_metrics
                        .rollback_bound_tail(&error.0.events)
                    {
                        warn!("runtime recorder rejected durable tail could not be rolled back");
                    }
                    true
                }
            }
        };
        if queue_failed {
            saturating_sub_atomic(&self.inner.recorder_metrics.queued, event_count);
            self.inner
                .recorder_metrics
                .failed_total
                .fetch_add(event_count as u64, Ordering::Relaxed);
            return Err("runtime recorder worker is unavailable".to_owned());
        }
        ack_rx
            .await
            .map_err(|_| "runtime recorder worker dropped durable acknowledgment".to_owned())?
    }

    async fn accept_durable_events(&self, events: &[RuntimeEvent]) {
        {
            let mut state = self.inner.data.write().await;
            state.runtime_events += events.len();
            for event in events {
                state.recent_events.push_back(event.clone());
            }
            truncate(&mut state.recent_events, RECENT_LIMIT);
        }
        for event in events {
            if let Err(error) = self.inner.broadcaster.send(event.clone()) {
                debug!("runtime event had no subscribers: {error}");
            }
        }
    }

    async fn record_event_inner<P>(
        &self,
        event_type: &str,
        payload: P,
        publish_type: Option<&str>,
        publish_payload: Option<Value>,
        force_persistence: bool,
    ) -> bool
    where
        P: Serialize,
    {
        let data = serde_json::to_value(payload).unwrap_or(Value::Null);
        let event = RuntimeEvent {
            event_type: event_type.to_owned(),
            ts: Utc::now(),
            data: data.clone(),
        };
        let persist = self
            .inner
            .persistence_filter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .should_persist(
                &self.inner.settings,
                event_type,
                &data,
                event.ts,
                force_persistence,
            );
        let (recorder_queue_failed, recorder_queue_incremented) = if persist {
            match self.acquire_recorder_admission().await {
                Ok(permit) => {
                    let _enqueue_gate = self
                        .inner
                        .recorder_enqueue_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if self.inner.shutting_down.load(Ordering::SeqCst) {
                        (true, false)
                    } else {
                        match self.inner.recorder_metrics.bind(event.clone()) {
                            Ok(event) => {
                                let sequence = event.recorder_sequence();
                                self.inner
                                    .recorder_metrics
                                    .queued
                                    .fetch_add(1, Ordering::Relaxed);
                                self.inner
                                    .recorder_metrics
                                    .enqueued_total
                                    .fetch_add(1, Ordering::Relaxed);
                                let queue_failed = match self
                                    .inner
                                    .recorder_tx
                                    .send(RecorderRequest::admitted_best_effort(event, permit))
                                {
                                    Ok(()) => {
                                        self.inner.recorder_metrics.mark_enqueued(sequence);
                                        false
                                    }
                                    Err(error) => {
                                        if !self
                                            .inner
                                            .recorder_metrics
                                            .rollback_bound_tail(&error.0.events)
                                        {
                                            warn!("runtime recorder rejected best-effort tail could not be rolled back");
                                        }
                                        true
                                    }
                                };
                                (queue_failed, true)
                            }
                            Err(error) => {
                                warn!("runtime recorder sequence allocation failed: {error}");
                                (true, false)
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!("runtime recorder admission failed: {error}");
                    (true, false)
                }
            }
        } else {
            self.inner
                .recorder_metrics
                .filtered_total
                .fetch_add(1, Ordering::Relaxed);
            (false, false)
        };
        if recorder_queue_failed {
            if recorder_queue_incremented {
                saturating_sub_atomic(&self.inner.recorder_metrics.queued, 1);
            }
            self.inner
                .recorder_metrics
                .failed_total
                .fetch_add(1, Ordering::Relaxed);
        }
        {
            let mut state = self.inner.data.write().await;
            state.runtime_events += 1;
            if recorder_queue_failed {
                *state
                    .drop_counts
                    .entry("recorder_queue_send_error".to_owned())
                    .or_insert(0) += 1;
                warn!("runtime recorder queue is unavailable; event was not persisted");
            }
            state.recent_events.push_back(event.clone());
            truncate(&mut state.recent_events, RECENT_LIMIT);
        }
        let publish_event = RuntimeEvent {
            event_type: publish_type.unwrap_or(event_type).to_owned(),
            ts: event.ts,
            data: publish_payload.unwrap_or(data),
        };
        if let Err(error) = self.inner.broadcaster.send(publish_event) {
            debug!("runtime event had no subscribers: {error}");
        }
        persist && !recorder_queue_failed
    }

    async fn record_pre_decision_book(&self, decision: &TradeDecision) {
        if !self.inner.settings.deploy.runtime_role.is_shadow()
            || !self.inner.settings.azure.compact_shadow_recording
        {
            return;
        }
        let Some(token_id) = decision.token_id.as_ref() else {
            return;
        };
        let book = {
            let data = self.inner.data.read().await;
            data.books.get(token_id).map(compact_recorded_book)
        };
        if let Some(book) = book {
            self.force_record_book(&book).await;
        }
    }

    async fn force_record_book(&self, book: &BookState) {
        if self.inner.settings.deploy.runtime_role.is_shadow()
            && self.inner.settings.azure.compact_shadow_recording
        {
            self.record_event_inner("book", compact_recorded_book(book), None, None, true)
                .await;
        }
    }

    async fn publish_only<P>(&self, event_type: &str, payload: P)
    where
        P: Serialize,
    {
        let event = RuntimeEvent {
            event_type: event_type.to_owned(),
            ts: Utc::now(),
            data: serde_json::to_value(payload).unwrap_or(Value::Null),
        };
        let _ = self.inner.broadcaster.send(event);
    }

    async fn set_feed_status(&self, name: &str, status: &str, message: Option<String>) {
        self.set_feed_status_at(name, status, message, Utc::now())
            .await;
    }

    async fn set_feed_status_at(
        &self,
        name: &str,
        status: &str,
        message: Option<String>,
        observed_at: DateTime<Utc>,
    ) {
        let mut data = self.inner.data.write().await;
        Self::set_feed_status_at_locked(&mut data, name, status, message, observed_at);
    }

    fn set_feed_status_at_locked(
        data: &mut RuntimeData,
        name: &str,
        status: &str,
        message: Option<String>,
        observed_at: DateTime<Utc>,
    ) {
        if data.feed_status.get(name).and_then(|value| {
            value["updated_at"]
                .as_str()
                .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
                .map(|timestamp| timestamp.with_timezone(&Utc) > observed_at)
        }) == Some(true)
        {
            return;
        }
        data.feed_status.insert(
            name.to_owned(),
            json!({
                "status": status,
                "message": message,
                "updated_at": observed_at
            }),
        );
    }

    async fn feed_error(&self, source: FeedName, message: String) {
        self.feed_error_at(source, message, Utc::now()).await;
    }

    fn accept_clob_sequence(
        &self,
        data: &mut RuntimeData,
        sequence: Option<(u64, u64)>,
        observed_at: DateTime<Utc>,
    ) -> bool {
        let Some((generation, sequence)) = sequence else {
            return true;
        };
        if data.clob_generation != Some(generation)
            || data
                .clob_lease
                .as_ref()
                .is_none_or(ClobGenerationLease::is_terminal)
            || sequence <= data.clob_last_sequence
        {
            return false;
        }
        data.clob_last_sequence = sequence;
        Self::set_feed_status_at_locked(data, "PolymarketClobMarket", "ok", None, observed_at);
        true
    }

    async fn begin_clob_generation(
        &self,
        generation: u64,
        token_ids: &[TokenId],
    ) -> ClobGenerationLease {
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut data = self.inner.data.write().await;
        let replaced_generation = Self::revoke_clob_generation_locked(&mut data);
        let lease = ClobGenerationLease::new();
        data.clob_pending_generation = Some(generation);
        data.clob_pending_tokens = token_ids.iter().cloned().collect();
        data.clob_lease = Some(lease.clone());
        data.decision_generation = data.decision_generation.wrapping_add(1);
        drop(data);
        if let Some(replaced_generation) = replaced_generation {
            let _ = self
                .record_required_events(vec![(
                    "clob_resync_aborted".to_owned(),
                    json!({
                        "generation": replaced_generation,
                        "reason": "replaced by a newer CLOB generation",
                        "fail_closed": true,
                        "ready": false
                    }),
                )])
                .await;
        }
        lease
    }

    // Must be called while decision_gate and the RuntimeData write lock are
    // held.  It is deliberately idempotent: exactly one caller wins the
    // terminal transition and is therefore responsible for its abort audit.
    fn revoke_clob_generation_locked(data: &mut RuntimeData) -> Option<u64> {
        let generation = data.clob_generation.or(data.clob_pending_generation)?;
        if let Some(lease) = &data.clob_lease {
            lease.terminate();
        }
        data.clob_generation = None;
        data.clob_pending_generation = None;
        data.clob_tokens.clear();
        data.clob_pending_tokens.clear();
        data.clob_last_sequence = 0;
        data.clob_terminal_generation = data.clob_terminal_generation.max(generation);
        data.decision_generation = data.decision_generation.wrapping_add(1);
        Some(generation)
    }

    async fn abort_clob_generation_while_gated(&self, generation: u64, reason: &str) -> bool {
        let revoked = {
            let mut data = self.inner.data.write().await;
            (data.clob_generation == Some(generation)
                || data.clob_pending_generation == Some(generation))
            .then(|| Self::revoke_clob_generation_locked(&mut data))
            .flatten()
        };
        let Some(generation) = revoked else {
            return false;
        };
        if !self
            .record_required_events(vec![(
                "clob_resync_aborted".to_owned(),
                json!({
                    "generation": generation,
                    "reason": reason,
                    "fail_closed": true,
                    "ready": false
                }),
            )])
            .await
        {
            warn!(
                generation,
                "CLOB resync abort audit was not durable; canonical feed error remains required"
            );
        }
        true
    }

    async fn handle_clob_resync_barrier(&self, barrier: ClobResyncBarrier) {
        let _decision_guard = self.inner.decision_gate.lock().await;
        let (expected, pending_generation, terminal_generation) = {
            let data = self.inner.data.read().await;
            (
                data.clob_pending_tokens.clone(),
                data.clob_pending_generation,
                data.clob_terminal_generation,
            )
        };
        let anchored = barrier.token_ids().cloned().collect::<BTreeSet<_>>();
        let valid = !barrier.lease.is_terminal()
            && pending_generation == Some(barrier.generation)
            && terminal_generation < barrier.generation
            && !expected.is_empty()
            && !anchored.is_empty()
            && anchored.is_subset(&expected)
            && expected.len() == barrier.token_count
            && clob_token_set_digest(&expected) == barrier.token_set_digest;
        if !valid {
            self.abort_clob_generation_while_gated(
                barrier.generation,
                "barrier token set no longer matches the pending generation",
            )
            .await;
            let _ = barrier.ready_ack.send(Err(
                "barrier token set no longer matches the pending generation".to_owned(),
            ));
            return;
        }
        let pre_ready_evidence = barrier
            .pre_ready_events
            .iter()
            .map(|event| {
                (
                    "raw_market_event".to_owned(),
                    serde_json::to_value(event).unwrap_or(Value::Null),
                )
            })
            .collect::<Vec<_>>();
        if !self.record_required_events(pre_ready_evidence).await {
            self.abort_clob_generation_while_gated(
                barrier.generation,
                "durable pre-ready market evidence failed",
            )
            .await;
            let _ = barrier
                .ready_ack
                .send(Err("durable pre-ready market evidence failed".to_owned()));
            return;
        }
        let authorization_event = json!({
            "generation": barrier.generation,
            "sequence": barrier.sequence,
            "token_count": barrier.token_count,
            "token_set_digest": barrier.token_set_digest,
            "authorization": "single_transport_snapshot_barrier",
            "ready": false,
            "authorized": true
        });
        if !self
            .record_required_events(vec![(
                "clob_resync_authorized".to_owned(),
                authorization_event,
            )])
            .await
        {
            self.abort_clob_generation_while_gated(
                barrier.generation,
                "durable readiness authorization failed",
            )
            .await;
            let _ = barrier
                .ready_ack
                .send(Err("durable readiness authorization failed".to_owned()));
            return;
        }
        {
            let mut data = self.inner.data.write().await;
            // The producer is blocked on ready_ack.  This transition therefore
            // follows the only possible final drain boundary with no await.
            if barrier.lease.is_terminal()
                || data.clob_pending_generation != Some(barrier.generation)
                || data.clob_terminal_generation >= barrier.generation
            {
                drop(data);
                self.abort_clob_generation_while_gated(
                    barrier.generation,
                    "generation terminated before readiness install",
                )
                .await;
                let _ = barrier.ready_ack.send(Err(
                    "generation terminated before readiness install".to_owned(),
                ));
                return;
            }
            data.books.retain(|token, _| !expected.contains(token));
            for book in barrier.anchors {
                data.books.insert(book.token_id.clone(), book);
            }
            data.clob_generation = Some(barrier.generation);
            data.clob_pending_generation = None;
            data.clob_pending_tokens.clear();
            data.clob_lease = Some(barrier.lease.clone());
            data.clob_last_sequence = barrier.sequence;
            data.clob_tokens = expected;
            data.decision_generation = data.decision_generation.wrapping_add(1);
            data.feed_status.insert(
                "PolymarketClobMarket".to_owned(),
                json!({"status": "ok", "message": Value::Null, "updated_at": Utc::now()}),
            );
        }
        if barrier.ready_ack.send(Ok(())).is_err() {
            self.abort_clob_generation_while_gated(
                barrier.generation,
                "CLOB readiness receiver dropped",
            )
            .await;
        }
    }

    async fn terminate_clob_generation(&self, generation: u64, reason: &str) {
        self.terminate_clob_generation_inner(generation, reason, false)
            .await;
    }

    async fn terminate_clob_generation_inner(
        &self,
        generation: u64,
        reason: &str,
        allow_during_shutdown: bool,
    ) {
        let _decision_guard = self.inner.decision_gate.lock().await;
        let revoked = {
            let mut data = self.inner.data.write().await;
            (data.clob_generation == Some(generation)
                || data.clob_pending_generation == Some(generation))
            .then(|| Self::revoke_clob_generation_locked(&mut data))
            .flatten()
        };
        if let Some(generation) = revoked {
            let event = json!({
                "generation": generation,
                "reason": reason,
                "fail_closed": true,
                "ready": false
            });
            let recorded = if allow_during_shutdown {
                self.record_required_events_during_shutdown(vec![(
                    "clob_resync_aborted".to_owned(),
                    event,
                )])
                .await
            } else {
                self.record_required_events(vec![("clob_resync_aborted".to_owned(), event)])
                    .await
            };
            if !recorded {
                warn!(generation, "CLOB resync abort audit was not durable; canonical feed error remains required");
            }
        }
    }

    async fn terminate_all_clob_generations(&self, reason: &str, allow_during_shutdown: bool) {
        let generation = {
            let data = self.inner.data.read().await;
            data.clob_generation.or(data.clob_pending_generation)
        };
        if let Some(generation) = generation {
            self.terminate_clob_generation_inner(generation, reason, allow_during_shutdown)
                .await;
        }
    }

    async fn mark_market_feed_connecting(&self) {
        let now = Utc::now();
        let fresh = {
            let data = self.inner.data.read().await;
            fresh_market_feed_ok(data.feed_status.get("PolymarketClobMarket"), now)
        };
        if !fresh {
            self.set_feed_status_at("PolymarketClobMarket", "connecting", None, now)
                .await;
        }
    }

    async fn feed_error_at(&self, source: FeedName, message: String, observed_at: DateTime<Utc>) {
        let source_text = format!("{source:?}");
        self.set_feed_status_at(&source_text, "error", Some(message.clone()), observed_at)
            .await;
        self.record_event(
            "feed_error",
            json!({
                "feed": source_text,
                "error": message
            }),
            None,
            None,
        )
        .await;
    }

    async fn record_feed_disconnect(&self, sources: &[FeedName], message: &str) {
        for source in sources {
            self.feed_error(source.clone(), message.to_owned()).await;
        }
    }

    async fn market_token_ids(&self) -> Vec<TokenId> {
        let data = self.inner.data.read().await;
        data.markets
            .values()
            .flat_map(|market| [market.up_token_id.clone(), market.down_token_id.clone()])
            .collect()
    }
}

fn clob_token_set_digest(tokens: &BTreeSet<TokenId>) -> String {
    let mut hasher = 0xcbf2_9ce4_8422_2325_u64;
    for token in tokens {
        for byte in token.as_ref().bytes().chain(std::iter::once(0)) {
            hasher ^= u64::from(byte);
            hasher = hasher.wrapping_mul(0x100_0000_01b3);
        }
    }
    format!("{hasher:016x}")
}

fn rtds_source_settings(settings: &RuntimeSettings, source: &FeedName) -> RuntimeSettings {
    let mut scoped = settings.clone();
    scoped.target.enable_polymarket_rtds_chainlink =
        matches!(source, FeedName::PolymarketRtdsChainlink);
    scoped.target.enable_polymarket_rtds_binance =
        matches!(source, FeedName::PolymarketRtdsBinance);
    scoped
}

fn spawn_recorder_worker(
    recorder: Arc<StdMutex<RuntimeRecorder>>,
    receiver: std_mpsc::Receiver<RecorderRequest>,
    metrics: Arc<RecorderMetrics>,
) {
    if let Err(error) = std::thread::Builder::new()
        .name("polyedge-recorder".to_owned())
        .spawn(move || {
            let mut last_flush = Instant::now();
            let mut deferred_request = None;
            let mut pending_best_effort: Option<Vec<RecorderRequest>> = None;
            let mut durability = RecorderDurabilityState::default();
            loop {
                if let Some(requests) = pending_best_effort.as_ref() {
                    std::thread::sleep(RECORDER_RETRY_DELAY);
                    let event_count = requests
                        .iter()
                        .map(|request| request.logical_event_count)
                        .sum::<usize>();
                    let result = match recorder.lock() {
                        Ok(mut recorder) => recorder.retry_pending(),
                        Err(error) => Err(format!("runtime recorder lock poisoned: {error}")),
                    };
                    update_recorder_flush_health(&metrics, &result);
                    match &result {
                        Ok(()) => {
                            saturating_sub_atomic(&metrics.queued, event_count);
                            let last_sequence = requests
                                .last()
                                .and_then(|request| request.events.last())
                                .map(RecordedRuntimeEvent::recorder_sequence)
                                .unwrap_or_default();
                            metrics.mark_persisted(event_count, last_sequence);
                            pending_best_effort = None;
                        }
                        Err(error) => {
                            metrics
                                .failed_total
                                .fetch_add(event_count as u64, Ordering::Relaxed);
                            warn!("runtime recorder retry failed: {error}");
                        }
                    }
                    continue;
                }
                let requests = loop {
                    let request = match deferred_request.take() {
                        Some(request) => request,
                        None => match receiver.recv_timeout(RECORDER_FLUSH_INTERVAL) {
                            Ok(request) => request,
                            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                                flush_or_resume_runtime_recorder(
                                    &recorder,
                                    &metrics,
                                    &mut durability,
                                );
                                last_flush = Instant::now();
                                continue;
                            }
                            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                                flush_or_resume_runtime_recorder(
                                    &recorder,
                                    &metrics,
                                    &mut durability,
                                );
                                return;
                            }
                        },
                    };
                    if let Some(batch_key) = request.durable_batch_key.as_deref() {
                        let event_count = request.logical_event_count;
                        metrics.batches_total.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .last_batch_size
                            .store(event_count, Ordering::Relaxed);
                        let mut result = Err("durable recorder retry was not attempted".to_owned());
                        for attempt in 1..=REQUIRED_RECORDER_ATTEMPTS {
                            result = persist_durable_recorder_request(
                                &recorder,
                                &metrics,
                                &mut durability,
                                batch_key,
                                &request.events,
                            );
                            if result.is_ok() {
                                break;
                            }
                            metrics
                                .failed_total
                                .fetch_add(event_count as u64, Ordering::Relaxed);
                            if attempt < REQUIRED_RECORDER_ATTEMPTS {
                                std::thread::sleep(RECORDER_RETRY_DELAY);
                            }
                        }
                        saturating_sub_atomic(&metrics.queued, event_count);
                        match &result {
                            Ok(()) => {
                                let last_sequence = request
                                    .events
                                    .last()
                                    .map(RecordedRuntimeEvent::recorder_sequence)
                                    .unwrap_or_default();
                                metrics.mark_persisted(event_count, last_sequence);
                            }
                            Err(error) => {
                                warn!("runtime recorder durable batch failed: {error}");
                            }
                        }
                        if let Some(ack) = request.durable_ack {
                            let _ = ack.send(result);
                        }
                        continue;
                    }
                    if durability.pending_batch_key.is_some()
                        && resume_pending_durable(&recorder, &metrics, &mut durability).is_err()
                    {
                        deferred_request = Some(request);
                        std::thread::sleep(RECORDER_RETRY_DELAY);
                        continue;
                    }
                    let mut requests = vec![request];
                    let mut event_count = requests[0].logical_event_count;
                    while event_count < RECORDER_BATCH_LIMIT {
                        match receiver.try_recv() {
                            Ok(request) => {
                                if request.durable_batch_key.is_some() {
                                    deferred_request = Some(request);
                                    break;
                                }
                                event_count += request.logical_event_count;
                                requests.push(request);
                            }
                            Err(std_mpsc::TryRecvError::Empty) => break,
                            Err(std_mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                    break requests;
                };
                let event_count = requests
                    .iter()
                    .map(|request| request.logical_event_count)
                    .sum::<usize>();
                metrics.batches_total.fetch_add(1, Ordering::Relaxed);
                metrics
                    .last_batch_size
                    .store(event_count, Ordering::Relaxed);
                let events = requests
                    .iter()
                    .flat_map(|request| request.events.iter().cloned())
                    .collect::<Vec<_>>();
                let result = match recorder.lock() {
                    Ok(mut recorder) => recorder.record_recorded_batch(&events),
                    Err(error) => Err(format!("runtime recorder lock poisoned: {error}")),
                };
                // `record_batch` can attempt a previously frozen Azure block.
                // Keep current health failed while that exact block remains in
                // the recorder's immutable retry queue, even though these
                // best-effort requests do not wait for a durable ack.
                update_recorder_flush_health(&metrics, &result);
                debug_assert!(requests.iter().all(|request| request.durable_ack.is_none()));
                match &result {
                    Ok(()) => {
                        saturating_sub_atomic(&metrics.queued, event_count);
                        let last_sequence = events
                            .last()
                            .map(RecordedRuntimeEvent::recorder_sequence)
                            .unwrap_or_default();
                        metrics.mark_persisted(event_count, last_sequence);
                    }
                    Err(error) => {
                        metrics
                            .failed_total
                            .fetch_add(event_count as u64, Ordering::Relaxed);
                        warn!("runtime recorder failed: {error}");
                        pending_best_effort = Some(requests);
                    }
                }
                if result.is_ok() && last_flush.elapsed() >= RECORDER_FLUSH_INTERVAL {
                    flush_or_resume_runtime_recorder(&recorder, &metrics, &mut durability);
                    last_flush = Instant::now();
                }
            }
        })
    {
        warn!("failed to start runtime recorder worker: {error}");
    }
}

fn persist_durable_recorder_request(
    recorder: &Arc<StdMutex<RuntimeRecorder>>,
    metrics: &Arc<RecorderMetrics>,
    durability: &mut RecorderDurabilityState,
    batch_key: &str,
    events: &[RecordedRuntimeEvent],
) -> Result<(), String> {
    if durability.completed_batch_keys.contains(batch_key) {
        return Ok(());
    }
    if durability.pending_batch_key.is_some() {
        resume_pending_durable(recorder, metrics, durability)?;
        if durability.completed_batch_keys.contains(batch_key) {
            return Ok(());
        }
    }

    // A failed best-effort flush must recover before this journal is staged.
    // Healthy buffered bytes can share the journal's atomic flush: the durable
    // acknowledgment still waits for every byte in that flush to persist.
    if metrics.flush_unrecovered.load(Ordering::Acquire) {
        recorder_flush_result(recorder, metrics, false)?;
    }
    durability.pending_batch_key = Some(batch_key.to_owned());
    let result = match recorder.lock() {
        Ok(mut recorder) => match recorder.record_recorded_batch(events) {
            Ok(()) => recorder.flush(),
            Err(error) => Err(error),
        },
        Err(error) => Err(format!("runtime recorder lock poisoned: {error}")),
    };
    update_recorder_flush_health(metrics, &result);
    match result {
        Ok(()) => {
            durability.pending_batch_key = None;
            durability.remember_completed(batch_key.to_owned());
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn resume_pending_durable(
    recorder: &Arc<StdMutex<RuntimeRecorder>>,
    metrics: &Arc<RecorderMetrics>,
    durability: &mut RecorderDurabilityState,
) -> Result<(), String> {
    let Some(batch_key) = durability.pending_batch_key.clone() else {
        return Ok(());
    };
    recorder_flush_result(recorder, metrics, true)?;
    durability.pending_batch_key = None;
    durability.remember_completed(batch_key);
    Ok(())
}

fn recorder_flush_result(
    recorder: &Arc<StdMutex<RuntimeRecorder>>,
    metrics: &Arc<RecorderMetrics>,
    pending_only: bool,
) -> Result<(), String> {
    let result = match recorder.lock() {
        Ok(mut recorder) => {
            if pending_only {
                recorder.retry_pending()
            } else {
                recorder.flush()
            }
        }
        Err(error) => Err(format!(
            "runtime recorder lock poisoned during flush: {error}"
        )),
    };
    update_recorder_flush_health(metrics, &result);
    result
}

fn update_recorder_flush_health(metrics: &Arc<RecorderMetrics>, result: &Result<(), String>) {
    if result.is_err() {
        metrics.flush_failed_total.fetch_add(1, Ordering::Relaxed);
        metrics.flush_unrecovered.store(true, Ordering::Relaxed);
    } else if metrics.flush_unrecovered.swap(false, Ordering::Relaxed) {
        metrics
            .flush_recovered_total
            .fetch_add(1, Ordering::Relaxed);
        info!("runtime recorder flush recovered after prior failure");
    }
}

fn flush_or_resume_runtime_recorder(
    recorder: &Arc<StdMutex<RuntimeRecorder>>,
    metrics: &Arc<RecorderMetrics>,
    durability: &mut RecorderDurabilityState,
) {
    if durability.pending_batch_key.is_some() {
        if let Err(error) = resume_pending_durable(recorder, metrics, durability) {
            warn!("runtime recorder pending durable flush failed: {error}");
        }
    } else {
        flush_runtime_recorder(recorder, metrics);
    }
}

fn flush_runtime_recorder(
    recorder: &Arc<StdMutex<RuntimeRecorder>>,
    metrics: &Arc<RecorderMetrics>,
) {
    if let Err(error) = recorder_flush_result(recorder, metrics, false) {
        warn!("runtime recorder flush failed: {error}");
    }
}

fn qset_v4_image_digest() -> Result<String, String> {
    let image = std::env::var("POLYEDGE_QSET_V4_WRITER_IMAGE")
        .map_err(|_| "POLYEDGE_QSET_V4_WRITER_IMAGE is required for retirement".to_owned())?;
    qset_v4_image_digest_from(&image)
        .ok_or_else(|| "POLYEDGE_QSET_V4_WRITER_IMAGE must be pinned by SHA-256 digest".to_owned())
}

fn qset_v4_image_digest_from(image: &str) -> Option<String> {
    let (_, digest) = image.rsplit_once("@sha256:")?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| format!("sha256:{digest}"))
}

fn qset_v4_source_revision() -> Result<String, String> {
    let configured = std::env::var("POLYEDGE_QSET_V4_WRITER_GIT_SHA")
        .map_err(|_| "POLYEDGE_QSET_V4_WRITER_GIT_SHA is required for retirement".to_owned())?;
    qset_v4_source_revision_from(&configured, embedded_git_sha())
}

fn qset_v4_source_revision_from(
    configured: &str,
    embedded: Option<&str>,
) -> Result<String, String> {
    if !polyedge_config::is_full_git_sha(configured) {
        return Err("POLYEDGE_QSET_V4_WRITER_GIT_SHA must be an exact lowercase commit".to_owned());
    }
    if embedded.is_some_and(|embedded| embedded != configured) {
        return Err("qset-v4 frozen source revision does not match the running binary".to_owned());
    }
    Ok(configured.to_owned())
}

fn qset_v5_image_digest() -> Result<String, String> {
    let image = std::env::var("POLYEDGE_QSET_V5_WRITER_IMAGE")
        .map_err(|_| "POLYEDGE_QSET_V5_WRITER_IMAGE is required for retirement".to_owned())?;
    qset_v5_image_digest_from(&image)
        .ok_or_else(|| "POLYEDGE_QSET_V5_WRITER_IMAGE must be pinned by SHA-256 digest".to_owned())
}

fn qset_v5_image_digest_from(image: &str) -> Option<String> {
    let (_, digest) = image.rsplit_once("@sha256:")?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| format!("sha256:{digest}"))
}

fn qset_v5_source_revision() -> Result<String, String> {
    let configured = std::env::var("POLYEDGE_QSET_V5_WRITER_GIT_SHA")
        .map_err(|_| "POLYEDGE_QSET_V5_WRITER_GIT_SHA is required for retirement".to_owned())?;
    qset_v5_source_revision_from(&configured, embedded_git_sha())
}

fn qset_v5_source_revision_from(
    configured: &str,
    embedded: Option<&str>,
) -> Result<String, String> {
    if !polyedge_config::is_full_git_sha(configured) {
        return Err("POLYEDGE_QSET_V5_WRITER_GIT_SHA must be an exact lowercase commit".to_owned());
    }
    if embedded.is_some_and(|embedded| embedded != configured) {
        return Err("qset-v5 frozen source revision does not match the running binary".to_owned());
    }
    Ok(configured.to_owned())
}

fn qset_v6_image_digest() -> Result<String, String> {
    let image = std::env::var("POLYEDGE_QSET_V6_WRITER_IMAGE")
        .map_err(|_| "POLYEDGE_QSET_V6_WRITER_IMAGE is required for retirement".to_owned())?;
    qset_v6_image_digest_from(&image)
        .ok_or_else(|| "POLYEDGE_QSET_V6_WRITER_IMAGE must be pinned by SHA-256 digest".to_owned())
}

fn qset_v6_image_digest_from(image: &str) -> Option<String> {
    let (_, digest) = image.rsplit_once("@sha256:")?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| format!("sha256:{digest}"))
}

fn qset_v6_source_revision() -> Result<String, String> {
    let configured = std::env::var("POLYEDGE_QSET_V6_WRITER_GIT_SHA")
        .map_err(|_| "POLYEDGE_QSET_V6_WRITER_GIT_SHA is required for retirement".to_owned())?;
    qset_v6_source_revision_from(&configured, embedded_git_sha())
}

fn qset_v6_source_revision_from(
    configured: &str,
    embedded: Option<&str>,
) -> Result<String, String> {
    if !polyedge_config::is_full_git_sha(configured) {
        return Err("POLYEDGE_QSET_V6_WRITER_GIT_SHA must be an exact lowercase commit".to_owned());
    }
    if embedded.is_some_and(|embedded| embedded != configured) {
        return Err("qset-v6 frozen source revision does not match the running binary".to_owned());
    }
    Ok(configured.to_owned())
}

fn qset_v7_image_digest() -> Result<String, String> {
    let image = std::env::var("POLYEDGE_QSET_V7_WRITER_IMAGE")
        .map_err(|_| "POLYEDGE_QSET_V7_WRITER_IMAGE is required for retirement".to_owned())?;
    qset_v7_image_digest_from(&image)
        .ok_or_else(|| "POLYEDGE_QSET_V7_WRITER_IMAGE must be pinned by SHA-256 digest".to_owned())
}

fn qset_v7_image_digest_from(image: &str) -> Option<String> {
    let (_, digest) = image.rsplit_once("@sha256:")?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| format!("sha256:{digest}"))
}

fn qset_v7_source_revision() -> Result<String, String> {
    let configured = std::env::var("POLYEDGE_QSET_V7_WRITER_GIT_SHA")
        .map_err(|_| "POLYEDGE_QSET_V7_WRITER_GIT_SHA is required for retirement".to_owned())?;
    qset_v7_source_revision_from(&configured, embedded_git_sha())
}

fn qset_v7_source_revision_from(
    configured: &str,
    embedded: Option<&str>,
) -> Result<String, String> {
    if !polyedge_config::is_full_git_sha(configured) {
        return Err("POLYEDGE_QSET_V7_WRITER_GIT_SHA must be an exact lowercase commit".to_owned());
    }
    if embedded.is_some_and(|embedded| embedded != configured) {
        return Err("qset-v7 frozen source revision does not match the running binary".to_owned());
    }
    Ok(configured.to_owned())
}

fn saturating_sub_atomic(value: &AtomicUsize, amount: usize) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

fn required_runtime_events(
    entries: Vec<(String, Value)>,
    recorded_at: DateTime<Utc>,
) -> Vec<RuntimeEvent> {
    entries
        .into_iter()
        .map(|(event_type, data)| RuntimeEvent {
            event_type,
            ts: recorded_at,
            data,
        })
        .collect()
}

fn required_recorder_batch_key(events: &[RuntimeEvent]) -> String {
    value_sha256(&json!(events
        .iter()
        .map(|event| json!({
            "event_type": event.event_type,
            "recorded_ts": event.ts,
            "payload": event.data
        }))
        .collect::<Vec<_>>()))
}

fn stale_decision_state_generation(
    observed_data_generation: u64,
    engine: &RuntimeEngine,
    evaluated: DecisionStateGeneration,
) -> Option<DecisionStateGeneration> {
    let observed = DecisionStateGeneration {
        data: observed_data_generation,
        engine: engine.decision_generation,
    };
    (observed != evaluated).then_some(observed)
}

fn market_start_evidence(
    market: &MarketSpec,
    reference: &ReferencePrice,
    settings: &RuntimeSettings,
) -> Option<MarketStartEvidenceV1> {
    let grace_millis = (settings.target.start_price_capture_grace_seconds * 1_000.0).round() as i64;
    let latest = market.start_ts + chrono::Duration::milliseconds(grace_millis.max(0));
    if reference.stale
        || !reference.exact_resolution_source
        || reference.source_ts < market.start_ts
        || reference.source_ts > latest
        || market.start_price != Some(reference.price)
    {
        return None;
    }
    Some(MarketStartEvidenceV1 {
        schema_version: 1,
        market_id: market.market_id.clone(),
        market_start_ts: market.start_ts,
        market_end_ts: market.end_ts,
        start_price: reference.price,
        reference_source: reference.source.clone(),
        reference_source_ts: reference.source_ts,
        reference_exact_resolution_source: true,
        reference_stale: false,
    })
}

fn active_markets(data: &RuntimeData) -> Vec<&MarketSpec> {
    let now = Utc::now();
    data.markets
        .values()
        .filter(|market| market.start_ts <= now && now < market.end_ts)
        .collect()
}

fn markets_by_token_from_data(data: &RuntimeData) -> BTreeMap<TokenId, MarketSpec> {
    let mut markets_by_token = BTreeMap::new();
    for market in data.markets.values() {
        markets_by_token.insert(market.up_token_id.clone(), market.clone());
        markets_by_token.insert(market.down_token_id.clone(), market.clone());
    }
    markets_by_token
}

fn books_for_market(
    market: &MarketSpec,
    books: &BTreeMap<TokenId, BookState>,
) -> BTreeMap<TokenId, BookState> {
    [&market.up_token_id, &market.down_token_id]
        .into_iter()
        .filter_map(|token_id| {
            books
                .get(token_id)
                .cloned()
                .map(|book| (token_id.clone(), book))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn runtime_regime_feature_input(
    market: &MarketSpec,
    fair_value: &FairValue,
    reference: &ReferencePrice,
    references: &VecDeque<ReferencePrice>,
    books: &BTreeMap<TokenId, BookState>,
    now: DateTime<Utc>,
    open_orders: usize,
    settings: &RuntimeSettings,
) -> RegimeFeatureInput {
    RegimeFeatureInput {
        now,
        market_start_ts: Some(market.start_ts),
        market_end_ts: Some(market.end_ts),
        start_price: market.start_price,
        tick_size: market.tick_size,
        reference: Some(RegimeReferencePoint {
            ts: reference.local_ts,
            price: reference.price,
            stale: reference.stale,
        }),
        reference_history: references
            .iter()
            .map(|point| RegimeReferencePoint {
                ts: point.local_ts,
                price: point.price,
                stale: point.stale,
            })
            .collect(),
        q_up: Some(fair_value.q_up),
        q_down: Some(fair_value.q_down),
        sigma: Some(fair_value.sigma),
        up_book: books.get(&market.up_token_id).map(runtime_book_snapshot),
        down_book: books.get(&market.down_token_id).map(runtime_book_snapshot),
        book_update_rate_10s: None,
        feed_divergence_bps: None,
        recent_feed_errors: 0,
        open_positions: None,
        open_orders,
        recent_fill_count: 0,
        recent_cancel_count: 0,
        adverse_move_after_fill_bps: None,
        max_reference_age_ms: settings.risk.max_reference_age_ms,
        max_book_age_ms: settings.risk.max_book_age_ms,
        final_no_trade_seconds: settings.strategy.final_no_trade_seconds,
        quality_flags: reference.quality_flags.clone(),
    }
}

fn runtime_book_snapshot(book: &BookState) -> RegimeBookSnapshot {
    RegimeBookSnapshot {
        bid: book.best_bid().map(|level| level.price),
        ask: book.best_ask().map(|level| level.price),
        bid_size: book.best_bid().map(|level| level.size),
        ask_size: book.best_ask().map(|level| level.size),
        local_ts: Some(book.local_ts),
    }
}

fn book_summary(book: &BookState, market: Option<&MarketSpec>) -> Value {
    let mut value = json!({
        "token_id": book.token_id,
        "best_bid": book.best_bid(),
        "best_ask": book.best_ask(),
        "last_trade_price": book.last_trade_price.map(|price| price.to_string()),
        "exchange_ts": book.exchange_ts,
        "local_ts": book.local_ts,
        "book_hash": book.book_hash
    });
    if let (Some(market), Value::Object(map)) = (market, &mut value) {
        map.insert("market_id".to_owned(), json!(market.market_id));
        if book.token_id == market.up_token_id {
            map.insert("outcome".to_owned(), json!("up"));
        } else if book.token_id == market.down_token_id {
            map.insert("outcome".to_owned(), json!("down"));
        }
    }
    value
}

fn compact_recorded_book(book: &BookState) -> BookState {
    BookState {
        token_id: book.token_id.clone(),
        bids: book.best_bid().cloned().into_iter().collect(),
        asks: book.best_ask().cloned().into_iter().collect(),
        last_trade_price: book.last_trade_price,
        exchange_ts: book.exchange_ts,
        local_ts: book.local_ts,
        book_hash: book.book_hash.clone(),
    }
}

fn feed_summary(data: &RuntimeData, settings: &RuntimeSettings) -> &'static str {
    let now = Utc::now();
    let healthy = |name: &str| {
        data.feed_status
            .get(name)
            .and_then(|status| status.get("status"))
            .and_then(Value::as_str)
            == Some("ok")
    };
    let fresh = |name: &str| fresh_market_feed_ok(data.feed_status.get(name), now);
    if healthy("Discovery")
        && fresh("PolymarketClobMarket")
        && (!settings.target.enable_polymarket_rtds_chainlink || fresh("PolymarketRtdsChainlink"))
        && (!settings.target.enable_polymarket_rtds_binance || fresh("PolymarketRtdsBinance"))
    {
        "running"
    } else if data.feed_status.values().any(|status| {
        status
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "error" || status == "disconnected")
    }) || (settings.target.enable_polymarket_rtds_chainlink
        && healthy("PolymarketRtdsChainlink")
        && !fresh("PolymarketRtdsChainlink"))
        || (settings.target.enable_polymarket_rtds_binance
            && healthy("PolymarketRtdsBinance")
            && !fresh("PolymarketRtdsBinance"))
    {
        "degraded"
    } else {
        "starting"
    }
}

fn fresh_market_feed_ok(status: Option<&Value>, now: DateTime<Utc>) -> bool {
    let Some(status) = status else {
        return false;
    };
    let Some(updated_at) = status["updated_at"]
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
    else {
        return false;
    };
    status["status"] == "ok"
        && updated_at <= now
        && now.signed_duration_since(updated_at)
            <= chrono::Duration::seconds(ESSENTIAL_FEED_MAX_AGE_SECONDS)
}

fn report_status(shadow_only: bool) -> Value {
    json!({
        "running_job": Value::Null,
        "known_jobs": 0,
        "store": {
            "backend_impl": "rust",
            "shadow_only": shadow_only
        }
    })
}

pub(super) fn runtime_git_sha() -> &'static str {
    embedded_git_sha().unwrap_or("unknown")
}

fn runtime_provenance(settings: &RuntimeSettings) -> Result<Value, String> {
    let git_sha = embedded_git_sha()
        .ok_or_else(|| "binary does not contain a canonical 40-character Git SHA".to_owned())?;
    runtime_provenance_with_git_sha(settings, git_sha)
}

fn runtime_provenance_with_git_sha(
    settings: &RuntimeSettings,
    git_sha: &str,
) -> Result<Value, String> {
    runtime_provenance_with_git_sha_at(settings, git_sha, Utc::now())
}

fn runtime_provenance_with_git_sha_at(
    settings: &RuntimeSettings,
    git_sha: &str,
    event_ts: DateTime<Utc>,
) -> Result<Value, String> {
    if !polyedge_config::is_full_git_sha(git_sha) {
        return Err("Git SHA is not a canonical 40-character lowercase commit ID".to_owned());
    }
    let adaptive_mode = configured_adaptive_mode(settings);
    let candidate = adaptive_mode
        .map(FrozenStrategyMode::candidate)
        .map(|candidate| serde_json::to_value(candidate).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);
    let decision_config_sha256 = decision_config_sha256(settings, adaptive_mode);
    let settings_bytes = serde_json::to_vec(settings)
        .map_err(|error| format!("failed to serialize runtime settings: {error}"))?;
    Ok(json!({
        "schema_version": 1,
        "backend_impl": "rust",
        "git_sha": git_sha,
        "runtime_config_hash": format!("sha256:{:x}", Sha256::digest(settings_bytes)),
        "app_name": settings.deploy.app_name,
        "runtime_role": settings.deploy.runtime_role.as_str(),
        "shadow_only": settings.deploy.runtime_role.is_shadow(),
        "execution_mode": execution_mode(settings),
        "allow_live": settings.live.allow_live,
        "enable_taker_orders": settings.strategy.enable_taker_orders,
        "allow_emergency_account_cancel": settings.live.allow_emergency_account_cancel,
        "paper_maker_fill_policy": settings.paper.maker_fill_policy,
        "adaptive_regime_enabled": settings.strategy.adaptive_regime_enabled,
        "adaptive_regime_mode": settings.strategy.adaptive_regime_mode,
        "decision_pipeline_schema": "polyedge.strategy_decision_batch.v4",
        "decision_pipeline_parity_scope": "full_decision_pipeline_recomputation",
        "decision_config_schema": "polyedge.decision_config.v1",
        "decision_config_sha256": decision_config_sha256,
        "candidate": candidate,
        "authoritative_recorder_backend": if settings.azure.storage_account_name.is_some() {
            "azure_append_blob"
        } else {
            "local_jsonl"
        },
        "storage_account": settings.azure.storage_account_name,
        "storage_container": settings.azure.storage_container_name,
        "event_blob_prefix": settings.azure.event_blob_prefix_at(event_ts),
        "event_blob_prefix_routing": {
            "before_cutover": settings.azure.event_blob_prefix,
            "after_cutover": settings.azure.event_blob_prefix_after_cutover,
            "cutover_utc": settings.azure.event_blob_prefix_cutover_utc,
            "evaluated_event_ts": event_ts,
            "effective_prefix": settings.azure.event_blob_prefix_at(event_ts)
        },
        "compact_shadow_recording": settings.azure.compact_shadow_recording,
        "shadow_book_sample_ms": settings.azure.shadow_book_sample_ms,
        "publish_strategy_canary_intents": settings.azure.publish_strategy_canary_intents,
        "execution_model": {
            "version": settings.azure.strategy_canary_fill_model_version,
            "blob_uri": settings.azure.strategy_canary_execution_model_blob_uri,
            "sha256": settings.azure.strategy_canary_execution_model_sha256
        },
        "research_only": !settings.live_requested()
    }))
}

fn execution_mode(settings: &RuntimeSettings) -> &'static str {
    match settings.live.execution_mode {
        ExecutionMode::Paper => "paper",
        ExecutionMode::Live => "live",
    }
}

fn secret_safe_pipeline_settings(settings: &RuntimeSettings) -> RuntimeSettings {
    let mut safe = settings.clone();
    safe.deploy.api_bearer_token = None;
    safe.target.chainlink_api_key = None;
    safe.live.polymarket_private_key = None;
    safe
}

fn configured_adaptive_mode(settings: &RuntimeSettings) -> Option<FrozenStrategyMode> {
    settings.strategy.adaptive_regime_enabled.then(|| {
        FrozenStrategyMode::from_runtime_mode(&settings.strategy.adaptive_regime_mode)
            .unwrap_or(FrozenStrategyMode::DynamicQuoteStyle)
    })
}

fn decision_config_projection(
    settings: &RuntimeSettings,
    adaptive_mode: Option<FrozenStrategyMode>,
) -> Value {
    decision_config_projection_v1(settings, adaptive_mode)
}

fn decision_config_sha256(
    settings: &RuntimeSettings,
    adaptive_mode: Option<FrozenStrategyMode>,
) -> String {
    value_sha256(&decision_config_projection(settings, adaptive_mode))
}

fn decision_event_payload(
    decision: &TradeDecision,
    metadata: Option<&StrategyDecisionMetadata>,
    binding: Option<&DecisionBatchBinding>,
) -> Value {
    let mut payload = serde_json::to_value(decision).unwrap_or(Value::Null);
    let Some(object) = payload.as_object_mut() else {
        return payload;
    };
    if let Some(metadata) = metadata {
        object.insert(
            "strategy_metadata".to_owned(),
            serde_json::to_value(metadata).unwrap_or(Value::Null),
        );
    }
    if let Some(binding) = binding {
        object.insert("decision_batch_schema_version".to_owned(), json!(4));
        object.insert("strategy_batch_id".to_owned(), json!(binding.batch_id));
        object.insert(
            "strategy_batch_output_index".to_owned(),
            json!(binding.output_index),
        );
        object.insert(
            "strategy_decision_sha256".to_owned(),
            json!(binding.decision_sha256),
        );
    }
    payload
}

fn bind_decision_event_payload(unbound_payload: &Value, binding: &DecisionBatchBinding) -> Value {
    let mut payload = unbound_payload.clone();
    let Some(object) = payload.as_object_mut() else {
        return payload;
    };
    object.insert("decision_batch_schema_version".to_owned(), json!(4));
    object.insert("strategy_batch_id".to_owned(), json!(binding.batch_id));
    object.insert(
        "strategy_batch_output_index".to_owned(),
        json!(binding.output_index),
    );
    object.insert(
        "strategy_decision_sha256".to_owned(),
        json!(binding.decision_sha256),
    );
    payload
}

fn bind_applied_decision_output(
    prepared: &PreparedDecision,
    mut reports: Vec<ExecutionReport>,
) -> Option<AppliedDecisionOutput> {
    if !matches!(
        prepared.decision.action,
        DecisionAction::Place | DecisionAction::CancelAll
    ) {
        return None;
    }
    if value_sha256(&prepared.unbound_payload) != prepared.binding.decision_sha256 {
        return None;
    }
    if reports.iter().any(|report| {
        report.market_id != prepared.decision.market_id || !report.status.starts_with("paper_")
    }) {
        return None;
    }
    let order_id = if prepared.decision.action == DecisionAction::Place {
        if reports.len() != 1
            || reports[0].token_id != prepared.decision.token_id
            || reports[0].order_id.is_none()
        {
            return None;
        }
        reports[0].order_id.clone()
    } else {
        None
    };
    let application_id = format!(
        "paper-application-{}",
        value_sha256(&json!({
            "schema": "polyedge.paper_decision_output_application.v1",
            "strategy_batch_id": prepared.binding.batch_id,
            "strategy_batch_output_index": prepared.binding.output_index,
            "strategy_decision_sha256": prepared.binding.decision_sha256
        }))
        .trim_start_matches("sha256:")
    );
    for report in &mut reports {
        report.raw.insert(
            "decision_application".to_owned(),
            json!({
                "schema": "polyedge.paper_decision_output_application.v1",
                "application_id": application_id,
                "strategy_batch_id": prepared.binding.batch_id,
                "strategy_batch_output_index": prepared.binding.output_index,
                "strategy_decision_sha256": prepared.binding.decision_sha256
            }),
        );
    }
    let report_values = reports
        .iter()
        .map(|report| serde_json::to_value(report).ok())
        .collect::<Option<Vec<_>>>()?;
    let execution_reports = Value::Array(report_values);
    let execution_reports_sha256 = value_sha256(&execution_reports);
    let application = json!({
        "schema": "polyedge.paper_decision_output_application.v1",
        "schema_version": 1,
        "application_id": application_id,
        "strategy_batch_id": prepared.binding.batch_id,
        "strategy_batch_output_index": prepared.binding.output_index,
        "strategy_decision_sha256": prepared.binding.decision_sha256,
        "action": prepared.decision.action,
        "market_id": prepared.decision.market_id,
        "token_id": prepared.decision.token_id,
        "side": prepared.decision.side,
        "price": prepared.decision.price.map(|value| value.to_string()),
        "size": prepared.decision.size.map(|value| value.to_string()),
        "order_kind": prepared.decision.order_kind,
        "order_id": order_id,
        "execution_report_count": reports.len(),
        "execution_reports_sha256": execution_reports_sha256,
        "execution_reports": execution_reports,
        "applied": true,
        "paper_only": true
    });
    Some(AppliedDecisionOutput {
        application,
        reports,
    })
}

fn value_sha256(value: &Value) -> String {
    canonical_json_sha256(value)
}

fn decision_batch_id_v4(pipeline_input_sha256: &str) -> String {
    format!(
        "strategy-batch-{}",
        pipeline_input_sha256.trim_start_matches("sha256:")
    )
}

fn validate_decision_batch_content_bindings(payload: &Value) -> Result<(), &'static str> {
    let input = payload
        .get("pipeline_input")
        .ok_or("missing pipeline input")?;
    let output = payload
        .get("pipeline_output")
        .ok_or("missing pipeline output")?;
    let input_sha256 = value_sha256(input);
    let output_sha256 = value_sha256(output);
    let start_sha256 = input
        .get("market_start_evidence")
        .map(value_sha256)
        .ok_or("missing market start evidence")?;
    if payload.get("pipeline_input_sha256").and_then(Value::as_str) != Some(input_sha256.as_str()) {
        return Err("pipeline input hash mismatch");
    }
    if payload
        .get("pipeline_output_sha256")
        .and_then(Value::as_str)
        != Some(output_sha256.as_str())
    {
        return Err("pipeline output hash mismatch");
    }
    if payload
        .get("market_start_evidence_sha256")
        .and_then(Value::as_str)
        != Some(start_sha256.as_str())
    {
        return Err("market start evidence hash mismatch");
    }
    if payload.get("batch_id").and_then(Value::as_str)
        != Some(decision_batch_id_v4(&input_sha256).as_str())
    {
        return Err("strategy batch id mismatch");
    }
    let bound = payload
        .get("bound_final_decisions")
        .and_then(Value::as_array)
        .ok_or("missing bound final decisions")?;
    for (index, entry) in bound.iter().enumerate() {
        let decision = entry.get("decision").ok_or("missing bound decision")?;
        let decision_sha256 = value_sha256(decision);
        if entry.get("output_index").and_then(Value::as_u64) != Some(index as u64)
            || entry.get("decision_sha256").and_then(Value::as_str)
                != Some(decision_sha256.as_str())
        {
            return Err("bound final decision mismatch");
        }
    }
    Ok(())
}

fn paper_settlement_journal_id(
    market: &MarketSpec,
    start_reference: &ReferencePrice,
    final_reference: &ReferencePrice,
) -> String {
    let seed = json!({
        "schema": "polyedge.paper_settlement_journal.v1",
        "market_id": market.market_id,
        "start_ts": market.start_ts,
        "end_ts": market.end_ts,
        "start_price": market.start_price,
        "start_reference_source": start_reference.source,
        "start_reference_source_ts": start_reference.source_ts,
        "start_reference_price": start_reference.price,
        "final_reference_source": final_reference.source,
        "final_reference_source_ts": final_reference.source_ts,
        "final_price": final_reference.price
    });
    format!(
        "paper-settlement-{}",
        value_sha256(&seed).trim_start_matches("sha256:")
    )
}

fn finalize_settlement_journal(
    journal_id: &str,
    unbound_events: Vec<(String, Value)>,
) -> Vec<(String, Value)> {
    let event_count = unbound_events.len();
    let projection = json!({
        "schema": "polyedge.paper_settlement_journal.v1",
        "settlement_journal_id": journal_id,
        "settlement_journal_event_count": event_count,
        "events": unbound_events
            .iter()
            .enumerate()
            .map(|(event_index, (event_type, payload))| json!({
                "event_index": event_index,
                "event_type": event_type,
                "payload": payload
            }))
            .collect::<Vec<_>>()
    });
    let journal_sha256 = value_sha256(&projection);
    unbound_events
        .into_iter()
        .enumerate()
        .map(|(event_index, (event_type, mut payload))| {
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "settlement_journal_schema".to_owned(),
                    json!("polyedge.paper_settlement_journal.v1"),
                );
                object.insert("settlement_journal_id".to_owned(), json!(journal_id));
                object.insert(
                    "settlement_journal_event_index".to_owned(),
                    json!(event_index),
                );
                object.insert(
                    "settlement_journal_event_count".to_owned(),
                    json!(event_count),
                );
                object.insert(
                    "settlement_journal_sha256".to_owned(),
                    json!(journal_sha256),
                );
            }
            (event_type, payload)
        })
        .collect()
}

fn truncate<T>(values: &mut VecDeque<T>, limit: usize) {
    while values.len() > limit {
        values.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyedge_domain::{
        BookLevel, ConditionId, MarketStatus, OrderId, OrderKind, Outcome, Side,
    };
    use polyedge_storage::{EventRecorder, StorageError};
    use serde_json::json;
    use std::fs;
    use std::thread;
    use std::time::Duration as StdDuration;

    fn clob_test_book(token: &str) -> BookState {
        BookState {
            token_id: TokenId::new(token),
            bids: Vec::new(),
            asks: Vec::new(),
            last_trade_price: None,
            exchange_ts: None,
            local_ts: Utc::now(),
            book_hash: None,
        }
    }

    fn clob_test_event(token: &str) -> polyedge_feeds::MarketChannelEvent {
        polyedge_feeds::MarketChannelEvent {
            event_type: "price_change".to_owned(),
            recorded_ts: Utc::now(),
            source_ts: None,
            market_id: None,
            condition_id: None,
            token_id: Some(token.to_owned()),
            asset_id: Some(token.to_owned()),
            side: Some("BUY".to_owned()),
            price: Some("0.5".to_owned()),
            size: Some("1".to_owned()),
            best_bid: None,
            best_ask: None,
            book_hash: None,
            raw_payload: json!({"event_type": "price_change", "asset_id": token}),
        }
    }

    #[tokio::test]
    async fn finished_runtime_task_marks_runtime_unhealthy() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        controller.inner.started.store(true, Ordering::SeqCst);
        *controller.inner.feed_task.lock().unwrap() = Some(tokio::spawn(async {}));
        controller
            .inner
            .background_tasks
            .lock()
            .unwrap()
            .push(tokio::spawn(std::future::pending()));
        tokio::task::yield_now().await;

        assert!(!controller.runtime_tasks_running());
        assert_eq!(controller.health().await["ok"], false);
        assert_eq!(
            controller.status().await["task_health"]["runtime_loop"],
            "failed"
        );
    }

    #[tokio::test]
    async fn clob_generation_rejects_pre_ready_and_stale_frames() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let mut data = controller.inner.data.write().await;
        let observed_at = Utc::now();
        assert!(!controller.accept_clob_sequence(&mut data, Some((7, 1)), observed_at));
        data.clob_generation = Some(7);
        data.clob_lease = Some(ClobGenerationLease::new());
        assert!(controller.accept_clob_sequence(&mut data, Some((7, 1)), observed_at));
        assert!(!controller.accept_clob_sequence(&mut data, Some((7, 1)), observed_at));
        assert!(!controller.accept_clob_sequence(&mut data, Some((6, 2)), observed_at));
        assert!(controller.accept_clob_sequence(&mut data, Some((7, 2)), observed_at));
        data.clob_lease.as_ref().unwrap().terminate();
        assert!(!controller.accept_clob_sequence(&mut data, Some((7, 3)), observed_at));
    }

    #[tokio::test]
    async fn accepted_clob_events_refresh_feed_status_but_rejected_events_do_not() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        {
            let mut data = controller.inner.data.write().await;
            data.clob_generation = Some(7);
            data.clob_lease = Some(ClobGenerationLease::new());
        }

        let book_ts = Utc::now() - chrono::Duration::seconds(2);
        let mut book = clob_test_book("test-token");
        book.local_ts = book_ts;
        controller
            .handle_feed_event(FeedEvent::ClobBook {
                generation: 7,
                sequence: 1,
                book,
            })
            .await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"]["updated_at"],
            json!(book_ts)
        );

        let raw_ts = book_ts + chrono::Duration::seconds(1);
        let mut event = clob_test_event("test-token");
        event.recorded_ts = raw_ts;
        controller
            .handle_feed_event(FeedEvent::ClobRawMarketEvent {
                generation: 7,
                sequence: 2,
                event,
            })
            .await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"]["updated_at"],
            json!(raw_ts)
        );

        let invalidated_after = Utc::now();
        controller
            .handle_feed_event(FeedEvent::ClobBookInvalidated {
                generation: 7,
                sequence: 3,
                token_id: TokenId::new("test-token"),
            })
            .await;
        let accepted_status =
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"].clone();
        let invalidated_at = accepted_status["updated_at"]
            .as_str()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .expect("invalidation freshness timestamp");
        assert!(invalidated_at >= invalidated_after);

        let mut stale_book = clob_test_book("test-token");
        stale_book.local_ts = Utc::now() + chrono::Duration::hours(1);
        controller
            .handle_feed_event(FeedEvent::ClobBook {
                generation: 7,
                sequence: 3,
                book: stale_book,
            })
            .await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"],
            accepted_status
        );

        let mut rejected_event = clob_test_event("test-token");
        rejected_event.recorded_ts = Utc::now() + chrono::Duration::hours(1);
        controller
            .handle_feed_event(FeedEvent::ClobRawMarketEvent {
                generation: 6,
                sequence: 4,
                event: rejected_event,
            })
            .await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"],
            accepted_status
        );

        controller
            .inner
            .data
            .write()
            .await
            .clob_lease
            .as_ref()
            .unwrap()
            .terminate();
        controller
            .handle_feed_event(FeedEvent::ClobBookInvalidated {
                generation: 7,
                sequence: 4,
                token_id: TokenId::new("test-token"),
            })
            .await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"],
            accepted_status
        );
    }

    #[tokio::test]
    async fn terminal_tombstone_precedes_an_unpolled_generation() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let lease = controller
            .begin_clob_generation(42, &[TokenId::new("test-token")])
            .await;
        assert!(!lease.is_terminal());
        controller
            .terminate_clob_generation(42, "test pre-install cancellation")
            .await;
        let data = controller.inner.data.read().await;
        assert_eq!(data.clob_pending_generation, None);
        assert_eq!(data.clob_generation, None);
        assert_eq!(data.clob_terminal_generation, 42);
    }

    #[tokio::test]
    async fn same_token_pending_replacement_tombstones_the_prior_lease() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let first = controller
            .begin_clob_generation(421, &[TokenId::new("same-token")])
            .await;
        let second = controller
            .begin_clob_generation(422, &[TokenId::new("same-token")])
            .await;
        let data = controller.inner.data.read().await;
        assert!(first.is_terminal());
        assert!(!second.is_terminal());
        assert_eq!(data.clob_pending_generation, Some(422));
        assert_eq!(data.clob_terminal_generation, 421);
    }

    #[tokio::test]
    async fn concurrent_terminal_signals_emit_one_abort_for_the_generation() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let lease = controller
            .begin_clob_generation(423, &[TokenId::new("terminal-token")])
            .await;
        let left = controller.terminate_clob_generation(423, "concurrent left");
        let right = controller.terminate_clob_generation(423, "concurrent right");
        tokio::join!(left, right);
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(
            data.recent_events
                .iter()
                .filter(|event| event.event_type == "clob_resync_aborted")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn terminal_first_prevents_the_real_live_publication_helper_from_calling_publisher() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let token = TokenId::new("publish-token");
        let book = clob_test_book(token.as_ref());
        let lease = controller
            .begin_clob_generation(424, std::slice::from_ref(&token))
            .await;
        {
            let mut data = controller.inner.data.write().await;
            data.clob_generation = Some(424);
            data.clob_pending_generation = None;
            data.books.insert(token.clone(), book.clone());
        }
        controller
            .terminate_clob_generation(424, "terminal before external publish")
            .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let publisher_calls = Arc::clone(&calls);
        assert_eq!(
            controller
                .execute_live_clob_publication(424, &token, &book, move || {
                    publisher_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap(),
            None
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(lease.is_terminal());
    }

    #[tokio::test]
    async fn publish_first_makes_terminal_wait_for_the_real_live_publication_helper() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let token = TokenId::new("publish-token");
        let book = clob_test_book(token.as_ref());
        let lease = controller
            .begin_clob_generation(425, std::slice::from_ref(&token))
            .await;
        {
            let mut data = controller.inner.data.write().await;
            data.clob_generation = Some(425);
            data.clob_pending_generation = None;
            data.books.insert(token.clone(), book.clone());
        }
        let (started_tx, started_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let published = Arc::new(AtomicUsize::new(0));
        let publication = {
            let controller = controller.clone();
            let published = Arc::clone(&published);
            tokio::spawn(async move {
                controller
                    .execute_live_clob_publication(425, &token, &book, move || {
                        started_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        published.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(StdDuration::from_secs(1)))
            .await
            .unwrap()
            .unwrap();
        let terminator = {
            let controller = controller.clone();
            tokio::spawn(async move {
                controller
                    .terminate_clob_generation(425, "terminal after external publish start")
                    .await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!lease.is_terminal());
        release_tx.send(()).unwrap();
        assert_eq!(publication.await.unwrap().unwrap(), Some(()));
        terminator.await.unwrap();
        assert_eq!(published.load(Ordering::SeqCst), 1);
        assert!(lease.is_terminal());
    }

    #[tokio::test]
    async fn transferred_decision_gate_publishes_before_a_queued_book_update() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let token = TokenId::new("publish-token");
        let book = clob_test_book(token.as_ref());
        controller
            .begin_clob_generation(426, std::slice::from_ref(&token))
            .await;
        {
            let mut data = controller.inner.data.write().await;
            data.clob_generation = Some(426);
            data.clob_pending_generation = None;
            data.books.insert(token.clone(), book.clone());
        }

        let decision_guard = Arc::clone(&controller.inner.decision_gate)
            .lock_owned()
            .await;
        let mut updated_book = book.clone();
        updated_book.local_ts += chrono::Duration::seconds(1);
        let update = {
            let controller = controller.clone();
            tokio::spawn(async move {
                controller
                    .handle_feed_event(FeedEvent::ClobBook {
                        generation: 426,
                        sequence: 1,
                        book: updated_book,
                    })
                    .await;
            })
        };
        tokio::task::yield_now().await;

        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);
        assert_eq!(
            controller
                .execute_live_clob_publication_while_gated(426, &token, &book, move || {
                    publish_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap(),
            Some(())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(controller.inner.data.read().await.books[&token], book);

        drop(decision_guard);
        update.await.unwrap();
        assert_ne!(controller.inner.data.read().await.books[&token], book);
    }

    #[tokio::test]
    async fn rejected_clob_barrier_atomically_tombstones_its_pending_generation() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let lease = controller
            .begin_clob_generation(43, &[TokenId::new("expected-token")])
            .await;
        let (ready_ack, ready_result) = oneshot::channel();
        controller
            .handle_clob_resync_barrier(ClobResyncBarrier {
                generation: 43,
                sequence: 0,
                token_set_digest: "not-the-pending-set".to_owned(),
                token_count: 0,
                anchors: Vec::new(),
                pre_ready_events: Vec::new(),
                lease: lease.clone(),
                ready_ack,
            })
            .await;
        assert!(ready_result.await.unwrap().is_err());
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(data.clob_pending_generation, None);
        assert_eq!(data.clob_generation, None);
        assert_eq!(data.clob_terminal_generation, 43);
    }

    #[tokio::test]
    async fn partial_clob_barrier_clears_unanchored_stale_book_before_authorization() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let expected = BTreeSet::from([TokenId::new("yes"), TokenId::new("no")]);
        {
            let mut data = controller.inner.data.write().await;
            data.books.insert(TokenId::new("no"), clob_test_book("no"));
        }
        let tokens = expected.iter().cloned().collect::<Vec<_>>();
        let lease = controller.begin_clob_generation(431, &tokens).await;
        let authorized_book = clob_test_book("yes");
        let (ready_ack, ready_result) = oneshot::channel();
        controller
            .handle_clob_resync_barrier(ClobResyncBarrier {
                generation: 431,
                sequence: 0,
                token_set_digest: clob_token_set_digest(&expected),
                token_count: expected.len(),
                anchors: vec![authorized_book.clone()],
                pre_ready_events: Vec::new(),
                lease,
                ready_ack,
            })
            .await;
        assert!(ready_result.await.unwrap().is_ok());
        {
            let data = controller.inner.data.read().await;
            assert_eq!(data.clob_generation, Some(431));
            assert_eq!(data.clob_tokens, expected);
            assert!(data.books.contains_key(&TokenId::new("yes")));
            assert!(!data.books.contains_key(&TokenId::new("no")));
            assert_eq!(data.feed_status["PolymarketClobMarket"]["status"], "ok");
        }

        let yes = TokenId::new("yes");
        controller
            .handle_feed_event(FeedEvent::ClobBookInvalidated {
                generation: 431,
                sequence: 1,
                token_id: yes.clone(),
            })
            .await;
        assert!(!controller.inner.data.read().await.books.contains_key(&yes));
        controller
            .handle_feed_event(FeedEvent::ClobBook {
                generation: 431,
                sequence: 2,
                book: clob_test_book("yes"),
            })
            .await;
        assert!(controller.inner.data.read().await.books.contains_key(&yes));
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);
        assert_eq!(
            controller
                .execute_live_clob_publication(431, &yes, &authorized_book, move || {
                    publish_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap(),
            None
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn clob_pre_ready_evidence_recorder_failure_rejects_the_barrier() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            durable_flush_failures_remaining: REQUIRED_RECORDER_ATTEMPTS,
            ..BufferedRecorderTestState::default()
        }));
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        let token = TokenId::new("pre-ready-token");
        let lease = controller.begin_clob_generation(44, &[token.clone()]).await;
        let (ready_ack, ready_result) = oneshot::channel();
        controller
            .handle_clob_resync_barrier(ClobResyncBarrier {
                generation: 44,
                sequence: 0,
                token_set_digest: clob_token_set_digest(&BTreeSet::from([token])),
                token_count: 1,
                anchors: vec![clob_test_book("pre-ready-token")],
                pre_ready_events: vec![clob_test_event("pre-ready-token")],
                lease: lease.clone(),
                ready_ack,
            })
            .await;
        assert!(ready_result.await.unwrap().is_err());
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(data.clob_pending_generation, None);
        assert_eq!(data.clob_generation, None);
    }

    #[tokio::test]
    async fn clob_authorization_recorder_failure_rejects_the_barrier() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            durable_flush_failures_remaining: REQUIRED_RECORDER_ATTEMPTS,
            ..BufferedRecorderTestState::default()
        }));
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        let token = TokenId::new("authorization-token");
        let lease = controller.begin_clob_generation(45, &[token.clone()]).await;
        let (ready_ack, ready_result) = oneshot::channel();
        controller
            .handle_clob_resync_barrier(ClobResyncBarrier {
                generation: 45,
                sequence: 0,
                token_set_digest: clob_token_set_digest(&BTreeSet::from([token])),
                token_count: 1,
                anchors: vec![clob_test_book("authorization-token")],
                pre_ready_events: Vec::new(),
                lease: lease.clone(),
                ready_ack,
            })
            .await;
        assert!(ready_result.await.unwrap().is_err());
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(data.clob_pending_generation, None);
        assert_eq!(data.clob_generation, None);
    }

    #[tokio::test]
    async fn dropped_clob_ready_ack_revokes_the_generation() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let token = TokenId::new("ack-token");
        let lease = controller.begin_clob_generation(46, &[token.clone()]).await;
        let (ready_ack, ready_result) = oneshot::channel();
        drop(ready_result);
        controller
            .handle_clob_resync_barrier(ClobResyncBarrier {
                generation: 46,
                sequence: 0,
                token_set_digest: clob_token_set_digest(&BTreeSet::from([token])),
                token_count: 1,
                anchors: vec![clob_test_book("ack-token")],
                pre_ready_events: Vec::new(),
                lease: lease.clone(),
                ready_ack,
            })
            .await;
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(data.clob_generation, None);
        assert_eq!(data.clob_terminal_generation, 46);
    }

    #[tokio::test]
    async fn token_refresh_and_repeated_terminal_signal_abort_once() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let lease = controller
            .begin_clob_generation(47, &[TokenId::new("old-token")])
            .await;
        controller.replace_markets(Vec::new()).await;
        controller
            .terminate_clob_generation(47, "duplicate terminal signal")
            .await;
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(data.clob_terminal_generation, 47);
        assert_eq!(
            data.recent_events
                .iter()
                .filter(|event| event.event_type == "clob_resync_aborted")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn clob_abort_recorder_failure_keeps_the_generation_terminal() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            durable_flush_failures_remaining: REQUIRED_RECORDER_ATTEMPTS,
            ..BufferedRecorderTestState::default()
        }));
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        let lease = controller
            .begin_clob_generation(48, &[TokenId::new("abort-audit-token")])
            .await;
        controller
            .terminate_clob_generation(48, "injected abort audit failure")
            .await;
        let data = controller.inner.data.read().await;
        assert!(lease.is_terminal());
        assert_eq!(data.clob_generation, None);
        assert_eq!(data.clob_pending_generation, None);
        assert_eq!(data.clob_terminal_generation, 48);
    }

    #[tokio::test]
    async fn feed_health_requires_every_enabled_source_and_market_data_clears_clob_error() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let base = Utc::now() - chrono::Duration::seconds(20);
        controller.set_feed_status("Discovery", "ok", None).await;
        controller
            .set_feed_status("PolymarketRtdsChainlink", "ok", None)
            .await;
        controller
            .set_feed_status("PolymarketRtdsBinance", "ok", None)
            .await;
        controller
            .feed_error_at(
                FeedName::PolymarketClobMarket,
                "injected disconnect".to_owned(),
                base + chrono::Duration::seconds(10),
            )
            .await;
        {
            let data = controller.inner.data.read().await;
            assert_eq!(feed_summary(&data, &controller.inner.settings), "degraded");
        }

        controller
            .handle_feed_event(FeedEvent::Book(BookState {
                token_id: TokenId::new("recovered-token"),
                bids: Vec::new(),
                asks: Vec::new(),
                last_trade_price: None,
                exchange_ts: None,
                local_ts: base,
                book_hash: None,
            }))
            .await;
        {
            let data = controller.inner.data.read().await;
            assert_eq!(data.feed_status["PolymarketClobMarket"]["status"], "error");
            assert_eq!(feed_summary(&data, &controller.inner.settings), "degraded");
        }

        controller
            .handle_feed_event(FeedEvent::Book(BookState {
                token_id: TokenId::new("recovered-token"),
                bids: Vec::new(),
                asks: Vec::new(),
                last_trade_price: None,
                exchange_ts: None,
                local_ts: base + chrono::Duration::seconds(11),
                book_hash: None,
            }))
            .await;

        let data = controller.inner.data.read().await;
        assert_eq!(data.feed_status["PolymarketClobMarket"]["status"], "ok");
        assert_eq!(feed_summary(&data, &controller.inner.settings), "running");
        drop(data);

        for source in [
            FeedName::PolymarketRtdsChainlink,
            FeedName::PolymarketRtdsBinance,
        ] {
            let name = format!("{source:?}");
            {
                let mut data = controller.inner.data.write().await;
                data.feed_status.get_mut(&name).unwrap()["updated_at"] =
                    json!(Utc::now() - chrono::Duration::minutes(6));
            }
            {
                let data = controller.inner.data.read().await;
                assert_eq!(feed_summary(&data, &controller.inner.settings), "degraded");
            }
            controller
                .handle_feed_event(FeedEvent::Heartbeat {
                    source,
                    ts: Utc::now(),
                })
                .await;
            let data = controller.inner.data.read().await;
            assert_eq!(feed_summary(&data, &controller.inner.settings), "running");
        }
    }

    #[tokio::test]
    async fn planned_market_resubscription_preserves_only_fresh_ok_status() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        controller.mark_market_feed_connecting().await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"]["status"],
            "connecting"
        );

        controller
            .set_feed_status("PolymarketClobMarket", "ok", None)
            .await;
        let before = controller.inner.data.read().await.feed_status["PolymarketClobMarket"].clone();
        controller.mark_market_feed_connecting().await;
        assert_eq!(
            controller.inner.data.read().await.feed_status["PolymarketClobMarket"],
            before
        );

        let now = Utc::now();
        for (name, status, expected) in [
            (
                "exact boundary",
                json!({"status":"ok","updated_at":now - chrono::Duration::seconds(300)}),
                true,
            ),
            (
                "past boundary",
                json!({"status":"ok","updated_at":now - chrono::Duration::seconds(301)}),
                false,
            ),
            (
                "future",
                json!({"status":"ok","updated_at":now + chrono::Duration::seconds(1)}),
                false,
            ),
            (
                "malformed",
                json!({"status":"ok","updated_at":"invalid"}),
                false,
            ),
            (
                "not ok",
                json!({"status":"connecting","updated_at":now}),
                false,
            ),
        ] {
            assert_eq!(fresh_market_feed_ok(Some(&status), now), expected, "{name}");
        }

        let stale_controller = RuntimeController::new(RuntimeSettings::default());
        for name in [
            "Discovery",
            "PolymarketRtdsChainlink",
            "PolymarketRtdsBinance",
        ] {
            stale_controller.set_feed_status(name, "ok", None).await;
        }
        stale_controller
            .set_feed_status_at(
                "PolymarketClobMarket",
                "ok",
                None,
                Utc::now() - chrono::Duration::minutes(6),
            )
            .await;
        let data = stale_controller.inner.data.read().await;
        assert_eq!(
            feed_summary(&data, &stale_controller.inner.settings),
            "starting"
        );
    }

    #[tokio::test]
    async fn clean_disconnects_are_durable_canonical_feed_errors() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        controller
            .record_feed_disconnect(
                &[
                    FeedName::PolymarketClobMarket,
                    FeedName::PolymarketRtdsChainlink,
                    FeedName::PolymarketRtdsBinance,
                ],
                "injected clean disconnect",
            )
            .await;

        let data = controller.inner.data.read().await;
        for source in [
            "PolymarketClobMarket",
            "PolymarketRtdsChainlink",
            "PolymarketRtdsBinance",
        ] {
            assert_eq!(data.feed_status[source]["status"], "error");
        }
        assert_eq!(feed_summary(&data, &controller.inner.settings), "degraded");
        assert_eq!(
            data.recent_events
                .iter()
                .filter(|event| event.event_type == "feed_error")
                .count(),
            3
        );
    }

    #[test]
    fn rtds_source_settings_enable_only_the_requested_subscription() {
        let settings = RuntimeSettings::default();
        let chainlink = rtds_source_settings(&settings, &FeedName::PolymarketRtdsChainlink);
        assert!(chainlink.target.enable_polymarket_rtds_chainlink);
        assert!(!chainlink.target.enable_polymarket_rtds_binance);

        let binance = rtds_source_settings(&settings, &FeedName::PolymarketRtdsBinance);
        assert!(!binance.target.enable_polymarket_rtds_chainlink);
        assert!(binance.target.enable_polymarket_rtds_binance);
    }

    #[tokio::test]
    async fn rtds_disconnect_is_scoped_to_the_failed_source() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        for source in [
            "Discovery",
            "PolymarketClobMarket",
            "PolymarketRtdsChainlink",
            "PolymarketRtdsBinance",
        ] {
            controller.set_feed_status(source, "ok", None).await;
        }

        controller
            .record_feed_disconnect(
                &[FeedName::PolymarketRtdsChainlink],
                "injected Chainlink disconnect",
            )
            .await;

        let data = controller.inner.data.read().await;
        assert_eq!(
            data.feed_status["PolymarketRtdsChainlink"]["status"],
            "error"
        );
        assert_eq!(data.feed_status["PolymarketRtdsBinance"]["status"], "ok");
        assert_eq!(feed_summary(&data, &controller.inner.settings), "degraded");
        assert_eq!(
            data.recent_events
                .iter()
                .filter(|event| event.event_type == "feed_error")
                .count(),
            1
        );
    }

    #[derive(Default)]
    struct BufferedRecorderTestState {
        pending: Vec<RuntimeEvent>,
        pending_sequences: Vec<u64>,
        retry_pending: Vec<RuntimeEvent>,
        retry_sequences: Vec<u64>,
        committed_event_types: Vec<Vec<String>>,
        committed_sequences: Vec<Vec<u64>>,
        record_batch_calls: Vec<Vec<String>>,
        best_effort_record_failures_remaining: usize,
        retry_flush_failures_remaining: usize,
        durable_flush_failures_remaining: usize,
    }

    struct BufferedRecorderTestDouble {
        state: Arc<StdMutex<BufferedRecorderTestState>>,
    }

    impl EventRecorder for BufferedRecorderTestDouble {
        fn record(&mut self, event: &RuntimeEvent) -> Result<(), StorageError> {
            self.record_batch(std::slice::from_ref(event))
        }

        fn record_batch(&mut self, events: &[RuntimeEvent]) -> Result<(), StorageError> {
            let mut state = self.state.lock().unwrap();
            state.record_batch_calls.push(
                events
                    .iter()
                    .map(|event| event.event_type.clone())
                    .collect(),
            );
            let best_effort_only = events.iter().all(|event| event.event_type == "book");
            if best_effort_only && state.best_effort_record_failures_remaining > 0 {
                state.best_effort_record_failures_remaining -= 1;
                state.retry_pending.extend_from_slice(events);
                return Err(StorageError::Io(std::io::Error::other(
                    "injected best-effort record failure",
                )));
            }
            state.pending.extend_from_slice(events);
            Ok(())
        }

        fn record_recorded_batch(
            &mut self,
            events: &[RecordedRuntimeEvent],
        ) -> Result<(), StorageError> {
            let mut state = self.state.lock().unwrap();
            state.record_batch_calls.push(
                events
                    .iter()
                    .map(|event| event.event().event_type.clone())
                    .collect(),
            );
            let best_effort_only = events
                .iter()
                .all(|event| event.event().event_type == "book");
            if best_effort_only && state.best_effort_record_failures_remaining > 0 {
                state.best_effort_record_failures_remaining -= 1;
                state
                    .retry_pending
                    .extend(events.iter().map(|event| event.event().clone()));
                state
                    .retry_sequences
                    .extend(events.iter().map(RecordedRuntimeEvent::recorder_sequence));
                return Err(StorageError::Io(std::io::Error::other(
                    "injected best-effort record failure",
                )));
            }
            state
                .pending
                .extend(events.iter().map(|event| event.event().clone()));
            state
                .pending_sequences
                .extend(events.iter().map(RecordedRuntimeEvent::recorder_sequence));
            Ok(())
        }

        fn flush(&mut self) -> Result<(), StorageError> {
            let mut state = self.state.lock().unwrap();
            let mut retry_pending = std::mem::take(&mut state.retry_pending);
            let mut retry_sequences = std::mem::take(&mut state.retry_sequences);
            state.pending.append(&mut retry_pending);
            state.pending_sequences.append(&mut retry_sequences);
            if state.retry_flush_failures_remaining > 0 {
                state.retry_flush_failures_remaining -= 1;
                return Err(StorageError::Io(std::io::Error::other(
                    "injected retry flush failure",
                )));
            }
            let durable_pending = state.pending.iter().any(|event| event.event_type != "book");
            if durable_pending && state.durable_flush_failures_remaining > 0 {
                state.durable_flush_failures_remaining -= 1;
                return Err(StorageError::Io(std::io::Error::other(
                    "injected durable flush failure",
                )));
            }
            if !state.pending.is_empty() {
                let event_types = state
                    .pending
                    .iter()
                    .map(|event| event.event_type.clone())
                    .collect();
                let sequences = std::mem::take(&mut state.pending_sequences);
                state.committed_event_types.push(event_types);
                state.committed_sequences.push(sequences);
                state.pending.clear();
            }
            Ok(())
        }
    }

    #[test]
    fn runtime_provenance_binds_shadow_safety_candidate_and_code() {
        let mut settings = RuntimeSettings::default();
        settings.deploy.runtime_role = polyedge_config::RuntimeRole::ProfitabilityShadow;
        settings.paper.maker_fill_policy = "none".to_owned();
        settings.strategy.adaptive_regime_enabled = true;
        settings.strategy.adaptive_regime_mode = "dynamic_quote_style".to_owned();
        settings.azure.publish_strategy_canary_intents = true;
        settings.azure.storage_container_name = "polyedge-shadow-events".to_owned();
        settings.azure.event_blob_prefix = "shadow-events/test".to_owned();
        settings.azure.compact_shadow_recording = true;
        settings.azure.shadow_book_sample_ms = 1_000;

        let payload =
            runtime_provenance_with_git_sha(&settings, "c40d9093783808b010eabd9c43697e9dcceb667b")
                .expect("valid provenance");
        assert_eq!(payload["runtime_role"], "profitability_shadow");
        assert_eq!(payload["shadow_only"], true);
        assert_eq!(payload["allow_live"], false);
        assert_eq!(payload["paper_maker_fill_policy"], "none");
        assert_eq!(payload["candidate"]["name"], "dynamic_quote_style");
        assert_eq!(payload["compact_shadow_recording"], true);
        assert_eq!(payload["shadow_book_sample_ms"], 1_000);
        assert_eq!(payload["authoritative_recorder_backend"], "local_jsonl");
        assert!(payload["storage_account"].is_null());
        assert_eq!(
            payload["decision_pipeline_schema"],
            "polyedge.strategy_decision_batch.v4"
        );
        assert_eq!(
            payload["decision_pipeline_parity_scope"],
            "full_decision_pipeline_recomputation"
        );
        assert_eq!(
            payload["decision_config_schema"],
            "polyedge.decision_config.v1"
        );
        assert_eq!(
            payload["decision_config_sha256"],
            decision_config_sha256(&settings, Some(FrozenStrategyMode::DynamicQuoteStyle))
        );
        assert!(payload["candidate"]["config_hash"]
            .as_str()
            .is_some_and(|value| value.starts_with("sha256:")));
        assert_eq!(
            payload["git_sha"],
            "c40d9093783808b010eabd9c43697e9dcceb667b"
        );
        assert!(payload["runtime_config_hash"]
            .as_str()
            .is_some_and(|value| value.starts_with("sha256:") && value.len() == 71));
    }

    #[test]
    fn runtime_provenance_and_decision_hash_bind_event_time_prefix_cutover() {
        let mut settings = RuntimeSettings::default();
        settings.azure.event_blob_prefix = "shadow-events/old".to_owned();
        settings.azure.event_blob_prefix_after_cutover = Some("shadow-events/new".to_owned());
        let cutover = DateTime::parse_from_rfc3339("2026-07-22T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        settings.azure.event_blob_prefix_cutover_utc = Some(cutover);
        let git_sha = "c40d9093783808b010eabd9c43697e9dcceb667b";

        let before = runtime_provenance_with_git_sha_at(
            &settings,
            git_sha,
            cutover - chrono::Duration::milliseconds(1),
        )
        .unwrap();
        let after = runtime_provenance_with_git_sha_at(&settings, git_sha, cutover).unwrap();
        assert_eq!(before["event_blob_prefix"], "shadow-events/old");
        assert_eq!(after["event_blob_prefix"], "shadow-events/new");
        assert_eq!(
            after["event_blob_prefix_routing"]["before_cutover"],
            "shadow-events/old"
        );
        assert_eq!(
            after["event_blob_prefix_routing"]["after_cutover"],
            "shadow-events/new"
        );
        assert_eq!(
            after["event_blob_prefix_routing"]["cutover_utc"],
            json!(cutover)
        );

        let bound_hash = decision_config_sha256(&settings, None);
        settings.azure.event_blob_prefix_after_cutover = Some("shadow-events/different".to_owned());
        assert_ne!(bound_hash, decision_config_sha256(&settings, None));
    }

    #[test]
    fn decision_pipeline_settings_remove_secrets_before_hashing_or_recording() {
        let mut settings = RuntimeSettings::default();
        settings.deploy.api_bearer_token = Some("do-not-record-bearer".to_owned());
        settings.target.chainlink_api_key = Some("do-not-record-chainlink-key".to_owned());
        settings.live.polymarket_private_key = Some("do-not-record-private-key".to_owned());

        let safe = secret_safe_pipeline_settings(&settings);
        assert!(safe.deploy.api_bearer_token.is_none());
        assert!(safe.target.chainlink_api_key.is_none());
        assert!(safe.live.polymarket_private_key.is_none());
        assert_eq!(safe.strategy.maker_margin, settings.strategy.maker_margin);
        assert_eq!(safe.risk.max_order_size, settings.risk.max_order_size);
        let serialized = serde_json::to_string(&safe).unwrap();
        assert!(!serialized.contains("do-not-record"));

        let baseline = decision_config_sha256(&settings, None);
        let mut secret_only = settings.clone();
        secret_only.live.polymarket_private_key = Some("another-secret".to_owned());
        secret_only.target.chainlink_api_key = Some("another-api-key".to_owned());
        assert_eq!(decision_config_sha256(&secret_only, None), baseline);
        let mut strategy_change = settings.clone();
        strategy_change.strategy.maker_margin += Decimal::new(1, 4);
        assert_ne!(decision_config_sha256(&strategy_change, None), baseline);
        let mut risk_change = settings.clone();
        risk_change.risk.max_order_size += Decimal::ONE;
        assert_ne!(decision_config_sha256(&risk_change, None), baseline);
        let mut execution_change = settings.clone();
        execution_change.paper.order_live_after_ms += 1;
        assert_ne!(decision_config_sha256(&execution_change, None), baseline);
        let mut target_change = settings.clone();
        target_change.target.discovery_limit += 1;
        assert_ne!(decision_config_sha256(&target_change, None), baseline);
        let mut reference_policy_change = settings.clone();
        reference_policy_change
            .target
            .start_price_capture_grace_seconds += 1.0;
        assert_ne!(
            decision_config_sha256(&reference_policy_change, None),
            baseline
        );
        let mut data_policy_change = settings.clone();
        data_policy_change.azure.shadow_book_sample_ms += 1;
        assert_ne!(decision_config_sha256(&data_policy_change, None), baseline);
        let mut safety_change = settings;
        safety_change.live.allow_live = !safety_change.live.allow_live;
        assert_ne!(decision_config_sha256(&safety_change, None), baseline);
    }

    #[test]
    fn compact_recorded_book_keeps_replay_top_of_book_without_full_depth() {
        let book = BookState {
            token_id: TokenId::new("token"),
            bids: vec![
                BookLevel {
                    price: Decimal::new(50, 2),
                    size: Decimal::from(5),
                },
                BookLevel {
                    price: Decimal::new(49, 2),
                    size: Decimal::from(10),
                },
            ],
            asks: vec![
                BookLevel {
                    price: Decimal::new(51, 2),
                    size: Decimal::from(7),
                },
                BookLevel {
                    price: Decimal::new(52, 2),
                    size: Decimal::from(12),
                },
            ],
            last_trade_price: Some(Decimal::new(50, 2)),
            exchange_ts: None,
            local_ts: Utc::now(),
            book_hash: Some("hash".to_owned()),
        };

        let compact = compact_recorded_book(&book);
        assert_eq!(compact.bids.len(), 1);
        assert_eq!(compact.asks.len(), 1);
        assert_eq!(compact.bids[0].price, Decimal::new(50, 2));
        assert_eq!(compact.asks[0].price, Decimal::new(51, 2));
    }

    #[test]
    fn pipeline_books_are_deterministically_scoped_to_market_tokens() {
        let now = Utc::now();
        let market = MarketSpec {
            asset: "BTC".to_owned(),
            horizon: "15m".to_owned(),
            event_id: None,
            event_slug: None,
            market_id: MarketId::new("scoped-books-market"),
            market_slug: None,
            condition_id: ConditionId::new("scoped-books-condition"),
            question: "BTC up?".to_owned(),
            description: None,
            up_token_id: TokenId::new("z-up-token"),
            down_token_id: TokenId::new("a-down-token"),
            start_ts: now,
            end_ts: now + chrono::Duration::minutes(15),
            start_price: Some(Decimal::from(100)),
            resolution_source: "chainlink_reference".to_owned(),
            tick_size: Decimal::new(1, 2),
            minimum_order_size: Decimal::from(5),
            neg_risk: false,
            fees_enabled: true,
            accepting_orders: true,
            status: MarketStatus::Tradeable,
            raw: BTreeMap::new(),
        };
        let book = |token: &str| BookState {
            token_id: TokenId::new(token),
            bids: Vec::new(),
            asks: Vec::new(),
            last_trade_price: None,
            exchange_ts: None,
            local_ts: now,
            book_hash: None,
        };
        let books = [
            (TokenId::new("unrelated-token"), book("unrelated-token")),
            (TokenId::new("z-up-token"), book("z-up-token")),
            (TokenId::new("a-down-token"), book("a-down-token")),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        let scoped = books_for_market(&market, &books);
        assert_eq!(scoped.len(), 2);
        assert_eq!(
            scoped.keys().map(ToString::to_string).collect::<Vec<_>>(),
            vec!["a-down-token".to_owned(), "z-up-token".to_owned()]
        );
        assert!(!scoped.contains_key(&TokenId::new("unrelated-token")));
    }

    #[test]
    fn funded_warmup_tracks_the_nearest_market_outside_the_final_six_minutes() {
        let now = Utc::now();
        let market = |id: &str, seconds_to_expiry: i64| MarketSpec {
            asset: "BTC".to_owned(),
            horizon: "15m".to_owned(),
            event_id: None,
            event_slug: None,
            market_id: MarketId::new(id),
            market_slug: None,
            condition_id: ConditionId::new(format!("{id}-condition")),
            question: "BTC up?".to_owned(),
            description: None,
            up_token_id: TokenId::new(format!("{id}-up")),
            down_token_id: TokenId::new(format!("{id}-down")),
            start_ts: now,
            end_ts: now + chrono::Duration::seconds(seconds_to_expiry),
            start_price: Some(Decimal::from(100)),
            resolution_source: "chainlink_reference".to_owned(),
            tick_size: Decimal::new(1, 2),
            minimum_order_size: Decimal::from(5),
            neg_risk: false,
            fees_enabled: true,
            accepting_orders: true,
            status: MarketStatus::Tradeable,
            raw: BTreeMap::new(),
        };
        let markets = [
            market("final-six", 300),
            market("active-window", 840),
            market("future", 1_740),
        ]
        .into_iter()
        .map(|market| (market.market_id.clone(), market))
        .collect::<BTreeMap<_, _>>();

        assert_eq!(
            select_funded_warmup_market(markets.values(), now, 360)
                .map(|market| market.market_id.to_string()),
            Some("active-window".to_owned())
        );
        assert_eq!(
            select_funded_warmup_market(
                markets.values(),
                now + chrono::Duration::seconds(500),
                360,
            )
            .map(|market| market.market_id.to_string()),
            Some("future".to_owned())
        );
    }

    #[tokio::test]
    async fn stale_decision_state_generation_rejects_data_or_engine_mutation() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let evaluated_generation = {
            let data = controller.inner.data.read().await;
            let engine = controller.inner.engine.lock().await;
            assert_eq!(
                stale_decision_state_generation(
                    data.decision_generation,
                    &engine,
                    DecisionStateGeneration {
                        data: data.decision_generation,
                        engine: engine.decision_generation,
                    }
                ),
                None
            );
            DecisionStateGeneration {
                data: data.decision_generation,
                engine: engine.decision_generation,
            }
        };
        {
            let _decision_guard = controller.inner.decision_gate.lock().await;
            let mut data = controller.inner.data.write().await;
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        let observed = {
            let data = controller.inner.data.read().await;
            let engine = controller.inner.engine.lock().await;
            stale_decision_state_generation(data.decision_generation, &engine, evaluated_generation)
                .unwrap()
        };
        assert_eq!(observed.data, evaluated_generation.data + 1);
        assert_eq!(observed.engine, evaluated_generation.engine);
        {
            let _decision_guard = controller.inner.decision_gate.lock().await;
            let mut engine = controller.inner.engine.lock().await;
            engine.decision_generation = engine.decision_generation.wrapping_add(1);
        }
        let data = controller.inner.data.read().await;
        let engine = controller.inner.engine.lock().await;
        let observed = stale_decision_state_generation(
            data.decision_generation,
            &engine,
            DecisionStateGeneration {
                data: data.decision_generation,
                engine: evaluated_generation.engine,
            },
        )
        .unwrap();
        assert_eq!(observed.engine, evaluated_generation.engine + 1);
    }

    #[tokio::test]
    async fn status_does_not_hold_data_while_waiting_for_engine() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let engine_guard = controller.inner.engine.lock().await;
        let status_controller = controller.clone();
        let status_task = tokio::spawn(async move { status_controller.status().await });

        // Give status a chance to block on the intentionally held engine lock.
        // A data-first implementation holds a read lock here and reproduces the
        // live telemetry/feed lock inversion.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let data_guard =
            tokio::time::timeout(Duration::from_millis(250), controller.inner.data.write())
                .await
                .expect("status must not hold data while it waits for the engine");
        drop(data_guard);
        drop(engine_guard);

        tokio::time::timeout(Duration::from_secs(1), status_task)
            .await
            .expect("status should complete after the engine is released")
            .expect("status task should not panic");
    }

    #[tokio::test]
    async fn decision_gate_prevents_control_mutation_during_compare_and_apply_window() {
        let controller = RuntimeController::new(RuntimeSettings::default());
        let apply_guard = controller.inner.decision_gate.lock().await;
        let (started_tx, started_rx) = oneshot::channel();
        let mutation_controller = controller.clone();
        let mutation = tokio::spawn(async move {
            let _ = started_tx.send(());
            mutation_controller
                .set_kill_switch(true, Some("race test".to_owned()))
                .await
        });
        started_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!mutation.is_finished());
        assert!(!controller.inner.data.read().await.kill_switch);

        drop(apply_guard);
        mutation.await.unwrap();
        assert!(controller.inner.data.read().await.kill_switch);
    }

    #[test]
    fn paper_application_proof_requires_a_successful_bound_place_report() {
        let decision = TradeDecision {
            action: DecisionAction::Place,
            market_id: MarketId::new("application-market"),
            condition_id: Some(ConditionId::new("application-condition")),
            token_id: Some(TokenId::new("application-token")),
            outcome: Some(Outcome::Up),
            side: Some(Side::Buy),
            price: Some(Decimal::new(50, 2)),
            size: Some(Decimal::from(7)),
            quote_amount: Some(Decimal::new(350, 2)),
            order_kind: Some(OrderKind::PostOnlyGtc),
            reason: "application proof test".to_owned(),
            ttl_ms: Some(60_000),
            expected_edge: Some(Decimal::new(2, 2)),
            post_only: true,
            tick_size: Some(Decimal::new(1, 2)),
            neg_risk: false,
        };
        let unbound = decision_event_payload(&decision, None, None);
        let binding = DecisionBatchBinding {
            batch_id: format!("strategy-batch-{}", "a".repeat(64)),
            output_index: 3,
            decision_sha256: value_sha256(&unbound),
        };
        let prepared = PreparedDecision {
            decision: decision.clone(),
            metadata: None,
            unbound_payload: unbound.clone(),
            payload: decision_event_payload(&decision, None, Some(&binding)),
            binding,
        };
        assert!(bind_applied_decision_output(&prepared, Vec::new()).is_none());

        let report = ExecutionReport {
            order_id: Some(OrderId::new("paper-restart-unique-1")),
            market_id: decision.market_id.clone(),
            token_id: decision.token_id.clone(),
            status: "paper_resting".to_owned(),
            filled_size: Decimal::ZERO,
            avg_price: None,
            fee: Decimal::ZERO,
            local_ts: Utc::now(),
            raw: BTreeMap::new(),
        };
        let applied = bind_applied_decision_output(&prepared, vec![report]).unwrap();
        assert_eq!(
            applied.application["schema"],
            "polyedge.paper_decision_output_application.v1"
        );
        assert_eq!(applied.application["applied"], true);
        assert_eq!(applied.application["order_id"], "paper-restart-unique-1");
        assert_eq!(applied.application["execution_report_count"], 1);
        assert_eq!(
            applied.reports[0].raw["decision_application"]["application_id"],
            applied.application["application_id"]
        );
    }

    #[test]
    fn shadow_persistence_filter_keeps_trades_and_bounded_books() {
        let mut settings = RuntimeSettings::default();
        settings.deploy.runtime_role = polyedge_config::RuntimeRole::ProfitabilityShadow;
        settings.azure.compact_shadow_recording = true;
        settings.azure.shadow_book_sample_ms = 1_000;
        let start = DateTime::parse_from_rfc3339("2026-07-14T00:00:00.100Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut filter = PersistenceFilter::default();

        assert!(filter.should_persist(
            &settings,
            "raw_market_event",
            &json!({"event_type": "price_change", "token_id": "up"}),
            start,
            false,
        ));
        assert!(!filter.should_persist(
            &settings,
            "raw_market_event",
            &json!({"event_type": "price_change", "token_id": "up"}),
            start + chrono::Duration::milliseconds(500),
            false,
        ));
        assert!(filter.should_persist(
            &settings,
            "raw_market_event",
            &json!({"event_type": "last_trade_price", "token_id": "up"}),
            start,
            false,
        ));
        assert!(filter.should_persist(&settings, "book", &json!({"token_id": "up"}), start, false,));
        assert!(!filter.should_persist(
            &settings,
            "book",
            &json!({"token_id": "up"}),
            start + chrono::Duration::milliseconds(500),
            false,
        ));
        assert!(filter.should_persist(
            &settings,
            "book",
            &json!({"token_id": "up"}),
            start + chrono::Duration::milliseconds(1_000),
            false,
        ));
        assert!(filter.should_persist(
            &settings,
            "book",
            &json!({"token_id": "up"}),
            start + chrono::Duration::milliseconds(500),
            true,
        ));
    }

    #[test]
    fn settlement_journal_hash_binds_the_full_ordered_unbound_journal() {
        let journal_id = "paper-settlement-journal-hash-test";
        let unbound = vec![
            (
                "paper_fill_markout_missing".to_owned(),
                json!({"fill_id": "fill-1"}),
            ),
            (
                "paper_settlement".to_owned(),
                json!({"market_id": "market-1"}),
            ),
        ];
        let projection = json!({
            "schema": "polyedge.paper_settlement_journal.v1",
            "settlement_journal_id": journal_id,
            "settlement_journal_event_count": 2,
            "events": [
                {"event_index": 0, "event_type": "paper_fill_markout_missing", "payload": {"fill_id": "fill-1"}},
                {"event_index": 1, "event_type": "paper_settlement", "payload": {"market_id": "market-1"}}
            ]
        });
        let expected_sha256 = value_sha256(&projection);

        let events = finalize_settlement_journal(journal_id, unbound);
        assert_eq!(events.len(), 2);
        for (event_index, (_, payload)) in events.iter().enumerate() {
            assert_eq!(payload["settlement_journal_event_index"], event_index);
            assert_eq!(payload["settlement_journal_event_count"], 2);
            assert_eq!(payload["settlement_journal_sha256"], expected_sha256);
            assert_eq!(
                payload["settlement_journal_schema"],
                "polyedge.paper_settlement_journal.v1"
            );
            assert_eq!(payload["settlement_journal_id"], journal_id);
        }

        let reversed = finalize_settlement_journal(
            journal_id,
            vec![
                (
                    "paper_settlement".to_owned(),
                    json!({"market_id": "market-1"}),
                ),
                (
                    "paper_fill_markout_missing".to_owned(),
                    json!({"fill_id": "fill-1"}),
                ),
            ],
        );
        assert_ne!(
            events[0].1["settlement_journal_sha256"],
            reversed[0].1["settlement_journal_sha256"]
        );
    }

    #[test]
    fn recorder_worker_serializes_burst_without_try_lock_drops() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-recorder-worker-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_path(path.clone())));
        let metrics = Arc::new(RecorderMetrics::default());
        let (sender, receiver) = std_mpsc::channel();
        spawn_recorder_worker(Arc::clone(&recorder), receiver, Arc::clone(&metrics));

        for index in 0..100 {
            metrics.queued.fetch_add(1, Ordering::Relaxed);
            metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
            sender
                .send(RecorderRequest::best_effort(
                    metrics
                        .bind(RuntimeEvent {
                            event_type: "book".to_owned(),
                            ts: Utc::now(),
                            data: json!({ "index": index }),
                        })
                        .unwrap(),
                ))
                .unwrap();
        }
        drop(sender);

        for _ in 0..100 {
            if metrics.queued.load(Ordering::Relaxed) == 0 {
                break;
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        assert_eq!(metrics.queued.load(Ordering::Relaxed), 0);

        let text = fs::read_to_string(&path).unwrap();
        let recorded = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(recorded.len(), 100);
        assert!(recorded.iter().all(|event| {
            event["recorder_instance_id"] == metrics.snapshot()["recorder_instance_id"]
        }));
        assert_eq!(
            recorded
                .iter()
                .map(|event| event["recorder_sequence"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            (1..=100).collect::<Vec<_>>()
        );
        assert_eq!(recorder.lock().unwrap().status(false)["error_count"], 0);
        assert_eq!(metrics.snapshot()["enqueued_total"], 100);
        assert_eq!(metrics.snapshot()["persisted_total"], 100);
        assert_eq!(metrics.snapshot()["failed_total"], 0);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn recorder_queue_rejection_rolls_back_an_unentered_tail() {
        let metrics = RecorderMetrics::default();
        let event = || RuntimeEvent {
            event_type: "book".to_owned(),
            ts: Utc::now(),
            data: json!({}),
        };
        let first = metrics.bind(event()).unwrap();
        let (sender, receiver) = std_mpsc::channel();
        drop(receiver);
        let rejected = match sender.send(RecorderRequest::best_effort(first)) {
            Ok(()) => panic!("recorder queue unexpectedly accepted a request"),
            Err(error) => error.0,
        };
        assert!(metrics.rollback_bound_tail(&rejected.events));
        let second = metrics.bind(event()).unwrap();
        assert_eq!(second.recorder_sequence(), 1);
        let status = metrics.snapshot();
        assert!(uuid::Uuid::parse_str(status["recorder_instance_id"].as_str().unwrap()).is_ok());
        assert_eq!(status["last_assigned_sequence"], 1);
    }

    #[tokio::test]
    async fn concurrent_recorder_enqueues_preserve_sequence_order() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-recorder-order-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_path(path.clone()),
        );
        let first = controller.record_event("book", json!({"index": 1}), None, None);
        let second = controller.record_event("book", json!({"index": 2}), None, None);
        tokio::join!(first, second);
        controller.shutdown().await.unwrap();
        let sequences = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()["recorder_sequence"]
                    .as_u64()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2]);
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn recorder_admission_yields_while_capacity_is_held() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            best_effort_record_failures_remaining: 1,
            retry_flush_failures_remaining: usize::MAX,
            ..BufferedRecorderTestState::default()
        }));
        let controller = RuntimeController::new_with_recorder_and_capacity(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
            1,
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["failed_total"] == 1 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }

        let (started_tx, started_rx) = oneshot::channel();
        let waiting_controller = controller.clone();
        let waiting = tokio::spawn(async move {
            let _ = started_tx.send(());
            waiting_controller
                .record_event("book", json!({"sequence": 2}), None, None)
                .await;
        });
        started_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        let heartbeat = tokio::time::timeout(
            StdDuration::from_millis(100),
            tokio::time::sleep(StdDuration::from_millis(1)),
        )
        .await;
        assert!(heartbeat.is_ok());

        state.lock().unwrap().retry_flush_failures_remaining = 0;
        tokio::time::timeout(StdDuration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap();
        controller.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn qset_v4_retirement_failure_stays_fenced_and_retryable() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            best_effort_record_failures_remaining: 1,
            retry_flush_failures_remaining: usize::MAX,
            ..BufferedRecorderTestState::default()
        }));
        let mut settings = RuntimeSettings::default();
        settings.deploy.app_name = QSET_V4_APP_NAME.to_owned();
        settings.azure.storage_container_name = QSET_V4_RAW_CONTAINER.to_owned();
        let controller = RuntimeController::new_with_recorder_and_capacity(
            settings,
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
            1,
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["failed_total"] == 1 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        let (started_tx, started_rx) = oneshot::channel();
        let waiting_controller = controller.clone();
        let waiting = tokio::spawn(async move {
            let _ = started_tx.send(());
            waiting_controller
                .record_event("book", json!({"sequence": 2}), None, None)
                .await;
        });
        started_rx.await.unwrap();

        let shutdown = tokio::time::timeout(
            RECORDER_SHUTDOWN_DRAIN_TIMEOUT + StdDuration::from_secs(1),
            controller.prepare_qset_v4_retirement(),
        )
        .await
        .unwrap();
        assert!(shutdown.is_err());
        waiting.await.unwrap();
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_assigned_sequence"],
            1
        );

        state.lock().unwrap().retry_flush_failures_remaining = 0;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["queued"] == 0 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        controller.shutdown().await.unwrap();
        let receipt = controller
            .qset_v4_retirement_receipt(format!("sha256:{}", "b".repeat(64)), "a".repeat(40))
            .unwrap();
        assert_eq!(receipt.final_assigned_sequence, 1);
        assert_eq!(receipt.final_persisted_sequence, 1);
    }

    #[tokio::test]
    async fn qset_v5_retirement_failure_stays_fenced_and_retryable() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            best_effort_record_failures_remaining: 1,
            retry_flush_failures_remaining: usize::MAX,
            ..BufferedRecorderTestState::default()
        }));
        let mut settings = RuntimeSettings::default();
        settings.deploy.app_name = QSET_V5_APP_NAME.to_owned();
        settings.azure.storage_container_name = QSET_V5_RAW_CONTAINER.to_owned();
        let controller = RuntimeController::new_with_recorder_and_capacity(
            settings,
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
            1,
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["failed_total"] == 1 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        let (started_tx, started_rx) = oneshot::channel();
        let waiting_controller = controller.clone();
        let waiting = tokio::spawn(async move {
            let _ = started_tx.send(());
            waiting_controller
                .record_event("book", json!({"sequence": 2}), None, None)
                .await;
        });
        started_rx.await.unwrap();

        let shutdown = tokio::time::timeout(
            RECORDER_SHUTDOWN_DRAIN_TIMEOUT + StdDuration::from_secs(1),
            controller.prepare_qset_v5_retirement(),
        )
        .await
        .unwrap();
        assert!(shutdown.is_err());
        waiting.await.unwrap();
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_assigned_sequence"],
            1
        );

        state.lock().unwrap().retry_flush_failures_remaining = 0;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["queued"] == 0 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        controller.shutdown().await.unwrap();
        let receipt = controller
            .qset_v5_retirement_receipt(format!("sha256:{}", "b".repeat(64)), "a".repeat(40))
            .unwrap();
        assert_eq!(receipt.final_assigned_sequence, 1);
        assert_eq!(receipt.final_persisted_sequence, 1);
    }

    #[tokio::test]
    async fn qset_v6_retirement_failure_stays_fenced_and_retryable() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            best_effort_record_failures_remaining: 1,
            retry_flush_failures_remaining: usize::MAX,
            ..BufferedRecorderTestState::default()
        }));
        let mut settings = RuntimeSettings::default();
        settings.deploy.app_name = QSET_V6_APP_NAME.to_owned();
        settings.azure.storage_container_name = QSET_V6_RAW_CONTAINER.to_owned();
        let controller = RuntimeController::new_with_recorder_and_capacity(
            settings,
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
            1,
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["failed_total"] == 1 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        let (started_tx, started_rx) = oneshot::channel();
        let waiting_controller = controller.clone();
        let waiting = tokio::spawn(async move {
            let _ = started_tx.send(());
            waiting_controller
                .record_event("book", json!({"sequence": 2}), None, None)
                .await;
        });
        started_rx.await.unwrap();

        let shutdown = tokio::time::timeout(
            RECORDER_SHUTDOWN_DRAIN_TIMEOUT + StdDuration::from_secs(1),
            controller.prepare_qset_v6_retirement(),
        )
        .await
        .unwrap();
        assert!(shutdown.is_err());
        waiting.await.unwrap();
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_assigned_sequence"],
            1
        );

        state.lock().unwrap().retry_flush_failures_remaining = 0;
        for _ in 0..100 {
            if controller.inner.recorder_metrics.snapshot()["queued"] == 0 {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        controller.shutdown().await.unwrap();
        let receipt = controller
            .qset_v6_retirement_receipt(format!("sha256:{}", "b".repeat(64)), "a".repeat(40))
            .unwrap();
        assert_eq!(receipt.final_assigned_sequence, 1);
        assert_eq!(receipt.final_persisted_sequence, 1);
    }

    #[tokio::test]
    async fn shutdown_drains_and_flushes_the_recorder_queue() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState::default()));
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;

        controller.shutdown().await.unwrap();

        assert_eq!(controller.inner.recorder_metrics.snapshot()["queued"], 0);
        assert_eq!(
            state.lock().unwrap().committed_event_types,
            vec![vec!["book"]]
        );
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_persisted_sequence"],
            1
        );
    }

    #[tokio::test]
    async fn qset_v4_retirement_receipt_proves_the_closed_durable_waterline() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState::default()));
        let mut settings = RuntimeSettings::default();
        settings.deploy.app_name = QSET_V4_APP_NAME.to_owned();
        settings.azure.storage_container_name = QSET_V4_RAW_CONTAINER.to_owned();
        let controller = RuntimeController::new_with_recorder(
            settings,
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;

        controller.shutdown().await.unwrap();
        let receipt = controller
            .qset_v4_retirement_receipt(format!("sha256:{}", "b".repeat(64)), "a".repeat(40))
            .unwrap();

        assert_eq!(
            receipt.schema,
            "polyedge.qset_v4_writer_retirement_receipt.v1"
        );
        assert_eq!(receipt.status, "prepared_for_retirement");
        assert_eq!(receipt.campaign_id, QSET_V4_CAMPAIGN_ID);
        assert_eq!(receipt.app_name, QSET_V4_APP_NAME);
        assert_eq!(receipt.final_assigned_sequence, 1);
        assert_eq!(receipt.final_enqueued_sequence, 1);
        assert_eq!(receipt.final_enqueued_total, 1);
        assert_eq!(receipt.final_persisted_sequence, 1);
        assert_eq!(receipt.final_persisted_total, 1);
        assert_eq!(receipt.final_queued, 0);
        assert!(receipt.flush_success);

        controller
            .record_event("book", json!({"sequence": 2}), None, None)
            .await;
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_assigned_sequence"],
            1
        );
    }

    #[tokio::test]
    async fn qset_v5_retirement_receipt_proves_the_closed_durable_waterline() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState::default()));
        let mut settings = RuntimeSettings::default();
        settings.deploy.app_name = QSET_V5_APP_NAME.to_owned();
        settings.azure.storage_container_name = QSET_V5_RAW_CONTAINER.to_owned();
        let controller = RuntimeController::new_with_recorder(
            settings,
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;

        controller.shutdown().await.unwrap();
        let receipt = controller
            .qset_v5_retirement_receipt(format!("sha256:{}", "b".repeat(64)), "a".repeat(40))
            .unwrap();

        assert_eq!(
            receipt.schema,
            "polyedge.qset_v5_writer_retirement_receipt.v1"
        );
        assert_eq!(receipt.status, "prepared_for_retirement");
        assert_eq!(receipt.campaign_id, QSET_V5_CAMPAIGN_ID);
        assert_eq!(receipt.app_name, QSET_V5_APP_NAME);
        assert_eq!(receipt.final_assigned_sequence, 1);
        assert_eq!(receipt.final_enqueued_sequence, 1);
        assert_eq!(receipt.final_enqueued_total, 1);
        assert_eq!(receipt.final_persisted_sequence, 1);
        assert_eq!(receipt.final_persisted_total, 1);
        assert_eq!(receipt.final_queued, 0);
        assert!(receipt.flush_success);

        controller
            .record_event("book", json!({"sequence": 2}), None, None)
            .await;
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_assigned_sequence"],
            1
        );
    }

    #[tokio::test]
    async fn qset_v6_retirement_receipt_proves_the_closed_durable_waterline() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState::default()));
        let mut settings = RuntimeSettings::default();
        settings.deploy.app_name = QSET_V6_APP_NAME.to_owned();
        settings.azure.storage_container_name = QSET_V6_RAW_CONTAINER.to_owned();
        let controller = RuntimeController::new_with_recorder(
            settings,
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );
        controller
            .record_event("book", json!({"sequence": 1}), None, None)
            .await;

        controller.shutdown().await.unwrap();
        let receipt = controller
            .qset_v6_retirement_receipt(format!("sha256:{}", "b".repeat(64)), "a".repeat(40))
            .unwrap();

        assert_eq!(
            receipt.schema,
            "polyedge.qset_v6_writer_retirement_receipt.v1"
        );
        assert_eq!(receipt.status, "prepared_for_retirement");
        assert_eq!(receipt.campaign_id, QSET_V6_CAMPAIGN_ID);
        assert_eq!(receipt.app_name, QSET_V6_APP_NAME);
        assert_eq!(receipt.final_assigned_sequence, 1);
        assert_eq!(receipt.final_enqueued_sequence, 1);
        assert_eq!(receipt.final_enqueued_total, 1);
        assert_eq!(receipt.final_persisted_sequence, 1);
        assert_eq!(receipt.final_persisted_total, 1);
        assert_eq!(receipt.final_queued, 0);
        assert!(receipt.flush_success);

        controller
            .record_event("book", json!({"sequence": 2}), None, None)
            .await;
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["last_assigned_sequence"],
            1
        );
    }

    #[test]
    fn qset_v4_receipt_requires_exact_frozen_identity() {
        assert_eq!(
            qset_v4_image_digest_from(&format!("registry/polyedge@sha256:{}", "a".repeat(64))),
            Some(format!("sha256:{}", "a".repeat(64)))
        );
        assert!(
            qset_v4_image_digest_from(&format!("registry/polyedge@sha256:{}", "A".repeat(64)))
                .is_none()
        );
        assert!(qset_v4_image_digest_from("user:secret@registry/polyedge:latest").is_none());
        assert!(qset_v4_image_digest_from("registry/polyedge@sha256:not-a-digest").is_none());
        let revision = "a".repeat(40);
        assert_eq!(
            qset_v4_source_revision_from(&revision, Some(&revision)).unwrap(),
            revision
        );
        assert!(qset_v4_source_revision_from(&"A".repeat(40), None).is_err());
        assert!(qset_v4_source_revision_from(&"a".repeat(40), Some(&"b".repeat(40))).is_err());
    }

    #[test]
    fn qset_v5_receipt_requires_exact_frozen_identity() {
        assert_eq!(
            qset_v5_image_digest_from(&format!("registry/polyedge@sha256:{}", "a".repeat(64))),
            Some(format!("sha256:{}", "a".repeat(64)))
        );
        assert!(
            qset_v5_image_digest_from(&format!("registry/polyedge@sha256:{}", "A".repeat(64)))
                .is_none()
        );
        assert!(qset_v5_image_digest_from("user:secret@registry/polyedge:latest").is_none());
        assert!(qset_v5_image_digest_from("registry/polyedge@sha256:not-a-digest").is_none());
        let revision = "a".repeat(40);
        assert_eq!(
            qset_v5_source_revision_from(&revision, Some(&revision)).unwrap(),
            revision
        );
        assert!(qset_v5_source_revision_from(&"A".repeat(40), None).is_err());
        assert!(qset_v5_source_revision_from(&"a".repeat(40), Some(&"b".repeat(40))).is_err());
    }

    #[test]
    fn qset_v6_receipt_requires_exact_frozen_identity() {
        assert_eq!(
            qset_v6_image_digest_from(&format!("registry/polyedge@sha256:{}", "a".repeat(64))),
            Some(format!("sha256:{}", "a".repeat(64)))
        );
        assert!(
            qset_v6_image_digest_from(&format!("registry/polyedge@sha256:{}", "A".repeat(64)))
                .is_none()
        );
        assert!(qset_v6_image_digest_from("user:secret@registry/polyedge:latest").is_none());
        assert!(qset_v6_image_digest_from("registry/polyedge@sha256:not-a-digest").is_none());
        let revision = "a".repeat(40);
        assert_eq!(
            qset_v6_source_revision_from(&revision, Some(&revision)).unwrap(),
            revision
        );
        assert!(qset_v6_source_revision_from(&"A".repeat(40), None).is_err());
        assert!(qset_v6_source_revision_from(&"a".repeat(40), Some(&"b".repeat(40))).is_err());
    }

    #[test]
    fn qset_v7_receipt_requires_exact_frozen_identity() {
        assert_eq!(
            qset_v7_image_digest_from(&format!("registry/polyedge@sha256:{}", "a".repeat(64))),
            Some(format!("sha256:{}", "a".repeat(64)))
        );
        assert!(
            qset_v7_image_digest_from(&format!("registry/polyedge@sha256:{}", "A".repeat(64)))
                .is_none()
        );
        assert!(qset_v7_image_digest_from("user:secret@registry/polyedge:latest").is_none());
        assert!(qset_v7_image_digest_from("registry/polyedge@sha256:not-a-digest").is_none());
        let revision = "a".repeat(40);
        assert_eq!(
            qset_v7_source_revision_from(&revision, Some(&revision)).unwrap(),
            revision
        );
        assert!(qset_v7_source_revision_from(&"A".repeat(40), None).is_err());
        assert!(qset_v7_source_revision_from(&"a".repeat(40), Some(&"b".repeat(40))).is_err());
    }

    #[test]
    fn best_effort_failure_retries_before_later_requests_and_durable() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            best_effort_record_failures_remaining: 1,
            ..BufferedRecorderTestState::default()
        }));
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_test_recorder(
            Box::new(BufferedRecorderTestDouble {
                state: Arc::clone(&state),
            }),
            true,
        )));
        let metrics = Arc::new(RecorderMetrics::default());
        let (sender, receiver) = std_mpsc::channel();
        spawn_recorder_worker(Arc::clone(&recorder), receiver, Arc::clone(&metrics));
        metrics.queued.fetch_add(1, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        sender
            .send(RecorderRequest::best_effort(
                metrics
                    .bind(RuntimeEvent {
                        event_type: "book".to_owned(),
                        ts: Utc::now(),
                        data: json!({"sequence": 1}),
                    })
                    .unwrap(),
            ))
            .unwrap();

        for _ in 0..100 {
            if metrics.snapshot()["failed_total"] == 1 {
                break;
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        assert_eq!(metrics.snapshot()["failed_total"], 1);
        assert_eq!(metrics.snapshot()["queued"], 1);
        assert_eq!(metrics.snapshot()["persisted_total"], 0);
        assert_eq!(metrics.snapshot()["flush_unrecovered"], true);
        assert_eq!(metrics.snapshot()["flush_failed_total"], 1);
        assert!(state.lock().unwrap().pending.is_empty());

        metrics.queued.fetch_add(1, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        sender
            .send(RecorderRequest::best_effort(
                metrics
                    .bind(RuntimeEvent {
                        event_type: "book".to_owned(),
                        ts: Utc::now(),
                        data: json!({"sequence": 2}),
                    })
                    .unwrap(),
            ))
            .unwrap();
        let (ack_tx, ack_rx) = oneshot::channel();
        metrics.queued.fetch_add(1, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        sender
            .send(RecorderRequest::durable(
                vec![metrics
                    .bind(RuntimeEvent {
                        event_type: "required_evidence".to_owned(),
                        ts: Utc::now(),
                        data: json!({"journal_id": "after-recovered-books"}),
                    })
                    .unwrap()],
                ack_tx,
            ))
            .unwrap();

        assert_eq!(ack_rx.blocking_recv().unwrap(), Ok(()));
        drop(sender);
        for _ in 0..100 {
            if metrics.snapshot()["queued"] == 0 {
                break;
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        assert_eq!(metrics.snapshot()["queued"], 0);
        assert_eq!(metrics.snapshot()["flush_unrecovered"], false);
        assert_eq!(metrics.snapshot()["flush_recovered_total"], 1);
        assert_eq!(metrics.snapshot()["persisted_total"], 3);
        let state = state.lock().unwrap();
        assert_eq!(state.committed_sequences, vec![vec![1], vec![2, 3]]);
        assert_eq!(
            state
                .record_batch_calls
                .iter()
                .filter(|events| events.as_slice() == ["book"])
                .count(),
            2
        );
        assert_eq!(
            state.committed_event_types,
            vec![
                vec!["book".to_owned()],
                vec!["book".to_owned(), "required_evidence".to_owned()]
            ]
        );
    }

    #[test]
    fn durable_recorder_retries_the_same_bound_request_without_restaging() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            durable_flush_failures_remaining: 1,
            ..BufferedRecorderTestState::default()
        }));
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_test_recorder(
            Box::new(BufferedRecorderTestDouble {
                state: Arc::clone(&state),
            }),
            true,
        )));
        let metrics = Arc::new(RecorderMetrics::default());
        let (sender, receiver) = std_mpsc::channel();
        spawn_recorder_worker(Arc::clone(&recorder), receiver, Arc::clone(&metrics));
        let event = RuntimeEvent {
            event_type: "required_evidence".to_owned(),
            ts: Utc::now(),
            data: json!({"journal_id": "stable-journal-1"}),
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        metrics.queued.fetch_add(1, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        sender
            .send(RecorderRequest::durable(
                vec![metrics.bind(event).unwrap()],
                ack_tx,
            ))
            .unwrap();
        assert_eq!(ack_rx.blocking_recv().unwrap(), Ok(()));
        drop(sender);

        let state = state.lock().unwrap();
        assert_eq!(state.committed_sequences, vec![vec![1]]);
        assert_eq!(state.record_batch_calls, vec![vec!["required_evidence"]]);
        assert_eq!(metrics.snapshot()["persisted_total"], 1);
        assert_eq!(metrics.snapshot()["failed_total"], 1);
        assert_eq!(metrics.snapshot()["last_assigned_sequence"], 1);
    }

    #[test]
    fn durable_requests_commit_buffered_best_effort_with_the_journal() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState::default()));
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_test_recorder(
            Box::new(BufferedRecorderTestDouble {
                state: Arc::clone(&state),
            }),
            true,
        )));
        let metrics = Arc::new(RecorderMetrics::default());
        let (sender, receiver) = std_mpsc::channel();
        let best_effort = RuntimeEvent {
            event_type: "book".to_owned(),
            ts: Utc::now(),
            data: json!({"sequence": 1}),
        };
        let durable = RuntimeEvent {
            event_type: "required_evidence".to_owned(),
            ts: Utc::now(),
            data: json!({"journal_id": "exclusive-journal-1"}),
        };
        metrics.queued.fetch_add(2, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(2, Ordering::Relaxed);
        sender
            .send(RecorderRequest::best_effort(
                metrics.bind(best_effort).unwrap(),
            ))
            .unwrap();
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(RecorderRequest::durable(
                vec![metrics.bind(durable).unwrap()],
                ack_tx,
            ))
            .unwrap();
        spawn_recorder_worker(Arc::clone(&recorder), receiver, Arc::clone(&metrics));

        assert_eq!(ack_rx.blocking_recv().unwrap(), Ok(()));
        drop(sender);
        for _ in 0..100 {
            if metrics.snapshot()["queued"] == 0 {
                break;
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        assert_eq!(metrics.snapshot()["batches_total"], 2);
        assert_eq!(metrics.snapshot()["persisted_total"], 2);
        let state = state.lock().unwrap();
        assert_eq!(
            state.committed_event_types,
            vec![vec!["book".to_owned(), "required_evidence".to_owned()]]
        );
    }

    #[test]
    fn durable_request_recovers_a_failed_best_effort_flush_first() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            retry_flush_failures_remaining: 1,
            ..BufferedRecorderTestState::default()
        }));
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_test_recorder(
            Box::new(BufferedRecorderTestDouble {
                state: Arc::clone(&state),
            }),
            true,
        )));
        let metrics = Arc::new(RecorderMetrics::default());
        let book = metrics
            .bind(RuntimeEvent {
                event_type: "book".to_owned(),
                ts: Utc::now(),
                data: json!({"sequence": 1}),
            })
            .unwrap();
        recorder
            .lock()
            .unwrap()
            .record_recorded_batch(std::slice::from_ref(&book))
            .unwrap();
        assert!(recorder_flush_result(&recorder, &metrics, false).is_err());

        let durable = metrics
            .bind(RuntimeEvent {
                event_type: "required_evidence".to_owned(),
                ts: Utc::now(),
                data: json!({"journal_id": "after-failed-book-flush"}),
            })
            .unwrap();
        let mut durability = RecorderDurabilityState::default();
        assert_eq!(
            persist_durable_recorder_request(
                &recorder,
                &metrics,
                &mut durability,
                "after-failed-book-flush",
                std::slice::from_ref(&durable),
            ),
            Ok(())
        );

        assert_eq!(
            state.lock().unwrap().committed_event_types,
            vec![
                vec!["book".to_owned()],
                vec!["required_evidence".to_owned()]
            ]
        );
        assert_eq!(metrics.snapshot()["flush_unrecovered"], false);
        assert_eq!(metrics.snapshot()["flush_recovered_total"], 1);
    }

    #[test]
    fn concurrent_same_key_durable_requests_record_once() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState::default()));
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_test_recorder(
            Box::new(BufferedRecorderTestDouble {
                state: Arc::clone(&state),
            }),
            true,
        )));
        let metrics = Arc::new(RecorderMetrics::default());
        let (sender, receiver) = std_mpsc::channel();
        let durable = RuntimeEvent {
            event_type: "required_evidence".to_owned(),
            ts: Utc::now(),
            data: json!({"journal_id": "concurrent-same-key-journal-1"}),
        };
        let (first_ack_tx, first_ack_rx) = oneshot::channel();
        let (second_ack_tx, second_ack_rx) = oneshot::channel();
        metrics.queued.fetch_add(2, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(2, Ordering::Relaxed);
        sender
            .send(RecorderRequest::durable(
                vec![metrics.bind(durable.clone()).unwrap()],
                first_ack_tx,
            ))
            .unwrap();
        sender
            .send(RecorderRequest::durable(
                vec![metrics.bind(durable).unwrap()],
                second_ack_tx,
            ))
            .unwrap();
        spawn_recorder_worker(Arc::clone(&recorder), receiver, Arc::clone(&metrics));

        assert_eq!(first_ack_rx.blocking_recv().unwrap(), Ok(()));
        assert_eq!(second_ack_rx.blocking_recv().unwrap(), Ok(()));
        drop(sender);
        let state = state.lock().unwrap();
        assert_eq!(
            state
                .record_batch_calls
                .iter()
                .filter(|events| { events.len() == 1 && events[0].as_str() == "required_evidence" })
                .count(),
            1
        );
        assert_eq!(
            state
                .committed_event_types
                .iter()
                .flatten()
                .filter(|event_type| event_type.as_str() == "required_evidence")
                .count(),
            1
        );
    }

    #[test]
    fn startup_provenance_retries_the_staged_block_without_rerecording() {
        let state = Arc::new(StdMutex::new(BufferedRecorderTestState {
            durable_flush_failures_remaining: 2,
            ..BufferedRecorderTestState::default()
        }));
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_test_recorder(
                Box::new(BufferedRecorderTestDouble {
                    state: Arc::clone(&state),
                }),
                true,
            ),
        );

        controller
            .persist_startup_provenance(json!({"git_sha": "a".repeat(40)}))
            .unwrap();

        let state = state.lock().unwrap();
        assert_eq!(
            state
                .record_batch_calls
                .iter()
                .filter(|events| events.len() == 1 && events[0].as_str() == "runtime_provenance")
                .count(),
            1
        );
        assert_eq!(
            state
                .committed_event_types
                .iter()
                .flatten()
                .filter(|event_type| event_type.as_str() == "runtime_provenance")
                .count(),
            1
        );
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["failed_total"],
            2
        );
        assert_eq!(
            controller.inner.recorder_metrics.snapshot()["persisted_total"],
            1
        );
    }

    #[test]
    fn recorder_health_clears_only_the_exact_frozen_batch() {
        let metrics = RecorderMetrics::default();
        let event = |ts: &str| RuntimeEvent {
            event_type: "required_evidence".to_owned(),
            ts: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            data: json!({"journal_id": "stable-journal-1"}),
        };
        let first = vec![event("2026-07-22T02:17:23Z")];
        let changed_timestamp = vec![event("2026-07-22T02:17:24Z")];

        metrics.mark_durable_batch_unrecovered(&first);
        assert_eq!(metrics.snapshot()["unrecovered_durable_events"], 1);
        assert!(!metrics.mark_durable_batch_recovered(&changed_timestamp));
        assert_eq!(metrics.snapshot()["unrecovered_durable_events"], 1);
        assert!(metrics.mark_durable_batch_recovered(&first));
        assert_eq!(metrics.snapshot()["unrecovered_durable_events"], 0);
        assert_eq!(metrics.snapshot()["recovered_total"], 1);
    }

    #[tokio::test]
    async fn exact_start_evidence_is_frozen_and_retried_after_recorder_recovery() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-start-evidence-retry-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        fs::create_dir_all(&path).unwrap();
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_path(path.clone()),
        );
        let start_ts = Utc::now() - chrono::Duration::seconds(1);
        let market_id = MarketId::new("start-evidence-retry-market");
        let market = MarketSpec {
            asset: "BTC".to_owned(),
            horizon: "15m".to_owned(),
            event_id: None,
            event_slug: None,
            market_id: market_id.clone(),
            market_slug: None,
            condition_id: ConditionId::new("start-evidence-retry-condition"),
            question: "BTC up?".to_owned(),
            description: None,
            up_token_id: TokenId::new("start-evidence-retry-up"),
            down_token_id: TokenId::new("start-evidence-retry-down"),
            start_ts,
            end_ts: start_ts + chrono::Duration::minutes(15),
            start_price: None,
            resolution_source: "chainlink_reference".to_owned(),
            tick_size: Decimal::new(1, 2),
            minimum_order_size: Decimal::from(5),
            neg_risk: false,
            fees_enabled: true,
            accepting_orders: true,
            status: MarketStatus::Tradeable,
            raw: BTreeMap::new(),
        };
        {
            let mut data = controller.inner.data.write().await;
            data.markets.insert(market_id.clone(), market);
        }
        let reference = ReferencePrice {
            source: "chainlink_rtds".to_owned(),
            price: Decimal::from(100_000),
            source_ts: start_ts + chrono::Duration::seconds(1),
            local_ts: Utc::now(),
            latency_ms: 0.0,
            stale: false,
            exact_resolution_source: true,
            quality_flags: Vec::new(),
        };

        controller.capture_market_start_prices(&reference).await;
        let frozen_event = {
            let data = controller.inner.data.read().await;
            assert!(data.pending_market_start_events.contains_key(&market_id));
            assert!(!data.market_start_evidence_durable.contains(&market_id));
            data.pending_market_start_events[&market_id].clone()
        };

        fs::remove_dir_all(&path).unwrap();
        controller.retry_pending_market_start_events().await;
        {
            let data = controller.inner.data.read().await;
            assert!(!data.pending_market_start_events.contains_key(&market_id));
            assert!(data.market_start_evidence_durable.contains(&market_id));
        }
        let text = fs::read_to_string(&path).unwrap();
        let persisted: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(persisted["event_type"], "market_start_price");
        assert_eq!(persisted["recorded_ts"], json!(frozen_event.ts));
        assert_eq!(persisted["payload"], frozen_event.data);
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn paper_application_retains_frozen_journal_until_durable_retry_succeeds() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-application-ack-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        fs::create_dir_all(&path).unwrap();
        let controller = RuntimeController::new_with_recorder(
            RuntimeSettings::default(),
            RuntimeRecorder::new_for_path(path.clone()),
        );
        let frozen = json!({
            "schema": "polyedge.paper_decision_output_application.v1",
            "application_id": "paper-application-frozen",
            "execution_reports_sha256": format!("sha256:{}", "a".repeat(64))
        });
        let frozen_ts = Utc::now() - chrono::Duration::minutes(5);
        {
            let mut engine = controller.inner.engine.lock().await;
            engine.pending_decision_application = Some(PendingDecisionApplication {
                batch_id: "strategy-batch-frozen".to_owned(),
                events: vec![RuntimeEvent {
                    event_type: "paper_decision_output_applied".to_owned(),
                    ts: frozen_ts,
                    data: frozen.clone(),
                }],
                reports: Vec::new(),
            });
        }

        assert_eq!(
            controller.retry_pending_decision_application().await,
            PendingApplicationRetry::Retained
        );
        {
            let engine = controller.inner.engine.lock().await;
            assert_eq!(
                engine.pending_decision_application.as_ref().unwrap().events[0].data,
                frozen
            );
            assert_eq!(
                engine.pending_decision_application.as_ref().unwrap().events[0].ts,
                frozen_ts
            );
        }

        fs::remove_dir_all(&path).unwrap();
        assert_eq!(
            controller.retry_pending_decision_application().await,
            PendingApplicationRetry::Committed
        );
        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains("paper-application-frozen"));
        assert!(text.contains(&format!("sha256:{}", "a".repeat(64))));
        let persisted: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(
            persisted["recorded_ts"]
                .as_str()
                .unwrap()
                .parse::<DateTime<Utc>>()
                .unwrap(),
            frozen_ts
        );
        assert!(controller
            .inner
            .engine
            .lock()
            .await
            .pending_decision_application
            .is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn paper_settlement_retains_state_until_durable_ack_then_retries() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-settlement-ack-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        fs::create_dir_all(&path).unwrap();
        let settings = RuntimeSettings::default();
        let controller = RuntimeController::new_with_recorder(
            settings,
            RuntimeRecorder::new_for_path(path.clone()),
        );
        let end_ts = Utc::now() - chrono::Duration::seconds(1);
        let start_ts = end_ts - chrono::Duration::minutes(15);
        let market_id = MarketId::new("settlement-retry-market");
        let market = MarketSpec {
            asset: "BTC".to_owned(),
            horizon: "15m".to_owned(),
            event_id: None,
            event_slug: None,
            market_id: market_id.clone(),
            market_slug: Some("settlement-retry-market".to_owned()),
            condition_id: ConditionId::new("settlement-retry-condition"),
            question: "BTC up?".to_owned(),
            description: None,
            up_token_id: TokenId::new("settlement-up"),
            down_token_id: TokenId::new("settlement-down"),
            start_ts,
            end_ts,
            start_price: Some(Decimal::from(100)),
            resolution_source: "chainlink_reference".to_owned(),
            tick_size: Decimal::new(1, 2),
            minimum_order_size: Decimal::from(5),
            neg_risk: false,
            fees_enabled: true,
            accepting_orders: true,
            status: MarketStatus::Tradeable,
            raw: BTreeMap::new(),
        };
        let start_reference = ReferencePrice {
            source: "chainlink_rtds".to_owned(),
            price: Decimal::from(100),
            source_ts: start_ts,
            local_ts: start_ts,
            latency_ms: 0.0,
            stale: false,
            exact_resolution_source: true,
            quality_flags: Vec::new(),
        };
        {
            let mut data = controller.inner.data.write().await;
            data.markets.insert(market_id.clone(), market);
            data.market_start_references
                .insert(market_id.clone(), start_reference);
        }
        {
            let mut engine = controller.inner.engine.lock().await;
            engine.risk.on_execution_report(&ExecutionReport {
                order_id: None,
                market_id: market_id.clone(),
                token_id: Some(TokenId::new("settlement-up")),
                status: "paper_filled_maker".to_owned(),
                filled_size: Decimal::from(5),
                avg_price: Some(Decimal::new(50, 2)),
                fee: Decimal::ZERO,
                local_ts: end_ts - chrono::Duration::seconds(30),
                raw: BTreeMap::new(),
            });
        }
        let final_reference = ReferencePrice {
            source: "chainlink_rtds".to_owned(),
            price: Decimal::from(101),
            source_ts: end_ts,
            local_ts: end_ts,
            latency_ms: 0.0,
            stale: false,
            exact_resolution_source: true,
            quality_flags: Vec::new(),
        };

        controller.settle_finished_markets(&final_reference).await;
        {
            let data = controller.inner.data.read().await;
            assert!(!data.settled_markets.contains(&market_id));
        }
        {
            let engine = controller.inner.engine.lock().await;
            let mut risk_preview = engine.risk.clone();
            assert_eq!(risk_preview.clear_market(&market_id), Decimal::from(5));
            assert!(engine.pending_settlements.contains_key(&market_id));
        }

        fs::remove_dir_all(&path).unwrap();
        let mut late_reference = final_reference.clone();
        late_reference.price = Decimal::from(999);
        late_reference.source_ts = end_ts + chrono::Duration::minutes(5);
        late_reference.local_ts = late_reference.source_ts;
        controller.settle_finished_markets(&late_reference).await;
        {
            let data = controller.inner.data.read().await;
            assert!(data.settled_markets.contains(&market_id));
        }
        {
            let engine = controller.inner.engine.lock().await;
            let mut risk_preview = engine.risk.clone();
            assert_eq!(risk_preview.clear_market(&market_id), Decimal::ZERO);
            assert!(!engine.pending_settlements.contains_key(&market_id));
        }
        let text = fs::read_to_string(&path).unwrap();
        let settlement_events = text
            .lines()
            .filter(|line| line.contains("\"event_type\":\"paper_settlement\""))
            .count();
        assert_eq!(settlement_events, 1);
        assert!(text.contains("final_reference_exact_resolution_source"));
        assert!(text.contains("start_reference_exact_resolution_source"));
        let settlement = text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|event| event["event_type"] == "paper_settlement")
            .expect("settlement journal event");
        assert_eq!(settlement["payload"]["final_price"], "101");
        assert_eq!(settlement["payload"]["settlement_journal_event_count"], 1);
        assert!(settlement["payload"]["settlement_journal_sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:") && hash.len() == 71));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn strategy_batch_hashes_bind_the_wire_normalized_payload() {
        let inputs = [
            json!({
                "market_start_evidence": {"price": "100000.0000", "source": "λ-chainlink"},
                "negative_zero": -0.0,
                "subnormal": f64::from_bits(1),
                "large": 1.7976931348623157e308_f64,
                "small": 2.2250738585072014e-308_f64,
                "nested": {"z": [3, 2, 1], "a": {"β": true}}
            }),
            json!({
                "market_start_evidence": {"price": "0.500000", "source": "chainlink_rtds"},
                "negative_zero": 0.0,
                "subnormal": -f64::from_bits(1),
                "large": -1.0e250_f64,
                "small": 1.0e-250_f64,
                "nested": {"emoji": "🧪", "escaped": "line\nfeed"}
            }),
        ];

        for input in inputs {
            let input = wire_normalized_json(&input).unwrap();
            let output = wire_normalized_json(&json!({
                "risk_assessment": {"allowed": true, "reasons": []},
                "score": input["small"].clone(),
                "final_decisions": [{"action": "hold", "reason": "no edge Δ"}]
            }))
            .unwrap();
            let input_sha256 = value_sha256(&input);
            let output_sha256 = value_sha256(&output);
            let start_sha256 = value_sha256(&input["market_start_evidence"]);
            let decision = wire_normalized_json(&json!({
                "action": "hold",
                "reason": "no edge Δ",
                "score": output["score"].clone()
            }))
            .unwrap();
            let batch = json!({
                "batch_id": decision_batch_id_v4(&input_sha256),
                "pipeline_input_sha256": input_sha256,
                "pipeline_output_sha256": output_sha256,
                "market_start_evidence_sha256": start_sha256,
                "pipeline_input": input,
                "pipeline_output": output,
                "bound_final_decisions": [{
                    "output_index": 0,
                    "decision_sha256": value_sha256(&decision),
                    "decision": decision
                }]
            });
            let wire_batch = wire_normalized_json(&batch).unwrap();
            validate_decision_batch_content_bindings(&wire_batch).unwrap();
            let reparsed: Value =
                serde_json::from_slice(&serde_json::to_vec(&wire_batch).unwrap()).unwrap();
            validate_decision_batch_content_bindings(&reparsed).unwrap();

            let mut tampered = reparsed;
            tampered["pipeline_output"]["score"] = json!(42.0);
            assert_eq!(
                validate_decision_batch_content_bindings(&tampered),
                Err("pipeline output hash mismatch")
            );
        }
    }
}
