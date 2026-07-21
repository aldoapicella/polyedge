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
    build_execution_intent_with_model, resolve_execution_model, IntentPublisherConfig,
};
use execution_quality::{deterministic_probe, ExecutionQualityTracker};
use polyedge_config::{embedded_git_sha, ExecutionMode, RuntimeSettings};
use polyedge_domain::{
    BookState, DecisionAction, ExecutionReport, FairValue, MarketId, MarketSpec, ReferencePrice,
    RuntimeEvent, TokenId, TradeDecision,
};
use polyedge_engine::{
    evaluate_decision_pipeline_v3, DecisionPipelineInputV3, FrozenStrategyMode,
    LogReturnFairValueModel, MarketStartEvidenceV1, OrderManager, PaperFillEngine,
    RegimeBookSnapshot, RegimeClassifier, RegimeFeatureInput, RegimeReferencePoint,
    RestingMakerOrder, RiskManager, StrategyDecisionMetadata,
};
use polyedge_execution::{ExecutionClient, PaperExecutionClient};
use polyedge_feeds::{self, FeedEvent, FeedName};
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
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

const RECENT_LIMIT: usize = 1_000;
const HISTORY_LIMIT: usize = 500;
const CHART_HISTORY_LIMIT: usize = 2_000;
const RECORDER_BATCH_LIMIT: usize = 500;
const RECORDER_FLUSH_INTERVAL: Duration = Duration::from_secs(10);
const REQUIRED_RECORDER_ATTEMPTS: usize = 3;
const RUNTIME_PROVENANCE_INTERVAL: Duration = Duration::from_secs(60);
const EXACT_REFERENCE_HISTORY_LIMIT: usize = 1_200;
const PENDING_SETTLEMENT_RETENTION_SECONDS: i64 = 6 * 60 * 60;

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
    decision_gate: Mutex<()>,
    recorder: Arc<StdMutex<RuntimeRecorder>>,
    recorder_tx: std_mpsc::Sender<RecorderRequest>,
    recorder_metrics: Arc<RecorderMetrics>,
    persistence_filter: StdMutex<PersistenceFilter>,
    broadcaster: broadcast::Sender<RuntimeEvent>,
    started: AtomicBool,
}

#[derive(Debug, Default)]
struct RecorderMetrics {
    queued: AtomicUsize,
    enqueued_total: AtomicU64,
    persisted_total: AtomicU64,
    filtered_total: AtomicU64,
    failed_total: AtomicU64,
    batches_total: AtomicU64,
    last_batch_size: AtomicUsize,
}

#[derive(Clone, Debug)]
struct DecisionBatchBinding {
    batch_id: String,
    output_index: usize,
    decision_sha256: String,
}

#[derive(Clone, Debug)]
struct StrategyDecisionLineage {
    evaluation_index: usize,
    strategy_output_index: usize,
    decision: TradeDecision,
    metadata: StrategyDecisionMetadata,
}

#[derive(Clone, Copy, Debug)]
struct StrategyLineageBinding {
    evaluation_index: usize,
    strategy_output_index: usize,
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
    lineage: Option<StrategyLineageBinding>,
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
    events: Vec<(String, Value)>,
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
    events: Vec<RuntimeEvent>,
    durable_ack: Option<oneshot::Sender<Result<(), String>>>,
}

impl RecorderRequest {
    fn best_effort(event: RuntimeEvent) -> Self {
        Self {
            events: vec![event],
            durable_ack: None,
        }
    }

    fn durable(
        events: Vec<RuntimeEvent>,
        durable_ack: oneshot::Sender<Result<(), String>>,
    ) -> Self {
        Self {
            events,
            durable_ack: Some(durable_ack),
        }
    }
}

impl RecorderMetrics {
    fn snapshot(&self) -> Value {
        json!({
            "queued": self.queued.load(Ordering::Relaxed),
            "enqueued_total": self.enqueued_total.load(Ordering::Relaxed),
            "persisted_total": self.persisted_total.load(Ordering::Relaxed),
            "filtered_total": self.filtered_total.load(Ordering::Relaxed),
            "failed_total": self.failed_total.load(Ordering::Relaxed),
            "batches_total": self.batches_total.load(Ordering::Relaxed),
            "last_batch_size": self.last_batch_size.load(Ordering::Relaxed)
        })
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
    markets: BTreeMap<MarketId, MarketSpec>,
    books: BTreeMap<TokenId, BookState>,
    reference: Option<ReferencePrice>,
    exact_references: VecDeque<ReferencePrice>,
    market_start_references: BTreeMap<MarketId, ReferencePrice>,
    market_start_evidence_durable: BTreeSet<MarketId>,
    pending_market_start_events: BTreeMap<MarketId, Value>,
    fair_values: BTreeMap<MarketId, Value>,
    chart_samples: BTreeMap<MarketId, VecDeque<Value>>,
    chart_last_persisted_ms: BTreeMap<MarketId, i64>,
    decisions: VecDeque<TradeDecision>,
    execution_reports: VecDeque<ExecutionReport>,
    recent_events: VecDeque<RuntimeEvent>,
    settled_markets: Vec<MarketId>,
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

impl RuntimeController {
    pub fn new(settings: RuntimeSettings) -> Self {
        let recorder = RuntimeRecorder::new(&settings);
        Self::new_with_recorder(settings, recorder)
    }

    fn new_with_recorder(settings: RuntimeSettings, recorder: RuntimeRecorder) -> Self {
        let (broadcaster, _) = broadcast::channel(1_000);
        let data = RuntimeData {
            decision_generation: 0,
            started_at: Utc::now(),
            paused: false,
            pause_reason: None,
            paused_at: None,
            kill_switch: false,
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
                decision_gate: Mutex::new(()),
                recorder,
                recorder_tx,
                recorder_metrics,
                persistence_filter: StdMutex::new(PersistenceFilter::default()),
                broadcaster,
                started: AtomicBool::new(false),
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
        self.spawn_feed_event_loop(receiver);
        self.spawn_discovery_loop();
        self.spawn_strategy_loop();
        self.spawn_runtime_telemetry_loop();
        self.spawn_runtime_provenance_loop();
        self.spawn_market_feed_loop(sender.clone());
        self.spawn_rtds_loop(sender.clone());
        self.spawn_chainlink_http_loop(sender.clone());
        if self.inner.settings.target.enable_direct_binance_book_ticker {
            self.spawn_binance_loop(sender);
        } else {
            info!("Direct Binance bookTicker feed disabled by configuration");
        }
        info!("Rust PolyEdge runtime started in paper mode");
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
        let result = self
            .inner
            .recorder
            .lock()
            .map_err(|error| format!("runtime recorder lock poisoned: {error}"))
            .and_then(|mut recorder| {
                recorder.record_batch(std::slice::from_ref(&event))?;
                recorder.flush()
            });
        match result {
            Ok(()) => {
                self.inner
                    .recorder_metrics
                    .persisted_total
                    .fetch_add(1, Ordering::Relaxed);
                if let Ok(mut state) = self.inner.data.try_write() {
                    state.runtime_events += 1;
                    state.recent_events.push_back(event.clone());
                    truncate(&mut state.recent_events, RECENT_LIMIT);
                }
                let _ = self.inner.broadcaster.send(event);
                Ok(())
            }
            Err(error) => {
                self.inner
                    .recorder_metrics
                    .failed_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
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
            loop {
                runtime.set_feed_status("discovery", "running", None).await;
                let settings = runtime.inner.settings.clone();
                let result = tokio::task::spawn_blocking(move || {
                    polyedge_feeds::discover_markets(&settings)
                })
                .await;
                match result {
                    Ok(Ok(markets)) => {
                        runtime.replace_markets(markets).await;
                        runtime.set_feed_status("discovery", "ok", None).await;
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
                let provenance = runtime_provenance(&runtime.inner.settings)
                    .expect("startup already validated exact runtime provenance");
                runtime
                    .record_event("runtime_provenance", provenance, None, None)
                    .await;
            }
        })
    }

    fn spawn_market_feed_loop(&self, sender: mpsc::Sender<FeedEvent>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                let token_ids = runtime.market_token_ids().await;
                if token_ids.is_empty() {
                    runtime
                        .set_feed_status("polymarket_clob_market", "waiting_for_markets", None)
                        .await;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                runtime
                    .set_feed_status("polymarket_clob_market", "connecting", None)
                    .await;
                let subscribed_tokens = token_ids.clone();
                let mut feed = tokio::spawn(polyedge_feeds::run_market_feed(
                    runtime.inner.settings.clone(),
                    token_ids,
                    sender.clone(),
                ));
                let mut refresh = tokio::time::interval(Duration::from_secs(2));
                loop {
                    tokio::select! {
                        result = &mut feed => {
                            match result {
                                Ok(Ok(())) => {
                                    runtime
                                        .set_feed_status("polymarket_clob_market", "disconnected", None)
                                        .await;
                                }
                                Ok(Err(error)) => {
                                    runtime
                                        .feed_error(FeedName::PolymarketClobMarket, error.to_string())
                                        .await;
                                }
                                Err(error) if !error.is_cancelled() => {
                                    runtime
                                        .feed_error(FeedName::PolymarketClobMarket, error.to_string())
                                        .await;
                                }
                                Err(_) => {}
                            }
                            break;
                        }
                        _ = refresh.tick() => {
                            if runtime.market_token_ids().await != subscribed_tokens {
                                feed.abort();
                                let _ = feed.await;
                                runtime
                                    .set_feed_status(
                                        "polymarket_clob_market",
                                        "resubscribing_token_set_changed",
                                        None,
                                    )
                                    .await;
                                break;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
    }

    fn spawn_rtds_loop(&self, sender: mpsc::Sender<FeedEvent>) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                runtime
                    .set_feed_status("polymarket_rtds", "connecting", None)
                    .await;
                match polyedge_feeds::run_rtds_feed(runtime.inner.settings.clone(), sender.clone())
                    .await
                {
                    Ok(()) => {
                        runtime
                            .set_feed_status("polymarket_rtds", "disconnected", None)
                            .await;
                    }
                    Err(error) => {
                        runtime
                            .feed_error(FeedName::PolymarketRtdsChainlink, error.to_string())
                            .await;
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
            FeedEvent::RawMarketEvent(event) => self.handle_raw_market_event(event).await,
            FeedEvent::Book(book) => self.handle_book(book).await,
            FeedEvent::Error {
                source, message, ..
            } => self.feed_error(source, message).await,
            FeedEvent::Heartbeat { source, .. } => {
                self.set_feed_status(&format!("{source:?}"), "ok", None)
                    .await;
            }
        }
    }

    async fn replace_markets(&self, markets: Vec<MarketSpec>) {
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut data = self.inner.data.write().await;
        let existing = data.markets.clone();
        let now = Utc::now();
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
                    .or_insert(recovered_start);
            }
            data = self.inner.data.write().await;
        }
        data.decision_generation = data.decision_generation.wrapping_add(1);
        drop(data);
        drop(_decision_guard);
        self.retry_pending_market_start_events().await;
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

    async fn handle_book(&self, book: BookState) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let (market, quality_events) = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let mut data = self.inner.data.write().await;
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
        self.handle_paper_fills(&book).await;
    }

    async fn handle_raw_market_event(&self, event: polyedge_feeds::MarketChannelEvent) {
        if self.retry_pending_decision_application().await == PendingApplicationRetry::Retained {
            return;
        }
        let quality_events = {
            let _decision_guard = self.inner.decision_gate.lock().await;
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

    async fn handle_paper_fills(&self, book: &BookState) {
        let markets_by_token = {
            let data = self.inner.data.read().await;
            markets_by_token_from_data(&data)
        };
        let reports = {
            let _decision_guard = self.inner.decision_gate.lock().await;
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
                if let Some(snapshot) = engine.execution_quality.register_order(
                    decision,
                    report,
                    book,
                    self.inner.settings.paper.order_live_after_ms,
                ) {
                    report.raw.insert("execution_quality".to_owned(), snapshot);
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
        let (reference, references, markets, books, paused, kill_switch, data_generation) = {
            let _decision_guard = self.inner.decision_gate.lock().await;
            let data = self.inner.data.read().await;
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
                let pipeline_input_value = match serde_json::to_value(&pipeline_input) {
                    Ok(value @ Value::Object(_)) => value,
                    Ok(_) => {
                        error!("decision pipeline input did not serialize to an object");
                        continue;
                    }
                    Err(error) => {
                        error!("decision pipeline input serialization failed: {error}");
                        continue;
                    }
                };
                let pipeline_input_sha256 = value_sha256(&pipeline_input_value);
                let market_start_evidence_sha256 = value_sha256(
                    &serde_json::to_value(&market_start_evidence).unwrap_or(Value::Null),
                );
                let batch_id = decision_batch_id_v3(&pipeline_input_sha256);
                let pipeline_output = evaluate_decision_pipeline_v3(&pipeline_input);
                let pipeline_output_value = match serde_json::to_value(&pipeline_output) {
                    Ok(value @ Value::Object(_)) => value,
                    Ok(_) => {
                        error!("decision pipeline output did not serialize to an object");
                        continue;
                    }
                    Err(error) => {
                        error!("decision pipeline output serialization failed: {error}");
                        continue;
                    }
                };
                let pipeline_output_sha256 = value_sha256(&pipeline_output_value);
                let classifier_after = pipeline_output.classifier_after.clone();
                let features = pipeline_input.regime_feature_input.clone().build();
                let mut strategy_lineage = Vec::new();
                let mut strategy_evidence = Vec::new();
                for evaluated in &pipeline_output.strategy_evaluations {
                    if let Some(evaluated_decision) = evaluated.evaluated_decision.as_ref() {
                        strategy_lineage.push(StrategyDecisionLineage {
                            evaluation_index: evaluated.evaluation_index,
                            strategy_output_index: strategy_lineage.len(),
                            decision: evaluated_decision.clone(),
                            metadata: evaluated.metadata.clone(),
                        });
                    }
                    strategy_evidence.push(json!({
                        "schema_version": 1,
                        "decision_batch_schema_version": 3,
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
                let decision_lineage = bind_final_decision_lineage(decisions, &strategy_lineage);
                let prepared_decisions = decisions
                    .iter()
                    .enumerate()
                    .map(|(output_index, decision)| {
                        let source = decision_lineage[output_index];
                        let metadata = source.map(|source| source.metadata.clone());
                        let lineage = source.map(|source| StrategyLineageBinding {
                            evaluation_index: source.evaluation_index,
                            strategy_output_index: source.strategy_output_index,
                        });
                        let payload = decision_event_payload(
                            decision,
                            metadata.as_ref(),
                            lineage.as_ref(),
                            None,
                        );
                        let binding = DecisionBatchBinding {
                            batch_id: batch_id.clone(),
                            output_index,
                            decision_sha256: value_sha256(&payload),
                        };
                        let recorded_payload = decision_event_payload(
                            decision,
                            metadata.as_ref(),
                            lineage.as_ref(),
                            Some(&binding),
                        );
                        PreparedDecision {
                            decision: decision.clone(),
                            metadata,
                            lineage,
                            binding,
                            payload: recorded_payload,
                        }
                    })
                    .collect::<Vec<_>>();
                let final_decisions = prepared_decisions
                    .iter()
                    .map(|prepared| {
                        let unbound_payload = decision_event_payload(
                            &prepared.decision,
                            prepared.metadata.as_ref(),
                            prepared.lineage.as_ref(),
                            None,
                        );
                        json!({
                            "output_index": prepared.binding.output_index,
                            "decision_sha256": prepared.binding.decision_sha256,
                            "decision": unbound_payload
                        })
                    })
                    .collect::<Vec<_>>();
                let strategy_batch = json!({
                    "schema_version": 3,
                    "schema": "polyedge.strategy_decision_batch.v3",
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
            let _decision_guard = self.inner.decision_gate.lock().await;
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
                drop(_decision_guard);
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
                drop(_decision_guard);
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
                    if let Some(snapshot) = report.raw.get("execution_quality") {
                        applied_events.push((
                            "paper_order_queue_registration".to_owned(),
                            snapshot.clone(),
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
            let application_evidence_durable =
                self.record_required_runtime_events(applied_events).await;
            if !application_evidence_durable {
                if let Some(pending) = pending_application {
                    let mut engine = self.inner.engine.lock().await;
                    engine.pending_decision_application = Some(pending);
                }
            }
            drop(_decision_guard);

            for (prepared, applied) in prepared_decisions.into_iter().zip(applied_outputs) {
                let decision = prepared.decision;
                let metadata = prepared.metadata;
                self.accept_durable_decision(decision.clone()).await;
                let actionable = matches!(
                    decision.action,
                    DecisionAction::Place | DecisionAction::CancelAll
                );
                if !actionable || (application_evidence_durable && applied.is_some()) {
                    self.maybe_publish_execution_intent(
                        &market,
                        &fair_value,
                        &reference,
                        &market_books,
                        &decision,
                        metadata.as_ref(),
                        decision_ts,
                    )
                    .await;
                }
                if application_evidence_durable {
                    if let Some(applied) = applied {
                        for report in applied.reports {
                            self.accept_persisted_execution_report(report, false).await;
                        }
                    }
                }
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
        let model_settings = self.inner.settings.clone();
        let execution_model = match tokio::task::spawn_blocking(move || {
            resolve_execution_model(&model_settings, decision_ts)
        })
        .await
        .map_err(|error| format!("execution-model control task failed: {error}"))
        .and_then(|result| result)
        {
            Ok(model) => model,
            Err(reason) => {
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
        let publisher = match IntentPublisherConfig::from_settings(&self.inner.settings) {
            Ok(publisher) => publisher,
            Err(reason) => {
                self.record_event(
                    "execution_intent_not_published",
                    json!({
                        "decision_id": intent.decision_id,
                        "market_id": intent.market_id,
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
        tokio::spawn(async move {
            let publish_intent = intent.clone();
            let result =
                tokio::task::spawn_blocking(move || publisher.publish(&publish_intent)).await;
            match result {
                Ok(Ok(published)) => {
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
                                "valid_until": intent.valid_until,
                                "order_submission_attempted": false,
                                "credential_free": true
                            }),
                            None,
                            None,
                        )
                        .await;
                }
                Ok(Err(reason)) => {
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
                Err(error) => {
                    runtime
                        .record_event(
                            "execution_intent_not_published",
                            json!({
                                "decision_id": intent.decision_id,
                                "market_id": intent.market_id,
                                "reason": format!("publisher task failed: {error}"),
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
        let payload = decision_event_payload(&decision, metadata.as_ref(), None, binding.as_ref());
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
        if let Some(snapshot) = report.raw.get("execution_quality") {
            self.record_event("paper_order_queue_registration", snapshot, None, None)
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
                    .or_insert_with(|| update.clone());
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
                .record_required_events(vec![("market_start_price".to_owned(), event.clone())])
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
        let _decision_guard = self.inner.decision_gate.lock().await;
        let mut engine = self.inner.engine.lock().await;
        let Some(pending) = engine.pending_settlements.get(market_id).cloned() else {
            return PendingSettlementRetry::NotPending;
        };
        if !self.record_required_events(pending.events).await {
            warn!(
                market_id = %market_id,
                settlement_journal_id = %pending.journal_id,
                "paper settlement retained for retry because durable persistence failed"
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
        let mut data = self.inner.data.write().await;
        if !data.settled_markets.contains(market_id) {
            data.settled_markets.push(market_id.clone());
            data.decision_generation = data.decision_generation.wrapping_add(1);
        }
        PendingSettlementRetry::Committed
    }

    async fn retry_pending_decision_application(&self) -> PendingApplicationRetry {
        let decision_guard = self.inner.decision_gate.lock().await;
        let mut engine = self.inner.engine.lock().await;
        let Some(pending) = engine.pending_decision_application.clone() else {
            return PendingApplicationRetry::NotPending;
        };
        if !self
            .record_required_runtime_events(pending.events.clone())
            .await
        {
            warn!(
                batch_id = %pending.batch_id,
                "paper decision application journal retained for retry because durable persistence failed"
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
            let events = finalize_settlement_journal(&journal_id, unbound_events);
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
        self.record_required_runtime_events(required_runtime_events(entries, Utc::now()))
            .await
    }

    async fn record_required_runtime_events(&self, events: Vec<RuntimeEvent>) -> bool {
        if events.is_empty() {
            return true;
        }
        let mut last_error = None;
        for attempt in 1..=REQUIRED_RECORDER_ATTEMPTS {
            match self.persist_required_batch(events.clone()).await {
                Ok(()) => {
                    self.accept_durable_events(&events).await;
                    return true;
                }
                Err(error) => {
                    warn!(
                        attempt,
                        max_attempts = REQUIRED_RECORDER_ATTEMPTS,
                        event_count = events.len(),
                        "required runtime evidence was not durably persisted: {error}"
                    );
                    last_error = Some(error);
                }
            }
        }
        let mut state = self.inner.data.write().await;
        *state
            .drop_counts
            .entry("required_recorder_write_error".to_owned())
            .or_insert(0) += events.len();
        warn!(
            event_count = events.len(),
            error = last_error.as_deref().unwrap_or("unknown recorder error"),
            "required runtime evidence exhausted durable recorder retries"
        );
        false
    }

    async fn persist_required_batch(&self, events: Vec<RuntimeEvent>) -> Result<(), String> {
        let event_count = events.len();
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .recorder_metrics
            .queued
            .fetch_add(event_count, Ordering::Relaxed);
        self.inner
            .recorder_metrics
            .enqueued_total
            .fetch_add(event_count as u64, Ordering::Relaxed);
        if self
            .inner
            .recorder_tx
            .send(RecorderRequest::durable(events, ack_tx))
            .is_err()
        {
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
        let recorder_queue_failed = if persist {
            self.inner
                .recorder_metrics
                .queued
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .recorder_metrics
                .enqueued_total
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .recorder_tx
                .send(RecorderRequest::best_effort(event.clone()))
                .is_err()
        } else {
            self.inner
                .recorder_metrics
                .filtered_total
                .fetch_add(1, Ordering::Relaxed);
            false
        };
        if recorder_queue_failed {
            saturating_sub_atomic(&self.inner.recorder_metrics.queued, 1);
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
        let mut data = self.inner.data.write().await;
        data.feed_status.insert(
            name.to_owned(),
            json!({
                "status": status,
                "message": message,
                "updated_at": Utc::now()
            }),
        );
    }

    async fn feed_error(&self, source: FeedName, message: String) {
        let source_text = format!("{source:?}");
        self.set_feed_status(&source_text, "error", Some(message.clone()))
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

    async fn market_token_ids(&self) -> Vec<TokenId> {
        let data = self.inner.data.read().await;
        data.markets
            .values()
            .flat_map(|market| [market.up_token_id.clone(), market.down_token_id.clone()])
            .collect()
    }
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
            loop {
                let request = match receiver.recv_timeout(RECORDER_FLUSH_INTERVAL) {
                    Ok(request) => request,
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {
                        flush_runtime_recorder(&recorder);
                        last_flush = Instant::now();
                        continue;
                    }
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                        flush_runtime_recorder(&recorder);
                        break;
                    }
                };
                let mut requests = vec![request];
                let mut event_count = requests[0].events.len();
                while event_count < RECORDER_BATCH_LIMIT {
                    match receiver.try_recv() {
                        Ok(request) => {
                            event_count += request.events.len();
                            requests.push(request);
                        }
                        Err(std_mpsc::TryRecvError::Empty) => break,
                        Err(std_mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                metrics.batches_total.fetch_add(1, Ordering::Relaxed);
                metrics
                    .last_batch_size
                    .store(event_count, Ordering::Relaxed);
                let flush_required = requests.iter().any(|request| request.durable_ack.is_some());
                let events = requests
                    .iter()
                    .flat_map(|request| request.events.iter().cloned())
                    .collect::<Vec<_>>();
                let result = match recorder.lock() {
                    Ok(mut recorder) => recorder.record_batch(&events).and_then(|()| {
                        if flush_required {
                            recorder.flush()
                        } else {
                            Ok(())
                        }
                    }),
                    Err(error) => Err(format!("runtime recorder lock poisoned: {error}")),
                };
                saturating_sub_atomic(&metrics.queued, event_count);
                match &result {
                    Ok(()) => {
                        metrics
                            .persisted_total
                            .fetch_add(event_count as u64, Ordering::Relaxed);
                    }
                    Err(error) => {
                        metrics
                            .failed_total
                            .fetch_add(event_count as u64, Ordering::Relaxed);
                        warn!("runtime recorder failed: {error}");
                    }
                }
                for request in requests {
                    if let Some(ack) = request.durable_ack {
                        let _ = ack.send(result.clone());
                    }
                }
                if last_flush.elapsed() >= RECORDER_FLUSH_INTERVAL {
                    flush_runtime_recorder(&recorder);
                    last_flush = Instant::now();
                }
            }
        })
    {
        warn!("failed to start runtime recorder worker: {error}");
    }
}

fn flush_runtime_recorder(recorder: &Arc<StdMutex<RuntimeRecorder>>) {
    match recorder.lock() {
        Ok(mut recorder) => {
            if let Err(error) = recorder.flush() {
                warn!("runtime recorder flush failed: {error}");
            }
        }
        Err(error) => warn!("runtime recorder lock poisoned during flush: {error}"),
    }
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

fn feed_summary(data: &RuntimeData) -> &'static str {
    if data.feed_status.values().any(|status| {
        status
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "ok" || status == "running" || status == "connecting")
    }) {
        "running"
    } else {
        "starting"
    }
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
        "decision_pipeline_schema": "polyedge.strategy_decision_batch.v3",
        "decision_pipeline_parity_scope": "full_decision_pipeline_recomputation",
        "decision_config_schema": "polyedge.decision_config.v1",
        "decision_config_sha256": decision_config_sha256,
        "candidate": candidate,
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
    json!({
        "schema": "polyedge.decision_config.v1",
        "target": settings.target,
        "data_policy": {
            "compact_shadow_recording": settings.azure.compact_shadow_recording,
            "shadow_book_sample_ms": settings.azure.shadow_book_sample_ms
        },
        "strategy": settings.strategy,
        "risk": settings.risk,
        "paper_execution": settings.paper,
        "execution_safety": {
            "execution_mode": execution_mode(settings),
            "allow_live": settings.live.allow_live,
            "confirm_non_restricted_location": settings.live.confirm_non_restricted_location,
            "require_exact_resolution_source_for_live": settings.live.require_exact_resolution_source_for_live,
            "allow_emergency_account_cancel": settings.live.allow_emergency_account_cancel
        },
        "event_blob_routing": {
            "before_cutover": settings.azure.event_blob_prefix,
            "after_cutover": settings.azure.event_blob_prefix_after_cutover,
            "cutover_utc": settings.azure.event_blob_prefix_cutover_utc
        },
        "adaptive_mode": adaptive_mode,
        "candidate": adaptive_mode.map(FrozenStrategyMode::candidate)
    })
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
    lineage: Option<&StrategyLineageBinding>,
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
    if let Some(lineage) = lineage {
        object.insert(
            "strategy_evaluation_index".to_owned(),
            json!(lineage.evaluation_index),
        );
        object.insert(
            "strategy_output_index".to_owned(),
            json!(lineage.strategy_output_index),
        );
    }
    if let Some(binding) = binding {
        object.insert("decision_batch_schema_version".to_owned(), json!(3));
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
    let unbound_decision = decision_event_payload(
        &prepared.decision,
        prepared.metadata.as_ref(),
        prepared.lineage.as_ref(),
        None,
    );
    if value_sha256(&unbound_decision) != prepared.binding.decision_sha256 {
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

fn bind_final_decision_lineage<'a>(
    decisions: &[TradeDecision],
    lineage: &'a [StrategyDecisionLineage],
) -> Vec<Option<&'a StrategyDecisionLineage>> {
    let mut used = vec![false; lineage.len()];
    decisions
        .iter()
        .map(|decision| {
            let exact = lineage
                .iter()
                .enumerate()
                .find(|(index, source)| !used[*index] && source.decision == *decision)
                .map(|(index, _)| index);
            let matched = exact.or_else(|| {
                if decision.action != DecisionAction::Place {
                    return None;
                }
                let candidates = lineage
                    .iter()
                    .enumerate()
                    .filter(|(index, source)| {
                        !used[*index] && same_place_lineage(&source.decision, decision)
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if candidates.len() == 1 {
                    Some(candidates[0])
                } else {
                    None
                }
            });
            matched.map(|index| {
                used[index] = true;
                &lineage[index]
            })
        })
        .collect()
}

fn same_place_lineage(source: &TradeDecision, final_decision: &TradeDecision) -> bool {
    source.action == DecisionAction::Place
        && final_decision.action == DecisionAction::Place
        && source.market_id == final_decision.market_id
        && source.condition_id == final_decision.condition_id
        && source.token_id == final_decision.token_id
        && source.outcome == final_decision.outcome
        && source.side == final_decision.side
        && source.price == final_decision.price
        && source.order_kind == final_decision.order_kind
        && source.ttl_ms == final_decision.ttl_ms
        && source.expected_edge == final_decision.expected_edge
        && source.post_only == final_decision.post_only
        && source.tick_size == final_decision.tick_size
        && source.neg_risk == final_decision.neg_risk
}

fn canonical_json(value: &Value) -> String {
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
                    .map(|(key, value)| format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("JSON key serializes"),
                        canonical_json(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        _ => serde_json::to_string(value).expect("JSON value serializes"),
    }
}

fn value_sha256(value: &Value) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(canonical_json(value).as_bytes())
    )
}

fn decision_batch_id_v3(pipeline_input_sha256: &str) -> String {
    format!(
        "strategy-batch-{}",
        pipeline_input_sha256.trim_start_matches("sha256:")
    )
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
    use polyedge_engine::{RegimeLabel, StrategyDataQuality};
    use serde_json::json;
    use std::fs;
    use std::thread;
    use std::time::Duration as StdDuration;

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
        assert_eq!(
            payload["decision_pipeline_schema"],
            "polyedge.strategy_decision_batch.v3"
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
    fn final_decision_lineage_is_one_to_one_and_never_borrowed_by_synthesized_cancel() {
        let place = |price: Decimal| TradeDecision {
            action: DecisionAction::Place,
            market_id: MarketId::new("lineage-market"),
            condition_id: Some(ConditionId::new("lineage-condition")),
            token_id: Some(TokenId::new("lineage-token")),
            outcome: Some(Outcome::Up),
            side: Some(Side::Buy),
            price: Some(price),
            size: Some(Decimal::from(10)),
            quote_amount: Some(price * Decimal::from(10)),
            order_kind: Some(OrderKind::PostOnlyGtc),
            reason: "strategy quote".to_owned(),
            ttl_ms: Some(1_000),
            expected_edge: Some(Decimal::new(2, 2)),
            post_only: true,
            tick_size: Some(Decimal::new(1, 2)),
            neg_risk: false,
        };
        let metadata = StrategyDecisionMetadata {
            candidate: FrozenStrategyMode::DynamicQuoteStyle.candidate(),
            regime: RegimeLabel::Normal,
            q: Some(Decimal::new(55, 2)),
            expected_edge: Some(Decimal::new(2, 2)),
            data_quality: StrategyDataQuality {
                decision_grade: true,
                reference_stale: false,
                book_stale: false,
                market_active: true,
                has_start_price: true,
                has_books: true,
                flags: Vec::new(),
            },
            features_summary: json!({}),
        };
        let source_a = place(Decimal::new(49, 2));
        let source_b = place(Decimal::new(48, 2));
        let lineage = vec![
            StrategyDecisionLineage {
                evaluation_index: 3,
                strategy_output_index: 0,
                decision: source_a,
                metadata: metadata.clone(),
            },
            StrategyDecisionLineage {
                evaluation_index: 7,
                strategy_output_index: 1,
                decision: source_b.clone(),
                metadata,
            },
        ];
        let mut risk_clamped = source_b;
        risk_clamped.size = Some(Decimal::from(5));
        risk_clamped.quote_amount = Some(Decimal::new(48, 2) * Decimal::from(5));
        let synthesized_cancel = TradeDecision {
            action: DecisionAction::CancelAll,
            market_id: MarketId::new("lineage-market"),
            condition_id: Some(ConditionId::new("lineage-condition")),
            token_id: None,
            outcome: None,
            side: None,
            price: None,
            size: None,
            quote_amount: None,
            order_kind: None,
            reason: "risk denied".to_owned(),
            ttl_ms: None,
            expected_edge: None,
            post_only: false,
            tick_size: None,
            neg_risk: false,
        };

        let bound = bind_final_decision_lineage(&[risk_clamped, synthesized_cancel], &lineage);
        assert_eq!(bound[0].unwrap().evaluation_index, 7);
        assert_eq!(bound[0].unwrap().strategy_output_index, 1);
        assert!(bound[1].is_none());

        let lineage_binding = StrategyLineageBinding {
            evaluation_index: 7,
            strategy_output_index: 1,
        };
        let unbound = decision_event_payload(
            &place(Decimal::new(48, 2)),
            Some(&lineage[1].metadata),
            Some(&lineage_binding),
            None,
        );
        let binding = DecisionBatchBinding {
            batch_id: "strategy-batch-v3".to_owned(),
            output_index: 0,
            decision_sha256: value_sha256(&unbound),
        };
        let payload = decision_event_payload(
            &place(Decimal::new(48, 2)),
            Some(&lineage[1].metadata),
            Some(&lineage_binding),
            Some(&binding),
        );
        assert_eq!(payload["decision_batch_schema_version"], 3);
        assert_eq!(payload["strategy_evaluation_index"], 7);
        assert_eq!(payload["strategy_output_index"], 1);
        assert_eq!(payload["strategy_decision_sha256"], value_sha256(&unbound));
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
        let unbound = decision_event_payload(&decision, None, None, None);
        let binding = DecisionBatchBinding {
            batch_id: format!("strategy-batch-{}", "a".repeat(64)),
            output_index: 3,
            decision_sha256: value_sha256(&unbound),
        };
        let prepared = PreparedDecision {
            decision: decision.clone(),
            metadata: None,
            lineage: None,
            payload: decision_event_payload(&decision, None, None, Some(&binding)),
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
                .send(RecorderRequest::best_effort(RuntimeEvent {
                    event_type: "book".to_owned(),
                    ts: Utc::now(),
                    data: json!({ "index": index }),
                }))
                .unwrap();
        }
        drop(sender);

        for _ in 0..100 {
            let lines = fs::read_to_string(&path)
                .map(|text| text.lines().count())
                .unwrap_or_default();
            if lines == 100 {
                break;
            }
            thread::sleep(StdDuration::from_millis(10));
        }

        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 100);
        assert_eq!(recorder.lock().unwrap().status(false)["error_count"], 0);
        assert_eq!(metrics.snapshot()["queued"], 0);
        assert_eq!(metrics.snapshot()["enqueued_total"], 100);
        assert_eq!(metrics.snapshot()["persisted_total"], 100);
        assert_eq!(metrics.snapshot()["failed_total"], 0);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn durable_recorder_ack_fails_then_retries_the_same_journal() {
        let dir = std::env::temp_dir().join(format!(
            "polyedge-recorder-ack-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let path = dir.join("events.jsonl");
        fs::create_dir_all(&path).unwrap();
        let recorder = Arc::new(StdMutex::new(RuntimeRecorder::new_for_path(path.clone())));
        let metrics = Arc::new(RecorderMetrics::default());
        let (sender, receiver) = std_mpsc::channel();
        spawn_recorder_worker(Arc::clone(&recorder), receiver, Arc::clone(&metrics));
        let event = RuntimeEvent {
            event_type: "required_evidence".to_owned(),
            ts: Utc::now(),
            data: json!({"journal_id": "stable-journal-1"}),
        };

        let (failed_ack_tx, failed_ack_rx) = oneshot::channel();
        metrics.queued.fetch_add(1, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        sender
            .send(RecorderRequest::durable(vec![event.clone()], failed_ack_tx))
            .unwrap();
        assert!(failed_ack_rx.blocking_recv().unwrap().is_err());

        fs::remove_dir_all(&path).unwrap();
        let (success_ack_tx, success_ack_rx) = oneshot::channel();
        metrics.queued.fetch_add(1, Ordering::Relaxed);
        metrics.enqueued_total.fetch_add(1, Ordering::Relaxed);
        sender
            .send(RecorderRequest::durable(vec![event], success_ack_tx))
            .unwrap();
        assert_eq!(success_ack_rx.blocking_recv().unwrap(), Ok(()));
        drop(sender);

        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains("stable-journal-1"));
        assert_eq!(metrics.snapshot()["persisted_total"], 1);
        assert_eq!(metrics.snapshot()["failed_total"], 1);
        let _ = fs::remove_dir_all(dir);
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
        assert_eq!(persisted["payload"], frozen_event);
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
}
