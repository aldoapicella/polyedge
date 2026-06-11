use chrono::{DateTime, SecondsFormat, Utc};
use polyedge_config::{ExecutionMode, RuntimeSettings};
use polyedge_domain::{
    BookState, DecisionAction, ExecutionReport, MarketId, MarketSpec, ReferencePrice, RuntimeEvent,
    TokenId, TradeDecision,
};
use polyedge_engine::{
    LogReturnFairValueModel, MakerFirstStrategy, OrderManager, PaperFillEngine, RestingMakerOrder,
    RiskManager,
};
use polyedge_execution::{ExecutionClient, PaperExecutionClient};
use polyedge_feeds::{self, FeedEvent, FeedName};
use polyedge_storage::{AzureAppendBlobRecorder, EventRecorder, JsonlRecorder};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

const RECENT_LIMIT: usize = 1_000;
const HISTORY_LIMIT: usize = 500;

#[derive(Clone)]
pub struct RuntimeController {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    settings: RuntimeSettings,
    data: RwLock<RuntimeData>,
    engine: Mutex<RuntimeEngine>,
    recorder: StdMutex<RuntimeRecorder>,
    broadcaster: broadcast::Sender<RuntimeEvent>,
    started: AtomicBool,
}

#[derive(Clone, Debug)]
struct RuntimeData {
    started_at: DateTime<Utc>,
    paused: bool,
    pause_reason: Option<String>,
    paused_at: Option<DateTime<Utc>>,
    kill_switch: bool,
    markets: BTreeMap<MarketId, MarketSpec>,
    books: BTreeMap<TokenId, BookState>,
    reference: Option<ReferencePrice>,
    fair_values: BTreeMap<MarketId, Value>,
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
    fair_model: LogReturnFairValueModel,
    strategy: MakerFirstStrategy,
    risk: RiskManager,
    order_manager: OrderManager,
    execution: PaperExecutionClient,
    paper_fill_engine: PaperFillEngine,
    reference_aggregator: ReferenceAggregator,
    last_volatility_update_key: Option<(String, DateTime<Utc>, Decimal)>,
}

struct RuntimeRecorder {
    recorders: Vec<Box<dyn EventRecorder + Send>>,
    error_count: usize,
    dropped_count: usize,
    last_error: Option<String>,
}

#[derive(Default)]
struct ReferenceAggregator {
    latest_by_source: BTreeMap<String, ReferencePrice>,
}

impl RuntimeController {
    pub fn new(settings: RuntimeSettings) -> Self {
        let (broadcaster, _) = broadcast::channel(1_000);
        let data = RuntimeData {
            started_at: Utc::now(),
            paused: false,
            pause_reason: None,
            paused_at: None,
            kill_switch: false,
            markets: BTreeMap::new(),
            books: BTreeMap::new(),
            reference: None,
            fair_values: BTreeMap::new(),
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
            fair_model: LogReturnFairValueModel::new(settings.clone()),
            strategy: MakerFirstStrategy::new(settings.clone()),
            risk: RiskManager::new(settings.clone()),
            order_manager: OrderManager::new(),
            execution: PaperExecutionClient::new(),
            paper_fill_engine: PaperFillEngine::new(settings.clone()),
            reference_aggregator: ReferenceAggregator::default(),
            last_volatility_update_key: None,
        };
        let recorder = RuntimeRecorder::new(&settings);
        Self {
            inner: Arc::new(RuntimeInner {
                settings,
                data: RwLock::new(data),
                engine: Mutex::new(engine),
                recorder: StdMutex::new(recorder),
                broadcaster,
                started: AtomicBool::new(false),
            }),
        }
    }

    pub fn start_if_configured(&self) {
        if !self.inner.settings.deploy.run_bot_on_startup {
            return;
        }
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (sender, receiver) = mpsc::channel(10_000);
        self.spawn_feed_event_loop(receiver);
        self.spawn_discovery_loop();
        self.spawn_strategy_loop();
        self.spawn_market_feed_loop(sender.clone());
        self.spawn_rtds_loop(sender.clone());
        self.spawn_chainlink_http_loop(sender.clone());
        self.spawn_binance_loop(sender);
        info!("Rust PolyEdge runtime started in paper mode");
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.inner.broadcaster.subscribe()
    }

    pub async fn health(&self) -> Value {
        let data = self.inner.data.read().await;
        json!({
            "ok": true,
            "backend_impl": "rust",
            "shadow_only": false,
            "runtime_active": self.inner.started.load(Ordering::SeqCst),
            "execution_mode": execution_mode(&self.inner.settings),
            "kill_switch": data.kill_switch,
            "reports": report_status(false)
        })
    }

    pub async fn status(&self) -> Value {
        let data = self.inner.data.read().await;
        let engine = self.inner.engine.lock().await;
        let now = Utc::now();
        let recorder_status = self.recorder_status();
        json!({
            "app": self.inner.settings.deploy.app_name,
            "backend_impl": "rust",
            "shadow_only": false,
            "git_sha": option_env!("GIT_SHA").unwrap_or("unknown"),
            "version": env!("CARGO_PKG_VERSION"),
            "execution_mode": execution_mode(&self.inner.settings),
            "started_at": data.started_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            "now": now.to_rfc3339_opts(SecondsFormat::Secs, true),
            "uptime": now.signed_duration_since(data.started_at).num_seconds(),
            "markets": data.markets.len(),
            "tradeable_markets": active_markets(&data).len(),
            "books": data.books.len(),
            "tracked_open_orders": engine.order_manager.open_order_count(),
            "control": {
                "paused": data.paused,
                "paused_at": data.paused_at.map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true)),
                "pause_reason": data.pause_reason
            },
            "kill_switch": data.kill_switch,
            "task_health": {
                "api": "ok",
                "runtime_loop": if self.inner.started.load(Ordering::SeqCst) { "running" } else { "not_started" },
                "feeds": feed_summary(&data)
            },
            "queue_depths": {
                "feed_events": 0,
                "runtime_events": 0,
                "recorder": 0
            },
            "drop_counts": data.drop_counts,
            "feed_status": data.feed_status,
            "recorder_status": recorder_status.clone(),
            "event_bus_subscribers": self.inner.broadcaster.receiver_count(),
            "paper_fill": {
                "paper_fill_policy": self.inner.settings.paper.maker_fill_policy,
                "paper_order_live_after_ms": self.inner.settings.paper.order_live_after_ms,
                "paper_open_resting_orders": engine.execution.resting_orders.len(),
                "paper_maker_fills": engine.paper_fill_engine.stats.maker_fills
            },
            "paper_fill_stats": engine.paper_fill_engine.stats,
            "heartbeat_status": {
                "enabled": self.inner.settings.live.enable_heartbeat,
                "status": "disabled_in_rust_paper"
            },
            "live_heartbeat": Value::Null,
            "recorder": recorder_status,
            "reference": data.reference,
            "reports": report_status(false),
            "latest_decisions": data.decisions.iter().rev().take(20).cloned().collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>(),
            "latest_execution_reports": data.execution_reports.iter().rev().take(20).cloned().collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>()
        })
    }

    pub async fn snapshot(&self) -> Value {
        json!({
            "status": self.status().await,
            "current_market": self.current_market().await,
            "markets": self.markets().await,
            "open_orders": self.orders().await,
            "fills": self.fills().await,
            "latest_decisions": self.decisions().await,
            "latest_execution_reports": self.execution_reports().await
        })
    }

    pub async fn markets(&self) -> Vec<Value> {
        let data = self.inner.data.read().await;
        let mut markets: Vec<_> = data
            .markets
            .values()
            .map(|market| self.market_summary_from_data(market, &data))
            .collect();
        markets.sort_by_key(|value| {
            value
                .get("start_ts")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        markets
    }

    pub async fn current_market(&self) -> Value {
        let data = self.inner.data.read().await;
        let current = active_markets(&data)
            .into_iter()
            .min_by_key(|market| market.end_ts)
            .map(|market| self.market_summary_from_data(market, &data));
        current.unwrap_or(Value::Null)
    }

    pub async fn market_detail(&self, market_id: &str) -> Option<Value> {
        let data = self.inner.data.read().await;
        let market = data.markets.get(&MarketId::new(market_id.to_owned()))?;
        let related_decisions: Vec<_> = data
            .decisions
            .iter()
            .filter(|decision| decision.market_id == market.market_id)
            .rev()
            .take(100)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let related_reports: Vec<_> = data
            .execution_reports
            .iter()
            .filter(|report| report.market_id == market.market_id)
            .rev()
            .take(100)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        Some(json!({
            "market": self.market_summary_from_data(market, &data),
            "fair_value": data.fair_values.get(&market.market_id).cloned().unwrap_or(Value::Null),
            "books": {
                "up": data.books.get(&market.up_token_id),
                "down": data.books.get(&market.down_token_id)
            },
            "decisions": related_decisions,
            "execution_reports": related_reports
        }))
    }

    pub async fn orders(&self) -> Vec<Value> {
        let engine = self.inner.engine.lock().await;
        engine
            .order_manager
            .open_quotes()
            .into_iter()
            .map(|quote| {
                json!({
                    "market_id": quote.decision.market_id,
                    "token_id": quote.decision.token_id,
                    "side": quote.decision.side,
                    "placed_ts": quote.placed_ts,
                    "expires_at": quote.expires_at,
                    "order_id": quote.order_id,
                    "decision": quote.decision
                })
            })
            .collect()
    }

    pub async fn fills(&self) -> Vec<ExecutionReport> {
        let data = self.inner.data.read().await;
        data.execution_reports
            .iter()
            .filter(|report| report.filled_size > Decimal::ZERO)
            .rev()
            .take(200)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub async fn decisions(&self) -> Vec<TradeDecision> {
        let data = self.inner.data.read().await;
        data.decisions
            .iter()
            .rev()
            .take(200)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub async fn execution_reports(&self) -> Vec<ExecutionReport> {
        let data = self.inner.data.read().await;
        data.execution_reports
            .iter()
            .rev()
            .take(200)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub async fn recent_events(
        &self,
        limit: usize,
        event_type: Option<String>,
        market_id: Option<String>,
    ) -> Vec<RuntimeEvent> {
        let data = self.inner.data.read().await;
        data.recent_events
            .iter()
            .rev()
            .filter(|event| {
                event_type
                    .as_ref()
                    .is_none_or(|target| &event.event_type == target)
                    && market_id.as_ref().is_none_or(|target| {
                        event
                            .data
                            .get("market_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == target)
                    })
            })
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub async fn pause(&self, reason: Option<String>) -> Value {
        {
            let mut data = self.inner.data.write().await;
            data.paused = true;
            data.paused_at = Some(Utc::now());
            data.pause_reason = reason.clone();
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
            let mut data = self.inner.data.write().await;
            data.paused = false;
            data.paused_at = None;
            data.pause_reason = None;
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
            let mut data = self.inner.data.write().await;
            data.kill_switch = enabled;
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
                match polyedge_feeds::run_market_feed(
                    runtime.inner.settings.clone(),
                    token_ids,
                    sender.clone(),
                )
                .await
                {
                    Ok(()) => {
                        runtime
                            .set_feed_status("polymarket_clob_market", "disconnected", None)
                            .await;
                    }
                    Err(error) => {
                        runtime
                            .feed_error(FeedName::PolymarketClobMarket, error.to_string())
                            .await;
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
        let mut data = self.inner.data.write().await;
        let existing = data.markets.clone();
        data.markets.clear();
        for mut market in markets {
            if market.start_price.is_none() {
                if let Some(prior) = existing.get(&market.market_id) {
                    if let Some(start_price) = prior.start_price {
                        market = market.with_start_price(start_price);
                    }
                }
            }
            let payload = serde_json::to_value(&market).unwrap_or(Value::Null);
            data.markets.insert(market.market_id.clone(), market);
            drop(data);
            self.record_event("market", payload, Some("market_discovered"), None)
                .await;
            data = self.inner.data.write().await;
        }
    }

    async fn handle_reference(&self, reference: ReferencePrice) {
        let composite = {
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
            composite
        };
        {
            let mut data = self.inner.data.write().await;
            data.reference = Some(composite.clone());
        }
        self.capture_market_start_prices(&composite).await;
        self.settle_finished_markets(&composite).await;
        self.record_event("reference", &composite, Some("reference_update"), None)
            .await;
    }

    async fn handle_book(&self, book: BookState) {
        let market = {
            let mut data = self.inner.data.write().await;
            data.books.insert(book.token_id.clone(), book.clone());
            markets_by_token_from_data(&data)
                .get(&book.token_id)
                .cloned()
        };
        let publish_payload = book_summary(&book, market.as_ref());
        self.record_event(
            "book",
            &book,
            Some("book_update_summary"),
            Some(publish_payload),
        )
        .await;
        self.handle_paper_fills(&book).await;
    }

    async fn handle_paper_fills(&self, book: &BookState) {
        let markets_by_token = {
            let data = self.inner.data.read().await;
            markets_by_token_from_data(&data)
        };
        let reports = {
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
            filled
        };
        for report in reports {
            self.record_execution_report(report, true).await;
        }
    }

    async fn evaluate_once(&self) {
        let (reference, markets, books, paused, kill_switch) = {
            let data = self.inner.data.read().await;
            (
                data.reference.clone(),
                active_markets(&data)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>(),
                data.books.clone(),
                data.paused,
                data.kill_switch,
            )
        };
        let Some(reference) = reference else {
            return;
        };
        if paused {
            return;
        }
        for market in markets {
            let decisions = {
                let mut engine = self.inner.engine.lock().await;
                engine.risk.open_order_count = engine.order_manager.open_order_count();
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
                self.record_event("fair_value", &fair_value, Some("fair_value_update"), None)
                    .await;
                let raw_decisions = engine.strategy.evaluate(&market, &fair_value, &books);
                let assessment =
                    engine
                        .risk
                        .assess_market(&market, &reference, &books, now, kill_switch);
                let risk_decisions =
                    engine
                        .risk
                        .filter_decisions(&raw_decisions, &market, &assessment);
                engine.order_manager.reconcile(
                    &market.market_id,
                    &risk_decisions,
                    Some(market.condition_id.clone()),
                    now,
                )
            };

            for decision in decisions {
                self.push_decision(decision.clone()).await;
                if matches!(
                    decision.action,
                    DecisionAction::Place | DecisionAction::CancelAll
                ) {
                    let report = {
                        let mut engine = self.inner.engine.lock().await;
                        match engine.execution.submit(&decision).await {
                            Ok(report) => {
                                engine.order_manager.on_execution_report(&decision, &report);
                                engine.risk.open_order_count =
                                    engine.order_manager.open_order_count();
                                engine.risk.on_execution_report(&report);
                                Some(report)
                            }
                            Err(error) => {
                                error!("paper execution failed: {error}");
                                None
                            }
                        }
                    };
                    if let Some(report) = report {
                        self.record_execution_report(report, false).await;
                    }
                }
            }
        }
    }

    async fn push_decision(&self, decision: TradeDecision) {
        {
            let mut data = self.inner.data.write().await;
            data.decisions.push_back(decision.clone());
            truncate(&mut data.decisions, HISTORY_LIMIT);
        }
        self.record_event("decision", &decision, None, None).await;
    }

    async fn record_execution_report(&self, report: ExecutionReport, publish_fill: bool) {
        {
            let mut data = self.inner.data.write().await;
            data.execution_reports.push_back(report.clone());
            truncate(&mut data.execution_reports, HISTORY_LIMIT);
        }
        self.record_event("execution_report", &report, None, None)
            .await;
        if publish_fill && report.status == "paper_filled_maker" {
            self.publish_only("paper_fill", &report).await;
        }
    }

    async fn capture_market_start_prices(&self, reference: &ReferencePrice) {
        if reference.stale || !reference.exact_resolution_source {
            return;
        }
        let grace = self.inner.settings.target.start_price_capture_grace_seconds;
        let mut updates = Vec::new();
        {
            let mut data = self.inner.data.write().await;
            for market in data.markets.values_mut() {
                if market.start_price.is_some() {
                    continue;
                }
                let seconds_after_start = reference
                    .source_ts
                    .signed_duration_since(market.start_ts)
                    .num_microseconds()
                    .map_or(-1.0, |micros| micros as f64 / 1_000_000.0);
                if seconds_after_start >= 0.0 && seconds_after_start <= grace {
                    *market = market.clone().with_start_price(reference.price);
                    updates.push(json!({
                        "market_id": market.market_id,
                        "market_slug": market.market_slug,
                        "start_price": reference.price.to_string(),
                        "reference_source": reference.source,
                        "reference_source_ts": reference.source_ts
                    }));
                }
            }
        }
        for update in updates {
            self.record_event("market_start_price", update, None, None)
                .await;
        }
    }

    async fn settle_finished_markets(&self, reference: &ReferencePrice) {
        if reference.stale || !reference.exact_resolution_source {
            return;
        }
        let markets = {
            let data = self.inner.data.read().await;
            data.markets.values().cloned().collect::<Vec<_>>()
        };
        for market in markets {
            if market.start_price.is_none() || reference.source_ts < market.end_ts {
                continue;
            }
            {
                let data = self.inner.data.read().await;
                if data.settled_markets.contains(&market.market_id) {
                    continue;
                }
            }
            let start_price = market.start_price.unwrap_or(Decimal::ZERO);
            let winning_outcome = if reference.price >= start_price {
                "up"
            } else {
                "down"
            };
            let cleared_position = {
                let mut engine = self.inner.engine.lock().await;
                engine.order_manager.clear_market(&market.market_id);
                engine.execution.clear_market(&market.market_id);
                engine.risk.clear_market(&market.market_id)
            };
            {
                let mut data = self.inner.data.write().await;
                data.settled_markets.push(market.market_id.clone());
            }
            self.record_event(
                "paper_settlement",
                json!({
                    "market_id": market.market_id,
                    "market_slug": market.market_slug,
                    "start_ts": market.start_ts,
                    "end_ts": market.end_ts,
                    "start_price": start_price.to_string(),
                    "final_price": reference.price.to_string(),
                    "winning_outcome": winning_outcome,
                    "reference_source": reference.source,
                    "reference_source_ts": reference.source_ts,
                    "cleared_position": cleared_position.to_string()
                }),
                None,
                None,
            )
            .await;
        }
    }

    async fn cancel_active_markets(&self, reason: String) {
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
            let report = {
                let mut engine = self.inner.engine.lock().await;
                match engine.execution.submit(&decision).await {
                    Ok(report) => {
                        engine.order_manager.on_execution_report(&decision, &report);
                        Some(report)
                    }
                    Err(error) => {
                        warn!("cancel during pause failed: {error}");
                        None
                    }
                }
            };
            if let Some(report) = report {
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
        let data = serde_json::to_value(payload).unwrap_or(Value::Null);
        let event = RuntimeEvent {
            event_type: event_type.to_owned(),
            ts: Utc::now(),
            data: data.clone(),
        };
        let recorder_inner = self.inner.clone();
        let recorder_event = event.clone();
        tokio::task::spawn_blocking(move || {
            let mut recorder = recorder_inner.recorder.lock().unwrap();
            if let Err(error) = recorder.record(&recorder_event) {
                warn!("runtime recorder failed: {error}");
            }
        });
        {
            let mut state = self.inner.data.write().await;
            state.runtime_events += 1;
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

    fn recorder_status(&self) -> Value {
        match self.inner.recorder.try_lock() {
            Ok(recorder) => recorder.status(false),
            Err(_) => RuntimeRecorder::busy_status(),
        }
    }

    fn market_summary_from_data(&self, market: &MarketSpec, data: &RuntimeData) -> Value {
        let now = Utc::now();
        let mut value = serde_json::to_value(market).unwrap_or(Value::Null);
        if let Value::Object(map) = &mut value {
            map.insert(
                "is_active".to_owned(),
                Value::Bool(market.start_ts <= now && now < market.end_ts),
            );
            map.insert(
                "is_tradeable".to_owned(),
                Value::Bool(market.is_tradeable()),
            );
            map.insert(
                "fair_value".to_owned(),
                data.fair_values
                    .get(&market.market_id)
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        value
    }
}

impl RuntimeRecorder {
    fn new(settings: &RuntimeSettings) -> Self {
        let mut recorders: Vec<Box<dyn EventRecorder + Send>> = Vec::new();
        let path = env::var("RECORDER_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/events.jsonl"));
        recorders.push(Box::new(JsonlRecorder::new(path)));
        if let Some(account) = settings.azure.storage_account_name.as_deref() {
            let client_id = env::var("AZURE_CLIENT_ID").ok();
            recorders.push(Box::new(AzureAppendBlobRecorder::new(
                account,
                settings.azure.storage_container_name.clone(),
                client_id,
            )));
        }
        Self {
            recorders,
            error_count: 0,
            dropped_count: 0,
            last_error: None,
        }
    }

    fn record(&mut self, event: &RuntimeEvent) -> Result<(), String> {
        let mut last_error = None;
        for recorder in &mut self.recorders {
            if let Err(error) = recorder.record(event) {
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

    fn status(&self, busy: bool) -> Value {
        json!({
            "type": "composite",
            "recorders": self.recorders.len(),
            "error_count": self.error_count,
            "dropped_count": self.dropped_count,
            "last_error": self.last_error,
            "busy": busy
        })
    }

    fn busy_status() -> Value {
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

impl ReferenceAggregator {
    fn update(&mut self, reference: ReferencePrice, settings: &RuntimeSettings) -> ReferencePrice {
        self.latest_by_source
            .insert(reference.source.clone(), reference);
        self.composite(settings)
    }

    fn composite(&self, settings: &RuntimeSettings) -> ReferencePrice {
        let now = Utc::now();
        let exact = self
            .latest_by_source
            .values()
            .filter(|reference| {
                reference.exact_resolution_source
                    && !reference.stale
                    && reference.age_ms(now) <= settings.risk.max_reference_age_ms as f64
            })
            .max_by_key(|reference| reference.local_ts)
            .cloned();
        if let Some(reference) = exact {
            return self.with_cross_check_quality(reference, settings, now);
        }

        let fresh = self
            .latest_by_source
            .values()
            .filter(|reference| {
                !reference.stale
                    && reference.age_ms(now) <= settings.risk.max_reference_age_ms as f64
            })
            .cloned()
            .collect::<Vec<_>>();
        if fresh.is_empty() {
            let mut stale = self
                .latest_by_source
                .values()
                .max_by_key(|reference| reference.local_ts)
                .cloned()
                .unwrap_or_else(|| ReferencePrice {
                    source: "unavailable".to_owned(),
                    price: Decimal::ZERO,
                    source_ts: now,
                    local_ts: now,
                    latency_ms: 0.0,
                    stale: true,
                    exact_resolution_source: false,
                    quality_flags: vec!["no references available".to_owned()],
                });
            stale.stale = true;
            return stale;
        }
        let mut prices = fresh
            .iter()
            .filter_map(|reference| reference.price.to_f64())
            .collect::<Vec<_>>();
        prices.sort_by(|left, right| left.total_cmp(right));
        let median = prices[prices.len() / 2];
        ReferencePrice {
            source: "cex_median_proxy".to_owned(),
            price: Decimal::from_str_exact(&median.to_string()).unwrap_or(Decimal::ZERO),
            source_ts: fresh
                .iter()
                .map(|reference| reference.source_ts)
                .max()
                .unwrap_or(now),
            local_ts: now,
            latency_ms: fresh
                .iter()
                .map(|reference| reference.latency_ms)
                .fold(0.0, f64::max),
            stale: false,
            exact_resolution_source: false,
            quality_flags: Vec::new(),
        }
    }

    fn with_cross_check_quality(
        &self,
        mut preferred: ReferencePrice,
        settings: &RuntimeSettings,
        now: DateTime<Utc>,
    ) -> ReferencePrice {
        let mut proxy_prices = self
            .latest_by_source
            .values()
            .filter(|reference| {
                !reference.exact_resolution_source
                    && !reference.stale
                    && reference.age_ms(now) <= settings.risk.max_reference_age_ms as f64
            })
            .filter_map(|reference| reference.price.to_f64())
            .collect::<Vec<_>>();
        if proxy_prices.is_empty() {
            return preferred;
        }
        proxy_prices.sort_by(|left, right| left.total_cmp(right));
        let proxy_median =
            Decimal::from_str_exact(&proxy_prices[proxy_prices.len() / 2].to_string())
                .unwrap_or(Decimal::ZERO);
        if preferred.price <= Decimal::ZERO {
            return preferred;
        }
        let divergence = (preferred.price - proxy_median).abs() / preferred.price;
        if divergence <= settings.target.reference_divergence_pause_threshold {
            return preferred;
        }
        preferred.stale = true;
        preferred.quality_flags.push(format!(
            "reference_divergence:{}:chainlink={}:proxy_median={}",
            divergence, preferred.price, proxy_median
        ));
        preferred
    }
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

fn execution_mode(settings: &RuntimeSettings) -> &'static str {
    match settings.live.execution_mode {
        ExecutionMode::Paper => "paper",
        ExecutionMode::Live => "live",
    }
}

fn truncate<T>(values: &mut VecDeque<T>, limit: usize) {
    while values.len() > limit {
        values.pop_front();
    }
}
