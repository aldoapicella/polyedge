mod discovery;
mod streams;
mod util;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

pub use discovery::discover_markets;
pub use streams::{
    fetch_chainlink_reference, run_binance_book_ticker_feed, run_market_feed,
    run_market_feed_generation, run_market_feed_generation_with_lease, run_rtds_feed,
};

use polyedge_domain::{BookState, ReferencePrice, TokenId};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::oneshot;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedName {
    PolymarketRtdsChainlink,
    PolymarketRtdsBinance,
    PolymarketClobMarket,
    BinanceBookTicker,
    CoinbaseTicker,
    ChainlinkHttp,
    Discovery,
    Mock,
}

#[derive(Debug)]
pub enum FeedEvent {
    Reference(ReferencePrice),
    Book(BookState),
    RawMarketEvent(MarketChannelEvent),
    Error {
        source: FeedName,
        message: String,
        ts: DateTime<Utc>,
    },
    Heartbeat {
        source: FeedName,
        ts: DateTime<Utc>,
    },
    /// A market socket has collected one exact full-book snapshot per subscribed
    /// asset and is stopped at its producer barrier.  The runtime must durably
    /// authorize this generation before the socket may forward any deltas.
    ClobResyncBarrier(ClobResyncBarrier),
    ClobRawMarketEvent {
        generation: u64,
        sequence: u64,
        event: MarketChannelEvent,
    },
    ClobBook {
        generation: u64,
        sequence: u64,
        book: BookState,
    },
}

#[derive(Debug)]
pub struct ClobResyncBarrier {
    pub generation: u64,
    pub sequence: u64,
    pub token_set_digest: String,
    pub token_count: usize,
    pub anchors: Vec<BookState>,
    pub pre_ready_events: Vec<MarketChannelEvent>,
    pub lease: ClobGenerationLease,
    pub ready_ack: oneshot::Sender<Result<(), String>>,
}

impl ClobResyncBarrier {
    pub fn token_ids(&self) -> impl Iterator<Item = &TokenId> {
        self.anchors.iter().map(|book| &book.token_id)
    }
}

#[derive(Clone, Debug)]
pub struct ClobGenerationLease(Arc<AtomicBool>);

impl ClobGenerationLease {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn terminate(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_terminal(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl Default for ClobGenerationLease {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketChannelEvent {
    pub event_type: String,
    pub recorded_ts: DateTime<Utc>,
    #[serde(default)]
    pub source_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub market_id: Option<String>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub token_id: Option<String>,
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub best_bid: Option<String>,
    #[serde(default)]
    pub best_ask: Option<String>,
    #[serde(default)]
    pub book_hash: Option<String>,
    pub raw_payload: Value,
}

#[derive(Debug, Error)]
pub enum FeedError {
    #[error("feed channel is closed")]
    ChannelClosed,
    #[error("HTTP status {0}")]
    HttpStatus(u16),
    #[error("HTTP transport error: {0}")]
    HttpTransport(String),
    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("WebSocket error: {0}")]
    WebSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("source stalled: {0}")]
    SourceStalled(String),
    #[error("market protocol error: {0}")]
    MarketProtocol(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for FeedError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}

#[derive(Clone, Debug)]
pub struct FeedPublisher {
    source: FeedName,
    sender: mpsc::Sender<FeedEvent>,
}

impl FeedPublisher {
    pub fn new(source: FeedName, sender: mpsc::Sender<FeedEvent>) -> Self {
        Self { source, sender }
    }

    pub async fn publish(&self, event: FeedEvent) -> Result<(), FeedError> {
        self.sender
            .send(event)
            .await
            .map_err(|_| FeedError::ChannelClosed)
    }

    pub async fn heartbeat(&self) -> Result<(), FeedError> {
        self.publish(FeedEvent::Heartbeat {
            source: self.source.clone(),
            ts: Utc::now(),
        })
        .await
    }
}

pub fn bounded_feed_channel(
    capacity: usize,
) -> (mpsc::Sender<FeedEvent>, mpsc::Receiver<FeedEvent>) {
    mpsc::channel(capacity)
}
