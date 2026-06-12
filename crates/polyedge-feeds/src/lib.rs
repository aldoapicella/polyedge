mod discovery;
mod streams;
mod util;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

pub use discovery::discover_markets;
pub use streams::{
    fetch_chainlink_reference, run_binance_book_ticker_feed, run_market_feed, run_rtds_feed,
};

use polyedge_domain::{BookState, ReferencePrice};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum FeedEvent {
    Reference(ReferencePrice),
    Book(BookState),
    Error {
        source: FeedName,
        message: String,
        ts: DateTime<Utc>,
    },
    Heartbeat {
        source: FeedName,
        ts: DateTime<Utc>,
    },
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
