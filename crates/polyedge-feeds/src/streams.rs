use crate::util::{
    decimal, levels, parse_datetime, parse_event_ts, parse_ms_timestamp, ureq_error,
    value_opt_text, value_text, websocket_json,
};
use crate::{FeedError, FeedEvent, FeedName};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use polyedge_config::RuntimeSettings;
use polyedge_domain::{BookLevel, BookState, ReferencePrice, TokenId};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

pub async fn run_rtds_feed(
    settings: RuntimeSettings,
    sender: mpsc::Sender<FeedEvent>,
) -> Result<(), FeedError> {
    let mut subscriptions = Vec::new();
    if settings.target.enable_polymarket_rtds_chainlink {
        subscriptions.push(json!({
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": json!({"symbol": settings.target.chainlink_symbol}).to_string()
        }));
    }
    if settings.target.enable_polymarket_rtds_binance {
        subscriptions.push(json!({
            "topic": "crypto_prices",
            "type": "update"
        }));
    }
    if subscriptions.is_empty() {
        return Ok(());
    }

    let subscribe = json!({
        "action": "subscribe",
        "subscriptions": subscriptions
    })
    .to_string();
    let (stream, _) = connect_async(settings.target.polymarket_rtds_url.as_str()).await?;
    let (mut write, mut read) = stream.split();
    write.send(Message::Text(subscribe)).await?;
    let mut ping = tokio::time::interval(Duration::from_secs_f64(
        settings.target.rtds_ping_interval_seconds.max(1.0),
    ));
    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Text("PING".to_owned())).await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let message = message?;
                if let Some(reference) = parse_rtds_message(message, &settings) {
                    let source = if reference.exact_resolution_source {
                        FeedName::PolymarketRtdsChainlink
                    } else {
                        FeedName::PolymarketRtdsBinance
                    };
                    publish(&sender, FeedEvent::Reference(reference)).await?;
                    publish(&sender, FeedEvent::Heartbeat { source, ts: Utc::now() }).await?;
                }
            }
        }
    }
}

pub async fn run_market_feed(
    settings: RuntimeSettings,
    token_ids: Vec<TokenId>,
    sender: mpsc::Sender<FeedEvent>,
) -> Result<(), FeedError> {
    if token_ids.is_empty() {
        return Ok(());
    }
    let token_texts: Vec<_> = token_ids.iter().map(ToString::to_string).collect();
    let subscribe = json!({
        "assets_ids": token_texts,
        "type": "market",
        "custom_feature_enabled": true
    })
    .to_string();
    let (stream, _) = connect_async(settings.target.polymarket_ws_url.as_str()).await?;
    let (mut write, mut read) = stream.split();
    write.send(Message::Text(subscribe)).await?;
    let mut books = BTreeMap::new();
    while let Some(message) = read.next().await {
        let message = message?;
        for book in parse_market_message(message, &mut books) {
            publish(&sender, FeedEvent::Book(book)).await?;
        }
    }
    Ok(())
}

pub async fn run_binance_book_ticker_feed(
    settings: RuntimeSettings,
    sender: mpsc::Sender<FeedEvent>,
) -> Result<(), FeedError> {
    let url = format!(
        "wss://stream.binance.com:9443/ws/{}@bookTicker",
        settings.target.binance_symbol
    );
    let (stream, _) = connect_async(url.as_str()).await?;
    let (_, mut read) = stream.split();
    while let Some(message) = read.next().await {
        let message = message?;
        let Some(payload) = websocket_json(message) else {
            continue;
        };
        let (Some(bid), Some(ask)) = (decimal(payload.get("b")), decimal(payload.get("a"))) else {
            continue;
        };
        let now = Utc::now();
        let reference = ReferencePrice {
            source: settings.binance_book_ticker_source_name(),
            price: (bid + ask) / Decimal::from(2),
            source_ts: now,
            local_ts: now,
            latency_ms: 0.0,
            stale: false,
            exact_resolution_source: false,
            quality_flags: Vec::new(),
        };
        publish(&sender, FeedEvent::Reference(reference)).await?;
    }
    Ok(())
}

pub fn fetch_chainlink_reference(
    settings: &RuntimeSettings,
) -> Result<Option<ReferencePrice>, FeedError> {
    let Some(url) = settings.target.chainlink_reference_url.as_deref() else {
        return Ok(None);
    };
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(5))
        .timeout_read(Duration::from_secs(5))
        .build();
    let mut request = agent.get(url);
    if let Some(api_key) = settings.target.chainlink_api_key.as_deref() {
        request = request.set("authorization", &format!("Bearer {api_key}"));
    }
    let response = request.call().map_err(ureq_error)?;
    let payload: Value = serde_json::from_str(
        &response
            .into_string()
            .map_err(|error| FeedError::HttpTransport(error.to_string()))?,
    )?;
    let Some(price) = extract_price(&payload) else {
        return Ok(None);
    };
    let local_ts = Utc::now();
    let source_ts = extract_timestamp(&payload).unwrap_or(local_ts);
    Ok(Some(ReferencePrice {
        source: settings.target.resolution_source.clone(),
        price,
        source_ts,
        local_ts,
        latency_ms: local_ts
            .signed_duration_since(source_ts)
            .num_microseconds()
            .map_or(0.0, |micros| (micros.max(0) as f64) / 1000.0),
        stale: false,
        exact_resolution_source: true,
        quality_flags: Vec::new(),
    }))
}

async fn publish(sender: &mpsc::Sender<FeedEvent>, event: FeedEvent) -> Result<(), FeedError> {
    sender
        .send(event)
        .await
        .map_err(|_| FeedError::ChannelClosed)
}

fn parse_rtds_message(message: Message, settings: &RuntimeSettings) -> Option<ReferencePrice> {
    let payload = websocket_json(message)?;
    if !matches!(
        payload.get("type").and_then(Value::as_str),
        Some("update" | "subscribe")
    ) {
        return None;
    }
    let topic = payload
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let body = payload.get("payload").and_then(Value::as_object)?;
    let symbol = body
        .get("symbol")
        .map(value_text)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let price = decimal(body.get("value"))?;
    let source_ts = parse_ms_timestamp(body.get("timestamp").or_else(|| payload.get("timestamp")))
        .unwrap_or_else(Utc::now);
    let local_ts = Utc::now();
    let latency_ms = local_ts
        .signed_duration_since(source_ts)
        .num_microseconds()
        .map_or(0.0, |micros| (micros.max(0) as f64) / 1000.0);
    if topic == "crypto_prices_chainlink"
        && symbol == settings.target.chainlink_symbol.to_ascii_lowercase()
    {
        return Some(ReferencePrice {
            source: settings.rtds_chainlink_source_name(),
            price,
            source_ts,
            local_ts,
            latency_ms,
            stale: false,
            exact_resolution_source: true,
            quality_flags: Vec::new(),
        });
    }
    if topic == "crypto_prices" && symbol == settings.target.binance_symbol.to_ascii_lowercase() {
        return Some(ReferencePrice {
            source: settings.rtds_binance_source_name(),
            price,
            source_ts,
            local_ts,
            latency_ms,
            stale: false,
            exact_resolution_source: false,
            quality_flags: Vec::new(),
        });
    }
    None
}

fn parse_market_message(
    message: Message,
    books: &mut BTreeMap<TokenId, BookState>,
) -> Vec<BookState> {
    let Some(payload) = websocket_json(message) else {
        return Vec::new();
    };
    if let Some(items) = payload.as_array() {
        return items
            .iter()
            .flat_map(|item| handle_market_event(item, books))
            .collect();
    }
    handle_market_event(&payload, books)
}

fn handle_market_event(event: &Value, books: &mut BTreeMap<TokenId, BookState>) -> Vec<BookState> {
    let event_type = event
        .get("event_type")
        .or_else(|| event.get("type"))
        .map(value_text)
        .unwrap_or_default()
        .to_ascii_lowercase();
    match event_type.as_str() {
        "book" | "orderbook" | "snapshot" => {
            let book = book_from_snapshot(event);
            books.insert(book.token_id.clone(), book.clone());
            vec![book]
        }
        "price_change" | "pricechange" => apply_price_change(event, books),
        "last_trade_price" | "trade" | "last_trade" => apply_last_trade(event, books),
        _ => Vec::new(),
    }
}

fn book_from_snapshot(event: &Value) -> BookState {
    BookState {
        token_id: TokenId::new(value_text(
            event
                .get("asset_id")
                .or_else(|| event.get("token_id"))
                .or_else(|| event.get("market"))
                .unwrap_or(&Value::Null),
        )),
        bids: levels(event.get("bids")),
        asks: levels(event.get("asks")),
        last_trade_price: decimal(event.get("last_trade_price")),
        exchange_ts: parse_event_ts(event.get("timestamp").or_else(|| event.get("ts"))),
        local_ts: Utc::now(),
        book_hash: value_opt_text(event.get("hash")),
    }
}

fn apply_price_change(event: &Value, books: &mut BTreeMap<TokenId, BookState>) -> Vec<BookState> {
    let changes = match event.get("price_changes").or_else(|| event.get("changes")) {
        Some(Value::Array(items)) => items.clone(),
        Some(Value::Object(_)) => vec![event.clone()],
        _ => Vec::new(),
    };
    let mut updated = Vec::new();
    for change in changes {
        let token_id = TokenId::new(value_text(
            change
                .get("asset_id")
                .or_else(|| change.get("token_id"))
                .unwrap_or(&Value::Null),
        ));
        if token_id.as_ref().is_empty() {
            continue;
        }
        let mut book = books
            .get(&token_id)
            .cloned()
            .unwrap_or_else(|| empty_book(token_id.clone()));
        if let Some(best_bid) = decimal(change.get("best_bid")) {
            book.bids = vec![BookLevel {
                price: best_bid,
                size: Decimal::ZERO,
            }];
        }
        if let Some(best_ask) = decimal(change.get("best_ask")) {
            book.asks = vec![BookLevel {
                price: best_ask,
                size: Decimal::ZERO,
            }];
        }
        book.exchange_ts =
            parse_event_ts(change.get("timestamp").or_else(|| event.get("timestamp")));
        book.local_ts = Utc::now();
        books.insert(token_id, book.clone());
        updated.push(book);
    }
    updated
}

fn apply_last_trade(event: &Value, books: &mut BTreeMap<TokenId, BookState>) -> Vec<BookState> {
    let token_id = TokenId::new(value_text(
        event
            .get("asset_id")
            .or_else(|| event.get("token_id"))
            .unwrap_or(&Value::Null),
    ));
    let Some(price) = decimal(event.get("price").or_else(|| event.get("last_trade_price"))) else {
        return Vec::new();
    };
    let mut book = books
        .get(&token_id)
        .cloned()
        .unwrap_or_else(|| empty_book(token_id.clone()));
    book.last_trade_price = Some(price);
    book.local_ts = Utc::now();
    books.insert(token_id, book.clone());
    vec![book]
}

fn empty_book(token_id: TokenId) -> BookState {
    BookState {
        token_id,
        bids: Vec::new(),
        asks: Vec::new(),
        last_trade_price: None,
        exchange_ts: None,
        local_ts: Utc::now(),
        book_hash: None,
    }
}

fn extract_price(payload: &Value) -> Option<Decimal> {
    let candidates = [
        payload.get("price"),
        payload.get("answer"),
        payload.get("value"),
        payload.get("median"),
        payload.get("data").and_then(|data| data.get("price")),
    ];
    for candidate in candidates {
        let price = decimal(candidate);
        if let Some(price) = price {
            if price > Decimal::from(1_000_000) {
                return Some(price / Decimal::from(100_000_000));
            }
            return Some(price);
        }
    }
    None
}

fn extract_timestamp(payload: &Value) -> Option<chrono::DateTime<Utc>> {
    let candidates = [
        payload.get("timestamp"),
        payload.get("updatedAt"),
        payload.get("observationsTimestamp"),
        payload.get("data").and_then(|data| data.get("timestamp")),
    ];
    candidates
        .into_iter()
        .find_map(|candidate| parse_ms_timestamp(candidate).or_else(|| parse_datetime(candidate)))
}
