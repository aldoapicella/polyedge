use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use polyedge_config::RuntimeSettings;
use polyedge_domain::{
    BookLevel, BookState, MarketId, MarketSpec, MarketStatus, ReferencePrice, TokenId,
};
use regex::Regex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

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

pub fn discover_markets(settings: &RuntimeSettings) -> Result<Vec<MarketSpec>, FeedError> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(10))
        .build();
    let mut markets = BTreeMap::new();
    let mut seen_events = BTreeSet::new();

    for params in gamma_event_queries(settings) {
        let url = with_query(
            &format!("{}/events", settings.target.polymarket_gamma_url),
            &params,
        )?;
        let payload = get_json(&agent, url.as_str())?;
        let Some(events) = payload.as_array() else {
            continue;
        };
        for event in events {
            let event_id = event
                .get("id")
                .or_else(|| event.get("slug"))
                .map(value_text)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("{:p}", event));
            if !seen_events.insert(event_id) {
                continue;
            }
            collect_gamma_event(settings, event, &mut markets);
        }
    }

    for query in search_queries(settings) {
        let url = with_query(
            &format!("{}/public-search", settings.target.polymarket_gamma_url),
            &[("q".to_owned(), query)],
        )?;
        let Ok(payload) = get_json(&agent, url.as_str()) else {
            continue;
        };
        if let Some(events) = payload.get("events").and_then(Value::as_array) {
            for event in events {
                collect_gamma_event(settings, event, &mut markets);
            }
        }
    }

    let limit = settings.target.discovery_limit.min(500).to_string();
    let url = with_query(
        &format!("{}/markets", settings.target.polymarket_clob_url),
        &[("limit".to_owned(), limit)],
    )?;
    if let Ok(payload) = get_json(&agent, url.as_str()) {
        let market_values = payload
            .get("data")
            .or_else(|| payload.get("markets"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for market in market_values {
            if !looks_like_target(
                settings,
                value_opt_text(market.get("market_slug")),
                value_opt_text(market.get("question")),
            ) {
                continue;
            }
            if let Some(spec) = parse_clob_market(settings, &market) {
                markets.entry(spec.market_id.to_string()).or_insert(spec);
            }
        }
    }

    let now = Utc::now();
    let mut values: Vec<_> = markets
        .into_values()
        .filter(|market| market.end_ts > now)
        .collect();
    values.sort_by_key(|market| market.end_ts);
    Ok(values)
}

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
                    sender.send(FeedEvent::Reference(reference)).await.map_err(|_| FeedError::ChannelClosed)?;
                    sender.send(FeedEvent::Heartbeat { source, ts: Utc::now() }).await.map_err(|_| FeedError::ChannelClosed)?;
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
            sender
                .send(FeedEvent::Book(book))
                .await
                .map_err(|_| FeedError::ChannelClosed)?;
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
        sender
            .send(FeedEvent::Reference(reference))
            .await
            .map_err(|_| FeedError::ChannelClosed)?;
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

fn collect_gamma_event(
    settings: &RuntimeSettings,
    event: &Value,
    markets: &mut BTreeMap<String, MarketSpec>,
) {
    if !looks_like_target(
        settings,
        value_opt_text(event.get("slug")),
        value_opt_text(event.get("title")),
    ) {
        return;
    }
    let Some(items) = event.get("markets").and_then(Value::as_array) else {
        return;
    };
    for market in items {
        if !looks_like_target(
            settings,
            value_opt_text(market.get("slug").or_else(|| market.get("marketSlug"))),
            value_opt_text(market.get("question").or_else(|| event.get("title"))),
        ) {
            continue;
        }
        if let Some(spec) = parse_gamma_market(settings, event, market) {
            markets.insert(spec.market_id.to_string(), spec);
        }
    }
}

fn parse_gamma_market(
    settings: &RuntimeSettings,
    event: &Value,
    market: &Value,
) -> Option<MarketSpec> {
    let token_map = token_map_from_gamma(market);
    let (Some(up), Some(down)) = (token_map.get("up"), token_map.get("down")) else {
        return None;
    };
    let start_ts = parse_datetime(
        market
            .get("eventStartTime")
            .or_else(|| event.get("startTime"))
            .or_else(|| market.get("startTime"))
            .or_else(|| event.get("eventStartTime"))
            .or_else(|| market.get("startDate"))
            .or_else(|| event.get("startDate")),
    )?;
    let end_ts = parse_datetime(market.get("endDate").or_else(|| event.get("endDate")))?;
    let description = value_opt_text(
        market
            .get("description")
            .or_else(|| event.get("description")),
    );
    let accepting_orders = market
        .get("acceptingOrders")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let start_price = parse_start_price(description.as_deref());
    let status = status_for(start_price, accepting_orders, end_ts);
    Some(MarketSpec {
        asset: settings.target.asset.clone(),
        horizon: settings.target.horizon.clone(),
        event_id: value_opt_text(event.get("id")),
        event_slug: value_opt_text(event.get("slug")),
        market_id: MarketId::new(value_text(
            market
                .get("id")
                .or_else(|| market.get("conditionId"))
                .unwrap_or(&Value::Null),
        )),
        market_slug: value_opt_text(market.get("slug")),
        condition_id: value_text(market.get("conditionId").unwrap_or(&Value::Null)).into(),
        question: value_opt_text(market.get("question").or_else(|| event.get("title")))
            .unwrap_or_default(),
        description,
        up_token_id: TokenId::new(up.clone()),
        down_token_id: TokenId::new(down.clone()),
        start_ts,
        end_ts,
        start_price,
        resolution_source: settings.target.resolution_source.clone(),
        tick_size: decimal(market.get("orderPriceMinTickSize")).unwrap_or(Decimal::new(1, 2)),
        minimum_order_size: decimal(market.get("orderMinSize")).unwrap_or(Decimal::from(5)),
        neg_risk: market
            .get("negRisk")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fees_enabled: market
            .get("feesEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        accepting_orders,
        status,
        raw: BTreeMap::new(),
    })
}

fn parse_clob_market(settings: &RuntimeSettings, market: &Value) -> Option<MarketSpec> {
    let token_map = token_map_from_clob(market);
    let (Some(up), Some(down)) = (token_map.get("up"), token_map.get("down")) else {
        return None;
    };
    let end_ts = parse_datetime(market.get("end_date_iso").or_else(|| market.get("endDate")))?;
    let start_ts = parse_datetime(
        market
            .get("event_start_time")
            .or_else(|| market.get("start_time"))
            .or_else(|| market.get("game_start_time"))
            .or_else(|| market.get("startDate")),
    )
    .unwrap_or_else(|| end_ts - horizon_duration(settings));
    let description = value_opt_text(market.get("description"));
    let accepting_orders = market
        .get("accepting_orders")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let start_price = parse_start_price(description.as_deref());
    let status = status_for(start_price, accepting_orders, end_ts);
    Some(MarketSpec {
        asset: settings.target.asset.clone(),
        horizon: settings.target.horizon.clone(),
        event_id: None,
        event_slug: None,
        market_id: MarketId::new(value_text(
            market
                .get("condition_id")
                .or_else(|| market.get("question_id"))
                .or_else(|| market.get("market_slug"))
                .unwrap_or(&Value::Null),
        )),
        market_slug: value_opt_text(market.get("market_slug")),
        condition_id: value_text(market.get("condition_id").unwrap_or(&Value::Null)).into(),
        question: value_opt_text(market.get("question")).unwrap_or_default(),
        description,
        up_token_id: TokenId::new(up.clone()),
        down_token_id: TokenId::new(down.clone()),
        start_ts,
        end_ts,
        start_price,
        resolution_source: settings.target.resolution_source.clone(),
        tick_size: decimal(market.get("minimum_tick_size")).unwrap_or(Decimal::new(1, 2)),
        minimum_order_size: decimal(market.get("minimum_order_size")).unwrap_or(Decimal::from(5)),
        neg_risk: market
            .get("neg_risk")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fees_enabled: decimal(market.get("taker_base_fee")).unwrap_or(Decimal::ZERO)
            > Decimal::ZERO,
        accepting_orders,
        status,
        raw: BTreeMap::new(),
    })
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
        let mut book = books.get(&token_id).cloned().unwrap_or_else(|| BookState {
            token_id: token_id.clone(),
            bids: Vec::new(),
            asks: Vec::new(),
            last_trade_price: None,
            exchange_ts: None,
            local_ts: Utc::now(),
            book_hash: None,
        });
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
    let mut book = books.get(&token_id).cloned().unwrap_or_else(|| BookState {
        token_id: token_id.clone(),
        bids: Vec::new(),
        asks: Vec::new(),
        last_trade_price: None,
        exchange_ts: None,
        local_ts: Utc::now(),
        book_hash: None,
    });
    book.last_trade_price = Some(price);
    book.local_ts = Utc::now();
    books.insert(token_id, book.clone());
    vec![book]
}

fn gamma_event_queries(settings: &RuntimeSettings) -> Vec<Vec<(String, String)>> {
    let base = vec![
        ("active".to_owned(), "true".to_owned()),
        ("closed".to_owned(), "false".to_owned()),
        (
            "limit".to_owned(),
            settings.target.discovery_limit.to_string(),
        ),
    ];
    let mut queries = vec![
        with_extra(&base, "order", "volume24hr", "ascending", "false"),
        with_extra_one(&base, "tag_slug", "crypto"),
    ];
    for term in asset_terms(settings) {
        queries.push(with_extra_one(&base, "tag_slug", &slug_term(&term)));
    }
    for query in search_queries(settings) {
        queries.push(with_extra_one(&base, "q", &query));
    }
    dedupe_queries(queries)
}

fn with_extra(
    base: &[(String, String)],
    key_a: &str,
    value_a: &str,
    key_b: &str,
    value_b: &str,
) -> Vec<(String, String)> {
    let mut out = base.to_vec();
    out.push((key_a.to_owned(), value_a.to_owned()));
    out.push((key_b.to_owned(), value_b.to_owned()));
    out
}

fn with_extra_one(base: &[(String, String)], key: &str, value: &str) -> Vec<(String, String)> {
    let mut out = base.to_vec();
    out.push((key.to_owned(), value.to_owned()));
    out
}

fn dedupe_queries(queries: Vec<Vec<(String, String)>>) -> Vec<Vec<(String, String)>> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for mut query in queries {
        query.sort();
        if seen.insert(query.clone()) {
            output.push(query);
        }
    }
    output
}

fn search_queries(settings: &RuntimeSettings) -> Vec<String> {
    asset_terms(settings)
        .into_iter()
        .map(|asset| {
            let label = if asset.len() <= 5 {
                asset.to_ascii_uppercase()
            } else {
                title_case(&asset)
            };
            format!("{label} Up or Down {}", settings.target.horizon)
        })
        .collect()
}

fn asset_terms(settings: &RuntimeSettings) -> Vec<String> {
    let mut terms = BTreeSet::new();
    for term in [&settings.target.asset, &settings.target.asset_name] {
        let trimmed = term.trim().to_ascii_lowercase();
        if !trimmed.is_empty() {
            terms.insert(trimmed);
        }
    }
    terms.into_iter().collect()
}

fn looks_like_target(
    settings: &RuntimeSettings,
    slug: Option<String>,
    text: Option<String>,
) -> bool {
    let haystack = format!("{} {}", slug.unwrap_or_default(), text.unwrap_or_default());
    let compact = compact_term(&haystack);
    let horizon = compact_term(&settings.target.horizon);
    for asset in asset_terms(settings) {
        let asset_compact = compact_term(&asset);
        if compact.contains(&format!("{asset_compact}updown{horizon}"))
            || compact.contains(&format!("{asset_compact}upordown{horizon}"))
        {
            return true;
        }
    }
    let words = word_text(&haystack);
    let asset_match = asset_terms(settings)
        .iter()
        .any(|asset| words.contains(&format!("{} up or down", asset.to_ascii_lowercase())));
    if !asset_match {
        return false;
    }
    horizon_terms(settings)
        .iter()
        .any(|term| words.contains(term) || compact.contains(&compact_term(term)))
}

fn horizon_terms(settings: &RuntimeSettings) -> Vec<String> {
    let horizon = settings.target.horizon.to_ascii_lowercase();
    if let Some((amount, unit)) = split_horizon(&horizon) {
        if unit == "m" {
            return vec![
                horizon,
                format!("{amount} min"),
                format!("{amount} minute"),
                format!("{amount}-minute"),
            ];
        }
        return vec![
            horizon,
            format!("{amount} hr"),
            format!("{amount} hour"),
            format!("{amount}-hour"),
        ];
    }
    vec![horizon]
}

fn horizon_duration(settings: &RuntimeSettings) -> chrono::Duration {
    if let Some((amount, unit)) = split_horizon(&settings.target.horizon) {
        if unit == "h" {
            return chrono::Duration::hours(amount);
        }
        return chrono::Duration::minutes(amount);
    }
    chrono::Duration::minutes(15)
}

fn split_horizon(value: &str) -> Option<(i64, &str)> {
    let unit = value.chars().last()?;
    if unit != 'm' && unit != 'h' {
        return None;
    }
    value
        .get(..value.len() - 1)?
        .parse::<i64>()
        .ok()
        .map(|amount| (amount, if unit == 'm' { "m" } else { "h" }))
}

fn token_map_from_gamma(market: &Value) -> BTreeMap<String, String> {
    let outcomes = json_list(market.get("outcomes"))
        .into_iter()
        .map(|value| value_text(&value).to_ascii_lowercase())
        .collect::<Vec<_>>();
    let token_ids = json_list(market.get("clobTokenIds"))
        .into_iter()
        .map(|value| value_text(&value))
        .collect::<Vec<_>>();
    outcomes
        .into_iter()
        .zip(token_ids)
        .filter(|(outcome, _)| outcome == "up" || outcome == "down")
        .collect()
}

fn token_map_from_clob(market: &Value) -> BTreeMap<String, String> {
    let mut token_map = BTreeMap::new();
    let Some(tokens) = market.get("tokens").and_then(Value::as_array) else {
        return token_map;
    };
    for token in tokens {
        let outcome = value_text(token.get("outcome").unwrap_or(&Value::Null)).to_ascii_lowercase();
        let token_id = value_text(token.get("token_id").unwrap_or(&Value::Null));
        if outcome == "up" || outcome == "down" {
            token_map.insert(outcome, token_id);
        }
    }
    token_map
}

fn json_list(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(items)) => items.clone(),
        Some(Value::String(text)) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn parse_datetime(value: Option<&Value>) -> Option<DateTime<Utc>> {
    match value? {
        Value::Number(number) => number
            .as_f64()
            .and_then(|value| Utc.timestamp_opt(value as i64, 0).single()),
        Value::String(text) => parse_datetime_text(text),
        _ => None,
    }
}

fn parse_datetime_text(text: &str) -> Option<DateTime<Utc>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed)
        .map(|value| value.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|value| value.and_utc())
        })
}

fn parse_ms_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let raw = match value? {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.parse::<f64>().ok()?,
        _ => return None,
    };
    let seconds = if raw > 10_000_000_000.0 {
        raw / 1000.0
    } else {
        raw
    };
    Utc.timestamp_opt(seconds as i64, 0).single()
}

fn parse_event_ts(value: Option<&Value>) -> Option<DateTime<Utc>> {
    parse_ms_timestamp(value).or_else(|| parse_datetime(value))
}

fn parse_start_price(description: Option<&str>) -> Option<Decimal> {
    let description = description?;
    let re = Regex::new(
        r"(?i)(?:initial|starting|start|beginning|open|opening)\s+(?:price|value)[^\d$]{0,80}\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
    )
    .ok()?;
    re.captures(description)
        .and_then(|captures| captures.get(1))
        .and_then(|matched| Decimal::from_str_exact(&matched.as_str().replace(',', "")).ok())
        .filter(|value| *value > Decimal::ZERO)
}

fn status_for(
    start_price: Option<Decimal>,
    accepting_orders: bool,
    end_ts: DateTime<Utc>,
) -> MarketStatus {
    if end_ts <= Utc::now() {
        MarketStatus::Closed
    } else if start_price.is_some() && accepting_orders {
        MarketStatus::Tradeable
    } else {
        MarketStatus::ObserveOnly
    }
}

fn levels(value: Option<&Value>) -> Vec<BookLevel> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            Some(BookLevel {
                price: decimal(item.get("price"))?,
                size: decimal(item.get("size"))?,
            })
        })
        .collect()
}

fn decimal(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
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

fn extract_timestamp(payload: &Value) -> Option<DateTime<Utc>> {
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

fn websocket_json(message: Message) -> Option<Value> {
    match message {
        Message::Text(text) if text == "PING" || text == "PONG" => None,
        Message::Text(text) => serde_json::from_str(&text).ok(),
        Message::Binary(bytes) => serde_json::from_slice(&bytes).ok(),
        _ => None,
    }
}

fn get_json(agent: &ureq::Agent, url: &str) -> Result<Value, FeedError> {
    let response = agent.get(url).call().map_err(ureq_error)?;
    let text = response
        .into_string()
        .map_err(|error| FeedError::HttpTransport(error.to_string()))?;
    Ok(serde_json::from_str(&text)?)
}

fn ureq_error(error: ureq::Error) -> FeedError {
    match error {
        ureq::Error::Status(status, _) => FeedError::HttpStatus(status),
        ureq::Error::Transport(error) => FeedError::HttpTransport(error.to_string()),
    }
}

fn with_query(base: &str, params: &[(String, String)]) -> Result<Url, FeedError> {
    let mut url = Url::parse(base)?;
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in params {
            query.append_pair(key, value);
        }
    }
    Ok(url)
}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.to_owned(),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn value_opt_text(value: Option<&Value>) -> Option<String> {
    value
        .map(value_text)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn compact_term(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn word_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn slug_term(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
}

fn title_case(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    format!(
                        "{}{}",
                        first.to_ascii_uppercase(),
                        chars.as_str().to_ascii_lowercase()
                    )
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
