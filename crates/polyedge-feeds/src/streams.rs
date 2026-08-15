use crate::util::{
    decimal, levels, parse_datetime, parse_event_ts, parse_ms_timestamp, ureq_error,
    value_opt_text, value_text, websocket_json,
};
use crate::{FeedError, FeedEvent, FeedName, MarketChannelEvent};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use polyedge_config::RuntimeSettings;
use polyedge_domain::{BookLevel, BookState, ReferencePrice, TokenId};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

pub async fn run_rtds_feed(
    settings: RuntimeSettings,
    sender: mpsc::Sender<FeedEvent>,
) -> Result<(), FeedError> {
    run_rtds_feed_inner(settings, sender, None).await
}

async fn run_rtds_feed_inner(
    settings: RuntimeSettings,
    sender: mpsc::Sender<FeedEvent>,
    observation_processed: Option<mpsc::UnboundedSender<(usize, u64)>>,
) -> Result<(), FeedError> {
    if !settings.target.enable_polymarket_rtds_chainlink
        && !settings.target.enable_polymarket_rtds_binance
    {
        return Ok(());
    }
    if settings.target.enable_polymarket_rtds_chainlink
        && settings.target.enable_polymarket_rtds_binance
    {
        return Err(FeedError::SourceStalled(
            "redundant RTDS feed requires exactly one enabled logical source".to_owned(),
        ));
    }

    let source_timeout =
        Duration::from_secs_f64(settings.target.rtds_chainlink_watchdog_seconds.max(5.0));
    let (observation_sender, mut observations) = mpsc::channel(256);
    let mut connections = JoinSet::new();
    let mut generations = [0_u64, 0_u64];
    spawn_rtds_connection(
        &mut connections,
        0,
        generations[0],
        Duration::ZERO,
        settings.clone(),
        observation_sender.clone(),
    );
    spawn_rtds_connection(
        &mut connections,
        1,
        generations[1],
        Duration::from_secs(2),
        settings.clone(),
        observation_sender.clone(),
    );

    let mut running = [true, true];
    let mut last_observed: [Option<RtdsSlotObservation>; 2] = [None, None];
    let mut forward_state = RtdsForwardState::default();
    'supervisor: loop {
        tokio::select! {
            biased;
            Some((slot, generation, reference)) = observations.recv() => {
                if !rtds_slot_is_current(&running, &generations, slot, generation) {
                    continue;
                }
                last_observed[slot] = Some(RtdsSlotObservation {
                    arrived_at: Instant::now(),
                    key: reference_key(&reference),
                });
                if should_forward_rtds_reference(&reference, &mut forward_state) {
                    forward_rtds_reference(&sender, reference).await?;
                }
                if let Some(observation_processed) = &observation_processed {
                    let _ = observation_processed.send((slot, generation));
                }
            }
            result = connections.join_next() => {
                let Some(result) = result else {
                    return Err(rtds_continuity_error());
                };
                let (slot, generation, result) = result.map_err(|error| {
                    FeedError::SourceStalled(format!("RTDS connection task failed: {error}"))
                })?;
                if !rtds_slot_is_current(&running, &generations, slot, generation) {
                    continue;
                }
                let had_observation = last_observed[slot].is_some();
                running[slot] = false;
                last_observed[slot] = None;
                let peer = 1 - slot;
                if !rtds_slot_covers(
                    &running,
                    &last_observed,
                    peer,
                    forward_state.last.as_ref(),
                    source_timeout,
                ) {
                    let synchronization_deadline = Instant::now() + source_timeout;
                    let synchronization_timeout =
                        tokio::time::sleep_until(synchronization_deadline);
                    tokio::pin!(synchronization_timeout);
                    loop {
                        if rtds_synchronization_expired(synchronization_deadline) {
                            let peer_result = try_current_rtds_terminal_result(
                                &mut connections,
                                &running,
                                &generations,
                            );
                            return uncovered_rtds_result(result, peer_result);
                        }
                        tokio::select! {
                            biased;
                            Some((observed_slot, observed_generation, reference)) = observations.recv() => {
                                if !rtds_slot_is_current(
                                    &running,
                                    &generations,
                                    observed_slot,
                                    observed_generation,
                                ) {
                                    continue;
                                }
                                last_observed[observed_slot] = Some(RtdsSlotObservation {
                                    arrived_at: Instant::now(),
                                    key: reference_key(&reference),
                                });
                                if should_forward_rtds_reference(&reference, &mut forward_state) {
                                    forward_rtds_reference(&sender, reference).await?;
                                }
                                if let Some(observation_processed) = &observation_processed {
                                    let _ = observation_processed.send((observed_slot, observed_generation));
                                }
                                if rtds_slot_covers(
                                    &running,
                                    &last_observed,
                                    peer,
                                    forward_state.last.as_ref(),
                                    source_timeout,
                                ) {
                                    break;
                                }
                            }
                            peer_result = connections.join_next() => {
                                let Some(peer_result) = peer_result else {
                                    return uncovered_rtds_result(result, None);
                                };
                                let peer_result = match peer_result {
                                    Ok((peer_slot, peer_generation, peer_result))
                                        if rtds_slot_is_current(
                                            &running,
                                            &generations,
                                            peer_slot,
                                            peer_generation,
                                        ) => {
                                            Some(peer_result)
                                        }
                                    Ok(_) => continue,
                                    Err(error) => Some(Err(FeedError::SourceStalled(format!(
                                        "RTDS connection task failed: {error}"
                                    )))),
                                };
                                return uncovered_rtds_result(result, peer_result);
                            }
                            _ = &mut synchronization_timeout => {
                                let peer_result = try_current_rtds_terminal_result(
                                    &mut connections,
                                    &running,
                                    &generations,
                                );
                                return uncovered_rtds_result(result, peer_result);
                            }
                        }
                    }
                }
                if let Some(peer_result) = try_current_rtds_terminal_result(
                    &mut connections,
                    &running,
                    &generations,
                ) {
                    return uncovered_rtds_result(result, Some(peer_result));
                }

                tracing::warn!(
                    failed_slot = slot,
                    failed_generation = generation,
                    synchronized_slot = peer,
                    error = ?result.as_ref().err(),
                    "RTDS connection ended; fresh sequence-synchronized peer preserved feed continuity"
                );
                generations[slot] = generations[slot].wrapping_add(1);
                spawn_rtds_connection(
                    &mut connections,
                    slot,
                    generations[slot],
                    rtds_replacement_delay(had_observation),
                    settings.clone(),
                    observation_sender.clone(),
                );
                running[slot] = true;
                continue 'supervisor;
            }
        }
    }
}

#[derive(Clone)]
struct RtdsSlotObservation {
    arrived_at: Instant,
    key: (FeedName, chrono::DateTime<Utc>, Decimal),
}

const RTDS_MAX_PRICES_PER_SOURCE_TIMESTAMP: usize = 64;

#[derive(Default)]
struct RtdsForwardState {
    last: Option<(FeedName, chrono::DateTime<Utc>, Decimal)>,
    prices_at_timestamp: BTreeSet<Decimal>,
}

fn rtds_slot_covers(
    running: &[bool; 2],
    observations: &[Option<RtdsSlotObservation>; 2],
    slot: usize,
    last_forwarded: Option<&(FeedName, chrono::DateTime<Utc>, Decimal)>,
    source_timeout: Duration,
) -> bool {
    running[slot]
        && observations[slot].as_ref().is_some_and(|observation| {
            !source_watchdog_expired(observation.arrived_at, source_timeout)
                && last_forwarded
                    .is_some_and(|last| rtds_key_is_synchronized(&observation.key, last))
        })
}

fn rtds_key_is_synchronized(
    observed: &(FeedName, chrono::DateTime<Utc>, Decimal),
    last: &(FeedName, chrono::DateTime<Utc>, Decimal),
) -> bool {
    observed == last || (observed.0 == last.0 && observed.1 > last.1)
}

fn rtds_synchronization_expired(deadline: Instant) -> bool {
    Instant::now() >= deadline
}

fn rtds_continuity_error() -> FeedError {
    FeedError::SourceStalled(
        "RTDS continuity unavailable: no fresh sequence-synchronized connection".to_owned(),
    )
}

fn uncovered_rtds_result(
    current: Result<(), FeedError>,
    peer: Option<Result<(), FeedError>>,
) -> Result<(), FeedError> {
    if current.is_err() {
        current
    } else if peer.as_ref().is_some_and(Result::is_err) {
        peer.unwrap_or(Ok(()))
    } else {
        Err(rtds_continuity_error())
    }
}

fn try_current_rtds_terminal_result(
    connections: &mut JoinSet<(usize, u64, Result<(), FeedError>)>,
    running: &[bool; 2],
    generations: &[u64; 2],
) -> Option<Result<(), FeedError>> {
    while let Some(result) = connections.try_join_next() {
        match result {
            Ok((slot, generation, result))
                if rtds_slot_is_current(running, generations, slot, generation) =>
            {
                return Some(result);
            }
            Ok(_) => continue,
            Err(error) => {
                return Some(Err(FeedError::SourceStalled(format!(
                    "RTDS connection task failed: {error}"
                ))));
            }
        }
    }
    None
}

fn rtds_replacement_delay(had_observation: bool) -> Duration {
    if had_observation {
        Duration::ZERO
    } else {
        Duration::from_secs(2)
    }
}

fn should_forward_rtds_reference(reference: &ReferencePrice, state: &mut RtdsForwardState) -> bool {
    let key = reference_key(reference);
    let Some(last) = state.last.as_ref() else {
        state.prices_at_timestamp.insert(key.2);
        state.last = Some(key);
        return true;
    };
    if key.0 != last.0 || key.1 < last.1 {
        return false;
    }
    if key.1 > last.1 {
        state.prices_at_timestamp.clear();
        state.prices_at_timestamp.insert(key.2);
        state.last = Some(key);
        return true;
    }
    if state.prices_at_timestamp.contains(&key.2)
        || state.prices_at_timestamp.len() >= RTDS_MAX_PRICES_PER_SOURCE_TIMESTAMP
    {
        return false;
    }
    state.prices_at_timestamp.insert(key.2);
    state.last = Some(key);
    true
}

fn rtds_slot_is_current(
    running: &[bool; 2],
    generations: &[u64; 2],
    slot: usize,
    generation: u64,
) -> bool {
    running[slot] && generations[slot] == generation
}

fn spawn_rtds_connection(
    connections: &mut JoinSet<(usize, u64, Result<(), FeedError>)>,
    slot: usize,
    generation: u64,
    delay: Duration,
    settings: RuntimeSettings,
    sender: mpsc::Sender<(usize, u64, ReferencePrice)>,
) {
    connections.spawn(async move {
        tokio::time::sleep(delay).await;
        (
            slot,
            generation,
            run_rtds_connection(settings, slot, generation, sender).await,
        )
    });
}

async fn forward_rtds_reference(
    sender: &mpsc::Sender<FeedEvent>,
    reference: ReferencePrice,
) -> Result<(), FeedError> {
    let source = if reference.exact_resolution_source {
        FeedName::PolymarketRtdsChainlink
    } else {
        FeedName::PolymarketRtdsBinance
    };
    publish(sender, FeedEvent::Reference(reference)).await?;
    publish(
        sender,
        FeedEvent::Heartbeat {
            source,
            ts: Utc::now(),
        },
    )
    .await
}

fn reference_key(reference: &ReferencePrice) -> (FeedName, chrono::DateTime<Utc>, Decimal) {
    let source = if reference.exact_resolution_source {
        FeedName::PolymarketRtdsChainlink
    } else {
        FeedName::PolymarketRtdsBinance
    };
    (source, reference.source_ts, reference.price)
}

async fn run_rtds_connection(
    settings: RuntimeSettings,
    slot: usize,
    generation: u64,
    sender: mpsc::Sender<(usize, u64, ReferencePrice)>,
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
            "type": "update",
            "filters": json!({"symbol": settings.target.binance_symbol}).to_string()
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
    let source_timeout =
        Duration::from_secs_f64(settings.target.rtds_chainlink_watchdog_seconds.max(5.0));
    let mut source_watchdog = tokio::time::interval(Duration::from_secs(1));
    source_watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_chainlink = Instant::now();
    let mut last_binance = Instant::now();
    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Text("PING".to_owned())).await?;
            }
            _ = source_watchdog.tick() => {
                if settings.target.enable_polymarket_rtds_chainlink
                    && source_watchdog_expired(last_chainlink, source_timeout)
                {
                    return Err(FeedError::SourceStalled(format!(
                        "polymarket RTDS Chainlink produced no matching update for {:.0}s while the socket remained connected",
                        source_timeout.as_secs_f64()
                    )));
                }
                if settings.target.enable_polymarket_rtds_binance
                    && source_watchdog_expired(last_binance, source_timeout)
                {
                    return Err(FeedError::SourceStalled(format!(
                        "polymarket RTDS Binance produced no matching update for {:.0}s while the socket remained connected",
                        source_timeout.as_secs_f64()
                    )));
                }
            }
            message = read.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let message = message?;
                if let Some(reference) = parse_rtds_message(message, &settings) {
                    if reference.exact_resolution_source {
                        last_chainlink = Instant::now();
                    } else {
                        last_binance = Instant::now();
                    }
                    sender
                        .send((slot, generation, reference))
                        .await
                        .map_err(|_| FeedError::ChannelClosed)?;
                }
            }
        }
    }
}

fn source_watchdog_expired(last_observation: Instant, timeout: Duration) -> bool {
    last_observation.elapsed() >= timeout
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
    let ping_loop = async move {
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ping.tick().await;
            write.send(Message::Text("PING".to_owned())).await?;
        }
    };
    let read_loop = async move {
        let mut books = BTreeMap::new();
        while let Some(message) = read.next().await {
            for event in parse_market_message(message?, &mut books) {
                publish(&sender, event).await?;
            }
        }
        Ok::<(), FeedError>(())
    };
    tokio::select! {
        result = ping_loop => result,
        result = read_loop => result,
    }
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
) -> Vec<FeedEvent> {
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

fn handle_market_event(event: &Value, books: &mut BTreeMap<TokenId, BookState>) -> Vec<FeedEvent> {
    let event_type = event
        .get("event_type")
        .or_else(|| event.get("type"))
        .map(value_text)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mut events = market_channel_events(event, &event_type)
        .into_iter()
        .map(FeedEvent::RawMarketEvent)
        .collect::<Vec<_>>();
    let books = match event_type.as_str() {
        "book" | "orderbook" | "snapshot" => {
            let book = book_from_snapshot(event);
            books.insert(book.token_id.clone(), book.clone());
            vec![book]
        }
        "price_change" | "pricechange" => apply_price_change(event, books),
        "last_trade_price" | "trade" | "last_trade" => apply_last_trade(event, books),
        _ => Vec::new(),
    };
    events.extend(books.into_iter().map(FeedEvent::Book));
    events
}

fn market_channel_events(event: &Value, event_type: &str) -> Vec<MarketChannelEvent> {
    if matches!(event_type, "price_change" | "pricechange") {
        if let Some(changes) = event
            .get("price_changes")
            .or_else(|| event.get("changes"))
            .and_then(Value::as_array)
        {
            return changes
                .iter()
                .map(|change| market_channel_event(event, event_type, change))
                .collect();
        }
    }
    vec![market_channel_event(event, event_type, event)]
}

fn market_channel_event(event: &Value, event_type: &str, focus: &Value) -> MarketChannelEvent {
    let change = focus;
    MarketChannelEvent {
        event_type: if event_type.is_empty() {
            "unknown".to_owned()
        } else {
            event_type.to_owned()
        },
        recorded_ts: Utc::now(),
        source_ts: parse_event_ts(
            change
                .get("timestamp")
                .or_else(|| change.get("ts"))
                .or_else(|| event.get("timestamp"))
                .or_else(|| event.get("ts")),
        ),
        market_id: value_opt_text(change.get("market_id").or_else(|| event.get("market_id"))),
        condition_id: value_opt_text(
            change
                .get("condition_id")
                .or_else(|| event.get("condition_id")),
        ),
        token_id: value_opt_text(change.get("token_id").or_else(|| event.get("token_id"))),
        asset_id: value_opt_text(change.get("asset_id").or_else(|| event.get("asset_id"))),
        side: value_opt_text(change.get("side").or_else(|| event.get("side"))),
        price: decimal(change.get("price"))
            .or_else(|| decimal(event.get("price").or_else(|| event.get("last_trade_price"))))
            .map(|value| value.to_string()),
        size: decimal(change.get("size"))
            .or_else(|| {
                decimal(
                    event
                        .get("size")
                        .or_else(|| event.get("trade_size"))
                        .or_else(|| event.get("last_trade_size")),
                )
            })
            .map(|value| value.to_string()),
        best_bid: decimal(change.get("best_bid").or_else(|| event.get("best_bid")))
            .map(|value| value.to_string()),
        best_ask: decimal(change.get("best_ask").or_else(|| event.get("best_ask")))
            .map(|value| value.to_string()),
        book_hash: value_opt_text(change.get("hash").or_else(|| event.get("hash"))),
        raw_payload: focus.clone(),
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
        Some(Value::Object(change)) => vec![Value::Object(change.clone())],
        _ => vec![event.clone()],
    };
    let mut updated_tokens = std::collections::BTreeSet::new();
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
        let book = books
            .entry(token_id.clone())
            .or_insert_with(|| empty_book(token_id.clone()));
        if let (Some(side), Some(price), Some(size)) = (
            change.get("side").and_then(Value::as_str),
            decimal(change.get("price")),
            decimal(change.get("size")),
        ) {
            let levels = if side.eq_ignore_ascii_case("buy") {
                &mut book.bids
            } else if side.eq_ignore_ascii_case("sell") {
                &mut book.asks
            } else {
                continue;
            };
            levels.retain(|level| level.price != price);
            if size > Decimal::ZERO {
                levels.push(BookLevel { price, size });
            }
            if side.eq_ignore_ascii_case("buy") {
                levels.sort_by_key(|level| std::cmp::Reverse(level.price));
            } else {
                levels.sort_by_key(|level| level.price);
            }
        }
        book.exchange_ts =
            parse_event_ts(change.get("timestamp").or_else(|| event.get("timestamp")));
        book.local_ts = Utc::now();
        book.book_hash = value_opt_text(event.get("hash").or_else(|| change.get("hash")))
            .or_else(|| book.book_hash.clone());
        updated_tokens.insert(token_id);
    }
    updated_tokens
        .into_iter()
        .filter_map(|token_id| books.get(&token_id).cloned())
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_change_preserves_depth_and_emits_each_child_as_raw_evidence() {
        let mut books = BTreeMap::new();
        let snapshot = json!({
            "event_type": "book",
            "asset_id": "token",
            "bids": [
                {"price": "0.49", "size": "10"},
                {"price": "0.48", "size": "8"}
            ],
            "asks": [{"price": "0.51", "size": "7"}]
        });
        handle_market_event(&snapshot, &mut books);

        let change = json!({
            "event_type": "price_change",
            "timestamp": "2026-07-10T00:00:00Z",
            "price_changes": [
                {"asset_id": "token", "side": "BUY", "price": "0.49", "size": "6"},
                {"asset_id": "token", "side": "BUY", "price": "0.50", "size": "4"}
            ]
        });
        let events = handle_market_event(&change, &mut books);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, FeedEvent::RawMarketEvent(_)))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, FeedEvent::Book(_)))
                .count(),
            1
        );
        let book = books.get(&TokenId::new("token")).unwrap();
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.bids[0].price, Decimal::new(50, 2));
        assert_eq!(book.bids[0].size, Decimal::from(4));
        assert_eq!(book.bids[1].price, Decimal::new(49, 2));
        assert_eq!(book.bids[1].size, Decimal::from(6));
        assert_eq!(book.bids[2].price, Decimal::new(48, 2));
    }

    #[test]
    fn zero_size_price_change_removes_only_the_target_level() {
        let token = TokenId::new("token");
        let mut books = BTreeMap::from([(
            token.clone(),
            BookState {
                token_id: token.clone(),
                bids: vec![
                    BookLevel {
                        price: Decimal::new(50, 2),
                        size: Decimal::from(4),
                    },
                    BookLevel {
                        price: Decimal::new(49, 2),
                        size: Decimal::from(6),
                    },
                ],
                asks: Vec::new(),
                last_trade_price: None,
                exchange_ts: None,
                local_ts: Utc::now(),
                book_hash: None,
            },
        )]);
        apply_price_change(
            &json!({
                "price_changes": [
                    {"asset_id": "token", "side": "BUY", "price": "0.50", "size": "0"}
                ]
            }),
            &mut books,
        );
        let book = books.get(&token).unwrap();
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].price, Decimal::new(49, 2));
    }

    #[test]
    fn binance_rtds_update_matches_the_explicit_subscription_symbol() {
        let settings = RuntimeSettings::default();
        let reference = parse_rtds_message(
            Message::Text(
                json!({
                    "topic": "crypto_prices",
                    "type": "update",
                    "timestamp": 1_786_687_200_000_i64,
                    "payload": {
                        "symbol": "btcusdt",
                        "value": 118500.25,
                        "timestamp": 1_786_687_200_000_i64
                    }
                })
                .to_string(),
            ),
            &settings,
        )
        .expect("documented Binance RTDS update did not match the configured symbol");
        assert_eq!(reference.source, settings.rtds_binance_source_name());
        assert_eq!(reference.price, Decimal::new(11_850_025, 2));
        assert!(!reference.exact_resolution_source);
    }

    #[test]
    fn direct_price_change_is_applied_and_child_fields_take_priority() {
        let mut books = BTreeMap::new();
        let direct = json!({
            "event_type": "price_change",
            "asset_id": "token",
            "side": "BUY",
            "price": "0.50",
            "size": "3"
        });
        handle_market_event(&direct, &mut books);
        assert_eq!(books[&TokenId::new("token")].bids[0].size, Decimal::from(3));

        let nested = json!({
            "event_type": "price_change",
            "asset_id": "wrong-parent-token",
            "price": "0.10",
            "size": "99",
            "price_changes": [
                {"asset_id": "token", "side": "BUY", "price": "0.50", "size": "2"}
            ]
        });
        let events = handle_market_event(&nested, &mut books);
        let raw = events
            .iter()
            .find_map(|event| match event {
                FeedEvent::RawMarketEvent(event) => Some(event),
                _ => None,
            })
            .unwrap();
        assert_eq!(raw.asset_id.as_deref(), Some("token"));
        assert_eq!(raw.price.as_deref(), Some("0.50"));
        assert_eq!(raw.size.as_deref(), Some("2"));
        assert_eq!(books[&TokenId::new("token")].bids[0].size, Decimal::from(2));
    }

    #[tokio::test]
    async fn binance_connected_socket_without_matching_updates_exits_for_reconnect() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(8)))
                .unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let subscription: Value =
                serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(subscription["subscriptions"].as_array().unwrap().len(), 1);
            assert_eq!(subscription["subscriptions"][0]["topic"], "crypto_prices");
            assert_eq!(subscription["subscriptions"][0]["type"], "update");
            assert_eq!(
                subscription["subscriptions"][0]["filters"],
                r#"{"symbol":"btcusdt"}"#
            );
            while socket.read().is_ok() {}
        });

        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_rtds_url = format!("ws://{address}");
        settings.target.enable_polymarket_rtds_chainlink = false;
        settings.target.enable_polymarket_rtds_binance = true;
        settings.target.rtds_chainlink_watchdog_seconds = 5.0;
        settings.target.rtds_ping_interval_seconds = 1.0;
        let (sender, _receiver) = mpsc::channel(8);

        let error = tokio::time::timeout(
            Duration::from_secs(8),
            run_rtds_connection(settings, 0, 0, sender),
        )
        .await
        .expect("Binance watchdog did not return the stalled socket for reconnect")
        .unwrap_err();
        match error {
            FeedError::SourceStalled(message) => {
                assert!(message.contains("RTDS Binance"));
                assert!(message.contains("5s"));
            }
            other => panic!("unexpected RTDS result: {other}"),
        }
        server.join().unwrap();
    }

    #[test]
    fn replaced_rtds_slot_rejects_late_old_generation() {
        let running = [true, true];
        let generations = [1, 0];
        assert!(!rtds_slot_is_current(&running, &generations, 0, 0));
        assert!(rtds_slot_is_current(&running, &generations, 0, 1));
    }

    #[test]
    fn observed_rtds_slot_reconnects_immediately_but_never_healthy_slot_backs_off() {
        assert_eq!(rtds_replacement_delay(true), Duration::ZERO);
        assert_eq!(rtds_replacement_delay(false), Duration::from_secs(2));
    }

    #[test]
    fn rtds_a_b_a_same_timestamp_forwards_only_a_b_and_replay_a_does_not_cover_b() {
        let now = Utc::now();
        let reference = |source_ts, price| ReferencePrice {
            source: "polymarket_rtds_binance_btcusdt".to_owned(),
            price,
            source_ts,
            local_ts: now,
            latency_ms: 0.0,
            stale: false,
            exact_resolution_source: false,
            quality_flags: Vec::new(),
        };
        let mut state = RtdsForwardState::default();
        assert!(should_forward_rtds_reference(
            &reference(now, Decimal::new(100, 0)),
            &mut state,
        ));
        assert!(!should_forward_rtds_reference(
            &reference(
                now - chrono::Duration::milliseconds(1),
                Decimal::new(101, 0)
            ),
            &mut state,
        ));
        assert!(!should_forward_rtds_reference(
            &reference(now, Decimal::new(100, 0)),
            &mut state,
        ));
        assert!(should_forward_rtds_reference(
            &reference(now, Decimal::new(101, 0)),
            &mut state,
        ));
        assert!(!should_forward_rtds_reference(
            &reference(now, Decimal::new(100, 0)),
            &mut state,
        ));
        let replaying_peer = [
            Some(RtdsSlotObservation {
                arrived_at: Instant::now(),
                key: reference_key(&reference(now, Decimal::new(100, 0))),
            }),
            None,
        ];
        assert!(!rtds_slot_covers(
            &[true, true],
            &replaying_peer,
            0,
            state.last.as_ref(),
            Duration::from_secs(5),
        ));
        assert!(should_forward_rtds_reference(
            &reference(
                now + chrono::Duration::milliseconds(1),
                Decimal::new(100, 0)
            ),
            &mut state,
        ));
        assert_eq!(state.prices_at_timestamp.len(), 1);
    }

    #[test]
    fn rtds_same_timestamp_correction_history_is_bounded_until_time_advances() {
        let now = Utc::now();
        let reference = |source_ts, price| ReferencePrice {
            source: "polymarket_rtds_binance_btcusdt".to_owned(),
            price,
            source_ts,
            local_ts: now,
            latency_ms: 0.0,
            stale: false,
            exact_resolution_source: false,
            quality_flags: Vec::new(),
        };
        let mut state = RtdsForwardState::default();
        for price in 0..RTDS_MAX_PRICES_PER_SOURCE_TIMESTAMP {
            assert!(should_forward_rtds_reference(
                &reference(now, Decimal::from(price)),
                &mut state,
            ));
        }
        assert!(!should_forward_rtds_reference(
            &reference(now, Decimal::from(RTDS_MAX_PRICES_PER_SOURCE_TIMESTAMP + 1),),
            &mut state,
        ));
        assert_eq!(
            state.prices_at_timestamp.len(),
            RTDS_MAX_PRICES_PER_SOURCE_TIMESTAMP,
        );
        assert!(should_forward_rtds_reference(
            &reference(
                now + chrono::Duration::milliseconds(1),
                Decimal::new(100, 0),
            ),
            &mut state,
        ));
        assert_eq!(state.prices_at_timestamp.len(), 1);
    }

    #[test]
    fn rtds_sequence_readiness_rejects_replaying_and_lagging_peers() {
        let now = Utc::now();
        let last = (FeedName::PolymarketRtdsBinance, now, Decimal::new(102, 0));
        let running = [true, true];
        let observations = [
            Some(RtdsSlotObservation {
                arrived_at: Instant::now(),
                key: (
                    FeedName::PolymarketRtdsBinance,
                    now - chrono::Duration::milliseconds(1),
                    Decimal::new(101, 0),
                ),
            }),
            None,
        ];
        assert!(!rtds_slot_covers(
            &running,
            &observations,
            0,
            Some(&last),
            Duration::from_secs(5),
        ));

        let stale = [
            Some(RtdsSlotObservation {
                arrived_at: Instant::now() - Duration::from_secs(6),
                key: last.clone(),
            }),
            None,
        ];
        assert!(!rtds_slot_covers(
            &running,
            &stale,
            0,
            Some(&last),
            Duration::from_secs(5),
        ));

        let synchronized = [
            Some(RtdsSlotObservation {
                arrived_at: Instant::now(),
                key: last.clone(),
            }),
            None,
        ];
        assert!(rtds_slot_covers(
            &running,
            &synchronized,
            0,
            Some(&last),
            Duration::from_secs(5),
        ));
        assert!(rtds_key_is_synchronized(
            &(
                FeedName::PolymarketRtdsBinance,
                now + chrono::Duration::milliseconds(1),
                Decimal::new(103, 0),
            ),
            &last,
        ));
        assert!(!rtds_key_is_synchronized(
            &(FeedName::PolymarketRtdsBinance, now, Decimal::new(101, 0),),
            &last,
        ));
        assert!(!rtds_key_is_synchronized(
            &(
                FeedName::PolymarketRtdsChainlink,
                now + chrono::Duration::milliseconds(1),
                Decimal::new(103, 0),
            ),
            &last,
        ));
    }

    #[tokio::test]
    async fn rtds_synchronization_deadline_survives_a_continuous_replay_flood() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (replays, mut replay_receiver) = mpsc::channel(1_024);
        let stop = Arc::new(AtomicBool::new(false));
        let producer_stop = Arc::clone(&stop);
        let producer = std::thread::spawn(move || {
            while !producer_stop.load(Ordering::Relaxed) {
                if replays.blocking_send(()).is_err() {
                    break;
                }
            }
        });
        let started = Instant::now();
        let deadline = started + Duration::from_millis(25);
        let timeout = tokio::time::sleep_until(deadline);
        tokio::pin!(timeout);
        let mut replay_count = 0_u64;
        loop {
            if rtds_synchronization_expired(deadline) {
                break;
            }
            tokio::select! {
                biased;
                Some(()) = replay_receiver.recv() => replay_count += 1,
                _ = &mut timeout => break,
            }
        }
        stop.store(true, Ordering::Relaxed);
        drop(replay_receiver);
        producer.join().unwrap();

        assert!(replay_count > 0);
        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[tokio::test]
    async fn rtds_expired_deadline_drain_preserves_a_queued_peer_error() {
        let mut connections = JoinSet::new();
        let (queued, mut queued_receiver) = mpsc::unbounded_channel();
        connections.spawn(async move {
            queued.send(()).unwrap();
            (
                1,
                0,
                Err(FeedError::SourceStalled("queued peer error".to_owned())),
            )
        });
        queued_receiver
            .recv()
            .await
            .expect("peer task did not queue its terminal result");
        tokio::task::yield_now().await;

        let deadline = Instant::now() - Duration::from_millis(1);
        assert!(rtds_synchronization_expired(deadline));
        let peer_result =
            try_current_rtds_terminal_result(&mut connections, &[false, true], &[0, 0]);
        let result = uncovered_rtds_result(Ok(()), peer_result);
        assert!(matches!(
            result,
            Err(FeedError::SourceStalled(message)) if message == "queued peer error"
        ));
    }

    #[test]
    fn both_ended_rtds_results_prefer_real_errors() {
        let result = uncovered_rtds_result(
            Ok(()),
            Some(Err(FeedError::SourceStalled("peer error".to_owned()))),
        );
        assert!(matches!(
            result,
            Err(FeedError::SourceStalled(message)) if message == "peer error"
        ));
        let result = uncovered_rtds_result(
            Err(FeedError::SourceStalled("current error".to_owned())),
            Some(Err(FeedError::SourceStalled("peer error".to_owned()))),
        );
        assert!(matches!(
            result,
            Err(FeedError::SourceStalled(message)) if message == "current error"
        ));
        let result = uncovered_rtds_result(Ok(()), Some(Ok(())));
        assert!(matches!(
            result,
            Err(FeedError::SourceStalled(message)) if message.contains("no fresh sequence-synchronized connection")
        ));
    }

    #[tokio::test]
    async fn rtds_feed_rejects_ambiguous_dual_topic_pooling() {
        let (sender, _receiver) = mpsc::channel(1);
        let error = run_rtds_feed(RuntimeSettings::default(), sender)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            FeedError::SourceStalled(message) if message.contains("exactly one enabled")
        ));
    }

    #[tokio::test]
    async fn rtds_merge_survives_peer_sync_after_legacy_handoff_window_without_duplicates() {
        enum Command {
            Send { timestamp: i64, price: &'static str },
            Close,
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (primary, primary_commands) = std::sync::mpsc::channel();
        let (secondary, secondary_commands) = std::sync::mpsc::channel();
        let (connected, mut connections) = mpsc::unbounded_channel();
        let (primary_closed, mut primary_closures) = mpsc::unbounded_channel();
        let server = std::thread::spawn(move || {
            let mut handlers = Vec::new();
            let mut command_receivers = [Some(primary_commands), Some(secondary_commands)];
            for slot in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                let commands = command_receivers[slot].take().unwrap();
                let connected = connected.clone();
                let primary_closed = primary_closed.clone();
                handlers.push(std::thread::spawn(move || {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(8)))
                        .unwrap();
                    let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
                    let subscription: Value =
                        serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
                    assert_eq!(subscription["subscriptions"][0]["topic"], "crypto_prices");
                    connected.send(slot).unwrap();
                    while let Ok(command) = commands.recv_timeout(Duration::from_secs(8)) {
                        match command {
                            Command::Send { timestamp, price } => socket
                                .send(Message::Text(
                                    json!({
                                        "topic": "crypto_prices",
                                        "type": "update",
                                        "timestamp": timestamp,
                                        "payload": {
                                            "symbol": "btcusdt",
                                            "value": price,
                                            "timestamp": timestamp
                                        }
                                    })
                                    .to_string(),
                                ))
                                .unwrap(),
                            Command::Close => break,
                        }
                    }
                    drop(socket);
                    if slot == 0 {
                        primary_closed.send(()).unwrap();
                    }
                }));
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });

        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_rtds_url = format!("ws://{address}");
        settings.target.enable_polymarket_rtds_chainlink = false;
        settings.target.enable_polymarket_rtds_binance = true;
        settings.target.rtds_chainlink_watchdog_seconds = 5.0;
        settings.target.rtds_ping_interval_seconds = 60.0;
        let (sender, mut receiver) = mpsc::channel(16);
        let (observation_processed, mut processed_observations) = mpsc::unbounded_channel();
        let feed = tokio::spawn(run_rtds_feed_inner(
            settings,
            sender,
            Some(observation_processed),
        ));

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), connections.recv())
                .await
                .expect("primary RTDS connection was not established"),
            Some(0)
        );
        primary
            .send(Command::Send {
                timestamp: 1_786_687_200_000,
                price: "118500.25",
            })
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), processed_observations.recv())
                .await
                .expect("primary observation was not processed"),
            Some((0, 0))
        );
        let mut reference_prices = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            if let FeedEvent::Reference(reference) = event {
                reference_prices.push(reference.price);
            }
        }
        assert_eq!(reference_prices, [Decimal::new(11_850_025, 2)]);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(4), connections.recv())
                .await
                .expect("secondary RTDS connection was not established"),
            Some(1)
        );
        secondary
            .send(Command::Send {
                timestamp: 1_786_687_200_000,
                price: "118501.25",
            })
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), processed_observations.recv())
                .await
                .expect("newer secondary observation was not processed"),
            Some((1, 0))
        );
        while let Ok(event) = receiver.try_recv() {
            if let FeedEvent::Reference(reference) = event {
                reference_prices.push(reference.price);
            }
        }
        assert_eq!(
            reference_prices,
            [Decimal::new(11_850_025, 2), Decimal::new(11_850_125, 2)]
        );

        primary
            .send(Command::Send {
                timestamp: 1_786_687_200_000,
                price: "118500.25",
            })
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), processed_observations.recv())
                .await
                .expect("replayed primary observation was not processed"),
            Some((0, 0))
        );
        while let Ok(event) = receiver.try_recv() {
            assert!(!matches!(event, FeedEvent::Reference(_)));
        }

        primary
            .send(Command::Send {
                timestamp: 1_786_687_202_000,
                price: "118502.25",
            })
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), processed_observations.recv())
                .await
                .expect("new primary observation was not processed"),
            Some((0, 0))
        );
        while let Ok(event) = receiver.try_recv() {
            if let FeedEvent::Reference(reference) = event {
                reference_prices.push(reference.price);
            }
        }

        primary.send(Command::Close).unwrap();
        tokio::time::timeout(Duration::from_secs(2), primary_closures.recv())
            .await
            .expect("primary test socket did not close")
            .expect("primary close coordinator ended unexpectedly");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !feed.is_finished(),
            "feed stopped before its source-timeout handoff window elapsed"
        );
        secondary
            .send(Command::Send {
                timestamp: 1_786_687_202_000,
                price: "118502.25",
            })
            .unwrap();
        let synchronized =
            tokio::time::timeout(Duration::from_secs(2), processed_observations.recv())
                .await
                .expect("queued peer synchronization observation was not processed");
        assert_eq!(synchronized, Some((1, 0)));
        while let Ok(event) = receiver.try_recv() {
            assert!(!matches!(event, FeedEvent::Reference(_)));
        }
        assert!(!feed.is_finished());

        secondary
            .send(Command::Send {
                timestamp: 1_786_687_203_000,
                price: "118503.25",
            })
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), processed_observations.recv())
                .await
                .expect("post-failover secondary observation was not processed"),
            Some((1, 0))
        );
        while let Ok(event) = receiver.try_recv() {
            if let FeedEvent::Reference(reference) = event {
                reference_prices.push(reference.price);
            }
        }
        assert_eq!(
            reference_prices,
            [
                Decimal::new(11_850_025, 2),
                Decimal::new(11_850_125, 2),
                Decimal::new(11_850_225, 2),
                Decimal::new(11_850_325, 2),
            ]
        );

        feed.abort();
        let _ = feed.await;
        let _ = secondary.send(Command::Close);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn market_socket_sends_required_ping() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(12)))
                .unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let subscription: Value =
                serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(subscription["type"], "market");
            assert_eq!(subscription["assets_ids"], json!(["token"]));
            assert_eq!(socket.read().unwrap(), Message::Text("PING".to_owned()));
            let first_ping = Instant::now();
            socket.send(Message::Text("PONG".to_owned())).unwrap();
            assert_eq!(socket.read().unwrap(), Message::Text("PING".to_owned()));
            assert!(first_ping.elapsed() >= Duration::from_secs(9));
            socket.close(None).unwrap();
        });

        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        let (sender, _receiver) = mpsc::channel(8);

        tokio::time::timeout(
            Duration::from_secs(15),
            run_market_feed(settings, vec![TokenId::new("token")], sender),
        )
        .await
        .expect("market socket did not send its required PING")
        .unwrap();
        server.join().unwrap();
    }
}
#[test]
fn source_watchdog_is_independent_of_other_topic_activity() {
    let timeout = Duration::from_secs(30);
    assert!(!source_watchdog_expired(Instant::now(), timeout));
    assert!(source_watchdog_expired(
        Instant::now() - Duration::from_secs(31),
        timeout
    ));
}
