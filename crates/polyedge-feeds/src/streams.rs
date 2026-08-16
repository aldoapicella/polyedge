use crate::util::{
    decimal, levels, parse_datetime, parse_event_ts, parse_ms_timestamp, ureq_error,
    value_opt_text, value_text, websocket_json,
};
use crate::{
    ClobGenerationLease, ClobResyncBarrier, FeedError, FeedEvent, FeedName, MarketChannelEvent,
};
use chrono::Utc;
use futures_util::{Sink, SinkExt, StreamExt};
use polyedge_config::RuntimeSettings;
use polyedge_domain::{BookLevel, BookState, ReferencePrice, TokenId};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const MARKET_RESYNC_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const MARKET_RESYNC_BUFFER_AGE: Duration = Duration::from_secs(5);
const MARKET_ANCHOR_MAX_AGE: Duration = Duration::from_secs(15);
const MARKET_RESYNC_MAX_FRAMES: usize = 512;
const MARKET_RETAINED_DELTA_BYTES: usize = 1024 * 1024;
const MARKET_RETAINED_DELTA_CHILDREN: usize = 1024;
const MARKET_RETAINED_FIELD_BYTES: usize = 4 * 1024;
const MARKET_READY_HEARTBEAT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

struct ClobTerminalGuard(ClobGenerationLease);

impl Drop for ClobTerminalGuard {
    fn drop(&mut self) {
        self.0.terminate();
    }
}

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
    run_market_feed_generation(settings, token_ids, 1, sender).await
}

pub async fn run_market_feed_generation(
    settings: RuntimeSettings,
    token_ids: Vec<TokenId>,
    generation: u64,
    sender: mpsc::Sender<FeedEvent>,
) -> Result<(), FeedError> {
    run_market_feed_generation_with_lease(
        settings,
        token_ids,
        generation,
        ClobGenerationLease::new(),
        sender,
    )
    .await
}

pub async fn run_market_feed_generation_with_lease(
    settings: RuntimeSettings,
    token_ids: Vec<TokenId>,
    generation: u64,
    lease: ClobGenerationLease,
    sender: mpsc::Sender<FeedEvent>,
) -> Result<(), FeedError> {
    let _terminal_guard = ClobTerminalGuard(lease.clone());
    #[cfg(test)]
    if generation == u64::MAX {
        panic!("injected CLOB generation task panic");
    }
    // This one deadline covers connection, subscription, snapshot collection,
    // barrier enqueue, and authorization.  A full runtime event channel must
    // never turn a bounded resync into an unbounded producer await.
    let generation_deadline = Instant::now() + MARKET_ANCHOR_MAX_AGE;
    if token_ids.is_empty() {
        return Ok(());
    }
    let expected_tokens = exact_token_set(&token_ids)?;
    let token_set_digest = token_set_digest(&expected_tokens);
    let token_texts: Vec<_> = token_ids.iter().map(ToString::to_string).collect();
    let subscribe = json!({
        "assets_ids": token_texts,
        "type": "market",
        "custom_feature_enabled": true
    })
    .to_string();
    let source_timeout =
        Duration::from_secs_f64(settings.target.polymarket_market_watchdog_seconds.max(5.0));
    let (stream, _) = tokio::time::timeout_at(
        generation_deadline,
        connect_async(settings.target.polymarket_ws_url.as_str()),
    )
    .await
    .map_err(|_| {
        FeedError::MarketProtocol("CLOB generation exceeded its absolute deadline".to_owned())
    })??;
    let (mut write, mut read) = stream.split();
    websocket_send_before_deadline(&mut write, generation_deadline, Message::Text(subscribe))
        .await?;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut source_watchdog = tokio::time::interval(Duration::from_secs(1));
    source_watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_observation = Instant::now();
    let mut books = BTreeMap::new();
    let mut anchors = BTreeMap::new();
    let mut sequence = 0_u64;
    let mut ready_ack: Option<oneshot::Receiver<Result<(), String>>> = None;
    let mut buffered = VecDeque::new();
    let mut anchored_deltas: VecDeque<Value> = VecDeque::new();
    let mut buffered_bytes = 0_usize;
    let mut retained_delta_bytes = 0_usize;
    // Before the runtime authorizes this generation, every received frame is
    // part of the untrusted resync interval.  Count control and data frames
    // alike: otherwise a peer could keep the connection alive indefinitely
    // with non-data traffic while withholding one required snapshot.
    let mut resync_bytes = 0_usize;
    let mut resync_frames = 0_usize;
    let mut barrier_started: Option<Instant> = None;
    let mut authorized = false;
    loop {
        tokio::select! {
            biased;
            result = async { ready_ack.as_mut().expect("guarded above").await }, if ready_ack.is_some() => {
                match result {
                    Ok(Ok(())) => {
                        // The producer emits nothing after the barrier until this exact
                        // authorization arrives; therefore the queue position is the final
                        // drain boundary, not a best-effort timing assumption.
                        ready_ack = None;
                        barrier_started = None;
                        authorized = true;
                        while let Some(message) = buffered.pop_front() {
                            let events = strict_market_events(message, &expected_tokens, &mut books)?;
                            forward_market_events(&sender, generation, &mut sequence, events).await?;
                        }
                        buffered_bytes = 0;
                    }
                    Ok(Err(reason)) => return Err(FeedError::MarketProtocol(format!("CLOB resync authorization rejected: {reason}"))),
                    Err(_) => return Err(FeedError::MarketProtocol("CLOB resync authorization channel closed".to_owned())),
                }
            }
            _ = tokio::time::sleep_until(generation_deadline), if !authorized => {
                return Err(FeedError::MarketProtocol("CLOB generation exceeded its absolute deadline".to_owned()));
            }
            _ = ping.tick() => {
                let write_deadline = market_write_deadline(
                    generation_deadline,
                    !authorized,
                );
                websocket_send_before_deadline(
                    &mut write,
                    write_deadline,
                    Message::Text("PING".to_owned()),
                )
                .await?;
            }
            _ = source_watchdog.tick() => {
                if barrier_started.is_some_and(|started| started.elapsed() > MARKET_RESYNC_BUFFER_AGE) {
                    return Err(FeedError::MarketProtocol("CLOB resync authorization exceeded its bounded wait".to_owned()));
                }
                if source_watchdog_expired(last_observation, source_timeout) {
                    return Err(FeedError::SourceStalled(format!(
                        "Polymarket CLOB market feed produced no matching event for {:.0}s while the socket remained connected",
                        source_timeout.as_secs_f64()
                    )));
                }
            }
            message = read.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let message = message?;
                if !authorized {
                    resync_frames = resync_frames.saturating_add(1);
                    resync_bytes = resync_bytes.saturating_add(market_message_bytes(&message));
                    if resync_frames > MARKET_RESYNC_MAX_FRAMES
                        || resync_bytes > MARKET_RESYNC_BUFFER_BYTES
                    {
                        return Err(FeedError::MarketProtocol("CLOB resync aggregate exceeded its bounded frame, byte, or age budget".to_owned()));
                    }
                }
                if let Some(started) = barrier_started {
                    let bytes = market_message_bytes(&message);
                    buffered_bytes = buffered_bytes.saturating_add(bytes);
                    if buffered_bytes > MARKET_RESYNC_BUFFER_BYTES || started.elapsed() > MARKET_RESYNC_BUFFER_AGE {
                        return Err(FeedError::MarketProtocol("CLOB resync barrier exceeded its bounded buffered frame budget".to_owned()));
                    }
                    buffered.push_back(message);
                    continue;
                }
                if !authorized && ready_ack.is_none() {
                    let anchored = collect_snapshot_anchors(
                        message,
                        &expected_tokens,
                        &mut anchors,
                        &mut anchored_deltas,
                        &mut retained_delta_bytes,
                    )?;
                    if anchored {
                        last_observation = Instant::now();
                    }
                    if anchors.len() == expected_tokens.len() {
                        books = anchors.clone();
                        let mut pre_ready_events = Vec::new();
                        while let Some(payload) = anchored_deltas.pop_front() {
                            for event in strict_market_events(
                                Message::Text(payload.to_string()),
                                &expected_tokens,
                                &mut books,
                            )? {
                                if let FeedEvent::RawMarketEvent(event) = event {
                                    pre_ready_events.push(event);
                                }
                            }
                        }
                        let (ack_tx, ack_rx) = oneshot::channel();
                        send_before_deadline(&sender, generation_deadline, FeedEvent::ClobResyncBarrier(ClobResyncBarrier {
                            generation,
                            sequence,
                            token_set_digest: token_set_digest.clone(),
                            token_count: expected_tokens.len(),
                            anchors: books.values().cloned().collect(),
                            pre_ready_events,
                            lease: lease.clone(),
                            ready_ack: ack_tx,
                        })).await?;
                        ready_ack = Some(ack_rx);
                        barrier_started = Some(Instant::now());
                    }
                    continue;
                }
                let events = strict_market_events(message, &expected_tokens, &mut books)?;
                forward_market_events(&sender, generation, &mut sequence, events).await?;
                last_observation = Instant::now();
            }
        }
    }
}

fn market_write_deadline(generation_deadline: Instant, resync_pending: bool) -> Instant {
    if resync_pending {
        generation_deadline
    } else {
        Instant::now() + MARKET_READY_HEARTBEAT_WRITE_TIMEOUT
    }
}

fn exact_token_set(token_ids: &[TokenId]) -> Result<BTreeSet<TokenId>, FeedError> {
    let set = token_ids.iter().cloned().collect::<BTreeSet<_>>();
    if set.len() != token_ids.len() || set.iter().any(|token| token.as_ref().is_empty()) {
        return Err(FeedError::MarketProtocol(
            "CLOB subscription requires a non-empty unique token set".to_owned(),
        ));
    }
    Ok(set)
}

fn token_set_digest(tokens: &BTreeSet<TokenId>) -> String {
    // Fixed FNV-1a is used only as a compact audit digest.  The runtime also
    // compares the complete in-memory set before authorizing the barrier.
    let mut hasher = 0xcbf2_9ce4_8422_2325_u64;
    for token in tokens {
        for byte in token.as_ref().bytes().chain(std::iter::once(0)) {
            hasher ^= u64::from(byte);
            hasher = hasher.wrapping_mul(0x100_0000_01b3);
        }
    }
    format!("{hasher:016x}")
}

fn market_message_bytes(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.len(),
        Message::Binary(bytes) => bytes.len(),
        _ => 0,
    }
}

async fn send_before_deadline(
    sender: &mpsc::Sender<FeedEvent>,
    deadline: Instant,
    event: FeedEvent,
) -> Result<(), FeedError> {
    tokio::time::timeout_at(deadline, sender.send(event))
        .await
        .map_err(|_| {
            FeedError::MarketProtocol(
                "CLOB generation barrier enqueue exceeded its absolute deadline".to_owned(),
            )
        })?
        .map_err(|_| FeedError::ChannelClosed)
}

async fn websocket_send_before_deadline<S>(
    write: &mut S,
    deadline: Instant,
    message: Message,
) -> Result<(), FeedError>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::time::timeout_at(deadline, write.send(message))
        .await
        .map_err(|_| {
            FeedError::MarketProtocol("CLOB generation exceeded its absolute deadline".to_owned())
        })??;
    Ok(())
}

fn retain_anchored_delta(
    retained: &mut VecDeque<Value>,
    retained_bytes: &mut usize,
    delta: Value,
) -> Result<(), FeedError> {
    let bytes = serde_json::to_vec(&delta)?.len();
    if retained.len() >= MARKET_RETAINED_DELTA_CHILDREN
        || bytes > MARKET_RETAINED_DELTA_BYTES
        || retained_bytes.saturating_add(bytes) > MARKET_RETAINED_DELTA_BYTES
    {
        return Err(FeedError::MarketProtocol(
            "CLOB retained pre-ready delta budget exceeded".to_owned(),
        ));
    }
    *retained_bytes += bytes;
    retained.push_back(delta);
    Ok(())
}

fn collect_snapshot_anchors(
    message: Message,
    expected: &BTreeSet<TokenId>,
    anchors: &mut BTreeMap<TokenId, BookState>,
    retained: &mut VecDeque<Value>,
    retained_bytes: &mut usize,
) -> Result<bool, FeedError> {
    let Some(payload) = websocket_json(message) else {
        return Ok(false);
    };
    let items: Vec<&Value> = payload
        .as_array()
        .map(|items| items.iter())
        .into_iter()
        .flatten()
        .collect();
    let items = if items.is_empty() {
        vec![&payload]
    } else {
        items
    };
    let mut saw_snapshot = false;
    for item in items {
        let event_type = item
            .get("event_type")
            .or_else(|| item.get("type"))
            .map(value_text)
            .unwrap_or_default()
            .to_ascii_lowercase();
        match event_type.as_str() {
            "book" => {
                let book = strict_snapshot(item)?;
                if !expected.contains(&book.token_id) {
                    return Err(FeedError::MarketProtocol(
                        "CLOB snapshot included an unsubscribed token".to_owned(),
                    ));
                }
                if anchors.insert(book.token_id.clone(), book).is_some() {
                    return Err(FeedError::MarketProtocol(
                        "CLOB generation repeated a snapshot token".to_owned(),
                    ));
                }
                saw_snapshot = true;
            }
            "price_change" | "pricechange" | "last_trade_price" | "trade" | "last_trade" => {
                validate_incremental(item, expected)?;
                // No replay/sequence contract exists on this endpoint.  Admit each
                // normalized child directly into the bounded queue; never create an
                // unbounded vector of cloned parent payloads before applying limits.
                retain_anchored_incremental_children(item, anchors, retained, retained_bytes)?;
            }
            _ => {}
        }
    }
    Ok(saw_snapshot)
}

fn retain_anchored_incremental_children(
    event: &Value,
    anchors: &BTreeMap<TokenId, BookState>,
    retained: &mut VecDeque<Value>,
    retained_bytes: &mut usize,
) -> Result<(), FeedError> {
    let Some((field, changes)) = event
        .get("price_changes")
        .or_else(|| event.get("changes"))
        .and_then(Value::as_array)
        .map(|changes| {
            (
                if event.get("price_changes").is_some() {
                    "price_changes"
                } else {
                    "changes"
                },
                changes,
            )
        })
    else {
        if incremental_tokens(event)
            .iter()
            .all(|token| anchors.contains_key(token))
        {
            retain_anchored_delta(
                retained,
                retained_bytes,
                normalized_incremental_event(event, event, None)?,
            )?;
        }
        return Ok(());
    };
    for change in changes {
        let anchored = {
            let token = TokenId::new(value_text(
                change
                    .get("asset_id")
                    .or_else(|| change.get("token_id"))
                    .or_else(|| event.get("asset_id"))
                    .or_else(|| event.get("token_id"))
                    .unwrap_or(&Value::Null),
            ));
            anchors.contains_key(&token)
        };
        if anchored {
            retain_anchored_delta(
                retained,
                retained_bytes,
                normalized_incremental_event(event, change, Some(field))?,
            )?;
        }
    }
    Ok(())
}

fn normalized_incremental_event(
    event: &Value,
    change: &Value,
    array_field: Option<&str>,
) -> Result<Value, FeedError> {
    const FIELDS: &[&str] = &[
        "asset_id",
        "token_id",
        "side",
        "price",
        "size",
        "last_trade_price",
        "timestamp",
        "ts",
        "hash",
        "market_id",
        "condition_id",
        "best_bid",
        "best_ask",
    ];
    let mut normalized = serde_json::Map::new();
    normalized.insert(
        "event_type".to_owned(),
        bounded_normalized_value(
            event
                .get("event_type")
                .or_else(|| event.get("type"))
                .unwrap_or(&Value::String("unknown".to_owned())),
        )?,
    );
    let mut child = serde_json::Map::new();
    for field in FIELDS {
        if let Some(value) = change.get(*field).or_else(|| event.get(*field)) {
            child.insert((*field).to_owned(), bounded_normalized_value(value)?);
        }
    }
    if let Some(field) = array_field {
        normalized.insert(field.to_owned(), Value::Array(vec![Value::Object(child)]));
    } else {
        normalized.extend(child);
    }
    Ok(Value::Object(normalized))
}

fn bounded_normalized_value(value: &Value) -> Result<Value, FeedError> {
    if serde_json::to_vec(value)?.len() > MARKET_RETAINED_FIELD_BYTES {
        return Err(FeedError::MarketProtocol(
            "CLOB retained pre-ready delta field exceeds its bounded size".to_owned(),
        ));
    }
    Ok(value.clone())
}

fn incremental_tokens(event: &Value) -> Vec<TokenId> {
    let changes = event
        .get("price_changes")
        .or_else(|| event.get("changes"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![event]);
    changes
        .into_iter()
        .map(|change| {
            TokenId::new(value_text(
                change
                    .get("asset_id")
                    .or_else(|| change.get("token_id"))
                    .or_else(|| event.get("asset_id"))
                    .or_else(|| event.get("token_id"))
                    .unwrap_or(&Value::Null),
            ))
        })
        .collect()
}

fn strict_snapshot(event: &Value) -> Result<BookState, FeedError> {
    let token_id = TokenId::new(value_text(
        event
            .get("asset_id")
            .or_else(|| event.get("token_id"))
            .unwrap_or(&Value::Null),
    ));
    if token_id.as_ref().is_empty() {
        return Err(FeedError::MarketProtocol(
            "CLOB snapshot is missing asset_id".to_owned(),
        ));
    }
    let bids = strict_levels(event.get("bids"), true)?;
    let asks = strict_levels(event.get("asks"), false)?;
    if let (Some(bid), Some(ask)) = (bids.last(), asks.last()) {
        if bid.price >= ask.price {
            return Err(FeedError::MarketProtocol(
                "CLOB snapshot is crossed".to_owned(),
            ));
        }
    }
    let last_trade_price = match event.get("last_trade_price") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if value.trim().is_empty() => None,
        Some(value) => Some(strict_price(value, "last_trade_price")?),
    };
    Ok(BookState {
        token_id,
        bids,
        asks,
        last_trade_price,
        exchange_ts: parse_event_ts(event.get("timestamp").or_else(|| event.get("ts"))),
        local_ts: Utc::now(),
        book_hash: value_opt_text(event.get("hash")),
    })
}

fn strict_levels(value: Option<&Value>, ascending: bool) -> Result<Vec<BookLevel>, FeedError> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Err(FeedError::MarketProtocol(
            "CLOB snapshot is missing a level array".to_owned(),
        ));
    };
    let mut levels = Vec::with_capacity(items.len());
    let mut prior = None;
    for item in items {
        let price = strict_price(item.get("price").unwrap_or(&Value::Null), "level price")?;
        let size = decimal(item.get("size"))
            .filter(|size| *size > Decimal::ZERO)
            .ok_or_else(|| FeedError::MarketProtocol("CLOB level has invalid size".to_owned()))?;
        if let Some(previous) = prior {
            let ordered = if ascending {
                previous < price
            } else {
                previous > price
            };
            if !ordered {
                return Err(FeedError::MarketProtocol(
                    "CLOB snapshot levels are not strictly wire ordered".to_owned(),
                ));
            }
        }
        prior = Some(price);
        levels.push(BookLevel { price, size });
    }
    Ok(levels)
}

fn strict_price(value: &Value, field: &str) -> Result<Decimal, FeedError> {
    let price = decimal(Some(value))
        .filter(|price| *price >= Decimal::ZERO && *price <= Decimal::ONE)
        .ok_or_else(|| {
            FeedError::MarketProtocol(format!("CLOB {field} is outside [0,1] or malformed"))
        })?;
    Ok(price)
}

fn validate_incremental(event: &Value, expected: &BTreeSet<TokenId>) -> Result<(), FeedError> {
    let event_type = event
        .get("event_type")
        .or_else(|| event.get("type"))
        .map(value_text)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let changes: Vec<&Value> = match event_type.as_str() {
        "price_change" | "pricechange" => event
            .get("price_changes")
            .or_else(|| event.get("changes"))
            .and_then(Value::as_array)
            .map(|items| items.iter().collect())
            .unwrap_or_else(|| vec![event]),
        "last_trade_price" | "trade" | "last_trade" => vec![event],
        _ => return Ok(()),
    };
    for change in changes {
        let token = TokenId::new(value_text(
            change
                .get("asset_id")
                .or_else(|| change.get("token_id"))
                .or_else(|| event.get("asset_id"))
                .or_else(|| event.get("token_id"))
                .unwrap_or(&Value::Null),
        ));
        if !expected.contains(&token) {
            return Err(FeedError::MarketProtocol(
                "CLOB incremental event included an unsubscribed or missing token".to_owned(),
            ));
        }
        strict_price(
            change
                .get("price")
                .or_else(|| change.get("last_trade_price"))
                .or_else(|| event.get("price"))
                .or_else(|| event.get("last_trade_price"))
                .unwrap_or(&Value::Null),
            "incremental price",
        )?;
        if matches!(event_type.as_str(), "price_change" | "pricechange") {
            let side = change
                .get("side")
                .or_else(|| event.get("side"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !matches!(side, "BUY" | "SELL") {
                return Err(FeedError::MarketProtocol(
                    "CLOB price change side must be uppercase BUY or SELL".to_owned(),
                ));
            }
            if decimal(change.get("size").or_else(|| event.get("size")))
                .filter(|size| *size >= Decimal::ZERO)
                .is_none()
            {
                return Err(FeedError::MarketProtocol(
                    "CLOB price change size is malformed".to_owned(),
                ));
            }
        } else {
            let side = change
                .get("side")
                .or_else(|| event.get("side"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let size = change.get("size").or_else(|| event.get("size"));
            if !matches!(side, "BUY" | "SELL")
                || !matches!(size, Some(Value::String(_)))
                || decimal(size).filter(|size| *size > Decimal::ZERO).is_none()
            {
                return Err(FeedError::MarketProtocol(
                    "CLOB last trade requires uppercase side and a positive string size".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn strict_market_events(
    message: Message,
    expected: &BTreeSet<TokenId>,
    books: &mut BTreeMap<TokenId, BookState>,
) -> Result<Vec<FeedEvent>, FeedError> {
    let Some(payload) = websocket_json(message) else {
        return Ok(Vec::new());
    };
    let items: Vec<&Value> = payload
        .as_array()
        .map(|items| items.iter())
        .into_iter()
        .flatten()
        .collect();
    let items = if items.is_empty() {
        vec![&payload]
    } else {
        items
    };
    let mut output = Vec::new();
    for item in items {
        let event_type = item
            .get("event_type")
            .or_else(|| item.get("type"))
            .map(value_text)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if matches!(event_type.as_str(), "book") {
            let book = strict_snapshot(item)?;
            if !expected.contains(&book.token_id) {
                return Err(FeedError::MarketProtocol(
                    "CLOB snapshot included an unsubscribed token".to_owned(),
                ));
            }
        } else if matches!(
            event_type.as_str(),
            "price_change" | "pricechange" | "last_trade_price" | "trade" | "last_trade"
        ) {
            validate_incremental(item, expected)?;
        }
        let events = handle_market_event(item, books);
        for event in &events {
            if let FeedEvent::Book(book) = event {
                if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
                    if bid.price >= ask.price {
                        return Err(FeedError::MarketProtocol(
                            "CLOB resulting book is crossed".to_owned(),
                        ));
                    }
                }
            }
        }
        output.extend(events);
    }
    Ok(output)
}

async fn forward_market_events(
    sender: &mpsc::Sender<FeedEvent>,
    generation: u64,
    sequence: &mut u64,
    events: Vec<FeedEvent>,
) -> Result<(), FeedError> {
    for event in events {
        *sequence = sequence.wrapping_add(1);
        let event = match event {
            FeedEvent::RawMarketEvent(event) => FeedEvent::ClobRawMarketEvent {
                generation,
                sequence: *sequence,
                event,
            },
            FeedEvent::Book(book) => FeedEvent::ClobBook {
                generation,
                sequence: *sequence,
                book,
            },
            _ => continue,
        };
        sender
            .send(event)
            .await
            .map_err(|_| FeedError::ChannelClosed)?;
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
    fn clob_anchor_requires_documented_wire_order_and_exact_tokens() {
        let expected = BTreeSet::from([TokenId::new("yes"), TokenId::new("no")]);
        let mut anchors = BTreeMap::new();
        let mut retained = VecDeque::new();
        let mut retained_bytes = 0;
        let first = json!({
            "event_type": "book", "asset_id": "yes",
            "bids": [{"price": "0.40", "size": "1"}, {"price": "0.41", "size": "1"}],
            "asks": [{"price": "0.60", "size": "1"}, {"price": "0.59", "size": "1"}]
        });
        assert!(collect_snapshot_anchors(
            Message::Text(first.to_string()),
            &expected,
            &mut anchors,
            &mut retained,
            &mut retained_bytes,
        )
        .unwrap());
        assert_eq!(anchors.len(), 1);

        let duplicate = json!({
            "event_type": "book", "asset_id": "yes", "bids": [], "asks": []
        });
        assert!(collect_snapshot_anchors(
            Message::Text(duplicate.to_string()),
            &expected,
            &mut anchors,
            &mut retained,
            &mut retained_bytes,
        )
        .is_err());

        let crossed = json!({
            "event_type": "book", "asset_id": "no",
            "bids": [{"price": "0.70", "size": "1"}],
            "asks": [{"price": "0.69", "size": "1"}]
        });
        assert!(strict_snapshot(&crossed).is_err());
    }

    #[test]
    fn optional_snapshot_last_trade_price_preserves_the_decimal_contract() {
        let snapshot = |last_trade_price: Option<Value>| {
            let mut snapshot = json!({
                "event_type": "book",
                "asset_id": "token",
                "bids": [],
                "asks": []
            });
            if let Some(value) = last_trade_price {
                snapshot["last_trade_price"] = value;
            }
            snapshot
        };

        for value in [None, Some(Value::Null), Some(json!("  \t"))] {
            assert_eq!(
                strict_snapshot(&snapshot(value)).unwrap().last_trade_price,
                None
            );
        }
        assert_eq!(
            strict_snapshot(&snapshot(Some(json!("0.42"))))
                .unwrap()
                .last_trade_price,
            Some(Decimal::new(42, 2))
        );
        assert_eq!(
            strict_snapshot(&snapshot(Some(json!(0.42))))
                .unwrap()
                .last_trade_price,
            Some(Decimal::new(42, 2))
        );
        for value in [json!("not-a-price"), json!("1.01"), json!(-0.01)] {
            assert!(strict_snapshot(&snapshot(Some(value))).is_err());
        }
    }

    #[test]
    fn clob_incrementals_are_strict_after_the_snapshot_barrier() {
        let expected = BTreeSet::from([TokenId::new("yes")]);
        let mut books = BTreeMap::from([(
            TokenId::new("yes"),
            strict_snapshot(&json!({
                "event_type": "book", "asset_id": "yes",
                "bids": [{"price": "0.40", "size": "1"}],
                "asks": [{"price": "0.60", "size": "1"}]
            }))
            .unwrap(),
        )]);
        let valid = json!({
            "event_type": "price_change",
            "price_changes": [{"asset_id": "yes", "side": "BUY", "price": "0.41", "size": "2"}]
        });
        assert!(
            !strict_market_events(Message::Text(valid.to_string()), &expected, &mut books)
                .unwrap()
                .is_empty()
        );
        let malformed = json!({
            "event_type": "price_change",
            "price_changes": [{"asset_id": "yes", "side": "buy", "price": "1.2", "size": "2"}]
        });
        assert!(
            strict_market_events(Message::Text(malformed.to_string()), &expected, &mut books)
                .is_err()
        );
    }

    #[tokio::test]
    async fn market_generation_emits_no_delta_before_barrier_authorization() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let _: Value = serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "event_type": "book", "asset_id": "token",
                        "bids": [{"price": "0.40", "size": "1"}],
                        "asks": [{"price": "0.60", "size": "1"}]
                    })
                    .to_string(),
                ))
                .unwrap();
            std::thread::sleep(Duration::from_millis(100));
            socket
                .send(Message::Text(
                    json!({
                        "event_type": "price_change",
                        "price_changes": [{"asset_id": "token", "side": "BUY", "price": "0.41", "size": "2"}]
                    })
                    .to_string(),
                ))
                .unwrap();
            std::thread::sleep(Duration::from_millis(100));
            socket.close(None).unwrap();
        });
        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        let (sender, mut receiver) = mpsc::channel(8);
        let feed = tokio::spawn(run_market_feed_generation(
            settings,
            vec![TokenId::new("token")],
            9,
            sender,
        ));
        let barrier = match receiver.recv().await {
            Some(FeedEvent::ClobResyncBarrier(barrier)) => barrier,
            _ => panic!("expected a CLOB barrier before any market event"),
        };
        assert_eq!(barrier.generation, 9);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.recv())
                .await
                .is_err()
        );
        barrier.ready_ack.send(Ok(())).unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(FeedEvent::ClobRawMarketEvent {
                generation: 9,
                sequence: 1,
                ..
            })
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(FeedEvent::ClobBook {
                generation: 9,
                sequence: 2,
                ..
            })
        ));
        let _ = feed.await.unwrap();
        server.join().unwrap();
    }

    #[tokio::test]
    async fn two_token_blank_optional_price_reaches_one_barrier_and_holds_events_until_ack() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let _ = socket.read().unwrap();
            for event in [
                json!({"event_type":"book", "asset_id":"yes", "last_trade_price":"", "bids":[{"price":"0.40","size":"1"}], "asks":[{"price":"0.60","size":"1"}]}),
                json!({"event_type":"price_change", "price_changes":[{"asset_id":"yes","side":"BUY","price":"0.41","size":"2"}]}),
                json!({"event_type":"book", "asset_id":"no", "last_trade_price":"  ", "bids":[], "asks":[]}),
            ] {
                socket.send(Message::Text(event.to_string())).unwrap();
            }
            std::thread::sleep(Duration::from_millis(75));
            socket
                .send(Message::Text(
                    json!({"event_type":"price_change", "price_changes":[{"asset_id":"yes","side":"BUY","price":"0.42","size":"3"}]}).to_string(),
                ))
                .unwrap();
            std::thread::sleep(Duration::from_millis(100));
            let _ = socket.close(None);
        });
        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        let lease = ClobGenerationLease::new();
        let (sender, mut receiver) = mpsc::channel(8);
        let feed = tokio::spawn(run_market_feed_generation_with_lease(
            settings,
            vec![TokenId::new("yes"), TokenId::new("no")],
            14,
            lease,
            sender,
        ));
        let barrier = match receiver.recv().await {
            Some(FeedEvent::ClobResyncBarrier(barrier)) => barrier,
            _ => panic!("expected CLOB barrier"),
        };
        let yes_anchor = barrier
            .anchors
            .iter()
            .find(|book| book.token_id == TokenId::new("yes"))
            .unwrap();
        assert_eq!(yes_anchor.best_bid().unwrap().price, Decimal::new(41, 2));
        assert!(barrier
            .anchors
            .iter()
            .all(|book| book.last_trade_price.is_none()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.recv())
                .await
                .is_err()
        );
        barrier.ready_ack.send(Ok(())).unwrap();
        let mut barrier_count = 1;
        let mut saw_later_book = false;
        while let Some(event) = receiver.recv().await {
            match event {
                FeedEvent::ClobResyncBarrier(_) => barrier_count += 1,
                FeedEvent::ClobRawMarketEvent { generation: 14, .. } => {}
                FeedEvent::ClobBook {
                    generation: 14,
                    book,
                    ..
                } => {
                    saw_later_book = book.token_id == TokenId::new("yes")
                        && book
                            .best_bid()
                            .is_some_and(|bid| bid.price == Decimal::new(42, 2));
                }
                _ => panic!("unexpected event or stale generation after authorization"),
            }
        }
        assert_eq!(barrier_count, 1);
        assert!(saw_later_book);
        let _ = feed.await.unwrap();
        server.join().unwrap();
    }

    #[tokio::test]
    async fn eof_while_ack_is_pending_tombstones_the_queued_barrier() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let _ = socket.read().unwrap();
            socket
                .send(Message::Text(
                    json!({"event_type":"book","asset_id":"token","bids":[],"asks":[]}).to_string(),
                ))
                .unwrap();
            socket.close(None).unwrap();
        });
        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        let lease = ClobGenerationLease::new();
        let (sender, mut receiver) = mpsc::channel(2);
        let feed = tokio::spawn(run_market_feed_generation_with_lease(
            settings,
            vec![TokenId::new("token")],
            10,
            lease.clone(),
            sender,
        ));
        let barrier = match receiver.recv().await {
            Some(FeedEvent::ClobResyncBarrier(barrier)) => barrier,
            _ => panic!("expected barrier"),
        };
        let _ = feed.await.unwrap();
        assert!(lease.is_terminal());
        assert!(barrier.lease.is_terminal());
        drop(barrier);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn cancelled_market_generation_tombstones_its_lease() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (connected_tx, connected_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let _ = socket.read().unwrap();
            connected_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(100));
        });
        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        let lease = ClobGenerationLease::new();
        let (sender, _receiver) = mpsc::channel(1);
        let feed = tokio::spawn(run_market_feed_generation_with_lease(
            settings,
            vec![TokenId::new("token")],
            11,
            lease.clone(),
            sender,
        ));
        tokio::task::spawn_blocking(move || connected_rx.recv_timeout(Duration::from_secs(1)))
            .await
            .unwrap()
            .unwrap();
        feed.abort();
        assert!(feed.await.unwrap_err().is_cancelled());
        assert!(lease.is_terminal());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn pre_ready_frame_flood_exceeds_the_generation_budget() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let _ = socket.read().unwrap();
            for _ in 0..=MARKET_RESYNC_MAX_FRAMES {
                if socket.send(Message::Text("\"PONG\"".to_owned())).is_err() {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        });
        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        let (sender, _receiver) = mpsc::channel(1);
        let error = run_market_feed_generation(settings, vec![TokenId::new("token")], 12, sender)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
            FeedError::MarketProtocol(ref message) if message.contains("aggregate")
            ),
            "unexpected flood result: {error:?}"
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn full_event_channel_fails_barrier_enqueue_at_an_expired_absolute_deadline() {
        let (sender, _receiver) = mpsc::channel(1);
        sender
            .send(FeedEvent::Heartbeat {
                source: FeedName::Mock,
                ts: Utc::now(),
            })
            .await
            .unwrap();
        let (ready_ack, _ready_result) = oneshot::channel();
        let error = send_before_deadline(
            &sender,
            Instant::now(),
            FeedEvent::ClobResyncBarrier(ClobResyncBarrier {
                generation: 13,
                sequence: 0,
                token_set_digest: "test".to_owned(),
                token_count: 0,
                anchors: Vec::new(),
                pre_ready_events: Vec::new(),
                lease: ClobGenerationLease::new(),
                ready_ack,
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            FeedError::MarketProtocol(message) if message.contains("enqueue")
        ));
    }

    #[tokio::test]
    async fn blocked_ping_write_respects_the_same_absolute_generation_deadline() {
        struct NeverWritable;
        impl Sink<Message> for NeverWritable {
            type Error = tokio_tungstenite::tungstenite::Error;

            fn poll_ready(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }

            fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
                unreachable!("a permanently blocked sink is never ready")
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }

            fn poll_close(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Pending
            }
        }

        let mut write = NeverWritable;
        let error = websocket_send_before_deadline(
            &mut write,
            Instant::now(),
            Message::Text("PING".to_owned()),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            FeedError::MarketProtocol(message) if message.contains("absolute deadline")
        ));
    }

    #[tokio::test]
    async fn ready_generation_uses_a_fresh_heartbeat_deadline_after_resync_horizon() {
        struct BrieflyPending {
            pending_once: bool,
        }
        impl Sink<Message> for BrieflyPending {
            type Error = tokio_tungstenite::tungstenite::Error;

            fn poll_ready(
                mut self: std::pin::Pin<&mut Self>,
                context: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                if self.pending_once {
                    self.pending_once = false;
                    context.waker().wake_by_ref();
                    std::task::Poll::Pending
                } else {
                    std::task::Poll::Ready(Ok(()))
                }
            }

            fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
                Ok(())
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn poll_close(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let expired_resync_deadline = Instant::now() - Duration::from_secs(1);
        let ready_write_deadline = market_write_deadline(expired_resync_deadline, false);
        assert!(ready_write_deadline > Instant::now());
        let mut write = BrieflyPending { pending_once: true };
        websocket_send_before_deadline(
            &mut write,
            ready_write_deadline,
            Message::Text("PING".to_owned()),
        )
        .await
        .unwrap();
    }

    #[test]
    fn retained_delta_before_final_snapshot_is_applied_at_readiness() {
        let expected = BTreeSet::from([TokenId::new("yes"), TokenId::new("no")]);
        let mut anchors = BTreeMap::new();
        let mut retained = VecDeque::new();
        let mut retained_bytes = 0;
        let yes_snapshot = json!({
            "event_type": "book", "asset_id": "yes",
            "bids": [{"price": "0.40", "size": "1"}],
            "asks": [{"price": "0.60", "size": "1"}]
        });
        collect_snapshot_anchors(
            Message::Text(yes_snapshot.to_string()),
            &expected,
            &mut anchors,
            &mut retained,
            &mut retained_bytes,
        )
        .unwrap();
        let delta = json!({
            "event_type": "price_change",
            "price_changes": [{"asset_id": "yes", "side": "BUY", "price": "0.41", "size": "2"}]
        });
        collect_snapshot_anchors(
            Message::Text(delta.to_string()),
            &expected,
            &mut anchors,
            &mut retained,
            &mut retained_bytes,
        )
        .unwrap();
        let no_snapshot = json!({"event_type": "book", "asset_id": "no", "bids": [], "asks": []});
        collect_snapshot_anchors(
            Message::Text(no_snapshot.to_string()),
            &expected,
            &mut anchors,
            &mut retained,
            &mut retained_bytes,
        )
        .unwrap();
        let mut books = anchors;
        while let Some(delta) = retained.pop_front() {
            strict_market_events(Message::Text(delta.to_string()), &expected, &mut books).unwrap();
        }
        assert_eq!(
            books[&TokenId::new("yes")].best_bid().unwrap().price,
            Decimal::new(41, 2)
        );
    }

    #[test]
    fn mixed_token_retained_delta_keeps_only_anchored_children_without_parent_amplification() {
        let expected = BTreeSet::from([TokenId::new("yes"), TokenId::new("no")]);
        let mut anchors = BTreeMap::from([(
            TokenId::new("yes"),
            strict_snapshot(&json!({"event_type":"book", "asset_id":"yes", "bids":[], "asks":[]}))
                .unwrap(),
        )]);
        let event = json!({
            "event_type": "price_change",
            "untrusted_parent_blob": "x".repeat(4096),
            "price_changes": [
                {"asset_id": "yes", "side": "BUY", "price": "0.41", "size": "2"},
                {"asset_id": "no", "side": "SELL", "price": "0.59", "size": "3"}
            ]
        });
        let mut retained = VecDeque::new();
        let mut retained_bytes = 0;
        collect_snapshot_anchors(
            Message::Text(event.to_string()),
            &expected,
            &mut anchors,
            &mut retained,
            &mut retained_bytes,
        )
        .unwrap();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].get("untrusted_parent_blob").is_none());
        assert_eq!(retained[0]["price_changes"].as_array().unwrap().len(), 1);
        assert_eq!(retained[0]["price_changes"][0]["asset_id"], "yes");
    }

    #[test]
    fn retained_delta_byte_budget_is_enforced() {
        let mut retained = VecDeque::new();
        let mut retained_bytes = 0;
        let oversized = json!({"event_type": "price_change", "payload": "x".repeat(MARKET_RETAINED_DELTA_BYTES)});
        assert!(retain_anchored_delta(&mut retained, &mut retained_bytes, oversized).is_err());
        assert!(retained.is_empty());
        assert_eq!(retained_bytes, 0);
    }

    #[test]
    fn panic_unwind_runs_the_same_generation_terminal_guard() {
        let lease = ClobGenerationLease::new();
        let panic_result = std::panic::catch_unwind({
            let lease = lease.clone();
            move || {
                let _guard = ClobTerminalGuard(lease);
                panic!("injected generation panic");
            }
        });
        assert!(panic_result.is_err());
        assert!(lease.is_terminal());
    }

    #[tokio::test]
    async fn panicking_generation_task_returns_join_error_and_tombstones_its_lease() {
        let lease = ClobGenerationLease::new();
        let (sender, _receiver) = mpsc::channel(1);
        let task = tokio::spawn(run_market_feed_generation_with_lease(
            RuntimeSettings::default(),
            vec![TokenId::new("panic-token")],
            u64::MAX,
            lease.clone(),
            sender,
        ));
        assert!(task.await.unwrap_err().is_panic());
        assert!(lease.is_terminal());
    }

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
            for (slot, receiver) in command_receivers.iter_mut().enumerate() {
                let (stream, _) = listener.accept().unwrap();
                let commands = receiver.take().unwrap();
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

    #[tokio::test]
    async fn connected_market_socket_without_events_exits_for_reconnect() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(8)))
                .unwrap();
            let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
            let _: Value = serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
            while let Ok(message) = socket.read() {
                if message == Message::Text("PING".to_owned()) {
                    socket.send(Message::Text("PONG".to_owned())).unwrap();
                }
            }
        });

        let mut settings = RuntimeSettings::default();
        settings.target.polymarket_ws_url = format!("ws://{address}");
        settings.target.polymarket_market_watchdog_seconds = 5.0;
        let (sender, _receiver) = mpsc::channel(8);

        let error = tokio::time::timeout(
            Duration::from_secs(8),
            run_market_feed(settings, vec![TokenId::new("token")], sender),
        )
        .await
        .expect("market watchdog did not return the stalled socket for reconnect")
        .unwrap_err();
        match error {
            FeedError::SourceStalled(message) => {
                assert!(message.contains("CLOB market feed"));
                assert!(message.contains("5s"));
            }
            other => panic!("unexpected market feed result: {other}"),
        }
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
