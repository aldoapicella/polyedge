use chrono::{DateTime, Duration, Utc};
use polyedge_domain::{
    BookLevel, BookState, DecisionAction, ExecutionReport, MarketId, OrderId, OrderKind, Side,
    TokenId, TradeDecision,
};
use polyedge_feeds::MarketChannelEvent;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::str::FromStr;

const MARKOUT_HORIZONS_SECONDS: [i64; 3] = [1, 5, 30];

#[derive(Clone, Debug)]
pub(super) struct QualityEvent {
    pub event_type: &'static str,
    pub payload: Value,
}

#[derive(Clone, Debug)]
struct TrackedOrder {
    order_id: OrderId,
    market_id: MarketId,
    token_id: TokenId,
    side: Side,
    quote_price: Decimal,
    original_size: Decimal,
    shadow_remaining_size: Decimal,
    shadow_filled_size: Decimal,
    size_ahead: Decimal,
    initial_size_ahead: Decimal,
    same_level_size: Decimal,
    better_level_size: Decimal,
    submitted_ts: DateTime<Utc>,
    live_ts: DateTime<Utc>,
    last_visible_size_at_quote: Decimal,
    snapshot_finalized: bool,
}

#[derive(Clone, Debug)]
struct PendingMarkout {
    fill_id: String,
    source: &'static str,
    order_id: OrderId,
    market_id: MarketId,
    token_id: TokenId,
    side: Side,
    fill_price: Decimal,
    fill_size: Decimal,
    fee_per_share: Decimal,
    fill_ts: DateTime<Utc>,
    horizon_seconds: i64,
}

#[derive(Clone, Default)]
pub(super) struct ExecutionQualityTracker {
    orders: BTreeMap<OrderId, TrackedOrder>,
    pending_markouts: Vec<PendingMarkout>,
    next_fill_id: u64,
}

impl ExecutionQualityTracker {
    pub(super) fn register_order(
        &mut self,
        decision: &TradeDecision,
        report: &ExecutionReport,
        book: Option<&BookState>,
        order_live_after_ms: i64,
    ) -> Option<Value> {
        if decision.action != DecisionAction::Place || report.status != "paper_resting" {
            return None;
        }
        let order_id = report.order_id.clone()?;
        let token_id = decision.token_id.clone()?;
        let side = decision.side.clone()?;
        let quote_price = decision.price?;
        let original_size = decision.size?;
        let (
            submit_same_level_size,
            submit_better_level_size,
            best_bid,
            best_ask,
            book_hash,
            book_ts,
        ) = book
            .map(|book| {
                let (same, better) = visible_depth(book, &side, quote_price);
                (
                    same,
                    better,
                    book.best_bid().map(|level| level.price),
                    book.best_ask().map(|level| level.price),
                    book.book_hash.clone(),
                    Some(book.local_ts),
                )
            })
            .unwrap_or((Decimal::ZERO, Decimal::ZERO, None, None, None, None));
        let submitted_ts = report.local_ts;
        let live_ts = submitted_ts + Duration::milliseconds(order_live_after_ms.max(0));
        self.orders.insert(
            order_id.clone(),
            TrackedOrder {
                order_id: order_id.clone(),
                market_id: decision.market_id.clone(),
                token_id: token_id.clone(),
                side: side.clone(),
                quote_price,
                original_size,
                shadow_remaining_size: original_size,
                shadow_filled_size: Decimal::ZERO,
                size_ahead: Decimal::ZERO,
                initial_size_ahead: Decimal::ZERO,
                same_level_size: Decimal::ZERO,
                better_level_size: Decimal::ZERO,
                submitted_ts,
                live_ts,
                last_visible_size_at_quote: submit_same_level_size,
                snapshot_finalized: false,
            },
        );
        Some(json!({
            "order_id": order_id,
            "market_id": decision.market_id,
            "token_id": token_id,
            "side": side,
            "quote_price": quote_price.to_string(),
            "order_size": original_size.to_string(),
            "submitted_ts": submitted_ts,
            "live_ts": live_ts,
            "queue_position_source": "public_l2_shadow",
            "queue_position_method": "first_l2_snapshot_at_or_after_order_live_ts",
            "queue_snapshot_finalized": false,
            "submit_visible_same_price_size": submit_same_level_size.to_string(),
            "submit_visible_better_price_size": submit_better_level_size.to_string(),
            "best_bid": best_bid.map(|value| value.to_string()),
            "best_ask": best_ask.map(|value| value.to_string()),
            "book_hash": book_hash,
            "book_ts": book_ts,
            "research_only": true,
            "live_order_placed": false
        }))
    }

    pub(super) fn observe_market_event(&mut self, event: &MarketChannelEvent) -> Vec<QualityEvent> {
        match event.event_type.as_str() {
            "last_trade_price" | "last_trade" | "trade" => self.observe_trade(event),
            "price_change" | "pricechange" => self.observe_level_changes(event),
            _ => Vec::new(),
        }
    }

    pub(super) fn observe_book(&mut self, book: &BookState) -> Vec<QualityEvent> {
        let observed_ts = book.local_ts;
        let pending_snapshots = self
            .orders
            .values()
            .filter(|order| {
                order.token_id == book.token_id
                    && !order.snapshot_finalized
                    && observed_ts >= order.live_ts
            })
            .map(|order| order.order_id.clone())
            .collect::<Vec<_>>();
        let order_context = self.orders.values().cloned().collect::<Vec<_>>();
        let mut due = Vec::new();
        for order_id in pending_snapshots {
            let Some(order) = self.orders.get_mut(&order_id) else {
                continue;
            };
            let (same_level_size, better_level_size) =
                visible_depth(book, &order.side, order.quote_price);
            let own_size_ahead = order_context
                .iter()
                .filter(|earlier| {
                    earlier.order_id != order.order_id
                        && earlier.token_id == order.token_id
                        && earlier.side == order.side
                        && earlier.quote_price == order.quote_price
                        && earlier.submitted_ts < order.submitted_ts
                })
                .map(|earlier| earlier.shadow_remaining_size)
                .sum::<Decimal>();
            order.same_level_size = same_level_size;
            order.better_level_size = better_level_size;
            order.size_ahead = same_level_size + better_level_size + own_size_ahead;
            order.initial_size_ahead = order.size_ahead;
            order.last_visible_size_at_quote = same_level_size;
            order.snapshot_finalized = true;
            due.push(QualityEvent {
                event_type: "paper_order_queue_snapshot",
                payload: json!({
                    "order_id": order.order_id,
                    "market_id": order.market_id,
                    "token_id": order.token_id,
                    "side": order.side,
                    "quote_price": order.quote_price.to_string(),
                    "order_size": order.original_size.to_string(),
                    "submitted_ts": order.submitted_ts,
                    "live_ts": order.live_ts,
                    "snapshot_ts": observed_ts,
                    "queue_position_source": "public_l2_shadow",
                    "visible_size_ahead_estimate": order.size_ahead.to_string(),
                    "same_price_public_size_ahead": same_level_size.to_string(),
                    "better_price_public_size_ahead": better_level_size.to_string(),
                    "earlier_shadow_order_size_ahead": own_size_ahead.to_string(),
                    "best_bid": book.best_bid().map(|level| level.price.to_string()),
                    "best_ask": book.best_ask().map(|level| level.price.to_string()),
                    "book_hash": book.book_hash,
                    "research_only": true,
                    "live_order_placed": false
                }),
            });
        }
        let mark_price = match (book.best_bid(), book.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::TWO),
            (Some(bid), None) => Some(bid.price),
            (None, Some(ask)) => Some(ask.price),
            (None, None) => book.last_trade_price,
        };
        let Some(mark_price) = mark_price else {
            return Vec::new();
        };
        let mut pending = Vec::with_capacity(self.pending_markouts.len());
        for markout in self.pending_markouts.drain(..) {
            if markout.token_id == book.token_id
                && observed_ts >= markout.fill_ts + Duration::seconds(markout.horizon_seconds)
            {
                let per_share = match markout.side {
                    Side::Buy => mark_price - markout.fill_price,
                    Side::Sell => markout.fill_price - mark_price,
                };
                let net_per_share = per_share - markout.fee_per_share;
                let executable_mark_price = match markout.side {
                    Side::Buy => book.best_bid().map(|level| level.price),
                    Side::Sell => book.best_ask().map(|level| level.price),
                };
                let executable_per_share =
                    executable_mark_price.map(|executable| match markout.side {
                        Side::Buy => executable - markout.fill_price,
                        Side::Sell => markout.fill_price - executable,
                    });
                let net_executable_per_share =
                    executable_per_share.map(|gross| gross - markout.fee_per_share);
                due.push(QualityEvent {
                    event_type: "paper_fill_markout",
                    payload: json!({
                        "fill_id": markout.fill_id,
                        "fill_source": markout.source,
                        "order_id": markout.order_id,
                        "market_id": markout.market_id,
                        "token_id": markout.token_id,
                        "side": markout.side,
                        "fill_price": markout.fill_price.to_string(),
                        "fill_size": markout.fill_size.to_string(),
                        "fee_per_share": markout.fee_per_share.to_string(),
                        "fill_ts": markout.fill_ts,
                        "horizon_seconds": markout.horizon_seconds,
                        "mark_price": mark_price.to_string(),
                        "markout_per_share": per_share.to_string(),
                        "markout_pnl": (per_share * markout.fill_size).to_string(),
                        "net_markout_per_share": net_per_share.to_string(),
                        "net_markout_pnl": (net_per_share * markout.fill_size).to_string(),
                        "executable_mark_price": executable_mark_price.map(|value| value.to_string()),
                        "executable_markout_per_share": executable_per_share.map(|value| value.to_string()),
                        "executable_markout_pnl": executable_per_share.map(|value| (value * markout.fill_size).to_string()),
                        "net_executable_markout_per_share": net_executable_per_share.map(|value| value.to_string()),
                        "net_executable_markout_pnl": net_executable_per_share.map(|value| (value * markout.fill_size).to_string()),
                        "best_bid": book.best_bid().map(|level| level.price.to_string()),
                        "best_ask": book.best_ask().map(|level| level.price.to_string()),
                        "observed_ts": observed_ts,
                        "observation_delay_ms": observed_ts.signed_duration_since(
                            markout.fill_ts + Duration::seconds(markout.horizon_seconds)
                        ).num_milliseconds().max(0),
                        "research_only": true
                    }),
                });
            } else {
                pending.push(markout);
            }
        }
        self.pending_markouts = pending;
        due
    }

    pub(super) fn observe_execution_report(
        &mut self,
        report: &ExecutionReport,
    ) -> Vec<QualityEvent> {
        let mut events = Vec::new();
        if report.filled_size > Decimal::ZERO {
            if let (Some(order_id), Some(token_id), Some(fill_price)) = (
                report.order_id.clone(),
                report.token_id.clone(),
                report.avg_price,
            ) {
                let (side, market_id) = self
                    .orders
                    .get(&order_id)
                    .map(|order| (order.side.clone(), order.market_id.clone()))
                    .unwrap_or((Side::Buy, report.market_id.clone()));
                self.schedule_markouts(
                    "touch_fill",
                    order_id,
                    market_id,
                    token_id,
                    side,
                    fill_price,
                    report.filled_size,
                    if report.filled_size > Decimal::ZERO {
                        report.fee / report.filled_size
                    } else {
                        Decimal::ZERO
                    },
                    report.local_ts,
                );
            }
        }
        if report.status == "paper_cancelled" {
            if let Some(order_id) = report.order_id.as_ref() {
                if let Some(order) = self.orders.remove(order_id) {
                    let requested_ts = report
                        .raw
                        .get("cancel_requested_ts")
                        .and_then(Value::as_str)
                        .and_then(parse_ts)
                        .unwrap_or(report.local_ts);
                    events.push(QualityEvent {
                        event_type: "paper_cancel_latency",
                        payload: json!({
                            "order_id": order.order_id,
                            "market_id": order.market_id,
                            "token_id": order.token_id,
                            "cancel_requested_ts": requested_ts,
                            "cancel_ack_ts": report.local_ts,
                            "cancel_latency_ms": report.local_ts.signed_duration_since(requested_ts).num_microseconds().unwrap_or(0).max(0) as f64 / 1000.0,
                            "order_age_ms": report.local_ts.signed_duration_since(order.submitted_ts).num_milliseconds().max(0),
                            "initial_size_ahead": order.initial_size_ahead.to_string(),
                            "remaining_size_ahead": order.size_ahead.to_string(),
                            "shadow_filled_size": order.shadow_filled_size.to_string(),
                            "shadow_remaining_size": order.shadow_remaining_size.to_string(),
                            "research_only": true,
                            "latency_scope": "local_paper_cancel_pipeline"
                        }),
                    });
                }
            }
        }
        events
    }

    pub(super) fn clear_market(&mut self, market_id: &MarketId) -> Vec<QualityEvent> {
        self.orders.retain(|_, order| &order.market_id != market_id);
        let mut retained = Vec::with_capacity(self.pending_markouts.len());
        let mut missing = Vec::new();
        for markout in self.pending_markouts.drain(..) {
            if &markout.market_id == market_id {
                missing.push(QualityEvent {
                    event_type: "paper_fill_markout_missing",
                    payload: json!({
                        "fill_id": markout.fill_id,
                        "fill_source": markout.source,
                        "order_id": markout.order_id,
                        "market_id": markout.market_id,
                        "token_id": markout.token_id,
                        "side": markout.side,
                        "fill_price": markout.fill_price.to_string(),
                        "fill_size": markout.fill_size.to_string(),
                        "fee_per_share": markout.fee_per_share.to_string(),
                        "fill_ts": markout.fill_ts,
                        "horizon_seconds": markout.horizon_seconds,
                        "reason": "market_settled_before_observation",
                        "research_only": true
                    }),
                });
            } else {
                retained.push(markout);
            }
        }
        self.pending_markouts = retained;
        missing
    }

    fn observe_trade(&mut self, event: &MarketChannelEvent) -> Vec<QualityEvent> {
        let Some(token_id) = event.token_id.as_deref().or(event.asset_id.as_deref()) else {
            return Vec::new();
        };
        let (Some(trade_price), Some(trade_size)) = (
            decimal_text(event.price.as_deref()),
            decimal_text(event.size.as_deref()),
        ) else {
            return Vec::new();
        };
        if trade_size <= Decimal::ZERO {
            return Vec::new();
        }
        let trade_ts = event.source_ts.unwrap_or(event.recorded_ts);
        let trade_side = event.side.as_deref().map(str::to_ascii_lowercase);
        let mut events = Vec::new();
        let mut scheduled = Vec::new();
        let mut matching = self
            .orders
            .values()
            .filter(|order| {
                order.token_id.as_ref() == token_id
                    && trade_ts >= order.live_ts
                    && order.snapshot_finalized
                    && order.shadow_remaining_size > Decimal::ZERO
            })
            .map(|order| order.order_id.clone())
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| {
            let left = &self.orders[left];
            let right = &self.orders[right];
            let price_priority = match left.side {
                Side::Buy => right.quote_price.cmp(&left.quote_price),
                Side::Sell => left.quote_price.cmp(&right.quote_price),
            };
            price_priority
                .then_with(|| left.submitted_ts.cmp(&right.submitted_ts))
                .then_with(|| left.order_id.cmp(&right.order_id))
        });
        let mut unallocated_trade_size = trade_size;
        for order_id in matching {
            if unallocated_trade_size <= Decimal::ZERO {
                break;
            }
            let Some(order) = self.orders.get_mut(&order_id) else {
                continue;
            };
            let hits_quote = match order.side {
                Side::Buy => trade_price <= order.quote_price,
                Side::Sell => trade_price >= order.quote_price,
            };
            let correct_taker_side = matches!(
                (order.side.clone(), trade_side.as_deref()),
                (Side::Buy, Some("sell")) | (Side::Sell, Some("buy"))
            );
            if !hits_quote {
                continue;
            }
            let strict_trade_through = match order.side {
                Side::Buy => trade_price < order.quote_price,
                Side::Sell => trade_price > order.quote_price,
            };
            let ahead_before = order.size_ahead;
            let remaining_before = order.shadow_remaining_size;
            let consumed_ahead = if correct_taker_side {
                unallocated_trade_size.min(order.size_ahead)
            } else {
                Decimal::ZERO
            };
            order.size_ahead -= consumed_ahead;
            unallocated_trade_size -= consumed_ahead;
            let fill_size = if correct_taker_side {
                unallocated_trade_size.min(order.shadow_remaining_size)
            } else {
                Decimal::ZERO
            };
            order.shadow_remaining_size -= fill_size;
            order.shadow_filled_size += fill_size;
            unallocated_trade_size -= fill_size;
            events.push(QualityEvent {
                event_type: if fill_size > Decimal::ZERO {
                    "paper_queue_shadow_fill"
                } else {
                    "paper_queue_observation"
                },
                payload: json!({
                    "order_id": order.order_id,
                    "market_id": order.market_id,
                    "token_id": order.token_id,
                    "side": order.side,
                    "quote_price": order.quote_price.to_string(),
                    "trade_price": trade_price.to_string(),
                    "trade_size": trade_size.to_string(),
                    "trade_size_unallocated": unallocated_trade_size.to_string(),
                    "trade_side": event.side,
                    "trade_ts": trade_ts,
                    "at_or_through_quote": hits_quote,
                    "strict_trade_through": strict_trade_through,
                    "taker_side_compatible": correct_taker_side,
                    "size_ahead_before": ahead_before.to_string(),
                    "size_ahead_consumed": consumed_ahead.to_string(),
                    "size_ahead_after": order.size_ahead.to_string(),
                    "shadow_fill_size": fill_size.to_string(),
                    "shadow_filled_size": order.shadow_filled_size.to_string(),
                    "shadow_remaining_before": remaining_before.to_string(),
                    "shadow_remaining_after": order.shadow_remaining_size.to_string(),
                    "partial_fill": fill_size > Decimal::ZERO && order.shadow_remaining_size > Decimal::ZERO,
                    "research_only": true,
                    "changes_live_execution": false
                }),
            });
            if fill_size > Decimal::ZERO {
                scheduled.push((
                    order.order_id.clone(),
                    order.market_id.clone(),
                    order.token_id.clone(),
                    order.side.clone(),
                    order.quote_price,
                    fill_size,
                    trade_ts,
                ));
            }
        }
        for (order_id, market_id, token_id, side, price, size, ts) in scheduled {
            self.schedule_markouts(
                "queue_shadow_fill",
                order_id,
                market_id,
                token_id,
                side,
                price,
                size,
                Decimal::ZERO,
                ts,
            );
        }
        events
    }

    fn observe_level_changes(&mut self, event: &MarketChannelEvent) -> Vec<QualityEvent> {
        level_rows(event)
            .into_iter()
            .flat_map(|(token_id, side, price, size)| {
                self.orders
                    .values_mut()
                    .filter_map(move |order| {
                        if order.token_id.as_ref() != token_id || order.quote_price != price {
                            return None;
                        }
                        let compatible_side = match order.side {
                            Side::Buy => side.eq_ignore_ascii_case("buy"),
                            Side::Sell => side.eq_ignore_ascii_case("sell"),
                        };
                        if !compatible_side {
                            return None;
                        }
                        let previous = order.last_visible_size_at_quote;
                        order.last_visible_size_at_quote = size;
                        Some(QualityEvent {
                            event_type: "paper_queue_level_observation",
                            payload: json!({
                                "order_id": order.order_id,
                                "market_id": order.market_id,
                                "token_id": order.token_id,
                                "side": order.side,
                                "quote_price": order.quote_price.to_string(),
                                "previous_visible_size_at_quote": previous.to_string(),
                                "visible_size_at_quote": size.to_string(),
                                "visible_size_delta": (size - previous).to_string(),
                                "conservative_size_ahead": order.size_ahead.to_string(),
                                "observed_ts": event.source_ts.unwrap_or(event.recorded_ts),
                                "depletion_can_fill": false,
                                "research_only": true
                            }),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_markouts(
        &mut self,
        source: &'static str,
        order_id: OrderId,
        market_id: MarketId,
        token_id: TokenId,
        side: Side,
        fill_price: Decimal,
        fill_size: Decimal,
        fee_per_share: Decimal,
        fill_ts: DateTime<Utc>,
    ) {
        self.next_fill_id += 1;
        let fill_id = format!("paper-quality-{}-{}", order_id, self.next_fill_id);
        self.pending_markouts
            .extend(
                MARKOUT_HORIZONS_SECONDS
                    .into_iter()
                    .map(|horizon_seconds| PendingMarkout {
                        fill_id: fill_id.clone(),
                        source,
                        order_id: order_id.clone(),
                        market_id: market_id.clone(),
                        token_id: token_id.clone(),
                        side: side.clone(),
                        fill_price,
                        fill_size,
                        fee_per_share,
                        fill_ts,
                        horizon_seconds,
                    }),
            );
    }
}

fn decimal_text(value: Option<&str>) -> Option<Decimal> {
    Decimal::from_str(value?).ok()
}

fn visible_depth(book: &BookState, side: &Side, quote_price: Decimal) -> (Decimal, Decimal) {
    let levels = match side {
        Side::Buy => &book.bids,
        Side::Sell => &book.asks,
    };
    let same = levels
        .iter()
        .filter(|level| level.price == quote_price)
        .map(|level| level.size)
        .sum();
    let better = levels
        .iter()
        .filter(|level| match side {
            Side::Buy => level.price > quote_price,
            Side::Sell => level.price < quote_price,
        })
        .map(|level| level.size)
        .sum();
    (same, better)
}

fn parse_ts(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn level_rows(event: &MarketChannelEvent) -> Vec<(String, String, Decimal, Decimal)> {
    let rows = event
        .raw_payload
        .get("price_changes")
        .or_else(|| event.raw_payload.get("changes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| vec![event.raw_payload.clone()]);
    rows.into_iter()
        .filter_map(|row| {
            let token = row
                .get("asset_id")
                .or_else(|| row.get("token_id"))
                .and_then(Value::as_str)?
                .to_owned();
            let side = row.get("side").and_then(Value::as_str)?.to_owned();
            let price = decimal_value(row.get("price"))?;
            let size = decimal_value(row.get("size"))?;
            Some((token, side, price, size))
        })
        .collect()
}

fn decimal_value(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(value) => Decimal::from_str(value).ok(),
        Value::Number(value) => Decimal::from_str(&value.to_string()).ok(),
        _ => None,
    }
}

pub(super) fn deterministic_probe(at: DateTime<Utc>) -> Vec<QualityEvent> {
    let run_id = format!("paper-eq-probe-{}", at.timestamp_micros());
    let market_id = MarketId::new(format!("{run_id}-market"));
    let token_id = TokenId::new(format!("{run_id}-token"));
    let mut tracker = ExecutionQualityTracker::default();
    let mut events = Vec::new();

    let fill_decision = probe_decision(&market_id, &token_id, "0.50", "5");
    let fill_report = probe_report(
        &market_id,
        &token_id,
        &format!("{run_id}-fill-order"),
        "paper_resting",
        at,
    );
    let initial = probe_book(&token_id, at, "0.50", "10", "0.52", "4", true);
    if let Some(payload) = tracker.register_order(&fill_decision, &fill_report, Some(&initial), 250)
    {
        events.push(QualityEvent {
            event_type: "paper_order_queue_registration",
            payload,
        });
    }
    events.extend(tracker.observe_book(&probe_book(
        &token_id,
        at + Duration::seconds(1),
        "0.50",
        "10",
        "0.52",
        "4",
        true,
    )));
    events.extend(tracker.observe_market_event(&probe_trade(
        &market_id,
        &token_id,
        at + Duration::seconds(2),
        "0.49",
        "16",
    )));
    events.extend(tracker.observe_book(&probe_book(
        &token_id,
        at + Duration::seconds(3),
        "0.51",
        "5",
        "0.53",
        "5",
        false,
    )));
    events.extend(tracker.observe_market_event(&probe_trade(
        &market_id,
        &token_id,
        at + Duration::seconds(4),
        "0.49",
        "3",
    )));
    for seconds in [7, 9, 32, 34] {
        events.extend(tracker.observe_book(&probe_book(
            &token_id,
            at + Duration::seconds(seconds),
            "0.52",
            "5",
            "0.54",
            "5",
            false,
        )));
    }

    let cancel_decision = probe_decision(&market_id, &token_id, "0.48", "2");
    let cancel_order_id = format!("{run_id}-cancel-order");
    let cancel_resting = probe_report(
        &market_id,
        &token_id,
        &cancel_order_id,
        "paper_resting",
        at + Duration::seconds(10),
    );
    if let Some(payload) =
        tracker.register_order(&cancel_decision, &cancel_resting, Some(&initial), 250)
    {
        events.push(QualityEvent {
            event_type: "paper_order_queue_registration",
            payload,
        });
    }
    events.extend(tracker.observe_book(&probe_book(
        &token_id,
        at + Duration::seconds(11),
        "0.48",
        "8",
        "0.52",
        "4",
        true,
    )));
    let mut cancelled = probe_report(
        &market_id,
        &token_id,
        &cancel_order_id,
        "paper_cancelled",
        at + Duration::seconds(12),
    );
    cancelled.raw.insert(
        "cancel_requested_ts".to_owned(),
        json!((at + Duration::milliseconds(11_500)).to_rfc3339()),
    );
    events.extend(tracker.observe_execution_report(&cancelled));

    for event in &mut events {
        if let Value::Object(payload) = &mut event.payload {
            payload.insert("probe".to_owned(), json!(true));
            payload.insert("probe_run_id".to_owned(), json!(run_id));
            payload.insert("changes_live_execution".to_owned(), json!(false));
        }
    }
    let count = |kind: &str| {
        events
            .iter()
            .filter(|event| event.event_type == kind)
            .count()
    };
    let partial_fills = events
        .iter()
        .filter(|event| {
            event.event_type == "paper_queue_shadow_fill"
                && event.payload["partial_fill"].as_bool() == Some(true)
        })
        .count();
    let completed = count("paper_order_queue_registration") == 2
        && count("paper_order_queue_snapshot") == 2
        && count("paper_queue_shadow_fill") == 2
        && partial_fills == 1
        && count("paper_cancel_latency") == 1
        && count("paper_fill_markout") == 6;
    events.push(QualityEvent {
        event_type: "execution_quality_probe_completed",
        payload: json!({
            "probe": true,
            "probe_run_id": run_id,
            "status": if completed { "pass" } else { "fail" },
            "queue_registrations": count("paper_order_queue_registration"),
            "queue_snapshots": count("paper_order_queue_snapshot"),
            "queue_shadow_fills": count("paper_queue_shadow_fill"),
            "partial_fills": partial_fills,
            "strict_trade_through_events": events.iter().filter(|event| event.payload["strict_trade_through"].as_bool() == Some(true)).count(),
            "cancel_latency_events": count("paper_cancel_latency"),
            "markout_1s": events.iter().filter(|event| event.payload["horizon_seconds"] == 1).count(),
            "markout_5s": events.iter().filter(|event| event.payload["horizon_seconds"] == 5).count(),
            "markout_30s": events.iter().filter(|event| event.payload["horizon_seconds"] == 30).count(),
            "venue_contacted": false,
            "live_order_placed": false,
            "changes_live_execution": false,
            "research_only": true
        }),
    });
    events
}

fn probe_decision(
    market_id: &MarketId,
    token_id: &TokenId,
    price: &str,
    size: &str,
) -> TradeDecision {
    TradeDecision {
        action: DecisionAction::Place,
        market_id: market_id.clone(),
        condition_id: None,
        token_id: Some(token_id.clone()),
        outcome: None,
        side: Some(Side::Buy),
        price: decimal_text(Some(price)),
        size: decimal_text(Some(size)),
        quote_amount: None,
        order_kind: Some(OrderKind::PostOnlyGtc),
        reason: "deterministic paper execution quality probe".to_owned(),
        ttl_ms: Some(60_000),
        expected_edge: None,
        post_only: true,
        tick_size: Some(Decimal::new(1, 2)),
        neg_risk: false,
    }
}

fn probe_report(
    market_id: &MarketId,
    token_id: &TokenId,
    order_id: &str,
    status: &str,
    local_ts: DateTime<Utc>,
) -> ExecutionReport {
    ExecutionReport {
        order_id: Some(OrderId::new(order_id)),
        market_id: market_id.clone(),
        token_id: Some(token_id.clone()),
        status: status.to_owned(),
        filled_size: Decimal::ZERO,
        avg_price: None,
        fee: Decimal::ZERO,
        local_ts,
        raw: BTreeMap::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn probe_book(
    token_id: &TokenId,
    local_ts: DateTime<Utc>,
    bid_price: &str,
    bid_size: &str,
    ask_price: &str,
    ask_size: &str,
    add_better_bid: bool,
) -> BookState {
    let mut bids = vec![BookLevel {
        price: decimal_text(Some(bid_price)).unwrap_or(Decimal::ZERO),
        size: decimal_text(Some(bid_size)).unwrap_or(Decimal::ZERO),
    }];
    if add_better_bid {
        bids.push(BookLevel {
            price: Decimal::new(51, 2),
            size: Decimal::from(4),
        });
    }
    BookState {
        token_id: token_id.clone(),
        bids,
        asks: vec![BookLevel {
            price: decimal_text(Some(ask_price)).unwrap_or(Decimal::ZERO),
            size: decimal_text(Some(ask_size)).unwrap_or(Decimal::ZERO),
        }],
        last_trade_price: None,
        exchange_ts: Some(local_ts),
        local_ts,
        book_hash: Some("deterministic-probe".to_owned()),
    }
}

fn probe_trade(
    market_id: &MarketId,
    token_id: &TokenId,
    recorded_ts: DateTime<Utc>,
    price: &str,
    size: &str,
) -> MarketChannelEvent {
    MarketChannelEvent {
        event_type: "trade".to_owned(),
        recorded_ts,
        source_ts: Some(recorded_ts),
        market_id: Some(market_id.to_string()),
        condition_id: None,
        token_id: Some(token_id.to_string()),
        asset_id: Some(token_id.to_string()),
        side: Some("sell".to_owned()),
        price: Some(price.to_owned()),
        size: Some(size.to_owned()),
        best_bid: None,
        best_ask: None,
        book_hash: None,
        raw_payload: json!({
            "event_type": "trade",
            "asset_id": token_id,
            "side": "sell",
            "price": price,
            "size": size
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyedge_domain::{BookLevel, OrderKind};
    use std::collections::BTreeMap;

    #[test]
    fn captures_size_ahead_partial_fills_trade_through_and_markouts() {
        let mut tracker = ExecutionQualityTracker::default();
        let decision = decision();
        let report = report("paper_resting", Decimal::ZERO, None, ts(0));
        let initial_book = book("0.51", "10", "0.52", "4", ts(0));
        let registration = tracker
            .register_order(&decision, &report, Some(&initial_book), 250)
            .unwrap();
        assert_eq!(registration["queue_snapshot_finalized"], false);
        let snapshot = tracker.observe_book(&book("0.51", "10", "0.52", "4", ts(1)));
        assert_eq!(snapshot[0].payload["visible_size_ahead_estimate"], "14");

        let first = tracker.observe_market_event(&trade("0.49", "16", ts(2)));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].event_type, "paper_queue_shadow_fill");
        assert_eq!(first[0].payload["shadow_fill_size"], "2");
        assert_eq!(first[0].payload["partial_fill"], true);
        assert_eq!(first[0].payload["strict_trade_through"], true);

        let second = tracker.observe_market_event(&trade("0.50", "3", ts(3)));
        assert_eq!(second[0].payload["shadow_fill_size"], "3");
        assert_eq!(second[0].payload["shadow_remaining_after"], "0");

        assert!(!tracker
            .observe_book(&book("0.51", "5", "0.53", "5", ts(4)))
            .is_empty());
        let later = tracker.observe_book(&book("0.52", "5", "0.54", "5", ts(34)));
        assert!(later.iter().any(|event| {
            event.payload["horizon_seconds"] == 30 && event.event_type == "paper_fill_markout"
        }));
        let fill_ids = later
            .iter()
            .filter_map(|event| event.payload["fill_id"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(fill_ids.contains("paper-quality-paper-1-1"));
        assert!(fill_ids.contains("paper-quality-paper-1-2"));
    }

    #[test]
    fn cancellation_reports_local_pipeline_latency_and_removes_tracker() {
        let mut tracker = ExecutionQualityTracker::default();
        let decision = decision();
        let resting_report = report("paper_resting", Decimal::ZERO, None, ts(0));
        tracker.register_order(&decision, &resting_report, None, 250);
        let mut cancelled = report("paper_cancelled", Decimal::ZERO, None, ts(2));
        cancelled
            .raw
            .insert("cancel_requested_ts".to_owned(), json!(ts(1).to_rfc3339()));
        let events = tracker.observe_execution_report(&cancelled);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "paper_cancel_latency");
        assert_eq!(
            events[0].payload["latency_scope"],
            "local_paper_cancel_pipeline"
        );
    }

    #[test]
    fn settlement_records_unobservable_markout_horizons() {
        let mut tracker = ExecutionQualityTracker::default();
        let decision = decision();
        let resting_report = report("paper_resting", Decimal::ZERO, None, ts(0));
        tracker.register_order(&decision, &resting_report, None, 250);
        tracker.observe_book(&book("0.51", "10", "0.52", "4", ts(1)));
        tracker.observe_market_event(&trade("0.49", "20", ts(2)));

        let missing = tracker.clear_market(&MarketId::new("market"));
        assert_eq!(missing.len(), 3);
        assert!(missing.iter().all(|event| {
            event.event_type == "paper_fill_markout_missing"
                && event.payload["reason"] == "market_settled_before_observation"
        }));
    }

    #[test]
    fn executable_markouts_are_net_of_fill_fees() {
        let mut tracker = ExecutionQualityTracker::default();
        let decision = decision();
        let resting = report("paper_resting", Decimal::ZERO, None, ts(0));
        tracker.register_order(&decision, &resting, None, 250);
        tracker.observe_book(&book("0.50", "4", "0.51", "4", ts(1)));
        let mut filled = report("paper_filled", dec("5"), Some(dec("0.50")), ts(2));
        filled.fee = dec("0.05");
        tracker.observe_execution_report(&filled);

        let markouts = tracker.observe_book(&book("0.52", "4", "0.53", "4", ts(33)));
        let thirty = markouts
            .iter()
            .find(|event| event.payload["horizon_seconds"] == 30)
            .expect("30-second markout");
        assert_eq!(thirty.payload["executable_markout_per_share"], "0.02");
        assert_eq!(thirty.payload["fee_per_share"], "0.01");
        assert_eq!(thirty.payload["net_executable_markout_per_share"], "0.01");
        assert_eq!(thirty.payload["net_executable_markout_pnl"], "0.05");
    }

    #[test]
    fn deterministic_probe_exercises_complete_lifecycle_without_venue_contact() {
        let events = deterministic_probe(ts(0));
        let completed = events
            .iter()
            .find(|event| event.event_type == "execution_quality_probe_completed")
            .unwrap();
        assert_eq!(completed.payload["status"], "pass");
        assert_eq!(completed.payload["queue_registrations"], 2);
        assert_eq!(completed.payload["queue_snapshots"], 2);
        assert_eq!(completed.payload["queue_shadow_fills"], 2);
        assert_eq!(completed.payload["partial_fills"], 1);
        assert_eq!(completed.payload["cancel_latency_events"], 1);
        assert_eq!(completed.payload["markout_1s"], 2);
        assert_eq!(completed.payload["markout_5s"], 2);
        assert_eq!(completed.payload["markout_30s"], 2);
        assert_eq!(completed.payload["venue_contacted"], false);
        assert!(events.iter().all(|event| event.payload["probe"] == true));
    }

    fn decision() -> TradeDecision {
        TradeDecision {
            action: DecisionAction::Place,
            market_id: MarketId::new("market"),
            condition_id: None,
            token_id: Some(TokenId::new("token")),
            outcome: None,
            side: Some(Side::Buy),
            price: Some(dec("0.50")),
            size: Some(dec("5")),
            quote_amount: None,
            order_kind: Some(OrderKind::PostOnlyGtc),
            reason: "test".to_owned(),
            ttl_ms: Some(60_000),
            expected_edge: None,
            post_only: true,
            tick_size: Some(dec("0.01")),
            neg_risk: false,
        }
    }

    fn report(
        status: &str,
        filled_size: Decimal,
        avg_price: Option<Decimal>,
        local_ts: DateTime<Utc>,
    ) -> ExecutionReport {
        ExecutionReport {
            order_id: Some(OrderId::new("paper-1")),
            market_id: MarketId::new("market"),
            token_id: Some(TokenId::new("token")),
            status: status.to_owned(),
            filled_size,
            avg_price,
            fee: Decimal::ZERO,
            local_ts,
            raw: BTreeMap::new(),
        }
    }

    fn book(
        bid_price: &str,
        bid_size: &str,
        ask_price: &str,
        ask_size: &str,
        local_ts: DateTime<Utc>,
    ) -> BookState {
        BookState {
            token_id: TokenId::new("token"),
            bids: vec![
                BookLevel {
                    price: dec(bid_price),
                    size: dec(bid_size),
                },
                BookLevel {
                    price: dec("0.50"),
                    size: dec("4"),
                },
            ],
            asks: vec![BookLevel {
                price: dec(ask_price),
                size: dec(ask_size),
            }],
            last_trade_price: None,
            exchange_ts: Some(local_ts),
            local_ts,
            book_hash: Some("hash".to_owned()),
        }
    }

    fn trade(price: &str, size: &str, recorded_ts: DateTime<Utc>) -> MarketChannelEvent {
        MarketChannelEvent {
            event_type: "last_trade_price".to_owned(),
            recorded_ts,
            source_ts: Some(recorded_ts),
            market_id: Some("market".to_owned()),
            condition_id: None,
            token_id: Some("token".to_owned()),
            asset_id: None,
            side: Some("SELL".to_owned()),
            price: Some(price.to_owned()),
            size: Some(size.to_owned()),
            best_bid: None,
            best_ask: None,
            book_hash: None,
            raw_payload: json!({}),
        }
    }

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn ts(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_750_000_000 + seconds, 0).unwrap()
    }
}
