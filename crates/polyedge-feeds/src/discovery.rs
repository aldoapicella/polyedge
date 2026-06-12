use crate::util::{decimal, get_json, parse_datetime, value_opt_text, value_text, with_query};
use crate::FeedError;
use chrono::{DateTime, Utc};
use polyedge_config::RuntimeSettings;
use polyedge_domain::{MarketId, MarketSpec, MarketStatus, TokenId};
use regex::Regex;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

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
