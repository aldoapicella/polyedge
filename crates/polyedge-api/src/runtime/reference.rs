use chrono::{DateTime, Utc};
use polyedge_config::RuntimeSettings;
use polyedge_domain::ReferencePrice;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct ReferenceAggregator {
    latest_by_source: BTreeMap<String, ReferencePrice>,
}

impl ReferenceAggregator {
    pub(super) fn update(
        &mut self,
        reference: ReferencePrice,
        settings: &RuntimeSettings,
    ) -> ReferencePrice {
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
