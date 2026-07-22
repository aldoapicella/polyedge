use super::*;

const ORDER_FACT_SCHEMA: &str = "polyedge.loss_diagnostics.order_lifecycle_fact.v1";
const FILL_FACT_SCHEMA: &str = "polyedge.loss_diagnostics.fill_markout_fact.v1";
const SUMMARY_SCHEMA: &str = "polyedge.loss_diagnostics.summary.v1";
const ORDER_FACT_FILE: &str = "order_lifecycle_fact.jsonl";
const FILL_FACT_FILE: &str = "fill_markout_fact.jsonl";
const SUMMARY_FILE: &str = "loss_diagnostics.json";
const MARKDOWN_FILE: &str = "loss_diagnostics.md";
const ARTIFACT_MANIFEST_FILE: &str = "loss_diagnostics_artifact_manifest.json";
const ARTIFACT_MANIFEST_SCHEMA: &str = "polyedge.loss_diagnostics.artifact_manifest.v1";

#[derive(Clone, Debug)]
pub struct LossDiagnosticsOptions {
    pub input: PathBuf,
    pub out: PathBuf,
}

#[derive(Clone, Debug)]
struct ObservedEvent {
    event_type: String,
    recorded_ts: DateTime<Utc>,
    payload: Value,
    event_sha256: String,
    journal_event_sha256: Option<String>,
    settlement_journal_id: Option<String>,
    settlement_journal_sha256: Option<String>,
}

impl ObservedEvent {
    fn from_event(event: &EventLine) -> Option<Self> {
        Some(Self {
            event_type: event.event_type.clone(),
            recorded_ts: event.recorded_ts,
            payload: event.payload.clone(),
            event_sha256: source_event_sha256(event)?,
            journal_event_sha256: None,
            settlement_journal_id: None,
            settlement_journal_sha256: None,
        })
    }
}

fn source_event_sha256(event: &EventLine) -> Option<String> {
    canonical_value_sha256(&json!({
        "event_type": event.event_type,
        "recorded_ts": ts(event.recorded_ts),
        "payload": event.payload
    }))
}

#[derive(Default)]
struct ExactTimestampDuplicateDetector {
    recorded_ts: Option<DateTime<Utc>>,
    hashes: BTreeSet<String>,
}

impl ExactTimestampDuplicateDetector {
    fn observe(&mut self, recorded_ts: DateTime<Utc>, event_sha256: String) -> bool {
        if self.recorded_ts != Some(recorded_ts) {
            self.recorded_ts = Some(recorded_ts);
            self.hashes.clear();
        }
        !self.hashes.insert(event_sha256)
    }
}

#[derive(Clone, Debug)]
struct MarketEvidenceRef {
    event_sha256: String,
    journal_event_sha256: Option<String>,
    settlement_journal_id: Option<String>,
    settlement_journal_sha256: Option<String>,
}

impl MarketEvidenceRef {
    fn from_event(event: &ObservedEvent) -> Self {
        Self {
            event_sha256: event.event_sha256.clone(),
            journal_event_sha256: event.journal_event_sha256.clone(),
            settlement_journal_id: event.settlement_journal_id.clone(),
            settlement_journal_sha256: event.settlement_journal_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct BatchRecord {
    recorded_ts: DateTime<Utc>,
    event_sha256: String,
    decision_config_sha256: String,
    input: DecisionPipelineInputV3,
    output_hashes: BTreeMap<u64, String>,
    place_output_hashes: BTreeMap<u64, String>,
    market_start_evidence_sha256: String,
}

#[derive(Clone, Debug)]
struct DecisionRecord {
    parsed: DurableDecisionOutputV3,
    recorded_ts: DateTime<Utc>,
    event_sha256: String,
    payload: Value,
}

#[derive(Clone, Debug)]
struct ApplicationRecord {
    parsed: AppliedDecisionOutputV1,
    recorded_ts: DateTime<Utc>,
    payload: Value,
}

#[derive(Clone, Debug)]
struct JournalBuffer {
    event_count: u64,
    journal_sha256: String,
    events: BTreeMap<u64, ObservedEvent>,
    conflicted: bool,
}

#[derive(Clone, Debug)]
struct ParsedMarkout {
    observation: MarkoutObservation,
    reason: Option<String>,
    event_sha256: String,
    recorded_ts: DateTime<Utc>,
}

impl ParsedMarkout {
    fn envelope_chronology_is_valid(&self) -> bool {
        let Some(observed_ts) = self.observation.observed_ts else {
            return self.observation.missing;
        };
        let target_ts = self.observation.key.fill_ts + Duration::seconds(self.observation.horizon);
        self.recorded_ts >= observed_ts && observed_ts >= target_ts
    }
}

struct ParsedMarkouts {
    rows: BTreeMap<(FillLifecycleJoinKey, i64), ParsedMarkout>,
    fill_ids_by_key: BTreeMap<FillLifecycleJoinKey, String>,
    orphan: usize,
    invalid: usize,
}

struct ValidatedMarketEvidence {
    starts: BTreeMap<String, MarketEvidenceRef>,
    settlements: BTreeMap<String, MarketEvidenceRef>,
}

#[derive(Default)]
struct LossDiagnosticsAccumulator {
    exact_duplicate_detector: ExactTimestampDuplicateDetector,
    batches: BTreeMap<String, BatchRecord>,
    decisions: BTreeMap<DecisionOutputKeyV3, DecisionRecord>,
    applications: BTreeMap<DecisionOutputKeyV3, ApplicationRecord>,
    registrations: BTreeMap<String, Vec<ObservedEvent>>,
    snapshots: BTreeMap<String, Vec<ObservedEvent>>,
    execution_reports: BTreeMap<String, Vec<ObservedEvent>>,
    cancel_events: BTreeMap<String, Vec<ObservedEvent>>,
    fill_events: Vec<ObservedEvent>,
    markout_events: Vec<ObservedEvent>,
    runtime_provenance: Vec<(DateTime<Utc>, Value)>,
    start_evidence_events: BTreeMap<String, Vec<ObservedEvent>>,
    settlement_evidence_events: BTreeMap<String, Vec<ObservedEvent>>,
    journals: BTreeMap<String, JournalBuffer>,
    fatal_errors: BTreeSet<String>,
    claimed_v3_decision_events: usize,
    invalid_v3_decisions: usize,
    invalid_v3_batches: usize,
    invalid_applications: usize,
    invalid_registrations: usize,
    invalid_snapshots: usize,
    invalid_fill_lifecycles: usize,
    invalid_cancellations: usize,
    invalid_execution_reports: usize,
    journal_retry_duplicates: usize,
    journal_incomplete: usize,
    decision_retry_duplicates: usize,
    application_retry_duplicates: usize,
    batch_retry_duplicates: usize,
    probe_events_excluded: usize,
    duplicate_event_lines: usize,
}

struct DiagnosticsFacts {
    order_rows: Vec<Value>,
    fill_rows: Vec<Value>,
    summary: Value,
}

impl LossDiagnosticsAccumulator {
    fn observe(&mut self, event: &EventLine) {
        let Some(event_sha256) = source_event_sha256(event) else {
            self.fatal_errors
                .insert("event could not be canonically hashed".to_owned());
            return;
        };
        if self
            .exact_duplicate_detector
            .observe(event.recorded_ts, event_sha256)
        {
            self.duplicate_event_lines += 1;
        }
        if event.payload["probe"].as_bool().unwrap_or(false)
            || event.event_type.starts_with("execution_quality_probe")
        {
            self.probe_events_excluded += 1;
            return;
        }
        if has_settlement_journal_fields(&event.payload) {
            self.observe_journal_event(event);
            return;
        }
        self.observe_verified(event);
    }

    fn observe_journal_event(&mut self, event: &EventLine) {
        let parsed = (|| {
            let source_event_sha256 = ObservedEvent::from_event(event)?.event_sha256;
            if event.payload["settlement_journal_schema"].as_str()
                != Some("polyedge.paper_settlement_journal.v1")
            {
                return None;
            }
            let journal_id = optional_text(&event.payload, "settlement_journal_id")?;
            let event_index = event.payload["settlement_journal_event_index"].as_u64()?;
            let event_count = event.payload["settlement_journal_event_count"].as_u64()?;
            let journal_sha256 = optional_text(&event.payload, "settlement_journal_sha256")?;
            if !valid_settlement_journal_id(&journal_id)
                || event_count == 0
                || event_index >= event_count
                || !valid_prefixed_sha256(&journal_sha256)
            {
                return None;
            }
            let mut payload = event.payload.clone();
            let object = payload.as_object_mut()?;
            for field in settlement_journal_fields() {
                object.remove(field);
            }
            let journal_event_sha256 = canonical_value_sha256(&json!({
                "event_index": event_index,
                "event_type": event.event_type,
                "payload": payload
            }))?;
            Some((
                journal_id.clone(),
                event_index,
                event_count,
                journal_sha256.clone(),
                ObservedEvent {
                    event_type: event.event_type.clone(),
                    recorded_ts: event.recorded_ts,
                    payload,
                    event_sha256: source_event_sha256,
                    journal_event_sha256: Some(journal_event_sha256),
                    settlement_journal_id: Some(journal_id.clone()),
                    settlement_journal_sha256: Some(journal_sha256.clone()),
                },
            ))
        })();
        let Some((journal_id, event_index, event_count, journal_sha256, observed)) = parsed else {
            self.fatal_errors
                .insert("invalid settlement-journal identity or binding".to_owned());
            return;
        };
        let journal = self
            .journals
            .entry(journal_id.clone())
            .or_insert_with(|| JournalBuffer {
                event_count,
                journal_sha256: journal_sha256.clone(),
                events: BTreeMap::new(),
                conflicted: false,
            });
        if journal.event_count != event_count || journal.journal_sha256 != journal_sha256 {
            journal.conflicted = true;
            self.fatal_errors
                .insert(format!("conflicting settlement journal {journal_id}"));
            return;
        }
        if let Some(existing) = journal.events.get(&event_index) {
            if existing.journal_event_sha256 == observed.journal_event_sha256 {
                self.journal_retry_duplicates += 1;
            } else {
                journal.conflicted = true;
                self.fatal_errors.insert(format!(
                    "duplicate settlement-journal identity {journal_id}/{event_index}"
                ));
            }
            return;
        }
        journal.events.insert(event_index, observed);
    }

    fn apply_complete_journals(&mut self) {
        for (journal_id, journal) in std::mem::take(&mut self.journals) {
            if journal.conflicted {
                continue;
            }
            let complete = journal.events.len() == journal.event_count as usize
                && (0..journal.event_count).all(|index| journal.events.contains_key(&index));
            let settlement_count = journal
                .events
                .values()
                .filter(|event| event.event_type == "paper_settlement")
                .count();
            if !complete || settlement_count != 1 {
                self.journal_incomplete += 1;
                continue;
            }
            let events = journal
                .events
                .iter()
                .map(|(event_index, event)| {
                    json!({
                        "event_index": event_index,
                        "event_type": event.event_type,
                        "payload": event.payload
                    })
                })
                .collect::<Vec<_>>();
            let expected = canonical_value_sha256(&json!({
                "schema": "polyedge.paper_settlement_journal.v1",
                "settlement_journal_id": journal_id,
                "settlement_journal_event_count": journal.event_count,
                "events": events
            }));
            if expected.as_deref() != Some(journal.journal_sha256.as_str()) {
                self.fatal_errors
                    .insert(format!("hash-invalid settlement journal {journal_id}"));
                continue;
            }
            for event in journal.events.into_values() {
                self.observe_observed(event);
            }
        }
    }

    fn observe_verified(&mut self, event: &EventLine) {
        let Some(observed) = ObservedEvent::from_event(event) else {
            self.fatal_errors
                .insert("event could not be canonically hashed".to_owned());
            return;
        };
        self.observe_observed(observed);
    }

    fn observe_observed(&mut self, observed: ObservedEvent) {
        match observed.event_type.as_str() {
            "runtime_provenance" => self
                .runtime_provenance
                .push((observed.recorded_ts, observed.payload.clone())),
            "strategy_decision_batch" => self.observe_batch(observed),
            "decision"
                if observed
                    .payload
                    .get("decision_batch_schema_version")
                    .is_some() =>
            {
                self.observe_decision(observed)
            }
            "paper_decision_output_applied" => self.observe_application(observed),
            "paper_order_queue_registration" => {
                if let Some(order_id) = optional_text(&observed.payload, "order_id") {
                    self.registrations
                        .entry(order_id)
                        .or_default()
                        .push(observed);
                } else {
                    self.invalid_registrations += 1;
                }
            }
            "paper_order_queue_snapshot" => {
                if let Some(order_id) = optional_text(&observed.payload, "order_id") {
                    self.snapshots.entry(order_id).or_default().push(observed);
                } else {
                    self.invalid_snapshots += 1;
                }
            }
            "execution_report" => {
                let filled_size = decimal(observed.payload.get("filled_size"));
                if let Some(order_id) = optional_text(&observed.payload, "order_id") {
                    if filled_size.is_some_and(|size| size > Decimal::ZERO) {
                        self.fill_events.push(observed.clone());
                    }
                    self.execution_reports
                        .entry(order_id)
                        .or_default()
                        .push(observed);
                } else {
                    self.invalid_execution_reports += 1;
                    if filled_size.is_some_and(|size| size > Decimal::ZERO) {
                        self.invalid_fill_lifecycles += 1;
                    }
                }
            }
            "paper_queue_shadow_fill" => self.fill_events.push(observed),
            "paper_cancel_latency" => {
                if let Some(order_id) = optional_text(&observed.payload, "order_id") {
                    self.cancel_events
                        .entry(order_id)
                        .or_default()
                        .push(observed);
                } else {
                    self.invalid_cancellations += 1;
                }
            }
            "paper_fill_markout" | "paper_fill_markout_missing" => {
                self.markout_events.push(observed)
            }
            "market_start_price" => {
                if let Some(market_id) = optional_text(&observed.payload, "market_id") {
                    self.start_evidence_events
                        .entry(market_id)
                        .or_default()
                        .push(observed);
                }
            }
            "paper_settlement" => {
                if let Some(market_id) = optional_text(&observed.payload, "market_id") {
                    self.settlement_evidence_events
                        .entry(market_id)
                        .or_default()
                        .push(observed);
                }
            }
            _ => {}
        }
    }

    fn observe_batch(&mut self, event: ObservedEvent) {
        let Some((outputs, decision_config_sha256, start)) =
            validate_strategy_batch_v3(&event.payload)
        else {
            self.invalid_v3_batches += 1;
            return;
        };
        let Some(batch_id) = optional_text(&event.payload, "batch_id") else {
            self.invalid_v3_batches += 1;
            return;
        };
        let Ok(input) = serde_json::from_value::<DecisionPipelineInputV3>(
            event.payload["pipeline_input"].clone(),
        ) else {
            self.invalid_v3_batches += 1;
            return;
        };
        let place_output_hashes = outputs
            .iter()
            .enumerate()
            .filter(|(_, output)| output.decision["action"].as_str() == Some("place"))
            .map(|(index, output)| (index as u64, output.decision_sha256.clone()))
            .collect::<BTreeMap<_, _>>();
        let output_hashes = outputs
            .into_iter()
            .enumerate()
            .map(|(index, output)| (index as u64, output.decision_sha256))
            .collect::<BTreeMap<_, _>>();
        let Some(market_start_evidence_sha256) =
            canonical_value_sha256(&serde_json::to_value(start).unwrap_or(Value::Null))
        else {
            self.invalid_v3_batches += 1;
            return;
        };
        let record = BatchRecord {
            recorded_ts: event.recorded_ts,
            event_sha256: event.event_sha256.clone(),
            decision_config_sha256,
            input,
            output_hashes,
            place_output_hashes,
            market_start_evidence_sha256,
        };
        if let Some(existing) = self.batches.get(&batch_id) {
            if existing.event_sha256 == event.event_sha256 {
                self.batch_retry_duplicates += 1;
            } else {
                self.fatal_errors.insert(format!(
                    "duplicate/conflicting Protocol-v3 batch {batch_id}"
                ));
            }
            return;
        }
        self.batches.insert(batch_id, record);
    }

    fn observe_decision(&mut self, event: ObservedEvent) {
        self.claimed_v3_decision_events += 1;
        if event.payload["decision_batch_schema_version"].as_u64() != Some(3) {
            self.invalid_v3_decisions += 1;
            return;
        }
        let Some(parsed) = durable_decision_output_v3(&event.payload) else {
            self.invalid_v3_decisions += 1;
            return;
        };
        if parsed.action != "place" {
            return;
        }
        let record = DecisionRecord {
            parsed: parsed.clone(),
            recorded_ts: event.recorded_ts,
            event_sha256: event.event_sha256.clone(),
            payload: event.payload,
        };
        if let Some(existing) = self.decisions.get(&parsed.key) {
            if existing.event_sha256 == event.event_sha256 {
                self.decision_retry_duplicates += 1;
            } else {
                self.fatal_errors.insert(format!(
                    "duplicate/conflicting Protocol-v3 decision output {}/{}",
                    parsed.key.batch_id, parsed.key.output_index
                ));
            }
            return;
        }
        self.decisions.insert(parsed.key.clone(), record);
    }

    fn observe_application(&mut self, event: ObservedEvent) {
        let Some(parsed) = applied_decision_output_v1(&event.payload) else {
            self.invalid_applications += 1;
            return;
        };
        if parsed.action != "place" {
            return;
        }
        let record = ApplicationRecord {
            parsed: parsed.clone(),
            recorded_ts: event.recorded_ts,
            payload: event.payload,
        };
        if let Some(existing) = self.applications.get(&parsed.key) {
            if existing.parsed.event_sha256 == parsed.event_sha256 {
                self.application_retry_duplicates += 1;
            } else {
                self.fatal_errors.insert(format!(
                    "duplicate/conflicting applied decision output {}/{}",
                    parsed.key.batch_id, parsed.key.output_index
                ));
            }
            return;
        }
        self.applications.insert(parsed.key.clone(), record);
    }

    fn finish(
        mut self,
        audit: &Value,
        snapshot: &Value,
    ) -> Result<DiagnosticsFacts, ResearchError> {
        self.apply_complete_journals();
        if self.batches.is_empty() && self.decisions.is_empty() {
            return Err(ResearchError::InvalidInput(
                "loss diagnostics requires explicit valid Protocol-v3 decision-output identity or strategy batch"
                    .to_owned(),
            ));
        }
        self.reject_duplicate_lifecycle_inputs();
        if !self.fatal_errors.is_empty() {
            return Err(ResearchError::InvalidInput(format!(
                "loss diagnostics failed closed: {}",
                self.fatal_errors.into_iter().collect::<Vec<_>>().join("; ")
            )));
        }

        let runtime_provenance = summarize_runtime_provenance(&self.runtime_provenance);
        let runtime_stable_identity = summarize_stable_runtime_identity(&self.runtime_provenance);
        let runtime_v3 = self.runtime_provenance_is_stable_v3(&runtime_stable_identity);
        let runtime_hashes = self
            .runtime_provenance
            .iter()
            .filter_map(|(_, payload)| canonical_value_sha256(payload))
            .collect::<BTreeSet<_>>();
        let expected_place_outputs = self
            .batches
            .values()
            .map(|batch| batch.place_output_hashes.len())
            .sum::<usize>();
        let expected_markets = self
            .batches
            .values()
            .filter(|batch| !batch.place_output_hashes.is_empty())
            .map(|batch| batch.input.market.market_id.to_string())
            .collect::<BTreeSet<_>>();
        let ValidatedMarketEvidence {
            starts: valid_start_evidence,
            settlements: valid_settlement_evidence,
        } = self.validated_market_evidence(&expected_markets)?;

        let mut order_ids = BTreeMap::<String, DecisionOutputKeyV3>::new();
        for application in self.applications.values() {
            let Some(order_id) = application.parsed.order_id.as_ref() else {
                continue;
            };
            if let Some(existing) = order_ids.get(order_id) {
                if existing != &application.parsed.key {
                    return Err(ResearchError::InvalidInput(format!(
                        "loss diagnostics failed closed: order ID {order_id} is reused across applied outputs"
                    )));
                }
            } else {
                order_ids.insert(order_id.clone(), application.parsed.key.clone());
            }
        }

        let (registration_identity, invalid_registration_rows) = self.unique_registrations();
        let (snapshots, invalid_snapshot_rows) = self.unique_snapshots();
        let cancel_events = unique_single_events(&self.cancel_events, "cancel lifecycle")?;
        let (fills, invalid_fill_lifecycles) = self.parse_unique_fills(&registration_identity)?;
        let expected_fill_keys = fills.keys().cloned().collect::<BTreeSet<_>>();
        let parsed_markouts = self.parse_unique_markouts(&expected_fill_keys)?;
        let markouts = parsed_markouts.rows;
        let markout_fill_ids = parsed_markouts.fill_ids_by_key;
        let orphan_markouts = parsed_markouts.orphan;
        let mut invalid_markouts = parsed_markouts.invalid;

        let valid_order_ids = order_ids.keys().cloned().collect::<BTreeSet<_>>();
        let orphan_registrations = registration_identity
            .keys()
            .filter(|order_id| !valid_order_ids.contains(*order_id))
            .count();
        let orphan_snapshots = snapshots
            .keys()
            .filter(|order_id| !valid_order_ids.contains(*order_id))
            .count();
        let orphan_fills = fills
            .keys()
            .filter(|key| !valid_order_ids.contains(&key.order_id))
            .count();
        let orphan_execution_reports = self
            .execution_reports
            .iter()
            .filter(|(order_id, _)| !valid_order_ids.contains(*order_id))
            .map(|(_, rows)| rows.len())
            .sum::<usize>();
        let orphan_applications = self
            .applications
            .keys()
            .filter(|key| !self.decisions.contains_key(*key))
            .count();

        let mut order_rows = Vec::new();
        let mut order_execution_complete = 0_usize;
        let mut order_queue_complete = 0_usize;
        let mut applied_decision_joins = 0_usize;
        for application in self.applications.values() {
            let Some(order_id) = application.parsed.order_id.as_ref() else {
                continue;
            };
            let decision = self.decisions.get(&application.parsed.key);
            let decision_joined = decision.is_some_and(|decision| {
                application_matches_decision(&application.parsed, &decision.parsed)
            });
            let batch = decision.and_then(|decision| self.matching_batch(decision));
            let protocol_v3_bound = decision_joined && batch.is_some();
            if protocol_v3_bound {
                applied_decision_joins += 1;
            }
            if let (Some(decision), Some(batch)) = (decision, batch) {
                self.validate_pre_send_chronology(decision, application, batch)?;
            }
            let identity = application
                .parsed
                .place_identity
                .as_ref()
                .expect("validated place application has identity");
            let registration = registration_identity.get(order_id);
            let snapshot = snapshots.get(order_id);
            if registration.is_some_and(|event| {
                QueueRegistrationIdentity::from_payload(&event.payload)
                    .is_none_or(|registration| !registration.matches_place_output(identity))
            }) {
                return Err(lifecycle_error(
                    order_id,
                    "queue registration identity mismatch",
                ));
            }
            if snapshot.is_some_and(|event| {
                QueueRegistrationIdentity::from_payload(&event.payload)
                    .is_none_or(|snapshot| !snapshot.matches_place_output(identity))
            }) {
                return Err(lifecycle_error(
                    order_id,
                    "queue snapshot identity mismatch",
                ));
            }
            let queue_complete = registration.is_some()
                && snapshot.is_some()
                && snapshot
                    .and_then(|snapshot| {
                        decimal(snapshot.payload.get("visible_size_ahead_estimate"))
                    })
                    .is_some();
            if queue_complete && protocol_v3_bound {
                order_queue_complete += 1;
            }
            let application_report = application
                .payload
                .get("execution_reports")
                .and_then(Value::as_array)
                .and_then(|reports| (reports.len() == 1).then(|| &reports[0]));
            let application_report_lower_bound = decision.map(|decision| {
                let lower_bound = decision.recorded_ts;
                batch.map_or(lower_bound, |batch| {
                    lower_bound
                        .max(batch.recorded_ts)
                        .max(batch.input.decision_ts)
                })
            });
            if application_report.is_none_or(|report| {
                !application_execution_report_is_valid(
                    report,
                    identity,
                    order_id,
                    application_report_lower_bound,
                    application.recorded_ts,
                )
            }) {
                return Err(lifecycle_error(
                    order_id,
                    "applied execution report identity, paper status, size, fee, or chronology is invalid",
                ));
            }
            let execution_complete = decision.is_some_and(|decision| {
                decision.payload.get("order_kind").is_some()
                    && decision.payload.get("ttl_ms").is_some()
                    && decision.payload.get("post_only").is_some()
                    && decision.payload.get("tick_size").is_some()
            }) && application_report.is_some();
            if execution_complete && protocol_v3_bound {
                order_execution_complete += 1;
            }
            let order_fills = fills
                .iter()
                .filter(|(key, _)| key.order_id == *order_id)
                .collect::<Vec<_>>();
            if order_fills
                .iter()
                .map(|(key, _)| key.source.as_str())
                .collect::<BTreeSet<_>>()
                .len()
                > 1
            {
                return Err(lifecycle_error(
                    order_id,
                    "mixes alternative fill sources in one economic aggregate",
                ));
            }
            let total_filled = order_fills
                .iter()
                .map(|(key, _)| key.fill_size)
                .sum::<Decimal>();
            let first_fill_ts = order_fills.iter().map(|(key, _)| key.fill_ts).min();
            let submitted_ts = registration
                .and_then(|record| parse_datetime(record.payload.get("submitted_ts")))
                .or_else(|| {
                    application
                        .payload
                        .pointer("/execution_reports/0/local_ts")
                        .and_then(|value| parse_datetime(Some(value)))
                })
                .unwrap_or(application.recorded_ts);
            if submitted_ts > application.recorded_ts
                || registration.is_some_and(|event| event.recorded_ts < submitted_ts)
            {
                return Err(lifecycle_error(
                    order_id,
                    "submission chronology is invalid",
                ));
            }
            let execution_reports = self
                .execution_reports
                .get(order_id)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            for report in &execution_reports {
                if !execution_report_matches_place_identity_and_chronology(
                    report,
                    identity,
                    submitted_ts,
                ) {
                    return Err(lifecycle_error(
                        order_id,
                        "execution report identity or chronology is invalid",
                    ));
                }
            }
            for (key, event) in &order_fills {
                if key.market_id != identity.market_id
                    || key.token_id != identity.token_id
                    || key.side != identity.side
                    || key.fill_price != identity.price
                {
                    return Err(lifecycle_error(order_id, "fill identity mismatch"));
                }
                if key.fill_ts < submitted_ts || event.recorded_ts < key.fill_ts {
                    return Err(lifecycle_error(order_id, "fill chronology is invalid"));
                }
            }
            if total_filled > identity.size {
                return Err(lifecycle_error(
                    order_id,
                    "filled size exceeds applied order size",
                ));
            }
            let cancel = cancel_events.get(order_id);
            let cancel_requested_ts =
                cancel.and_then(|event| parse_datetime(event.payload.get("cancel_requested_ts")));
            let cancel_ack_ts =
                cancel.and_then(|event| parse_datetime(event.payload.get("cancel_ack_ts")));
            if let Some(cancel) = cancel {
                if !payload_matches_place_identity(&cancel.payload, identity)
                    || cancel_requested_ts.is_none()
                    || cancel_ack_ts.is_none()
                    || cancel_requested_ts.is_some_and(|requested| requested < submitted_ts)
                    || cancel_requested_ts
                        .zip(cancel_ack_ts)
                        .is_some_and(|(requested, ack)| ack < requested)
                    || cancel_ack_ts.is_some_and(|ack| cancel.recorded_ts < ack)
                {
                    return Err(lifecycle_error(
                        order_id,
                        "cancel identity or chronology is invalid",
                    ));
                }
            }
            let queue_snapshot_ts = snapshot
                .and_then(|event| parse_datetime(event.payload.get("snapshot_ts")))
                .or_else(|| snapshot.map(|event| event.recorded_ts));
            if snapshot.is_some_and(|event| {
                queue_snapshot_ts.is_none_or(|snapshot_ts| {
                    snapshot_ts < submitted_ts
                        || event.recorded_ts < snapshot_ts
                        || first_fill_ts.is_some_and(|fill_ts| snapshot_ts > fill_ts)
                        || cancel_requested_ts.is_some_and(|requested| snapshot_ts > requested)
                })
            }) {
                return Err(lifecycle_error(
                    order_id,
                    "queue snapshot chronology is invalid",
                ));
            }
            let cancelled_report = execution_reports
                .iter()
                .any(|event| event.payload["status"].as_str() == Some("paper_cancelled"));
            let cancelled = cancel.is_some() || cancelled_report;
            let cancel_race = cancel_requested_ts.is_some_and(|requested| {
                order_fills.iter().any(|(key, _)| {
                    key.fill_ts >= requested && cancel_ack_ts.is_none_or(|ack| key.fill_ts <= ack)
                })
            });
            let full_fill = order_fills.iter().any(|(_, event)| {
                decimal(event.payload.get("shadow_remaining_after"))
                    .is_some_and(|remaining| remaining <= Decimal::ZERO)
            }) || total_filled >= identity.size;
            let partial_fill = !order_fills.is_empty() && !full_fill;
            let state = match (full_fill, partial_fill, cancelled, cancel_race) {
                (_, _, true, true) => "fill_cancel_race",
                (true, _, _, _) => "fully_filled",
                (_, true, true, _) => "partially_filled_cancelled",
                (_, true, false, _) => "partially_filled_open_or_expired",
                (false, false, true, _) => "cancelled_unfilled",
                _ => "unfilled_open_or_expired",
            };
            let classification = if protocol_v3_bound && runtime_v3 {
                "protocol_v3_bound_diagnostic"
            } else {
                "diagnostic_ineligible"
            };
            let pre_send =
                decision.map(|decision| pre_send_json(decision, batch, identity, application));
            let start_evidence = valid_start_evidence.get(&identity.market_id);
            let settlement_evidence = valid_settlement_evidence.get(&identity.market_id);
            let mut row = json!({
                "schema": ORDER_FACT_SCHEMA,
                "schema_version": 1,
                "evidence_classification": classification,
                "diagnostic_only": true,
                "counts_toward_protocol_v3_evidence": false,
                "strategy_batch_id": application.parsed.key.batch_id,
                "strategy_batch_output_index": application.parsed.key.output_index,
                "strategy_decision_sha256": application.parsed.decision_sha256,
                "application_id": application.payload["application_id"],
                "application_event_sha256": application.parsed.event_sha256,
                "batch_event_sha256": batch.map(|event| event.event_sha256.clone()),
                "decision_event_sha256": decision.map(|event| event.event_sha256.clone()),
                "queue_registration_event_sha256": registration.map(|event| event.event_sha256.clone()),
                "queue_snapshot_event_sha256": snapshot.map(|event| event.event_sha256.clone()),
                "cancel_event_sha256": cancel.map(|event| event.event_sha256.clone()),
                "execution_report_event_sha256s": execution_reports.iter().map(|event| event.event_sha256.clone()).collect::<Vec<_>>(),
                "market_start_event_sha256": start_evidence.map(|evidence| evidence.event_sha256.clone()),
                "terminal_settlement_event_sha256": settlement_evidence.map(|evidence| evidence.event_sha256.clone()),
                "terminal_settlement_journal_id": settlement_evidence.and_then(|evidence| evidence.settlement_journal_id.clone()),
                "terminal_settlement_journal_event_sha256": settlement_evidence.and_then(|evidence| evidence.journal_event_sha256.clone()),
                "terminal_settlement_journal_sha256": settlement_evidence.and_then(|evidence| evidence.settlement_journal_sha256.clone()),
                "order_id": order_id,
                "market_id": identity.market_id,
                "token_id": identity.token_id,
                "side": identity.side,
                "quote_price": identity.price.to_string(),
                "order_size": identity.size.to_string(),
                "decision_recorded_ts": decision.map(|value| ts(value.recorded_ts)),
                "application_recorded_ts": ts(application.recorded_ts),
                "submitted_ts": ts(submitted_ts),
                "pre_send": pre_send,
                "execution_fields_complete": execution_complete,
                "queue_position_source": "paper_shadow_lifecycle_plus_public_l2",
                "queue_position": "inferred_size_ahead",
                "inferred_size_ahead": snapshot.and_then(|event| decimal(event.payload.get("visible_size_ahead_estimate"))).map(|value| value.to_string()),
                "queue_snapshot_ts": queue_snapshot_ts.map(ts),
                "fill_count": order_fills.len(),
                "fill_lifecycle_ids": order_fills.iter().map(|(key, _)| fill_lifecycle_id(key)).collect::<Vec<_>>(),
                "filled_size": total_filled.to_string(),
                "no_fill": order_fills.is_empty(),
                "partial_fill": partial_fill,
                "full_fill": full_fill,
                "first_fill_ts": first_fill_ts.map(ts),
                "time_to_first_fill_ms": first_fill_ts.map(|fill_ts| fill_ts.signed_duration_since(submitted_ts).num_milliseconds()),
                "strict_trade_through": order_fills.iter().any(|(_, event)| event.payload["strict_trade_through"].as_bool().unwrap_or(false)),
                "cancelled": cancelled,
                "cancel_state": state,
                "cancel_requested_ts": cancel_requested_ts.map(ts),
                "cancel_ack_ts": cancel_ack_ts.map(ts),
                "cancellation_age_ms": cancel.and_then(|event| event.payload["order_age_ms"].as_i64()),
                "cancellation_latency_ms": cancel.and_then(|event| decimal(event.payload.get("cancel_latency_ms"))).map(|value| value.to_string()),
                "fill_raced_cancellation": cancel_race
            });
            insert_fact_sha256(&mut row)?;
            order_rows.push(row);
        }
        order_rows
            .sort_by(|left, right| left["order_id"].as_str().cmp(&right["order_id"].as_str()));

        let mut fill_rows = Vec::new();
        let mut observed_markouts = BTreeMap::<i64, usize>::new();
        for (key, event) in &fills {
            if !valid_order_ids.contains(&key.order_id) {
                continue;
            }
            let lifecycle_id = fill_lifecycle_id(key);
            let fill_id = markout_fill_ids.get(key).cloned();
            let mut row = json!({
                "schema": FILL_FACT_SCHEMA,
                "schema_version": 1,
                "evidence_classification": "diagnostic_only",
                "counts_toward_protocol_v3_evidence": false,
                "fill_lifecycle_id": lifecycle_id,
                "fill_id": fill_id,
                "fill_event_sha256": event.event_sha256,
                "fill_source": key.source,
                "order_id": key.order_id,
                "market_id": key.market_id,
                "token_id": key.token_id,
                "side": key.side,
                "fill_price": key.fill_price.to_string(),
                "fill_size": key.fill_size.to_string(),
                "fee_per_share": key.fee_per_share.to_string(),
                "entry_fee": (key.fee_per_share * key.fill_size).to_string(),
                "fill_ts": ts(key.fill_ts),
                "partial_fill": event.payload["partial_fill"].as_bool().unwrap_or(false),
                "strict_trade_through": event.payload["strict_trade_through"].as_bool().unwrap_or(false)
            });
            let object = row.as_object_mut().expect("fill fact is an object");
            for horizon in MARKOUT_HORIZONS_SECONDS {
                let markout = markouts.get(&(key.clone(), horizon));
                let (status, per_share, pnl, observed_ts, reason) = match markout {
                    None => (
                        "missing",
                        None,
                        None,
                        None,
                        Some("no_markout_event".to_owned()),
                    ),
                    Some(markout)
                        if markout.envelope_chronology_is_valid()
                            && markout.observation.is_complete_and_timely() =>
                    {
                        *observed_markouts.entry(horizon).or_insert(0) += 1;
                        (
                            "observed",
                            markout.observation.net_executable_markout_per_share,
                            markout.observation.net_executable_markout_pnl,
                            markout.observation.observed_ts,
                            None,
                        )
                    }
                    Some(markout)
                        if markout.envelope_chronology_is_valid()
                            && markout.observation.missing =>
                    {
                        (
                            "missing",
                            None,
                            None,
                            None,
                            markout
                                .reason
                                .clone()
                                .or_else(|| Some("explicit_missing_event".to_owned())),
                        )
                    }
                    Some(markout) => (
                        "invalid",
                        None,
                        None,
                        markout.observation.observed_ts,
                        Some(
                            "fee, executable price, PnL, or timeliness validation failed"
                                .to_owned(),
                        ),
                    ),
                };
                if status == "invalid" {
                    invalid_markouts += 1;
                }
                object.insert(format!("markout_{horizon}s_status"), json!(status));
                object.insert(
                    format!("markout_{horizon}s_event_sha256"),
                    markout
                        .map(|value| Value::String(value.event_sha256.clone()))
                        .unwrap_or(Value::Null),
                );
                object.insert(
                    format!("net_executable_markout_{horizon}s_per_share"),
                    per_share
                        .map(|value| json!(value.to_string()))
                        .unwrap_or(Value::Null),
                );
                object.insert(
                    format!("net_executable_markout_{horizon}s_pnl"),
                    pnl.map(|value| json!(value.to_string()))
                        .unwrap_or(Value::Null),
                );
                object.insert(
                    format!("markout_{horizon}s_observed_ts"),
                    observed_ts
                        .map(ts)
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                );
                object.insert(
                    format!("markout_{horizon}s_missing_reason"),
                    reason.map(Value::String).unwrap_or(Value::Null),
                );
            }
            insert_fact_sha256(&mut row)?;
            fill_rows.push(row);
        }
        fill_rows.sort_by(|left, right| {
            left["fill_lifecycle_id"]
                .as_str()
                .cmp(&right["fill_lifecycle_id"].as_str())
        });

        let missing_start_market_ids = expected_markets
            .difference(&valid_start_evidence.keys().cloned().collect())
            .cloned()
            .collect::<BTreeSet<_>>();
        let missing_settlement_market_ids = expected_markets
            .difference(&valid_settlement_evidence.keys().cloned().collect())
            .cloned()
            .collect::<BTreeSet<_>>();
        let coverage = json!({
            "market_starts": coverage_row(valid_start_evidence.len(), expected_markets.len(), "authoritative validated-batch place markets with exact typed start evidence"),
            "market_settlements": coverage_row(valid_settlement_evidence.len(), expected_markets.len(), "authoritative validated-batch place markets with one locally validated terminal settlement"),
            "decisions": coverage_row(applied_decision_joins, expected_place_outputs, "authoritative validated-batch place outputs joined through durable decision and application"),
            "execution_fields": coverage_row(order_execution_complete, expected_place_outputs, "authoritative validated-batch place outputs with complete immutable decision and initial execution fields"),
            "queue_fields": coverage_row(order_queue_complete, expected_place_outputs, "authoritative validated-batch place outputs with one exact registration and inferred_size_ahead snapshot"),
            "markout_1s": coverage_row(observed_markouts.get(&1).copied().unwrap_or(0), fill_rows.len(), "fill lifecycles with one valid entry-fee-net executable 1-second markout"),
            "markout_5s": coverage_row(observed_markouts.get(&5).copied().unwrap_or(0), fill_rows.len(), "fill lifecycles with one valid entry-fee-net executable 5-second markout"),
            "markout_30s": coverage_row(observed_markouts.get(&30).copied().unwrap_or(0), fill_rows.len(), "fill lifecycles with one valid entry-fee-net executable 30-second markout")
        });
        let all_v3_bound = !order_rows.is_empty()
            && order_rows.iter().all(|row| {
                row["evidence_classification"].as_str() == Some("protocol_v3_bound_diagnostic")
            });
        let expected_outputs_complete = expected_place_outputs > 0
            && self.decisions.len() == expected_place_outputs
            && self.applications.len() == expected_place_outputs
            && order_rows.len() == expected_place_outputs
            && self.all_expected_place_outputs_bound();
        let no_local_quality_failures = self.invalid_v3_batches == 0
            && self.invalid_v3_decisions == 0
            && self.invalid_applications == 0
            && self.invalid_registrations + invalid_registration_rows == 0
            && self.invalid_snapshots + invalid_snapshot_rows == 0
            && self.invalid_fill_lifecycles + invalid_fill_lifecycles == 0
            && self.invalid_cancellations == 0
            && self.invalid_execution_reports == 0
            && invalid_markouts == 0
            && orphan_applications == 0
            && orphan_registrations == 0
            && orphan_snapshots == 0
            && orphan_fills == 0
            && orphan_execution_reports == 0
            && orphan_markouts == 0
            && self.journal_incomplete == 0
            && self.batch_retry_duplicates == 0
            && self.decision_retry_duplicates == 0
            && self.application_retry_duplicates == 0
            && self.journal_retry_duplicates == 0
            && self.duplicate_event_lines == 0;
        let audit_quality_complete = [
            "invalid_market_start_prices",
            "invalid_paper_settlements",
            "settlement_journal_conflicts",
            "settlement_journal_invalid",
            "settlement_journal_unbound_settlements",
            "strategy_batch_invalid",
            "strategy_batch_ineligible",
            "strategy_batch_conflicts",
            "strategy_binding_conflicts",
            "strategy_binding_ineligible",
            "unbound_strategy_decisions",
            "unbound_actionable_decision_outputs",
            "orphan_decision_applications",
            "decision_application_invalid",
            "decision_application_conflicts",
        ]
        .iter()
        .all(|field| audit[*field].as_u64() == Some(0));
        let complete_diagnostic = all_v3_bound
            && expected_outputs_complete
            && no_local_quality_failures
            && audit_quality_complete
            && coverage_is_complete(&coverage);
        let summary = json!({
            "schema": SUMMARY_SCHEMA,
            "schema_version": 1,
            "status": if complete_diagnostic { "complete_diagnostic" } else { "diagnostic_ineligible" },
            "diagnostic_only": true,
            "research_only": true,
            "promotion_eligible": false,
            "counts_toward_protocol_v3_evidence": false,
            "eligible_protocol_v3_identity": all_v3_bound,
            "queue_position_source": "paper_shadow_lifecycle_plus_public_l2",
            "queue_position_field": "inferred_size_ahead",
            "literal_fifo_rank_available": false,
            "duplicate_event_line_contract": "exact canonical event_type+recorded_ts+payload duplicates counted on raw normalized source events before probe exclusion and settlement-journal routing; completion requires zero",
            "snapshot_identity": snapshot,
            "runtime_provenance": runtime_provenance,
            "runtime_provenance_stable_identity": runtime_stable_identity,
            "runtime_provenance_sha256": runtime_hashes,
            "market_evidence": {
                "authoritative_expected_market_ids": expected_markets,
                "valid_start_evidence": market_evidence_json(&valid_start_evidence),
                "valid_terminal_settlement_evidence": market_evidence_json(&valid_settlement_evidence),
                "missing_start_market_ids": missing_start_market_ids,
                "missing_terminal_settlement_market_ids": missing_settlement_market_ids
            },
            "coverage": coverage,
            "completion_checks": {
                "all_order_rows_v3_bound": all_v3_bound,
                "expected_place_outputs_complete": expected_outputs_complete,
                "no_local_quality_failures": no_local_quality_failures,
                "audit_quality_complete": audit_quality_complete,
                "all_coverage_complete": coverage_is_complete(&coverage),
                "runtime_provenance_stable_v3": runtime_v3,
                "no_exact_duplicate_event_lines": self.duplicate_event_lines == 0
            },
            "counts": {
                "order_lifecycle_rows": order_rows.len(),
                "fill_markout_rows": fill_rows.len(),
                "valid_v3_batches": self.batches.len(),
                "expected_v3_place_outputs": expected_place_outputs,
                "valid_v3_place_decisions": self.decisions.len(),
                "claimed_v3_decision_events": self.claimed_v3_decision_events,
                "valid_applications": self.applications.len(),
                "invalid_v3_batches": self.invalid_v3_batches,
                "invalid_v3_decisions": self.invalid_v3_decisions,
                "invalid_applications": self.invalid_applications,
                "invalid_registrations": self.invalid_registrations + invalid_registration_rows,
                "invalid_snapshots": self.invalid_snapshots + invalid_snapshot_rows,
                "invalid_fill_lifecycles": self.invalid_fill_lifecycles + invalid_fill_lifecycles,
                "invalid_cancellations": self.invalid_cancellations,
                "invalid_execution_reports": self.invalid_execution_reports,
                "invalid_markouts": invalid_markouts,
                "orphan_applications": orphan_applications,
                "orphan_registrations": orphan_registrations,
                "orphan_snapshots": orphan_snapshots,
                "orphan_fill_lifecycles": orphan_fills,
                "orphan_execution_reports": orphan_execution_reports,
                "orphan_markout_rows": orphan_markouts,
                "duplicate_batch_retries_deduplicated": self.batch_retry_duplicates,
                "duplicate_decision_retries_deduplicated": self.decision_retry_duplicates,
                "duplicate_application_retries_deduplicated": self.application_retry_duplicates,
                "duplicate_journal_retries_deduplicated": self.journal_retry_duplicates,
                "duplicate_event_lines": self.duplicate_event_lines,
                "incomplete_settlement_journals": self.journal_incomplete,
                "probe_events_excluded": self.probe_events_excluded
            },
            "audit_context": {
                "markets_seen": audit["markets_seen"],
                "markets_with_start_price": audit["markets_with_start_price"],
                "markets_settled": audit["markets_settled"],
                "invalid_market_start_prices": audit["invalid_market_start_prices"],
                "invalid_paper_settlements": audit["invalid_paper_settlements"],
                "settlement_journal_conflicts": audit["settlement_journal_conflicts"],
                "settlement_journal_invalid": audit["settlement_journal_invalid"],
                "settlement_journal_unbound_settlements": audit["settlement_journal_unbound_settlements"],
                "strategy_batch_invalid": audit["strategy_batch_invalid"],
                "strategy_batch_ineligible": audit["strategy_batch_ineligible"],
                "strategy_batch_conflicts": audit["strategy_batch_conflicts"],
                "strategy_binding_conflicts": audit["strategy_binding_conflicts"],
                "strategy_binding_ineligible": audit["strategy_binding_ineligible"],
                "unbound_strategy_decisions": audit["unbound_strategy_decisions"],
                "unbound_actionable_decision_outputs": audit["unbound_actionable_decision_outputs"],
                "orphan_decision_applications": audit["orphan_decision_applications"],
                "decision_application_invalid": audit["decision_application_invalid"],
                "decision_application_conflicts": audit["decision_application_conflicts"]
            },
            "fact_contracts": {
                "order_lifecycle_fact": {"schema": ORDER_FACT_SCHEMA, "rows": order_rows.len(), "identity": "exactly one row per unique applied order_id"},
                "fill_markout_fact": {"schema": FILL_FACT_SCHEMA, "rows": fill_rows.len(), "identity": "exactly one row per complete fill lifecycle key; 1/5/30-second markouts are columns, never rows"}
            }
        });
        Ok(DiagnosticsFacts {
            order_rows,
            fill_rows,
            summary,
        })
    }

    fn reject_duplicate_lifecycle_inputs(&mut self) {
        for (order_id, rows) in &self.registrations {
            if rows.len() > 1 {
                self.fatal_errors.insert(format!(
                    "duplicate order lifecycle identity: {order_id} has {} queue registrations",
                    rows.len()
                ));
            }
        }
        for (order_id, rows) in &self.snapshots {
            if rows.len() > 1 {
                self.fatal_errors.insert(format!(
                    "ambiguous many-to-one queue join: {order_id} has {} initial snapshots",
                    rows.len()
                ));
            }
        }
        for (order_id, rows) in &self.cancel_events {
            if rows.len() > 1 {
                self.fatal_errors.insert(format!(
                    "duplicate cancel lifecycle identity: {order_id} has {} cancel events",
                    rows.len()
                ));
            }
        }
    }

    fn unique_registrations(&self) -> (BTreeMap<String, ObservedEvent>, usize) {
        let mut invalid = 0_usize;
        let rows = self
            .registrations
            .iter()
            .filter_map(|(order_id, rows)| {
                let event = rows.first()?;
                if QueueRegistrationIdentity::from_payload(&event.payload).is_none() {
                    invalid += 1;
                    return None;
                }
                Some((order_id.clone(), event.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        (rows, invalid)
    }

    fn unique_snapshots(&self) -> (BTreeMap<String, ObservedEvent>, usize) {
        let mut invalid = 0_usize;
        let rows = self
            .snapshots
            .iter()
            .filter_map(|(order_id, rows)| {
                let event = rows.first()?;
                if decimal(event.payload.get("visible_size_ahead_estimate")).is_none()
                    || QueueRegistrationIdentity::from_payload(&event.payload).is_none()
                {
                    invalid += 1;
                    return None;
                }
                Some((order_id.clone(), event.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        (rows, invalid)
    }

    fn parse_unique_fills(
        &self,
        registrations: &BTreeMap<String, ObservedEvent>,
    ) -> Result<(BTreeMap<FillLifecycleJoinKey, ObservedEvent>, usize), ResearchError> {
        let identities = registrations
            .iter()
            .filter_map(|(order_id, event)| {
                QueueRegistrationIdentity::from_payload(&event.payload)
                    .map(|identity| (order_id.clone(), identity))
            })
            .collect::<BTreeMap<_, _>>();
        let mut fills = BTreeMap::new();
        let mut invalid = 0_usize;
        for event in &self.fill_events {
            let source = if event.event_type == "paper_queue_shadow_fill" {
                "queue_shadow_fill"
            } else {
                "touch_fill"
            };
            let Some(key) = parse_fill_key(source, &event.payload, &identities) else {
                invalid += 1;
                continue;
            };
            if identities
                .get(&key.order_id)
                .is_some_and(|identity| !identity.matches_lifecycle(&key))
            {
                return Err(lifecycle_error(
                    &key.order_id,
                    "fill conflicts with queue registration identity",
                ));
            }
            if fills.insert(key.clone(), event.clone()).is_some() {
                return Err(ResearchError::InvalidInput(format!(
                    "loss diagnostics failed closed: duplicate fill lifecycle identity {}",
                    fill_lifecycle_id(&key)
                )));
            }
        }
        Ok((fills, invalid))
    }

    fn parse_unique_markouts(
        &self,
        expected: &BTreeSet<FillLifecycleJoinKey>,
    ) -> Result<ParsedMarkouts, ResearchError> {
        let mut rows = BTreeMap::new();
        let mut fill_ids_by_key = BTreeMap::<FillLifecycleJoinKey, String>::new();
        let mut keys_by_fill_id = BTreeMap::<String, FillLifecycleJoinKey>::new();
        let mut orphan = 0_usize;
        let mut invalid = 0_usize;
        for event in &self.markout_events {
            let missing = event.event_type == "paper_fill_markout_missing";
            let Some(mut parsed) = parse_markout(&event.payload, missing, event.recorded_ts) else {
                invalid += 1;
                continue;
            };
            parsed.event_sha256 = event.event_sha256.clone();
            let key = parsed.observation.key.clone();
            if !expected.contains(&key) {
                orphan += 1;
                continue;
            }
            let fill_id = parsed.observation.fill_id.clone();
            if let Some(existing) = fill_ids_by_key.get(&key) {
                if existing != &fill_id {
                    return Err(ResearchError::InvalidInput(format!(
                        "loss diagnostics failed closed: one fill lifecycle joins multiple fill IDs ({existing}, {fill_id})"
                    )));
                }
            } else {
                fill_ids_by_key.insert(key.clone(), fill_id.clone());
            }
            if let Some(existing) = keys_by_fill_id.get(&fill_id) {
                if existing != &key {
                    return Err(ResearchError::InvalidInput(format!(
                        "loss diagnostics failed closed: fill ID {fill_id} joins multiple fill lifecycles"
                    )));
                }
            } else {
                keys_by_fill_id.insert(fill_id, key.clone());
            }
            let slot = (key, parsed.observation.horizon);
            if rows.insert(slot.clone(), parsed).is_some() {
                return Err(ResearchError::InvalidInput(format!(
                    "loss diagnostics failed closed: duplicate markout lifecycle/horizon slot {}s",
                    slot.1
                )));
            }
        }
        Ok(ParsedMarkouts {
            rows,
            fill_ids_by_key,
            orphan,
            invalid,
        })
    }

    fn validated_market_evidence(
        &self,
        expected_markets: &BTreeSet<String>,
    ) -> Result<ValidatedMarketEvidence, ResearchError> {
        let mut starts = BTreeMap::new();
        let mut settlements = BTreeMap::new();
        for market_id in expected_markets {
            let batches = self
                .batches
                .values()
                .filter(|batch| {
                    !batch.place_output_hashes.is_empty()
                        && batch.input.market.market_id.to_string() == *market_id
                })
                .collect::<Vec<_>>();
            let valid_starts = self
                .start_evidence_events
                .get(market_id)
                .into_iter()
                .flatten()
                .filter(|event| {
                    market_start_evidence_from_event(&event.payload).is_some_and(|evidence| {
                        evidence.start_price > Decimal::ZERO
                            && event.recorded_ts >= evidence.market_start_ts
                            && event.recorded_ts >= evidence.reference_source_ts
                            && batches.iter().all(|batch| {
                                batch.input.market_start_evidence == evidence
                                    && event.recorded_ts <= batch.input.decision_ts
                                    && event.recorded_ts <= batch.recorded_ts
                            })
                    })
                })
                .collect::<Vec<_>>();
            if valid_starts.len() > 1 {
                return Err(ResearchError::InvalidInput(format!(
                    "loss diagnostics failed closed: market {market_id} has ambiguous exact-start evidence"
                )));
            }
            if let Some(event) = valid_starts.first() {
                starts.insert(market_id.clone(), MarketEvidenceRef::from_event(event));
            }

            let valid_settlements = self
                .settlement_evidence_events
                .get(market_id)
                .into_iter()
                .flatten()
                .filter(|event| {
                    batches
                        .iter()
                        .all(|batch| settlement_matches_validated_batch(event, batch))
                })
                .collect::<Vec<_>>();
            if valid_settlements.len() > 1 {
                return Err(ResearchError::InvalidInput(format!(
                    "loss diagnostics failed closed: market {market_id} has ambiguous terminal-settlement evidence"
                )));
            }
            if let Some(event) = valid_settlements.first() {
                settlements.insert(market_id.clone(), MarketEvidenceRef::from_event(event));
            }
        }
        Ok(ValidatedMarketEvidence {
            starts,
            settlements,
        })
    }

    fn runtime_provenance_is_stable_v3(&self, stable_identity: &Value) -> bool {
        if self.runtime_provenance.is_empty()
            || stable_identity["distinct_identity_count"].as_u64() != Some(1)
            || !self.runtime_provenance.iter().all(|(_, payload)| {
                run_bundle::shadow_runtime_provenance_errors(payload).is_empty()
            })
        {
            return false;
        }
        let config_hashes = self
            .runtime_provenance
            .iter()
            .filter_map(|(_, payload)| optional_text(payload, "decision_config_sha256"))
            .collect::<BTreeSet<_>>();
        let Some(config_hash) = (config_hashes.len() == 1)
            .then(|| config_hashes.iter().next())
            .flatten()
        else {
            return false;
        };
        !self.batches.is_empty()
            && self
                .batches
                .values()
                .all(|batch| &batch.decision_config_sha256 == config_hash)
    }

    fn all_expected_place_outputs_bound(&self) -> bool {
        self.batches.iter().all(|(batch_id, batch)| {
            batch
                .place_output_hashes
                .iter()
                .all(|(output_index, hash)| {
                    let key = DecisionOutputKeyV3 {
                        batch_id: batch_id.clone(),
                        output_index: *output_index,
                    };
                    self.decisions
                        .get(&key)
                        .is_some_and(|decision| &decision.parsed.decision_sha256 == hash)
                        && self.applications.get(&key).is_some_and(|application| {
                            application.parsed.order_id.is_some()
                                && self.decisions.get(&key).is_some_and(|decision| {
                                    application_matches_decision(
                                        &application.parsed,
                                        &decision.parsed,
                                    )
                                })
                        })
                })
        })
    }

    fn matching_batch(&self, decision: &DecisionRecord) -> Option<&BatchRecord> {
        self.batches
            .get(&decision.parsed.key.batch_id)
            .filter(|batch| {
                batch
                    .output_hashes
                    .get(&decision.parsed.key.output_index)
                    .is_some_and(|hash| hash == &decision.parsed.decision_sha256)
            })
    }

    fn validate_pre_send_chronology(
        &self,
        decision: &DecisionRecord,
        application: &ApplicationRecord,
        batch: &BatchRecord,
    ) -> Result<(), ResearchError> {
        let input = &batch.input;
        let feature_timestamps_valid = input.fair_value.computed_ts <= input.decision_ts
            && input.reference.source_ts <= input.decision_ts
            && input.reference.local_ts <= input.decision_ts
            && input
                .books
                .values()
                .all(|book| book.local_ts <= input.decision_ts)
            && input
                .regime_feature_input
                .reference_history
                .iter()
                .all(|point| point.ts <= input.decision_ts);
        if !feature_timestamps_valid
            || input.decision_ts > batch.recorded_ts
            || batch.recorded_ts > decision.recorded_ts
            || decision.recorded_ts > application.recorded_ts
        {
            return Err(ResearchError::InvalidInput(format!(
                "loss diagnostics failed closed: post-decision feature or invalid chronology for {}/{}",
                decision.parsed.key.batch_id, decision.parsed.key.output_index
            )));
        }
        Ok(())
    }
}

pub fn run_loss_diagnostics(options: LossDiagnosticsOptions) -> Result<Value, ResearchError> {
    let started = Instant::now();
    if !options.input.is_dir() {
        return Err(ResearchError::InvalidInput(
            "loss diagnostics requires an explicitly supplied local normalized snapshot directory"
                .to_owned(),
        ));
    }
    if options.out.exists() {
        return Err(ResearchError::InvalidInput(format!(
            "{} already exists; loss diagnostics never overwrites an output directory",
            options.out.display()
        )));
    }
    if options.out.starts_with(&options.input) {
        return Err(ResearchError::InvalidInput(
            "loss diagnostics output must be outside the immutable input snapshot".to_owned(),
        ));
    }
    let inventory_before =
        build_local_source_inventory(&options.input, EventPathMode::PreferEventsJsonl)?;
    let snapshot_manifest = validate_snapshot_manifest(&options.input, &inventory_before)?;
    let mut diagnostics = LossDiagnosticsAccumulator::default();
    let mut audit = AuditAccumulator::default();
    let stream = stream_events(
        &options.input,
        EventPathMode::PreferEventsJsonl,
        &[],
        |event| {
            audit.observe(event);
            diagnostics.observe(event);
        },
    )?;
    let audit = audit.finish();
    let inventory_after =
        build_local_source_inventory(&options.input, EventPathMode::PreferEventsJsonl)?;
    if inventory_before != inventory_after {
        return Err(ResearchError::InvalidInput(
            "immutable normalized snapshot changed while loss diagnostics was reading it"
                .to_owned(),
        ));
    }
    if stream.malformed_lines > 0 || stream.out_of_order_timestamps > 0 {
        return Err(ResearchError::InvalidInput(format!(
            "loss diagnostics refuses malformed or out-of-order input: {} malformed, {} out of order",
            stream.malformed_lines, stream.out_of_order_timestamps
        )));
    }
    let snapshot = json!({
        "input": options.input.to_string_lossy(),
        "manifest": snapshot_manifest,
        "source_inventory_schema_version": inventory_before.schema_version,
        "source_inventory_canonical_sha256": inventory_before.canonical_sha256,
        "source_inventory": inventory_before,
        "stable_before_after_read": true,
        "events_scanned": stream.events,
        "duplicate_line_estimate": stream.duplicate_estimate,
        "duplicate_line_estimate_authority": "shared_capped_stream_informational_only",
        "stream_notices": stream.notices
    });
    let facts = diagnostics.finish(&audit, &snapshot)?;
    let mut staging = StagingDirectory::create(&options.out)?;

    let final_order_path = options.out.join(ORDER_FACT_FILE);
    let final_fill_path = options.out.join(FILL_FACT_FILE);
    let final_summary_path = options.out.join(SUMMARY_FILE);
    let final_markdown_path = options.out.join(MARKDOWN_FILE);
    let final_manifest_path = options.out.join(ARTIFACT_MANIFEST_FILE);
    let order_path = staging.path.join(ORDER_FACT_FILE);
    let fill_path = staging.path.join(FILL_FACT_FILE);
    let summary_path = staging.path.join(SUMMARY_FILE);
    let markdown_path = staging.path.join(MARKDOWN_FILE);
    let manifest_path = staging.path.join(ARTIFACT_MANIFEST_FILE);
    write_jsonl(&order_path, &facts.order_rows)?;
    write_jsonl(&fill_path, &facts.fill_rows)?;
    let result = merge_summary_paths(
        facts.summary,
        &final_order_path,
        &final_fill_path,
        &final_summary_path,
        &final_markdown_path,
        &final_manifest_path,
    );
    let warnings = if result["status"].as_str() == Some("diagnostic_ineligible") {
        vec![json!("one or more rows lack complete validated Protocol-v3 batch/provenance binding; output remains diagnostic-only")]
    } else {
        Vec::new()
    };
    let report = envelope(
        "polyedge-rs research loss-diagnostics",
        &options.input,
        "immutable_protocol_v3_snapshot",
        "diagnostic_only",
        started.elapsed(),
        warnings,
        result,
    );
    write_json_synced(&summary_path, &report)?;
    write_bytes_synced(
        &markdown_path,
        loss_diagnostics_markdown(&report).as_bytes(),
    )?;
    let artifact_manifest = build_artifact_manifest(
        &order_path,
        facts.order_rows.len(),
        &fill_path,
        facts.fill_rows.len(),
        &summary_path,
        &markdown_path,
    )?;
    write_json_synced(&manifest_path, &artifact_manifest)?;
    sync_directory(&staging.path)?;
    staging.publish_to(&options.out)?;
    if let Some(parent) = options
        .out
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let _ = sync_directory(parent);
    }
    Ok(report)
}

fn validate_snapshot_manifest(
    input: &Path,
    normalized_inventory: &RawSourceInventory,
) -> Result<Value, ResearchError> {
    let campaign = input.join(projected_cache::PROJECTED_CAMPAIGN_INDEX_FILE);
    if campaign.is_file() {
        projected_cache::load_campaign_index(input)?;
        let bytes = fs::read(&campaign)?;
        return Ok(json!({
            "kind": "verified_projected_campaign_index",
            "path": campaign.to_string_lossy(),
            "sha256": sha256_prefixed(&bytes)
        }));
    }
    let path = input.join("events_manifest.json");
    if !path.is_file() {
        return Err(ResearchError::InvalidInput(
            "loss diagnostics requires events_manifest.json or a verified campaign_index.json"
                .to_owned(),
        ));
    }
    let bytes = fs::read(&path)?;
    let manifest: Value = serde_json::from_slice(&bytes)?;
    if manifest["decision_grade_projection"].as_bool() != Some(true)
        || manifest["sealed"].as_bool() != Some(true)
        || !manifest["format"]
            .as_str()
            .is_some_and(|format| format.starts_with("jsonl-indexed"))
        || manifest["normalized_source_inventory_sha256"].as_str()
            != Some(normalized_inventory.canonical_sha256.as_str())
    {
        return Err(ResearchError::InvalidInput(
            "loss diagnostics requires a sealed decision-grade normalized snapshot manifest bound to the exact normalized source inventory".to_owned(),
        ));
    }
    let raw_inventory = serde_json::from_value::<RawSourceInventory>(
        manifest
            .get("raw_source_inventory")
            .cloned()
            .ok_or_else(|| {
                ResearchError::InvalidInput(
                    "sealed loss-diagnostics snapshot is missing raw_source_inventory".to_owned(),
                )
            })?,
    )?;
    validate_raw_source_inventory(&raw_inventory)?;
    Ok(json!({
        "kind": "normalized_events_manifest",
        "path": path.to_string_lossy(),
        "sha256": sha256_prefixed(&bytes),
        "raw_source_inventory_sha256": raw_inventory.canonical_sha256,
        "normalized_source_inventory_sha256": normalized_inventory.canonical_sha256
    }))
}

fn has_settlement_journal_fields(payload: &Value) -> bool {
    settlement_journal_fields()
        .iter()
        .any(|field| payload.get(*field).is_some())
}

fn settlement_journal_fields() -> [&'static str; 5] {
    [
        "settlement_journal_schema",
        "settlement_journal_id",
        "settlement_journal_event_index",
        "settlement_journal_event_count",
        "settlement_journal_sha256",
    ]
}

fn unique_single_events(
    rows: &BTreeMap<String, Vec<ObservedEvent>>,
    label: &str,
) -> Result<BTreeMap<String, ObservedEvent>, ResearchError> {
    rows.iter()
        .map(|(id, events)| {
            if events.len() != 1 {
                return Err(ResearchError::InvalidInput(format!(
                    "loss diagnostics failed closed: {label} {id} has {} rows",
                    events.len()
                )));
            }
            Ok((id.clone(), events[0].clone()))
        })
        .collect()
}

fn lifecycle_error(order_id: &str, reason: &str) -> ResearchError {
    ResearchError::InvalidInput(format!(
        "loss diagnostics failed closed: order {order_id} {reason}"
    ))
}

fn paper_execution_status_and_size_are_valid(
    payload: &Value,
    identity: &PlaceOutputIdentityV3,
) -> bool {
    let Some(status) = payload.get("status").and_then(Value::as_str) else {
        return false;
    };
    let Some(filled_size) = decimal(payload.get("filled_size")) else {
        return false;
    };
    let Some(fee) = decimal(payload.get("fee")) else {
        return false;
    };
    if filled_size < Decimal::ZERO || filled_size > identity.size || fee < Decimal::ZERO {
        return false;
    }
    let avg_price = decimal(payload.get("avg_price"));
    match status {
        "paper_resting" | "paper_cancelled" => {
            filled_size == Decimal::ZERO && avg_price.is_none() && fee == Decimal::ZERO
        }
        "paper_filled" => {
            let expected_fee = crypto_taker_fee_per_share(identity.price)
                .ok()
                .map(|fee_per_share| fee_per_share * identity.size);
            filled_size == identity.size
                && avg_price == Some(identity.price)
                && expected_fee == Some(fee)
        }
        "paper_filled_maker" => {
            filled_size == identity.size
                && avg_price == Some(identity.price)
                && fee == Decimal::ZERO
        }
        _ => false,
    }
}

fn application_execution_report_is_valid(
    report: &Value,
    identity: &PlaceOutputIdentityV3,
    order_id: &str,
    lower_bound: Option<DateTime<Utc>>,
    application_recorded_ts: DateTime<Utc>,
) -> bool {
    let Some(local_ts) = parse_datetime(report.get("local_ts")) else {
        return false;
    };
    optional_text(report, "order_id").as_deref() == Some(order_id)
        && optional_text(report, "market_id").as_deref() == Some(identity.market_id.as_str())
        && optional_text(report, "token_id").as_deref() == Some(identity.token_id.as_str())
        && paper_execution_status_and_size_are_valid(report, identity)
        && lower_bound.is_none_or(|lower_bound| local_ts >= lower_bound)
        && local_ts <= application_recorded_ts
}

fn execution_report_matches_place_identity_and_chronology(
    report: &ObservedEvent,
    identity: &PlaceOutputIdentityV3,
    submitted_ts: DateTime<Utc>,
) -> bool {
    let payload = &report.payload;
    let Some(local_ts) = parse_datetime(payload.get("local_ts")) else {
        return false;
    };
    let raw_decision = payload.pointer("/raw/decision");
    let raw_price = raw_decision.and_then(|decision| decimal(decision.get("price")));
    let raw_size = raw_decision.and_then(|decision| decimal(decision.get("size")));
    let raw_side = raw_decision
        .and_then(|decision| optional_text(decision, "side"))
        .map(|side| side.to_ascii_lowercase());
    optional_text(payload, "market_id").as_deref() == Some(identity.market_id.as_str())
        && optional_text(payload, "token_id").as_deref() == Some(identity.token_id.as_str())
        && raw_decision
            .and_then(|decision| optional_text(decision, "market_id"))
            .as_deref()
            == Some(identity.market_id.as_str())
        && raw_decision
            .and_then(|decision| optional_text(decision, "token_id"))
            .as_deref()
            == Some(identity.token_id.as_str())
        && raw_side.as_deref() == Some(identity.side.as_str())
        && raw_price == Some(identity.price)
        && raw_size == Some(identity.size)
        && paper_execution_status_and_size_are_valid(payload, identity)
        && local_ts >= submitted_ts
        && report.recorded_ts >= local_ts
}

fn settlement_matches_validated_batch(event: &ObservedEvent, batch: &BatchRecord) -> bool {
    let payload = &event.payload;
    let market = &batch.input.market;
    let start = &batch.input.market_start_evidence;
    let Some(start_ts) = parse_datetime(payload.get("start_ts")) else {
        return false;
    };
    let Some(end_ts) = parse_datetime(payload.get("end_ts")) else {
        return false;
    };
    let Some(start_price) = decimal(payload.get("start_price")) else {
        return false;
    };
    let Some(final_price) = decimal(payload.get("final_price")) else {
        return false;
    };
    let Some(start_reference_ts) = parse_datetime(payload.get("start_reference_source_ts")) else {
        return false;
    };
    let Some(final_reference_ts) = parse_datetime(payload.get("final_reference_source_ts")) else {
        return false;
    };
    let expected_outcome = if final_price >= start_price {
        "up"
    } else {
        "down"
    };
    let market_id = market.market_id.to_string();
    optional_text(payload, "market_id").as_deref() == Some(market_id.as_str())
        && start_ts == market.start_ts
        && end_ts == market.end_ts
        && start_price == start.start_price
        && start_price > Decimal::ZERO
        && final_price > Decimal::ZERO
        && optional_text(payload, "start_reference_source").as_deref()
            == Some(start.reference_source.as_str())
        && start_reference_ts == start.reference_source_ts
        && payload["start_reference_exact_resolution_source"].as_bool() == Some(true)
        && payload["start_reference_stale"].as_bool() == Some(false)
        && (0..=START_PRICE_CAPTURE_WINDOW_SECONDS * 1_000).contains(
            &start_reference_ts
                .signed_duration_since(start_ts)
                .num_milliseconds(),
        )
        && optional_text(payload, "final_reference_source").is_some_and(|source| !source.is_empty())
        && payload["final_reference_exact_resolution_source"].as_bool() == Some(true)
        && payload["final_reference_stale"].as_bool() == Some(false)
        && (0..=SETTLEMENT_WINDOW_SECONDS * 1_000).contains(
            &final_reference_ts
                .signed_duration_since(end_ts)
                .num_milliseconds(),
        )
        && event.recorded_ts >= end_ts
        && event.recorded_ts >= final_reference_ts
        && optional_text(payload, "winning_outcome").as_deref() == Some(expected_outcome)
}

fn insert_fact_sha256(row: &mut Value) -> Result<(), ResearchError> {
    let hash = canonical_value_sha256(row).ok_or_else(|| {
        ResearchError::InvalidInput(
            "loss diagnostics fact could not be canonically hashed".to_owned(),
        )
    })?;
    row.as_object_mut()
        .ok_or_else(|| {
            ResearchError::InvalidInput("loss diagnostics fact is not an object".to_owned())
        })?
        .insert("fact_sha256".to_owned(), Value::String(hash));
    Ok(())
}

fn payload_matches_place_identity(payload: &Value, identity: &PlaceOutputIdentityV3) -> bool {
    if optional_text(payload, "market_id").as_deref() != Some(identity.market_id.as_str())
        || optional_text(payload, "token_id").as_deref() != Some(identity.token_id.as_str())
    {
        return false;
    }
    if let Some(side) = optional_text(payload, "side") {
        if !side.eq_ignore_ascii_case(&identity.side) {
            return false;
        }
    }
    if let Some(price) = decimal(payload.get("quote_price").or_else(|| payload.get("price"))) {
        if price != identity.price {
            return false;
        }
    }
    if let Some(size) = decimal(payload.get("order_size").or_else(|| payload.get("size"))) {
        if size != identity.size {
            return false;
        }
    }
    true
}

fn coverage_is_complete(coverage: &Value) -> bool {
    coverage.as_object().is_some_and(|rows| {
        !rows.is_empty()
            && rows.values().all(|row| {
                row["observed"].as_u64().is_some()
                    && row["observed"].as_u64() == row["denominator"].as_u64()
            })
    })
}

fn market_evidence_json(evidence: &BTreeMap<String, MarketEvidenceRef>) -> Value {
    Value::Object(
        evidence
            .iter()
            .map(|(market_id, evidence)| {
                (
                    market_id.clone(),
                    json!({
                        "source_event_sha256": evidence.event_sha256,
                        "journal_event_sha256": evidence.journal_event_sha256,
                        "settlement_journal_id": evidence.settlement_journal_id,
                        "settlement_journal_sha256": evidence.settlement_journal_sha256
                    }),
                )
            })
            .collect(),
    )
}

fn summarize_stable_runtime_identity(observations: &[(DateTime<Utc>, Value)]) -> Value {
    let identities = observations
        .iter()
        .filter_map(|(_, payload)| {
            let mut normalized = payload.clone();
            normalized
                .get_mut("event_blob_prefix_routing")
                .and_then(Value::as_object_mut)
                .map(|routing| routing.remove("evaluated_event_ts"));
            canonical_value_sha256(&normalized)
        })
        .collect::<BTreeSet<_>>();
    json!({
        "schema_version": 1,
        "normalization": "event_blob_prefix_routing.evaluated_event_ts excluded after raw provenance validation",
        "observations": observations.len(),
        "distinct_identity_count": identities.len(),
        "identity_sha256s": identities
    })
}

fn parse_fill_key(
    source: &str,
    payload: &Value,
    registrations: &BTreeMap<String, QueueRegistrationIdentity>,
) -> Option<FillLifecycleJoinKey> {
    let order_id = optional_text(payload, "order_id")?;
    let side = optional_text(payload, "side")
        .or_else(|| {
            payload
                .pointer("/raw/decision/side")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| registrations.get(&order_id).map(|row| row.side.clone()))?
        .to_ascii_lowercase();
    let (price_field, timestamp_field, size_field, fee_per_share) = if source == "queue_shadow_fill"
    {
        ("quote_price", "trade_ts", "shadow_fill_size", Decimal::ZERO)
    } else {
        let size = decimal(payload.get("filled_size"))?;
        let fee = decimal(payload.get("fee"))?;
        if size <= Decimal::ZERO || fee < Decimal::ZERO {
            return None;
        }
        ("avg_price", "local_ts", "filled_size", fee / size)
    };
    let key = FillLifecycleJoinKey {
        source: source.to_owned(),
        order_id,
        market_id: optional_text(payload, "market_id")?,
        token_id: optional_text(payload, "token_id")?,
        side,
        fill_price: decimal(payload.get(price_field))?,
        fill_ts: parse_datetime(payload.get(timestamp_field))?,
        fill_size: decimal(payload.get(size_field))?,
        fee_per_share,
    };
    (key.fill_size > Decimal::ZERO && key.fee_per_share >= Decimal::ZERO).then_some(key)
}

fn parse_markout(
    payload: &Value,
    missing: bool,
    recorded_ts: DateTime<Utc>,
) -> Option<ParsedMarkout> {
    let horizon = payload["horizon_seconds"]
        .as_i64()
        .filter(|value| MARKOUT_HORIZONS_SECONDS.contains(value))?;
    let observation = MarkoutObservation {
        key: FillLifecycleJoinKey {
            source: optional_text(payload, "fill_source")?,
            order_id: optional_text(payload, "order_id")?,
            market_id: optional_text(payload, "market_id")?,
            token_id: optional_text(payload, "token_id")?,
            side: optional_text(payload, "side")?.to_ascii_lowercase(),
            fill_price: decimal(payload.get("fill_price"))?,
            fill_ts: parse_datetime(payload.get("fill_ts"))?,
            fill_size: decimal(payload.get("fill_size"))?,
            fee_per_share: decimal(payload.get("fee_per_share"))?,
        },
        fill_id: optional_text(payload, "fill_id")?,
        horizon,
        missing,
        gross_markout_per_share: decimal(payload.get("markout_per_share")),
        gross_executable_markout_per_share: decimal(payload.get("executable_markout_per_share")),
        fee_per_share: decimal(payload.get("fee_per_share")),
        net_markout_per_share: decimal(payload.get("net_markout_per_share")),
        net_executable_markout_per_share: decimal(payload.get("net_executable_markout_per_share")),
        net_markout_pnl: decimal(payload.get("net_markout_pnl")),
        net_executable_markout_pnl: decimal(payload.get("net_executable_markout_pnl")),
        observation_delay_ms: payload["observation_delay_ms"].as_i64(),
        observed_ts: parse_datetime(payload.get("observed_ts")),
    };
    (observation.key.fill_size > Decimal::ZERO && observation.key.fee_per_share >= Decimal::ZERO)
        .then(|| ParsedMarkout {
            observation,
            reason: optional_text(payload, "reason"),
            event_sha256: String::new(),
            recorded_ts,
        })
}

fn fill_lifecycle_id(key: &FillLifecycleJoinKey) -> String {
    canonical_value_sha256(&json!({
        "schema": "polyedge.loss_diagnostics.fill_lifecycle_identity.v1",
        "source": key.source,
        "order_id": key.order_id,
        "market_id": key.market_id,
        "token_id": key.token_id,
        "side": key.side,
        "fill_price": key.fill_price.to_string(),
        "fill_ts": ts(key.fill_ts),
        "fill_size": key.fill_size.to_string(),
        "fee_per_share": key.fee_per_share.to_string()
    }))
    .expect("fill lifecycle identity is serializable")
}

fn pre_send_json(
    decision: &DecisionRecord,
    batch: Option<&BatchRecord>,
    identity: &PlaceOutputIdentityV3,
    application: &ApplicationRecord,
) -> Value {
    let batch_fields = batch.map(|batch| {
        let input = &batch.input;
        let features = input.regime_feature_input.clone().build();
        let book = input
            .books
            .iter()
            .find(|(token, _)| token.to_string() == identity.token_id)
            .map(|(_, book)| book);
        json!({
            "decision_ts": ts(input.decision_ts),
            "decision_config_sha256": batch.decision_config_sha256,
            "market_start_evidence_sha256": batch.market_start_evidence_sha256,
            "pipeline_input_sha256": canonical_value_sha256(&serde_json::to_value(input).unwrap_or(Value::Null)),
            "q_up": input.fair_value.q_up.to_string(),
            "q_down": input.fair_value.q_down.to_string(),
            "sigma": input.fair_value.sigma,
            "model_error": input.fair_value.model_error.to_string(),
            "reference_price": input.reference.price.to_string(),
            "reference_source_ts": ts(input.reference.source_ts),
            "reference_local_ts": ts(input.reference.local_ts),
            "reference_stale": input.reference.stale,
            "best_bid": book.and_then(|book| book.best_bid()).map(|level| level.price.to_string()),
            "best_ask": book.and_then(|book| book.best_ask()).map(|level| level.price.to_string()),
            "book_local_ts": book.map(|book| ts(book.local_ts)),
            "regime_features": serde_json::to_value(features).unwrap_or(Value::Null)
        })
    });
    json!({
        "source": if batch.is_some() { "validated_protocol_v3_pipeline_input" } else { "durable_decision_only_diagnostic" },
        "captured_no_later_than": decision.recorded_ts.min(application.recorded_ts).to_rfc3339_opts(SecondsFormat::Millis, true),
        "decision_event_sha256": decision.event_sha256,
        "outcome": decision.payload["outcome"],
        "order_kind": decision.payload["order_kind"],
        "expected_edge": decision.payload["expected_edge"],
        "ttl_ms": decision.payload["ttl_ms"],
        "post_only": decision.payload["post_only"],
        "tick_size": decision.payload["tick_size"],
        "neg_risk": decision.payload["neg_risk"],
        "protocol_v3_pipeline": batch_fields
    })
}

fn coverage_row(observed: usize, denominator: usize, definition: &str) -> Value {
    json!({
        "denominator": denominator,
        "observed": observed.min(denominator),
        "missing": denominator.saturating_sub(observed),
        "coverage": ratio_f64(observed.min(denominator), denominator),
        "definition": definition
    })
}

fn merge_summary_paths(
    mut summary: Value,
    order_path: &Path,
    fill_path: &Path,
    summary_path: &Path,
    markdown_path: &Path,
    manifest_path: &Path,
) -> Value {
    if let Some(object) = summary.as_object_mut() {
        object.insert(
            "artifacts".to_owned(),
            json!({
                "order_lifecycle_fact": order_path.to_string_lossy(),
                "fill_markout_fact": fill_path.to_string_lossy(),
                "summary_json": summary_path.to_string_lossy(),
                "summary_markdown": markdown_path.to_string_lossy(),
                "artifact_manifest": manifest_path.to_string_lossy()
            }),
        );
    }
    summary
}

struct StagingDirectory {
    path: PathBuf,
    published: bool,
}

impl StagingDirectory {
    fn create(out: &Path) -> Result<Self, ResearchError> {
        if out.exists() {
            return Err(ResearchError::InvalidInput(format!(
                "{} already exists; loss diagnostics never overwrites an output directory",
                out.display()
            )));
        }
        let parent = out
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let name = out
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                ResearchError::InvalidInput(
                    "loss diagnostics --out must name a directory".to_owned(),
                )
            })?;
        static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        for _ in 0..128 {
            let nonce = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = parent.join(format!(
                ".{name}.loss-diagnostics-staging-{}-{nonce}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        published: false,
                    })
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(ResearchError::InvalidInput(
            "could not allocate a unique sibling staging directory".to_owned(),
        ))
    }

    fn publish_to(&mut self, out: &Path) -> Result<(), ResearchError> {
        publish_directory_noreplace(&self.path, out)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(target_os = "linux")]
fn publish_directory_noreplace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in source path"))?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in destination path")
    })?;
    // SAFETY: both paths are owned NUL-terminated byte strings and the flags do not
    // permit replacement of an existing destination.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn publish_directory_noreplace(_source: &Path, _destination: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "loss diagnostics atomic no-replace publication requires Linux renameat2",
    ))
}

fn write_jsonl(path: &Path, rows: &[Value]) -> Result<(), ResearchError> {
    let mut writer = BufWriter::new(File::create(path)?);
    for row in rows {
        serde_json::to_writer(&mut writer, row)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_json_synced(path: &Path, value: &Value) -> Result<(), ResearchError> {
    let mut writer = BufWriter::new(File::create(path)?);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_bytes_synced(path: &Path, bytes: &[u8]) -> Result<(), ResearchError> {
    let mut writer = BufWriter::new(File::create(path)?);
    writer.write_all(bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), ResearchError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn build_artifact_manifest(
    order_path: &Path,
    order_rows: usize,
    fill_path: &Path,
    fill_rows: usize,
    summary_path: &Path,
    markdown_path: &Path,
) -> Result<Value, ResearchError> {
    let binding = |path: &Path, schema: &str, row_count: Option<usize>| {
        let bytes = fs::read(path)?;
        Ok::<_, ResearchError>(json!({
            "filename": path.file_name().and_then(|name| name.to_str()),
            "schema": schema,
            "row_count": row_count,
            "content_length": bytes.len(),
            "sha256": sha256_prefixed(&bytes)
        }))
    };
    Ok(json!({
        "schema": ARTIFACT_MANIFEST_SCHEMA,
        "schema_version": 1,
        "publication": "atomic_sibling_directory_rename",
        "artifacts": [
            binding(order_path, ORDER_FACT_SCHEMA, Some(order_rows))?,
            binding(fill_path, FILL_FACT_SCHEMA, Some(fill_rows))?,
            binding(summary_path, SUMMARY_SCHEMA, None)?,
            binding(markdown_path, "text/markdown", None)?
        ]
    }))
}

fn loss_diagnostics_markdown(report: &Value) -> String {
    let result = &report["result"];
    let coverage = &result["coverage"];
    format!(
        "# Loss Diagnostics\n\n- Status: **{}**\n- Diagnostic only / promotion eligible: **{} / {}**\n- Order lifecycle rows: **{}**\n- Fill-markout rows: **{}**\n- Decision binding coverage: **{} / {}**\n- Queue-field coverage: **{} / {}**\n- 1s markout coverage: **{} / {}**\n- 5s markout coverage: **{} / {}**\n- 30s markout coverage: **{} / {}**\n- Orphan fill lifecycles / markouts: **{} / {}**\n\nQueue depth remains `inferred_size_ahead`; literal FIFO rank is unavailable. Facts are research diagnostics and never count as promotion evidence.\n",
        result["status"].as_str().unwrap_or("unknown"),
        result["diagnostic_only"],
        result["promotion_eligible"],
        result["counts"]["order_lifecycle_rows"],
        result["counts"]["fill_markout_rows"],
        coverage["decisions"]["observed"],
        coverage["decisions"]["denominator"],
        coverage["queue_fields"]["observed"],
        coverage["queue_fields"]["denominator"],
        coverage["markout_1s"]["observed"],
        coverage["markout_1s"]["denominator"],
        coverage["markout_5s"]["observed"],
        coverage["markout_5s"]["denominator"],
        coverage["markout_30s"]["observed"],
        coverage["markout_30s"]["denominator"],
        result["counts"]["orphan_fill_lifecycles"],
        result["counts"]["orphan_markout_rows"]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loss_diagnostics_writes_one_row_per_order_and_fill_with_missing_markout_and_cancel_race() {
        let root = test_root("facts");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision = bound_place_decision("order-batch", 0, "market-1", "token-1", "0.50", "5");
        let application = applied_place(&decision, "order-1", start + Duration::milliseconds(10));
        let fill_one_ts = start + Duration::seconds(1);
        let fill_two_ts = start + Duration::seconds(2);
        let fill_one = queue_fill("order-1", fill_one_ts, "2", "3", true, false);
        let fill_two = queue_fill("order-1", fill_two_ts, "1", "2", true, true);
        let orphan = queue_fill("orphan-order", fill_two_ts, "1", "0", false, true);
        let mut events = vec![
            event("decision", decision, start),
            event(
                "paper_decision_output_applied",
                application,
                start + Duration::milliseconds(10),
            ),
            event(
                "paper_order_queue_registration",
                json!({
                    "order_id": "order-1",
                    "market_id": "market-1",
                    "token_id": "token-1",
                    "side": "buy",
                    "quote_price": "0.50",
                    "order_size": "5",
                    "submitted_ts": start + Duration::milliseconds(10),
                    "live_ts": start + Duration::milliseconds(260),
                    "queue_position_source": "public_l2_shadow"
                }),
                start + Duration::milliseconds(11),
            ),
            event(
                "paper_order_queue_snapshot",
                json!({
                    "order_id": "order-1",
                    "market_id": "market-1",
                    "token_id": "token-1",
                    "side": "buy",
                    "quote_price": "0.50",
                    "order_size": "5",
                    "snapshot_ts": start + Duration::milliseconds(300),
                    "visible_size_ahead_estimate": "12"
                }),
                start + Duration::milliseconds(300),
            ),
            event("paper_queue_shadow_fill", fill_one.clone(), fill_one_ts),
            event("paper_queue_shadow_fill", fill_two.clone(), fill_two_ts),
            event("paper_queue_shadow_fill", orphan, fill_two_ts),
            event(
                "paper_cancel_latency",
                json!({
                    "order_id": "order-1",
                    "market_id": "market-1",
                    "token_id": "token-1",
                    "cancel_requested_ts": start + Duration::milliseconds(1500),
                    "cancel_ack_ts": start + Duration::milliseconds(2100),
                    "cancel_latency_ms": "600",
                    "order_age_ms": 2090
                }),
                start + Duration::milliseconds(2100),
            ),
        ];
        for horizon in MARKOUT_HORIZONS_SECONDS {
            events.push(event(
                "paper_fill_markout",
                markout("fill-1", &fill_one, fill_one_ts, horizon, "2"),
                fill_one_ts + Duration::seconds(horizon) + Duration::milliseconds(3),
            ));
        }
        for horizon in [1, 5] {
            events.push(event(
                "paper_fill_markout",
                markout("fill-2", &fill_two, fill_two_ts, horizon, "1"),
                fill_two_ts + Duration::seconds(horizon) + Duration::milliseconds(3),
            ));
        }
        events.push(event(
            "paper_fill_markout_missing",
            missing_markout("fill-2", &fill_two, fill_two_ts, 30),
            fill_two_ts + Duration::seconds(30),
        ));
        events.extend([
            event(
                "paper_order_queue_registration",
                json!({"market_id": "market-1"}),
                start + Duration::milliseconds(20),
            ),
            event(
                "paper_order_queue_snapshot",
                json!({"market_id": "market-1", "visible_size_ahead_estimate": "4"}),
                start + Duration::milliseconds(21),
            ),
            event(
                "execution_report",
                json!({"filled_size": "1", "fee": "0", "avg_price": "0.50", "local_ts": start + Duration::seconds(3)}),
                start + Duration::seconds(3),
            ),
            event(
                "paper_cancel_latency",
                json!({"market_id": "market-1"}),
                start + Duration::milliseconds(22),
            ),
            event(
                "paper_fill_markout",
                markout("invalid-horizon", &fill_one, fill_one_ts, 99, "2"),
                fill_one_ts + Duration::seconds(99) + Duration::milliseconds(3),
            ),
        ]);
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);

        let report = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap();

        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(report["result"]["counts"]["order_lifecycle_rows"], 1);
        assert_eq!(report["result"]["counts"]["fill_markout_rows"], 2);
        assert_eq!(report["result"]["counts"]["orphan_fill_lifecycles"], 1);
        assert_eq!(report["result"]["counts"]["invalid_registrations"], 1);
        assert_eq!(report["result"]["counts"]["invalid_snapshots"], 1);
        assert_eq!(report["result"]["counts"]["invalid_fill_lifecycles"], 1);
        assert_eq!(report["result"]["counts"]["invalid_cancellations"], 1);
        assert_eq!(report["result"]["counts"]["invalid_markouts"], 1);
        assert_eq!(
            report["result"]["coverage"]["markout_30s"]["denominator"],
            2
        );
        assert_eq!(report["result"]["coverage"]["markout_30s"]["observed"], 1);
        let orders = read_jsonl(&out.join(ORDER_FACT_FILE));
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0]["inferred_size_ahead"], "12");
        assert_eq!(orders[0]["partial_fill"], true);
        assert_eq!(orders[0]["full_fill"], false);
        assert_eq!(orders[0]["fill_raced_cancellation"], true);
        assert_eq!(orders[0]["cancel_state"], "fill_cancel_race");
        let fills = read_jsonl(&out.join(FILL_FACT_FILE));
        assert_eq!(fills.len(), 2);
        assert_eq!(
            fills
                .iter()
                .filter(|row| row["markout_30s_status"] == "missing")
                .count(),
            1
        );
        assert!(fills
            .iter()
            .all(|row| row.get("markout_1s_status").is_some()));
        assert!(out.join(SUMMARY_FILE).is_file());
        assert!(out.join(MARKDOWN_FILE).is_file());
    }

    #[test]
    fn loss_diagnostics_rejects_duplicate_fill_lifecycle_identity() {
        let root = test_root("duplicate-fill");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision =
            bound_place_decision("duplicate-batch", 0, "market-1", "token-1", "0.50", "5");
        let fill = queue_fill(
            "order-1",
            start + Duration::seconds(1),
            "2",
            "3",
            true,
            false,
        );
        write_snapshot(
            &input,
            &[
                event("decision", decision.clone(), start),
                event(
                    "paper_decision_output_applied",
                    applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                    start + Duration::milliseconds(10),
                ),
                event(
                    "paper_order_queue_registration",
                    json!({
                        "order_id": "order-1", "market_id": "market-1", "token_id": "token-1",
                        "side": "buy", "quote_price": "0.50", "order_size": "5"
                    }),
                    start + Duration::milliseconds(11),
                ),
                event(
                    "paper_queue_shadow_fill",
                    fill.clone(),
                    start + Duration::seconds(1),
                ),
                event(
                    "paper_queue_shadow_fill",
                    fill,
                    start + Duration::seconds(1),
                ),
            ],
        );
        let error = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate fill lifecycle identity"));
    }

    #[test]
    fn loss_diagnostics_rejects_legacy_input_without_explicit_v3_identity() {
        let root = test_root("legacy");
        let input = root.join("snapshot");
        let out = root.join("out");
        write_snapshot(
            &input,
            &[event(
                "decision",
                json!({"action": "place", "market_id": "m1"}),
                test_ts("2026-07-20T12:00:00Z"),
            )],
        );
        let error = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap_err();
        assert!(error
            .to_string()
            .contains("requires explicit valid Protocol-v3 decision-output identity"));
    }

    #[test]
    fn loss_diagnostics_accepts_only_fully_bound_stable_v3_as_complete() {
        let root = test_root("fully-bound-v3");
        let input = root.join("snapshot");
        let out = root.join("out");
        write_snapshot(&input, &fully_bound_v3_events());

        let report = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap();
        assert_eq!(report["result"]["status"], "complete_diagnostic");
        assert_eq!(report["result"]["eligible_protocol_v3_identity"], true);
        assert_eq!(
            report["result"]["runtime_provenance"]["distinct_identity_count"],
            2
        );
        assert_eq!(
            report["result"]["runtime_provenance_stable_identity"]["distinct_identity_count"],
            1
        );
        assert_eq!(
            report["result"]["coverage"]["market_starts"]["coverage"],
            1.0
        );
        assert_eq!(
            report["result"]["coverage"]["market_settlements"]["coverage"],
            1.0
        );
        assert_eq!(
            report["result"]["counts"]["expected_v3_place_outputs"],
            report["result"]["counts"]["order_lifecycle_rows"]
        );
        let orders = read_jsonl(&out.join(ORDER_FACT_FILE));
        let fills = read_jsonl(&out.join(FILL_FACT_FILE));
        assert!(!orders.is_empty());
        for row in orders.iter().chain(&fills) {
            assert_fact_hash(row);
        }
        assert!(orders.iter().all(|row| {
            row["market_start_event_sha256"].is_string()
                && row["terminal_settlement_event_sha256"].is_string()
                && row["terminal_settlement_journal_sha256"].is_string()
        }));
        assert_artifact_manifest(&out);
        assert!(staging_siblings(&out).is_empty());
    }

    #[test]
    fn mixed_or_invalid_runtime_provenance_cannot_complete() {
        let root = test_root("mixed-provenance");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let first_ts = parse_datetime(events[0].get("recorded_ts")).unwrap();
        events.push(event(
            "runtime_provenance",
            json!({
                "schema_version": 1,
                "backend_impl": "rust",
                "decision_pipeline_schema": "legacy"
            }),
            first_ts + Duration::microseconds(1),
        ));
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);

        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(report["result"]["eligible_protocol_v3_identity"], false);
        assert_eq!(
            report["result"]["runtime_provenance"]["invalid_observations"],
            1
        );

        let root = test_root("changed-provenance-config");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let second_runtime = events
            .iter_mut()
            .filter(|event| event["event_type"].as_str() == Some("runtime_provenance"))
            .nth(1)
            .unwrap();
        second_runtime["payload"]["decision_config_sha256"] =
            json!(format!("sha256:{}", "e".repeat(64)));
        write_snapshot(&input, &events);
        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(
            report["result"]["runtime_provenance_stable_identity"]["distinct_identity_count"],
            2
        );
    }

    #[test]
    fn mismatched_zero_fill_cancel_report_cannot_affect_order_state_or_hashes() {
        let root = test_root("mismatched-cancel-report");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision = bound_place_decision("cancel-report", 0, "market-1", "token-1", "0.50", "5");
        write_snapshot(
            &input,
            &[
                event("decision", decision.clone(), start),
                event(
                    "paper_decision_output_applied",
                    applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                    start + Duration::milliseconds(10),
                ),
                event(
                    "paper_order_queue_registration",
                    queue_identity("order-1", "token-1", start + Duration::milliseconds(10)),
                    start + Duration::milliseconds(11),
                ),
                event(
                    "execution_report",
                    json!({
                        "order_id": "order-1", "market_id": "market-1", "token_id": "wrong-token",
                        "status": "paper_cancelled", "filled_size": "0", "avg_price": null,
                        "fee": "0", "local_ts": start + Duration::milliseconds(20),
                        "raw": {"decision": {
                            "market_id": "market-1", "token_id": "wrong-token", "side": "buy",
                            "price": "0.50", "size": "5"
                        }}
                    }),
                    start + Duration::milliseconds(20),
                ),
            ],
        );
        let error = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap_err();
        assert!(error
            .to_string()
            .contains("execution report identity or chronology is invalid"));
    }

    #[test]
    fn non_paper_and_status_size_inconsistent_reports_never_create_touch_fills() {
        for (case, status, filled_size, avg_price, fee) in [
            ("live-fill-report", "live_filled", "1", true, "0"),
            ("funded-fill-report", "funded_filled", "1", true, "0"),
            ("resting-with-fill-report", "paper_resting", "1", true, "0"),
            ("filled-with-zero-report", "paper_filled", "0", false, "0"),
            ("filled-with-partial-report", "paper_filled", "1", true, "0"),
            (
                "filled-with-wrong-taker-fee-report",
                "paper_filled",
                "FULL",
                true,
                "0",
            ),
            (
                "maker-filled-with-fee-report",
                "paper_filled_maker",
                "FULL",
                true,
                "0.01",
            ),
            (
                "resting-with-fee-report",
                "paper_resting",
                "0",
                false,
                "0.01",
            ),
            (
                "cancelled-with-fee-report",
                "paper_cancelled",
                "0",
                false,
                "0.01",
            ),
            (
                "unknown-paper-status-report",
                "paper_rejected",
                "0",
                false,
                "0",
            ),
        ] {
            let root = test_root(case);
            let input = root.join("snapshot");
            let out = root.join("out");
            let mut events = fully_bound_v3_events();
            let application = events
                .iter()
                .find(|event| event["event_type"] == "paper_decision_output_applied")
                .unwrap();
            let order_id = application["payload"]["order_id"]
                .as_str()
                .unwrap()
                .to_owned();
            let batch_id = application["payload"]["strategy_batch_id"]
                .as_str()
                .unwrap();
            let output_index = application["payload"]["strategy_batch_output_index"]
                .as_u64()
                .unwrap();
            let decision = events
                .iter()
                .find(|event| {
                    event["event_type"] == "decision"
                        && event["payload"]["strategy_batch_id"].as_str() == Some(batch_id)
                        && event["payload"]["strategy_batch_output_index"].as_u64()
                            == Some(output_index)
                })
                .unwrap();
            let parsed = durable_decision_output_v3(&decision["payload"]).unwrap();
            let identity = parsed.place_identity.unwrap();
            let filled_size = if filled_size == "FULL" {
                identity.size.to_string()
            } else {
                filled_size.to_owned()
            };
            let avg_price = avg_price.then(|| identity.price.to_string());
            let local_ts = parse_datetime(application.get("recorded_ts")).unwrap()
                + Duration::milliseconds(10);
            events.push(event(
                "execution_report",
                json!({
                    "order_id": order_id,
                    "market_id": identity.market_id,
                    "token_id": identity.token_id,
                    "status": status,
                    "filled_size": filled_size,
                    "avg_price": avg_price,
                    "fee": fee,
                    "local_ts": local_ts,
                    "raw": {"decision": {
                        "market_id": identity.market_id,
                        "token_id": identity.token_id,
                        "side": identity.side,
                        "price": identity.price.to_string(),
                        "size": identity.size.to_string()
                    }}
                }),
                local_ts,
            ));
            events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
            write_snapshot(&input, &events);

            let error = run_loss_diagnostics(LossDiagnosticsOptions {
                input,
                out: out.clone(),
            })
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("execution report identity or chronology is invalid"),
                "{case}: {error}"
            );
            assert!(!out.exists(), "{case} published invalid touch-fill facts");
        }
    }

    #[test]
    fn embedded_application_report_cannot_precede_its_bound_decision() {
        let root = test_root("pre-decision-application-report");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let application_index = events
            .iter()
            .position(|event| event["event_type"] == "paper_decision_output_applied")
            .unwrap();
        let batch_id = events[application_index]["payload"]["strategy_batch_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let output_index = events[application_index]["payload"]["strategy_batch_output_index"]
            .as_u64()
            .unwrap();
        let decision_recorded_ts = events
            .iter()
            .find(|event| {
                event["event_type"] == "decision"
                    && event["payload"]["strategy_batch_id"].as_str() == Some(batch_id.as_str())
                    && event["payload"]["strategy_batch_output_index"].as_u64()
                        == Some(output_index)
            })
            .and_then(|event| parse_datetime(event.get("recorded_ts")))
            .unwrap();
        let forged_local_ts = decision_recorded_ts - Duration::milliseconds(1);
        let application = &mut events[application_index]["payload"];
        application["execution_reports"][0]["local_ts"] = json!(forged_local_ts);
        application["execution_reports_sha256"] =
            json!(canonical_value_sha256(&application["execution_reports"]).unwrap());
        write_snapshot(&input, &events);

        let error = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("paper status, size, fee, or chronology is invalid"),
            "{error}"
        );
        assert!(!out.exists());
    }

    #[test]
    fn embedded_taker_fill_report_requires_the_exact_producer_fee() {
        let root = test_root("embedded-wrong-taker-fee");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let application_index = events
            .iter()
            .position(|event| event["event_type"] == "paper_decision_output_applied")
            .unwrap();
        let batch_id = events[application_index]["payload"]["strategy_batch_id"]
            .as_str()
            .unwrap();
        let output_index = events[application_index]["payload"]["strategy_batch_output_index"]
            .as_u64()
            .unwrap();
        let decision = events
            .iter()
            .find(|event| {
                event["event_type"] == "decision"
                    && event["payload"]["strategy_batch_id"].as_str() == Some(batch_id)
                    && event["payload"]["strategy_batch_output_index"].as_u64()
                        == Some(output_index)
            })
            .unwrap();
        let identity = durable_decision_output_v3(&decision["payload"])
            .unwrap()
            .place_identity
            .unwrap();
        let expected_fee = crypto_taker_fee_per_share(identity.price).unwrap() * identity.size;
        assert!(expected_fee > Decimal::ZERO);

        let application = &mut events[application_index]["payload"];
        let report = &mut application["execution_reports"][0];
        report["status"] = json!("paper_filled");
        report["filled_size"] = json!(identity.size.to_string());
        report["avg_price"] = json!(identity.price.to_string());
        report["fee"] = json!("0");
        application["execution_reports_sha256"] =
            json!(canonical_value_sha256(&application["execution_reports"]).unwrap());
        write_snapshot(&input, &events);

        let error = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("paper status, size, fee, or chronology is invalid"),
            "{error}"
        );
        assert!(!out.exists());
    }

    #[test]
    fn standalone_taker_fill_accepts_only_the_exact_producer_fee() {
        let root = test_root("standalone-exact-taker-fee");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let application = events
            .iter()
            .find(|event| event["event_type"] == "paper_decision_output_applied")
            .unwrap();
        let order_id = application["payload"]["order_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let batch_id = application["payload"]["strategy_batch_id"]
            .as_str()
            .unwrap();
        let output_index = application["payload"]["strategy_batch_output_index"]
            .as_u64()
            .unwrap();
        let decision = events
            .iter()
            .find(|event| {
                event["event_type"] == "decision"
                    && event["payload"]["strategy_batch_id"].as_str() == Some(batch_id)
                    && event["payload"]["strategy_batch_output_index"].as_u64()
                        == Some(output_index)
            })
            .unwrap();
        let identity = durable_decision_output_v3(&decision["payload"])
            .unwrap()
            .place_identity
            .unwrap();
        let expected_fee_per_share = crypto_taker_fee_per_share(identity.price).unwrap();
        let expected_fee = expected_fee_per_share * identity.size;
        let local_ts =
            parse_datetime(application.get("recorded_ts")).unwrap() + Duration::milliseconds(10);
        events.push(event(
            "execution_report",
            json!({
                "order_id": order_id,
                "market_id": identity.market_id,
                "token_id": identity.token_id,
                "status": "paper_filled",
                "filled_size": identity.size.to_string(),
                "avg_price": identity.price.to_string(),
                "fee": expected_fee.to_string(),
                "local_ts": local_ts,
                "raw": {"decision": {
                    "market_id": identity.market_id,
                    "token_id": identity.token_id,
                    "side": identity.side,
                    "price": identity.price.to_string(),
                    "size": identity.size.to_string()
                }}
            }),
            local_ts,
        ));
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);

        let report = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap();
        assert_eq!(report["result"]["counts"]["invalid_execution_reports"], 0);
        assert_eq!(report["result"]["counts"]["fill_markout_rows"], 1);
        let rows = read_jsonl(&out.join(FILL_FACT_FILE));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["fill_source"], "touch_fill");
        assert_eq!(rows[0]["fee_per_share"], expected_fee_per_share.to_string());
        assert_fact_hash(&rows[0]);
        assert_artifact_manifest(&out);
    }

    #[test]
    fn exact_duplicate_batch_retry_is_counted_and_blocks_completion() {
        let root = test_root("duplicate-batch-retry");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let batch = events
            .iter()
            .find(|event| event["event_type"] == "strategy_decision_batch")
            .unwrap()
            .clone();
        events.push(batch);
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);
        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(
            report["result"]["counts"]["duplicate_batch_retries_deduplicated"],
            1
        );
        assert_eq!(
            report["result"]["completion_checks"]["no_local_quality_failures"],
            false
        );
    }

    #[test]
    fn exact_duplicate_unrelated_line_is_counted_and_blocks_completion() {
        let root = test_root("duplicate-unrelated-line");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let duplicate = event(
            "execution_quality_probe_status",
            json!({"probe": true, "probe_id": "excluded-but-still-source-counted"}),
            test_ts("2026-07-20T12:01:00Z"),
        );
        events.push(duplicate.clone());
        events.push(duplicate);
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);

        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(report["result"]["counts"]["duplicate_event_lines"], 1);
        assert_eq!(
            report["result"]["completion_checks"]["no_exact_duplicate_event_lines"],
            false
        );
        assert_eq!(
            report["result"]["snapshot_identity"]["duplicate_line_estimate"],
            1
        );
        assert_eq!(report["result"]["counts"]["probe_events_excluded"], 2);
    }

    #[test]
    fn exact_duplicate_detector_remains_exact_after_more_than_one_hundred_thousand_events() {
        let mut detector = ExactTimestampDuplicateDetector::default();
        let start = test_ts("2026-07-20T00:00:00Z");
        for index in 0..100_001_i64 {
            assert!(!detector.observe(
                start + Duration::microseconds(index),
                format!("sha256:unique-{index:06}")
            ));
            assert_eq!(detector.hashes.len(), 1);
        }

        let current_ts = start + Duration::microseconds(100_001);
        assert!(!detector.observe(current_ts, "sha256:target".to_owned()));
        assert!(!detector.observe(current_ts, "sha256:other".to_owned()));
        assert!(detector.observe(current_ts, "sha256:target".to_owned()));
    }

    #[test]
    fn expected_batch_outputs_are_authoritative_when_a_durable_decision_is_missing() {
        let root = test_root("missing-durable-decision");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let missing_index = events
            .iter()
            .find(|event| {
                event["event_type"] == "decision" && event["payload"]["action"] == "place"
            })
            .and_then(|event| event["payload"]["strategy_batch_output_index"].as_u64())
            .unwrap();
        events.retain(|event| {
            !(event["event_type"] == "decision"
                && event["payload"]["strategy_batch_output_index"].as_u64() == Some(missing_index))
        });
        write_snapshot(&input, &events);
        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        for field in ["decisions", "execution_fields", "queue_fields"] {
            assert_eq!(report["result"]["coverage"][field]["denominator"], 2);
            assert_eq!(report["result"]["coverage"][field]["observed"], 1);
            assert_eq!(report["result"]["coverage"][field]["missing"], 1);
        }
    }

    #[test]
    fn book_only_market_never_counts_as_terminal_settlement() {
        let root = test_root("book-only-not-settlement");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let settlement_ts = events
            .iter()
            .find(|event| event["event_type"] == "paper_settlement")
            .and_then(|event| parse_datetime(event.get("recorded_ts")))
            .unwrap();
        events.retain(|event| event["event_type"] != "paper_settlement");
        events.push(event(
            "book",
            json!({"market_id": "market-1", "token_id": "up-token"}),
            settlement_ts,
        ));
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);
        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(
            report["result"]["coverage"]["market_settlements"]["denominator"],
            1
        );
        assert_eq!(
            report["result"]["coverage"]["market_settlements"]["observed"],
            0
        );
        assert_eq!(
            report["result"]["market_evidence"]["missing_terminal_settlement_market_ids"],
            json!(["market-1"])
        );
        assert_eq!(
            report["result"]["market_evidence"]["valid_terminal_settlement_evidence"],
            json!({})
        );
    }

    #[test]
    fn settlement_envelope_must_be_recorded_after_end_and_claimed_final_reference() {
        for (case, before_end) in [
            ("settlement-recorded-before-end", true),
            ("settlement-recorded-before-final-reference", false),
        ] {
            let root = test_root(case);
            let input = root.join("snapshot");
            let out = root.join("out");
            let mut events = fully_bound_v3_events();
            let settlement = events
                .iter_mut()
                .find(|event| event["event_type"] == "paper_settlement")
                .unwrap();
            let end_ts = parse_datetime(settlement["payload"].get("end_ts")).unwrap();
            let final_reference_ts =
                parse_datetime(settlement["payload"].get("final_reference_source_ts")).unwrap();
            let recorded_ts = if before_end {
                end_ts - Duration::milliseconds(1)
            } else {
                final_reference_ts - Duration::milliseconds(1)
            };
            settlement["recorded_ts"] = json!(recorded_ts);
            events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
            write_snapshot(&input, &events);

            let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
            assert_eq!(report["result"]["status"], "diagnostic_ineligible");
            assert_eq!(
                report["result"]["coverage"]["market_settlements"]["observed"],
                0
            );
            assert_eq!(
                report["result"]["market_evidence"]["missing_terminal_settlement_market_ids"],
                json!(["market-1"])
            );
        }
    }

    #[test]
    fn exact_start_envelope_cannot_claim_a_future_reference_observation() {
        let root = test_root("start-recorded-before-reference");
        let input = root.join("snapshot");
        let out = root.join("out");
        let mut events = fully_bound_v3_events();
        let start = events
            .iter_mut()
            .find(|event| event["event_type"] == "market_start_price")
            .unwrap();
        let reference_ts = parse_datetime(start["payload"].get("reference_source_ts")).unwrap();
        start["recorded_ts"] = json!(reference_ts - Duration::milliseconds(1));
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);

        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(report["result"]["coverage"]["market_starts"]["observed"], 0);
        assert_eq!(
            report["result"]["market_evidence"]["missing_start_market_ids"],
            json!(["market-1"])
        );
    }

    #[test]
    fn zero_expected_place_outputs_are_not_complete_or_reported_as_full_coverage() {
        let root = test_root("zero-expected-place");
        let input = root.join("snapshot");
        let out = root.join("out");
        let decision_ts = test_ts("2026-07-20T12:00:00Z");
        let mut pipeline = super::super::tests::decision_pipeline_v3_input(decision_ts);
        pipeline.kill_switch_enabled = true;
        let (batch, decisions) = super::super::tests::decision_pipeline_v3_evidence(&pipeline);
        assert!(decisions
            .iter()
            .all(|decision| decision["action"] != "place"));
        let config = batch["decision_config_sha256"].as_str().unwrap();
        let mut events = vec![
            event(
                "runtime_provenance",
                valid_runtime_provenance(config),
                decision_ts - Duration::milliseconds(1),
            ),
            event("strategy_decision_batch", batch, decision_ts),
        ];
        for (index, decision) in decisions.into_iter().enumerate() {
            events.push(event(
                "decision",
                decision,
                decision_ts + Duration::milliseconds(index as i64 + 1),
            ));
        }
        write_snapshot(&input, &events);
        let report = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(report["result"]["counts"]["expected_v3_place_outputs"], 0);
        for field in ["decisions", "execution_fields", "queue_fields"] {
            assert_eq!(report["result"]["coverage"][field]["denominator"], 0);
            assert!(report["result"]["coverage"][field]["coverage"].is_null());
        }
    }

    #[test]
    fn atomic_publication_refuses_existing_target_without_touching_it() {
        let root = test_root("atomic-existing-target");
        let input = root.join("snapshot");
        let out = root.join("out");
        write_snapshot(&input, &fully_bound_v3_events());
        fs::create_dir_all(&out).unwrap();
        fs::write(out.join("user-marker.txt"), b"preserve-me").unwrap();
        let error = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap_err();
        assert!(error.to_string().contains("never overwrites"));
        assert_eq!(
            fs::read(out.join("user-marker.txt")).unwrap(),
            b"preserve-me"
        );
        assert!(staging_siblings(&out).is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn atomic_publication_loses_destination_race_without_overwrite() {
        let root = test_root("atomic-publish-race");
        let out = root.join("out");
        let staging_path;
        {
            let mut staging = StagingDirectory::create(&out).unwrap();
            staging_path = staging.path.clone();
            write_bytes_synced(&staging.path.join("staged.txt"), b"staged").unwrap();

            fs::create_dir(&out).unwrap();
            fs::write(out.join("user-marker.txt"), b"preserve-race-winner").unwrap();
            let error = staging.publish_to(&out).unwrap_err();
            assert!(matches!(
                error,
                ResearchError::Io(ref io_error)
                    if io_error.raw_os_error() == Some(libc::EEXIST)
            ));
            assert!(staging_path.is_dir());
            assert_eq!(
                fs::read(out.join("user-marker.txt")).unwrap(),
                b"preserve-race-winner"
            );
            assert!(!out.join("staged.txt").exists());
        }
        assert!(!staging_path.exists());
        assert_eq!(
            fs::read(out.join("user-marker.txt")).unwrap(),
            b"preserve-race-winner"
        );
        assert!(staging_siblings(&out).is_empty());
    }

    #[test]
    fn lifecycle_identity_and_post_fill_snapshot_mismatches_fail_closed() {
        for (name, mutate, expected) in [
            (
                "wrong-token",
                "wrong-token",
                "queue snapshot identity mismatch",
            ),
            (
                "post-fill-snapshot",
                "post-fill-snapshot",
                "queue snapshot chronology is invalid",
            ),
        ] {
            let root = test_root(name);
            let input = root.join("snapshot");
            let out = root.join("out");
            let start = test_ts("2026-07-20T12:00:00Z");
            let decision = bound_place_decision(name, 0, "market-1", "token-1", "0.50", "5");
            let fill_ts = start + Duration::seconds(1);
            let fill = queue_fill("order-1", fill_ts, "1", "4", true, false);
            let snapshot_ts = if mutate == "post-fill-snapshot" {
                fill_ts + Duration::milliseconds(1)
            } else {
                start + Duration::milliseconds(300)
            };
            let snapshot_token = if mutate == "wrong-token" {
                "token-other"
            } else {
                "token-1"
            };
            let mut events = vec![
                event("decision", decision.clone(), start),
                event(
                    "paper_decision_output_applied",
                    applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                    start + Duration::milliseconds(10),
                ),
                event(
                    "paper_order_queue_registration",
                    queue_identity("order-1", "token-1", start + Duration::milliseconds(10)),
                    start + Duration::milliseconds(11),
                ),
                event(
                    "paper_order_queue_snapshot",
                    json!({
                        "order_id": "order-1", "market_id": "market-1",
                        "token_id": snapshot_token, "side": "buy", "quote_price": "0.50",
                        "order_size": "5", "snapshot_ts": snapshot_ts,
                        "visible_size_ahead_estimate": "12"
                    }),
                    snapshot_ts,
                ),
                event("paper_queue_shadow_fill", fill, fill_ts),
            ];
            events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
            write_snapshot(&input, &events);
            let error = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn alternative_fill_sources_cannot_be_combined_in_one_order_aggregate() {
        let root = test_root("mixed-fill-source");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision = bound_place_decision("mixed-fill", 0, "market-1", "token-1", "0.50", "5");
        let fill_ts = start + Duration::seconds(1);
        let events = vec![
            event("decision", decision.clone(), start),
            event(
                "paper_decision_output_applied",
                applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                start + Duration::milliseconds(10),
            ),
            event(
                "paper_order_queue_registration",
                queue_identity("order-1", "token-1", start + Duration::milliseconds(10)),
                start + Duration::milliseconds(11),
            ),
            event(
                "paper_queue_shadow_fill",
                queue_fill("order-1", fill_ts, "1", "4", true, false),
                fill_ts,
            ),
            event(
                "execution_report",
                json!({
                    "order_id": "order-1", "market_id": "market-1", "token_id": "token-1",
                    "side": "buy", "avg_price": "0.50", "filled_size": "1", "fee": "0",
                    "local_ts": fill_ts + Duration::milliseconds(1), "status": "paper_filled",
                    "raw": {"decision": {
                        "market_id": "market-1", "token_id": "token-1", "side": "buy",
                        "price": "0.50", "size": "5"
                    }}
                }),
                fill_ts + Duration::milliseconds(1),
            ),
        ];
        write_snapshot(&input, &events);
        let error = run_loss_diagnostics(LossDiagnosticsOptions { input, out }).unwrap_err();
        assert!(error.to_string().contains("mixes alternative fill sources"));
    }

    #[test]
    fn late_markout_is_counted_invalid_and_remains_a_fact_column() {
        let root = test_root("late-markout");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision = bound_place_decision("late-markout", 0, "market-1", "token-1", "0.50", "5");
        let fill_ts = start + Duration::seconds(1);
        let fill = queue_fill("order-1", fill_ts, "1", "4", true, false);
        let observed_ts = fill_ts + Duration::seconds(1) + Duration::milliseconds(2_501);
        let mut late = markout("late-fill", &fill, fill_ts, 1, "1");
        late["observation_delay_ms"] = json!(2_501);
        late["observed_ts"] = json!(observed_ts);
        let mut events = vec![
            event("decision", decision.clone(), start),
            event(
                "paper_decision_output_applied",
                applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                start + Duration::milliseconds(10),
            ),
            event(
                "paper_order_queue_registration",
                queue_identity("order-1", "token-1", start + Duration::milliseconds(10)),
                start + Duration::milliseconds(11),
            ),
            event("paper_queue_shadow_fill", fill, fill_ts),
            event("paper_fill_markout", late, observed_ts),
        ];
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);
        let report = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap();
        assert_eq!(report["result"]["counts"]["invalid_markouts"], 1);
        let rows = read_jsonl(&out.join(FILL_FACT_FILE));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["markout_1s_status"], "invalid");
        assert!(rows[0]["markout_1s_event_sha256"].is_string());
    }

    #[test]
    fn markout_claiming_a_future_observation_cannot_precede_its_envelope_time() {
        let root = test_root("future-observation-markout");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision =
            bound_place_decision("future-markout", 0, "market-1", "token-1", "0.50", "5");
        let fill_ts = start + Duration::seconds(1);
        let fill = queue_fill("order-1", fill_ts, "1", "4", true, false);
        let forged = markout("future-fill", &fill, fill_ts, 1, "1");
        let forged_recorded_ts = fill_ts - Duration::milliseconds(1);
        let mut events = vec![
            event("decision", decision.clone(), start),
            event(
                "paper_decision_output_applied",
                applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                start + Duration::milliseconds(10),
            ),
            event(
                "paper_order_queue_registration",
                queue_identity("order-1", "token-1", start + Duration::milliseconds(10)),
                start + Duration::milliseconds(11),
            ),
            event("paper_queue_shadow_fill", fill, fill_ts),
            event("paper_fill_markout", forged, forged_recorded_ts),
        ];
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        write_snapshot(&input, &events);

        let report = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: out.clone(),
        })
        .unwrap();
        assert_eq!(report["result"]["status"], "diagnostic_ineligible");
        assert_eq!(report["result"]["counts"]["invalid_markouts"], 1);
        assert_eq!(report["result"]["coverage"]["markout_1s"]["observed"], 0);
        let rows = read_jsonl(&out.join(FILL_FACT_FILE));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["markout_1s_status"], "invalid");
        assert!(rows[0]["markout_1s_event_sha256"].is_string());
    }

    #[test]
    fn unsealed_snapshot_is_rejected_and_unknown_market_has_zero_truth_coverage() {
        let root = test_root("snapshot-and-market-proof");
        let input = root.join("snapshot");
        let out = root.join("out");
        let start = test_ts("2026-07-20T12:00:00Z");
        let decision = bound_place_decision("unknown-market", 0, "unknown", "token-1", "0.50", "5");
        write_snapshot(
            &input,
            &[
                event("decision", decision.clone(), start),
                event(
                    "paper_decision_output_applied",
                    applied_place(&decision, "order-1", start + Duration::milliseconds(10)),
                    start + Duration::milliseconds(10),
                ),
            ],
        );
        let report = run_loss_diagnostics(LossDiagnosticsOptions {
            input: input.clone(),
            out,
        })
        .unwrap();
        assert_eq!(report["result"]["coverage"]["market_starts"]["observed"], 0);
        assert_eq!(
            report["result"]["coverage"]["market_settlements"]["observed"],
            0
        );

        let manifest_path = input.join("events_manifest.json");
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest.as_object_mut().unwrap().remove("sealed");
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let error = run_loss_diagnostics(LossDiagnosticsOptions {
            input,
            out: root.join("out-unsealed"),
        })
        .unwrap_err();
        assert!(error.to_string().contains("sealed decision-grade"));
    }

    fn fully_bound_v3_events() -> Vec<Value> {
        let decision_ts = test_ts("2026-07-20T12:00:00Z");
        let input = super::super::tests::decision_pipeline_v3_input(decision_ts);
        let (batch, decisions) = super::super::tests::decision_pipeline_v3_evidence(&input);
        let decision_config_sha256 = batch["decision_config_sha256"].as_str().unwrap();
        let mut runtime_one = valid_runtime_provenance(decision_config_sha256);
        runtime_one["event_blob_prefix_routing"] = json!({
            "evaluated_event_ts": decision_ts - Duration::milliseconds(2),
            "selected_prefix": "shadow-events/test-campaign"
        });
        let mut runtime_two = runtime_one.clone();
        runtime_two["event_blob_prefix_routing"]["evaluated_event_ts"] =
            json!(decision_ts - Duration::milliseconds(1));
        let mut events = vec![
            event(
                "market_start_price",
                json!({
                    "schema": "polyedge.market_start_price.v1",
                    "schema_version": 1,
                    "market_id": input.market_start_evidence.market_id,
                    "market_start_ts": input.market_start_evidence.market_start_ts,
                    "market_end_ts": input.market_start_evidence.market_end_ts,
                    "start_price": input.market_start_evidence.start_price.to_string(),
                    "reference_source": input.market_start_evidence.reference_source,
                    "reference_source_ts": input.market_start_evidence.reference_source_ts,
                    "reference_exact_resolution_source": true,
                    "reference_stale": false
                }),
                input.market_start_evidence.reference_source_ts,
            ),
            event(
                "runtime_provenance",
                runtime_one,
                decision_ts - Duration::milliseconds(2),
            ),
            event(
                "runtime_provenance",
                runtime_two,
                decision_ts - Duration::milliseconds(1),
            ),
            event("strategy_decision_batch", batch, decision_ts),
        ];
        let mut place_count = 0_usize;
        for (index, decision) in decisions.into_iter().enumerate() {
            let recorded_ts = decision_ts + Duration::milliseconds(10 + index as i64);
            let parsed = durable_decision_output_v3(&decision).unwrap();
            events.push(event("decision", decision.clone(), recorded_ts));
            if parsed.action != "place" {
                continue;
            }
            let order_id = format!("fully-bound-order-{place_count}");
            let application_ts = decision_ts + Duration::milliseconds(100 + index as i64);
            let identity = parsed.place_identity.as_ref().unwrap();
            events.push(event(
                "paper_decision_output_applied",
                applied_place(&decision, &order_id, application_ts),
                application_ts,
            ));
            events.push(event(
                "paper_order_queue_registration",
                queue_identity_for(&order_id, identity, application_ts),
                application_ts + Duration::milliseconds(1),
            ));
            let mut snapshot = queue_identity_for(&order_id, identity, application_ts);
            snapshot["snapshot_ts"] = json!(application_ts + Duration::milliseconds(2));
            snapshot["visible_size_ahead_estimate"] = json!("10");
            events.push(event(
                "paper_order_queue_snapshot",
                snapshot,
                application_ts + Duration::milliseconds(2),
            ));
            place_count += 1;
        }
        assert!(place_count > 0);
        let start_evidence = &input.market_start_evidence;
        let settlement = json!({
            "market_id": input.market.market_id,
            "start_ts": input.market.start_ts,
            "end_ts": input.market.end_ts,
            "start_price": start_evidence.start_price.to_string(),
            "start_reference_source": start_evidence.reference_source,
            "start_reference_source_ts": start_evidence.reference_source_ts,
            "start_reference_exact_resolution_source": true,
            "start_reference_stale": false,
            "final_price": (start_evidence.start_price + Decimal::ONE).to_string(),
            "final_reference_source": "chainlink_rtds",
            "final_reference_source_ts": input.market.end_ts + Duration::seconds(1),
            "final_reference_exact_resolution_source": true,
            "final_reference_stale": false,
            "winning_outcome": "up"
        });
        events.push(journaled_event(
            "paper_settlement",
            settlement,
            input.market.end_ts + Duration::seconds(1),
        ));
        events.sort_by_key(|event| parse_datetime(event.get("recorded_ts")).unwrap());
        events
    }

    fn valid_runtime_provenance(decision_config_sha256: &str) -> Value {
        let candidate = FrozenStrategyMode::DynamicQuoteStyle.candidate();
        json!({
            "schema_version": 1,
            "backend_impl": "rust",
            "app_name": "polyedge-shadow-neu",
            "runtime_role": "profitability_shadow",
            "execution_mode": "paper",
            "paper_maker_fill_policy": "none",
            "storage_account": "teststorage",
            "storage_container": "polyedge-shadow-events",
            "event_blob_prefix": "shadow-events/test-campaign",
            "git_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "runtime_config_hash": format!("sha256:{}", "b".repeat(64)),
            "shadow_only": true,
            "allow_live": false,
            "enable_taker_orders": false,
            "allow_emergency_account_cancel": false,
            "adaptive_regime_enabled": true,
            "adaptive_regime_mode": "dynamic_quote_style",
            "publish_strategy_canary_intents": true,
            "research_only": true,
            "decision_pipeline_schema": "polyedge.strategy_decision_batch.v3",
            "decision_pipeline_parity_scope": "full_decision_pipeline_recomputation",
            "decision_config_schema": "polyedge.decision_config.v1",
            "decision_config_sha256": decision_config_sha256,
            "candidate": {
                "name": candidate.name,
                "version": candidate.version,
                "config_hash": candidate.config_hash
            },
            "execution_model": {
                "version": "test-v1",
                "blob_uri": "azure://test/model.json",
                "sha256": format!("sha256:{}", "c".repeat(64))
            }
        })
    }

    fn queue_identity(order_id: &str, token_id: &str, submitted_ts: DateTime<Utc>) -> Value {
        json!({
            "order_id": order_id, "market_id": "market-1", "token_id": token_id,
            "side": "buy", "quote_price": "0.50", "order_size": "5",
            "submitted_ts": submitted_ts
        })
    }

    fn queue_identity_for(
        order_id: &str,
        identity: &PlaceOutputIdentityV3,
        submitted_ts: DateTime<Utc>,
    ) -> Value {
        json!({
            "order_id": order_id,
            "market_id": identity.market_id,
            "token_id": identity.token_id,
            "side": identity.side,
            "quote_price": identity.price.to_string(),
            "order_size": identity.size.to_string(),
            "submitted_ts": submitted_ts
        })
    }

    fn journaled_event(event_type: &str, mut payload: Value, recorded_ts: DateTime<Utc>) -> Value {
        let journal_id = format!("paper-settlement-{}", "d".repeat(64));
        let journal_sha256 = canonical_value_sha256(&json!({
            "schema": "polyedge.paper_settlement_journal.v1",
            "settlement_journal_id": journal_id,
            "settlement_journal_event_count": 1,
            "events": [{
                "event_index": 0,
                "event_type": event_type,
                "payload": payload
            }]
        }))
        .unwrap();
        let object = payload.as_object_mut().unwrap();
        object.insert(
            "settlement_journal_schema".to_owned(),
            json!("polyedge.paper_settlement_journal.v1"),
        );
        object.insert("settlement_journal_id".to_owned(), json!(journal_id));
        object.insert("settlement_journal_event_index".to_owned(), json!(0));
        object.insert("settlement_journal_event_count".to_owned(), json!(1));
        object.insert(
            "settlement_journal_sha256".to_owned(),
            json!(journal_sha256),
        );
        event(event_type, payload, recorded_ts)
    }

    fn bound_place_decision(
        batch_label: &str,
        output_index: u64,
        market_id: &str,
        token_id: &str,
        price: &str,
        size: &str,
    ) -> Value {
        let mut payload = json!({
            "action": "place",
            "market_id": market_id,
            "condition_id": format!("condition-{market_id}"),
            "token_id": token_id,
            "outcome": "up",
            "side": "buy",
            "price": price,
            "size": size,
            "quote_amount": (d(price) * d(size)).to_string(),
            "order_kind": "post_only_gtc",
            "reason": "loss diagnostics fixture",
            "ttl_ms": 60000,
            "expected_edge": "0.02",
            "post_only": true,
            "tick_size": "0.01",
            "neg_risk": false
        });
        let decision_sha256 = canonical_value_sha256(&payload).unwrap();
        let batch_hash = Sha256::digest(batch_label.as_bytes());
        let object = payload.as_object_mut().unwrap();
        object.insert("decision_batch_schema_version".to_owned(), json!(3));
        object.insert(
            "strategy_batch_id".to_owned(),
            json!(format!("strategy-batch-{batch_hash:x}")),
        );
        object.insert(
            "strategy_batch_output_index".to_owned(),
            json!(output_index),
        );
        object.insert(
            "strategy_decision_sha256".to_owned(),
            json!(decision_sha256),
        );
        payload
    }

    fn applied_place(decision: &Value, order_id: &str, local_ts: DateTime<Utc>) -> Value {
        let parsed = durable_decision_output_v3(decision).unwrap();
        let application_id = application_id_v1(&parsed.key, &parsed.decision_sha256).unwrap();
        let identity = parsed.place_identity.as_ref().unwrap();
        let report = json!({
            "order_id": order_id,
            "market_id": identity.market_id,
            "token_id": identity.token_id,
            "status": "paper_resting",
            "filled_size": "0",
            "fee": "0",
            "local_ts": local_ts,
            "raw": {"decision_application": {
                "schema": "polyedge.paper_decision_output_application.v1",
                "application_id": application_id,
                "strategy_batch_id": parsed.key.batch_id,
                "strategy_batch_output_index": parsed.key.output_index,
                "strategy_decision_sha256": parsed.decision_sha256
            }}
        });
        let reports = json!([report]);
        json!({
            "schema": "polyedge.paper_decision_output_application.v1",
            "schema_version": 1,
            "application_id": application_id,
            "strategy_batch_id": parsed.key.batch_id,
            "strategy_batch_output_index": parsed.key.output_index,
            "strategy_decision_sha256": parsed.decision_sha256,
            "action": "place",
            "market_id": identity.market_id,
            "token_id": identity.token_id,
            "side": identity.side,
            "price": identity.price.to_string(),
            "size": identity.size.to_string(),
            "order_kind": "post_only_gtc",
            "order_id": order_id,
            "execution_report_count": 1,
            "execution_reports_sha256": canonical_value_sha256(&reports).unwrap(),
            "execution_reports": reports,
            "applied": true,
            "paper_only": true
        })
    }

    fn queue_fill(
        order_id: &str,
        fill_ts: DateTime<Utc>,
        fill_size: &str,
        remaining: &str,
        partial: bool,
        trade_through: bool,
    ) -> Value {
        json!({
            "order_id": order_id,
            "market_id": "market-1",
            "token_id": "token-1",
            "side": "buy",
            "quote_price": "0.50",
            "trade_ts": fill_ts,
            "shadow_fill_size": fill_size,
            "shadow_remaining_after": remaining,
            "partial_fill": partial,
            "strict_trade_through": trade_through
        })
    }

    fn markout(
        fill_id: &str,
        fill: &Value,
        fill_ts: DateTime<Utc>,
        horizon: i64,
        size: &str,
    ) -> Value {
        let pnl = (d("0.005") * d(size)).to_string();
        json!({
            "fill_id": fill_id,
            "fill_source": "queue_shadow_fill",
            "order_id": fill["order_id"],
            "market_id": fill["market_id"],
            "token_id": fill["token_id"],
            "side": fill["side"],
            "fill_price": fill["quote_price"],
            "fill_size": size,
            "fee_per_share": "0",
            "fill_ts": fill_ts,
            "horizon_seconds": horizon,
            "markout_per_share": "0.01",
            "markout_pnl": (d("0.01") * d(size)).to_string(),
            "executable_markout_per_share": "0.005",
            "executable_markout_pnl": pnl,
            "net_markout_per_share": "0.01",
            "net_markout_pnl": (d("0.01") * d(size)).to_string(),
            "net_executable_markout_per_share": "0.005",
            "net_executable_markout_pnl": (d("0.005") * d(size)).to_string(),
            "observed_ts": fill_ts + Duration::seconds(horizon) + Duration::milliseconds(3),
            "observation_delay_ms": 3
        })
    }

    fn missing_markout(fill_id: &str, fill: &Value, fill_ts: DateTime<Utc>, horizon: i64) -> Value {
        json!({
            "fill_id": fill_id,
            "fill_source": "queue_shadow_fill",
            "order_id": fill["order_id"],
            "market_id": fill["market_id"],
            "token_id": fill["token_id"],
            "side": fill["side"],
            "fill_price": fill["quote_price"],
            "fill_size": fill["shadow_fill_size"],
            "fee_per_share": "0",
            "fill_ts": fill_ts,
            "horizon_seconds": horizon,
            "reason": "fixture_missing"
        })
    }

    fn event(event_type: &str, payload: Value, recorded_ts: DateTime<Utc>) -> Value {
        json!({
            "event_type": event_type,
            "payload": payload,
            "recorded_ts": recorded_ts
        })
    }

    fn write_snapshot(root: &Path, events: &[Value]) {
        fs::create_dir_all(root).unwrap();
        let mut writer = BufWriter::new(File::create(root.join("events.jsonl")).unwrap());
        for event in events {
            serde_json::to_writer(&mut writer, event).unwrap();
            writer.write_all(b"\n").unwrap();
        }
        writer.flush().unwrap();
        let inventory = build_local_source_inventory(root, EventPathMode::PreferEventsJsonl)
            .expect("fixture inventory");
        fs::write(
            root.join("events_manifest.json"),
            serde_json::to_vec(&json!({
                "format": "jsonl-indexed",
                "decision_grade_projection": true,
                "sealed": true,
                "normalized_source_inventory_sha256": inventory.canonical_sha256,
                "raw_source_inventory": inventory
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn read_jsonl(path: &Path) -> Vec<Value> {
        BufReader::new(File::open(path).unwrap())
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .collect()
    }

    fn assert_fact_hash(row: &Value) {
        let claimed = row["fact_sha256"].as_str().unwrap();
        let mut unhashed = row.clone();
        unhashed.as_object_mut().unwrap().remove("fact_sha256");
        assert_eq!(canonical_value_sha256(&unhashed).as_deref(), Some(claimed));
    }

    fn assert_artifact_manifest(out: &Path) {
        let manifest: Value =
            serde_json::from_slice(&fs::read(out.join(ARTIFACT_MANIFEST_FILE)).unwrap()).unwrap();
        assert_eq!(manifest["schema"], ARTIFACT_MANIFEST_SCHEMA);
        let artifacts = manifest["artifacts"].as_array().unwrap();
        assert_eq!(artifacts.len(), 4);
        for artifact in artifacts {
            let filename = artifact["filename"].as_str().unwrap();
            let bytes = fs::read(out.join(filename)).unwrap();
            assert_eq!(artifact["content_length"], bytes.len());
            assert_eq!(artifact["sha256"], sha256_prefixed(&bytes));
        }
    }

    fn staging_siblings(out: &Path) -> Vec<PathBuf> {
        let parent = out.parent().unwrap();
        let prefix = format!(
            ".{}.loss-diagnostics-staging-",
            out.file_name().unwrap().to_string_lossy()
        );
        fs::read_dir(parent)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
            })
            .collect()
    }

    fn test_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "polyedge-loss-diagnostics-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn test_ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }
}
