import { artifactLocationFromUri, sha256 } from "./canary-lib.mjs";

const TARGETS = [5, 25, 100, 200];
const GRANT_SCHEMA = "funded_stage_grant_v1";
const CONSUMPTION_SCHEMA = "polyedge.funded_stage_grant_consumption.v1";

export function loadFundedLadderConfig(env = process.env) {
  const config = {
    enabled: bool(env.FUNDED_LADDER_CONTROLLER_ENABLED),
    allowed: bool(env.ALLOW_FUNDED_LADDER),
    dryRun: env.FUNDED_LADDER_DRY_RUN !== "false",
    manifestBlobName: clean(env.FUNDED_LADDER_MANIFEST_BLOB_NAME),
    manifestHash: hash(env.FUNDED_LADDER_MANIFEST_SHA256),
    shadowManifestBlobName: clean(env.FUNDED_LADDER_SHADOW_MANIFEST_BLOB_NAME || "reports/research/profitability/latest.json"),
    grantBlobName: clean(env.FUNDED_LADDER_GRANT_BLOB_NAME),
    grantHash: hash(env.FUNDED_LADDER_GRANT_SHA256),
    consumptionBlobName: clean(env.FUNDED_LADDER_CONSUMPTION_BLOB_NAME),
    consumptionHash: hash(env.FUNDED_LADDER_CONSUMPTION_SHA256),
    researchContainerName: clean(env.FUNDED_LADDER_RESEARCH_CONTAINER_NAME || "polyedge-research"),
    intentContainerName: clean(env.FUNDED_LADDER_INTENT_CONTAINER_NAME || "polyedge-shadow-events"),
    intentPrefix: clean(env.STRATEGY_CANARY_INTENT_PREFIX || "reports/research/venue-probe/control/strategy-canary/intents").replace(/^\/+|\/+$/g, ""),
    controlPrefix: clean(env.FUNDED_LADDER_CONTROL_PREFIX || "reports/research/venue-probe/control/funded-ladder").replace(/^\/+|\/+$/g, ""),
    maxOrderNotional: number(env.STRATEGY_CANARY_MAX_ORDER_NOTIONAL, 1)
  };
  const errors = [];
  if (!config.enabled) errors.push("FUNDED_LADDER_CONTROLLER_ENABLED must be true");
  if (!config.allowed) errors.push("ALLOW_FUNDED_LADDER must be true");
  if (!config.manifestBlobName || !config.manifestHash) errors.push("exact canonical manifest blob and hash are required");
  if (!config.shadowManifestBlobName) errors.push("latest research shadow manifest blob is required");
  if (Boolean(config.grantBlobName) !== Boolean(config.grantHash)) errors.push("grant blob and hash must be provided together");
  if (Boolean(config.consumptionBlobName) !== Boolean(config.consumptionHash)) errors.push("consumption blob and hash must be provided together");
  if (Boolean(config.grantBlobName) === Boolean(config.consumptionBlobName)) errors.push("provide exactly one initiation grant or durable consumption record");
  if (!(config.maxOrderNotional > 0 && config.maxOrderNotional <= 1)) errors.push("order cap must be in (0,1]");
  if (!config.researchContainerName || !config.intentContainerName) errors.push("research and intent container names are required");
  if (errors.length) throw new Error(`funded_ladder_controller blocked: ${errors.join("; ")}`);
  return config;
}

export function canonicalStateHash(state) {
  return sha256(Buffer.from(JSON.stringify(state)));
}

export function validateStageStart({ manifest, manifestHash, grant, grantHash, now = new Date() }) {
  failUnless(manifest?.schema_version === "promotion_manifest_v1", "invalid canonical manifest schema");
  failUnless(manifest?.phase === "limited_live" && manifest?.promotion_allowed === false, "canonical manifest is not non-executable limited_live");
  validateCampaignWindow(manifest, now);
  const state = manifest?.funded_ladder;
  validateActiveState(state);
  failUnless(state.human_grant_required === true && state.stage_authorized === false, "stage is not awaiting a human grant");
  failUnless(grant?.schema_version === GRANT_SCHEMA && grant.single_use === true, "invalid funded stage grant schema");
  failUnless(grant.candidate?.name === state.candidate?.name && grant.candidate?.candidate_version === state.candidate?.candidate_version && grant.candidate?.config_hash === state.candidate?.config_hash, "grant candidate mismatch");
  failUnless(Number(grant.stage_target_orders) === Number(state.active_target_orders), "grant target mismatch");
  failUnless(hash(grant.source_state_sha256) === canonicalStateHash(state), "grant source state hash mismatch");
  const authorized = Date.parse(grant.authorized_at);
  const expires = Date.parse(grant.expires_at);
  const nowMs = now.getTime();
  failUnless(Number.isFinite(authorized) && Number.isFinite(expires) && authorized <= nowMs && expires > nowMs && expires > authorized && expires - authorized <= 300_000, "grant initiation window is invalid or expired");
  failUnless(clean(grant.grant_id) && hash(manifestHash) && hash(grantHash), "grant id and exact hashes are required");
  return { state, stateHash: canonicalStateHash(state), remainingQuota: Number(state.active_target_orders) - Number(state.metrics?.cumulative_funded_orders || 0) };
}

export function buildStageConsumption({ config, manifest, manifestHash, grant, grantHash, now = new Date() }) {
  const validated = validateStageStart({ manifest, manifestHash, grant, grantHash, now });
  // Immutable stage-init artifacts must be byte-identical across retries. The
  // exact grant is already time-bounded and hash-pinned, so use its immutable
  // authorization timestamp/hash instead of retry-local wall clock/run IDs.
  const initiatedAt = new Date(grant.authorized_at);
  const authorizedManifest = authorizeCanonicalManifest(manifest, grant.grant_id, initiatedAt);
  const authorizedManifestHash = sha256(Buffer.from(JSON.stringify(authorizedManifest, null, 2)));
  const campaignControlId = sha256(Buffer.from(validated.state.campaign_id)).slice("sha256:".length);
  const deterministicRunId = `stage-init-${hash(grantHash).slice("sha256:".length, "sha256:".length + 24)}`;
  return {
    blobName: `${config.controlPrefix}/campaigns/${campaignControlId}/stage-consumptions/${grant.grant_id}.json`,
    value: {
      schema: CONSUMPTION_SCHEMA,
      grant_id: grant.grant_id,
      grant_sha256: hash(grantHash),
      canonical_manifest_blob_name: config.manifestBlobName,
      source_manifest_sha256: hash(manifestHash),
      authorized_manifest_sha256: authorizedManifestHash,
      authorized_manifest_blob_name: `${config.controlPrefix}/campaigns/${campaignControlId}/stage-manifests/${grant.grant_id}.json`,
      candidate: manifest.candidate,
      campaign_id: validated.state.campaign_id,
      campaign_control_id: campaignControlId,
      execution_model: manifest.execution_model,
      shadow_gate_phase: manifest.gate_metrics?.phase,
      shadow_gates_passed: manifest.gate_metrics?.promotion_allowed,
      source_state_sha256: validated.stateHash,
      authorized_state_sha256: canonicalStateHash(authorizedManifest.funded_ladder),
      stage_target_orders: validated.state.active_target_orders,
      starting_funded_orders: validated.state.metrics.cumulative_funded_orders,
      quota_orders: validated.remainingQuota,
      run_id: deterministicRunId,
      consumed_at: initiatedAt.toISOString()
    },
    authorizedManifest
  };
}

export function authorizeCanonicalManifest(manifest, grantId, now = new Date()) {
  const copy = structuredClone(manifest);
  const state = copy.funded_ladder;
  validateActiveState(state);
  failUnless(state.human_grant_required === true && state.stage_authorized === false, "stage cannot be authorized from current state");
  failUnless(!state.consumed_grant_ids.includes(grantId), "grant id was already consumed");
  state.human_grant_required = false;
  state.stage_authorized = true;
  state.consumed_grant_ids.push(grantId);
  state.updated_at = now.toISOString();
  copy.phase = state.phase;
  copy.promotion_allowed = false;
  return copy;
}

export function validateStageResume({ manifest, manifestHash, consumption, now = new Date() }) {
  const state = manifest?.funded_ladder;
  validateActiveState(state);
  validateCampaignWindow(manifest, now);
  failUnless(consumption?.schema === CONSUMPTION_SCHEMA, "invalid durable stage consumption");
  failUnless(consumption.campaign_id === state.campaign_id && consumption.campaign_control_id === sha256(Buffer.from(state.campaign_id)).slice("sha256:".length), "durable stage consumption campaign identity mismatch");
  failUnless(consumption.canonical_manifest_blob_name && hash(manifestHash), "current canonical manifest exact hash is missing");
  failUnless(hash(manifestHash) === hash(consumption.authorized_manifest_sha256), "current canonical manifest is not the exact authorized stage manifest");
  failUnless(JSON.stringify(consumption.candidate) === JSON.stringify(manifest.candidate) && JSON.stringify(consumption.execution_model) === JSON.stringify(manifest.execution_model) && consumption.shadow_gate_phase === manifest.gate_metrics?.phase && consumption.shadow_gates_passed === manifest.gate_metrics?.promotion_allowed, "candidate, model, or shadow-gate identity changed after stage initiation");
  failUnless(hash(consumption.authorized_state_sha256) === canonicalStateHash(state), "current authorized canonical state changed after stage initiation");
  failUnless(Number(consumption.stage_target_orders) === Number(state.active_target_orders), "consumption target mismatch");
  failUnless(Number(consumption.starting_funded_orders) + Number(consumption.quota_orders) === Number(state.active_target_orders), "consumption quota does not reconcile");
  return { state, remainingQuota: Number(consumption.quota_orders) };
}

export function validateBeforeEveryOrder({ manifest, manifestHash, consumption, completedDecisionIds, runtime, now = new Date() }) {
  const validated = validateStageResume({ manifest, manifestHash, consumption, now });
  const unique = new Set(completedDecisionIds || []);
  failUnless(unique.size === (completedDecisionIds || []).length, "duplicate durable progress records");
  failUnless(unique.size < validated.remainingQuota, "stage quota is exhausted");
  failUnless(Number(runtime?.openOrderCount) === 0, "global open order exists");
  failUnless(Number(runtime?.unresolvedExposure) === 0 && Number(runtime?.unresolvedReservations) === 0, "global unresolved exposure exists");
  failUnless(runtime?.riskPassed === true, "terminal campaign risk/portfolio gate is not passing");
  return { ...validated, completed: unique.size, remaining: validated.remainingQuota - unique.size };
}

export function buildFundedIntentAuthorization({ config, manifest, manifestHash, consumptionDocument, consumptionHash, intentDocument, childRunId, now = new Date() }) {
  const { value: intent, blobName: intentBlobName, hash: intentHash } = intentDocument;
  const expiresAt = new Date(Math.min(Date.parse(intent.valid_until), now.getTime() + 30_000));
  failUnless(expiresAt > now, "intent is stale");
  const modelArtifact = artifactLocationFromUri(manifest.execution_model.blob_uri, config.storageAccount);
  const authorization = {
    schema: "polyedge.funded_stage_intent_authorization.v1",
    authorization_id: `${consumptionDocument.value.grant_id}-${intent.decision_id.slice(0, 16)}`,
    decision_id: intent.decision_id,
    child_run_id: clean(childRunId),
    intent_blob_name: intentBlobName,
    intent_sha256: hash(intentHash),
    promotion_manifest_blob_name: config.manifestBlobName,
    promotion_manifest_sha256: hash(manifestHash),
    funded_stage_consumption_blob_name: consumptionDocument.blobName,
    funded_stage_consumption_sha256: hash(consumptionHash),
    funded_stage_source_state_sha256: consumptionDocument.value.source_state_sha256,
    funded_stage_target_orders: consumptionDocument.value.stage_target_orders,
    campaign_id: consumptionDocument.value.campaign_id,
    campaign_control_id: consumptionDocument.value.campaign_control_id,
    candidate: manifest.candidate,
    candidate_name: manifest.candidate.name,
    candidate_version: manifest.candidate.candidate_version,
    candidate_config_hash: manifest.candidate.config_hash,
    required_fill_model_version: manifest.execution_model.model_version,
    execution_model_blob_uri: manifest.execution_model.blob_uri,
    execution_model_container_name: modelArtifact.container,
    execution_model_blob_name: modelArtifact.blobName,
    execution_model_sha256: manifest.execution_model.sha256,
    human_authorization_reference: `funded-stage:${consumptionDocument.value.grant_id}`,
    authorized_at: now.toISOString(),
    expires_at: expiresAt.toISOString(),
    single_use: true
  };
  failUnless(authorization.child_run_id, "funded child run id is required before authorization");
  return { value: authorization, blobName: `${config.controlPrefix}/campaigns/${consumptionDocument.value.campaign_control_id}/intent-authorizations/${consumptionDocument.value.grant_id}/${intent.decision_id}.json`, hash: sha256(Buffer.from(JSON.stringify(authorization, null, 2))) };
}

export function buildStageBlock({ config, consumption, decisionId, childRunId, reason, now = new Date() }) {
  failUnless(clean(config?.controlPrefix), "stage block control prefix is missing");
  failUnless(consumption?.schema === CONSUMPTION_SCHEMA, "stage block consumption schema is invalid");
  failUnless(clean(consumption.grant_id) && clean(consumption.campaign_id) && clean(consumption.campaign_control_id), "stage block campaign identity is incomplete");
  failUnless(JSON.stringify(consumption.candidate || {}) !== "{}", "stage block candidate is missing");
  failUnless(Number.isInteger(Number(consumption.stage_target_orders)) && TARGETS.includes(Number(consumption.stage_target_orders)), "stage block target is invalid");
  failUnless(hash(consumption.authorized_manifest_sha256) && hash(consumption.authorized_state_sha256), "stage block exact canonical manifest/state hashes are missing");
  failUnless(clean(decisionId) && clean(reason), "stage block decision and reason are required");
  return {
    blobName: `${config.controlPrefix}/campaigns/${consumption.campaign_control_id}/stage-blocks/${consumption.grant_id}/${decisionId}.json`,
    value: {
      schema: "polyedge.funded_stage_block.v1",
      grant_id: consumption.grant_id,
      campaign_id: consumption.campaign_id,
      campaign_control_id: consumption.campaign_control_id,
      candidate: consumption.candidate,
      stage_target_orders: Number(consumption.stage_target_orders),
      source_manifest_sha256: hash(consumption.authorized_manifest_sha256),
      source_state_sha256: hash(consumption.authorized_state_sha256),
      decision_id: clean(decisionId),
      child_run_id: clean(childRunId) || null,
      reason: clean(reason),
      blocked_at: now.toISOString()
    }
  };
}

export function validateProtocolV3ChildSummary({ summary, consumption, decisionId }) {
  const probes = Array.isArray(summary?.probes) ? summary.probes : [];
  const probe = probes[0];
  const observations = Array.isArray(probe?.model_observations) ? probe.model_observations : [];
  const provenance = summary?.provenance || {};
  const eligible = observations.filter((row) => row.eligible === true && row.quality_eligible === true && row.reconciliation_complete === true && row.zero_open_orders_confirmed === true && row.data_gap_detected !== true && row.cancellation_failure !== true && row.markout_complete === true && row.markout_timing_valid === true);
  failUnless(summary?.schema_version === 3 && summary?.evidence_protocol_version === 3, "child summary is not protocol-v3");
  failUnless(summary?.order_submission_attempted === true && Number(summary?.submitted_order_count) === 1 && probes.length === 1, "child did not submit exactly one bounded order");
  failUnless(provenance.authorization_kind === "funded_stage" && provenance.decision_id === decisionId, "child provenance is not funded-stage intent-bound");
  failUnless(provenance.funded_stage_grant_id === consumption.grant_id && hash(provenance.funded_stage_grant_sha256) === hash(consumption.grant_sha256) && hash(provenance.funded_stage_consumption_sha256) && hash(provenance.funded_stage_source_state_sha256) === hash(consumption.source_state_sha256) && Number(provenance.funded_stage_target_orders) === Number(consumption.stage_target_orders), "child provenance does not bind durable stage control");
  failUnless(eligible.length > 0, "submitted child order is ineligible; stage must stop without replacement");
  return { runId: summary.run_id, probeId: probe.probe_id, orderId: probe.lifecycle?.order_id, eligibleObservationCount: eligible.length, filled: Number(probe.lifecycle?.actual_matched_size || 0) > 0 };
}

export function validateProtocolV3ChildEvidence({ summary, terminal, consumption, decisionId }) {
  const validated = validateProtocolV3ChildSummary({ summary, consumption, decisionId });
  failUnless(terminal?.schema === "polyedge.canary_terminal_risk_portfolio.v1" && terminal.portfolio_reconciled === true && terminal.zero_open_orders_confirmed === true && Number(terminal.unresolved_exposure) === 0 && terminal.run_id === validated.runId && terminal.probe_id === validated.probeId && terminal.order_id === validated.orderId, "child terminal risk/portfolio evidence is incomplete or identity-mismatched");
  return validated;
}

export function buildFundedCheckpointEvidence({ manifest, entries }) {
  const state = manifest?.funded_ladder;
  validateActiveState(state);
  const target = Number(state.active_target_orders);
  failUnless(Array.isArray(entries) && entries.length === target, "checkpoint requires exact cumulative sequence count");
  const ordered = [...entries].sort((left, right) => Number(left.sequence) - Number(right.sequence));
  const identities = new Set();
  const baselines = new Set();
  const rows = ordered.map((entry, index) => {
    failUnless(Number(entry.sequence) === index + 1, "checkpoint sequence is missing, duplicated, or non-contiguous");
    const summary = entry.summary;
    const terminal = entry.terminal;
    const probes = Array.isArray(summary?.probes) ? summary.probes : [];
    const probe = probes[0];
    const identity = `${summary?.run_id || ""}\u0000${probe?.probe_id || ""}\u0000${probe?.lifecycle?.order_id || ""}`;
    failUnless(summary?.schema_version === 3 && summary?.evidence_protocol_version === 3 && summary?.status === "completed"
      && summary?.order_submission_attempted === true && summary?.order_submitted === true
      && Number(summary?.submitted_order_count) === 1 && Number(summary?.completed_probe_count) === 1 && probes.length === 1,
    "checkpoint summary is not exactly one completed protocol-v3 funded order");
    failUnless(JSON.stringify(summary?.candidate) === JSON.stringify(state.candidate), "checkpoint candidate mismatch");
    const observations = Array.isArray(probe?.model_observations) ? probe.model_observations : [];
    failUnless(observations.length > 0 && observations.every((row) => row.eligible === true && row.quality_eligible === true
      && row.reconciliation_complete === true && row.zero_open_orders_confirmed === true
      && row.data_gap_detected !== true && row.cancellation_failure !== true), "checkpoint contains ineligible lifecycle evidence");
    failUnless(identity.replaceAll("\u0000", "") && !identities.has(identity), "checkpoint run/probe/order identity is missing or duplicated");
    identities.add(identity);
    failUnless(terminal?.schema === "polyedge.canary_terminal_risk_portfolio.v1"
      && terminal.run_id === summary.run_id && terminal.probe_id === probe.probe_id && terminal.order_id === probe.lifecycle?.order_id
      && terminal.portfolio_reconciled === true && terminal.zero_open_orders_confirmed === true
      && Number(terminal.unresolved_exposure) === 0 && Number(terminal.reconciliation_discrepancy) <= 0.01,
    "checkpoint terminal evidence is incomplete or identity-mismatched");
    const baseline = finite(terminal.campaign_starting_equity, "campaign_starting_equity");
    const cashFlows = finite(terminal.net_external_cash_flows, "net_external_cash_flows");
    const ending = finite(terminal.cash_flow_adjusted_ending_equity, "cash_flow_adjusted_ending_equity");
    baselines.add(baseline);
    const matched = finite(probe.lifecycle?.actual_matched_size, "actual_matched_size");
    const markouts = Array.isArray(probe.markouts) ? probe.markouts : [];
    let netMarkout = null;
    if (matched > 0) {
      const thirty = markouts.filter((row) => Number(row.horizon_seconds) === 30);
      const size = thirty.reduce((sum, row) => sum + finite(row.fill_size, "fill_size"), 0);
      failUnless(size > 0 && Math.abs(size - matched) <= 1e-8, "checkpoint 30-second markouts do not reconcile to matched size");
      netMarkout = thirty.reduce((sum, row) => sum + finite(row.executable_markout_per_share, "executable_markout_per_share") * finite(row.fill_size, "fill_size"), 0) / size;
    } else {
      failUnless(markouts.length === 0, "no-fill checkpoint order contains markouts");
    }
    return {
      started: Date.parse(summary.started_ts), observed: Date.parse(terminal.observed_at), baseline, cashFlows, ending,
      pnl: stableNumber(ending - baseline - cashFlows), netMarkout: netMarkout === null ? null : stableNumber(netMarkout),
      summaryBinding: exactBinding(entry.summaryBinding, "summary"), terminalBinding: exactBinding(entry.terminalBinding, "terminal")
    };
  });
  failUnless(baselines.size === 1 && rows.every((row) => Number.isFinite(row.started) && Number.isFinite(row.observed) && row.observed >= row.started), "checkpoint chronology or campaign baseline is inconsistent");
  rows.sort((left, right) => left.observed - right.observed);
  let peak = rows[0].baseline;
  let drawdown = 0;
  for (const row of rows) {
    const adjusted = row.ending - row.cashFlows;
    peak = Math.max(peak, adjusted);
    drawdown = Math.max(drawdown, peak - adjusted);
  }
  const markouts = rows.flatMap((row) => row.netMarkout === null ? [] : [row.netMarkout]);
  const mean = markouts.length ? markouts.reduce((sum, value) => sum + value, 0) / markouts.length : 0;
  const lower95 = markouts.length < 2 ? 0 : mean - 1.96 * Math.sqrt(markouts.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (markouts.length - 1) / markouts.length);
  const firstStarted = Math.min(...rows.map((row) => row.started));
  const latest = rows.at(-1);
  return {
    schema_version: "funded_checkpoint_evidence_v1", evidence_protocol_version: 3,
    candidate: state.candidate, source_state_sha256: canonicalStateHash(state), stage_target_orders: target,
    exact_eligible_order_count: target, exact_funded_order_count: target,
    observed_calendar_days: Math.floor((new Date(latest.observed).setUTCHours(0, 0, 0, 0) - new Date(firstStarted).setUTCHours(0, 0, 0, 0)) / 86_400_000) + 1,
    cumulative_net_pnl: latest.pnl, cumulative_max_drawdown: stableNumber(drawdown),
    mean_net_markout_30s: stableNumber(mean), net_markout_30s_lower_95: stableNumber(lower95), markout_sample_size: markouts.length,
    data_quality_passed: true, unresolved_exposure: 0, lifecycle_reconciled: true,
    protocol_v3_order_artifacts: ordered.map((entry) => exactBinding(entry.summaryBinding, "summary")),
    terminal_risk_portfolio_artifacts: ordered.map((entry) => exactBinding(entry.terminalBinding, "terminal"))
  };
}

function exactBinding(binding, label) {
  failUnless(clean(binding?.blob_name) && hash(binding?.sha256), `checkpoint ${label} binding is invalid`);
  return { blob_name: clean(binding.blob_name), sha256: hash(binding.sha256) };
}

function finite(value, label) {
  const parsed = Number(value);
  failUnless(Number.isFinite(parsed), `checkpoint ${label} is invalid`);
  return parsed;
}
function stableNumber(value) { return Number(Number(value).toFixed(12)); }

function validateCampaignWindow(manifest, now) {
  const created = Date.parse(manifest?.created_at);
  const expires = Date.parse(manifest?.expires_at);
  const nowMs = now.getTime();
  failUnless(Number.isFinite(created) && created <= nowMs && Number.isFinite(expires) && expires > nowMs && expires > created && expires - created <= 60 * 86_400_000,
    "canonical funded campaign validity window is missing, invalid, or expired");
}

export async function putImmutableJson(container, document) {
  const bytes = Buffer.from(JSON.stringify(document.value, null, 2));
  try {
    await container.getBlockBlobClient(document.blobName).uploadData(bytes, { conditions: { ifNoneMatch: "*" }, blobHTTPHeaders: { blobContentType: "application/json" } });
  } catch (error) {
    if ([409, 412].includes(Number(error.statusCode))) throw new Error(`fail closed: immutable control artifact already exists (${document.blobName})`);
    throw error;
  }
  return { ...document, hash: sha256(bytes) };
}

function validateActiveState(state) {
  failUnless(state?.schema_version === "funded_ladder_state_v1" && !state.terminal, "funded ladder is absent or terminal");
  failUnless(TARGETS.includes(Number(state.active_target_orders)), "active target is not one of 5/25/100/200");
  failUnless(state.phase === "limited_live" && state.promotion_allowed === false, "funded ladder is not non-executable limited_live");
}
function failUnless(ok, message) { if (!ok) throw new Error(`fail closed: ${message}`); }
function clean(value) { return String(value || "").trim(); }
function hash(value) { const v = clean(value).toLowerCase(); const p = v.startsWith("sha256:") ? v : `sha256:${v}`; return /^sha256:[0-9a-f]{64}$/.test(p) ? p : ""; }
function bool(value) { return clean(value).toLowerCase() === "true"; }
function number(value, fallback) { const n = Number(value); return Number.isFinite(n) ? n : fallback; }
