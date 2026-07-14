import {
  artifactLocationFromUri,
  canonicalMarkoutBookHash,
  canonicalMarkoutBookSnapshot,
  polymarketV2FeePerShare,
  sha256
} from "./canary-lib.mjs";

const TARGETS = [5, 25, 100, 200];
const GRANT_SCHEMA = "funded_stage_grant_v1";
const CONSUMPTION_SCHEMA = "polyedge.funded_stage_grant_consumption.v1";
const LABEL_HORIZONS_SECONDS = [1, 5, 30, 60];
const MARKOUT_HORIZONS_SECONDS = [1, 5, 30];
const MAX_MARKOUT_DELAY_MS = 2_000;
const MAX_CLOCK_UNCERTAINTY_MS = 750;
const MIN_STABLE_FINALITY_OBSERVATION_MS = 10_000;
const SIZE_EPSILON = 1e-8;

export function loadFundedLadderConfig(env = process.env) {
  const config = {
    enabled: bool(env.FUNDED_LADDER_CONTROLLER_ENABLED),
    allowed: bool(env.ALLOW_FUNDED_LADDER),
    dryRun: env.FUNDED_LADDER_DRY_RUN !== "false",
    trustBoundaryReady: bool(env.FUNDED_EVIDENCE_TRUST_BOUNDARY_READY),
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
  if (!config.dryRun && !config.trustBoundaryReady) errors.push("FUNDED_EVIDENCE_TRUST_BOUNDARY_READY must be true only after signer/control isolation");
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

export function checkpointOneChainRoot(state) {
  validateActiveState(state);
  return sha256(Buffer.from(JSON.stringify({
    schema: "polyedge.funded_checkpoint_1_chain_root.v1",
    campaign_id: state.campaign_id,
    candidate: state.candidate,
    sequence: 1,
    protocol_v3_summary: exactBinding(state.checkpoint_1_protocol_v3_artifact, "checkpoint-1 summary"),
    terminal_risk_portfolio: exactBinding(state.checkpoint_1_terminal_artifact, "checkpoint-1 terminal")
  })));
}

export function progressPayloadHash({ sequence, decisionId, expectedControlBinding, summaryBinding, terminalBinding }) {
  const payload = {
    schema: "polyedge.funded_stage_progress_payload.v1",
    sequence: nonNegativeInteger(sequence, "progress sequence"),
    decision_id: clean(decisionId),
    expected_control_binding: exactParentControlBinding(expectedControlBinding),
    protocol_v3_summary: exactBinding(summaryBinding, "summary"),
    terminal_risk_portfolio: exactBinding(terminalBinding, "terminal")
  };
  failUnless(payload.sequence >= 2 && payload.decision_id, "progress payload sequence or decision is invalid");
  return sha256(Buffer.from(JSON.stringify(payload)));
}

export function cumulativeProgressHash(priorHash, progressHash) {
  failUnless(hash(priorHash) && hash(progressHash), "progress chain hashes are invalid");
  return sha256(Buffer.from(JSON.stringify({ prior: hash(priorHash), progress: hash(progressHash) })));
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
      checkpoint_1_chain_root_sha256: checkpointOneChainRoot(validated.state),
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
  failUnless(hash(consumption.checkpoint_1_chain_root_sha256) === checkpointOneChainRoot(state),
    "durable stage consumption does not preserve the canonical checkpoint-1 progress-chain root");
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

export function validateProtocolV3ChildSummary({ summary, consumption, decisionId, expectedBinding }) {
  const probes = Array.isArray(summary?.probes) ? summary.probes : [];
  const probe = probes[0];
  const provenance = summary?.provenance || {};
  failUnless(summary?.schema_version === 3 && summary?.evidence_protocol_version === 3, "child summary is not protocol-v3");
  failUnless(summary?.status === "completed" && summary?.order_submission_attempted === true && summary?.order_submitted === true
    && Number(summary?.submitted_order_count) === 1 && Number(summary?.completed_probe_count) === 1 && probes.length === 1,
  "child did not complete exactly one bounded submitted order");
  const parent = exactParentControlBinding(expectedBinding);
  failUnless(summary.run_id === parent.child_run_id
    && provenance.authorization_blob_name === parent.authorization_blob_name
    && hash(provenance.authorization_sha256) === parent.authorization_sha256
    && provenance.intent_blob_name === parent.intent_blob_name
    && hash(provenance.intent_sha256) === parent.intent_sha256
    && provenance.promotion_manifest_blob_name === parent.manifest_blob_name
    && hash(provenance.promotion_manifest_sha256) === parent.manifest_sha256
    && provenance.funded_stage_consumption_blob_name === parent.consumption_blob_name
    && hash(provenance.funded_stage_consumption_sha256) === parent.consumption_sha256
    && parent.manifest_sha256 === hash(consumption.authorized_manifest_sha256),
  "child summary does not match the exact loaded parent control artifacts");
  failUnless(provenance.authorization_kind === "funded_stage" && provenance.decision_id === decisionId, "child provenance is not funded-stage intent-bound");
  failUnless(provenance.funded_stage_grant_id === consumption.grant_id && hash(provenance.funded_stage_grant_sha256) === hash(consumption.grant_sha256) && hash(provenance.funded_stage_consumption_sha256) && hash(provenance.funded_stage_source_state_sha256) === hash(consumption.source_state_sha256) && Number(provenance.funded_stage_target_orders) === Number(consumption.stage_target_orders), "child provenance does not bind durable stage control");
  const startedAt = timestamp(summary.started_ts, "summary.started_ts");
  const finishedAt = timestamp(summary.finished_ts, "summary.finished_ts");
  failUnless(finishedAt >= startedAt, "child summary finished before it started");
  failUnless(JSON.stringify(summary.candidate) === JSON.stringify(consumption.candidate), "child candidate does not match canonical funded-stage control");
  const expectedModel = consumption.execution_model || {};
  const model = summary.prediction_model || {};
  failUnless(model.blob_uri === expectedModel.blob_uri && hash(model.sha256) === hash(expectedModel.sha256)
    && model.model_version === expectedModel.model_version
    && parent.prediction_model.blob_uri === expectedModel.blob_uri
    && parent.prediction_model.sha256 === hash(expectedModel.sha256)
    && parent.prediction_model.model_version === expectedModel.model_version,
  "child prediction model does not match canonical funded-stage control");
  const generatedAt = timestamp(model.generated_at, "prediction_model.generated_at");
  failUnless(generatedAt < startedAt, "child prediction model is not an immutable temporal prior");
  if (model.training_data_end_ts !== null && model.training_data_end_ts !== undefined && model.training_data_end_ts !== "") {
    failUnless(timestamp(model.training_data_end_ts, "prediction_model.training_data_end_ts") < startedAt,
      "child prediction model training data includes the funded order");
  }
  const validated = validateSubmittedProtocolV3Probe(probe, finishedAt);
  return {
    runId: summary.run_id, probeId: probe.probe_id, orderId: probe.lifecycle.order_id,
    eligibleObservationCount: validated.observationCount, filled: validated.matched > 0, finishedAt,
    conditionId: clean(probe.market?.conditionId), settlementWallet: clean(summary.funder_address || probe.funder_address)
  };
}

export function validateProtocolV3ChildEvidence({ summary, terminal, consumption, decisionId, expectedBinding }) {
  const validated = validateProtocolV3ChildSummary({ summary, consumption, decisionId, expectedBinding });
  validateTerminalEvidence(terminal, validated, validated.finishedAt);
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
    const parent = exactParentControlBinding(entry.expectedControlBinding);
    const provenance = summary?.provenance || {};
    const model = summary?.prediction_model || {};
    failUnless(summary.run_id === parent.child_run_id
      && provenance.funded_stage_consumption_blob_name === parent.consumption_blob_name
      && hash(provenance.funded_stage_consumption_sha256) === parent.consumption_sha256
      && provenance.authorization_blob_name === parent.authorization_blob_name
      && hash(provenance.authorization_sha256) === parent.authorization_sha256
      && provenance.intent_blob_name === parent.intent_blob_name
      && hash(provenance.intent_sha256) === parent.intent_sha256
      && provenance.promotion_manifest_blob_name === parent.manifest_blob_name
      && hash(provenance.promotion_manifest_sha256) === parent.manifest_sha256
      && model.blob_uri === parent.prediction_model.blob_uri
      && hash(model.sha256) === parent.prediction_model.sha256
      && model.model_version === parent.prediction_model.model_version,
    "checkpoint summary does not revalidate its exact parent control/model binding");
    failUnless(identity.replaceAll("\u0000", "") && !identities.has(identity), "checkpoint run/probe/order identity is missing or duplicated");
    identities.add(identity);
    const finishedAt = timestamp(summary.finished_ts, "summary.finished_ts");
    const lifecycle = validateSubmittedProtocolV3Probe(probe, finishedAt);
    validateTerminalEvidence(terminal, {
      runId: summary.run_id,
      probeId: probe.probe_id,
      orderId: probe.lifecycle.order_id,
      filled: lifecycle.matched > 0,
      conditionId: clean(probe.market?.conditionId),
      settlementWallet: clean(summary.funder_address || probe.funder_address)
    }, finishedAt);
    const baseline = finite(terminal.campaign_starting_equity, "campaign_starting_equity");
    const cashFlows = finite(terminal.net_external_cash_flows, "net_external_cash_flows");
    const ending = finite(terminal.cash_flow_adjusted_ending_equity, "cash_flow_adjusted_ending_equity");
    baselines.add(baseline);
    const matched = lifecycle.matched;
    const netMarkout = lifecycle.netMarkout30;
    let progressBinding = null;
    let progress = null;
    if (index > 0) {
      progressBinding = exactBinding(entry.progressBinding, "progress");
      progress = entry.progress;
      failUnless(progress?.schema === "polyedge.funded_stage_order_progress.v1"
        && Number(progress.sequence) === index + 1
        && progress.campaign_id === state.campaign_id
        && JSON.stringify(progress.candidate) === JSON.stringify(state.candidate)
        && progress.protocol_v3_summary_blob_name === entry.summaryBinding?.blob_name
        && hash(progress.protocol_v3_summary_sha256) === hash(entry.summaryBinding?.sha256)
        && progress.terminal_evidence_blob_name === entry.terminalBinding?.blob_name
        && hash(progress.terminal_evidence_sha256) === hash(entry.terminalBinding?.sha256)
        && JSON.stringify(exactParentControlBinding(progress.expected_control_binding)) === JSON.stringify(parent),
      "checkpoint progress does not retain the exact order/control evidence bindings");
    }
    return {
      started: Date.parse(summary.started_ts), observed: Date.parse(terminal.observed_at), baseline, cashFlows, ending,
      pnl: stableNumber(ending - baseline - cashFlows), netMarkout: netMarkout === null ? null : stableNumber(netMarkout),
      summaryBinding: exactBinding(entry.summaryBinding, "summary"), terminalBinding: exactBinding(entry.terminalBinding, "terminal"),
      progressBinding, progress, parent
    };
  });
  let cumulative = checkpointOneChainRoot(state);
  for (const [index, row] of rows.entries()) {
    if (index === 0) continue;
    const sequence = index + 1;
    const expectedPayload = progressPayloadHash({
      sequence,
      decisionId: row.progress.decision_id,
      expectedControlBinding: row.parent,
      summaryBinding: row.summaryBinding,
      terminalBinding: row.terminalBinding
    });
    const expectedCumulative = cumulativeProgressHash(cumulative, expectedPayload);
    failUnless(hash(row.progress.progress_payload_sha256) === expectedPayload
      && hash(row.progress.prior_cumulative_evidence_sha256) === cumulative
      && hash(row.progress.cumulative_evidence_sha256) === expectedCumulative,
    "checkpoint cumulative progress hash chain is invalid");
    cumulative = expectedCumulative;
  }
  const sequenceRows = [...rows];
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
    checkpoint_1_chain_root_sha256: checkpointOneChainRoot(state),
    final_cumulative_evidence_sha256: cumulative,
    protocol_v3_order_artifacts: ordered.map((entry) => exactBinding(entry.summaryBinding, "summary")),
    terminal_risk_portfolio_artifacts: ordered.map((entry) => exactBinding(entry.terminalBinding, "terminal")),
    progress_artifacts: sequenceRows.slice(1).map((row) => row.progressBinding),
    control_bindings: sequenceRows.map((row) => row.parent)
  };
}

function validateSubmittedProtocolV3Probe(probe, summaryFinishedAt) {
  failUnless(probe?.schema_version === 3 && probe?.evidence_protocol_version === 3
    && probe?.status === "completed" && probe?.order_submitted === true,
  "submitted probe is not exact completed protocol-v3 evidence");
  const lifecycle = probe.lifecycle || {};
  const context = probe.pre_send_context || {};
  const order = probe.order || {};
  const sendWallMs = nonNegative(lifecycle.send_wall_ms, "send_wall_ms");
  const ackWallMs = nonNegative(lifecycle.ack_wall_ms, "ack_wall_ms");
  const ackLatencyMs = nonNegative(lifecycle.client_to_http_ack_ms, "client_to_http_ack_ms");
  const clockOffsetMs = finite(lifecycle.clock_server_minus_local_ms, "clock_server_minus_local_ms");
  nonNegative(lifecycle.clock_round_trip_ms, "clock_round_trip_ms");
  const clockUncertaintyMs = nonNegative(lifecycle.clock_uncertainty_ms, "clock_uncertainty_ms");
  failUnless(clockUncertaintyMs <= MAX_CLOCK_UNCERTAINTY_MS, "clock uncertainty exceeds 750ms");
  failUnless(ackWallMs >= sendWallMs && Math.abs(ackWallMs - sendWallMs - ackLatencyMs) <= 10,
    "acknowledgement chronology is invalid");
  failUnless(context.source === "public_market_channel_before_submission", "pre-send context source is not authenticated public-market provenance");
  const capturedWallMs = nonNegative(context.captured_wall_ms, "pre_send_context.captured_wall_ms");
  failUnless(capturedWallMs <= sendWallMs, "pre-send context was captured after order submission");
  for (const field of ["observed_trade_count", "observed_trade_size", "observed_depth_changes", "price_volatility"]) {
    nonNegative(context[field], `pre_send_context.${field}`);
  }

  const matched = nonNegative(lifecycle.actual_matched_size, "actual_matched_size");
  const orderSize = positive(order.size, "order.size");
  failUnless(matched <= orderSize + SIZE_EPSILON, "actual matched size exceeds submitted order size");
  const venueFeeRate = nonNegative(lifecycle.venue_fee_rate, "venue_fee_rate");
  const venueFeeRateBps = nonNegative(lifecycle.venue_fee_rate_bps, "venue_fee_rate_bps");
  const venueFeeExponent = nonNegative(lifecycle.venue_fee_exponent, "venue_fee_exponent");
  failUnless(lifecycle.venue_fee_model === "polymarket_clob_v2_curve"
    && venueFeeRate <= 1 && venueFeeRateBps <= 10_000
    && nearlyEqual(venueFeeRate * 10_000, venueFeeRateBps)
    && venueFeeExponent <= 10
    && (venueFeeRate === 0 || lifecycle.venue_fee_taker_only === true),
  "lifecycle does not bind exact Polymarket V2 fee rate/exponent/taker-only economics");
  const roundTripCostPerShare = nonNegative(lifecycle.estimated_round_trip_cost_per_share, "estimated_round_trip_cost_per_share");
  failUnless(venueFeeRateBps === 0 || matched <= SIZE_EPSILON || roundTripCostPerShare > 0,
    "a fee-bearing fill cannot default funded cost to zero");
  const sourceSizes = ["rest_order_matched_size", "user_order_matched_size", "rest_trade_matched_size", "user_trade_matched_size"]
    .map((field) => nonNegative(lifecycle[field], field));
  failUnless(sourceSizes.every((value) => nearlyEqual(value, matched)), "REST/user matched-size sources do not independently reconcile");
  failUnless(lifecycle.matched_size_source_agreement === true, "matched-size source agreement was not asserted");

  const relatedTradeIds = exactIdSet(lifecycle.related_trade_ids, "related_trade_ids");
  const userTradeIds = exactIdSet(lifecycle.live_user_trade_ids, "live_user_trade_ids");
  failUnless(sameSet(relatedTradeIds, userTradeIds), "REST/user trade IDs do not independently reconcile");
  failUnless(lifecycle.trade_id_source_agreement === true, "trade-ID source agreement was not asserted");
  failUnless((matched > 0) === (relatedTradeIds.size > 0), "matched size and authenticated trade IDs disagree");

  const liveDurationMs = nonNegative(lifecycle.live_duration_ms, "live_duration_ms");
  const firstFillAfterAckMs = lifecycle.first_fill_after_ack_ms === null
    ? null
    : finite(lifecycle.first_fill_after_ack_ms, "first_fill_after_ack_ms");
  failUnless((matched > 0) === (firstFillAfterAckMs !== null), "matched size and first-fill timing disagree");
  const partialFill = matched > SIZE_EPSILON && matched < orderSize - SIZE_EPSILON;
  const fullyFilled = matched >= orderSize - SIZE_EPSILON;
  failUnless(lifecycle.partial_fill === partialFill && lifecycle.fully_filled === fullyFilled, "partial/full-fill claims do not match raw size evidence");

  const cancelSendWallMs = lifecycle.cancel_send_wall_ms;
  if (cancelSendWallMs === null) {
    failUnless(fullyFilled && lifecycle.client_cancel_round_trip_ms === null && lifecycle.client_to_user_cancel_ack_ms === null,
      "cancel latency may be absent only for an independently verified terminal full fill");
  } else {
    const cancelSend = nonNegative(cancelSendWallMs, "cancel_send_wall_ms");
    failUnless(cancelSend >= sendWallMs, "cancel was sent before the order");
    const cancelResponse = nonNegative(lifecycle.cancel_http_response_wall_ms, "cancel_http_response_wall_ms");
    const userCancelReceived = nonNegative(lifecycle.user_channel_cancel_received_wall_ms, "user_channel_cancel_received_wall_ms");
    const cancelRoundTrip = nonNegative(lifecycle.client_cancel_round_trip_ms, "client_cancel_round_trip_ms");
    const userCancelLatency = nonNegative(lifecycle.client_to_user_cancel_ack_ms, "client_to_user_cancel_ack_ms");
    failUnless(cancelResponse >= cancelSend && userCancelReceived >= cancelSend
      && Math.abs(cancelResponse - cancelSend - cancelRoundTrip) <= 10
      && Math.abs(userCancelReceived - cancelSend - userCancelLatency) <= 1,
    "cancel acknowledgement chronology contradicts raw timestamps");
  }
  for (const field of ["public_touch_trade_count", "public_strict_trade_through_count", "public_trade_through_without_fill_count",
    "post_cancel_fill_count",
    "authenticated_user_channel_reconnects", "public_market_channel_reconnects",
    "authenticated_user_channel_unparsed", "public_market_channel_unparsed",
    "authenticated_user_channel_duplicates", "public_market_channel_duplicates"]) {
    nonNegativeInteger(lifecycle[field], field);
  }
  failUnless(Number(lifecycle.public_trade_through_without_fill_count) <= Number(lifecycle.public_strict_trade_through_count),
    "trade-through-without-fill count exceeds strict trade-through count");
  failUnless(Number(lifecycle.authenticated_user_channel_reconnects) === 0
    && Number(lifecycle.public_market_channel_reconnects) === 0
    && Number(lifecycle.authenticated_user_channel_unparsed) === 0
    && Number(lifecycle.public_market_channel_unparsed) === 0,
  "reconnect or unparsed channel evidence creates an unclosed data-gap risk");
  failUnless(lifecycle.rest_order_returned === true && lifecycle.post_cancel_finality_stable === true
    && nonNegative(lifecycle.post_cancel_observation_ms, "post_cancel_observation_ms") >= MIN_STABLE_FINALITY_OBSERVATION_MS
    && lifecycle.reconciliation_complete === true && lifecycle.zero_open_orders_confirmed === true
    && lifecycle.data_gap_detected === false && lifecycle.cancellation_failure === false,
  "lifecycle lacks stable, zero-open, data-gap-free finality");

  const markouts = validateMarkouts({
    markouts: probe.markouts,
    relatedTradeIds,
    matched,
    tokenId: String(probe.market?.tokenId || ""),
    sendWallMs,
    summaryFinishedAt,
    clockOffsetMs,
    clockUncertaintyMs,
    feeParameters: {
      rate: venueFeeRate,
      rateBps: venueFeeRateBps,
      exponent: venueFeeExponent,
      takerOnly: lifecycle.venue_fee_taker_only === true
    }
  });
  failUnless(lifecycle.markout_capture_complete === true && markouts.complete, "markout capture is incomplete");
  failUnless(nearlyEqual(roundTripCostPerShare, markouts.roundTripCost30),
    "claimed round-trip cost does not equal independently derived venue fees");
  const derivedFirstFillAfterAckMs = markouts.fillTimestamps.size
    ? Math.min(...markouts.fillTimestamps.values()) - ackWallMs
    : null;
  failUnless((derivedFirstFillAfterAckMs === null && firstFillAfterAckMs === null)
    || (derivedFirstFillAfterAckMs !== null && firstFillAfterAckMs !== null
      && Math.abs(derivedFirstFillAfterAckMs - firstFillAfterAckMs) <= 1),
  "first-fill timing contradicts authenticated per-fill timestamps");
  const postCancelDelays = cancelSendWallMs === null
    ? []
    : [...markouts.fillTimestamps.values()].map((fillAt) => {
      const delay = fillAt - Number(cancelSendWallMs);
      failUnless(Math.abs(delay) > clockUncertaintyMs, "fill/cancel ordering is ambiguous within clock uncertainty");
      return delay;
    }).filter((delay) => delay > 0);
  const derivedFirstFillAfterCancelMs = postCancelDelays.length ? Math.min(...postCancelDelays) : null;
  failUnless(Number(lifecycle.post_cancel_fill_count) === postCancelDelays.length
    && lifecycle.fill_raced_cancellation === (postCancelDelays.length > 0)
    && ((derivedFirstFillAfterCancelMs === null && lifecycle.first_fill_after_cancel_ms === null)
      || (derivedFirstFillAfterCancelMs !== null
        && Math.abs(nonNegative(lifecycle.first_fill_after_cancel_ms, "first_fill_after_cancel_ms") - derivedFirstFillAfterCancelMs) <= 1)),
  "fill/cancel race evidence contradicts per-fill authenticated timestamps");
  const observations = Array.isArray(probe.model_observations) ? probe.model_observations : [];
  failUnless(observations.length === LABEL_HORIZONS_SECONDS.length, "model evidence must contain exactly 1/5/30/60-second labels");
  const inferredSizeAhead = nonNegative(order.inferredSizeAhead, "order.inferredSizeAhead");
  const orderSpread = optionalFinite(order.spread, "order.spread");
  failUnless(order.side === "BUY", "funded protocol-v3 economics require a BUY order");
  const orderPrice = boundedPrice(order.price, "order.price", true);
  const endTs = probe.market?.endTs === null || probe.market?.endTs === undefined || probe.market?.endTs === ""
    ? null
    : timestamp(probe.market.endTs, "market.endTs");
  const timeToExpiry = endTs === null ? null : Math.max(0, endTs - ackWallMs) / 1_000;
  for (const horizon of LABEL_HORIZONS_SECONDS) {
    const rows = observations.filter((row) => Number(row.horizon_seconds) === horizon);
    failUnless(rows.length === 1, `model evidence requires exactly one ${horizon}-second label`);
    const row = rows[0];
    if (derivedFirstFillAfterAckMs !== null) {
      failUnless(Math.abs(derivedFirstFillAfterAckMs - horizon * 1_000) > clockUncertaintyMs,
        `${horizon}-second fill label is ambiguous within clock uncertainty`);
    }
    const derivedFilled = derivedFirstFillAfterAckMs !== null && derivedFirstFillAfterAckMs <= horizon * 1_000;
    // Stable terminal finality, independent source agreement, and a globally
    // zero-open account make every later fill label observable even when the
    // strategy intentionally cancels before that horizon.
    const derivedObserved = liveDurationMs >= horizon * 1_000 || derivedFilled
      || lifecycle.post_cancel_finality_stable === true;
    failUnless(derivedObserved && row.label_observed === true && row.filled === derivedFilled
      && row.order_submitted === true && row.eligible === true && row.quality_eligible === true
      && row.reconciliation_complete === true && row.zero_open_orders_confirmed === true
      && row.data_gap_detected === false && row.cancellation_failure === false
      && row.markout_complete === true && row.markout_timing_valid === true,
    `${horizon}-second label is incomplete or contradicts raw lifecycle evidence`);
    failUnless(nearlyEqual(nonNegative(row.pre_send_trade_size, "pre_send_trade_size"), Number(context.observed_trade_size))
      && nearlyEqual(nonNegative(row.pre_send_depth_changes, "pre_send_depth_changes"), Number(context.observed_depth_changes))
      && nearlyEqual(nonNegative(row.pre_send_volatility, "pre_send_volatility"), Number(context.price_volatility)),
    `${horizon}-second label does not preserve pre-send context`);
    failUnless(nearlyEqual(nonNegative(row.inferred_size_ahead, "inferred_size_ahead"), inferredSizeAhead)
      && sameOptionalFinite(row.spread, orderSpread, "spread")
      && nearlyEqual(positive(row.order_price, "order_price"), orderPrice)
      && nearlyEqual(positive(row.order_size, "order_size"), orderSize)
      && sameOptionalFinite(row.time_to_expiry_seconds, timeToExpiry, "time_to_expiry_seconds"),
    `${horizon}-second label does not preserve raw order/market features`);
    const claimedMarkout30 = row.executable_markout_30s_per_share === null
      ? null
      : finite(row.executable_markout_30s_per_share, "executable_markout_30s_per_share");
    failUnless((markouts.netMarkout30 === null && claimedMarkout30 === null)
      || (markouts.netMarkout30 !== null && claimedMarkout30 !== null && nearlyEqual(markouts.netMarkout30, claimedMarkout30)),
    `${horizon}-second label does not bind the derived 30-second executable markout`);
    failUnless(row.venue_fee_model === "polymarket_clob_v2_curve"
      && nearlyEqual(nonNegative(row.venue_fee_rate, "model.venue_fee_rate"), venueFeeRate)
      && nearlyEqual(nonNegative(row.venue_fee_rate_bps, "model.venue_fee_rate_bps"), venueFeeRateBps)
      && nearlyEqual(nonNegative(row.venue_fee_exponent, "model.venue_fee_exponent"), venueFeeExponent)
      && row.venue_fee_taker_only === lifecycle.venue_fee_taker_only
      && nearlyEqual(nonNegative(row.entry_fee_per_share, "model.entry_fee_per_share"), markouts.entryFee30)
      && nearlyEqual(nonNegative(row.hypothetical_exit_fee_per_share, "model.hypothetical_exit_fee_per_share"), markouts.exitFee30)
      && nearlyEqual(nonNegative(row.estimated_round_trip_cost_per_share, "model.estimated_round_trip_cost_per_share"), markouts.roundTripCost30),
    `${horizon}-second label does not bind actual venue cost evidence`);
  }
  return {
    matched, observationCount: observations.length,
    netMarkout30: markouts.netMarkout30 === null ? null : markouts.netMarkout30 - markouts.roundTripCost30
  };
}

function validateMarkouts({ markouts, relatedTradeIds, matched, tokenId, sendWallMs, summaryFinishedAt, clockOffsetMs, clockUncertaintyMs, feeParameters }) {
  const rows = Array.isArray(markouts) ? markouts : [];
  if (matched <= SIZE_EPSILON) {
    failUnless(relatedTradeIds.size === 0 && rows.length === 0, "no-fill order contains fill/markout evidence");
    return { complete: true, fillTimestamps: new Map(), netMarkout30: null, entryFee30: 0, exitFee30: 0, roundTripCost30: 0 };
  }
  failUnless(rows.length === relatedTradeIds.size * MARKOUT_HORIZONS_SECONDS.length,
    "each authenticated fill requires exactly one 1/5/30-second markout triplet");
  let thirtySecondSize = 0;
  let thirtySecondExecutableMarkout = 0;
  let thirtySecondEntryFee = 0;
  let thirtySecondExitFee = 0;
  let thirtySecondRoundTripCost = 0;
  const fillTimestamps = new Map();
  const fillSizes = new Map();
  const fillPrices = new Map();
  const fillFeeEvidence = new Map();
  for (const fillId of relatedTradeIds) {
    for (const horizon of MARKOUT_HORIZONS_SECONDS) {
      const matches = rows.filter((row) => row.fill_id === fillId && Number(row.horizon_seconds) === horizon);
      failUnless(matches.length === 1, `fill ${fillId} does not have exactly one ${horizon}-second markout`);
      const row = matches[0];
      const delay = nonNegative(row.observation_delay_ms, "observation_delay_ms");
      const fillSize = positive(row.fill_size, "fill_size");
      const fillPrice = boundedPrice(row.fill_price, "fill_price");
      const rawBook = row.raw_orderbook;
      failUnless(rawBook && String(rawBook.token_id) === tokenId, "markout raw orderbook token binding is missing or wrong");
      const canonicalRawBook = canonicalMarkoutBookSnapshot(rawBook, tokenId);
      failUnless(JSON.stringify(rawBook) === JSON.stringify(canonicalRawBook)
        && canonicalMarkoutBookHash(canonicalRawBook) === hash(row.book_hash),
      "markout raw orderbook is not canonical or disagrees with book_hash");
      failUnless(/^[0-9a-f]{40}$/i.test(canonicalRawBook.venue_hash || "")
        && canonicalRawBook.venue_hash === clean(row.venue_book_hash),
      "markout venue hash is not bound by the immutable raw orderbook");
      const bidPrices = validatedBookPrices(canonicalRawBook.bids, "bid");
      const askPrices = validatedBookPrices(canonicalRawBook.asks, "ask");
      failUnless(bidPrices.length > 0 && askPrices.length > 0, "markout raw orderbook lacks executable two-sided depth");
      const recomputedBestBid = Math.max(...bidPrices);
      const recomputedBestAsk = Math.min(...askPrices);
      failUnless(recomputedBestBid < recomputedBestAsk, "markout raw orderbook is crossed or locked");
      const recomputedMidpoint = (recomputedBestBid + recomputedBestAsk) / 2;
      const midpoint = boundedPrice(row.midpoint, "midpoint");
      const executablePrice = boundedPrice(row.executable_price, "executable_price");
      failUnless(nearlyEqual(midpoint, recomputedMidpoint) && nearlyEqual(executablePrice, recomputedBestBid),
        "markout midpoint or executable BUY price disagrees with raw orderbook levels");
      const midpointMarkout = finite(row.midpoint_markout_per_share, "midpoint_markout_per_share");
      const executableMarkout = finite(row.executable_markout_per_share, "executable_markout_per_share");
      failUnless(nearlyEqual(midpointMarkout, midpoint - fillPrice)
        && nearlyEqual(executableMarkout, executablePrice - fillPrice),
      "claimed BUY markout does not equal observed price minus authenticated fill price");
      const traderSide = clean(row.trader_side).toUpperCase();
      const orderRole = clean(row.authenticated_order_role).toUpperCase();
      const authenticatedFeeRateBps = optionalFinite(row.authenticated_fee_rate_bps, "authenticated_fee_rate_bps");
      const authenticatedFeeAmount = optionalFinite(row.authenticated_fee_amount, "authenticated_fee_amount");
      failUnless(["MAKER", "TAKER"].includes(traderSide) || feeParameters.rate === 0,
        "fee-bearing authenticated fill is missing trader_side");
      failUnless(!["MAKER", "TAKER"].includes(orderRole) || traderSide === orderRole,
        "authenticated trader_side contradicts the order's matched role");
      if (feeParameters.rate > 0) {
        failUnless(authenticatedFeeRateBps !== null && (traderSide === "TAKER"
          ? nearlyEqual(authenticatedFeeRateBps, feeParameters.rateBps)
          : nearlyEqual(authenticatedFeeRateBps, 0) || nearlyEqual(authenticatedFeeRateBps, feeParameters.rateBps)),
        "authenticated fill fee_rate_bps disagrees with market fee parameters");
      }
      failUnless(authenticatedFeeAmount === null || authenticatedFeeAmount >= 0,
        "authenticated fill fee amount is negative");
      failUnless(traderSide !== "MAKER" || authenticatedFeeAmount === null || authenticatedFeeAmount <= 1e-12,
        "post-only maker fill reports a nonzero authenticated fee amount");
      const curveEntryFee = traderSide === "TAKER"
        ? polymarketV2FeePerShare(fillPrice, feeParameters.rate, feeParameters.exponent)
        : 0;
      const reportedEntryFee = traderSide !== "TAKER" || authenticatedFeeAmount === null
        ? 0
        : authenticatedFeeAmount / fillSize;
      const entryFee = Math.max(curveEntryFee, reportedEntryFee);
      const exitFee = polymarketV2FeePerShare(executablePrice, feeParameters.rate, feeParameters.exponent);
      const roundTripFee = entryFee + exitFee;
      failUnless(nearlyEqual(nonNegative(row.entry_fee_per_share, "entry_fee_per_share"), entryFee)
        && nearlyEqual(nonNegative(row.hypothetical_exit_fee_per_share, "hypothetical_exit_fee_per_share"), exitFee)
        && nearlyEqual(nonNegative(row.round_trip_fee_per_share, "round_trip_fee_per_share"), roundTripFee),
      "claimed markout fees do not equal independently recomputed Polymarket V2 economics");
      const feeFingerprint = JSON.stringify({ traderSide, orderRole, authenticatedFeeRateBps, authenticatedFeeAmount });
      const fillAt = timestamp(row.fill_timestamp, "fill_timestamp");
      const venueFillAt = timestamp(row.venue_fill_timestamp, "venue_fill_timestamp");
      failUnless(Math.abs(fillAt - (venueFillAt - clockOffsetMs)) <= 1,
        "normalized fill timestamp contradicts venue timestamp and measured clock offset");
      failUnless(fillAt >= sendWallMs - clockUncertaintyMs, "authenticated fill timestamp predates order submission beyond clock uncertainty");
      if (fillTimestamps.has(fillId)) {
        failUnless(fillTimestamps.get(fillId) === fillAt, "a fill has inconsistent authenticated timestamps across markouts");
        failUnless(nearlyEqual(fillSizes.get(fillId), fillSize), "a fill has inconsistent sizes across markouts");
        failUnless(nearlyEqual(fillPrices.get(fillId), fillPrice), "a fill has inconsistent prices across markouts");
        failUnless(fillFeeEvidence.get(fillId) === feeFingerprint, "a fill has inconsistent authenticated fee evidence across markouts");
      } else {
        fillTimestamps.set(fillId, fillAt);
        fillSizes.set(fillId, fillSize);
        fillPrices.set(fillId, fillPrice);
        fillFeeEvidence.set(fillId, feeFingerprint);
      }
      const targetAt = timestamp(row.target_observation_ts, "target_observation_ts");
      const requestStartedAt = timestamp(row.request_started_at, "request_started_at");
      const responseCompletedAt = timestamp(row.response_completed_at, "response_completed_at");
      const observedAt = timestamp(row.observed_at, "observed_at");
      const responseDuration = nonNegative(row.response_duration_ms, "response_duration_ms");
      failUnless(hash(row.book_hash), "markout book_hash is missing or invalid");
      if (row.venue_book_timestamp !== null && row.venue_book_timestamp !== undefined && row.venue_book_timestamp !== "") {
        const venueBookAt = timestamp(row.venue_book_timestamp, "venue_book_timestamp");
        const venueBookAtLocal = venueBookAt - clockOffsetMs;
        failUnless(venueBookAtLocal <= responseCompletedAt + clockUncertaintyMs
          && responseCompletedAt - venueBookAtLocal <= MAX_MARKOUT_DELAY_MS + clockUncertaintyMs,
          "venue order book is stale or future-dated relative to the markout response");
      }
      failUnless(Math.abs(targetAt - fillAt - horizon * 1_000) <= 1
        && targetAt <= requestStartedAt && requestStartedAt <= responseCompletedAt
        && observedAt === responseCompletedAt
        && Math.abs(responseCompletedAt - requestStartedAt - responseDuration) <= 1
        && Math.abs(responseCompletedAt - targetAt - delay) <= 1
        && delay <= MAX_MARKOUT_DELAY_MS && observedAt <= summaryFinishedAt,
      "markout timing does not match its raw timestamps or allowed delay");
      if (horizon === 30) {
        thirtySecondSize += fillSize;
        thirtySecondExecutableMarkout += (executablePrice - fillPrice) * fillSize;
        thirtySecondEntryFee += entryFee * fillSize;
        thirtySecondExitFee += exitFee * fillSize;
        thirtySecondRoundTripCost += roundTripFee * fillSize;
      }
    }
  }
  failUnless(nearlyEqual(thirtySecondSize, matched), "30-second markout sizes do not reconcile to matched size");
  return {
    complete: true,
    fillTimestamps,
    netMarkout30: thirtySecondExecutableMarkout / thirtySecondSize,
    entryFee30: thirtySecondEntryFee / thirtySecondSize,
    exitFee30: thirtySecondExitFee / thirtySecondSize,
    roundTripCost30: thirtySecondRoundTripCost / thirtySecondSize
  };
}

function validatedBookPrices(levels, label) {
  failUnless(Array.isArray(levels), `markout raw ${label} levels are missing`);
  return levels.map((level) => {
    const price = boundedPrice(level?.price, `${label}.price`, true);
    positive(level?.size, `${label}.size`);
    return price;
  });
}

function validateTerminalEvidence(terminal, identity, summaryFinishedAt) {
  const discrepancy = nonNegative(terminal?.reconciliation_discrepancy, "reconciliation_discrepancy");
  const baseline = nonNegative(terminal?.campaign_starting_equity, "campaign_starting_equity");
  const cashFlows = finite(terminal?.net_external_cash_flows, "net_external_cash_flows");
  const liquid = nonNegative(terminal?.liquid_collateral, "liquid_collateral");
  const positions = nonNegative(terminal?.summed_position_value, "summed_position_value");
  const ending = nonNegative(terminal?.cash_flow_adjusted_ending_equity, "cash_flow_adjusted_ending_equity");
  const minimum = nonNegative(terminal?.minimum_observed_equity, "minimum_observed_equity");
  const maximum = nonNegative(terminal?.maximum_observed_equity, "maximum_observed_equity");
  const calculatedDiscrepancy = Math.abs(liquid + positions - ending);
  failUnless(terminal?.schema === "polyedge.canary_terminal_risk_portfolio.v1"
    && terminal.producer === "polyedge_node_authenticated_risk_terminal"
    && terminal.settlement_verified === true && terminal.portfolio_reconciled === true
    && terminal.zero_open_orders_confirmed === true && terminal.trust_boundary_ready === true
    && Number(terminal.unresolved_exposure) === 0
    && Number(terminal.unresolved_risk_reservations) === 0
    && clean(identity.conditionId) && terminal.condition_id === identity.conditionId
    && discrepancy <= 0.01 && calculatedDiscrepancy <= 0.01 && Math.abs(calculatedDiscrepancy - discrepancy) <= 0.01
    && Number.isFinite(baseline + cashFlows) && maximum >= minimum
    && terminal.run_id === identity.runId && terminal.probe_id === identity.probeId && terminal.order_id === identity.orderId,
  "child terminal risk/portfolio evidence is incomplete or identity-mismatched");
  failUnless(timestamp(terminal.observed_at, "terminal.observed_at") >= summaryFinishedAt,
    "terminal evidence predates completion of the child summary");
  if (identity.filled === false) {
    failUnless(terminal.source === "authenticated_no_fill", "no-fill terminal source is invalid");
  } else if (identity.filled === true) {
    const redeemedConditions = exactIdSet(
      (terminal.redemption_condition_ids || []).map((value) => String(value).toLowerCase()),
      "redemption_condition_ids"
    );
    failUnless(terminal.source === "polymarket_data_api_plus_onchain_redemption"
      && /^0x[0-9a-fA-F]{64}$/.test(terminal.settlement_transaction_hash || "")
      && Number(terminal.polygon_chain_id) === 137 && terminal.transaction_receipt_status === "success"
      && Number.isInteger(Number(terminal.transaction_block_number)) && Number(terminal.transaction_block_number) > 0
      && Number.isInteger(Number(terminal.transaction_receipt_confirmations))
      && Number(terminal.transaction_receipt_confirmations) >= 2
      && redeemedConditions.has(String(identity.conditionId).toLowerCase())
      && /^0x[0-9a-fA-F]{40}$/.test(identity.settlementWallet || "")
      && String(terminal.settlement_wallet || "").toLowerCase() === identity.settlementWallet.toLowerCase(),
      "filled terminal evidence lacks authenticated settlement/redemption proof");
  }
}

function exactBinding(binding, label) {
  failUnless(clean(binding?.blob_name) && hash(binding?.sha256), `checkpoint ${label} binding is invalid`);
  return { blob_name: clean(binding.blob_name), sha256: hash(binding.sha256) };
}

function exactParentControlBinding(binding) {
  const model = binding?.prediction_model || {};
  const value = {
    child_run_id: clean(binding?.child_run_id),
    consumption_blob_name: clean(binding?.consumption_blob_name),
    consumption_sha256: hash(binding?.consumption_sha256),
    authorization_blob_name: clean(binding?.authorization_blob_name),
    authorization_sha256: hash(binding?.authorization_sha256),
    intent_blob_name: clean(binding?.intent_blob_name),
    intent_sha256: hash(binding?.intent_sha256),
    manifest_blob_name: clean(binding?.manifest_blob_name),
    manifest_sha256: hash(binding?.manifest_sha256),
    prediction_model: {
      blob_uri: clean(model.blob_uri),
      sha256: hash(model.sha256),
      model_version: clean(model.model_version)
    }
  };
  failUnless(value.child_run_id && value.consumption_blob_name && value.consumption_sha256
    && value.authorization_blob_name && value.authorization_sha256
    && value.intent_blob_name && value.intent_sha256
    && value.manifest_blob_name && value.manifest_sha256
    && value.prediction_model.blob_uri && value.prediction_model.sha256
    && value.prediction_model.model_version,
  "exact parent control/model binding is missing");
  return value;
}

function finite(value, label) {
  failUnless(value !== null && value !== undefined && value !== "" && typeof value !== "boolean", `checkpoint ${label} is invalid`);
  const parsed = Number(value);
  failUnless(Number.isFinite(parsed), `checkpoint ${label} is invalid`);
  return parsed;
}
function nonNegative(value, label) {
  const parsed = finite(value, label);
  failUnless(parsed >= 0, `${label} is negative`);
  return parsed;
}
function positive(value, label) {
  const parsed = finite(value, label);
  failUnless(parsed > 0, `${label} must be positive`);
  return parsed;
}
function boundedPrice(value, label, strictlyPositive = false) {
  const parsed = finite(value, label);
  failUnless(parsed >= 0 && parsed <= 1 && (!strictlyPositive || parsed > 0), `${label} must be ${strictlyPositive ? "in (0,1]" : "in [0,1]"}`);
  return parsed;
}
function nonNegativeInteger(value, label) {
  const parsed = nonNegative(value, label);
  failUnless(Number.isInteger(parsed), `${label} must be an integer`);
  return parsed;
}
function exactIdSet(value, label) {
  failUnless(Array.isArray(value), `${label} must be an array`);
  const ids = value.map((item) => clean(item));
  failUnless(ids.every(Boolean) && new Set(ids).size === ids.length, `${label} contains an empty or duplicate ID`);
  return new Set(ids);
}
function sameSet(left, right) {
  return left.size === right.size && [...left].every((value) => right.has(value));
}
function nearlyEqual(left, right) { return Math.abs(left - right) <= SIZE_EPSILON; }
function timestamp(value, label) {
  const parsed = Date.parse(value);
  failUnless(Number.isFinite(parsed), `${label} is invalid`);
  return parsed;
}
function optionalFinite(value, label) {
  return value === null || value === undefined || value === "" ? null : finite(value, label);
}
function sameOptionalFinite(value, expected, label) {
  const actual = optionalFinite(value, label);
  return actual === null || expected === null ? actual === expected : nearlyEqual(actual, expected);
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
