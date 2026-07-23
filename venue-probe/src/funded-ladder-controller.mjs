import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { storageContainer, sanitize } from "./lib.mjs";
import { loadHashedJson, sha256 } from "./canary-lib.mjs";
import { selectFirstQualifiedIntent } from "./canary-controller-lib.mjs";
import {
  buildFundedIntentAuthorization,
  buildFundedCheckpointEvidence,
  buildStageBlock,
  buildStageConsumption,
  cumulativeProgressHash,
  loadFundedLadderConfig,
  progressPayloadHash,
  putImmutableJson,
  validateBeforeEveryOrder,
  validateProtocolV3ChildEvidence,
  validateProtocolV3ChildSummary,
  validateStageResume
} from "./funded-ladder-controller-lib.mjs";

export async function runFundedLadderController({ env = process.env, invokeChild = invokeCanaryChild, containers, clock = () => new Date() } = {}) {
  const config = loadFundedLadderConfig(env);
  config.storageAccount = env.AZURE_STORAGE_ACCOUNT_NAME;
  config.storageContainer = env.AZURE_STORAGE_CONTAINER_NAME || "bot-events";
  config.storageAccountKey = env.AZURE_STORAGE_ACCOUNT_KEY;
  config.azureClientId = env.AZURE_CLIENT_ID;
  const clients = containers || {
    control: storageContainer(config),
    research: storageContainer({ ...config, storageContainer: config.researchContainerName }),
    intents: storageContainer({ ...config, storageContainer: config.intentContainerName })
  };
  if (!clients.control || !clients.research || !clients.intents) throw new Error("fail closed: all three isolated Azure storage containers are required");
  let manifestDocument = config.grantBlobName
    ? await loadJsonWithEtag(clients.control, config.manifestBlobName)
    : await loadHashedJsonWithEtag(clients.control, config.manifestBlobName, config.manifestHash);
  const shadowManifest = await loadJsonUntrustedHash(clients.research, config.shadowManifestBlobName);
  assertShadowGateFresh(shadowManifest.value, manifestDocument.value, clock());
  let consumptionDocument;
  let grantHash;
  if (config.grantBlobName) {
    const grantDocument = await loadHashedJson(clients.control, config.grantBlobName, config.grantHash);
    if (manifestDocument.hash === normalizeHash(config.manifestHash)) {
      const built = buildStageConsumption({ config, manifest: manifestDocument.value, manifestHash: manifestDocument.hash, grant: grantDocument.value, grantHash: grantDocument.hash, now: clock() });
      if (config.dryRun) {
        return { status: "funded_stage_dry_run_validated", stage_target_orders: built.value.stage_target_orders, writes: 0, child_invocations: 0, funded_execution_armed: false };
      }
      if (!manifestDocument.etag) throw new Error("fail closed: canonical manifest ETag is required for stage authorization CAS");
      consumptionDocument = await putImmutableOrVerify(clients.control, { blobName: built.blobName, value: built.value });
      await putImmutableOrVerify(clients.control, { blobName: built.value.authorized_manifest_blob_name, value: built.authorizedManifest });
      manifestDocument = await putCanonicalManifestIfMatch(
        clients.control,
        config.manifestBlobName,
        built.authorizedManifest,
        manifestDocument.etag
      );
    } else {
      const recovered = await recoverStageInitialization(clients.control, config, manifestDocument, grantDocument, clock());
      consumptionDocument = recovered.consumptionDocument;
      manifestDocument = recovered.manifestDocument;
    }
    grantHash = grantDocument.hash;
  } else {
    consumptionDocument = await loadHashedJson(clients.control, config.consumptionBlobName, config.consumptionHash);
    grantHash = consumptionDocument.value.grant_sha256;
  }
  validateStageResume({ manifest: manifestDocument.value, manifestHash: manifestDocument.hash, consumption: consumptionDocument.value, now: clock() });
  if (config.dryRun) {
    return { status: "funded_stage_dry_run_validated", stage_target_orders: consumptionDocument.value.stage_target_orders, writes: 0, child_invocations: 0, funded_execution_armed: false };
  }
  await assertNoStageBlock(clients.control, config, consumptionDocument.value);
  const inventory = await loadStageInventory(clients.control, config, consumptionDocument.value);
  if (inventory.orphanAuthorizations.length) {
    const orphan = inventory.orphanAuthorizations[0];
    await writeStageBlock(clients.control, config, consumptionDocument.value, orphan.decision_id, orphan.child_run_id, "authorization exists without completed or pending evidence; replacement spending is forbidden", clock());
    throw new Error("fail closed: orphan funded authorization durably blocked the stage");
  }
  if (inventory.pending.length) {
    const result = await settlePendingTerminal(clients.control, config, consumptionDocument, inventory.pending[0], inventory.completed, clock());
    if (result.status === "funded_stage_order_completed" && result.remaining === 0) {
      result.checkpoint = await publishStageCheckpoint(clients.control, config, manifestDocument.value);
    }
    return { ...result, attempted: inventory.authorizations.length, submitted: inventory.completed.length + inventory.pending.length, eligible: inventory.completed.length + (result.status === "funded_stage_order_completed" ? 1 : 0), funded_execution_armed: false };
  }
  const stageQuota = Number(consumptionDocument.value.quota_orders);
  if (inventory.completed.length > stageQuota) {
    throw new Error("fail closed: completed funded progress exceeds the durable stage quota");
  }
  if (inventory.completed.length === stageQuota) {
    const checkpoint = await publishStageCheckpoint(clients.control, config, manifestDocument.value);
    return {
      status: "funded_stage_checkpoint_recovered",
      attempted: inventory.authorizations.length,
      submitted: inventory.completed.length,
      eligible: inventory.completed.length,
      remaining: 0,
      checkpoint,
      funded_execution_armed: false
    };
  }
  const priorTerminal = await loadPriorTerminalGate(clients.control, manifestDocument.value, inventory.completed);
  const runtimeGate = terminalRuntimeGate(priorTerminal.value);
  const attempted = inventory.authorizations.map((row) => row.decision_id);
  const quota = validateBeforeEveryOrder({ manifest: manifestDocument.value, manifestHash: manifestDocument.hash, consumption: consumptionDocument.value, completedDecisionIds: attempted, runtime: runtimeGate, now: clock() });
  const sourceIntent = await firstFreshIntent(clients.intents, config, manifestDocument.value, consumptionDocument.value, new Set(attempted), clock());
  if (!sourceIntent) return { status: "stage_waiting_for_fresh_intent", attempted: attempted.length, submitted: inventory.completed.length, eligible: inventory.completed.length, remaining: quota.remaining, funded_execution_armed: false };
  const intent = await copyIntentToControl(clients.control, config, consumptionDocument.value, sourceIntent);
  const childRunId = runId(`funded-${consumptionDocument.value.stage_target_orders}`);
  const authorization = await putImmutableJson(clients.control, buildFundedIntentAuthorization({
    config, manifest: manifestDocument.value, manifestHash: manifestDocument.hash,
    consumptionDocument, consumptionHash: consumptionDocument.hash, intentDocument: intent, childRunId, now: clock()
  }));
  let exitCode;
  try {
    exitCode = await invokeChild(fundedChildEnvironment(env, {
      config, manifestDocument, consumptionDocument, grantHash, authorization, intent, childRunId
    }));
  } catch (error) {
    await writeStageBlock(clients.control, config, consumptionDocument.value, intent.value.decision_id, childRunId, `funded child crashed: ${error.message}`, clock());
    throw error;
  }
  if (exitCode !== 0) {
    await writeStageBlock(clients.control, config, consumptionDocument.value, intent.value.decision_id, childRunId, `funded child exited ${exitCode}`, clock());
    throw new Error(`fail closed: funded-stage canary child exited ${exitCode}`);
  }
  try {
    const summary = await locateChildSummary(clients.control, childRunId);
    const expectedBinding = exactChildControlBinding({ manifestDocument, consumptionDocument, authorization, intent, childRunId });
    const summaryEvidence = validateProtocolV3ChildSummary({
      summary: summary.value,
      consumption: consumptionDocument.value,
      decisionId: intent.value.decision_id,
      expectedBinding
    });
    const boundTerminalName = summary.value?.provenance?.terminal_evidence_blob_name;
    const boundTerminalHash = summary.value?.provenance?.terminal_evidence_sha256;
    if (!boundTerminalName || !boundTerminalHash) {
      if (!summaryEvidence.filled) throw new Error("no-fill child lacks its immediate exact terminal evidence binding");
      const pending = await putImmutableJson(clients.control, {
        blobName: `${campaignControlPrefix(config, consumptionDocument.value)}/pending-terminal/${consumptionDocument.value.grant_id}/${intent.value.decision_id}.json`,
        value: pendingProgress(consumptionDocument.value, intent, authorization, childRunId, summary, summaryEvidence, expectedBinding, clock())
      });
      return { status: "funded_stage_pending_terminal", attempted: attempted.length + 1, submitted: inventory.completed.length + 1, eligible: inventory.completed.length, remaining: quota.remaining - 1, pending_sha256: pending.hash, funded_execution_armed: false };
    }
    const terminal = await loadHashedJson(clients.control, boundTerminalName, boundTerminalHash);
    validateProtocolV3ChildEvidence({
      summary: summary.value,
      terminal: terminal.value,
      consumption: consumptionDocument.value,
      decisionId: intent.value.decision_id,
      expectedBinding
    });
    const completion = await recordCompletion(clients.control, config, consumptionDocument.value, intent, authorization, childRunId, summary, terminal, inventory.completed, expectedBinding, clock());
    const result = { status: "funded_stage_order_completed", attempted: attempted.length + 1, submitted: inventory.completed.length + 1, eligible: inventory.completed.length + 1, remaining: quota.remaining - 1, progress_sha256: completion.hash, funded_execution_armed: false };
    if (result.remaining === 0) result.checkpoint = await publishStageCheckpoint(clients.control, config, manifestDocument.value);
    return result;
  } catch (error) {
    await writeStageBlock(clients.control, config, consumptionDocument.value, intent.value.decision_id, childRunId, error.message, clock());
    throw error;
  }
}

async function loadHashedJsonWithEtag(container, blobName, expectedHash) {
  const document = await loadJsonWithEtag(container, blobName);
  if (document.hash !== normalizeHash(expectedHash)) throw new Error("fail closed: canonical manifest SHA-256 mismatch");
  return document;
}

async function loadJsonWithEtag(container, blobName) {
  const response = await container.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  const actual = sha256(bytes);
  return { value: JSON.parse(bytes.toString("utf8")), hash: actual, blobName, etag: response.etag };
}

function terminalRuntimeGate(value) {
  const valid = value?.schema === "polyedge.canary_terminal_risk_portfolio.v1"
    && value.portfolio_reconciled === true && value.zero_open_orders_confirmed === true
    && Number(value.unresolved_exposure) === 0 && Number(value.unresolved_risk_reservations || 0) === 0;
  return { openOrderCount: valid ? 0 : 1, unresolvedExposure: Number(value?.unresolved_exposure ?? 1), unresolvedReservations: Number(value?.unresolved_risk_reservations ?? 1), riskPassed: valid };
}

async function assertNoStageBlock(container, config, consumption) {
  for await (const _blob of container.listBlobsFlat({ prefix: `${campaignControlPrefix(config, consumption)}/stage-blocks/${consumption.grant_id}/` })) {
    throw new Error("fail closed: stage is durably blocked by prior submitted ineligible evidence");
  }
}

async function loadJsonUntrustedHash(container, blobName) {
  const response = await container.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  return { value: JSON.parse(bytes.toString("utf8")), hash: sha256(bytes), blobName };
}

async function firstFreshIntent(container, config, manifest, consumption, completed, now) {
  const candidates = [];
  for await (const blob of container.listBlobsFlat({ prefix: `${config.intentPrefix}/` })) {
    if (!blob.name.endsWith(".json")) continue;
    const response = await container.getBlobClient(blob.name).download();
    const bytes = await streamToBuffer(response.readableStreamBody);
    let value;
    try { value = JSON.parse(bytes.toString("utf8")); } catch { continue; }
    if (!completed.has(value.decision_id)) candidates.push({ value, blobName: blob.name, hash: sha256(bytes) });
  }
  return selectFirstQualifiedIntent({
    config: {
      intentPrefix: config.intentPrefix, candidateName: manifest.candidate.name,
      candidateVersion: manifest.candidate.candidate_version, candidateConfigHash: manifest.candidate.config_hash,
      requiredFillModelVersion: manifest.execution_model.model_version,
      requiredResolutionSource: "chainlink_reference", maxOrderNotional: config.maxOrderNotional
    },
    grant: {
      authorized_at: consumption.consumed_at, max_order_notional: config.maxOrderNotional,
      execution_model_blob_uri: manifest.execution_model.blob_uri,
      execution_model_sha256: manifest.execution_model.sha256
    }, candidates, now
  });
}

function assertShadowGateFresh(shadow, canonical, now) {
  const nowMs = now.getTime();
  const created = Date.parse(shadow?.created_at);
  const expires = Date.parse(shadow?.expires_at);
  const valid = shadow?.schema_version === "promotion_manifest_v1"
    && shadow?.phase === "shadow_passed" && shadow?.gate_metrics?.phase === "shadow_passed"
    && shadow?.gate_metrics?.promotion_allowed === true && shadow?.promotion_allowed === false
    && JSON.stringify(shadow?.candidate) === JSON.stringify(canonical?.candidate)
    && Number.isFinite(created) && created <= nowMs && Number.isFinite(expires) && expires > nowMs;
  if (!valid) throw new Error("fail closed: latest read-only research shadow gate is stale, failing, or candidate-mismatched");
}

async function putImmutableOrVerify(container, document) {
  const bytes = Buffer.from(JSON.stringify(document.value, null, 2));
  const expected = sha256(bytes);
  try {
    return await putImmutableJson(container, document);
  } catch (error) {
    if (!/already exists/.test(error.message)) throw error;
    const existing = await loadJsonUntrustedHash(container, document.blobName);
    if (existing.hash !== expected) throw new Error(`fail closed: immutable control artifact conflicts (${document.blobName})`);
    return existing;
  }
}

async function putCanonicalManifestIfMatch(container, blobName, value, etag) {
  const bytes = Buffer.from(JSON.stringify(value, null, 2));
  const hash = sha256(bytes);
  try {
    const response = await container.getBlockBlobClient(blobName).uploadData(bytes, {
      conditions: { ifMatch: etag },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
    return { value, hash, blobName, etag: response.etag };
  } catch (error) {
    if (error.statusCode === 412) {
      const current = await loadJsonWithEtag(container, blobName);
      if (current.hash === hash) return current;
      throw new Error("fail closed: canonical manifest authorization CAS lost to a concurrent state change");
    }
    throw error;
  }
}

async function recoverStageInitialization(container, config, manifestDocument, grantDocument, now) {
  const campaignId = String(manifestDocument.value?.funded_ladder?.campaign_id || "");
  const campaignControlId = sha256(Buffer.from(campaignId)).slice("sha256:".length);
  const consumptionBlobName = `${config.controlPrefix}/campaigns/${campaignControlId}/stage-consumptions/${grantDocument.value.grant_id}.json`;
  let consumptionDocument;
  try {
    consumptionDocument = await loadJsonUntrustedHash(container, consumptionBlobName);
  } catch (error) {
    throw new Error(`fail closed: canonical manifest changed and exact stage initialization cannot be recovered (${error.message})`);
  }
  const consumption = consumptionDocument.value;
  const exactGrantBinding = consumption?.grant_id === grantDocument.value.grant_id
    && normalizeHash(consumption?.grant_sha256) === grantDocument.hash
    && normalizeHash(consumption?.source_manifest_sha256) === normalizeHash(config.manifestHash)
    && normalizeHash(consumption?.source_state_sha256) === normalizeHash(grantDocument.value.source_state_sha256)
    && Number(consumption?.stage_target_orders) === Number(grantDocument.value.stage_target_orders)
    && consumption?.canonical_manifest_blob_name === config.manifestBlobName
    && JSON.stringify(consumption?.candidate) === JSON.stringify(grantDocument.value.candidate);
  if (!campaignId || !exactGrantBinding) {
    throw new Error("fail closed: recovered stage initialization is not exactly bound to the pinned grant and source state");
  }
  const authorizedManifest = await loadHashedJson(
    container,
    consumption.authorized_manifest_blob_name,
    consumption.authorized_manifest_sha256
  );
  if (authorizedManifest.hash !== manifestDocument.hash) {
    throw new Error("fail closed: canonical manifest authorization CAS lost to a concurrent state change");
  }
  validateStageResume({
    manifest: authorizedManifest.value,
    manifestHash: authorizedManifest.hash,
    consumption,
    now
  });
  return { consumptionDocument, manifestDocument };
}

async function loadStageInventory(container, config, consumption) {
  const base = campaignControlPrefix(config, consumption);
  const grantId = consumption.grant_id;
  const [authorizations, completed, pending] = await Promise.all([
    listJson(container, `${base}/intent-authorizations/${grantId}/`),
    listJson(container, `${base}/progress/${grantId}/`),
    listJson(container, `${base}/pending-terminal/${grantId}/`)
  ]);
  const completedIds = new Set(completed.map((row) => row.value.decision_id));
  const pendingIds = new Set(pending.map((row) => row.value.decision_id));
  const orphanAuthorizations = authorizations
    .map((row) => row.value)
    .filter((row) => !completedIds.has(row.decision_id) && !pendingIds.has(row.decision_id));
  completed.sort((a, b) => String(a.value.completed_at).localeCompare(String(b.value.completed_at)) || a.blobName.localeCompare(b.blobName));
  return { authorizations: authorizations.map((row) => row.value), completed, pending: pending.filter((row) => !completedIds.has(row.value.decision_id)), orphanAuthorizations };
}

async function listJson(container, prefix) {
  const rows = [];
  for await (const blob of container.listBlobsFlat({ prefix })) {
    if (blob.name.endsWith(".json")) rows.push(await loadJsonUntrustedHash(container, blob.name));
  }
  return rows;
}

async function loadPriorTerminalGate(container, manifest, completed) {
  const latest = completed.at(-1)?.value;
  const binding = latest ? {
    blob_name: latest.terminal_evidence_blob_name,
    sha256: latest.terminal_evidence_sha256
  } : manifest?.funded_ladder?.last_verified_terminal_artifact;
  if (!binding?.blob_name || !binding?.sha256) {
    throw new Error("fail closed: canonical funded state lacks the last verified terminal risk/portfolio binding");
  }
  return loadHashedJson(container, binding.blob_name, binding.sha256);
}

async function copyIntentToControl(container, config, consumption, source) {
  const blobName = `${campaignControlPrefix(config, consumption)}/intent-copies/${consumption.grant_id}/${source.value.decision_id}.json`;
  const copied = await putImmutableOrVerify(container, { blobName, value: source.value });
  if (copied.hash !== source.hash) throw new Error("fail closed: isolated intent copy does not preserve the immutable source hash");
  return { ...copied, sourceBlobName: source.blobName, sourceHash: source.hash };
}

async function locateChildSummary(container, childRunId) {
  const matches = [];
  for await (const blob of container.listBlobsFlat({ prefix: "reports/research/venue-probe/runs/" })) {
    if (blob.name.endsWith(`/${childRunId}/summary.json`)) matches.push(blob.name);
  }
  if (matches.length !== 1) throw new Error(`fail closed: expected exactly one protocol-v3 child summary across UTC dates, found ${matches.length}`);
  return loadJsonUntrustedHash(container, matches[0]);
}

function exactChildControlBinding({ manifestDocument, consumptionDocument, authorization, intent, childRunId }) {
  return {
    child_run_id: childRunId,
    consumption_blob_name: consumptionDocument.blobName,
    consumption_sha256: consumptionDocument.hash,
    authorization_blob_name: authorization.blobName,
    authorization_sha256: authorization.hash,
    intent_blob_name: intent.blobName,
    intent_sha256: intent.hash,
    manifest_blob_name: manifestDocument.blobName,
    manifest_sha256: manifestDocument.hash,
    prediction_model: {
      blob_uri: consumptionDocument.value.execution_model.blob_uri,
      sha256: consumptionDocument.value.execution_model.sha256,
      model_version: consumptionDocument.value.execution_model.model_version
    }
  };
}

function pendingProgress(consumption, intent, authorization, childRunId, summary, evidence, expectedBinding, now) {
  return {
    schema: "polyedge.funded_stage_pending_terminal.v1", grant_id: consumption.grant_id,
    campaign_id: consumption.campaign_id, campaign_control_id: consumption.campaign_control_id,
    candidate: consumption.candidate,
    stage_target_orders: consumption.stage_target_orders, decision_id: intent.value.decision_id,
    intent_blob_name: intent.blobName, intent_sha256: intent.hash,
    source_intent_blob_name: intent.sourceBlobName, source_intent_sha256: intent.sourceHash,
    authorization_blob_name: authorization.blobName, authorization_sha256: authorization.hash,
    child_run_id: childRunId, run_id: evidence.runId, probe_id: evidence.probeId, order_id: evidence.orderId,
    protocol_v3_summary_blob_name: summary.blobName, protocol_v3_summary_sha256: summary.hash,
    expected_control_binding: expectedBinding,
    attempted_order_count: 1, submitted_order_count: 1, eligible_order_count: 0,
    status: "pending_terminal", created_at: now.toISOString()
  };
}

async function settlePendingTerminal(container, config, consumptionDocument, pendingDocument, completed, now) {
  const consumption = consumptionDocument.value;
  const pending = pendingDocument.value;
  if (normalizeHash(pending.expected_control_binding?.consumption_sha256) !== consumptionDocument.hash) {
    throw new Error("fail closed: pending terminal does not bind the exact loaded stage consumption");
  }
  const terminal = await discoverTerminal(container, pending);
  if (!terminal) return { status: "funded_stage_pending_terminal", remaining: Number(consumption.quota_orders) - completed.length - 1 };
  const summary = await loadHashedJson(container, pending.protocol_v3_summary_blob_name, pending.protocol_v3_summary_sha256);
  validateProtocolV3ChildEvidence({
    summary: summary.value,
    terminal: terminal.value,
    consumption,
    decisionId: pending.decision_id,
    expectedBinding: pending.expected_control_binding
  });
  const completion = await recordCompletion(container, config, consumption,
    { value: { decision_id: pending.decision_id }, blobName: pending.intent_blob_name, hash: pending.intent_sha256, sourceBlobName: pending.source_intent_blob_name, sourceHash: pending.source_intent_sha256 },
    { blobName: pending.authorization_blob_name, hash: pending.authorization_sha256 }, pending.child_run_id,
    summary, terminal, completed, pending.expected_control_binding, now);
  return { status: "funded_stage_order_completed", remaining: Number(consumption.quota_orders) - completed.length - 1, progress_sha256: completion.hash };
}

async function discoverTerminal(container, pending) {
  const matches = [];
  for await (const blob of container.listBlobsFlat({ prefix: "reports/research/venue-probe/terminal-risk-portfolio/" })) {
    if (!blob.name.endsWith(`/${pending.probe_id}.json`)) continue;
    const document = await loadJsonUntrustedHash(container, blob.name);
    if (document.value?.run_id === pending.run_id && document.value?.probe_id === pending.probe_id && document.value?.order_id === pending.order_id) matches.push(document);
  }
  if (matches.length > 1) throw new Error("fail closed: multiple terminal artifacts claim the same run/probe/order identity");
  return matches[0] || null;
}

async function recordCompletion(container, config, consumption, intent, authorization, childRunId, summary, terminal, completed, expectedControlBinding, now) {
  const sequence = Number(consumption.starting_funded_orders) + completed.length + 1;
  const summaryBinding = { blob_name: summary.blobName, sha256: summary.hash };
  const terminalBinding = { blob_name: terminal.blobName, sha256: terminal.hash };
  const progressPayloadSha256 = progressPayloadHash({
    sequence,
    decisionId: intent.value.decision_id,
    expectedControlBinding,
    summaryBinding,
    terminalBinding
  });
  const priorDigest = await priorCumulativeProgressHash(container, config, consumption, completed, sequence);
  const cumulativeDigest = cumulativeProgressHash(priorDigest, progressPayloadSha256);
  const value = {
    schema: "polyedge.funded_stage_order_progress.v1", grant_id: consumption.grant_id,
    campaign_id: consumption.campaign_id, campaign_control_id: consumption.campaign_control_id,
    candidate: consumption.candidate,
    stage_target_orders: consumption.stage_target_orders, sequence, decision_id: intent.value.decision_id,
    intent_blob_name: intent.blobName, intent_sha256: intent.hash,
    source_intent_blob_name: intent.sourceBlobName || null, source_intent_sha256: intent.sourceHash || null,
    authorization_blob_name: authorization.blobName, authorization_sha256: authorization.hash, child_run_id: childRunId,
    protocol_v3_summary_blob_name: summary.blobName, protocol_v3_summary_sha256: summary.hash,
    terminal_evidence_blob_name: terminal.blobName, terminal_evidence_sha256: terminal.hash,
    expected_control_binding: expectedControlBinding,
    progress_payload_sha256: progressPayloadSha256,
    prior_cumulative_evidence_sha256: priorDigest, cumulative_evidence_sha256: cumulativeDigest,
    attempted_order_count: 1, submitted_order_count: 1, eligible_order_count: 1,
    completed_at: now.toISOString()
  };
  const progress = await putImmutableOrVerify(container, { blobName: `${campaignControlPrefix(config, consumption)}/progress/${consumption.grant_id}/${intent.value.decision_id}.json`, value });
  await putImmutableOrVerify(container, {
    blobName: `${campaignControlPrefix(config, consumption)}/funded-state/${consumption.grant_id}/${String(sequence).padStart(3, "0")}.json`,
    value: {
      schema: "polyedge.funded_runtime_state.v1", grant_id: consumption.grant_id, stage_target_orders: consumption.stage_target_orders,
      campaign_id: consumption.campaign_id, campaign_control_id: consumption.campaign_control_id,
      candidate: consumption.candidate,
      exact_funded_order_count: sequence, last_verified_terminal_artifact: { blob_name: terminal.blobName, sha256: terminal.hash },
      cumulative_evidence_sha256: cumulativeDigest, progress_blob_name: progress.blobName, progress_sha256: progress.hash, updated_at: now.toISOString()
    }
  });
  return progress;
}

async function priorCumulativeProgressHash(container, config, consumption, completed, nextSequence) {
  const all = await listJson(container, `${campaignControlPrefix(config, consumption)}/progress/`);
  const prior = all
    .filter((document) => Number(document.value?.sequence) < nextSequence)
    .sort((left, right) => Number(left.value.sequence) - Number(right.value.sequence));
  if (prior.length !== nextSequence - 2 || completed.length > prior.length) {
    throw new Error("fail closed: cumulative funded progress is missing or duplicated before the next order");
  }
  let cumulative = normalizeHash(consumption.checkpoint_1_chain_root_sha256);
  if (!cumulative) throw new Error("fail closed: checkpoint-1 progress-chain root is missing");
  for (const [index, document] of prior.entries()) {
    const value = document.value;
    const sequence = index + 2;
    if (value.schema !== "polyedge.funded_stage_order_progress.v1" ||
        Number(value.sequence) !== sequence || value.campaign_id !== consumption.campaign_id ||
        JSON.stringify(value.candidate) !== JSON.stringify(consumption.candidate)) {
      throw new Error("fail closed: cumulative funded progress sequence/control identity is invalid");
    }
    const payload = progressPayloadHash({
      sequence,
      decisionId: value.decision_id,
      expectedControlBinding: value.expected_control_binding,
      summaryBinding: { blob_name: value.protocol_v3_summary_blob_name, sha256: value.protocol_v3_summary_sha256 },
      terminalBinding: { blob_name: value.terminal_evidence_blob_name, sha256: value.terminal_evidence_sha256 }
    });
    const expectedCumulative = cumulativeProgressHash(cumulative, payload);
    if (normalizeHash(value.progress_payload_sha256) !== payload ||
        normalizeHash(value.prior_cumulative_evidence_sha256) !== cumulative ||
        normalizeHash(value.cumulative_evidence_sha256) !== expectedCumulative) {
      throw new Error("fail closed: cumulative funded progress hash chain is invalid");
    }
    cumulative = expectedCumulative;
  }
  return cumulative;
}

async function writeStageBlock(container, config, consumption, decisionId, childRunId, reason, now) {
  return putImmutableOrVerify(container, buildStageBlock({
    config, consumption, decisionId, childRunId, reason, now
  }));
}

async function publishStageCheckpoint(container, config, manifest) {
  const state = manifest.funded_ladder;
  const target = Number(state.active_target_orders);
  const initialSummary = state.checkpoint_1_protocol_v3_artifact;
  const initialTerminal = state.checkpoint_1_terminal_artifact;
  if (!initialSummary?.blob_name || !initialTerminal?.blob_name) {
    throw new Error("fail closed: canonical funded state lacks checkpoint-1 immutable bindings");
  }
  const entries = [{
    sequence: 1,
    summaryBinding: initialSummary,
    terminalBinding: initialTerminal,
    summary: (await loadHashedJson(container, initialSummary.blob_name, initialSummary.sha256)).value,
    terminal: (await loadHashedJson(container, initialTerminal.blob_name, initialTerminal.sha256)).value,
    progress: null,
    progressBinding: null
  }];
  entries[0].expectedControlBinding = controlBindingFromSummary(entries[0].summary);
  const base = `${config.controlPrefix}/campaigns/${sha256(Buffer.from(state.campaign_id)).slice("sha256:".length)}`;
  const progress = await listJson(container, `${base}/progress/`);
  for (const document of progress) {
    const value = document.value;
    if (value.campaign_id !== state.campaign_id || JSON.stringify(value.candidate) !== JSON.stringify(state.candidate)) {
      throw new Error("fail closed: cross-campaign or candidate progress contamination detected");
    }
    if (Number(value.sequence) < 2 || Number(value.sequence) > target) continue;
    const summary = await loadHashedJson(container, value.protocol_v3_summary_blob_name, value.protocol_v3_summary_sha256);
    const terminal = await loadHashedJson(container, value.terminal_evidence_blob_name, value.terminal_evidence_sha256);
    entries.push({
      sequence: Number(value.sequence),
      summaryBinding: { blob_name: summary.blobName, sha256: summary.hash },
      terminalBinding: { blob_name: terminal.blobName, sha256: terminal.hash },
      progressBinding: { blob_name: document.blobName, sha256: document.hash },
      progress: value,
      expectedControlBinding: value.expected_control_binding,
      summary: summary.value,
      terminal: terminal.value
    });
  }
  const value = buildFundedCheckpointEvidence({ manifest, entries });
  const bytes = Buffer.from(JSON.stringify(value, null, 2));
  const hash = sha256(bytes);
  const blobName = `${base}/checkpoints/${target}/${hash.slice("sha256:".length)}.json`;
  const document = await putImmutableOrVerify(container, { blobName, value });
  return { blob_name: document.blobName, sha256: document.hash, stage_target_orders: target };
}

function controlBindingFromSummary(summary) {
  const provenance = summary?.provenance || {};
  const model = summary?.prediction_model || {};
  return {
    child_run_id: summary?.run_id,
    consumption_blob_name: provenance.funded_stage_consumption_blob_name,
    consumption_sha256: provenance.funded_stage_consumption_sha256,
    authorization_blob_name: provenance.authorization_blob_name,
    authorization_sha256: provenance.authorization_sha256,
    intent_blob_name: provenance.intent_blob_name,
    intent_sha256: provenance.intent_sha256,
    manifest_blob_name: provenance.promotion_manifest_blob_name,
    manifest_sha256: provenance.promotion_manifest_sha256,
    prediction_model: {
      blob_uri: model.blob_uri,
      sha256: model.sha256,
      model_version: model.model_version
    }
  };
}

function campaignControlPrefix(config, consumption) {
  const expected = sha256(Buffer.from(consumption.campaign_id)).slice("sha256:".length);
  if (!consumption.campaign_id || consumption.campaign_control_id !== expected) {
    throw new Error("fail closed: funded control campaign identity is invalid");
  }
  return `${config.controlPrefix}/campaigns/${expected}`;
}

export function fundedChildEnvironment(env, { config, manifestDocument, consumptionDocument, grantHash, authorization, intent, childRunId }) {
  return {
    ...env, EXECUTION_MODE: "strategy_canary", ALLOW_LIVE: "false", ALLOW_STRATEGY_CANARY: "true",
    STRATEGY_CANARY_DRY_RUN: config.dryRun ? "true" : "false", STRATEGY_CANARY_RUN_ID: childRunId,
    STRATEGY_CANARY_INTENT_BLOB_NAME: intent.blobName, STRATEGY_CANARY_INTENT_SHA256: intent.hash,
    STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME: manifestDocument.blobName, STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256: manifestDocument.hash,
    STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME: authorization.blobName, STRATEGY_CANARY_AUTHORIZATION_SHA256: authorization.hash,
    STRATEGY_CANARY_HUMAN_GRANT_ID: consumptionDocument.value.grant_id, STRATEGY_CANARY_HUMAN_GRANT_SHA256: grantHash,
    STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_BLOB_NAME: consumptionDocument.blobName,
    STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_SHA256: consumptionDocument.hash,
    STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI: manifestDocument.value.execution_model.blob_uri,
    STRATEGY_CANARY_EXECUTION_MODEL_SHA256: manifestDocument.value.execution_model.sha256,
    STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: manifestDocument.value.execution_model.model_version
  };
}

function invokeCanaryChild(env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [new URL("./canary.mjs", import.meta.url).pathname], { env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => signal ? reject(new Error(`funded child terminated by ${signal}`)) : resolve(code ?? 1));
  });
}
async function streamToBuffer(stream) { const chunks = []; for await (const chunk of stream) chunks.push(Buffer.from(chunk)); return Buffer.concat(chunks); }
function normalizeHash(value) { const text = String(value || "").trim().toLowerCase(); const prefixed = text.startsWith("sha256:") ? text : `sha256:${text}`; return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : ""; }
function runId(prefix) { return `${prefix}-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`; }

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runFundedLadderController().then((result) => console.log(JSON.stringify(sanitize(result)))).catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify({ schema: "polyedge.funded_ladder_controller_run.v1", status: "failed_closed", error: error.message }));
  });
}
