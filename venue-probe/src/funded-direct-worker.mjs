import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import {
  artifactLocationFromUri,
  loadHashedJson,
  sha256,
  VENUE_GTD_SECURITY_BUFFER_MS
} from "./canary-lib.mjs";
import { sanitize, storageContainer } from "./lib.mjs";
import { validateProfitQuarantineManifest } from "./profit-quarantine.mjs";

const EXPECTED_CANDIDATE = "dynamic_quote_style";
const EXPECTED_VERSION = "dynamic_quote_style@2026-06-14";
const SESSION_SCHEMA = "polyedge.operator_funded_session.v1";
const AUTHORIZATION_SCHEMA = "polyedge.operator_funded_intent_authorization.v1";
const MAX_INTENT_TTL_MS = 30_000;

export function loadFundedDirectConfig(env = process.env) {
  const sessionJson = String(env.FUNDED_DIRECT_SESSION_MANIFEST_JSON || "").trim();
  const session = parseJson(sessionJson);
  const config = {
    enabled: env.FUNDED_DIRECT_WORKER_ENABLED === "true",
    allowed: env.ALLOW_FUNDED_DIRECT === "true",
    dryRun: env.FUNDED_DIRECT_DRY_RUN !== "false",
    session,
    sessionBlobName: clean(env.FUNDED_DIRECT_SESSION_MANIFEST_BLOB_NAME),
    sessionHash: hash(env.FUNDED_DIRECT_SESSION_MANIFEST_SHA256),
    candidate: clean(env.STRATEGY_CANARY_CANDIDATE_NAME),
    candidateVersion: clean(env.STRATEGY_CANARY_CANDIDATE_VERSION),
    candidateConfigHash: hash(env.STRATEGY_CANARY_CANDIDATE_CONFIG_HASH),
    requiredFillModelVersion: clean(env.STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION),
    requiredResolutionSource: clean(env.STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE || "chainlink_reference"),
    targetOrderNotional: number(env.STRATEGY_INTENT_TARGET_ORDER_NOTIONAL),
    maxOrderNotional: number(env.STRATEGY_CANARY_MAX_ORDER_NOTIONAL),
    minimumSecondsToExpiry: integer(env.STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY, 360),
    maximumSecondsToExpiry: integer(env.STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY, 900),
    intentPrefix: clean(env.STRATEGY_CANARY_INTENT_PREFIX).replace(/^\/+|\/+$/g, ""),
    controlPrefix: clean(env.FUNDED_DIRECT_CONTROL_PREFIX || "reports/funded/dynamic-quote").replace(/^\/+|\/+$/g, ""),
    intentContainerName: clean(env.STRATEGY_CANARY_INTENT_CONTAINER_NAME),
    controlContainerName: clean(env.AZURE_STORAGE_CONTAINER_NAME),
    storageAccount: clean(env.AZURE_STORAGE_ACCOUNT_NAME),
    storageAccountKey: env.AZURE_STORAGE_ACCOUNT_KEY,
    azureClientId: env.AZURE_CLIENT_ID,
    maxIterations: integer(env.FUNDED_DIRECT_MAX_ITERATIONS, 200),
    pollIntervalMs: integer(env.FUNDED_DIRECT_POLL_INTERVAL_MS, 1_000),
    maxIdleMs: integer(env.FUNDED_DIRECT_MAX_IDLE_MS, 300_000),
    minRemainingTtlMs: integer(env.FUNDED_DIRECT_MIN_REMAINING_TTL_MS, 7_000),
    childMinRemainingTtlMs: integer(env.FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS, 2_000)
  };
  const errors = [];
  if (!config.enabled) errors.push("FUNDED_DIRECT_WORKER_ENABLED must be true");
  if (!config.allowed) errors.push("ALLOW_FUNDED_DIRECT must be true");
  if (!config.session) errors.push("FUNDED_DIRECT_SESSION_MANIFEST_JSON must be valid JSON");
  if (!config.sessionBlobName || !config.sessionHash) errors.push("exact operator session manifest blob and SHA-256 are required");
  if (config.candidate !== EXPECTED_CANDIDATE || config.candidateVersion !== EXPECTED_VERSION) {
    errors.push("worker must remain bound to the frozen Dynamic Quote candidate");
  }
  if (!config.candidateConfigHash || !config.requiredFillModelVersion) errors.push("candidate config and execution-model version are required");
  if (!(config.targetOrderNotional > 1 && config.targetOrderNotional <= config.maxOrderNotional)) {
    errors.push("operator-funded target notional must be in (1, max order notional]");
  }
  if (!(config.maxOrderNotional > 1 && config.maxOrderNotional <= 100)) errors.push("operator-funded order cap must be in (1, 100]");
  if (!(config.minimumSecondsToExpiry === 360 && config.maximumSecondsToExpiry === 900)) {
    errors.push("operator-funded Dynamic Quote window must be exactly 360-900 seconds to expiry");
  }
  if (!config.intentPrefix || !config.intentContainerName || !config.controlContainerName || !config.storageAccount) {
    errors.push("isolated intent/control storage configuration is required");
  }
  if (!(config.maxIterations >= 1 && config.maxIterations <= 2_000)) errors.push("FUNDED_DIRECT_MAX_ITERATIONS must be in [1, 2000]");
  if (!(config.pollIntervalMs >= 1_000 && config.pollIntervalMs <= 60_000)) errors.push("FUNDED_DIRECT_POLL_INTERVAL_MS must be in [1000, 60000]");
  if (!(config.maxIdleMs >= config.pollIntervalMs && config.maxIdleMs <= 3_600_000)) errors.push("FUNDED_DIRECT_MAX_IDLE_MS must be between the poll interval and 3600000");
  if (!(config.minRemainingTtlMs >= 5_000 && config.minRemainingTtlMs <= MAX_INTENT_TTL_MS)) {
    errors.push("FUNDED_DIRECT_MIN_REMAINING_TTL_MS must be in [5000, 30000]");
  }
  if (!(config.childMinRemainingTtlMs >= 1_000 && config.childMinRemainingTtlMs <= config.minRemainingTtlMs)) {
    errors.push("FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS must be in [1000, FUNDED_DIRECT_MIN_REMAINING_TTL_MS]");
  }
  if (String(env.FUNDED_DIRECT_ENGINE || "") === "persistent_v1" &&
      (config.minRemainingTtlMs < 15_000 || config.childMinRemainingTtlMs < 15_000)) {
    errors.push("persistent_v1 requires at least 15000ms before authorization and child execution");
  }
  if (errors.length) throw new Error(`funded_direct_worker blocked: ${errors.join("; ")}`);
  validateSession(config);
  return config;
}

export async function runFundedDirectWorker({
  env = process.env,
  containers,
  invokeChild = invokeCanaryChild,
  sleep = delay,
  clock = () => new Date()
} = {}) {
  const config = loadFundedDirectConfig(env);
  const clients = containers || {
    control: storageContainer({ ...config, storageContainer: config.controlContainerName }),
    intents: storageContainer({ ...config, storageContainer: config.intentContainerName })
  };
  if (!clients.control || !clients.intents) throw new Error("fail closed: operator-funded storage clients are unavailable");
  const sessionDocument = await putImmutableOrVerify(clients.control, {
    blobName: config.sessionBlobName,
    value: config.session
  });
  if (sessionDocument.hash !== config.sessionHash) throw new Error("fail closed: operator session manifest SHA-256 mismatch");

  let idleSince = null;
  let childInvocations = 0;
  for (let iteration = 1; iteration <= config.maxIterations; iteration += 1) {
    const selected = await firstFreshIntent(clients, config, sessionDocument.value, clock());
    if (!selected) {
      idleSince ??= clock().getTime();
      if (clock().getTime() - idleSince >= config.maxIdleMs) {
        return result("idle_waiting_for_fresh_intent", config, { iteration, childInvocations });
      }
      await sleep(config.pollIntervalMs);
      continue;
    }
    idleSince = null;
    const executed = await executeSelectedIntent({
      env,
      config,
      clients,
      sessionDocument,
      selected,
      invokeChild,
      clock,
      iteration,
      childInvocations
    });
    childInvocations = executed.childInvocations;
    if (executed.result) return executed.result;
  }
  return result("iteration_limit_reached", config, { iteration: config.maxIterations, childInvocations });
}

export async function createFundedDirectProcessor({
  env = process.env,
  containers,
  executeCanary,
  clock = () => new Date()
} = {}) {
  if (typeof executeCanary !== "function") throw new Error("fail closed: persistent canary executor is required");
  const config = loadFundedDirectConfig(env);
  const clients = containers || {
    control: storageContainer({ ...config, storageContainer: config.controlContainerName }),
    intents: storageContainer({ ...config, storageContainer: config.intentContainerName })
  };
  if (!clients.control || !clients.intents) throw new Error("fail closed: operator-funded storage clients are unavailable");
  const sessionDocument = await putImmutableOrVerify(clients.control, {
    blobName: config.sessionBlobName,
    value: config.session
  });
  if (sessionDocument.hash !== config.sessionHash) throw new Error("fail closed: operator session manifest SHA-256 mismatch");
  return {
    async process(handoff) {
      const processorStartedWallMs = Date.now();
      const processorStartedMonotonicMs = performance.now();
      const selected = await selectedFromHandoff(clients, config, sessionDocument.value, handoff, clock());
      const executionTiming = {
        processor_started_wall_ms: processorStartedWallMs,
        processor_started_monotonic_ms: processorStartedMonotonicMs,
        ...selected.handoffTiming
      };
      if (selected.duplicateCompletion) {
        return result("already_completed_idempotent", config, {
          iteration: 1,
          childInvocations: 0,
          decisionId: selected.value.decision_id,
          completion: selected.duplicateCompletion,
          execution_timing: executionTiming
        });
      }
      const executed = await executeSelectedIntent({
        env,
        config,
        clients,
        sessionDocument,
        selected,
        invokeChild: async (childEnv) => {
          try {
            const value = await executeCanary(childEnv);
            return {
              exitCode: 0,
              error: "",
              orderSubmissionAttempted: value?.order_submission_attempted === true,
              value
            };
          } catch (error) {
            return {
              exitCode: 1,
              error: error.message,
              orderSubmissionAttempted: error?.orderSubmissionAttempted === true
            };
          }
        },
        clock,
        iteration: 1,
        childInvocations: 0,
        executionTiming
      });
      if (executed.result) return { ...executed.result, execution_timing: executionTiming };
      return result("persistent_intent_completed", config, {
        iteration: 1,
        childInvocations: executed.childInvocations,
        decisionId: selected.value.decision_id,
        execution: executed.execution || null,
        execution_timing: executionTiming
      });
    }
  };
}

async function executeSelectedIntent({
  env,
  config,
  clients,
  sessionDocument,
  selected,
  invokeChild,
  clock,
  iteration,
  childInvocations,
  executionTiming = {}
}) {
  const authorizationNow = clock();
  if (!hasMinimumRemainingTtl(selected.value, authorizationNow, config.minRemainingTtlMs)) {
    return {
      childInvocations,
      result: result("stale_handoff_rejected", config, {
        iteration,
        childInvocations,
        decisionId: selected.value.decision_id
      })
    };
  }
  const childRunId = runId();
  executionTiming.authorization_started_wall_ms = Date.now();
  executionTiming.authorization_started_monotonic_ms = performance.now();
  const authorization = await putImmutableOrVerify(
    clients.control,
    buildAuthorization(config, sessionDocument, selected, childRunId, authorizationNow)
  );
  executionTiming.authorization_persisted_wall_ms = Date.now();
  executionTiming.authorization_persisted_monotonic_ms = performance.now();
  if (config.dryRun) {
    return {
      childInvocations,
      result: result("dry_run_validated", config, {
        iteration,
        childInvocations,
        decisionId: selected.value.decision_id,
        authorizationHash: authorization.hash
      })
    };
  }
  const launchNow = clock();
  if (!hasMinimumRemainingTtl(selected.value, launchNow, config.childMinRemainingTtlMs)) {
    await writeCompletion(clients.control, config, selected, authorization, childRunId, launchNow, {
      status: "expired_before_child_launch",
      order_submission_attempted: false,
      authorization_consumed: false,
      risk_reservation_created: false
    });
    return {
      childInvocations,
      result: result("expired_before_child_launch", config, {
        iteration,
        childInvocations,
        decisionId: selected.value.decision_id
      })
    };
  }
  executionTiming.child_launch_wall_ms = Date.now();
  executionTiming.child_launch_monotonic_ms = performance.now();
  const child = await invokeChild(childEnvironment(env, config, sessionDocument, selected, authorization, childRunId));
  executionTiming.child_completed_wall_ms = Date.now();
  executionTiming.child_completed_monotonic_ms = performance.now();
  childInvocations += 1;
  if (child.exitCode !== 0) {
    if (/existing_unresolved_position_blocks_submission|unresolved_risk_reservation|equity_floor_breached|campaign_drawdown_exhausted|authorized_starting_collateral|external_cash_flow_record/.test(child.error || "")) {
      await writeCompletion(clients.control, config, selected, authorization, childRunId, clock(), {
        status: "child_failed_closed_pre_submission",
        order_submission_attempted: false,
        authorization_consumed: false,
        risk_reservation_created: false,
        error: child.error
      });
      return {
        childInvocations,
        result: result("paused_by_account_risk_state", config, {
          iteration,
          childInvocations,
          decisionId: selected.value.decision_id,
          error: child.error
        })
      };
    }
    throw new Error(`fail closed: funded Dynamic Quote child exited ${child.exitCode} (${child.error || "unknown"})`);
  }
  await writeCompletion(clients.control, config, selected, authorization, childRunId, clock(), {
    status: "child_completed",
    order_submission_attempted: child.orderSubmissionAttempted === true,
    authorization_consumed: true,
    risk_reservation_created: true
  });
  executionTiming.completion_persisted_wall_ms = Date.now();
  executionTiming.completion_persisted_monotonic_ms = performance.now();
  return { childInvocations, result: null, execution: child.value || null };
}

async function selectedFromHandoff(clients, config, session, handoff, now) {
  const verificationStartedWallMs = Date.now();
  const verificationStartedMonotonicMs = performance.now();
  if (handoff?.schema !== "polyedge.funded_intent_handoff.v1") {
    throw new Error("fail closed: unsupported funded intent handoff schema");
  }
  const decisionId = clean(handoff.decision_id);
  const blobName = clean(handoff.intent_blob_name);
  const expectedHash = hash(handoff.intent_sha256);
  if (!/^[0-9a-f]{64}$/.test(decisionId) ||
      blobName !== `${config.intentPrefix}/${decisionId}.json` ||
      !expectedHash ||
      Date.parse(handoff.decision_ts) > now.getTime() ||
      Date.parse(handoff.valid_until) - now.getTime() < config.minRemainingTtlMs) {
    throw new Error("fail closed: funded intent handoff binding or TTL is invalid");
  }
  const response = await clients.intents.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  const actualHash = sha256(bytes);
  if (actualHash !== expectedHash) throw new Error("fail closed: funded intent handoff SHA-256 mismatch");
  let value;
  try { value = JSON.parse(bytes.toString("utf8")); }
  catch { throw new Error("fail closed: funded intent handoff blob is not valid JSON"); }
  if (value.decision_id !== decisionId ||
      value.decision_ts !== handoff.decision_ts ||
      value.valid_until !== handoff.valid_until ||
      !qualifies(value, blobName, actualHash, config, session, now)) {
    throw new Error("fail closed: funded intent handoff does not qualify for execution");
  }
  const authorizationName = authorizationBlobName(config, session, value);
  if (await clients.control.getBlobClient(authorizationName).exists()) {
    const completionName = completionBlobName(config, session, value);
    if (await clients.control.getBlobClient(completionName).exists()) {
      const completion = await readJsonBlob(clients.control, completionName);
      if (completion?.schema === "polyedge.operator_funded_intent_completion.v1" &&
          completion.session_id === session.session_id &&
          completion.decision_id === value.decision_id &&
          completion.authorization_blob_name === authorizationName &&
          ["child_completed", "expired_before_child_launch", "child_failed_closed_pre_submission"].includes(completion.status)) {
        return {
          value,
          blobName,
          hash: actualHash,
          decisionMs: Date.parse(value.decision_ts),
          duplicateCompletion: completion,
          handoffTiming: {
            hash_verification_started_wall_ms: verificationStartedWallMs,
            hash_verification_started_monotonic_ms: verificationStartedMonotonicMs,
            hash_verified_wall_ms: Date.now(),
            hash_verified_monotonic_ms: performance.now()
          }
        };
      }
      throw new Error("fail closed: funded intent completion is not bound to its authorization");
    }
    throw new Error("fail closed: funded intent handoff already has an authorization");
  }
  return {
    value,
    blobName,
    hash: actualHash,
    decisionMs: Date.parse(value.decision_ts),
    handoffTiming: {
      hash_verification_started_wall_ms: verificationStartedWallMs,
      hash_verification_started_monotonic_ms: verificationStartedMonotonicMs,
      hash_verified_wall_ms: Date.now(),
      hash_verified_monotonic_ms: performance.now()
    }
  };
}

function validateSession(config) {
  const value = config.session;
  const created = Date.parse(value?.created_at);
  const expires = Date.parse(value?.expires_at);
  const expectedHash = sha256(Buffer.from(JSON.stringify(value, null, 2)));
  let profitQuarantineValid = value?.profit_quarantine === undefined
    && value?.verified_internal_settlements === undefined;
  if (value?.profit_quarantine?.enabled === true) {
    try {
      validateProfitQuarantineManifest(value);
      profitQuarantineValid = true;
    } catch {
      profitQuarantineValid = false;
    }
  }
  const valid = value?.schema_version === SESSION_SCHEMA
    && clean(value.session_id)
    && value.authorization_mode === "operator_direct"
    && value.authorized_by_user_reference === "Codex task 2026-07-27 funded Dynamic Quote"
    && value.research_promotion_bypassed === true
    && value.research_lane_isolated === true
    && value.maker_only === true
    && value.no_deposits === true
    && value.allow_automatic_replenishment === false
    && value.allow_compounding === false
    && profitQuarantineValid
    && Array.isArray(value.external_cash_flows)
    && value.external_cash_flows.length === 0
    && Number(value.max_open_orders) === 1
    && Number(value.target_order_notional) === config.targetOrderNotional
    && Number(value.max_order_notional) === config.maxOrderNotional
    && Number(value.max_account_loss) === Number(value.starting_collateral)
    && Number(value.starting_collateral) > 0
    && Number(value.max_reconciliation_discrepancy) >= 0
    && Number(value.max_reconciliation_discrepancy) <= 0.01
    && Number(value.max_account_loss) > config.maxOrderNotional
    && Number(value.source_simulated_pnl) === 379.19
    && Number(value.execution_window_seconds_to_expiry?.minimum) === config.minimumSecondsToExpiry
    && Number(value.execution_window_seconds_to_expiry?.maximum) === config.maximumSecondsToExpiry
    && value.evidence_trust_boundary_ready === false
    && value.candidate?.name === config.candidate
    && value.candidate?.candidate_version === config.candidateVersion
    && hash(value.candidate?.config_hash) === config.candidateConfigHash
    && value.execution_model?.model_version === config.requiredFillModelVersion
    && hash(value.execution_model?.sha256)
    && clean(value.execution_model?.blob_uri)
    && Number.isFinite(created)
    && Number.isFinite(expires)
    && expires > created
    && expectedHash === config.sessionHash;
  if (!valid) throw new Error("funded_direct_worker blocked: operator-funded session contract is invalid or hash-mismatched");
}

async function firstFreshIntent(clients, config, session, now) {
  const candidates = [];
  let currentSessionCandidates = 0;
  const sessionStartMs = Date.parse(session.created_at);
  const freshBlobFloorMs = now.getTime() - MAX_INTENT_TTL_MS;
  for await (const blob of clients.intents.listBlobsFlat({ prefix: `${config.intentPrefix}/` })) {
    if (!blob.name.endsWith(".json")) continue;
    const createdMs = Date.parse(blob.properties?.createdOn);
    if (Number.isFinite(createdMs) && createdMs < sessionStartMs) continue;
    if (Number.isFinite(createdMs) && createdMs < freshBlobFloorMs) continue;
    currentSessionCandidates += 1;
    if (currentSessionCandidates > 10_000) throw new Error("fail closed: operator-funded session exceeded the bounded intent scan");
    const response = await clients.intents.getBlobClient(blob.name).download();
    const bytes = await streamToBuffer(response.readableStreamBody);
    let value;
    try { value = JSON.parse(bytes.toString("utf8")); } catch { continue; }
    if (!qualifies(value, blob.name, sha256(bytes), config, session, now)) continue;
    const authorizationName = authorizationBlobName(config, session, value);
    if (await clients.control.getBlobClient(authorizationName).exists()) continue;
    candidates.push({ value, blobName: blob.name, hash: sha256(bytes), decisionMs: Date.parse(value.decision_ts) });
  }
  candidates.sort((left, right) => right.decisionMs - left.decisionMs || left.blobName.localeCompare(right.blobName));
  return candidates[0] || null;
}

function qualifies(intent, blobName, intentHash, config, session, now) {
  const decisionMs = Date.parse(intent?.decision_ts);
  const validUntilMs = Date.parse(intent?.valid_until);
  const venueExpiryMs = Date.parse(intent?.gtd_expiry_ts);
  const marketEndMs = Date.parse(intent?.market_end_ts);
  const sessionStartMs = Date.parse(session.created_at);
  const sessionExpiryMs = Date.parse(session.expires_at);
  const nowMs = now.getTime();
  const price = Number(intent?.price);
  const shares = Number(intent?.shares);
  const notional = Number(intent?.notional);
  const feeAllowance = Number(intent?.fee_allowance);
  const reservedNotional = notional + shares * feeAllowance;
  return intent?.schema === "polyedge.execution_intent.v1"
    && /^[0-9a-f]{64}$/.test(String(intent?.decision_id || ""))
    && blobName === `${config.intentPrefix}/${intent.decision_id}.json`
    && hash(intentHash)
    && intent.candidate_name === config.candidate
    && intent.candidate_version === config.candidateVersion
    && hash(intent.candidate_config_hash) === config.candidateConfigHash
    && intent.required_fill_model_version === session.execution_model.model_version
    && intent.execution_model_blob_uri === session.execution_model.blob_uri
    && hash(intent.execution_model_sha256) === hash(session.execution_model.sha256)
    && intent.resolution_source === config.requiredResolutionSource
    && intent.exact_resolution_source === true
    && String(intent.side).toUpperCase() === "BUY"
    && intent.post_only === true
    && intent.order_kind === "post_only_gtd"
    && Number.isFinite(decisionMs)
    && decisionMs >= sessionStartMs
    && decisionMs <= nowMs
    && Number.isFinite(validUntilMs)
    && validUntilMs - nowMs >= config.minRemainingTtlMs
    && validUntilMs <= sessionExpiryMs
    && Number.isFinite(venueExpiryMs)
    && venueExpiryMs === validUntilMs + VENUE_GTD_SECURITY_BUFFER_MS
    && Number.isFinite(marketEndMs)
    && marketEndMs - decisionMs >= config.minimumSecondsToExpiry * 1_000
    && marketEndMs - decisionMs <= config.maximumSecondsToExpiry * 1_000
    && venueExpiryMs < marketEndMs
    && Number(intent.ttl_ms) > 0
    && Number(intent.ttl_ms) <= MAX_INTENT_TTL_MS
    && validUntilMs === decisionMs + Number(intent.ttl_ms)
    && Number.isFinite(price)
    && Number.isFinite(shares)
    && Number.isFinite(notional)
    && Number.isFinite(feeAllowance)
    && feeAllowance >= 0
    && Number.isFinite(reservedNotional)
    && notional > 1 + 1e-9
    && reservedNotional >= config.targetOrderNotional - 0.01 - 1e-9
    && reservedNotional <= config.targetOrderNotional + 1e-9
    && notional <= config.maxOrderNotional
    && Math.abs(price * shares - notional) <= 1e-9
    && shares >= Number(intent.minimum_order_size)
    && Number(intent.net_edge_lower_bound) > 0;
}

function buildAuthorization(config, sessionDocument, intent, childRunId, now) {
  const model = sessionDocument.value.execution_model;
  const artifact = artifactLocationFromUri(model.blob_uri, config.storageAccount);
  const value = {
    schema: AUTHORIZATION_SCHEMA,
    authorization_id: `${sessionDocument.value.session_id}-${intent.value.decision_id.slice(0, 16)}`,
    authorization_mode: "operator_direct",
    session_id: sessionDocument.value.session_id,
    decision_id: intent.value.decision_id,
    child_run_id: childRunId,
    intent_blob_name: intent.blobName,
    intent_sha256: intent.hash,
    promotion_manifest_blob_name: sessionDocument.blobName,
    promotion_manifest_sha256: sessionDocument.hash,
    operator_session_manifest_blob_name: sessionDocument.blobName,
    operator_session_manifest_sha256: sessionDocument.hash,
    research_promotion_bypassed: true,
    candidate_name: config.candidate,
    candidate_version: config.candidateVersion,
    candidate_config_hash: config.candidateConfigHash,
    required_fill_model_version: model.model_version,
    execution_model_blob_uri: model.blob_uri,
    execution_model_container_name: artifact.container,
    execution_model_blob_name: artifact.blobName,
    execution_model_sha256: hash(model.sha256),
    max_order_notional: config.maxOrderNotional,
    human_authorization_reference: sessionDocument.value.authorized_by_user_reference,
    authorized_at: now.toISOString(),
    expires_at: intent.value.valid_until,
    single_use: true
  };
  return {
    blobName: authorizationBlobName(config, sessionDocument.value, intent.value),
    value
  };
}

function authorizationBlobName(config, session, intent) {
  return `${config.controlPrefix}/sessions/${session.session_id}/authorizations/${intent.decision_id}.json`;
}

function completionBlobName(config, session, intent) {
  return `${config.controlPrefix}/sessions/${session.session_id}/completed/${intent.decision_id}.json`;
}

function childEnvironment(env, config, session, intent, authorization, childRunId) {
  return {
    ...env,
    EXECUTION_MODE: "funded_direct",
    ALLOW_LIVE: "false",
    ALLOW_STRATEGY_CANARY: "false",
    ALLOW_FUNDED_DIRECT: "true",
    ENABLE_TAKER_ORDERS: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "false",
    STRATEGY_CANARY_DRY_RUN: "false",
    STRATEGY_CANARY_RUN_ID: childRunId,
    STRATEGY_CANARY_MARKET_ID: intent.value.market_id,
    STRATEGY_CANARY_CONDITION_ID: intent.value.condition_id,
    STRATEGY_CANARY_TOKEN_ID: intent.value.token_id,
    STRATEGY_CANARY_MARKET_END_TS: intent.value.market_end_ts,
    STRATEGY_CANARY_INTENT_BLOB_NAME: intent.blobName,
    STRATEGY_CANARY_INTENT_SHA256: intent.hash,
    STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME: session.blobName,
    STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256: session.hash,
    STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME: authorization.blobName,
    STRATEGY_CANARY_AUTHORIZATION_SHA256: authorization.hash,
    STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI: session.value.execution_model.blob_uri,
    STRATEGY_CANARY_EXECUTION_MODEL_SHA256: session.value.execution_model.sha256,
    STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: session.value.execution_model.model_version,
    STRATEGY_CANARY_MAX_ORDER_NOTIONAL: String(config.maxOrderNotional),
    STRATEGY_CANARY_MIN_REMAINING_TTL_MS: String(config.childMinRemainingTtlMs),
    VENUE_PROBE_CAMPAIGN_CASH_FLOWS: "[]"
  };
}

function invokeCanaryChild(env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [new URL("./canary.mjs", import.meta.url).pathname], {
      env,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; process.stdout.write(chunk); });
    child.stderr.on("data", (chunk) => { stderr += chunk; process.stderr.write(chunk); });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (signal) return reject(new Error(`funded Dynamic Quote child terminated by ${signal}`));
      const records = [...stderr.trim().split("\n"), ...stdout.trim().split("\n")]
        .reverse()
        .map((line) => {
          try { return JSON.parse(line); } catch { return null; }
        })
        .filter(Boolean);
      resolve({
        exitCode: code ?? 1,
        error: records.find((record) => record.error)?.error || "",
        orderSubmissionAttempted: records.some((record) => record.order_submission_attempted === true)
      });
    });
  });
}

async function writeCompletion(container, config, selected, authorization, childRunId, now, details) {
  return putImmutableOrVerify(container, {
    blobName: completionBlobName(config, config.session, selected.value),
    value: {
      schema: "polyedge.operator_funded_intent_completion.v1",
      session_id: config.session.session_id,
      decision_id: selected.value.decision_id,
      authorization_blob_name: authorization.blobName,
      authorization_sha256: authorization.hash,
      child_run_id: childRunId,
      completed_at: now.toISOString(),
      ...details
    }
  });
}

async function readJsonBlob(container, blobName) {
  const response = await container.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  try { return JSON.parse(bytes.toString("utf8")); }
  catch { throw new Error(`fail closed: durable funded control blob is not valid JSON (${blobName})`); }
}

async function putImmutableOrVerify(container, document) {
  const bytes = Buffer.from(JSON.stringify(document.value, null, 2));
  const expected = sha256(bytes);
  try {
    await container.getBlockBlobClient(document.blobName).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error.statusCode))) throw error;
    const existing = await loadHashedJson(container, document.blobName, expected);
    return existing;
  }
  return { value: document.value, blobName: document.blobName, hash: expected };
}

function result(status, config, details) {
  return {
    schema: "polyedge.funded_direct_worker.v1",
    status,
    session_id: config.session.session_id,
    candidate: config.candidate,
    candidate_version: config.candidateVersion,
    max_account_loss: config.session.max_account_loss,
    target_order_notional: config.targetOrderNotional,
    max_order_notional: config.maxOrderNotional,
    execution_window_seconds_to_expiry: {
      minimum: config.minimumSecondsToExpiry,
      maximum: config.maximumSecondsToExpiry
    },
    no_deposits: true,
    no_replenishment: true,
    no_compounding: true,
    external_cash_flow_count: 0,
    research_promotion_bypassed: true,
    ...details
  };
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function clean(value) { return String(value || "").trim(); }
function hash(value) {
  const text = clean(value).toLowerCase();
  const prefixed = text.startsWith("sha256:") ? text : `sha256:${text}`;
  return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : "";
}
function integer(value, fallback) { const parsed = Number(value); return Number.isInteger(parsed) ? parsed : fallback; }
function number(value) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : 0; }
function parseJson(value) {
  try { return JSON.parse(value); } catch { return null; }
}
function hasMinimumRemainingTtl(intent, now, minimumMs) {
  return Date.parse(intent?.valid_until) - now.getTime() >= minimumMs;
}
function delay(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function runId() { return `funded-direct-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`; }

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runFundedDirectWorker()
    .then((value) => console.log(JSON.stringify(sanitize(value))))
    .catch((error) => {
      process.exitCode = 1;
      console.error(JSON.stringify({
        schema: "polyedge.funded_direct_worker.v1",
        status: "failed_closed",
        error: error.message
      }));
    });
}
