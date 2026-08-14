import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import {
  artifactLocationFromUri,
  loadHashedJson,
  sha256,
  VENUE_GTD_SECURITY_BUFFER_MS
} from "./canary-lib.mjs";
import {
  validateProtectedCompoundingManifest,
  validateProtectedCompoundingPredecessorState
} from "./compounding-risk.mjs";
import { sanitize, storageContainer } from "./lib.mjs";
import { validateProfitQuarantineManifest } from "./profit-quarantine.mjs";

const EXPECTED_CANDIDATE = "dynamic_quote_style";
const EXPECTED_VERSION = "dynamic_quote_style@2026-06-14";
const SESSION_SCHEMA_V1 = "polyedge.operator_funded_session.v1";
const SESSION_SCHEMA_V2 = "polyedge.operator_funded_session.v2";
const SESSION_SCHEMA_V3 = "polyedge.operator_funded_session.v3";
const AUTHORIZATION_SCHEMA = "polyedge.operator_funded_intent_authorization.v1";
const EXECUTION_HANDOFF_TTL_MS = 10_000;
const MAX_INTENT_TTL_MS = 30_000;
const MAX_PREFLIGHT_IDLE_MS = 10_800_000;

export function loadFundedDirectConfig(env = process.env) {
  const sessionJson = String(env.FUNDED_DIRECT_SESSION_MANIFEST_JSON || "").trim();
  const session = parseJson(sessionJson);
  const intentPrefix = clean(env.STRATEGY_CANARY_INTENT_PREFIX).replace(/^\/+|\/+$/g, "");
  const config = {
    enabled: env.FUNDED_DIRECT_WORKER_ENABLED === "true",
    allowed: env.ALLOW_FUNDED_DIRECT === "true",
    dryRun: env.FUNDED_DIRECT_DRY_RUN !== "false",
    preflightOnly: env.FUNDED_DIRECT_PREFLIGHT_ONLY === "true",
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
    intentPrefix,
    currentIntentBlobName: currentIntentHandoffBlobName(intentPrefix),
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
  if (config.preflightOnly && config.session?.schema_version !== SESSION_SCHEMA_V3) {
    errors.push("FUNDED_DIRECT_PREFLIGHT_ONLY requires an exact predecessor-bound v3 session");
  }
  if (config.preflightOnly && env.FUNDED_DIRECT_DRY_RUN !== "true") {
    errors.push("FUNDED_DIRECT_PREFLIGHT_ONLY requires FUNDED_DIRECT_DRY_RUN=true");
  }
  if (config.preflightOnly && String(env.FUNDED_DIRECT_ENGINE || "") === "persistent_v1") {
    errors.push("FUNDED_DIRECT_PREFLIGHT_ONLY is supported only by the one-shot worker");
  }
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
  const maxIterations = config.preflightOnly ? 10_801 : 2_000;
  if (!(config.maxIterations >= 1 && config.maxIterations <= maxIterations)) {
    errors.push("FUNDED_DIRECT_MAX_ITERATIONS must be in [1, " + maxIterations + "]");
  }
  if (!(config.pollIntervalMs >= 1_000 && config.pollIntervalMs <= 60_000)) errors.push("FUNDED_DIRECT_POLL_INTERVAL_MS must be in [1000, 60000]");
  if (config.preflightOnly && config.pollIntervalMs !== 1_000) {
    errors.push("FUNDED_DIRECT_PREFLIGHT_ONLY requires a 1000ms poll interval");
  }
  const maxIdleMs = config.preflightOnly ? MAX_PREFLIGHT_IDLE_MS : 3_600_000;
  if (config.preflightOnly && config.maxIdleMs === MAX_PREFLIGHT_IDLE_MS &&
      (config.maxIterations - 1) * config.pollIntervalMs < config.maxIdleMs) {
    errors.push("three-hour preflight requires enough iterations to reach its idle timeout");
  }
  if (!(config.maxIdleMs >= config.pollIntervalMs && config.maxIdleMs <= maxIdleMs)) {
    errors.push(`FUNDED_DIRECT_MAX_IDLE_MS must be between the poll interval and ${maxIdleMs}`);
  }
  if (!(config.minRemainingTtlMs >= 5_000 && config.minRemainingTtlMs <= MAX_INTENT_TTL_MS)) {
    errors.push("FUNDED_DIRECT_MIN_REMAINING_TTL_MS must be in [5000, 30000]");
  }
  if (!(config.childMinRemainingTtlMs >= 1_000 && config.childMinRemainingTtlMs <= config.minRemainingTtlMs)) {
    errors.push("FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS must be in [1000, FUNDED_DIRECT_MIN_REMAINING_TTL_MS]");
  }
  if (String(env.FUNDED_DIRECT_ENGINE || "") === "persistent_v1" &&
      (config.minRemainingTtlMs !== 7_000 || config.childMinRemainingTtlMs !== 2_000)) {
    errors.push("persistent_v1 requires the reviewed 7000ms admission and 2000ms child TTL gates");
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
  const { sessionDocument, predecessorDocument } = await loadWorkerBindings(clients.control, config);

  let idleSince = null;
  let childInvocations = 0;
  const intentScan = intentScanDiagnostics();
  for (let iteration = 1; iteration <= config.maxIterations; iteration += 1) {
    const scan = await firstFreshIntent(clients, config, sessionDocument.value, clock(), clock);
    mergeIntentScanDiagnostics(intentScan, scan.diagnostics);
    const selected = scan.selected;
    if (!selected) {
      idleSince ??= clock().getTime();
      if (clock().getTime() - idleSince >= config.maxIdleMs) {
        if (config.preflightOnly) {
          throw new Error("funded_direct_worker blocked: preflight timed out waiting for a fresh current intent");
        }
        return result("idle_waiting_for_fresh_intent", config, {
          iteration,
          childInvocations,
          intent_scan: intentScan
        });
      }
      await sleep(config.pollIntervalMs);
      continue;
    }
    idleSince = null;
    if (config.preflightOnly) {
      return preflightResult(config, sessionDocument, predecessorDocument, selected, {
        iteration,
        childInvocations
      });
    }
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
  if (config.preflightOnly) {
    throw new Error("funded_direct_worker blocked: preflight exhausted its iteration limit waiting for a fresh current intent");
  }
  return result("iteration_limit_reached", config, {
    iteration: config.maxIterations,
    childInvocations,
    intent_scan: intentScan
  });
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
      const selected = await selectedFromHandoff(
        clients,
        config,
        sessionDocument.value,
        handoff,
        clock(),
        { clock }
      );
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
              orderSubmissionAttempted: error?.orderSubmissionAttempted === true,
              executionEvidence: error?.executionEvidence || null
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
    },
    async rejectBusy(handoff) {
      const selected = await selectedFromHandoff(
        clients,
        config,
        sessionDocument.value,
        handoff,
        clock(),
        { clock }
      );
      if (selected.duplicateCompletion) {
        return result("already_completed_idempotent", config, {
          iteration: 1,
          childInvocations: 0,
          decisionId: selected.value.decision_id,
          completion: selected.duplicateCompletion
        });
      }
      const completion = await writeBusyCompletion(clients.control, config, selected, clock());
      return result("one_workflow_busy", config, {
        iteration: 1,
        childInvocations: 0,
        decisionId: selected.value.decision_id,
        completion: completion.value
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
  if (!hasMinimumRemainingTtl(selected.value, authorizationNow, config.childMinRemainingTtlMs)) {
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
    if (child.orderSubmissionAttempted === true) {
      const terminalReservation = await loadTerminalNoFillReservation(
        clients.control,
        selected,
        authorization,
        clock()
      );
      if (terminalReservation) {
        await writeCompletion(clients.control, config, selected, authorization, childRunId, clock(), {
          status: "child_completed",
          order_submission_attempted: true,
          authorization_consumed: true,
          risk_reservation_created: true,
          evidence_upload_status: "degraded_post_submission",
          terminal_risk_state: terminalReservation.state,
          order_id: terminalReservation.order_id,
          matched_notional: 0,
          reconciliation_complete: true,
          zero_open_orders_confirmed: true,
          post_submission_error: child.error
        });
        executionTiming.completion_persisted_wall_ms = Date.now();
        executionTiming.completion_persisted_monotonic_ms = performance.now();
        return {
          childInvocations,
          result: null,
          execution: {
            status: "terminal_no_fill_evidence_degraded",
            order_submission_attempted: true,
            order_submitted: true,
            lifecycle: {
              order_id: terminalReservation.order_id,
              send_wall_ms: null,
              matched_notional: 0,
              reconciliation_complete: true,
              zero_open_orders_confirmed: true
            }
          }
        };
      }
      const evidence = child.executionEvidence;
      await writeCompletion(clients.control, config, selected, authorization, childRunId, clock(), {
        status: "child_failed_closed_post_submission_unresolved",
        order_submission_attempted: true,
        authorization_consumed: true,
        risk_reservation_created: true,
        order_id: evidence?.lifecycle?.order_id || null,
        matched_notional: Number(evidence?.lifecycle?.matched_notional || 0),
        reconciliation_complete: false,
        zero_open_orders_confirmed: evidence?.lifecycle?.zero_open_orders_confirmed === true,
        post_submission_error: child.error || "unknown post-submission failure"
      });
      executionTiming.completion_persisted_wall_ms = Date.now();
      executionTiming.completion_persisted_monotonic_ms = performance.now();
      return {
        childInvocations,
        result: result("paused_by_account_risk_state", config, {
          iteration,
          childInvocations,
          decisionId: selected.value.decision_id,
          error: child.error || "unknown post-submission failure",
          execution: evidence || {
            status: "post_submission_unresolved",
            order_submission_attempted: true,
            order_submitted: true,
            lifecycle: null
          }
        })
      };
    }
    if (/existing_unresolved_position_blocks_submission|unresolved_risk_reservation|equity_floor_breached|campaign_drawdown_exhausted|projected_equity_floor_breach|projected_campaign_drawdown_breach|authorized_starting_collateral|external_cash_flow_record|protected_reserve|protected_order|operable_capital/.test(child.error || "")) {
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
    await writeCompletion(clients.control, config, selected, authorization, childRunId, clock(), {
      status: "child_failed_closed_pre_submission",
      order_submission_attempted: false,
      authorization_consumed: false,
      risk_reservation_created: false,
      error: child.error || "unknown pre-submission failure"
    });
    executionTiming.completion_persisted_wall_ms = Date.now();
    executionTiming.completion_persisted_monotonic_ms = performance.now();
    return {
      childInvocations,
      result: result("child_failed_closed_pre_submission", config, {
        iteration,
        childInvocations,
        decisionId: selected.value.decision_id,
        error: child.error || "unknown pre-submission failure"
      })
    };
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

async function selectedFromHandoff(
  clients,
  config,
  session,
  handoff,
  now,
  { readOnly = false, clock = () => now } = {}
) {
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
      !expectedHash) {
    throw new Error("fail closed: funded intent handoff binding is invalid");
  }
  if (!Number.isFinite(Date.parse(handoff.decision_ts)) ||
      Date.parse(handoff.decision_ts) > now.getTime()) {
    throw new Error("fail closed: funded intent handoff decision time is invalid");
  }
  const initialRemainingTtlMs = Date.parse(handoff.valid_until) - now.getTime();
  if (!Number.isFinite(initialRemainingTtlMs) || initialRemainingTtlMs < config.minRemainingTtlMs) {
    throw fundedHandoffRejection(
      "remaining_ttl",
      "fail closed: funded intent handoff has insufficient remaining TTL",
      initialRemainingTtlMs
    );
  }
  const response = await clients.intents.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  const downloadedAt = clock();
  const remainingTtlMs = Date.parse(handoff.valid_until) - downloadedAt.getTime();
  if (!Number.isFinite(remainingTtlMs) || remainingTtlMs < config.childMinRemainingTtlMs) {
    throw fundedHandoffRejection(
      "remaining_ttl",
      "fail closed: funded intent handoff has insufficient child TTL after immutable intent download",
      remainingTtlMs
    );
  }
  const actualHash = sha256(bytes);
  if (actualHash !== expectedHash) throw new Error("fail closed: funded intent handoff SHA-256 mismatch");
  let value;
  try { value = JSON.parse(bytes.toString("utf8")); }
  catch { throw new Error("fail closed: funded intent handoff blob is not valid JSON"); }
  if (value.decision_id !== decisionId ||
      value.decision_ts !== handoff.decision_ts ||
      value.valid_until !== handoff.valid_until ||
      !qualifies(
        value,
        blobName,
        actualHash,
        config,
        session,
        downloadedAt,
        config.childMinRemainingTtlMs
      )) {
    throw new Error("fail closed: funded intent handoff does not qualify for execution");
  }
  const authorizationName = authorizationBlobName(config, session, value);
  const completionName = completionBlobName(config, session, value);
  const [authorizationExists, completionExists] = await Promise.all([
    clients.control.getBlobClient(authorizationName).exists(),
    clients.control.getBlobClient(completionName).exists()
  ]);
  if (readOnly) {
    if (authorizationExists || completionExists) {
      throw new Error("fail closed: preflight handoff already has durable execution state");
    }
    return {
      value,
      blobName,
      hash: actualHash,
      decisionMs: Date.parse(value.decision_ts)
    };
  }
  if (completionExists) {
    const completion = await readJsonBlob(clients.control, completionName);
    const busyCompletion = completion?.schema === "polyedge.operator_funded_intent_completion.v1" &&
      completion.session_id === session.session_id &&
      completion.decision_id === value.decision_id &&
      completion.intent_blob_name === blobName &&
      hash(completion.intent_sha256) === hash(actualHash) &&
      completion.status === "one_workflow_busy" &&
      completion.authorization_blob_name === null &&
      completion.authorization_sha256 === null &&
      completion.order_submission_attempted === false &&
      completion.authorization_consumed === false &&
      completion.risk_reservation_created === false;
    const authorizedCompletion = authorizationExists &&
      completion?.schema === "polyedge.operator_funded_intent_completion.v1" &&
      completion.session_id === session.session_id &&
      completion.decision_id === value.decision_id &&
      completion.authorization_blob_name === authorizationName &&
      [
        "child_completed",
        "expired_before_child_launch",
        "child_failed_closed_pre_submission",
        "child_failed_closed_post_submission_unresolved"
      ].includes(completion.status);
    if (busyCompletion || authorizedCompletion) {
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
    throw new Error("fail closed: funded intent completion is not exactly bound");
  }
  if (authorizationExists) {
    const authorization = await readJsonBlobDocument(clients.control, authorizationName);
    assertExistingAuthorizationBinding(authorization, config, session, {
      value,
      blobName,
      hash: actualHash
    });
    const terminalReservation = await loadTerminalNoFillReservation(
      clients.control,
      { value, blobName, hash: actualHash },
      authorization,
      now
    );
    if (terminalReservation) {
      const completion = await writeCompletion(
        clients.control,
        config,
        { value, blobName, hash: actualHash },
        authorization,
        authorization.value.child_run_id,
        now,
        {
          status: "child_completed",
          order_submission_attempted: true,
          authorization_consumed: true,
          risk_reservation_created: true,
          evidence_upload_status: "degraded_post_submission",
          terminal_risk_state: terminalReservation.state,
          order_id: terminalReservation.order_id,
          matched_notional: 0,
          reconciliation_complete: true,
          zero_open_orders_confirmed: true,
          post_submission_error: "recovered from durable terminal reservation after incomplete handoff"
        }
      );
      return {
        value,
        blobName,
        hash: actualHash,
        decisionMs: Date.parse(value.decision_ts),
        duplicateCompletion: completion.value,
        handoffTiming: {
          hash_verification_started_wall_ms: verificationStartedWallMs,
          hash_verification_started_monotonic_ms: verificationStartedMonotonicMs,
          hash_verified_wall_ms: Date.now(),
          hash_verified_monotonic_ms: performance.now()
        }
      };
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
  let capitalModeValid = false;
  if ([SESSION_SCHEMA_V2, SESSION_SCHEMA_V3].includes(value?.schema_version)) {
    try {
      validateProtectedCompoundingManifest(value);
      capitalModeValid = value.allow_compounding === true;
    } catch {
      capitalModeValid = false;
    }
  } else if (value?.schema_version === SESSION_SCHEMA_V1) {
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
    capitalModeValid = value.allow_compounding === false && profitQuarantineValid;
  }
  const valid = [SESSION_SCHEMA_V1, SESSION_SCHEMA_V2, SESSION_SCHEMA_V3]
    .includes(value?.schema_version)
    && clean(value.session_id)
    && value.authorization_mode === "operator_direct"
    && clean(value.authorized_by_user_reference)
    && value.research_promotion_bypassed === true
    && value.research_lane_isolated === true
    && value.maker_only === true
    && value.no_deposits === true
    && value.allow_automatic_replenishment === false
    && capitalModeValid
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

async function firstFreshIntent(clients, config, session, now, clock = () => now) {
  if (config.preflightOnly) return currentPreflightIntent(clients, config, session, now, clock);
  const candidates = [];
  const diagnostics = intentScanDiagnostics();
  diagnostics.scans = 1;
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
    diagnostics.candidate_blobs += 1;
    const response = await clients.intents.getBlobClient(blob.name).download();
    const bytes = await streamToBuffer(response.readableStreamBody);
    let value;
    try { value = JSON.parse(bytes.toString("utf8")); }
    catch {
      recordIntentRejection(diagnostics, "invalid_json");
      continue;
    }
    const rejection = qualificationRejection(value, blob.name, sha256(bytes), config, session, now);
    if (rejection) {
      recordIntentRejection(diagnostics, rejection, Date.parse(value.valid_until) - now.getTime());
      continue;
    }
    const authorizationName = authorizationBlobName(config, session, value);
    if (await clients.control.getBlobClient(authorizationName).exists()) {
      recordIntentRejection(diagnostics, "existing_grant", Date.parse(value.valid_until) - now.getTime());
      continue;
    }
    candidates.push({ value, blobName: blob.name, hash: sha256(bytes), decisionMs: Date.parse(value.decision_ts) });
  }
  candidates.sort((left, right) => right.decisionMs - left.decisionMs || left.blobName.localeCompare(right.blobName));
  return { selected: candidates[0] || null, diagnostics };
}

async function currentPreflightIntent(clients, config, session, now, clock) {
  const diagnostics = intentScanDiagnostics();
  diagnostics.scans = 1;
  let response;
  try {
    response = await clients.intents.getBlobClient(config.currentIntentBlobName).download();
  } catch (error) {
    if (Number(error.statusCode) === 404) return { selected: null, diagnostics };
    throw error;
  }
  diagnostics.candidate_blobs = 1;
  const bytes = await streamToBuffer(response.readableStreamBody);
  let handoff;
  try { handoff = JSON.parse(bytes.toString("utf8")); }
  catch { throw new Error("fail closed: funded current intent handoff is not valid JSON"); }

  const decisionId = clean(handoff?.decision_id);
  const decisionMs = Date.parse(handoff?.decision_ts);
  const validUntilMs = Date.parse(handoff?.valid_until);
  const remainingTtlMs = validUntilMs - now.getTime();
  const exactKeys = [
    "schema",
    "decision_id",
    "intent_blob_name",
    "intent_sha256",
    "decision_ts",
    "valid_until"
  ];
  if (handoff?.schema !== "polyedge.funded_intent_handoff.v1" ||
      !handoff || typeof handoff !== "object" || Array.isArray(handoff) ||
      Object.keys(handoff).length !== exactKeys.length ||
      exactKeys.some((key) => !Object.prototype.hasOwnProperty.call(handoff, key)) ||
      !/^[0-9a-f]{64}$/.test(decisionId) ||
      clean(handoff?.intent_blob_name) !== `${config.intentPrefix}/${decisionId}.json` ||
      !hash(handoff?.intent_sha256)) {
    throw new Error("fail closed: funded current intent pointer binding is invalid");
  }
  if (!Number.isFinite(decisionMs) || decisionMs > now.getTime()) {
    throw new Error("fail closed: funded current intent pointer decision time is invalid");
  }
  if (decisionMs < Date.parse(session.created_at)) {
    recordIntentRejection(diagnostics, "decision_time", remainingTtlMs);
    return { selected: null, diagnostics };
  }
  if (!Number.isFinite(validUntilMs) || validUntilMs <= decisionMs) {
    throw new Error("fail closed: funded current intent pointer expiry is invalid");
  }
  if (remainingTtlMs < config.minRemainingTtlMs) {
    recordIntentRejection(diagnostics, "remaining_ttl", remainingTtlMs);
    return { selected: null, diagnostics };
  }

  let selected;
  try {
    selected = await selectedFromHandoff(
      clients,
      config,
      session,
      handoff,
      now,
      { readOnly: true, clock }
    );
  } catch (error) {
    if (error?.code !== "remaining_ttl") throw error;
    recordIntentRejection(
      diagnostics,
      "remaining_ttl",
      error.remainingTtlMs
    );
    return { selected: null, diagnostics };
  }
  return {
    selected: { ...selected, handoffBlobName: config.currentIntentBlobName },
    diagnostics
  };
}

function currentIntentHandoffBlobName(intentPrefix) {
  const parts = String(intentPrefix || "").split("/");
  if (parts.pop() !== "intents") {
    throw new Error("funded_direct_worker blocked: intent prefix must end with the exact intents segment");
  }
  return [...parts, "current-funded-intent.json"].filter(Boolean).join("/");
}

function fundedHandoffRejection(code, message, remainingTtlMs = null) {
  const error = new Error(message);
  error.code = code;
  error.remainingTtlMs = remainingTtlMs;
  return error;
}

function qualificationRejection(
  intent,
  blobName,
  intentHash,
  config,
  session,
  now,
  minimumRemainingTtlMs = config.minRemainingTtlMs
) {
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
  if (!(intent?.schema === "polyedge.execution_intent.v1"
    && /^[0-9a-f]{64}$/.test(String(intent?.decision_id || ""))
    && blobName === `${config.intentPrefix}/${intent.decision_id}.json`
    && hash(intentHash))) return "schema_binding";
  if (!(intent.candidate_name === config.candidate
    && intent.candidate_version === config.candidateVersion
    && hash(intent.candidate_config_hash) === config.candidateConfigHash)) return "strategy_binding";
  if (!(intent.required_fill_model_version === session.execution_model.model_version
    && intent.execution_model_blob_uri === session.execution_model.blob_uri
    && hash(intent.execution_model_sha256) === hash(session.execution_model.sha256))) return "model_binding";
  if (!(intent.resolution_source === config.requiredResolutionSource
    && intent.exact_resolution_source === true
    && String(intent.side).toUpperCase() === "BUY"
    && intent.post_only === true
    && intent.order_kind === "post_only_gtd")) return "execution_policy";
  if (!(Number.isFinite(decisionMs) && decisionMs >= sessionStartMs && decisionMs <= nowMs)) return "decision_time";
  if (!(Number.isFinite(validUntilMs) && validUntilMs - nowMs >= minimumRemainingTtlMs)) return "remaining_ttl";
  if (!(validUntilMs <= sessionExpiryMs
    && Number.isFinite(venueExpiryMs)
    && venueExpiryMs === validUntilMs + VENUE_GTD_SECURITY_BUFFER_MS
    && Number(intent.ttl_ms) === EXECUTION_HANDOFF_TTL_MS
    && validUntilMs === decisionMs + Number(intent.ttl_ms))) return "expiry_binding";
  if (!(Number.isFinite(marketEndMs)
    && marketEndMs - decisionMs >= config.minimumSecondsToExpiry * 1_000
    && marketEndMs - decisionMs <= config.maximumSecondsToExpiry * 1_000
    && venueExpiryMs < marketEndMs)) return "market_window";
  if (!(Number.isFinite(price)
    && Number.isFinite(shares)
    && Number.isFinite(notional)
    && Number.isFinite(feeAllowance)
    && feeAllowance >= 0
    && Number.isFinite(reservedNotional)
    && notional > 1 + 1e-9
    && reservedNotional >= config.targetOrderNotional - 0.01 - 1e-9
    && reservedNotional <= config.targetOrderNotional + 1e-9
    && notional <= config.maxOrderNotional
    && Math.abs(price * shares - notional) <= 1e-9)) return "reserved_notional";
  if (!(shares >= Number(intent.minimum_order_size))) return "minimum_size";
  if (!(Number(intent.net_edge_lower_bound) > 0)) return "net_edge";
  return null;
}

function qualifies(intent, blobName, intentHash, config, session, now, minimumRemainingTtlMs) {
  return qualificationRejection(
    intent,
    blobName,
    intentHash,
    config,
    session,
    now,
    minimumRemainingTtlMs
  ) === null;
}

function intentScanDiagnostics() {
  return {
    scans: 0,
    candidate_blobs: 0,
    rejections: {
      invalid_json: 0,
      schema_binding: 0,
      strategy_binding: 0,
      model_binding: 0,
      execution_policy: 0,
      decision_time: 0,
      remaining_ttl: 0,
      expiry_binding: 0,
      market_window: 0,
      reserved_notional: 0,
      minimum_size: 0,
      net_edge: 0,
      existing_grant: 0
    },
    last_rejection: null,
    last_remaining_ttl_ms: null
  };
}

function recordIntentRejection(diagnostics, code, remainingTtlMs = null) {
  diagnostics.rejections[code] += 1;
  diagnostics.last_rejection = code;
  diagnostics.last_remaining_ttl_ms = Number.isFinite(remainingTtlMs)
    ? Math.max(-MAX_INTENT_TTL_MS, Math.min(MAX_INTENT_TTL_MS, Math.trunc(remainingTtlMs)))
    : null;
}

function mergeIntentScanDiagnostics(target, source) {
  target.scans += source.scans;
  target.candidate_blobs += source.candidate_blobs;
  for (const [code, count] of Object.entries(source.rejections)) target.rejections[code] += count;
  if (source.last_rejection) {
    target.last_rejection = source.last_rejection;
    target.last_remaining_ttl_ms = source.last_remaining_ttl_ms;
  }
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

async function writeBusyCompletion(container, config, selected, now) {
  return putImmutableOrVerify(container, {
    blobName: completionBlobName(config, config.session, selected.value),
    value: {
      schema: "polyedge.operator_funded_intent_completion.v1",
      session_id: config.session.session_id,
      decision_id: selected.value.decision_id,
      intent_blob_name: selected.blobName,
      intent_sha256: selected.hash,
      authorization_blob_name: null,
      authorization_sha256: null,
      child_run_id: null,
      completed_at: now.toISOString(),
      status: "one_workflow_busy",
      order_submission_attempted: false,
      authorization_consumed: false,
      risk_reservation_created: false
    }
  });
}

function assertExistingAuthorizationBinding(authorization, config, session, selected) {
  const value = authorization?.value;
  if (authorization?.blobName !== authorizationBlobName(config, session, selected.value) ||
      value?.schema !== AUTHORIZATION_SCHEMA ||
      value?.session_id !== session.session_id ||
      value?.decision_id !== selected.value.decision_id ||
      value?.intent_blob_name !== selected.blobName ||
      hash(value?.intent_sha256) !== hash(selected.hash) ||
      value?.candidate_name !== config.candidate ||
      value?.candidate_version !== config.candidateVersion ||
      hash(value?.candidate_config_hash) !== config.candidateConfigHash ||
      value?.single_use !== true ||
      !clean(value?.child_run_id)) {
    throw new Error("fail closed: funded intent handoff already has an authorization that is not exactly bound");
  }
}

async function loadTerminalNoFillReservation(container, selected, authorization, now) {
  const decisionId = selected.value.decision_id;
  const probeId = `funded-direct-${decisionId}`;
  const dates = [...new Set([
    String(selected.value.decision_ts || "").slice(0, 10),
    now.toISOString().slice(0, 10)
  ].filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value)))];
  for (const date of dates) {
    const blobName = `reports/research/venue-probe/risk-reservations/${date}/${probeId}.json`;
    if (!await container.getBlobClient(blobName).exists()) continue;
    const reservation = await readJsonBlob(container, blobName);
    const updatedMs = Date.parse(reservation?.updated_ts);
    if (reservation?.schema_version === 1 &&
        reservation?.probe_id === probeId &&
        reservation?.run_id === authorization.value.child_run_id &&
        reservation?.market_id === selected.value.market_id &&
        reservation?.condition_id === selected.value.condition_id &&
        reservation?.token_id === selected.value.token_id &&
        reservation?.order_submission_intended === true &&
        reservation?.order_submitted === true &&
        reservation?.state === "finalized_no_fill" &&
        /^0x[0-9a-f]{64}$/i.test(String(reservation?.order_id || "")) &&
        Number(reservation?.matched_notional) === 0 &&
        reservation?.reconciliation_complete === true &&
        reservation?.zero_open_orders_confirmed === true &&
        Number.isFinite(updatedMs) &&
        updatedMs >= Date.parse(selected.value.decision_ts) &&
        updatedMs <= now.getTime() + 60_000) {
      return reservation;
    }
  }
  return null;
}

async function readJsonBlob(container, blobName) {
  return (await readJsonBlobDocument(container, blobName)).value;
}

async function readJsonBlobDocument(container, blobName) {
  const response = await container.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  try {
    return {
      value: JSON.parse(bytes.toString("utf8")),
      blobName,
      hash: sha256(bytes)
    };
  }
  catch { throw new Error(`fail closed: durable funded control blob is not valid JSON (${blobName})`); }
}

async function loadWorkerBindings(control, config) {
  if (!config.preflightOnly) {
    const sessionDocument = await putImmutableOrVerify(control, {
      blobName: config.sessionBlobName,
      value: config.session
    });
    if (sessionDocument.hash !== config.sessionHash) {
      throw new Error("fail closed: operator session manifest SHA-256 mismatch");
    }
    return { sessionDocument, predecessorDocument: null };
  }

  const policy = validateProtectedCompoundingManifest(config.session);
  let predecessorDocument;
  try {
    predecessorDocument = await readJsonBlobDocument(control, policy.priorStateBlobName);
  } catch (error) {
    if (Number(error.statusCode) !== 404) throw error;
    validateProtectedCompoundingPredecessorState(null, policy);
  }
  validateProtectedCompoundingPredecessorState(
    predecessorDocument?.value,
    policy,
    predecessorDocument?.hash
  );
  return {
    sessionDocument: {
      value: config.session,
      blobName: config.sessionBlobName,
      hash: config.sessionHash
    },
    predecessorDocument
  };
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
    no_compounding: config.session.allow_compounding !== true,
    allow_compounding: config.session.allow_compounding === true,
    reserve_ratio: config.session.capital_policy?.reserve_ratio ?? null,
    operating_buffer_ratio: config.session.capital_policy?.operating_buffer_ratio ?? null,
    external_cash_flow_count: 0,
    research_promotion_bypassed: true,
    ...details
  };
}

function preflightResult(config, sessionDocument, predecessorDocument, selected, details) {
  return result("preflight_validated", config, {
    ...details,
    decisionId: selected.value.decision_id,
    preflight_only: true,
    writes_performed: false,
    order_submission_attempted: false,
    execution_grant_created: false,
    risk_reservation_created: false,
    completion_created: false,
    session_manifest_blob_name: sessionDocument.blobName,
    session_manifest_sha256: sessionDocument.hash,
    predecessor_state_session_id: predecessorDocument.value.session_id,
    predecessor_state_blob_name: predecessorDocument.blobName,
    predecessor_state_sha256: predecessorDocument.hash,
    intent_handoff_blob_name: selected.handoffBlobName,
    intent_blob_name: selected.blobName,
    intent_sha256: selected.hash
  });
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
