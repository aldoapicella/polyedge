import { artifactLocationFromUri, sha256 } from "./canary-lib.mjs";

const HUMAN_GRANT_SCHEMA = "polyedge.strategy_canary_human_grant.v1";
const INTENT_SCHEMA = "polyedge.execution_intent.v1";
const MANIFEST_SCHEMA = "promotion_manifest_v1";
const MAX_GRANT_WINDOW_MS = 5 * 60 * 1000;

export function loadControllerConfig(env = process.env) {
  const config = {
    enabled: boolean(env.STRATEGY_CANARY_CONTROLLER_ENABLED),
    allowCanary: boolean(env.ALLOW_STRATEGY_CANARY),
    dryRun: env.STRATEGY_CANARY_DRY_RUN !== "false",
    humanGrantBlobName: clean(env.STRATEGY_CANARY_HUMAN_GRANT_BLOB_NAME),
    humanGrantBlobHash: normalizeHash(env.STRATEGY_CANARY_HUMAN_GRANT_SHA256),
    manifestBlobName: clean(env.STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME),
    manifestBlobHash: normalizeHash(env.STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256),
    intentPrefix: clean(env.STRATEGY_CANARY_INTENT_PREFIX || "reports/research/venue-probe/control/strategy-canary/intents").replace(/^\/+|\/+$/g, ""),
    candidateName: clean(env.STRATEGY_CANARY_CANDIDATE_NAME || "dynamic_quote_style"),
    candidateVersion: clean(env.STRATEGY_CANARY_CANDIDATE_VERSION || "dynamic_quote_style@2026-06-14"),
    candidateConfigHash: normalizeHash(env.STRATEGY_CANARY_CANDIDATE_CONFIG_HASH),
    requiredFillModelVersion: clean(env.STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION),
    requiredResolutionSource: clean(env.STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE || "chainlink_reference"),
    maxOrderNotional: number(env.STRATEGY_CANARY_MAX_ORDER_NOTIONAL, 1),
    pollIntervalMs: integer(env.STRATEGY_CANARY_CONTROLLER_POLL_INTERVAL_MS, 5000),
    maxWaitMs: integer(env.STRATEGY_CANARY_CONTROLLER_MAX_WAIT_SECONDS, 300) * 1000,
    storageAccount: clean(env.AZURE_STORAGE_ACCOUNT_NAME),
    storageContainer: clean(env.AZURE_STORAGE_CONTAINER_NAME || "bot-events"),
    intentContainerName: clean(env.STRATEGY_CANARY_INTENT_CONTAINER_NAME || env.AZURE_STORAGE_CONTAINER_NAME || "bot-events"),
    manifestContainerName: clean(env.STRATEGY_CANARY_MANIFEST_CONTAINER_NAME || env.AZURE_STORAGE_CONTAINER_NAME || "bot-events"),
    storageAccountKey: env.AZURE_STORAGE_ACCOUNT_KEY,
    azureClientId: env.AZURE_CLIENT_ID
  };
  const errors = [];
  if (!config.enabled) errors.push("STRATEGY_CANARY_CONTROLLER_ENABLED must be true");
  if (!config.allowCanary) errors.push("ALLOW_STRATEGY_CANARY must be true");
  for (const [name, value] of [
    ["STRATEGY_CANARY_HUMAN_GRANT_BLOB_NAME", config.humanGrantBlobName],
    ["STRATEGY_CANARY_HUMAN_GRANT_SHA256", config.humanGrantBlobHash],
    ["STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME", config.manifestBlobName],
    ["STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256", config.manifestBlobHash],
    ["STRATEGY_CANARY_INTENT_PREFIX", config.intentPrefix],
    ["STRATEGY_CANARY_CANDIDATE_CONFIG_HASH", config.candidateConfigHash],
    ["STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION", config.requiredFillModelVersion]
  ]) if (!value) errors.push(`${name} is required`);
  if (!config.storageAccount) errors.push("AZURE_STORAGE_ACCOUNT_NAME is required");
  if (!config.intentContainerName) errors.push("STRATEGY_CANARY_INTENT_CONTAINER_NAME is required");
  if (!config.manifestContainerName) errors.push("STRATEGY_CANARY_MANIFEST_CONTAINER_NAME is required");
  if (!(config.maxOrderNotional > 0 && config.maxOrderNotional <= 1)) errors.push("STRATEGY_CANARY_MAX_ORDER_NOTIONAL must be in (0, 1]");
  if (!(config.pollIntervalMs >= 1000 && config.pollIntervalMs <= 10_000)) errors.push("controller poll interval must be in [1000, 10000] ms");
  if (!(config.maxWaitMs > 0 && config.maxWaitMs <= MAX_GRANT_WINDOW_MS)) errors.push("controller wait must be in (0, 300] seconds");
  if (errors.length) throw new Error(`strategy_canary_controller blocked: ${errors.join("; ")}`);
  return config;
}

export function validateHumanGrant({ config, manifest, grant, now = new Date() }) {
  const fail = (message) => { throw new Error(`fail closed: ${message}`); };
  const nowMs = now.getTime();
  if (manifest?.schema_version !== MANIFEST_SCHEMA || manifest.phase !== "shadow_passed" || manifest.gate_metrics?.phase !== "shadow_passed") fail("promotion manifest phase is not shadow_passed");
  if (manifest.gate_metrics?.promotion_allowed !== true || manifest.human_authorization_required !== true) fail("promotion gates are not passing or do not require human authorization");
  // Research output must never arm itself. The separate, exact human grant is
  // the only authorization transition accepted by this controller.
  if (manifest.promotion_allowed !== false) fail("promotion manifest must remain non-executable");
  if (manifest.candidate?.name !== config.candidateName || manifest.candidate?.candidate_version !== config.candidateVersion || manifest.candidate?.config_hash !== config.candidateConfigHash) fail("promotion manifest candidate identity mismatch");
  if (!manifest.execution_model?.blob_uri || !normalizeHash(manifest.execution_model?.sha256) || manifest.execution_model?.model_version !== config.requiredFillModelVersion) fail("promotion manifest exact execution model binding is invalid");
  const manifestCreatedMs = Date.parse(manifest.created_at);
  const manifestExpiresMs = Date.parse(manifest.expires_at);
  if (!Number.isFinite(manifestCreatedMs) || !Number.isFinite(manifestExpiresMs) || manifestCreatedMs > nowMs || manifestExpiresMs <= nowMs) fail("promotion manifest is stale or invalid");

  if (grant?.schema !== HUMAN_GRANT_SCHEMA || grant.single_use !== true) fail("invalid human grant schema or reuse policy");
  for (const field of ["grant_id", "human_authorization_reference", "authorized_at", "expires_at"]) if (!clean(grant?.[field])) fail(`human grant ${field} is required`);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,95}$/.test(grant.grant_id)) fail("unsafe human grant id");
  if (grant.selection_policy !== "first_fresh_after_authorized_at" || clean(grant.intent_prefix).replace(/^\/+|\/+$/g, "") !== config.intentPrefix) fail("human grant intent selection is broader than the configured first-fresh prefix");
  if (grant.promotion_manifest_blob_name !== config.manifestBlobName || normalizeHash(grant.promotion_manifest_sha256) !== config.manifestBlobHash) fail("human grant promotion-manifest binding mismatch");
  if (grant.candidate_name !== config.candidateName || grant.candidate_version !== config.candidateVersion || grant.candidate_config_hash !== config.candidateConfigHash || grant.required_fill_model_version !== config.requiredFillModelVersion) fail("human grant candidate or fill-model binding mismatch");
  if (grant.execution_model_blob_uri !== manifest.execution_model.blob_uri || normalizeHash(grant.execution_model_sha256) !== normalizeHash(manifest.execution_model.sha256)) fail("human grant exact execution model binding mismatch");
  const maxNotional = Number(grant.max_order_notional);
  if (!(maxNotional > 0 && maxNotional <= config.maxOrderNotional && maxNotional <= 1)) fail("human grant order cap is invalid");
  const authorizedMs = Date.parse(grant.authorized_at);
  const expiresMs = Date.parse(grant.expires_at);
  if (!Number.isFinite(authorizedMs) || !Number.isFinite(expiresMs) || authorizedMs > nowMs || expiresMs <= nowMs || expiresMs <= authorizedMs || expiresMs - authorizedMs > MAX_GRANT_WINDOW_MS) fail("human grant is stale or exceeds the five-minute window");
  if (expiresMs > manifestExpiresMs) fail("human grant outlives its promotion manifest");
  return { authorizedMs, expiresMs, maxNotional };
}

export function selectFirstQualifiedIntent({ config, grant, candidates, now = new Date() }) {
  const authorizedMs = Date.parse(grant.authorized_at);
  const nowMs = now.getTime();
  const qualified = candidates.flatMap((candidate) => {
    const intent = candidate?.value;
    const decisionMs = Date.parse(intent?.decision_ts);
    const validUntilMs = Date.parse(intent?.valid_until);
    const venueExpiryMs = Date.parse(intent?.gtd_expiry_ts);
    const notional = Number(intent?.notional);
    const shares = Number(intent?.shares);
    const price = Number(intent?.price);
    const minimumOrderSize = Number(intent?.minimum_order_size);
    const ttlMs = Number(intent?.ttl_ms);
    const exactName = `${config.intentPrefix}/${intent?.decision_id}.json`;
    const valid = intent?.schema === INTENT_SCHEMA
      && /^[0-9a-f]{64}$/.test(String(intent?.decision_id || ""))
      && candidate.blobName === exactName
      && normalizeHash(candidate.hash)
      && intent.candidate_name === config.candidateName
      && intent.candidate_version === config.candidateVersion
      && intent.candidate_config_hash === config.candidateConfigHash
      && intent.required_fill_model_version === config.requiredFillModelVersion
      && intent.execution_model_blob_uri === grant.execution_model_blob_uri
      && normalizeHash(intent.execution_model_sha256) === normalizeHash(grant.execution_model_sha256)
      && intent.resolution_source === config.requiredResolutionSource
      && intent.exact_resolution_source === true
      && String(intent.side).toUpperCase() === "BUY"
      && intent.post_only === true
      && intent.order_kind === "post_only_gtd"
      && Number.isFinite(decisionMs) && decisionMs >= authorizedMs && decisionMs <= nowMs
      && Number.isFinite(validUntilMs) && validUntilMs > nowMs
      && Number.isFinite(venueExpiryMs) && venueExpiryMs === validUntilMs + 60_000
      && Number.isFinite(ttlMs) && ttlMs > 0 && ttlMs <= 30_000 && validUntilMs === decisionMs + ttlMs
      && Number.isFinite(notional) && notional > 0 && notional <= Number(grant.max_order_notional) && notional <= 1
      && Number.isFinite(shares) && Number.isFinite(price) && Number.isFinite(minimumOrderSize)
      && minimumOrderSize > 0 && shares >= minimumOrderSize && Math.abs(shares * price - notional) <= 1e-9
      && Number(intent.net_edge_lower_bound) > 0;
    return valid ? [{ ...candidate, decisionMs }] : [];
  });
  qualified.sort((left, right) => left.decisionMs - right.decisionMs || left.blobName.localeCompare(right.blobName));
  return qualified[0] || null;
}

export async function consumeHumanGrant(container, { config, grant, grantHash, selected, runId, now = new Date() }) {
  const blobName = `reports/research/venue-probe/control/strategy-canary/human-grants/consumed/${grant.grant_id}.json`;
  const payload = {
    schema: "polyedge.strategy_canary_human_grant_consumption.v1",
    grant_id: grant.grant_id,
    human_grant_sha256: normalizeHash(grantHash),
    promotion_manifest_container_name: config.manifestContainerName,
    promotion_manifest_blob_name: config.manifestBlobName,
    promotion_manifest_sha256: config.manifestBlobHash,
    selected_intent_container_name: config.intentContainerName,
    selected_intent_blob_name: selected.blobName,
    selected_intent_sha256: normalizeHash(selected.hash),
    decision_id: selected.value.decision_id,
    run_id: runId,
    consumed_at: now.toISOString(),
    consumption_blob_name: blobName
  };
  const bytes = Buffer.from(JSON.stringify(payload, null, 2));
  try {
    await container.getBlockBlobClient(blobName).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if ([409, 412].includes(Number(error.statusCode))) throw new Error("fail closed: human grant was already consumed");
    throw error;
  }
  return { value: payload, blobName, hash: sha256(bytes) };
}

export async function createIntentBoundAuthorization(container, { config, grant, grantHash, selected, consumption, now = new Date() }) {
  const expiresAt = new Date(Math.min(Date.parse(grant.expires_at), Date.parse(selected.value.valid_until)));
  if (!(expiresAt.getTime() > now.getTime())) throw new Error("fail closed: selected intent expired before internal authorization creation");
  const modelArtifact = artifactLocationFromUri(grant.execution_model_blob_uri, config.storageAccount);
  const authorization = {
    schema: "polyedge.strategy_canary_authorization.v1",
    authorization_id: `${grant.grant_id}-${selected.value.decision_id.slice(0, 16)}`,
    decision_id: selected.value.decision_id,
    intent_blob_name: selected.blobName,
    intent_sha256: normalizeHash(selected.hash),
    promotion_manifest_blob_name: config.executionManifestBlobName || config.manifestBlobName,
    promotion_manifest_sha256: config.executionManifestBlobHash || config.manifestBlobHash,
    promotion_manifest_container_name: config.storageContainer,
    source_promotion_manifest_container_name: config.manifestContainerName,
    source_promotion_manifest_blob_name: config.manifestBlobName,
    source_promotion_manifest_sha256: config.manifestBlobHash,
    intent_container_name: config.intentContainerName,
    human_grant_id: grant.grant_id,
    human_grant_sha256: normalizeHash(grantHash),
    human_grant_consumption_blob_name: consumption.blobName,
    human_grant_consumption_sha256: consumption.hash,
    candidate_name: config.candidateName,
    candidate_version: config.candidateVersion,
    candidate_config_hash: config.candidateConfigHash,
    required_fill_model_version: config.requiredFillModelVersion,
    execution_model_blob_uri: grant.execution_model_blob_uri,
    execution_model_sha256: normalizeHash(grant.execution_model_sha256),
    execution_model_container_name: modelArtifact.container,
    execution_model_blob_name: modelArtifact.blobName,
    human_authorization_reference: grant.human_authorization_reference,
    authorized_at: now.toISOString(),
    expires_at: expiresAt.toISOString(),
    single_use: true
  };
  const blobName = `reports/research/venue-probe/control/strategy-canary/authorizations/${grant.grant_id}/${selected.value.decision_id}.json`;
  const bytes = Buffer.from(JSON.stringify(authorization, null, 2));
  try {
    await container.getBlockBlobClient(blobName).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if ([409, 412].includes(Number(error.statusCode))) throw new Error("fail closed: intent-bound authorization already exists after human-grant consumption");
    throw error;
  }
  return { value: authorization, blobName, hash: sha256(bytes) };
}

export async function createCanaryReadyManifest(container, { config, sourceManifest, grant, grantHash, consumption, now = new Date() }) {
  const expiresAt = new Date(Math.min(Date.parse(sourceManifest.expires_at), Date.parse(grant.expires_at)));
  if (!(expiresAt.getTime() > now.getTime())) throw new Error("fail closed: source promotion evidence expired before canary transition");
  const manifest = {
    ...sourceManifest,
    phase: "canary_ready",
    gate_metrics: { ...sourceManifest.gate_metrics, phase: "canary_ready" },
    // The manifest remains non-executable by itself. Only the separately
    // consumed exact authorization allows the child executor to proceed.
    promotion_allowed: false,
    created_at: now.toISOString(),
    expires_at: expiresAt.toISOString(),
    controller_transition: {
      schema: "polyedge.strategy_canary_transition.v1",
      source_manifest_blob_name: config.manifestBlobName,
      source_manifest_sha256: config.manifestBlobHash,
      source_manifest_container_name: config.manifestContainerName,
      human_grant_id: grant.grant_id,
      human_grant_sha256: normalizeHash(grantHash),
      human_grant_consumption_blob_name: consumption.blobName,
      human_grant_consumption_sha256: consumption.hash
    },
    execution_model: sourceManifest.execution_model
  };
  const blobName = `reports/research/venue-probe/control/strategy-canary/manifests/${grant.grant_id}.json`;
  const bytes = Buffer.from(JSON.stringify(manifest, null, 2));
  try {
    await container.getBlockBlobClient(blobName).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if ([409, 412].includes(Number(error.statusCode))) throw new Error("fail closed: canary-ready transition manifest already exists");
    throw error;
  }
  return { value: manifest, blobName, hash: sha256(bytes) };
}

export function canaryChildEnvironment(env, { config, grant, grantHash, selected, consumption, executionManifest, authorization }) {
  return {
    ...env,
    EXECUTION_MODE: "strategy_canary",
    STRATEGY_CANARY_INTENT_BLOB_NAME: selected.blobName,
    STRATEGY_CANARY_INTENT_SHA256: selected.hash,
    STRATEGY_CANARY_INTENT_CONTAINER_NAME: config.intentContainerName,
    STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME: executionManifest.blobName,
    STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256: executionManifest.hash,
    STRATEGY_CANARY_MANIFEST_CONTAINER_NAME: config.storageContainer,
    STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME: authorization.blobName,
    STRATEGY_CANARY_AUTHORIZATION_SHA256: authorization.hash,
    STRATEGY_CANARY_HUMAN_GRANT_ID: grant.grant_id,
    STRATEGY_CANARY_HUMAN_GRANT_SHA256: grantHash,
    STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_BLOB_NAME: consumption.blobName,
    STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_SHA256: consumption.hash,
    STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI: grant.execution_model_blob_uri,
    STRATEGY_CANARY_EXECUTION_MODEL_SHA256: grant.execution_model_sha256
  };
}

export async function executeCanaryControllerTransaction({
  config,
  grantDocument,
  manifestDocument,
  selected,
  container,
  runId,
  invokeChild,
  childEnv = process.env,
  now = new Date()
}) {
  validateHumanGrant({
    config,
    grant: grantDocument.value,
    manifest: manifestDocument.value,
    now
  });
  if (config.dryRun) {
    return {
      status: "controller_dry_run_validated_no_mutation",
      human_grant_consumed: false,
      selected_decision_id: selected.value.decision_id,
      selected_intent_sha256: selected.hash,
      authorization_sha256: null,
      canary_invocations: 0
    };
  }
  const consumption = await consumeHumanGrant(container, {
    config,
    grant: grantDocument.value,
    grantHash: grantDocument.hash,
    selected,
    runId,
    now
  });
  const executionManifest = await createCanaryReadyManifest(container, {
    config,
    sourceManifest: manifestDocument.value,
    grant: grantDocument.value,
    grantHash: grantDocument.hash,
    consumption,
    now
  });
  const executionConfig = {
    ...config,
    executionManifestBlobName: executionManifest.blobName,
    executionManifestBlobHash: executionManifest.hash
  };
  const authorization = await createIntentBoundAuthorization(container, {
    config: executionConfig,
    grant: grantDocument.value,
    grantHash: grantDocument.hash,
    selected,
    consumption,
    now
  });
  const exitCode = await invokeChild(canaryChildEnvironment(childEnv, {
    config: executionConfig,
    grant: grantDocument.value,
    grantHash: grantDocument.hash,
    selected,
    consumption,
    executionManifest,
    authorization
  }));
  if (exitCode !== 0) throw new Error(`fail closed: exact one-shot canary exited with code ${exitCode}`);
  return {
    status: "controller_one_shot_completed",
    human_grant_consumed: true,
    selected_decision_id: selected.value.decision_id,
    selected_intent_sha256: selected.hash,
    authorization_sha256: authorization.hash,
    canary_invocations: 1
  };
}

function normalizeHash(value) {
  const normalized = clean(value).toLowerCase();
  const prefixed = normalized.startsWith("sha256:") ? normalized : `sha256:${normalized}`;
  return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : "";
}
function clean(value) { return String(value || "").trim(); }
function boolean(value) { return String(value || "").toLowerCase() === "true"; }
function number(value, fallback) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : fallback; }
function integer(value, fallback) { const parsed = Number.parseInt(value, 10); return Number.isFinite(parsed) ? parsed : fallback; }
