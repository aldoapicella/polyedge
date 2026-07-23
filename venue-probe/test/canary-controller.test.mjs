import test from "node:test";
import assert from "node:assert/strict";
import {
  canaryChildEnvironment,
  consumeHumanGrant,
  createCanaryReadyManifest,
  createIntentBoundAuthorization,
  executeCanaryControllerTransaction,
  selectFirstQualifiedIntent,
  validateHumanGrant
} from "../src/canary-controller-lib.mjs";

const now = new Date("2026-07-12T12:00:30Z");
const hash = (character) => `sha256:${character.repeat(64)}`;

function fixture() {
  const config = {
    intentPrefix: "reports/intents", candidateName: "dynamic_quote_style",
    candidateVersion: "dynamic_quote_style@2026-06-14", candidateConfigHash: "sha256:config",
    requiredFillModelVersion: "conservative-execution-prior-v1", requiredResolutionSource: "chainlink_reference",
    manifestBlobName: "promotion/manifest.json", manifestBlobHash: hash("1"), maxOrderNotional: 1,
    storageAccount: "storage", storageContainer: "bot-events", intentContainerName: "polyedge-shadow-events",
    manifestContainerName: "polyedge-research"
  };
  const executionModel = {
    blob_uri: "azure://storage/bot-events/reports/research/venue-probe/conservative_execution_prior_v1.json",
    sha256: hash("7"),
    model_version: config.requiredFillModelVersion
  };
  const manifest = {
    schema_version: "promotion_manifest_v1", phase: "shadow_passed",
    gate_metrics: { phase: "shadow_passed", promotion_allowed: true },
    human_authorization_required: true, promotion_allowed: false,
    candidate: { name: config.candidateName, candidate_version: config.candidateVersion, config_hash: config.candidateConfigHash },
    created_at: "2026-07-12T11:00:00Z", expires_at: "2026-07-12T13:00:00Z",
    execution_model: executionModel
  };
  const grant = {
    schema: "polyedge.strategy_canary_human_grant.v1", grant_id: "grant-1",
    human_authorization_reference: "review-ticket-1", authorized_at: "2026-07-12T12:00:00Z",
    expires_at: "2026-07-12T12:05:00Z", single_use: true,
    selection_policy: "first_fresh_after_authorized_at", intent_prefix: config.intentPrefix,
    promotion_manifest_blob_name: config.manifestBlobName, promotion_manifest_sha256: config.manifestBlobHash,
    candidate_name: config.candidateName, candidate_version: config.candidateVersion,
    candidate_config_hash: config.candidateConfigHash, required_fill_model_version: config.requiredFillModelVersion,
    execution_model_blob_uri: executionModel.blob_uri, execution_model_sha256: executionModel.sha256,
    max_order_notional: 1
  };
  const intent = (decisionId, decisionTs) => {
    const validUntil = new Date(Date.parse(decisionTs) + 30_000).toISOString();
    const venueExpiry = new Date(Date.parse(validUntil) + 60_000).toISOString();
    return ({
    schema: "polyedge.execution_intent.v1", decision_id: decisionId,
    decision_ts: decisionTs, valid_until: validUntil, gtd_expiry_ts: venueExpiry, ttl_ms: 30_000,
    price: "0.20", shares: "5", minimum_order_size: "5", notional: "1",
    candidate_name: config.candidateName, candidate_version: config.candidateVersion,
    candidate_config_hash: config.candidateConfigHash, required_fill_model_version: config.requiredFillModelVersion,
    execution_model_blob_uri: executionModel.blob_uri, execution_model_sha256: executionModel.sha256,
    resolution_source: config.requiredResolutionSource, exact_resolution_source: true,
    side: "BUY", post_only: true, order_kind: "post_only_gtd", net_edge_lower_bound: "0.01"
  }); };
  return { config, manifest, grant, intent };
}

function immutableContainer() {
  const names = new Set();
  return {
    names,
    getBlockBlobClient: (name) => ({
      uploadData: async (_bytes, options) => {
        assert.equal(options.conditions.ifNoneMatch, "*");
        if (names.has(name)) throw Object.assign(new Error("exists"), { statusCode: 412 });
        names.add(name);
      }
    })
  };
}

test("human grant is short-lived and exactly manifest/candidate/model bound", () => {
  const value = fixture();
  assert.equal(validateHumanGrant({ ...value, now }).maxNotional, 1);
  value.grant.candidate_config_hash = "sha256:other";
  assert.throws(() => validateHumanGrant({ ...value, now }), /candidate or fill-model binding/);
  const modelValue = fixture();
  modelValue.grant.execution_model_sha256 = hash("8");
  assert.throws(() => validateHumanGrant({ ...modelValue, now }), /exact execution model binding/);
});

test("controller selects the earliest future qualified immutable intent", () => {
  const { config, grant, intent } = fixture();
  const firstId = "a".repeat(64);
  const secondId = "b".repeat(64);
  const candidates = [
    { value: intent(secondId, "2026-07-12T12:00:20Z"), blobName: `${config.intentPrefix}/${secondId}.json`, hash: hash("2") },
    { value: intent(firstId, "2026-07-12T12:00:10Z"), blobName: `${config.intentPrefix}/${firstId}.json`, hash: hash("3") },
    { value: intent("c".repeat(64), "2026-07-12T11:59:59Z"), blobName: `${config.intentPrefix}/${"c".repeat(64)}.json`, hash: hash("4") }
  ];
  assert.equal(selectFirstQualifiedIntent({ config, grant, candidates, now }).value.decision_id, firstId);
});

test("human grant burns once before exact internal authorization is created", async () => {
  const { config, grant, intent } = fixture();
  const id = "a".repeat(64);
  const selected = { value: intent(id, "2026-07-12T12:00:10Z"), blobName: `${config.intentPrefix}/${id}.json`, hash: hash("2") };
  const container = immutableContainer();
  const consumption = await consumeHumanGrant(container, { config, grant, grantHash: hash("3"), selected, runId: "run-1", now });
  const executionManifest = await createCanaryReadyManifest(container, { config, sourceManifest: fixture().manifest, grant, grantHash: hash("3"), consumption, now });
  const executionConfig = { ...config, executionManifestBlobName: executionManifest.blobName, executionManifestBlobHash: executionManifest.hash };
  const authorization = await createIntentBoundAuthorization(container, { config: executionConfig, grant, grantHash: hash("3"), selected, consumption, now });
  assert.equal(authorization.value.decision_id, id);
  assert.equal(authorization.value.human_grant_consumption_sha256, consumption.hash);
  const env = canaryChildEnvironment({}, { config: executionConfig, grant, grantHash: hash("3"), selected, consumption, executionManifest, authorization });
  assert.equal(env.STRATEGY_CANARY_INTENT_SHA256, selected.hash);
  assert.equal(env.STRATEGY_CANARY_AUTHORIZATION_SHA256, authorization.hash);
  assert.equal(env.STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256, executionManifest.hash);
  assert.equal(env.STRATEGY_CANARY_INTENT_CONTAINER_NAME, "polyedge-shadow-events");
  assert.equal(env.STRATEGY_CANARY_MANIFEST_CONTAINER_NAME, "bot-events");
  await assert.rejects(consumeHumanGrant(container, { config, grant, grantHash: hash("3"), selected, runId: "run-2", now }), /already consumed/);
});

test("controller dry-run validates exact inputs without writes, grant burn, authorization, or child", async () => {
  const { config, grant, manifest, intent } = fixture();
  config.dryRun = true;
  const id = "d".repeat(64);
  const selected = {
    value: intent(id, "2026-07-12T12:00:10Z"),
    blobName: `${config.intentPrefix}/${id}.json`,
    hash: hash("d")
  };
  const container = immutableContainer();
  let childInvocations = 0;
  const result = await executeCanaryControllerTransaction({
    config,
    grantDocument: { value: grant, hash: hash("3") },
    manifestDocument: { value: manifest, hash: config.manifestBlobHash },
    selected,
    container,
    runId: "dry-run",
    invokeChild: async () => { childInvocations += 1; return 0; },
    now
  });
  assert.equal(result.status, "controller_dry_run_validated_no_mutation");
  assert.equal(result.human_grant_consumed, false);
  assert.equal(result.authorization_sha256, null);
  assert.equal(result.canary_invocations, 0);
  assert.equal(container.names.size, 0);
  assert.equal(childInvocations, 0);
});
