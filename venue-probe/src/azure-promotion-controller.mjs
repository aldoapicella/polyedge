import { DefaultAzureCredential } from "@azure/identity";
import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, open, readFile, rename } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";

const API_VERSION = "2025-07-01";
const RESOURCE_GROUP = "rg-polyedge-dev";
const APP_NAME = "polyedge-dev";
const JOB_NAME = "polyedge-hourly-quality-job";
const FREEZE_SCHEMA = "polyedge.azure_promotion_freeze.v1";
const COMPETING_WORKFLOWS = ["deploy-polyedge-active.yml", "deploy-polyedge-research-jobs.yml", "promote-conduit-backend-to-azure.yml"];
const TERMINAL_EXECUTIONS = new Set(["Succeeded", "Failed", "Canceled", "Cancelled", "Stopped", "Degraded"]);
const READINESS_TIMEOUT_MS = 300_000;
const PROOF_TIMEOUT_MS = 900_000;
const PROOF_STOP_TIMEOUT_MS = 120_000;
const ROLLBACK_TIMEOUT_MS = 600_000;
const MAX_TRANSACTION_MS = READINESS_TIMEOUT_MS * 2 + PROOF_TIMEOUT_MS + PROOF_STOP_TIMEOUT_MS + ROLLBACK_TIMEOUT_MS;
const MAX_EXECUTION_PAGES = 20;
const MAX_EXECUTIONS = 1_000;
const MAX_RETRY_AFTER_MS = 30_000;
const ARM_REQUEST_TIMEOUT_MS = 30_000;
const ARM_LRO_TIMEOUT_MS = 300_000;
const FREEZE_EVIDENCE_MAX_AGE_MS = 900_000;
const HOURLY_COMMAND = ["/bin/sh", "-lc"];
const HOURLY_ARGS = ["TARGET=$(date -u -d \"1 hour ago\" +%Y/%m/%d/%H); DAY=${TARGET%/*}; HOUR=${TARGET##*/}; OUT=\"reports/research/hourly/$DAY/$HOUR/audit.json\"; /bin/sh /app/research/run_compact_report_job.sh polyedge_hourly_quality \"$OUT\" polyedge-rs research audit --input \"azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/$HOUR/?prefetch_blobs=8\" --out \"$OUT\" --markdown \"reports/research/hourly/$DAY/$HOUR/audit.md\" --exclude-file \"data_quality/exclusion_windows.yaml\""];
const EXPECTED_HOURLY_ENVIRONMENT = {
  APP_NAME: "polyedge",
  EXECUTION_MODE: "paper",
  ALLOW_LIVE: "false",
  RUN_BOT_ON_STARTUP: "false",
  REQUIRE_API_AUTH: "true",
  AZURE_CLIENT_ID: "f76a4a3d-d287-4dfd-b348-19f39fe698a5",
  AZURE_SUBSCRIPTION_ID: "73783c0c-5a53-4f9b-b244-6f64e813814c",
  AZURE_RESOURCE_GROUP: "rg-polyedge-dev",
  AZURE_STORAGE_ACCOUNT_NAME: "stpolyedge6urdjr5nmwx7w",
  AZURE_STORAGE_CONTAINER_NAME: "bot-events",
  AZURE_STORAGE_TABLE_NAME: "BotEventIndex",
  AZURE_CHART_TABLE_NAME: "BotChartSeries",
  AZURE_MARKET_TABLE_NAME: "BotMarketCatalog",
  ENABLE_TAKER_ORDERS: "false",
  ALLOW_EMERGENCY_ACCOUNT_CANCEL: "false",
  POLYEDGE_GENERATOR_PLATFORM: "azure_container_apps_job"
};

export function imageIsImmutable(image) {
  return /^crpolyedge6urdjr5nmwx7w\.azurecr\.io\/polyedge-rust-research@sha256:[a-f0-9]{64}$/.test(image);
}

export function safeSnapshot(value) {
  if (Array.isArray(value)) return value.map(safeSnapshot);
  if (!value || typeof value !== "object") return value;
  const result = {};
  const sensitiveEnvironment = typeof value.name === "string" && /(secret|token|password|connectionstring|private.?key|api.?key)/i.test(value.name);
  for (const [key, child] of Object.entries(value)) {
    if (key === "secrets") {
      result.secrets = Array.isArray(child) ? child.map(({ name, identity, keyVaultUrl }) => ({ name, identity, keyVaultUrl })) : [];
    } else if (!(sensitiveEnvironment && key === "value") && (!/(secret|token|password|connectionstring|private.?key|api.?key)/i.test(key) || key === "secretRef")) {
      result[key] = safeSnapshot(child);
    }
  }
  return result;
}

export function surface(value) {
  return safeSnapshot(mutableSurface(value));
}

export function mutableSurface(value) {
  return {
    identity: value.identity ?? null,
    properties: {
      configuration: value.properties?.configuration ?? null,
      template: value.properties?.template ?? null
    }
  };
}

export function hash(value) {
  return createHash("sha256").update(JSON.stringify(sort(value))).digest("hex");
}

function sort(value) {
  if (Array.isArray(value)) return value.map(sort);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sort(value[key])]));
}

function fail(message) { throw new Error(`fail closed: ${message}`); }
function rolledBack(message) {
  const error = new Error(`fail closed: ${message}`);
  error.code = "POLYEDGE_ROLLED_BACK";
  return error;
}

function exactContainer(resource, name) {
  const containers = resource.properties?.template?.containers;
  if (!Array.isArray(containers)) fail("template containers are missing");
  const matches = containers.filter((container) => container.name === name);
  if (matches.length !== 1) fail(`expected exactly one ${name} container`);
  return matches[0];
}

function exactEnv(container, name) {
  const matches = (container.env ?? []).filter((entry) => entry.name === name);
  if (matches.length !== 1 || matches[0].secretRef || typeof matches[0].value !== "string") fail(`expected one plain ${name} environment value`);
  return matches[0];
}

function exactEnvValue(container, name, expected) {
  const value = exactEnv(container, name).value;
  if (value !== expected) fail(`${name} is not ${expected}`);
}

function exactIdentity(resource) {
  const identities = Object.keys(resource.identity?.userAssignedIdentities ?? {});
  if (resource.identity?.type !== "UserAssigned" || identities.length !== 1 || !identities[0].endsWith("/userAssignedIdentities/polyedge-dev-id")) fail("target does not have exactly the expected user-assigned identity");
}

function noFundedEnvironment(container) {
  if ((container.env ?? []).some((entry) => /POLYMARKET|FUNDED|PRIVATE_KEY|API_KEY|API_SECRET|API_PASSPHRASE/i.test(entry.name ?? ""))) fail("target has a funded credential environment variable");
}

export function validateTarget(app, job) {
  if (app.properties?.provisioningState !== "Succeeded" || app.properties?.configuration?.activeRevisionsMode !== "Single" || !app.properties?.latestRevisionName || app.properties.latestReadyRevisionName !== app.properties.latestRevisionName) fail("primary app is not single-revision ready");
  if (!Array.isArray(app.properties?.template?.containers) || app.properties.template.containers.length !== 2 || new Set(app.properties.template.containers.map(({ name }) => name)).size !== 2 || !app.properties.template.containers.some(({ name }) => name === "frontend")) fail("primary app containers drifted");
  exactIdentity(app);
  const bot = exactContainer(app, "bot");
  exactEnvValue(bot, "EXECUTION_MODE", "paper");
  exactEnvValue(bot, "ALLOW_LIVE", "false");
  exactEnvValue(bot, "ENABLE_TAKER_ORDERS", "false");

  const configuration = job.properties?.configuration;
  if (job.properties?.provisioningState !== "Succeeded" || configuration?.triggerType !== "Schedule" || configuration?.replicaTimeout !== 1800 || configuration?.replicaRetryLimit !== 1 || configuration?.scheduleTriggerConfig?.cronExpression !== "10 * * * *" || configuration?.scheduleTriggerConfig?.parallelism !== 1 || configuration?.scheduleTriggerConfig?.replicaCompletionCount !== 1) fail("hourly job schedule or timeout drifted");
  if (JSON.stringify((configuration.secrets ?? []).map(({ name }) => name).sort()) !== JSON.stringify(["api-bearer-token"])) fail("hourly job secrets drifted");
  exactIdentity(job);
  const research = exactContainer(job, "research-job");
  validateResearchTemplate(job);
  const generator = exactEnv(research, "POLYEDGE_GENERATOR_IMAGE");
  if (!imageIsImmutable(bot.image) || !imageIsImmutable(research.image) || !imageIsImmutable(generator.value)) fail("target image is not an approved immutable research digest");
  return { appImage: bot.image, jobImage: research.image, generatorImage: generator.value };
}

function changed(resource, containerName, envName, target) {
  const copy = structuredClone(resource);
  const container = exactContainer(copy, containerName);
  const image = typeof target === "string" ? target : target.image;
  container.image = image;
  if (envName) exactEnv(container, envName).value = typeof target === "string" ? target : target.generatorImage;
  return copy;
}

function normalizedWithoutTargets(resource, containerName, envName) {
  const copy = structuredClone(mutableSurface(resource));
  const container = exactContainer(copy, containerName);
  delete container.image;
  if (envName) delete exactEnv(container, envName).value;
  return copy;
}

export function assertOnlyAllowedDiff(before, after, kind, target) {
  const [container, envName] = kind === "app" ? ["bot", null] : ["research-job", "POLYEDGE_GENERATOR_IMAGE"];
  if (hash(normalizedWithoutTargets(before, container, envName)) !== hash(normalizedWithoutTargets(after, container, envName))) fail(`${kind} changed outside its allowed image fields`);
  const expectedImage = typeof target === "string" ? target : target.image;
  const expectedGenerator = typeof target === "string" ? target : target.generatorImage;
  const afterTarget = exactContainer(after, container);
  if (afterTarget.image !== expectedImage || (envName && exactEnv(afterTarget, envName).value !== expectedGenerator)) fail(`${kind} target fields did not equal requested image`);
}

function patchFor(resource, kind, target) {
  const [container, envName] = kind === "app" ? ["bot", null] : ["research-job", "POLYEDGE_GENERATOR_IMAGE"];
  const copy = changed(resource, container, envName, target);
  return {
    location: copy.location,
    properties: { template: copy.properties.template }
  };
}

function targetPath(config, kind) {
  const type = kind === "app" ? `containerApps/${APP_NAME}` : `jobs/${JOB_NAME}`;
  return `/subscriptions/${config.subscriptionId}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.App/${type}`;
}

function timestamp(value, label) {
  const parsed = Date.parse(value ?? "");
  if (!Number.isFinite(parsed)) fail(`${label} must be an ISO timestamp`);
  return parsed;
}

export async function ensureFreezeMarker(marker, config, nowMs = Date.now(), expectedUid = 0) {
  const stat = await lstat(marker).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink() || stat.uid !== expectedUid || (stat.mode & 0o777) !== 0o600) fail("freeze marker must be a root-owned 0600 regular file");
  let attestation;
  try { attestation = JSON.parse(await readFile(marker, "utf8")); } catch { fail("freeze marker is not valid JSON"); }
  if (attestation.schema !== FREEZE_SCHEMA || !Array.isArray(attestation.workflows) || attestation.workflows.length !== COMPETING_WORKFLOWS.length) fail("freeze marker attestation is incomplete");
  const createdAt = timestamp(attestation.created_at, "freeze marker created_at");
  const expiresAt = timestamp(attestation.expires_at, "freeze marker expires_at");
  if (createdAt > nowMs || expiresAt <= nowMs || expiresAt - createdAt > 86_400_000) fail("freeze marker is stale or has an invalid validity window");
  if (!config || attestation.candidate?.image !== config.image || attestation.candidate?.commit !== config.candidateCommit || attestation.candidate?.run_id !== config.candidateRunId) fail("freeze marker is not bound to this candidate image, commit, and run");
  const verifiedManifest = attestation.verified_manifest;
  const verifiedAt = verifiedManifest ? timestamp(verifiedManifest.verified_at, "freeze marker verified_manifest.verified_at") : null;
  const manifestEvidence = verifiedManifest?.source === "verified-manifest" && /^[a-f0-9]{64}$/i.test(verifiedManifest.sha256 ?? "") && verifiedManifest.sha256 === config.freezeManifestSha256 && verifiedAt >= createdAt && verifiedAt <= nowMs && nowMs - verifiedAt <= FREEZE_EVIDENCE_MAX_AGE_MS;
  for (const workflow of COMPETING_WORKFLOWS) {
    const matches = attestation.workflows.filter((entry) => entry.path === workflow && entry.disabled === true && entry.active_runs === 0);
    if (matches.length !== 1) fail("freeze marker does not attest all competing workflows disabled with zero active runs");
    const evidence = matches[0].evidence;
    const observedAt = evidence ? timestamp(evidence.observed_at, "freeze marker workflow evidence observed_at") : null;
    const liveEvidence = evidence?.source === "github-api" && observedAt >= createdAt && observedAt <= nowMs && nowMs - observedAt <= FREEZE_EVIDENCE_MAX_AGE_MS;
    if (!liveEvidence && !manifestEvidence) fail("freeze marker lacks live disabled/active-zero evidence or a verified manifest");
  }
}

export async function secureStateDirectory(directory, expectedUid = 0) {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
  const stat = await lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== expectedUid || (stat.mode & 0o777) !== 0o700) fail("state directory must be a root-owned 0700 directory");
}

async function writeJournal(filename, journal) {
  const temporary = `${filename}.${process.pid}.${randomUUID()}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(journal)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, filename);
  await chmod(filename, 0o600);
  const stat = await lstat(filename);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.uid !== 0 || (stat.mode & 0o777) !== 0o600) fail("journal is not a root-owned 0600 regular file");
  const directory = await open(path.dirname(filename), "r");
  try { await directory.sync(); } finally { await directory.close(); }
}

async function readJournal(filename) {
  try {
    const stat = await lstat(filename);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.uid !== 0 || (stat.mode & 0o777) !== 0o600) fail("journal is not a root-owned 0600 regular file");
    const value = JSON.parse(await readFile(filename, "utf8"));
    if (value.schema !== "polyedge.azure_promotion_controller.v1") fail("journal schema is invalid");
    return value;
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

function journalFrom(config, app, job) {
  const targets = validateTarget(app, job);
  return {
    schema: "polyedge.azure_promotion_controller.v1",
    runId: randomUUID(),
    phase: "prepared",
    requestedImage: config.image,
    scheduleDeadlineMs: scheduleWindow(config).deadlineMs,
    before: {
      app: { snapshot: safeSnapshot(app), surfaceHash: hash(mutableSurface(app)), image: targets.appImage },
      job: { snapshot: safeSnapshot(job), surfaceHash: hash(mutableSurface(job)), image: targets.jobImage, generatorImage: targets.generatorImage }
    },
    expected: {
      appSurfaceHash: hash(mutableSurface(changed(app, "bot", null, config.image))),
      jobSurfaceHash: hash(mutableSurface(changed(job, "research-job", "POLYEDGE_GENERATOR_IMAGE", { image: config.image, generatorImage: config.image })))
    }
  };
}

function currentHash(value) { return hash(mutableSurface(value)); }

function clockMs(config) { return config.clock ? config.clock() : Date.now(); }
function monotonicMs(config) { return config.monotonicNow ? config.monotonicNow() : performance.now(); }
function timingFrom(nowMs) {
  const transactionDeadlineMonotonicMs = nowMs + MAX_TRANSACTION_MS;
  return Object.freeze({
    transactionDeadlineMonotonicMs,
    forwardDeadlineMonotonicMs: transactionDeadlineMonotonicMs - ROLLBACK_TIMEOUT_MS
  });
}
function validTiming(timing) {
  return timing && Number.isFinite(timing.transactionDeadlineMonotonicMs) && Number.isFinite(timing.forwardDeadlineMonotonicMs)
    && timing.forwardDeadlineMonotonicMs <= timing.transactionDeadlineMonotonicMs - ROLLBACK_TIMEOUT_MS;
}
function beginTransaction(config) {
  if (!config.promotionTiming) {
    Object.defineProperty(config, "promotionTiming", {
      value: timingFrom(monotonicMs(config)),
      enumerable: false,
      configurable: false,
      writable: false
    });
  }
  if (!validTiming(config.promotionTiming)) fail("promotion timing is invalid");
  return config.promotionTiming;
}
function phaseDeadline(config, phase) {
  const timing = beginTransaction(config);
  return phase === "rollback" ? timing.transactionDeadlineMonotonicMs : timing.forwardDeadlineMonotonicMs;
}
function phaseRemaining(config, phase, label) {
  const remaining = phaseDeadline(config, phase) - monotonicMs(config);
  if (remaining <= 0) fail(`${label} exceeded the monotonic ${phase} deadline`);
  return remaining;
}
function setArmPhaseDeadline(arm, config, phase) {
  const timing = beginTransaction(config);
  if (typeof arm.setPromotionTiming === "function") arm.setPromotionTiming(timing);
  if (typeof arm.setDeadlineMonotonicMs === "function") arm.setDeadlineMonotonicMs(phaseDeadline(config, phase), phase);
}
async function boundedSleep(config, phase, sleep, requestedMs, label) {
  if (requestedMs > phaseRemaining(config, phase, label)) fail(`${label} would exceed the monotonic ${phase} deadline`);
  await sleep(requestedMs);
}

function scheduleWindow(config, atMs = clockMs(config)) {
  const now = new Date(atMs);
  const next = new Date(now);
  next.setUTCMinutes(10, 0, 0);
  if (next <= now) next.setUTCHours(next.getUTCHours() + 1);
  if (next.getTime() - atMs <= MAX_TRANSACTION_MS) fail("promotion refuses a start window that cannot finish before the next scheduled hourly execution");
  return { deadlineMs: next.getTime() };
}

async function waitFor(arm, config, resourcePath, predicate, label, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))) {
  for (let attempt = 0; attempt < READINESS_TIMEOUT_MS / 10_000; attempt += 1) {
    phaseRemaining(config, "forward", label);
    const value = await arm.get(resourcePath);
    if (predicate(value)) return value;
    await boundedSleep(config, "forward", sleep, 10_000, label);
  }
  fail(`${label} did not become ready before its bound`);
}

function appReady(app) {
  return app.properties?.provisioningState === "Succeeded" && app.properties?.configuration?.activeRevisionsMode === "Single" && app.properties?.latestRevisionName && app.properties.latestReadyRevisionName === app.properties.latestRevisionName;
}

function jobReady(job) { return job.properties?.provisioningState === "Succeeded"; }

function validateResearchTemplate(resource, execution = false) {
  const containers = resource.properties?.template?.containers;
  if (!Array.isArray(containers) || containers.length !== 1) fail("hourly proof template container count drifted");
  const container = exactContainer(resource, "research-job");
  if (JSON.stringify(container.command) !== JSON.stringify(HOURLY_COMMAND) || JSON.stringify(container.args) !== JSON.stringify(HOURLY_ARGS)) fail("hourly proof command or argv drifted");
  const bearer = (container.env ?? []).filter((entry) => entry.name === "API_BEARER_TOKEN");
  const expectedBearerRef = execution ? `cappjob-${JOB_NAME}` : "api-bearer-token";
  if (bearer.length !== 1 || bearer[0].secretRef !== expectedBearerRef || bearer[0].value || (container.env ?? []).filter((entry) => entry.secretRef).length !== 1) fail("hourly proof bearer reference drifted");
  for (const [name, expected] of Object.entries(EXPECTED_HOURLY_ENVIRONMENT)) exactEnvValue(container, name, expected);
  if (!imageIsImmutable(exactEnv(container, "POLYEDGE_GENERATOR_IMAGE").value)) fail("hourly proof generator image is not immutable");
  const expectedEnv = ["API_BEARER_TOKEN", "POLYEDGE_GENERATOR_IMAGE", ...Object.keys(EXPECTED_HOURLY_ENVIRONMENT)];
  if ((container.env ?? []).length !== expectedEnv.length || JSON.stringify((container.env ?? []).map(({ name }) => name).sort()) !== JSON.stringify(expectedEnv.slice().sort())) fail("hourly proof template environment surface drifted");
  noFundedEnvironment(container);
  return container;
}

function validateProofExecution(execution, image) {
  const container = validateResearchTemplate(execution, true);
  if (container.image !== image || exactEnv(container, "POLYEDGE_GENERATOR_IMAGE").value !== image) fail("hourly proof used a different image");
}

async function findProofExecution(arm, jobPath, proof) {
  if (proof.executionName) return proof.executionName;
  const executions = await arm.listExecutions(jobPath);
  const names = executions.filter((execution) => !proof.beforeExecutionNames.includes(execution.name)).map((execution) => execution.name);
  if (names.length > 1) fail("hourly proof found more than one new execution");
  return names[0] ?? null;
}

async function proveHourlyExecution({ arm, config, journal, save }) {
  const jobPath = targetPath(config, "job");
  let proof = journal.proof;
  if (!proof) {
    phaseRemaining(config, "forward", "hourly proof start");
    const now = clockMs(config);
    const proofDeadline = Math.min(now + config.proofTimeoutMs, journal.scheduleDeadlineMs - PROOF_STOP_TIMEOUT_MS);
    if (proofDeadline <= now) fail("hourly proof has no safe deadline before the scheduled execution");
    const before = await arm.listExecutions(jobPath);
    proof = { beforeExecutionNames: before.map((execution) => execution.name), deadlineMs: proofDeadline, executionName: null };
    journal.phase = "proof_start_intent";
    journal.proof = proof;
    await save(journal);
    phaseRemaining(config, "forward", "hourly proof start");
    const started = await arm.startJob(jobPath);
    if (started?.name) proof.executionName = started.name;
    await save(journal);
  }
  while (clockMs(config) < proof.deadlineMs) {
    phaseRemaining(config, "forward", "hourly proof");
    const executionName = await findProofExecution(arm, jobPath, proof);
    if (executionName) {
      proof.executionName = executionName;
      await save(journal);
      const execution = await arm.getExecution(jobPath, executionName);
      const status = execution.properties?.status;
      if (status === "Succeeded") {
        validateProofExecution(execution, journal.requestedImage);
        journal.phase = "proof_succeeded";
        await save(journal);
        return;
      }
      if (TERMINAL_EXECUTIONS.has(status)) fail(`hourly proof ended ${status}`);
    }
    await boundedSleep(config, "forward", config.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))), 10_000, "hourly proof");
  }
  const executionName = await findProofExecution(arm, jobPath, proof);
  if (!executionName) fail("hourly proof did not create an execution before its bound");
  journal.phase = "proof_stop_intent";
  proof.executionName = executionName;
  await save(journal);
  phaseRemaining(config, "forward", "hourly proof stop");
  await arm.stopExecution(jobPath, executionName);
  for (let attempt = 0; attempt < PROOF_STOP_TIMEOUT_MS / 10_000; attempt += 1) {
    phaseRemaining(config, "forward", "hourly proof stop");
    const execution = await arm.getExecution(jobPath, executionName);
    if (TERMINAL_EXECUTIONS.has(execution.properties?.status)) fail("hourly proof exceeded its bound and was stopped");
    await boundedSleep(config, "forward", config.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))), 10_000, "hourly proof stop");
  }
  fail("hourly proof stop did not reach a terminal state");
}

async function assertIdle(arm, config, phase = "forward") {
  phaseRemaining(config, phase, "hourly idle check");
  const executions = await arm.listExecutions(targetPath(config, "job"));
  if (!Array.isArray(executions) || executions.some((execution) => !TERMINAL_EXECUTIONS.has(execution.properties?.status))) fail("hourly job has a nonterminal execution");
}

async function stopRecordedProof(arm, config, journal) {
  if (!journal.proof) return;
  phaseRemaining(config, "rollback", "recorded proof stop");
  const jobPath = targetPath(config, "job");
  const executionName = await findProofExecution(arm, jobPath, journal.proof);
  if (!executionName) return;
  journal.proof.executionName = executionName;
  const execution = await arm.getExecution(jobPath, executionName);
  if (!TERMINAL_EXECUTIONS.has(execution.properties?.status)) await arm.stopExecution(jobPath, executionName);
}

async function reconcileOrWrite({ arm, config, journal, kind, write }) {
  const pathName = targetPath(config, kind);
  const before = journal.before[kind];
  const expected = journal.expected[`${kind}SurfaceHash`];
  const current = await arm.get(pathName);
  const actual = currentHash(current);
  if (actual === expected) return { written: false, resource: current };
  if (actual !== before.surfaceHash) fail(`${kind} no longer matches the recorded pre- or post-write state`);
  if (!write) return { written: false, resource: current };
  const response = await arm.patch(pathName, patchFor(current, kind, journal.requestedImage));
  assertOnlyAllowedDiff(current, response, kind, journal.requestedImage);
  if (currentHash(response) !== expected) fail(`${kind} write did not produce the recorded post-write state`);
  return { written: true, resource: response };
}

export async function rollbackPromotion({ arm, config, journal, save }) {
  beginTransaction(config);
  setArmPhaseDeadline(arm, config, "rollback");
  journal.phase = "rollback_intent";
  await save(journal);
  await stopRecordedProof(arm, config, journal);
  await assertIdle(arm, config, "rollback");
  for (const kind of ["job", "app"]) {
    phaseRemaining(config, "rollback", `${kind} rollback`);
    const current = await arm.get(targetPath(config, kind));
    const actual = currentHash(current);
    const before = journal.before[kind];
    const expected = journal.expected[`${kind}SurfaceHash`];
    if (actual === before.surfaceHash) continue;
    if (actual !== expected) fail(`${kind} rollback refused because the current state is not controller-owned post-write state`);
    const target = kind === "app" ? before.image : { image: before.image, generatorImage: before.generatorImage };
    journal.phase = `rollback_${kind}_intent`;
    await save(journal);
    const restored = await arm.patch(targetPath(config, kind), patchFor(current, kind, target));
    assertOnlyAllowedDiff(current, restored, kind, target);
    if (currentHash(restored) !== before.surfaceHash) fail(`${kind} rollback did not restore the recorded prior state`);
    journal.phase = `rollback_${kind}_restored`;
    await save(journal);
  }
  journal.phase = "rolled_back";
  await save(journal);
  return { status: "rolled_back", phase: journal.phase, runId: journal.runId };
}

export async function runPromotion({ arm, config, save = async () => {}, load = async () => null }) {
  beginTransaction(config);
  setArmPhaseDeadline(arm, config, "forward");
  let journal = await load();
  // `prepared` is written before the first ARM request. Every other nonterminal
  // forward phase follows an intent record whose PATCH/start request may have
  // reached ARM even if this process was SIGKILLed before receiving a response.
  // Such a request is intentionally ambiguous and is never resumed forward.
  if (journal && !["prepared", "promoted", "rolled_back"].includes(journal.phase)) return rollbackPromotion({ arm, config, journal, save });
  await assertIdle(arm, config, "forward");
  if (!journal) {
    phaseRemaining(config, "forward", "initial target read");
    const [app, job] = await Promise.all([arm.get(targetPath(config, "app")), arm.get(targetPath(config, "job"))]);
    validateTarget(app, job);
    journal = journalFrom(config, app, job);
    await save(journal);
  }
  if (journal.requestedImage !== config.image) fail("existing journal is bound to a different requested image");
  if (journal.phase === "rolled_back") return { status: "already_rolled_back", phase: journal.phase, runId: journal.runId };
  if (journal.phase === "promoted") return { status: "already_promoted", phase: journal.phase, runId: journal.runId };
  try {
    journal.phase = "app_write_intent";
    await save(journal);
    phaseRemaining(config, "forward", "primary app write");
    await reconcileOrWrite({ arm, config, journal, kind: "app", write: true });
    await waitFor(arm, config, targetPath(config, "app"), appReady, "primary app", config.sleep);
    journal.phase = "app_written";
    await save(journal);

    journal.phase = "job_write_intent";
    await save(journal);
    phaseRemaining(config, "forward", "hourly job write");
    await reconcileOrWrite({ arm, config, journal, kind: "job", write: true });
    await waitFor(arm, config, targetPath(config, "job"), jobReady, "hourly job", config.sleep);
    phaseRemaining(config, "forward", "pre-proof target read");
    const [appBeforeProof, jobBeforeProof] = await Promise.all([arm.get(targetPath(config, "app")), arm.get(targetPath(config, "job"))]);
    validateTarget(appBeforeProof, jobBeforeProof);
    if (currentHash(appBeforeProof) !== journal.expected.appSurfaceHash || currentHash(jobBeforeProof) !== journal.expected.jobSurfaceHash) fail("target drifted before hourly proof");
    if (config.proveExecution) await proveHourlyExecution({ arm, config, journal, save });
    journal.phase = "promoted";
    await save(journal);
    return { status: "promoted", phase: journal.phase, runId: journal.runId };
  } catch (error) {
    try {
      await rollbackPromotion({ arm, config, journal, save });
    } catch (rollbackError) {
      throw new Error(`${error.message}; rollback failed: ${rollbackError.message}`);
    }
    throw rolledBack(error.message);
  }
}

export function loadConfig(environment = process.env) {
  const mode = environment.POLYEDGE_PROMOTION_MODE ?? "promote";
  if (mode !== "promote" && mode !== "rollback") fail("promotion mode must be promote or rollback");
  const subscriptionId = environment.AZURE_SUBSCRIPTION_ID;
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(subscriptionId ?? "")) fail("AZURE_SUBSCRIPTION_ID is invalid");
  for (const name of ["AZURE_TENANT_ID", "AZURE_CLIENT_ID"]) {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(environment[name] ?? "")) fail(`${name} is invalid`);
  }
  if (environment.AZURE_FEDERATED_TOKEN_FILE !== "/run/credentials/polyedge-azure-promotion.service/azure-federated-token") fail("AZURE_FEDERATED_TOKEN_FILE must use the dedicated systemd promotion credential");
  if (environment.AZURE_TOKEN_CREDENTIALS !== "WorkloadIdentityCredential") fail("AZURE_TOKEN_CREDENTIALS must pin WorkloadIdentityCredential");
  if (environment.AZURE_RESOURCE_GROUP !== RESOURCE_GROUP) fail("AZURE_RESOURCE_GROUP must be rg-polyedge-dev");
  if (environment.POLYEDGE_PROMOTION_STATE_DIR !== "/var/lib/polyedge/azure-promotion" || !environment.POLYEDGE_PROMOTION_FREEZE_MARKER) fail("state directory must be the systemd StateDirectory path and freeze marker is required");
  if (mode === "promote" && !imageIsImmutable(environment.POLYEDGE_PROMOTION_IMAGE ?? "")) fail("POLYEDGE_PROMOTION_IMAGE is invalid");
  if (!/^[a-f0-9]{40}$/i.test(environment.POLYEDGE_PROMOTION_CANDIDATE_COMMIT ?? "")) fail("POLYEDGE_PROMOTION_CANDIDATE_COMMIT must be a full Git commit");
  if (!/^[1-9][0-9]*$/.test(environment.POLYEDGE_PROMOTION_CANDIDATE_RUN_ID ?? "")) fail("POLYEDGE_PROMOTION_CANDIDATE_RUN_ID is invalid");
  if (environment.POLYEDGE_PROMOTION_FREEZE_MANIFEST_SHA256 && !/^[a-f0-9]{64}$/i.test(environment.POLYEDGE_PROMOTION_FREEZE_MANIFEST_SHA256)) fail("POLYEDGE_PROMOTION_FREEZE_MANIFEST_SHA256 is invalid");
  if (environment.POLYEDGE_PROMOTION_PROVE_HOURLY !== "true") fail("POLYEDGE_PROMOTION_PROVE_HOURLY must equal true");
  return { mode, subscriptionId, image: environment.POLYEDGE_PROMOTION_IMAGE, candidateCommit: environment.POLYEDGE_PROMOTION_CANDIDATE_COMMIT, candidateRunId: environment.POLYEDGE_PROMOTION_CANDIDATE_RUN_ID, freezeManifestSha256: environment.POLYEDGE_PROMOTION_FREEZE_MANIFEST_SHA256, stateDirectory: environment.POLYEDGE_PROMOTION_STATE_DIR, freezeMarker: environment.POLYEDGE_PROMOTION_FREEZE_MARKER, proveExecution: true, proofTimeoutMs: PROOF_TIMEOUT_MS };
}

export class ArmClient {
  constructor(credential = new DefaultAzureCredential({
    excludeManagedIdentityCredential: true,
    excludeVisualStudioCodeCredential: true,
    excludeAzureCliCredential: true,
    excludeAzurePowerShellCredential: true,
    excludeAzureDeveloperCliCredential: true,
    excludeInteractiveBrowserCredential: true,
    excludeBrokerCredential: true
  }), sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), monotonicNow = () => performance.now()) {
    this.credential = credential;
    this.sleep = sleep;
    this.monotonicNow = monotonicNow;
    this.promotionTiming = null;
    this.deadlineMonotonicMs = null;
  }
  timing() {
    if (!this.promotionTiming) this.setPromotionTiming(timingFrom(this.monotonicNow()));
    return this.promotionTiming;
  }
  setPromotionTiming(timing) {
    if (!validTiming(timing)) throw new Error("Azure ARM promotion timing is invalid");
    if (this.promotionTiming && this.promotionTiming !== timing) throw new Error("Azure ARM promotion timing cannot change during a transaction");
    this.promotionTiming = timing;
    this.transactionDeadlineMonotonicMs = timing.transactionDeadlineMonotonicMs;
    this.forwardDeadlineMonotonicMs = timing.forwardDeadlineMonotonicMs;
    if (!Number.isFinite(this.deadlineMonotonicMs)) this.deadlineMonotonicMs = timing.forwardDeadlineMonotonicMs;
  }
  setDeadlineMonotonicMs(deadlineMonotonicMs, phase) {
    const timing = this.timing();
    if (!Number.isFinite(deadlineMonotonicMs)) throw new Error("Azure ARM phase deadline is invalid");
    if (phase === "forward" && deadlineMonotonicMs > timing.forwardDeadlineMonotonicMs) throw new Error("Azure ARM forward deadline consumes the rollback reserve");
    if (phase === "rollback" && deadlineMonotonicMs > timing.transactionDeadlineMonotonicMs) throw new Error("Azure ARM rollback deadline exceeds the transaction deadline");
    this.deadlineMonotonicMs = deadlineMonotonicMs;
  }
  remainingMs(label) {
    this.timing();
    const remaining = this.deadlineMonotonicMs - this.monotonicNow();
    if (remaining <= 0) throw new Error(`Azure ARM ${label} exceeded the monotonic transaction deadline`);
    return remaining;
  }
  async token() {
    const timeoutMs = Math.ceil(Math.min(this.remainingMs("token acquisition"), ARM_REQUEST_TIMEOUT_MS));
    let timeout;
    try {
      const value = await Promise.race([
        this.credential.getToken("https://management.azure.com/.default"),
        new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error("Azure ARM token acquisition exceeded its phase deadline")), timeoutMs); })
      ]);
      if (!value || typeof value.token !== "string" || !value.token) throw new Error("Azure ARM token acquisition returned an invalid token");
      return value;
    } finally {
      clearTimeout(timeout);
    }
  }
  async authorizedFetch(url, options = {}) {
    const token = await this.token();
    return fetch(url, { ...options, signal: options.signal ?? AbortSignal.timeout(Math.ceil(Math.min(this.remainingMs("request"), ARM_REQUEST_TIMEOUT_MS))), headers: { authorization: `Bearer ${token.token}`, "content-type": "application/json", ...options.headers } });
  }
  trustedOperationUrl(location) {
    const url = new URL(location);
    if (url.protocol !== "https:" || url.hostname !== "management.azure.com") throw new Error("Azure ARM returned an untrusted operation polling URL");
    return url.toString();
  }
  retryAfter(response, fallbackMs = 2000) {
    const raw = response.headers.get("retry-after");
    if (raw === null) return fallbackMs;
    const seconds = Number(raw);
    return Number.isFinite(seconds) && seconds >= 0 ? Math.min(seconds * 1000, MAX_RETRY_AFTER_MS) : fallbackMs;
  }
  async request(method, resourcePath, body) {
    this.remainingMs(`${method} request`);
    const response = await this.authorizedFetch(`https://management.azure.com${resourcePath}?api-version=${API_VERSION}`, {
      method,
      body: body ? JSON.stringify(body) : undefined
    });
    if (!response.ok) throw new Error(`Azure ARM ${method} failed with HTTP ${response.status}`);
    if (response.status === 202) {
      await this.waitForOperation(response.headers.get("azure-asyncoperation") ?? response.headers.get("location"), this.retryAfter(response));
      return null;
    }
    return response.status === 204 ? null : response.json();
  }
  async waitForOperation(location, initialDelayMs) {
    if (!location) throw new Error("Azure ARM accepted an operation without a poll location");
    this.timing();
    const trusted = this.trustedOperationUrl(location);
    let delayMs = initialDelayMs;
    const operationDeadlineMonotonicMs = Math.min(this.deadlineMonotonicMs, this.monotonicNow() + ARM_LRO_TIMEOUT_MS);
    for (let attempt = 0; attempt < 150; attempt += 1) {
      const operationRemainingMs = operationDeadlineMonotonicMs - this.monotonicNow();
      if (operationRemainingMs <= 0 || delayMs >= operationRemainingMs || delayMs >= this.remainingMs("operation poll")) throw new Error("Azure ARM operation polling would exceed its forward or transaction deadline");
      await this.sleep(delayMs);
      const response = await this.authorizedFetch(trusted, { signal: AbortSignal.timeout(Math.ceil(Math.min(ARM_REQUEST_TIMEOUT_MS, operationDeadlineMonotonicMs - this.monotonicNow(), this.remainingMs("operation poll")))) });
      if (!response.ok) throw new Error(`Azure ARM operation poll failed with HTTP ${response.status}`);
      if (response.status === 202) { delayMs = this.retryAfter(response); continue; }
      const value = response.status === 204 ? {} : await response.json();
      const status = value.status ?? value.properties?.provisioningState;
      if (!status || status === "Succeeded") return;
      if (["Failed", "Canceled", "Cancelled"].includes(status)) throw new Error(`Azure ARM operation ended ${status}`);
      delayMs = this.retryAfter(response);
    }
    throw new Error("Azure ARM operation polling timed out");
  }
  get(resourcePath) { return this.request("GET", resourcePath); }
  async patch(resourcePath, body) {
    await this.request("PATCH", resourcePath, body);
    return this.get(resourcePath);
  }
  startJob(jobPath) { return this.request("POST", `${jobPath}/start`); }
  getExecution(jobPath, name) { return this.get(`${jobPath}/executions/${encodeURIComponent(name)}`); }
  stopExecution(jobPath, name) { return this.request("POST", `${jobPath}/executions/${encodeURIComponent(name)}/stop`); }
  async listExecutions(jobPath) {
    const executions = [];
    let url = `https://management.azure.com${jobPath}/executions?api-version=${API_VERSION}`;
    const seen = new Set();
    for (let pageCount = 0; url; pageCount += 1) {
      this.remainingMs("executions read");
      if (pageCount >= MAX_EXECUTION_PAGES) throw new Error("Azure ARM executions pagination exceeded its page bound");
      const trusted = this.trustedOperationUrl(url);
      if (seen.has(trusted)) throw new Error("Azure ARM executions pagination repeated a nextLink");
      seen.add(trusted);
      const response = await this.authorizedFetch(trusted);
      if (!response.ok) throw new Error(`Azure ARM executions read failed with HTTP ${response.status}`);
      const page = await response.json();
      executions.push(...(page.value ?? []));
      if (executions.length > MAX_EXECUTIONS) throw new Error("Azure ARM executions pagination exceeded its execution bound");
      url = page.nextLink ? this.trustedOperationUrl(page.nextLink) : null;
    }
    return executions;
  }
}

function runUnderFlock() {
  if (process.env.POLYEDGE_PROMOTION_FLOCKED === "1") return false;
  const directory = process.env.POLYEDGE_PROMOTION_STATE_DIR;
  if (!directory) return false;
  const result = spawnSync("/usr/bin/flock", ["-n", path.join(directory, "controller.lock"), process.execPath, process.argv[1], ...process.argv.slice(2)], {
    env: { ...process.env, POLYEDGE_PROMOTION_FLOCKED: "1" }, stdio: "inherit"
  });
  process.exitCode = result.status ?? 1;
  return true;
}

async function main() {
  const config = loadConfig();
  await secureStateDirectory(config.stateDirectory);
  if (runUnderFlock()) return;
  await ensureFreezeMarker(config.freezeMarker, config);
  const filename = path.join(config.stateDirectory, "promotion.json");
  const load = () => readJournal(filename);
  const save = (journal) => writeJournal(filename, journal);
  const journal = await load();
  const arm = new ArmClient();
  const result = config.mode === "rollback"
    ? await rollbackPromotion({ arm, config, journal: journal ?? fail("rollback requires an existing journal"), save })
    : await runPromotion({ arm, config, load, save });
  console.log(JSON.stringify({ schema: "polyedge.azure_promotion_controller_run.v1", status: result.status, phase: result.phase, run_id: result.runId }));
  if (config.mode === "promote" && ["rolled_back", "already_rolled_back"].includes(result.status)) process.exitCode = 78;
}

if (process.argv[1] && path.resolve(process.argv[1]) === new URL(import.meta.url).pathname) {
  main().catch((error) => {
    process.exitCode = error.code === "POLYEDGE_ROLLED_BACK" ? 78 : 1;
    console.error(JSON.stringify({ schema: "polyedge.azure_promotion_controller_run.v1", status: error.code === "POLYEDGE_ROLLED_BACK" ? "rolled_back" : "failed_closed", error: error.message }));
  });
}
