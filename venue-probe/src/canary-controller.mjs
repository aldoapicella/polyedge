import { spawn } from "node:child_process";
import { storageContainer, sanitize } from "./lib.mjs";
import { loadHashedJson, sha256 } from "./canary-lib.mjs";
import {
  executeCanaryControllerTransaction,
  loadControllerConfig,
  selectFirstQualifiedIntent,
  validateHumanGrant
} from "./canary-controller-lib.mjs";

const config = loadControllerConfig();
const runId = `strategy-canary-controller-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`;

try {
  const result = await main();
  console.log(JSON.stringify(sanitize({ schema: "polyedge.strategy_canary_controller_run.v1", run_id: runId, ...result })));
} catch (error) {
  process.exitCode = 1;
  console.error(JSON.stringify({ schema: "polyedge.strategy_canary_controller_run.v1", run_id: runId, status: "failed_closed", error: error.message }));
}

async function main() {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable Azure Blob storage is unavailable");
  const intentContainer = storageContainer({ ...config, storageContainer: config.intentContainerName });
  const manifestContainer = storageContainer({ ...config, storageContainer: config.manifestContainerName });
  if (!intentContainer || !manifestContainer) throw new Error("fail closed: intent or manifest source container is unavailable");
  const [grantDocument, manifestDocument] = await Promise.all([
    loadHashedJson(container, config.humanGrantBlobName, config.humanGrantBlobHash),
    loadHashedJson(manifestContainer, config.manifestBlobName, config.manifestBlobHash)
  ]);
  const window = validateHumanGrant({ config, grant: grantDocument.value, manifest: manifestDocument.value });
  const selected = await waitForFirstQualifiedIntent(intentContainer, grantDocument.value, window.expiresMs);

  return executeCanaryControllerTransaction({
    config,
    grantDocument,
    manifestDocument,
    selected,
    container,
    runId,
    invokeChild: invokeCanaryOnce
  });
}

async function waitForFirstQualifiedIntent(container, grant, grantExpiresMs) {
  const deadline = Math.min(grantExpiresMs, Date.now() + config.maxWaitMs);
  const authorizedMs = Date.parse(grant.authorized_at);
  const seen = new Set();
  const candidates = [];
  while (Date.now() < deadline) {
    for await (const blob of container.listBlobsFlat({ prefix: `${config.intentPrefix}/` })) {
      if (!blob.name.endsWith(".json") || seen.has(blob.name)) continue;
      seen.add(blob.name);
      // Immutable publisher blobs from before the human window cannot be the
      // authorized future intent and are never downloaded.
      if (blob.properties?.lastModified && blob.properties.lastModified.getTime() < authorizedMs) continue;
      if (candidates.length >= 512) throw new Error("fail closed: too many fresh intent candidates inside one human grant window");
      const response = await container.getBlobClient(blob.name).download();
      const bytes = await streamToBuffer(response.readableStreamBody);
      let value;
      try { value = JSON.parse(bytes.toString("utf8")); } catch { continue; }
      candidates.push({ value, blobName: blob.name, hash: sha256(bytes) });
    }
    const selected = selectFirstQualifiedIntent({ config, grant, candidates, now: new Date() });
    if (selected) return selected;
    await sleep(Math.min(config.pollIntervalMs, Math.max(0, deadline - Date.now())));
  }
  throw new Error("fail closed: no fresh qualified immutable intent arrived inside the human grant window");
}

async function invokeCanaryOnce(env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [new URL("./canary.mjs", import.meta.url).pathname], {
      env,
      stdio: "inherit"
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => signal ? reject(new Error(`canary child terminated by ${signal}`)) : resolve(code ?? 1));
  });
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
