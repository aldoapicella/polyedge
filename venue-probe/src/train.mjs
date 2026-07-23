import {
  fitEffectiveQueueModel,
  loadCheckpointProbeObservations,
  sanitize,
  uploadModel
} from "./lib.mjs";

if (process.env.QUEUE_MODEL_TRAINING_ENABLED !== "true") {
  throw new Error("fail closed: QUEUE_MODEL_TRAINING_ENABLED must be true");
}
if (process.env.ALLOW_LIVE !== "false" || process.env.ENABLE_TAKER_ORDERS !== "false") {
  throw new Error("fail closed: training job must remain non-executable");
}
const config = {
  storageAccount: process.env.AZURE_STORAGE_ACCOUNT_NAME,
  storageContainer: process.env.QUEUE_MODEL_OUTPUT_CONTAINER_NAME || "polyedge-models",
  storageAccountKey: process.env.AZURE_STORAGE_ACCOUNT_KEY,
  azureClientId: process.env.AZURE_CLIENT_ID
};
if (!config.storageAccount) throw new Error("fail closed: Azure storage account is required");
const sourceConfig = {
  ...config,
  storageContainer: process.env.QUEUE_MODEL_SOURCE_CONTAINER_NAME || "polyedge-funded-evidence"
};

const evidence = await loadCheckpointProbeObservations(
  sourceConfig,
  process.env.QUEUE_MODEL_CHECKPOINT_BLOB_NAME,
  process.env.QUEUE_MODEL_CHECKPOINT_SHA256
);
const model = fitEffectiveQueueModel(evidence.observations, 100);
const uploaded = await uploadModel(config, model, {
  observations: evidence.observations,
  checkpoint: evidence.checkpoint,
  candidate: evidence.candidate
});
console.log(JSON.stringify(sanitize({ model, immutable_model: uploaded })));
