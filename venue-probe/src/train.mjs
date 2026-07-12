import {
  fitEffectiveQueueModel,
  loadProbeConfig,
  loadProbeObservations,
  sanitize,
  uploadModel
} from "./lib.mjs";

const config = loadProbeConfig({
  ...process.env,
  EXECUTION_MODE: process.env.EXECUTION_MODE || "venue_probe",
  ALLOW_LIVE: "false",
  ALLOW_VENUE_PROBE: "true",
  ENABLE_TAKER_ORDERS: "false",
  MAX_OPEN_ORDERS: "1",
  VENUE_PROBE_MAXIMUM_ORDERS: "1",
  POLYMARKET_PRIVATE_KEY: process.env.POLYMARKET_PRIVATE_KEY || "unused-for-training",
  POLYMARKET_API_KEY: process.env.POLYMARKET_API_KEY || "unused-for-training",
  POLYMARKET_API_SECRET: process.env.POLYMARKET_API_SECRET || "unused-for-training",
  POLYMARKET_API_PASSPHRASE: process.env.POLYMARKET_API_PASSPHRASE || "unused-for-training",
  POLYMARKET_FUNDER_ADDRESS: process.env.POLYMARKET_FUNDER_ADDRESS || "unused-for-training"
});

const observations = await loadProbeObservations(config);
const model = fitEffectiveQueueModel(observations);
await uploadModel(config, model);
console.log(JSON.stringify(sanitize(model)));
