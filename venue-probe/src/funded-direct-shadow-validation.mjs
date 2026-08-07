import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { sha256 } from "./canary-lib.mjs";

export const FUNDED_DIRECT_SHADOW_SEAL_SCHEMA = "polyedge.funded_direct_shadow_validation.v1";
export const FUNDED_DIRECT_SHADOW_TRANSITION_SCHEMA = "polyedge.funded_direct_shadow_transition.v1";
export const REQUIRED_ELIGIBLE_TRANSITIONS = 100;
export const REQUIRED_TRAINING_TRANSITIONS = 70;
export const REQUIRED_HOLDOUT_TRANSITIONS = 30;
export const REQUIRED_DISTINCT_MARKETS = 20;
export const LISTED_FAILURES = Object.freeze([
  "stale_child_launch",
  "authorization_leak",
  "reservation_leak",
  "unexpected_equity_bypass",
  "open_order_mismatch",
  "terminal_reconciliation_failure"
]);

export function buildFundedDirectShadowSeal({
  transitions,
  candidateName,
  candidateVersion,
  candidateConfigHash,
  generatedAt = new Date()
}) {
  if (!Array.isArray(transitions)) throw new Error("funded_direct_shadow_validation blocked: transitions must be an array");
  const eligible = transitions.filter((row) => row?.eligible === true);
  const failures = Object.fromEntries(LISTED_FAILURES.map((name) => [name, 0]));
  const errors = [];
  if (eligible.length !== REQUIRED_ELIGIBLE_TRANSITIONS) {
    errors.push(`eligible transition count must equal ${REQUIRED_ELIGIBLE_TRANSITIONS}`);
  }
  let previousObservedMs = -Infinity;
  const seenSequences = new Set();
  for (const [index, transition] of eligible.entries()) {
    const observedMs = Date.parse(transition?.observed_at);
    if (transition?.schema !== FUNDED_DIRECT_SHADOW_TRANSITION_SCHEMA) errors.push(`transition ${index + 1} has an invalid schema`);
    if (!Number.isFinite(observedMs) || observedMs < previousObservedMs) errors.push(`transition ${index + 1} is not time ordered`);
    previousObservedMs = observedMs;
    if (!Number.isInteger(transition?.sequence) || transition.sequence < 1 || seenSequences.has(transition.sequence)) {
      errors.push(`transition ${index + 1} has an invalid or duplicate sequence`);
    }
    seenSequences.add(transition?.sequence);
    if (!["paper", "shadow"].includes(transition?.mode)) errors.push(`transition ${index + 1} is not paper/shadow`);
    if (!clean(transition?.market_id)) errors.push(`transition ${index + 1} is missing market_id`);
    if (transition?.funded_order_submitted !== false) errors.push(`transition ${index + 1} attempted funded execution`);
    for (const name of LISTED_FAILURES) {
      if (transition?.failures?.[name] === true) failures[name] += 1;
      else if (transition?.failures?.[name] !== false) errors.push(`transition ${index + 1} is missing ${name}`);
    }
  }
  const distinctMarketCount = new Set(eligible.map((row) => clean(row?.market_id)).filter(Boolean)).size;
  if (distinctMarketCount < REQUIRED_DISTINCT_MARKETS) {
    errors.push(`distinct market count must be at least ${REQUIRED_DISTINCT_MARKETS}`);
  }
  if (Object.values(failures).some((count) => count !== 0)) errors.push("listed failure count must be zero");
  if (errors.length) throw new Error(`funded_direct_shadow_validation blocked: ${[...new Set(errors)].join("; ")}`);
  const generated = generatedAt instanceof Date ? generatedAt : new Date(generatedAt);
  if (!Number.isFinite(generated.getTime())) throw new Error("funded_direct_shadow_validation blocked: generated_at is invalid");
  return {
    schema: FUNDED_DIRECT_SHADOW_SEAL_SCHEMA,
    passed: true,
    generated_at: generated.toISOString(),
    mode: "isolated_paper_shadow",
    funded_execution_enabled: false,
    qset_untouched: true,
    split: {
      method: "time_ordered_70_30",
      training_transitions: REQUIRED_TRAINING_TRANSITIONS,
      holdout_transitions: REQUIRED_HOLDOUT_TRANSITIONS
    },
    eligible_transition_count: eligible.length,
    distinct_market_count: distinctMarketCount,
    listed_failures: failures,
    candidate: {
      name: clean(candidateName),
      candidate_version: clean(candidateVersion),
      config_hash: normalizeHash(candidateConfigHash)
    },
    transition_evidence_sha256: sha256(Buffer.from(stableJson(eligible)))
  };
}

export function validateFundedDirectShadowSeal(seal, expected = {}) {
  const generatedMs = Date.parse(seal?.generated_at);
  const valid = seal?.schema === FUNDED_DIRECT_SHADOW_SEAL_SCHEMA
    && seal.passed === true
    && seal.mode === "isolated_paper_shadow"
    && seal.funded_execution_enabled === false
    && seal.qset_untouched === true
    && seal.split?.method === "time_ordered_70_30"
    && Number(seal.split?.training_transitions) === REQUIRED_TRAINING_TRANSITIONS
    && Number(seal.split?.holdout_transitions) === REQUIRED_HOLDOUT_TRANSITIONS
    && Number(seal.eligible_transition_count) === REQUIRED_ELIGIBLE_TRANSITIONS
    && Number(seal.distinct_market_count) >= REQUIRED_DISTINCT_MARKETS
    && LISTED_FAILURES.every((name) => Number(seal.listed_failures?.[name]) === 0)
    && normalizeHash(seal.transition_evidence_sha256)
    && Number.isFinite(generatedMs)
    && (!expected.candidateName || seal.candidate?.name === expected.candidateName)
    && (!expected.candidateVersion || seal.candidate?.candidate_version === expected.candidateVersion)
    && (!expected.candidateConfigHash ||
      normalizeHash(seal.candidate?.config_hash) === normalizeHash(expected.candidateConfigHash));
  if (!valid) throw new Error("funded_direct_worker blocked: isolated paper/shadow 70/30 validation seal is missing or invalid");
  return seal;
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function clean(value) {
  return String(value || "").trim();
}

function normalizeHash(value) {
  const text = clean(value).toLowerCase();
  const prefixed = text.startsWith("sha256:") ? text : `sha256:${text}`;
  return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : "";
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  const inputPath = process.argv[2];
  if (!inputPath) throw new Error("usage: node src/funded-direct-shadow-validation.mjs <transitions.json>");
  const input = JSON.parse(await readFile(inputPath, "utf8"));
  console.log(JSON.stringify(buildFundedDirectShadowSeal(input), null, 2));
}
