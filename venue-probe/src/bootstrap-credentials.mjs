import { readFile } from "node:fs/promises";
import { DefaultAzureCredential } from "@azure/identity";
import { AssetType, Chain, ClobClient } from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";

const keyPath = process.env.POLYMARKET_PRIVATE_KEY_PATH;
const funderAddress = process.env.POLYMARKET_FUNDER_ADDRESS;
const signatureType = Number(process.env.POLYMARKET_SIGNATURE_TYPE || "3");
const vaultUrl = String(process.env.AZURE_KEY_VAULT_URL || "").replace(/\/?$/, "/");
if (!keyPath || !funderAddress || !vaultUrl) {
  throw new Error("POLYMARKET_PRIVATE_KEY_PATH, POLYMARKET_FUNDER_ADDRESS, and AZURE_KEY_VAULT_URL are required");
}

const privateKeyRaw = (await readFile(keyPath, "utf8")).trim();
if (!/^(0x)?[0-9a-fA-F]{64}$/.test(privateKeyRaw)) {
  throw new Error("private key file does not contain exactly one 32-byte hex key");
}
const privateKey = privateKeyRaw.startsWith("0x") ? privateKeyRaw : `0x${privateKeyRaw}`;
const account = privateKeyToAccount(privateKey);
const signer = createWalletClient({ account, chain: polygon, transport: http("https://polygon-bor-rpc.publicnode.com") });
const l1Client = new ClobClient({
  host: "https://clob.polymarket.com",
  chain: Chain.POLYGON,
  signer,
  signatureType,
  funderAddress,
  useServerTime: true,
  // createOrDeriveApiKey needs the create error response in order to fall back
  // to deterministic derivation when a key already exists.
  throwOnError: false
});
const creds = await l1Client.createOrDeriveApiKey();
if (!creds?.key || !creds?.secret || !creds?.passphrase) {
  throw new Error("Polymarket did not return complete API credentials");
}

const azureCredential = new DefaultAzureCredential();
const token = await azureCredential.getToken("https://vault.azure.net/.default");
for (const [name, value] of [
  ["polymarket-private-key", privateKey],
  ["polymarket-api-key", creds.key],
  ["polymarket-api-secret", creds.secret],
  ["polymarket-api-passphrase", creds.passphrase]
]) {
  const response = await fetch(`${vaultUrl}secrets/${name}?api-version=7.4`, {
    method: "PUT",
    headers: {
      authorization: `Bearer ${token.token}`,
      "content-type": "application/json"
    },
    body: JSON.stringify({ value, attributes: { enabled: true }, tags: { app: "polyedge", purpose: "venue-probe" } })
  });
  if (!response.ok) throw new Error(`Key Vault write failed for ${name}: HTTP ${response.status}`);
}

const l2Client = new ClobClient({
  host: "https://clob.polymarket.com",
  chain: Chain.POLYGON,
  signer,
  creds,
  signatureType,
  funderAddress,
  useServerTime: true,
  throwOnError: true
});
await l2Client.updateBalanceAllowance({ asset_type: AssetType.COLLATERAL });
const [serverTime, openOrders, balance] = await Promise.all([
  l2Client.getServerTime(),
  l2Client.getOpenOrders(undefined, true),
  l2Client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL })
]);

console.log(JSON.stringify({
  status: "credentials_derived_stored_and_l2_validated",
  signer_address: account.address,
  funder_address: funderAddress,
  signature_type: signatureType,
  server_time_available: Number.isFinite(Number(serverTime)),
  open_order_count: openOrders.length,
  collateral_balance_available: Number(balance.balance) > 0,
  allowance_count: Object.keys(balance.allowances || {}).length,
  key_vault_secret_count: 4,
  secret_values_printed: false,
  order_submitted: false
}));
