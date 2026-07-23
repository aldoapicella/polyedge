import { readFile } from "node:fs/promises";
import { AssetType, Chain, ClobClient } from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";

const keyPath = process.env.POLYMARKET_PRIVATE_KEY_PATH;
const funderAddress = process.env.POLYMARKET_FUNDER_ADDRESS;
if (!keyPath || !/^0x[0-9a-fA-F]{40}$/.test(funderAddress || "")) {
  throw new Error("POLYMARKET_PRIVATE_KEY_PATH and valid POLYMARKET_FUNDER_ADDRESS are required");
}
const raw = (await readFile(keyPath, "utf8")).trim();
const privateKey = raw.startsWith("0x") ? raw : `0x${raw}`;
const account = privateKeyToAccount(privateKey);
const signer = createWalletClient({ account, chain: polygon, transport: http("https://polygon-bor-rpc.publicnode.com") });
const l1 = new ClobClient({
  host: "https://clob.polymarket.com",
  chain: Chain.POLYGON,
  signer,
  useServerTime: true,
  throwOnError: false
});
const creds = await l1.createOrDeriveApiKey();
const mappings = [];
for (const signatureType of [1, 2, 3]) {
  const client = new ClobClient({
    host: "https://clob.polymarket.com",
    chain: Chain.POLYGON,
    signer,
    creds,
    signatureType,
    funderAddress,
    useServerTime: true,
    throwOnError: true
  });
  await client.updateBalanceAllowance({ asset_type: AssetType.COLLATERAL });
  await new Promise((resolve) => setTimeout(resolve, 1500));
  const [balance, orders] = await Promise.all([
    client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL }),
    client.getOpenOrders(undefined, true)
  ]);
  mappings.push({
    signature_type: signatureType,
    collateral_balance_base_units: balance.balance,
    allowance_count: Object.keys(balance.allowances || {}).length,
    open_order_count: orders.length
  });
}
console.log(JSON.stringify({
  signer_address: account.address,
  funder_address: funderAddress,
  mappings,
  secret_values_printed: false,
  order_submitted: false
}));
