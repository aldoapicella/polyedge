import { createPublicClient, formatUnits, http } from "viem";
import { polygon } from "viem/chains";

const funder = process.env.POLYMARKET_FUNDER_ADDRESS;
if (!/^0x[0-9a-fA-F]{40}$/.test(funder || "")) throw new Error("valid POLYMARKET_FUNDER_ADDRESS is required");

const client = createPublicClient({ chain: polygon, transport: http("https://polygon-bor-rpc.publicnode.com") });
const balanceOfAbi = [{
  type: "function",
  stateMutability: "view",
  name: "balanceOf",
  inputs: [{ name: "account", type: "address" }],
  outputs: [{ name: "", type: "uint256" }]
}];
const tokens = {
  pusd: "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
  usdce: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
};
const [bytecode, pUsd, usdce] = await Promise.all([
  client.getCode({ address: funder }),
  client.readContract({ address: tokens.pusd, abi: balanceOfAbi, functionName: "balanceOf", args: [funder] }),
  client.readContract({ address: tokens.usdce, abi: balanceOfAbi, functionName: "balanceOf", args: [funder] })
]);
console.log(JSON.stringify({
  funder,
  smart_wallet_deployed: Boolean(bytecode && bytecode !== "0x"),
  pusd: formatUnits(pUsd, 6),
  legacy_usdce: formatUnits(usdce, 6),
  order_submitted: false
}));
