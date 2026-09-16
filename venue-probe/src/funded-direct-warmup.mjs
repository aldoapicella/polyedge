import { DefaultAzureCredential } from "@azure/identity";
import { ServiceBusClient } from "@azure/service-bus";

const namespace = required("FUNDED_DIRECT_SERVICE_BUS_NAMESPACE");
const queue = required("FUNDED_DIRECT_SERVICE_BUS_QUEUE");
const credential = new DefaultAzureCredential({ managedIdentityClientId: required("AZURE_CLIENT_ID") });
const market = await activeBtcFifteenMinuteMarket();
const tokenIds = parseTokenIds(market.clobTokenIds);
const preflightStarted = performance.now();
const [geoblock, venueClock, orderBook] = await Promise.all([
  fetchJson("https://polymarket.com/api/geoblock"),
  fetchJson("https://clob.polymarket.com/time"),
  fetchJson(`https://clob.polymarket.com/book?token_id=${encodeURIComponent(tokenIds[0])}`)
]);
if (geoblock?.blocked !== false ||
    String(geoblock?.country || "").toUpperCase() !== "CL" ||
    !Number.isFinite(Number(venueClock?.server_time ?? venueClock?.time ?? venueClock)) ||
    !Array.isArray(orderBook?.asks) ||
    !orderBook.asks.length) {
  throw new Error("fail closed: no-sign warmup REST/origin validation failed");
}

const messageId = `warmup-${market.conditionId}-${crypto.randomUUID()}`;
const body = {
  schema: "polyedge.funded_market_warmup.v1",
  market_id: String(market.id),
  condition_id: String(market.conditionId),
  token_id: tokenIds[0],
  token_ids: tokenIds,
  market_end_ts: new Date(market.endDate).toISOString(),
  producer_ts: new Date().toISOString()
};
const bus = new ServiceBusClient(`${namespace}.servicebus.windows.net`, credential);
const sender = bus.createSender(queue);
const sendStarted = performance.now();
try {
  await sender.sendMessages({
    body,
    messageId,
    contentType: "application/json",
    timeToLive: 30_000
  });
  console.log(JSON.stringify({
    schema: "polyedge.funded_cloud_no_sign_rehearsal.v1",
    status: "warmup_sent",
    message_id: messageId,
    market_id: body.market_id,
    condition_id: body.condition_id,
    token_ids: body.token_ids,
    preflight_duration_ms: performance.now() - preflightStarted,
    queue_send_duration_ms: performance.now() - sendStarted,
    no_sign: true,
    private_key_present: Boolean(process.env.POLYMARKET_PRIVATE_KEY)
  }));
} finally {
  await sender.close().catch(() => null);
  await bus.close().catch(() => null);
}

async function activeBtcFifteenMinuteMarket() {
  const nowSeconds = Math.floor(Date.now() / 1_000);
  const currentStart = Math.floor(nowSeconds / 900) * 900;
  for (const start of [currentStart, currentStart + 900]) {
    const values = await fetchJson(`https://gamma-api.polymarket.com/markets?slug=btc-updown-15m-${start}`);
    const market = Array.isArray(values) ? values[0] : null;
    if (market?.active === true &&
        market?.closed !== true &&
        market?.acceptingOrders === true &&
        Date.parse(market.endDate) > Date.now() + 30_000) {
      return market;
    }
  }
  throw new Error("fail closed: active BTC 15-minute market was not discoverable for cloud warmup");
}

function parseTokenIds(value) {
  const values = Array.isArray(value) ? value : JSON.parse(String(value || "[]"));
  const tokens = values.map(String).filter(Boolean);
  if (tokens.length !== 2) throw new Error("fail closed: warmup market must have exactly two token ids");
  return tokens;
}

async function fetchJson(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`fail closed: warmup request failed (${response.status} ${url})`);
  return response.json();
}

function required(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) throw new Error(`fail closed: ${name} is required`);
  return value;
}
