# Chainlink Source Confirmation

The default BTC 15-minute Up/Down target uses Polymarket RTDS Chainlink `btc/usd` as the exact public resolution-aligned reference source.

## RTDS Source

```text
wss://ws-live-data.polymarket.com
topic: crypto_prices_chainlink
symbol: btc/usd
```

Subscription payload:

```json
{
  "action": "subscribe",
  "subscriptions": [
    {
      "topic": "crypto_prices_chainlink",
      "type": "*",
      "filters": "{\"symbol\":\"btc/usd\"}"
    }
  ]
}
```

Binance RTDS and other proxy prices are cross-checks only. The Rust risk gate treats the Chainlink RTDS source as exact and can mark divergent exact/proxy combinations stale.

## Confirmation Command

Run:

```bash
cargo run -p polyedge-cli -- confirm-source
```

The command discovers current target markets, inspects market descriptions for Chainlink source language, and reports:

```text
backend_impl
shadow_only
configured_rtds_url
configured_chainlink_symbol
configured_resolution_source
discovered_markets
matched_markets
ok
```

`ok=true` requires at least one discovered market description mentioning the configured Chainlink source and RTDS Chainlink enabled in config.
