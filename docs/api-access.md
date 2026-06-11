# API Access

The public entrypoint is the Next.js frontend and server-side backend proxy:

```text
https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io
```

Browser/API callers use:

```text
/api/backend/<path>
```

The frontend server forwards those calls to the Rust backend sidecar:

```text
http://127.0.0.1:8081/api/v1/<path>
```

The bearer token is kept server-side in the frontend container and must not be exposed to browser code.

## Health Check

```bash
curl https://polyedge-dev.graypond-7f5d8417.eastus.azurecontainerapps.io/api/backend/health
```

Expected fields:

```json
{
  "ok": true,
  "backend_impl": "rust",
  "shadow_only": false,
  "execution_mode": "paper",
  "kill_switch": false
}
```

## Main Routes

```text
GET  /api/backend/health
GET  /api/backend/status
GET  /api/backend/snapshot
GET  /api/backend/markets
GET  /api/backend/markets/current
GET  /api/backend/markets/history?limit=100
GET  /api/backend/markets/{market_id}
GET  /api/backend/markets/{market_id}/chart
GET  /api/backend/orders
GET  /api/backend/fills
GET  /api/backend/decisions
GET  /api/backend/events/recent?limit=100
GET  /api/backend/pnl
POST /api/backend/reports/build
GET  /api/backend/reports/latest
POST /api/backend/control/pause
POST /api/backend/control/resume
POST /api/backend/control/kill-switch
GET  /api/realtime
```

## Production Validation

On the active Rust revision, these frontend proxy paths returned HTTP 200 in under one second:

```text
/api/backend/health
/api/backend/status
/api/backend/snapshot
/api/backend/markets/current
/api/backend/orders
/api/backend/fills
/api/backend/decisions
/dashboard
```
