# Webhook Testing & Simulation Laboratory

A single-file Node.js + Express server that sends real HTTP webhook requests
to a receiver endpoint you control, so you can practice building robust
webhook consumers: signature verification, retries, idempotency, replay
defense, malformed payloads, concurrency, and more.

It **sends** webhooks. It does not receive them — point it at your own
application's webhook endpoint.

## 1. Dependencies

Only one runtime dependency:

```json
"dependencies": {
  "express": "^4.19.2"
}
```

Everything else (HTTP client, HMAC, UUIDs, timers) uses Node's built-ins
(`fetch`, `node:crypto`). Requires Node.js 20+.

## 2. Local setup

```bash
npm install
cp .env.example .env
# edit .env and set MANAGEMENT_API_KEY
npm start
```

Server listens on `http://localhost:3000` (or `$PORT`).

## 3. Deploying to Render

1. Push this project to a GitHub repo (`server.js`, `package.json`, etc).
2. In Render: **New → Web Service**, connect the repo.
3. Build command: `npm install`
4. Start command: `npm start`
5. Environment: Node
6. Add an environment variable `MANAGEMENT_API_KEY` with a long random
   value (don't skip this — without it every `/api/*` route refuses
   requests).
7. Optionally add any of the tuning variables from `.env.example`.
8. Deploy. Render assigns `PORT` automatically; the server already binds to
   `0.0.0.0` and reads `process.env.PORT`, so no changes are needed.
9. `GET https://<your-service>.onrender.com/health` should return
   `{"ok":true,"status":"healthy",...}` with no API key required.

Your **receiver** application (the thing being tested) is a separate app —
deploy it anywhere reachable over the internet (Render, a tunnel, etc.) and
register its URL as a target (see below).

## 4. Core concept: Targets

Everything starts with registering a **target** — the receiver endpoint
this simulator will send requests to.

```
POST /api/targets
{
  "name": "my-app",
  "url": "https://my-app.onrender.com/webhooks/incoming",
  "security": {
    "enabled": true,
    "algorithm": "sha256",
    "secret": "a-shared-signing-secret",
    "signatureHeader": "X-Webhook-Signature",
    "signatureFormat": "sha256={sig}"
  },
  "retryPolicy": { "maxAttempts": 4, "strategy": "exponential", "baseDelayMs": 500 }
}
```

Use the returned `target.id` in every simulate/scheduler/polling call.
All `/api/*` routes require header `X-API-Key: <MANAGEMENT_API_KEY>`.

## 5. Bruno request examples

Create a Bruno collection with an environment variable `apiKey` and
`baseUrl` (e.g. `http://localhost:3000`). Example requests:

**Health (no auth)**
```
GET {{baseUrl}}/health
```

**Create a target**
```
POST {{baseUrl}}/api/targets
Headers: X-API-Key: {{apiKey}}
Body (json):
{
  "name": "local-test",
  "url": "https://my-receiver.example.com/webhooks",
  "security": { "enabled": true, "secret": "topsecret" }
}
```

**Send one random event**
```
POST {{baseUrl}}/api/simulate/random
Headers: X-API-Key: {{apiKey}}
Body: { "targetId": "tgt_xxx" }
```

**Send a specific event type**
```
POST {{baseUrl}}/api/simulate/event
Headers: X-API-Key: {{apiKey}}
Body: { "targetId": "tgt_xxx", "type": "payment.successful" }
```

**Run a full order lifecycle**
```
POST {{baseUrl}}/api/simulate/lifecycle
Headers: X-API-Key: {{apiKey}}
Body: { "targetId": "tgt_xxx", "domain": "order", "outcome": "success", "delayMs": 1000 }
```

**Run a security scenario**
```
POST {{baseUrl}}/api/simulate/security
Headers: X-API-Key: {{apiKey}}
Body: { "targetId": "tgt_xxx", "scenario": "expired_timestamp" }
```

**Replay a past delivery**
```
POST {{baseUrl}}/api/simulate/replay
Headers: X-API-Key: {{apiKey}}
Body: { "deliveryId": "del_xxx", "mode": "exact_replay" }
```

**Start the scheduler**
```
POST {{baseUrl}}/api/scheduler/start
Headers: X-API-Key: {{apiKey}}
Body: { "targetId": "tgt_xxx", "intervalMs": 15000, "mode": "mixed" }
```

**Inspect recent deliveries**
```
GET {{baseUrl}}/api/deliveries?limit=20&success=false
Headers: X-API-Key: {{apiKey}}
```

## 6. Example scenarios by capability

| Capability | Request |
|---|---|
| Basic JSON webhook | `POST /api/simulate/event {"targetId":"...","type":"user.registered"}` |
| Custom headers / content type | `POST /api/simulate/event {"targetId":"...","headers":{"X-Test":"1"},"contentType":"application/x-www-form-urlencoded"}` |
| Full lifecycle (success) | `POST /api/simulate/lifecycle {"targetId":"...","domain":"payment","outcome":"success"}` |
| Lifecycle that fails mid-way | `POST /api/simulate/lifecycle {"targetId":"...","domain":"payment","outcome":"failure"}` |
| Out-of-order lifecycle | `POST /api/simulate/lifecycle {"targetId":"...","domain":"order","mutation":"out_of_order"}` |
| Pending lifecycle + resume later | `POST /api/simulate/lifecycle {"...","mutation":"pending"}` then `POST /api/simulate/lifecycle {"resumeOf":"life_xxx"}` |
| Valid HMAC signature | `POST /api/simulate/security {"scenario":"valid_hmac"}` |
| Invalid / missing signature | `POST /api/simulate/security {"scenario":"incorrect_signature"}` |
| Expired / future timestamp (clock skew) | `POST /api/simulate/security {"scenario":"expired_timestamp"}` |
| Payload tampering after signing | `POST /api/simulate/security {"scenario":"modified_payload_after_signing"}` |
| Duplicate deliveries (exact / concurrent) | `POST /api/simulate/event {"duplicate":{"count":3,"variant":"exact","concurrent":true}}` |
| Malformed JSON / empty body / large payload | `POST /api/simulate/security {"scenario":"malformed_json"}` |
| Retry + exponential backoff | `POST /api/simulate/event {"retryOverride":{"maxAttempts":5,"strategy":"exponential"}}` |
| Concurrent batch delivery | `POST /api/simulate/batch {"targetId":"...","count":10,"mode":"concurrent","concurrencyLimit":4}` |
| Replay attack | `POST /api/simulate/replay {"deliveryId":"del_xxx","mode":"exact_replay"}` |
| Polling vs. webhooks | `POST /api/polling/start {"targetUrl":"https://my-app/poll","intervalMs":5000}` |
| Background random traffic | `POST /api/scheduler/start {"targetId":"...","mode":"mixed","intervalMs":15000}` |

See `GET /api/scenarios` for the full beginner/intermediate/advanced
catalogue with descriptions, and `GET /api/events` / `GET /api/deliveries`
/ `GET /api/stats` to inspect what's happened so far.

## 7. Notes on security scenarios

Every scenario in `SECURITY_SCENARIOS` only manipulates the request this
process sends to **the target you registered**. It never scans, probes, or
sends traffic to any third-party system, and payload sizes are bounded by
`MAX_PAYLOAD_BYTES`. This is a tool for exercising your own receiver's
input validation and auth logic, not for attacking anyone else's system.
