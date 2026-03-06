# Kimp Worker (Cloudflare)

This folder contains a Cloudflare Worker + Durable Object skeleton for real-time kimchi premium streaming.

## What it does
- Durable Object `KimpHub` keeps upstream WS connections:
  - Upbit ticker WS (single connection)
  - Binance spot WS (sharded by stream chunk)
- Computes symbol intersection (`UPBIT KRW` x `BINANCE USDT`)
- Maintains in-memory latest tick cache
- Pushes snapshots to frontend clients via one WS endpoint: `/ws`

## Payload schema
Server -> client (`type: snapshot`):
```json
{
  "type": "snapshot",
  "timestamp": 1772730000000,
  "fxRate": 1400,
  "rows": [
    {
      "symbol": "BTC",
      "name": "비트코인",
      "domestic": { "price": 104000000, "change": -1.2, "volume": 123000000000 },
      "foreign": { "price": 68000, "change": -0.9, "volume": 90000000000 },
      "gimp": 1.8
    }
  ]
}
```

Client -> server (`type: subscribe`) optional:
```json
{ "type": "subscribe", "domestic": "upbit", "foreign": "binance" }
```

## Deploy
1. `cd worker`
2. `wrangler deploy`
3. Bind route/domain so frontend can access `/ws` on same origin.
   - Or set `window.KIMP_WS_URL` (or localStorage `KIMP_WS_URL`) in frontend to explicit WS URL.

## Wallet Status Sources
- Frontend should call only `/asset-status`.
- Worker runs collectors and caches wallet status for about 5 minutes.
- Upbit collector uses `/v1/deposits/chance/coin` per currency (JWT signed with query_hash).
- Public sources:
  - Binance, Bitget, Gate, Bithumb, Coinone
- API-key required sources:
  - Upbit (`UPBIT_ACCESS_KEY`, `UPBIT_SECRET_KEY`)
  - OKX (`OKX_API_KEY`, `OKX_SECRET_KEY`, `OKX_PASSPHRASE`)
  - Bybit (`BYBIT_API_KEY`, `BYBIT_SECRET_KEY`)

If keys are missing for private endpoints, response returns `AUTH_REQUIRED` network marker.

## Notes
- This is a production-oriented skeleton, not final hardened code.
- Add auth/rate-limit/observability before public launch.
- If Binance stream URL length grows, lower `BINANCE_STREAM_CHUNK`.
