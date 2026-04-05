# Polymarket BTC Bot (Rust + Go)

Hybrid architecture for BTC market watch and execution logic.

- `rust-core`: realtime streams + signal + execution loop
- `go-services`: API/control-plane, reading from Redis cache

## Data Flow (Redis-centric)
1. Rust trader consumes Polymarket and Binance streams.
2. Trader computes signal/execution state.
3. Trader publishes latest state to Redis (`REDIS_TRADER_STATE_KEY`).
4. Go API refreshes snapshot/status/metrics/trades into Redis keys.
5. REST + WebSocket endpoints serve data from Redis.

## Quick Start
1. Copy env: `Copy-Item .env.example .env`
2. Set one of `POLYMARKET_ASSET_ID` or `POLYMARKET_MARKET_ID`
3. Start stack: `docker compose up --build -d`
4. Verify:
   - `curl http://localhost:8080/health`
   - `curl http://localhost:8080/trader-state`

## APIs
- `GET /health`
- `GET /status`
- `GET /metrics`
- `GET /config`
- `GET /trades?limit=20&status=submitted`
- `GET /snapshot?limit=20`
- `GET /data?limit=20`
- `GET /trader-state`
- `WS /ws/stream?interval_ms=1000&limit=20`

`/ws/stream` includes real trader fields from Redis:
- `market.polymarket_mid`
- `market.binance_mid`
- `signal.edge_bps`
- `signal.net_edge_bps`
- `signal.direction`
- `execution.execution_status`
- `execution.open_positions`
- `execution.day_pnl_usd`
