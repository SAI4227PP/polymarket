# Integration Tests

Purpose: validate service/module integration with Redis as central data plane.

Scenarios:
1. Trader publishes live state to Redis key `trader:state:latest`.
2. API refresh job writes snapshot/status/metrics/trades to Redis.
3. REST endpoints read from Redis and return data.
4. `/ws/stream` pushes unified frames from Redis-backed data.

API checklist:
- `GET /health`
- `GET /status`
- `GET /metrics`
- `GET /config`
- `GET /trades?limit=20&status=submitted`
- `GET /snapshot?limit=20`
- `GET /data?limit=20`
- `GET /trader-state`
- `WS /ws/stream?interval_ms=1000&limit=20`

Run manually:
- `docker compose up --build`
- `curl http://localhost:8080/health`
- `curl http://localhost:8080/trader-state`
- `curl http://localhost:8080/snapshot`
