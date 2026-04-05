# Docker Deploy

Build and run full stack:
- `docker compose up --build -d`

Services:
- `trader` (Rust critical path)
- `api` (Go control plane API on internal :8080, fronted by nginx on :80)
- `supervisor` (Go control loop)
- `postgres`
- `redis`

Logs:
- `docker compose logs -f trader`
- `docker compose logs -f api`

Shutdown:
- `docker compose down`

Nginx:
- reverse proxy exposed on :80
- proxies REST and /ws/stream to api service


Endpoint:
- http://polymarket.trackeatfit.me/health
- ws://polymarket.trackeatfit.me/ws/stream

