# Docker Deploy

Build and run full stack:
- `docker compose up --build -d`

Services:
- `trader` (Rust critical path)
- `api` (Go control plane API on :8080)
- `supervisor` (Go control loop)
- `postgres`
- `redis`

Logs:
- `docker compose logs -f trader`
- `docker compose logs -f api`

Shutdown:
- `docker compose down`
