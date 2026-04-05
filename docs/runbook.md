# Runbook

## Local Docker Bring-up
1. `Copy-Item .env.example .env`
2. Set `POLYMARKET_ASSET_ID` or `POLYMARKET_MARKET_ID`.
3. `docker compose up --build -d`
4. Verify:
   - `curl http://localhost:8080/health`
   - `curl http://localhost:8080/status`

## Incident Actions
- Restart trader only:
  - `docker compose restart trader`
- Tail live logs:
  - `docker compose logs -f trader`
- Hard stop all:
  - `docker compose down`

## Rollback
- Re-deploy previous image tag in k8s/deployment manifests.
- Confirm API health and zero critical alerts before re-enabling trader.
