# Architecture

## Runtime split
- Rust (`rust-core`) handles websocket ingestion and trading loop logic.
- Go (`go-services`) handles orchestration, API, telemetry, and ops workflows.

## Data flow
1. Binance and Polymarket adapters stream quotes.
2. Trader computes signal and checks risk.
3. Execution creates order intent (paper/live extension point).
4. Position settlement updates day PnL.
5. Kill switch and daily loss protections gate runtime.

## Deployment topology
- `trader`: Rust service (stateless process)
- `api`: Go HTTP service (`/health`, `/status`, `/metrics`)
- `supervisor`: Go orchestration loop
- `postgres`, `redis`: infrastructure services
