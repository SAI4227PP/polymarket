# Polymarket BTC Bot (Rust + Go)

Hybrid architecture:
- `rust-core`: critical trading path (market data, strategy, risk, execution, portfolio)
- `go-services`: control plane (API, supervisor, jobs, monitoring)

## Quick Start
1. Copy `.env.example` to `.env` and fill keys.
2. Update `config/markets.yaml` with your Polymarket BTC market id.
3. Run Rust trader (paper mode):
   - `cd rust-core`
   - `cargo run -p trader`
4. Run Go dry run service:
   - `cd go-services`
   - `go run ./cmd/dry-run`

## Status
This scaffold is production-oriented structure with starter code and placeholders.
