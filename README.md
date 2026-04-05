# Polymarket BTC Bot (Rust + Go)

Hybrid architecture:
- `rust-core`: critical trading path (market data, strategy, risk, execution, portfolio)
- `go-services`: control plane (API, supervisor, jobs, monitoring)

## Quick Start
1. Copy `.env.example` to `.env` and fill keys.
2. Set `POLYMARKET_ASSET_ID` for the BTC market token id.
3. Run Rust trader (paper mode):
   - `cd rust-core`
   - `cargo run -p trader`

The Rust trader now connects to:
- Binance WS: `wss://stream.binance.com:9443/ws/<symbol>@bookTicker`
- Polymarket WS: `wss://ws-subscriptions-clob.polymarket.com/ws/market`

## Status
This scaffold includes real websocket quote ingestion and placeholder execution/risk logic.
