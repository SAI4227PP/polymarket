# Unit Tests

Purpose: validate deterministic pure logic.

Coverage targets:
- Rust: signal math, risk checks, execution routing, slippage.
- Go: retry behavior, numeric utils, repository semantics.

Run:
- Rust: `cd rust-core && cargo test`
- Go: `cd go-services && go test ./...`
