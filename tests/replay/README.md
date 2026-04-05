# Replay Tests

Purpose: deterministic playback checks on captured websocket messages.

Plan:
1. Save real Binance and Polymarket frames to fixtures.
2. Replay frames through parser functions.
3. Assert quote extraction and stale-message handling.
4. Assert no parser panic on malformed payloads.

Recommended fixture location:
- `tests/replay/fixtures/*.jsonl`
