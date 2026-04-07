# Risk Controls

## Pre-trade limits
- `MAX_NOTIONAL_USD`: block order if exceeded.
- `MAX_OPEN_POSITIONS`: block new intent when open positions hit cap.
- `MIN_EDGE_BPS`: ignore low-conviction opportunities.

## Runtime protections
- `MAX_DAILY_LOSS_USD`: stop process when breached.
- `KILL_SWITCH_DRAWDOWN_PCT`: emergency stop based on live drawdown from intraday peak PnL.
- `KILL_SWITCH_MIN_PEAK_PNL_USD`: only enable percent drawdown kill once peak PnL reaches this minimum.
- `KILL_SWITCH_ERROR_STREAK`: stop after repeated failures.
- `KILL_SWITCH_STALE_DATA_MS`: stop when quote freshness degrades beyond this threshold.
- `HOLD_STRONG_EDGE_BPS` and `INVALIDATION_CLOSE_BPS`: position hold vs invalidation thresholds.
- `NO_ENTRY_FIRST_SECONDS_5M`, `NO_ENTRY_LAST_SECONDS_5M`, and `NEAR_CERTAINTY_HOLD_SECONDS`: 5-minute market rollover safeguards.

## 5-Minute BTC Up/Down model notes
- Signal anchor is the start-of-window Binance price for the active 5-minute bucket.
- Fair value is centered around 0.5 and moves with relative return versus that anchor.
- This aligns trading logic with market rules (end price compared against start price).

## Execution quality filters
- `EXECUTION_MIN_NET_EDGE_MARGIN_BPS`: require extra edge above entry threshold before placing risk.
- `EXECUTION_MAX_SLIPPAGE_BPS`: skip entries when estimated slippage is too expensive.

## Ops playbook
1. Switch to paper mode for investigation.
2. Reduce notional and max open positions.
3. Verify quote stream stability and stale-data checks.
4. Resume only after metrics and logs normalize.
