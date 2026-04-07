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
- `NO_ENTRY_LAST_SECONDS_5M` and `NEAR_CERTAINTY_HOLD_SECONDS`: 5-minute market rollover safeguards.

## Ops playbook
1. Switch to paper mode for investigation.
2. Reduce notional and max open positions.
3. Verify quote stream stability and stale-data checks.
4. Resume only after metrics and logs normalize.
