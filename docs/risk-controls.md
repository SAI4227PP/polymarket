# Risk Controls

## Pre-trade limits
- `MAX_NOTIONAL_USD`: block order if exceeded.
- `MAX_OPEN_POSITIONS`: block new intent when open positions hit cap.
- `MIN_EDGE_BPS`: ignore low-conviction opportunities.

## Runtime protections
- `MAX_DAILY_LOSS_USD`: stop process when breached.
- `KILL_SWITCH_DRAWDOWN_PCT`: emergency stop based on drawdown signal.
- `KILL_SWITCH_ERROR_STREAK`: stop after repeated failures.

## Ops playbook
1. Switch to paper mode for investigation.
2. Reduce notional and max open positions.
3. Verify quote stream stability and stale-data checks.
4. Resume only after metrics and logs normalize.
