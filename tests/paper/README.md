# Paper Tests

Purpose: validate non-live workflow safety.

Checklist:
1. `MODE=paper` is set.
2. Trader opens and settles simulated positions.
3. `day_pnl_usd` changes as simulated trades settle.
4. Portfolio fields update in trader state: `portfolio_net_qty`, `portfolio_avg_entry`, `portfolio_unrealized_pnl_usd`, `portfolio_total_pnl_usd`.
5. `MAX_OPEN_POSITIONS` is respected.
6. Daily loss threshold stops process when breached.

Run:
- `powershell -File scripts/run_paper.ps1`
