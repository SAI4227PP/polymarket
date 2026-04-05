# Paper Tests

Purpose: validate non-live workflow safety.

Checklist:
1. `MODE=paper` is set.
2. Trader opens and settles simulated positions.
3. `day_pnl_usd` changes as simulated trades settle.
4. `MAX_OPEN_POSITIONS` is respected.
5. Daily loss threshold stops process when breached.

Run:
- `powershell -File scripts/run_paper.ps1`
