pub fn unrealized_pnl(entry: f64, mark: f64, qty: f64) -> f64 {
    (mark - entry) * qty
}

pub fn total_pnl(realized: f64, entry: f64, mark: f64, qty: f64) -> f64 {
    realized + unrealized_pnl(entry, mark, qty)
}

pub fn pnl_bps(entry: f64, mark: f64) -> f64 {
    if entry <= 0.0 {
        return 0.0;
    }
    ((mark - entry) / entry) * 10_000.0
}

pub fn running_drawdown_pct(equity_curve: &[f64]) -> f64 {
    if equity_curve.is_empty() {
        return 0.0;
    }
    let mut peak = equity_curve[0];
    let mut worst_dd = 0.0;
    for &v in equity_curve {
        if v > peak {
            peak = v;
        }
        if peak > 0.0 {
            let dd = ((peak - v) / peak) * 100.0;
            if dd > worst_dd {
                worst_dd = dd;
            }
        }
    }
    worst_dd
}

pub fn sharpe_like(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let var = returns
        .iter()
        .map(|r| {
            let d = *r - mean;
            d * d
        })
        .sum::<f64>()
        / (returns.len() as f64 - 1.0);
    let std = var.sqrt();
    if std <= f64::EPSILON {
        return 0.0;
    }
    mean / std
}

#[cfg(test)]
mod tests {
    use super::{pnl_bps, running_drawdown_pct, total_pnl, unrealized_pnl};

    fn approx_eq(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "left={} right={}", a, b);
    }

    #[test]
    fn unrealized_supports_short_qty() {
        approx_eq(unrealized_pnl(0.60, 0.50, -10.0), 1.0);
    }

    #[test]
    fn total_pnl_adds_realized_and_unrealized() {
        approx_eq(total_pnl(2.5, 0.40, 0.45, 10.0), 3.0);
    }

    #[test]
    fn drawdown_tracks_peak_to_trough() {
        let dd = running_drawdown_pct(&[100.0, 110.0, 105.0, 90.0, 95.0]);
        approx_eq(dd, 18.181818181818183);
    }

    #[test]
    fn pnl_bps_handles_zero_entry() {
        approx_eq(pnl_bps(0.0, 0.6), 0.0);
    }
}

