pub fn unrealized_pnl(entry: f64, mark: f64, qty: f64) -> f64 {
    if !entry.is_finite() || !mark.is_finite() || !qty.is_finite() {
        return 0.0;
    }
    (mark - entry) * qty
}

pub fn total_pnl(realized: f64, entry: f64, mark: f64, qty: f64) -> f64 {
    if !realized.is_finite() {
        return 0.0;
    }
    realized + unrealized_pnl(entry, mark, qty)
}

pub fn pnl_bps(entry: f64, mark: f64) -> f64 {
    if !entry.is_finite() || !mark.is_finite() || entry <= 0.0 {
        return 0.0;
    }
    ((mark - entry) / entry) * 10_000.0
}

pub fn running_drawdown_pct(equity_curve: &[f64]) -> f64 {
    let mut peak: Option<f64> = None;
    let mut worst_dd = 0.0;

    for v in equity_curve.iter().copied().filter(|v| v.is_finite()) {
        let current_peak = match peak {
            Some(p) if p >= v => p,
            _ => {
                peak = Some(v);
                v
            }
        };

        if current_peak > 0.0 {
            let dd = ((current_peak - v) / current_peak) * 100.0;
            if dd.is_finite() && dd > worst_dd {
                worst_dd = dd;
            }
        }
    }

    worst_dd
}

pub fn sharpe_like(returns: &[f64]) -> f64 {
    let finite_returns: Vec<f64> = returns.iter().copied().filter(|r| r.is_finite()).collect();
    if finite_returns.len() < 2 {
        return 0.0;
    }

    let mean = finite_returns.iter().sum::<f64>() / finite_returns.len() as f64;
    let var = finite_returns
        .iter()
        .map(|r| {
            let d = *r - mean;
            d * d
        })
        .sum::<f64>()
        / (finite_returns.len() as f64 - 1.0);

    if !var.is_finite() || var <= f64::EPSILON {
        return 0.0;
    }

    let score = mean / var.sqrt();
    if score.is_finite() {
        score
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::{pnl_bps, running_drawdown_pct, sharpe_like, total_pnl, unrealized_pnl};

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

    #[test]
    fn non_finite_inputs_return_zero() {
        approx_eq(unrealized_pnl(f64::NAN, 0.5, 1.0), 0.0);
        approx_eq(total_pnl(f64::INFINITY, 0.5, 0.6, 1.0), 0.0);
        approx_eq(pnl_bps(0.5, f64::NAN), 0.0);
    }

    #[test]
    fn sharpe_ignores_non_finite_returns() {
        let s = sharpe_like(&[0.1, f64::NAN, 0.2, f64::INFINITY]);
        assert!(s.is_finite());
        assert!(s > 0.0);
    }

    #[test]
    fn drawdown_ignores_non_finite_points() {
        let dd = running_drawdown_pct(&[100.0, f64::NAN, 80.0]);
        approx_eq(dd, 20.0);
    }
}

