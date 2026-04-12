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


