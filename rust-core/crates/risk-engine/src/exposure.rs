pub fn net_exposure(long_qty: f64, short_qty: f64) -> f64 {
    long_qty - short_qty
}

pub fn gross_exposure(long_notional: f64, short_notional: f64) -> f64 {
    long_notional.abs() + short_notional.abs()
}

pub fn is_hedged(net_qty: f64, tolerance_qty: f64) -> bool {
    net_qty.abs() <= tolerance_qty.abs()
}

pub fn concentration_ratio(position_notional: f64, gross_exposure_notional: f64) -> f64 {
    if gross_exposure_notional <= 0.0 {
        return 0.0;
    }
    (position_notional.abs() / gross_exposure_notional.abs()).clamp(0.0, 1.0)
}

pub fn effective_leverage(gross_exposure_usd: f64, equity_usd: f64) -> f64 {
    if equity_usd <= 0.0 {
        return f64::INFINITY;
    }
    gross_exposure_usd / equity_usd
}

pub fn risk_parity_weight(volatility_bps: f64, floor_bps: f64) -> f64 {
    let v = volatility_bps.max(floor_bps).max(1e-6);
    1.0 / v
}

pub fn normalized_risk_weights(vols_bps: &[f64], floor_bps: f64) -> Vec<f64> {
    if vols_bps.is_empty() {
        return Vec::new();
    }

    let mut raw: Vec<f64> = vols_bps
        .iter()
        .map(|v| risk_parity_weight(*v, floor_bps))
        .collect();

    let sum: f64 = raw.iter().sum();
    if sum <= 0.0 || !sum.is_finite() {
        return vec![1.0 / vols_bps.len() as f64; vols_bps.len()];
    }

    for w in &mut raw {
        *w /= sum;
    }
    raw
}
