use common::Quote;

const EPS: f64 = 1e-12;

pub fn fair_value_from_binance(binance_price: f64, anchor_price: f64) -> f64 {
    fair_value_advanced(binance_price, anchor_price, 0.0, 40.0, 0, 300_000)
}

pub fn fair_value_with_basis(binance_quote: &Quote, basis_bps: f64, anchor_price: f64) -> f64 {
    fair_value_advanced(
        binance_quote.price,
        anchor_price,
        basis_bps,
        40.0,
        0,
        300_000,
    )
}

pub fn fair_value_advanced(
    binance_price: f64,
    anchor_price: f64,
    basis_bps: f64,
    realized_vol_bps: f64,
    elapsed_ms: u64,
    horizon_ms: u64,
) -> f64 {
    if anchor_price <= 0.0 || !binance_price.is_finite() || !anchor_price.is_finite() {
        return 0.5;
    }

    let move_bps = ((binance_price - anchor_price) / anchor_price) * 10_000.0;
    let vol_bps = realized_vol_bps.abs().max(10.0);
    let horizon = horizon_ms.max(1) as f64;
    let t = ((elapsed_ms as f64) / horizon).clamp(0.05, 1.0);
    let expected_sigma_bps = (vol_bps * t.sqrt()).max(5.0);

    // Translate relative move into a smooth directional probability around 0.5.
    let z = (move_bps / (expected_sigma_bps + EPS)).clamp(-8.0, 8.0);
    let directional = 0.5 + 0.5 * (z / 1.35).tanh();

    let basis_adjustment = basis_bps / 10_000.0;
    (directional + basis_adjustment).clamp(0.0, 1.0)
}

pub fn dynamic_blend_weight(
    base_model_weight: f64,
    divergence_bps: f64,
    max_divergence_bps: f64,
    liquidity_score: f64,
) -> f64 {
    let base = base_model_weight.clamp(0.0, 1.0);
    let liq = liquidity_score.clamp(0.0, 1.0);
    let max_div = max_divergence_bps.max(1.0);
    let divergence_ratio = (divergence_bps.abs() / max_div).clamp(0.0, 2.0);

    // Penalize model weight when model/market diverge too much, but keep
    // more model influence when market liquidity is thin.
    let divergence_penalty = (divergence_ratio * 0.45).min(0.65);
    let liquidity_support = (1.0 - liq) * 0.20;

    (base - divergence_penalty + liquidity_support).clamp(0.05, 0.95)
}

pub fn blended_fair_value(
    model_value: f64,
    market_value: f64,
    model_weight: f64,
    microstructure_bias_bps: f64,
) -> f64 {
    let w = model_weight.clamp(0.0, 1.0);
    let blended = (w * model_value) + ((1.0 - w) * market_value);
    let biased = blended + (microstructure_bias_bps / 10_000.0);
    biased.clamp(0.0, 1.0)
}
