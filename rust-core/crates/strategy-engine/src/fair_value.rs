use common::Quote;

pub fn fair_value_from_binance(binance_price: f64, anchor_price: f64) -> f64 {
    if anchor_price <= 0.0 || !binance_price.is_finite() || !anchor_price.is_finite() {
        return 0.5;
    }

    // For 5m up/down markets, "fair" probability should be centered near 0.5
    // when current price ~= window start, and move smoothly with relative return.
    let move_bps = ((binance_price - anchor_price) / anchor_price) * 10_000.0;
    let scaled = (move_bps / 12.0).clamp(-8.0, 8.0);
    (0.5 + 0.5 * scaled.tanh()).clamp(0.0, 1.0)
}

pub fn fair_value_with_basis(binance_quote: &Quote, basis_bps: f64, anchor_price: f64) -> f64 {
    let raw = fair_value_from_binance(binance_quote.price, anchor_price);
    let adjusted = raw + (basis_bps / 10_000.0);
    adjusted.clamp(0.0, 1.0)
}

pub fn blended_fair_value(
    model_value: f64,
    market_value: f64,
    model_weight: f64,
    microstructure_bias_bps: f64,
) -> f64 {
    let w = model_weight.clamp(0.0, 1.0);
    let blended = (w * model_value) + ((1.0 - w) * market_value);
    let biased = blended * (1.0 + microstructure_bias_bps / 10_000.0);
    biased.clamp(0.0, 1.0)
}
