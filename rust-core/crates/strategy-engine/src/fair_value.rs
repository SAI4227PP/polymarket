use common::Quote;

pub fn fair_value_from_binance(binance_price: f64, anchor_price: f64) -> f64 {
    if anchor_price <= 0.0 {
        return 0.5;
    }
    (binance_price / anchor_price).clamp(0.0, 1.0)
}

pub fn fair_value_with_basis(binance_quote: &Quote, basis_bps: f64, anchor_price: f64) -> f64 {
    let raw = fair_value_from_binance(binance_quote.price, anchor_price);
    let adjusted = raw * (1.0 + basis_bps / 10_000.0);
    adjusted.clamp(0.0, 1.0)
}
