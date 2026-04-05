pub fn fair_value_from_binance(binance_price: f64) -> f64 {
    (binance_price / 100000.0).clamp(0.0, 1.0)
}
