pub fn apply_slippage(price: f64, slippage_bps: f64) -> f64 {
    price * (1.0 + slippage_bps / 10_000.0)
}
