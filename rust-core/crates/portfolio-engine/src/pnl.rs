pub fn unrealized_pnl(entry: f64, mark: f64, qty: f64) -> f64 {
    (mark - entry) * qty
}
