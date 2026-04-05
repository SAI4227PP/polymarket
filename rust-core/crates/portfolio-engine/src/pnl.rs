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
