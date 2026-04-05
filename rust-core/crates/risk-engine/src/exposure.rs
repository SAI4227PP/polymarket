pub fn net_exposure(long_qty: f64, short_qty: f64) -> f64 {
    long_qty - short_qty
}

pub fn gross_exposure(long_notional: f64, short_notional: f64) -> f64 {
    long_notional.abs() + short_notional.abs()
}

pub fn is_hedged(net_qty: f64, tolerance_qty: f64) -> bool {
    net_qty.abs() <= tolerance_qty.abs()
}
