pub fn to_bps(decimal_return: f64) -> f64 {
    decimal_return * 10_000.0
}

pub fn probability_to_decimal_odds(probability: f64) -> Option<f64> {
    if !(0.0..=1.0).contains(&probability) || probability == 0.0 {
        return None;
    }
    Some(1.0 / probability)
}

pub fn clamp_probability(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

pub fn binance_price_to_prob(binance_price: f64, anchor_price: f64) -> f64 {
    if anchor_price <= 0.0 {
        return 0.5;
    }
    clamp_probability(binance_price / anchor_price)
}
