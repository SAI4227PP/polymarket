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

pub fn logit(probability: f64) -> f64 {
    let p = clamp_probability(probability).clamp(1e-9, 1.0 - 1e-9);
    (p / (1.0 - p)).ln()
}

pub fn inv_logit(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

pub fn implied_probability_from_yes_no(yes_price: f64, no_price: f64) -> f64 {
    let denom = yes_price + no_price;
    if denom <= 0.0 {
        return 0.5;
    }
    clamp_probability(yes_price / denom)
}
