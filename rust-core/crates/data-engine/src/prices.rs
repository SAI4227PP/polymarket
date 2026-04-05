use common::Quote;

#[derive(Debug, Clone)]
pub struct PriceSnapshot {
    pub symbol: String,
    pub venue: String,
    pub mid: f64,
    pub spread_bps: f64,
    pub ts_ms: u64,
}

pub fn mid_price(q: &Quote) -> f64 {
    (q.bid + q.ask) / 2.0
}

pub fn build_snapshot(q: &Quote) -> PriceSnapshot {
    PriceSnapshot {
        symbol: q.symbol.clone(),
        venue: q.venue.clone(),
        mid: mid_price(q),
        spread_bps: q.spread_bps(),
        ts_ms: q.ts_ms,
    }
}

pub fn stale_by_ms(now_ms: u64, quote_ts_ms: u64, max_age_ms: u64) -> bool {
    now_ms.saturating_sub(quote_ts_ms) > max_age_ms
}

pub fn ewma(prev: f64, x: f64, alpha: f64) -> f64 {
    let a = alpha.clamp(0.0, 1.0);
    (a * x) + ((1.0 - a) * prev)
}

pub fn realized_volatility_bps(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().copied().sum::<f64>() / returns.len() as f64;
    let var = returns
        .iter()
        .map(|r| {
            let d = *r - mean;
            d * d
        })
        .sum::<f64>()
        / (returns.len() as f64 - 1.0);
    var.sqrt() * 10_000.0
}

pub fn zscore(last: f64, mean: f64, std: f64) -> f64 {
    if std <= f64::EPSILON {
        return 0.0;
    }
    (last - mean) / std
}
