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
