#[derive(Debug, Clone)]
pub struct Quote {
    pub venue: String,
    pub symbol: String,
    pub price: f64,
    pub ts_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub edge_bps: f64,
    pub should_trade: bool,
}

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub allowed: bool,
    pub reason: String,
}
