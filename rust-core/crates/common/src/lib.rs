#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeDirection {
    BuyPolymarketSellBinance,
    SellPolymarketBuyBinance,
    Flat,
}

#[derive(Debug, Clone)]
pub struct Quote {
    pub venue: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub ts_ms: u64,
}

impl Quote {
    pub fn spread_bps(&self) -> f64 {
        if self.price <= 0.0 {
            return 0.0;
        }
        ((self.ask - self.bid) / self.price) * 10_000.0
    }
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub edge_bps: f64,
    pub net_edge_bps: f64,
    pub should_trade: bool,
    pub direction: TradeDirection,
}

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub allowed: bool,
    pub reason: String,
    pub violations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub pair: String,
    pub direction: TradeDirection,
    pub quantity: f64,
    pub limit_price: f64,
}

#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub accepted: bool,
    pub status: String,
    pub routed_path: String,
}
