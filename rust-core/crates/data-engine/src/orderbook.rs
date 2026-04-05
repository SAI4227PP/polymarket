#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderBookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<OrderBookLevel> {
        self.bids
            .iter()
            .copied()
            .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal))
    }

    pub fn best_ask(&self) -> Option<OrderBookLevel> {
        self.asks
            .iter()
            .copied()
            .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal))
    }

    pub fn mid(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some((bid.price + ask.price) / 2.0)
    }

    pub fn spread_bps(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let mid = (bid.price + ask.price) / 2.0;
        if mid <= 0.0 {
            return None;
        }
        Some(((ask.price - bid.price) / mid) * 10_000.0)
    }
}
