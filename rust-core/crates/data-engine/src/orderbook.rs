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

    pub fn microprice(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let denom = bid.size + ask.size;
        if denom <= 0.0 {
            return self.mid();
        }
        Some((ask.price * bid.size + bid.price * ask.size) / denom)
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

    pub fn top_n_depth(&self, n: usize) -> (f64, f64) {
        let bid_depth = self
            .bids
            .iter()
            .take(n)
            .map(|l| l.size.max(0.0))
            .sum::<f64>();
        let ask_depth = self
            .asks
            .iter()
            .take(n)
            .map(|l| l.size.max(0.0))
            .sum::<f64>();
        (bid_depth, ask_depth)
    }

    pub fn imbalance(&self, n: usize) -> Option<f64> {
        let (bid_depth, ask_depth) = self.top_n_depth(n);
        let total = bid_depth + ask_depth;
        if total <= 0.0 {
            return None;
        }
        Some((bid_depth - ask_depth) / total)
    }

    pub fn depth_weighted_mid(&self, n: usize) -> Option<f64> {
        let bid_levels = self.bids.iter().take(n);
        let ask_levels = self.asks.iter().take(n);

        let mut notional = 0.0;
        let mut qty = 0.0;

        for l in bid_levels.chain(ask_levels) {
            let s = l.size.max(0.0);
            notional += l.price * s;
            qty += s;
        }

        if qty <= 0.0 {
            return self.mid();
        }
        Some(notional / qty)
    }
}
