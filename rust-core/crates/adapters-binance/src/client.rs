use anyhow::Result;
use common::Quote;

use crate::ws;

pub struct BinanceClient;

impl BinanceClient {
    pub fn new() -> Self {
        Self
    }

    pub async fn best_bid_ask_mid(&self, symbol: &str) -> Result<Quote> {
        ws::stream_best_bid_ask(symbol).await
    }
}
