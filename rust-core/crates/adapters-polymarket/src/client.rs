use anyhow::Result;
use common::Quote;

use crate::ws;

pub struct PolymarketClient;

impl PolymarketClient {
    pub fn new() -> Self {
        Self
    }

    pub async fn best_bid_ask_mid(&self, asset_id: &str) -> Result<Quote> {
        ws::stream_best_bid_ask(asset_id).await
    }
}
