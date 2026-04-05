use anyhow::{Context, Result};
use common::Quote;
use tokio::time::{timeout, Duration};

use crate::{mapper, ws};

pub struct PolymarketClient {
    timeout_ms: u64,
}

impl PolymarketClient {
    pub fn new() -> Self {
        Self { timeout_ms: 6_000 }
    }

    pub fn with_timeout_ms(timeout_ms: u64) -> Self {
        Self { timeout_ms }
    }

    pub async fn best_bid_ask_mid(&self, asset_id: &str) -> Result<Quote> {
        let mapped = mapper::map_market_to_symbol(asset_id);
        timeout(
            Duration::from_millis(self.timeout_ms),
            ws::stream_best_bid_ask(&mapped),
        )
        .await
        .context("polymarket quote timeout")?
    }

    pub async fn btc_reference_price(&self) -> Result<Quote> {
        timeout(
            Duration::from_millis(self.timeout_ms),
            ws::stream_chainlink_btc_reference(),
        )
        .await
        .context("polymarket live-data timeout")?
    }
}
