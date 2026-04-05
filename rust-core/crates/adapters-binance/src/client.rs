use anyhow::{Context, Result};
use common::Quote;
use tokio::time::{timeout, Duration};

use crate::{mapper, ws};

pub struct BinanceClient {
    timeout_ms: u64,
}

impl BinanceClient {
    pub fn new() -> Self {
        Self { timeout_ms: 4_000 }
    }

    pub fn with_timeout_ms(timeout_ms: u64) -> Self {
        Self { timeout_ms }
    }

    pub async fn best_bid_ask_mid(&self, symbol: &str) -> Result<Quote> {
        let normalized = mapper::normalize_symbol(symbol);
        timeout(
            Duration::from_millis(self.timeout_ms),
            ws::stream_best_bid_ask(&normalized),
        )
        .await
        .context("binance quote timeout")?
    }
}
