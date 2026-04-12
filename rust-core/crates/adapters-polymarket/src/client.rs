use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use common::Quote;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::time::{timeout, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{mapper, ws};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveOrderSide {
    Buy,
    Sell,
}

impl LiveOrderSide {
    fn as_str(self) -> &'static str {
        match self {
            LiveOrderSide::Buy => "buy",
            LiveOrderSide::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiveLimitOrderRequest {
    pub market_id: String,
    pub token_id: String,
    pub price: f64,
    pub size: f64,
    pub side: LiveOrderSide,
}

#[derive(Debug, Clone)]
pub struct LiveOrderReceipt {
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct LiveFillCriteria {
    pub market_id: String,
    pub token_id: String,
    pub side: LiveOrderSide,
    pub min_size: f64,
}

#[derive(Debug, Clone)]
pub struct LiveFillReceipt {
    pub status: String,
    pub matched_size: f64,
    pub ts_ms: u64,
}

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

    pub async fn place_limit_order_live(&self, req: LiveLimitOrderRequest) -> Result<LiveOrderReceipt> {
        use polymarket_client_sdk::auth::{LocalSigner, Signer as _};
        use polymarket_client_sdk::clob::types::Side;
        use polymarket_client_sdk::clob::{Client, Config};
        use polymarket_client_sdk::types::Decimal;
        use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};

        if req.market_id.trim().is_empty() {
            bail!("live order market_id is empty");
        }
        if req.token_id.trim().is_empty() {
            bail!("live order token_id is empty");
        }
        if !req.price.is_finite() || req.price <= 0.0 || req.price >= 1.0 {
            bail!("live order price must be within (0,1), got {}", req.price);
        }
        if !req.size.is_finite() || req.size <= 0.0 {
            bail!("live order size must be > 0, got {}", req.size);
        }

        let private_key = std::env::var(PRIVATE_KEY_VAR)
            .or_else(|_| std::env::var("POLYMARKET_PRIVATE_KEY"))
            .context("missing POLYMARKET_PRIVATE_KEY for live mode")?;

        let signer = LocalSigner::from_str(&private_key)
            .context("invalid POLYMARKET_PRIVATE_KEY")?
            .with_chain_id(Some(POLYGON));

        let clob_url = std::env::var("POLYMARKET_CLOB_API_URL")
            .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());

        let authed = timeout(
            Duration::from_millis(self.timeout_ms),
            async {
                Client::new(&clob_url, Config::default())?
                    .authentication_builder(&signer)
                    .authenticate()
                    .await
            },
        )
        .await
        .context("polymarket live auth timeout")?
        .context("polymarket clob authentication failed")?;

        let side = match req.side {
            LiveOrderSide::Buy => Side::Buy,
            LiveOrderSide::Sell => Side::Sell,
        };

        let size = Decimal::from_str(&format!("{:.6}", req.size))
            .context("invalid live size decimal")?;
        let price = Decimal::from_str(&format!("{:.6}", req.price.clamp(0.001, 0.999)))
            .context("invalid live price decimal")?;

        let order = timeout(
            Duration::from_millis(self.timeout_ms),
            async {
                authed
                    .limit_order()
                    .token_id(req.token_id.clone())
                    .size(size)
                    .price(price)
                    .side(side)
                    .build()
                    .await
            },
        )
        .await
        .context("polymarket live order build timeout")?
        .context("polymarket live order build failed")?;

        let signed_order = timeout(
            Duration::from_millis(self.timeout_ms),
            authed.sign(&signer, order),
        )
        .await
        .context("polymarket live sign timeout")?
        .context("polymarket live sign failed")?;

        let response = timeout(
            Duration::from_millis(self.timeout_ms),
            authed.post_order(signed_order),
        )
        .await
        .context("polymarket live post-order timeout")?
        .context("polymarket live post-order failed")?;

        Ok(LiveOrderReceipt {
            status: format!("{response:?}"),
        })
    }

    pub async fn wait_for_user_fill(
        &self,
        criteria: &LiveFillCriteria,
        wait_timeout_ms: u64,
    ) -> Result<Option<LiveFillReceipt>> {
        let api_key = std::env::var("POLYMARKET_API_KEY")
            .context("missing POLYMARKET_API_KEY for user websocket fill reconciliation")?;
        let api_secret = std::env::var("POLYMARKET_API_SECRET")
            .context("missing POLYMARKET_API_SECRET for user websocket fill reconciliation")?;
        let passphrase = std::env::var("POLYMARKET_PASSPHRASE")
            .context("missing POLYMARKET_PASSPHRASE for user websocket fill reconciliation")?;

        let ws_url = std::env::var("POLYMARKET_USER_WS_URL")
            .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string());
        let url = Url::parse(&ws_url).context("invalid POLYMARKET_USER_WS_URL")?;

        let (mut socket, _) = connect_async(url)
            .await
            .context("failed to connect to polymarket user websocket")?;

        let subscribe = json!({
            "auth": {
                "apiKey": api_key,
                "secret": api_secret,
                "passphrase": passphrase
            },
            "markets": [criteria.market_id.clone()],
            "type": "user"
        });
        socket
            .send(Message::Text(subscribe.to_string()))
            .await
            .context("failed to subscribe to polymarket user websocket")?;

        let deadline = Instant::now() + Duration::from_millis(wait_timeout_ms.max(100));
        let heartbeat_interval = Duration::from_secs(10);
        let mut next_heartbeat_at = Instant::now() + heartbeat_interval;

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let remaining = deadline.saturating_duration_since(now);
            let until_heartbeat = next_heartbeat_at.saturating_duration_since(now);
            let wait_window = remaining.min(until_heartbeat);
            let next = timeout(wait_window, socket.next()).await;
            let msg = match next {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => return Err(anyhow!("user websocket read error: {e}")),
                Ok(None) => return Ok(None),
                Err(_) => {
                    if Instant::now() >= next_heartbeat_at {
                        socket
                            .send(Message::Text("PING".to_string()))
                            .await
                            .context("failed to send user websocket heartbeat ping")?;
                        next_heartbeat_at = Instant::now() + heartbeat_interval;
                        continue;
                    }
                    return Ok(None);
                }
            };

            match msg {
                Message::Text(text) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        if let Some(fill) = parse_user_fill_message(&v, criteria) {
                            return Ok(Some(fill));
                        }
                    }
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .context("failed to pong user websocket")?;
                }
                Message::Close(_) => return Ok(None),
                _ => {}
            }

            if Instant::now() >= next_heartbeat_at {
                socket
                    .send(Message::Text("PING".to_string()))
                    .await
                    .context("failed to send user websocket heartbeat ping")?;
                next_heartbeat_at = Instant::now() + heartbeat_interval;
            }
        }
    }
}

fn parse_user_fill_message(v: &Value, criteria: &LiveFillCriteria) -> Option<LiveFillReceipt> {
    if let Some(fill) = parse_user_fill_object(v, criteria) {
        return Some(fill);
    }

    match v {
        Value::Array(items) => items.iter().find_map(|x| parse_user_fill_message(x, criteria)),
        Value::Object(map) => map.values().find_map(|x| parse_user_fill_message(x, criteria)),
        _ => None,
    }
}

fn parse_user_fill_object(v: &Value, criteria: &LiveFillCriteria) -> Option<LiveFillReceipt> {
    let obj = v.as_object()?;

    let is_trade_event = extract_any_string(obj, &["event_type", "type"])
        .map(|t| {
            let up = t.to_ascii_uppercase();
            up == "TRADE"
        })
        .unwrap_or(false);
    if !is_trade_event {
        return None;
    }

    let market_ok = extract_any_string(obj, &["market", "market_id", "condition_id"])
        .map(|m| m == criteria.market_id)
        .unwrap_or(false);

    let maker_order_matched_amount = extract_matching_maker_order_matched_amount(obj, &criteria.token_id);
    let mut token_ok = extract_any_string(
        obj,
        &["token_id", "asset_id", "maker_asset_id", "taker_asset_id"],
    )
    .map(|t| t == criteria.token_id)
    .unwrap_or(false);
    if !token_ok {
        token_ok = maker_order_matched_amount.is_some();
    }

    let side_ok = if maker_order_matched_amount.is_some() {
        // For maker fills, event side can represent taker direction.
        true
    } else {
        extract_any_string(obj, &["side", "taker_side", "maker_side"])
            .map(|s| s.eq_ignore_ascii_case(criteria.side.as_str()))
            .unwrap_or(false)
    };

    let status = extract_any_string(obj, &["status", "state", "trade_status"])
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let status_up = status.to_ascii_uppercase();
    let status_ok = matches!(status_up.as_str(), "MATCHED" | "MINED" | "CONFIRMED");
    if !status_ok {
        return None;
    }

    let matched_size = extract_any_f64(
        obj,
        &["size", "matched_size", "filled_size", "last_size", "maker_amount", "taker_amount"],
    )
    .or(maker_order_matched_amount)
    .unwrap_or(0.0);

    if market_ok && token_ok && side_ok && matched_size > 0.0 && matched_size >= criteria.min_size.max(0.0) {
        return Some(LiveFillReceipt {
            status,
            matched_size,
            ts_ms: extract_any_u64(obj, &["matchtime", "timestamp", "last_update", "ts", "ts_ms"])
                .unwrap_or_else(now_ms),
        });
    }

    None
}

fn extract_matching_maker_order_matched_amount(
    map: &serde_json::Map<String, Value>,
    token_id: &str,
) -> Option<f64> {
    let maker_orders = map.get("maker_orders")?.as_array()?;
    for item in maker_orders {
        let maker = item.as_object()?;
        let asset_matches = maker
            .get("asset_id")
            .and_then(|v| v.as_str())
            .map(|v| v == token_id)
            .unwrap_or(false);
        if !asset_matches {
            continue;
        }
        if let Some(amount) = extract_any_f64(maker, &["matched_amount", "size", "matched_size"]) {
            return Some(amount);
        }
    }
    None
}

fn extract_any_string(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(v) = map.get(*k) {
            match v {
                Value::String(s) if !s.trim().is_empty() => return Some(s.trim().to_string()),
                Value::Number(n) => return Some(n.to_string()),
                _ => {}
            }
        }
    }
    None
}

fn extract_any_f64(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(v) = map.get(*k) {
            match v {
                Value::Number(n) => {
                    if let Some(x) = n.as_f64() {
                        return Some(x);
                    }
                }
                Value::String(s) => {
                    if let Ok(x) = s.parse::<f64>() {
                        return Some(x);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn extract_any_u64(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<u64> {
    for k in keys {
        if let Some(v) = map.get(*k) {
            match v {
                Value::Number(n) => {
                    if let Some(x) = n.as_u64() {
                        return Some(x);
                    }
                }
                Value::String(s) => {
                    if let Ok(x) = s.parse::<u64>() {
                        return Some(x);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
