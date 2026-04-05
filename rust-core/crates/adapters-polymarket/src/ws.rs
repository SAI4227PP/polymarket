use anyhow::{anyhow, bail, Context, Result};
use common::Quote;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::watch;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const POLYMARKET_WS_MARKET_FRONTEND: &str =
    "wss://ws-subscriptions-frontend-clob.polymarket.com/ws/market";
const POLYMARKET_WS_MARKET_CLOB: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_WS_LIVE_DATA: &str = "wss://ws-live-data.polymarket.com/";
const WS_HEARTBEAT_SECONDS: u64 = 15;

#[derive(Debug, Clone)]
enum InstrumentMode {
    Asset,
    Market,
}

#[derive(Debug, Clone)]
struct MarketSubscription {
    mode: InstrumentMode,
    market_id: Option<String>,
    asset_ids: Vec<String>,
}

impl MarketSubscription {
    fn from_instrument_id(instrument_id: &str) -> Result<Self> {
        if instrument_id.starts_with("0x") {
            let asset_ids = resolve_market_asset_ids()
                .context("missing POLYMARKET_OUTCOME_ASSET_IDS for market-id subscription")?;
            if asset_ids.is_empty() {
                bail!("POLYMARKET_OUTCOME_ASSET_IDS must include at least one asset id");
            }
            return Ok(Self {
                mode: InstrumentMode::Market,
                market_id: Some(instrument_id.to_string()),
                asset_ids,
            });
        }

        Ok(Self {
            mode: InstrumentMode::Asset,
            market_id: None,
            asset_ids: vec![instrument_id.to_string()],
        })
    }

    fn subscribe_payload(&self) -> Value {
        json!({
            "assets_ids": self.asset_ids,
            "type": "market",
            "custom_feature_enabled": true
        })
    }

    fn accepts_asset(&self, asset_id: &str, preferred_asset: Option<&str>) -> bool {
        if let Some(preferred) = preferred_asset {
            return asset_id == preferred;
        }
        self.asset_ids.iter().any(|a| a == asset_id)
    }

    fn accepts_market(&self, market_id: Option<&str>) -> bool {
        match (&self.mode, &self.market_id) {
            (InstrumentMode::Asset, _) => true,
            (InstrumentMode::Market, Some(expected)) => market_id == Some(expected.as_str()),
            _ => false,
        }
    }
}

pub async fn stream_best_bid_ask(instrument_id: &str) -> Result<Quote> {
    let market_ws_url = std::env::var("POLYMARKET_MARKET_WS_URL")
        .unwrap_or_else(|_| POLYMARKET_WS_MARKET_CLOB.to_string());

    match stream_market_quote_from_endpoint(instrument_id, &market_ws_url).await {
        Ok(q) => Ok(q),
        Err(_) => stream_market_quote_from_endpoint(instrument_id, POLYMARKET_WS_MARKET_FRONTEND).await,
    }
}

pub async fn run_market_quote_stream(
    instrument_id: &str,
    tx: watch::Sender<Option<Quote>>,
) -> Result<()> {
    let market_ws_url = std::env::var("POLYMARKET_MARKET_WS_URL")
        .unwrap_or_else(|_| POLYMARKET_WS_MARKET_CLOB.to_string());

    match run_market_quote_stream_from_endpoint(instrument_id, &market_ws_url, tx.clone()).await {
        Ok(()) => Ok(()),
        Err(_) => {
            run_market_quote_stream_from_endpoint(instrument_id, POLYMARKET_WS_MARKET_FRONTEND, tx).await
        }
    }
}

pub async fn stream_chainlink_btc_reference() -> Result<Quote> {
    let url = Url::parse(POLYMARKET_WS_LIVE_DATA).context("invalid polymarket live-data ws url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .context("failed to connect to polymarket live-data websocket")?;

    let subscribe = json!({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "filters": "{\"symbol\":\"btc/usd\"}"
            }
        ]
    });

    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to subscribe to polymarket live-data")?;

    loop {
        let next = timeout(Duration::from_secs(WS_HEARTBEAT_SECONDS), ws.next()).await;

        let Some(msg) = (match next {
            Ok(v) => v,
            Err(_) => {
                ws.send(Message::Ping(Vec::new().into()))
                    .await
                    .context("failed to send live-data keepalive ping")?;
                continue;
            }
        }) else {
            return Err(anyhow!("polymarket live-data websocket ended without quote"));
        };

        let msg = msg.context("polymarket live-data read error")?;
        match msg {
            Message::Text(text) => {
                let payload: Value = serde_json::from_str(&text)
                    .context("failed to parse polymarket live-data message")?;
                if let Some(q) = parse_live_data_quote(&payload) {
                    return Ok(q);
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong polymarket live-data websocket")?;
            }
            Message::Close(_) => return Err(anyhow!("polymarket live-data websocket closed")),
            _ => {}
        }
    }
}

async fn stream_market_quote_from_endpoint(instrument_id: &str, endpoint: &str) -> Result<Quote> {
    let subscription = MarketSubscription::from_instrument_id(instrument_id)?;

    let url = Url::parse(endpoint).context("invalid polymarket market websocket url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to polymarket market websocket {endpoint}"))?;

    let subscribe = subscription.subscribe_payload();
    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to send polymarket market subscription")?;

    let mut preferred_asset: Option<String> = None;

    loop {
        let next = timeout(Duration::from_secs(WS_HEARTBEAT_SECONDS), ws.next()).await;

        let Some(msg) = (match next {
            Ok(v) => v,
            Err(_) => {
                ws.send(Message::Ping(Vec::new().into()))
                    .await
                    .context("failed to send market keepalive ping")?;
                continue;
            }
        }) else {
            return Err(anyhow!("polymarket market websocket ended without quote"));
        };

        let msg = msg.context("polymarket market websocket read error")?;
        match msg {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(&text).context("failed to parse polymarket market payload")?;

                if let Some(quote) = parse_quote_from_payload(
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                ) {
                    if preferred_asset.is_none() {
                        preferred_asset = Some(quote.symbol.clone());
                    }
                    return Ok(quote);
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong polymarket market websocket")?;
            }
            Message::Close(_) => return Err(anyhow!("polymarket market websocket closed")),
            _ => {}
        }
    }
}

async fn run_market_quote_stream_from_endpoint(
    instrument_id: &str,
    endpoint: &str,
    tx: watch::Sender<Option<Quote>>,
) -> Result<()> {
    let subscription = MarketSubscription::from_instrument_id(instrument_id)?;

    let url = Url::parse(endpoint).context("invalid polymarket market websocket url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to polymarket market websocket {endpoint}"))?;

    let subscribe = subscription.subscribe_payload();
    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to send polymarket market subscription")?;

    let mut preferred_asset: Option<String> = None;

    loop {
        let next = timeout(Duration::from_secs(WS_HEARTBEAT_SECONDS), ws.next()).await;

        let Some(msg) = (match next {
            Ok(v) => v,
            Err(_) => {
                ws.send(Message::Ping(Vec::new().into()))
                    .await
                    .context("failed to send market keepalive ping")?;
                continue;
            }
        }) else {
            return Err(anyhow!("polymarket market websocket ended"));
        };

        let msg = msg.context("polymarket market websocket read error")?;
        match msg {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(&text).context("failed to parse polymarket market payload")?;

                if let Some(quote) = parse_quote_from_payload(
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                ) {
                    if preferred_asset.is_none() {
                        preferred_asset = Some(quote.symbol.clone());
                    }
                    let _ = tx.send(Some(quote));
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong polymarket market websocket")?;
            }
            Message::Close(_) => return Err(anyhow!("polymarket market websocket closed")),
            _ => {}
        }
    }
}

fn resolve_market_asset_ids() -> Result<Vec<String>> {
    let raw = std::env::var("POLYMARKET_OUTCOME_ASSET_IDS")
        .context("POLYMARKET_OUTCOME_ASSET_IDS is required when using POLYMARKET_MARKET_ID")?;

    let mut out = Vec::new();
    for token in raw.split(',') {
        let v = token.trim();
        if !v.is_empty() && !out.iter().any(|existing| existing == v) {
            out.push(v.to_string());
        }
    }

    Ok(out)
}

fn parse_quote_from_payload(
    payload: &Value,
    subscription: &MarketSubscription,
    preferred_asset: Option<&str>,
) -> Option<Quote> {
    match payload {
        Value::Object(_) => parse_quote_from_event(payload, subscription, preferred_asset),
        Value::Array(events) => events
            .iter()
            .find_map(|event| parse_quote_from_event(event, subscription, preferred_asset)),
        _ => None,
    }
}

fn parse_quote_from_event(
    payload: &Value,
    subscription: &MarketSubscription,
    preferred_asset: Option<&str>,
) -> Option<Quote> {
    let event_type = payload.get("event_type")?.as_str()?;

    let (symbol, bid, ask) = match event_type {
        "best_bid_ask" => {
            let event_market = payload.get("market").and_then(Value::as_str);
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;

            if !subscription.accepts_market(event_market)
                || !subscription.accepts_asset(event_asset, preferred_asset)
            {
                return None;
            }

            let bid = parse_price(payload.get("best_bid")?)?;
            let ask = parse_price(payload.get("best_ask")?)?;
            (event_asset.to_string(), bid, ask)
        }
        "book" => {
            let event_market = payload.get("market").and_then(Value::as_str);
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;

            if !subscription.accepts_market(event_market)
                || !subscription.accepts_asset(event_asset, preferred_asset)
            {
                return None;
            }

            let bids = payload.get("bids")?.as_array()?;
            let asks = payload.get("asks")?.as_array()?;
            let bid = bids.first()?.get("price").and_then(parse_price)?;
            let ask = asks.first()?.get("price").and_then(parse_price)?;
            (event_asset.to_string(), bid, ask)
        }
        "price_change" => parse_price_change_bid_ask(payload, subscription, preferred_asset)?,
        _ => return None,
    };

    if !(bid.is_finite() && ask.is_finite()) || ask < bid {
        return None;
    }

    Some(Quote {
        venue: "polymarket".to_string(),
        symbol,
        bid,
        ask,
        price: (bid + ask) / 2.0,
        ts_ms: parse_timestamp_ms(payload.get("timestamp")),
    })
}

fn parse_price_change_bid_ask(
    payload: &Value,
    subscription: &MarketSubscription,
    preferred_asset: Option<&str>,
) -> Option<(String, f64, f64)> {
    let event_market = payload.get("market").and_then(Value::as_str);
    if !subscription.accepts_market(event_market) {
        return None;
    }

    let levels = payload.get("price_changes")?.as_array()?;

    let mut candidates: Vec<(String, f64, f64)> = levels
        .iter()
        .filter_map(|level| {
            let level_asset = level.get("asset_id").and_then(Value::as_str)?;
            if !subscription.asset_ids.iter().any(|a| a == level_asset) {
                return None;
            }
            let bid = level.get("best_bid").and_then(parse_price)?;
            let ask = level.get("best_ask").and_then(parse_price)?;
            Some((level_asset.to_string(), bid, ask))
        })
        .collect();

    if candidates.is_empty() {
        return None;
    }

    if let Some(preferred) = preferred_asset {
        if let Some(found) = candidates
            .into_iter()
            .find(|(asset, _, _)| asset.as_str() == preferred)
        {
            return Some(found);
        }
        return None;
    }

    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    candidates.into_iter().next()
}

fn parse_live_data_quote(payload: &Value) -> Option<Quote> {
    let topic = payload.get("topic").and_then(Value::as_str)?;
    if topic != "crypto_prices" && topic != "crypto_prices_chainlink" {
        return None;
    }

    let data = payload.get("payload")?.get("data")?.as_array()?;
    let symbol = payload
        .get("payload")
        .and_then(|p| p.get("symbol"))
        .and_then(Value::as_str)
        .unwrap_or("btc/usd");

    let latest = data.last()?;
    let price = latest.get("value")?.as_f64()?;
    let ts_ms = latest
        .get("timestamp")
        .and_then(Value::as_i64)
        .map(|v| v as u64)
        .unwrap_or_else(now_ms);

    Some(Quote {
        venue: "polymarket-live-data".to_string(),
        symbol: symbol.to_string(),
        bid: price,
        ask: price,
        price,
        ts_ms,
    })
}

fn parse_price(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn parse_timestamp_ms(v: Option<&Value>) -> u64 {
    match v {
        Some(Value::String(s)) => s.parse::<u64>().unwrap_or_else(|_| now_ms()),
        Some(Value::Number(n)) => n.as_u64().unwrap_or_else(now_ms),
        _ => now_ms(),
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
