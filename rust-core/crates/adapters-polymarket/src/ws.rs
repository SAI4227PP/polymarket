use anyhow::{anyhow, Context, Result};
use common::Quote;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const POLYMARKET_WS_MARKET: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

pub async fn stream_best_bid_ask(asset_id: &str) -> Result<Quote> {
    let url = Url::parse(POLYMARKET_WS_MARKET).context("invalid polymarket websocket url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .context("failed to connect to polymarket websocket")?;

    let subscribe = json!({
        "assets_ids": [asset_id],
        "type": "market",
        "custom_feature_enabled": true
    });

    ws.send(Message::Text(subscribe.to_string()))
        .await
        .context("failed to send polymarket subscription")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("polymarket websocket read error")?;
        match msg {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(&text).context("failed to parse polymarket message")?;

                if let Some(quote) = parse_quote_from_event(&payload, asset_id) {
                    return Ok(quote);
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong polymarket websocket")?;
            }
            Message::Close(_) => return Err(anyhow!("polymarket websocket closed")),
            _ => {}
        }
    }

    Err(anyhow!("polymarket websocket ended without quote"))
}

fn parse_quote_from_event(payload: &Value, asset_id: &str) -> Option<Quote> {
    let event_type = payload.get("event_type")?.as_str()?;

    let (bid, ask) = match event_type {
        "best_bid_ask" => {
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;
            if event_asset != asset_id {
                return None;
            }
            let bid = parse_price_str(payload.get("best_bid")?)?;
            let ask = parse_price_str(payload.get("best_ask")?)?;
            (bid, ask)
        }
        "book" => {
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;
            if event_asset != asset_id {
                return None;
            }
            let bids = payload.get("bids")?.as_array()?;
            let asks = payload.get("asks")?.as_array()?;
            let bid = bids
                .first()?
                .get("price")
                .and_then(parse_price_str)?;
            let ask = asks
                .first()?
                .get("price")
                .and_then(parse_price_str)?;
            (bid, ask)
        }
        _ => return None,
    };

    Some(Quote {
        venue: "polymarket".to_string(),
        symbol: asset_id.to_string(),
        price: (bid + ask) / 2.0,
        ts_ms: parse_timestamp_ms(payload.get("timestamp")),
    })
}

fn parse_price_str(v: &Value) -> Option<f64> {
    v.as_str()?.parse::<f64>().ok()
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
