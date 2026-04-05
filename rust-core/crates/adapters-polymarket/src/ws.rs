use anyhow::{anyhow, Context, Result};
use common::Quote;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const POLYMARKET_WS_MARKET_FRONTEND: &str =
    "wss://ws-subscriptions-frontend-clob.polymarket.com/ws/market";
const POLYMARKET_WS_MARKET_CLOB: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_WS_LIVE_DATA: &str = "wss://ws-live-data.polymarket.com/";

pub async fn stream_best_bid_ask(instrument_id: &str) -> Result<Quote> {
    let market_ws_url = std::env::var("POLYMARKET_MARKET_WS_URL")
        .unwrap_or_else(|_| POLYMARKET_WS_MARKET_FRONTEND.to_string());

    stream_market_quote_from_endpoint(instrument_id, &market_ws_url)
        .await
        .or_else(|_| stream_market_quote_from_endpoint(instrument_id, POLYMARKET_WS_MARKET_CLOB))
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

    while let Some(msg) = ws.next().await {
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

    Err(anyhow!("polymarket live-data websocket ended without quote"))
}

async fn stream_market_quote_from_endpoint(instrument_id: &str, endpoint: &str) -> Result<Quote> {
    let url = Url::parse(endpoint).context("invalid polymarket market websocket url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to polymarket market websocket {endpoint}"))?;

    let subscribe = if instrument_id.starts_with("0x") {
        json!({
            "markets": [instrument_id],
            "type": "market",
            "custom_feature_enabled": true
        })
    } else {
        json!({
            "assets_ids": [instrument_id],
            "type": "market",
            "custom_feature_enabled": true
        })
    };

    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to send polymarket market subscription")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("polymarket market websocket read error")?;
        match msg {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(&text).context("failed to parse polymarket market payload")?;

                if let Some(quote) = parse_quote_from_payload(&payload, instrument_id) {
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

    Err(anyhow!("polymarket market websocket ended without quote"))
}

fn parse_quote_from_payload(payload: &Value, instrument_id: &str) -> Option<Quote> {
    match payload {
        Value::Object(_) => parse_quote_from_event(payload, instrument_id),
        Value::Array(events) => events
            .iter()
            .find_map(|event| parse_quote_from_event(event, instrument_id)),
        _ => None,
    }
}

fn parse_quote_from_event(payload: &Value, instrument_id: &str) -> Option<Quote> {
    let event_type = payload.get("event_type")?.as_str()?;
    let by_market = instrument_id.starts_with("0x");

    let (symbol, bid, ask) = match event_type {
        "best_bid_ask" => {
            let event_market = payload.get("market").and_then(Value::as_str).unwrap_or_default();
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;
            if (by_market && event_market != instrument_id)
                || (!by_market && event_asset != instrument_id)
            {
                return None;
            }
            let bid = parse_price(payload.get("best_bid")?)?;
            let ask = parse_price(payload.get("best_ask")?)?;
            (event_asset.to_string(), bid, ask)
        }
        "book" => {
            let event_market = payload.get("market").and_then(Value::as_str).unwrap_or_default();
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;
            if (by_market && event_market != instrument_id)
                || (!by_market && event_asset != instrument_id)
            {
                return None;
            }
            let bids = payload.get("bids")?.as_array()?;
            let asks = payload.get("asks")?.as_array()?;
            let bid = bids.first()?.get("price").and_then(parse_price)?;
            let ask = asks.first()?.get("price").and_then(parse_price)?;
            (event_asset.to_string(), bid, ask)
        }
        "price_change" => parse_price_change_bid_ask(payload, instrument_id, by_market)?,
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
    instrument_id: &str,
    by_market: bool,
) -> Option<(String, f64, f64)> {
    if by_market {
        let event_market = payload.get("market").and_then(Value::as_str)?;
        if event_market != instrument_id {
            return None;
        }
    }

    let levels = payload.get("price_changes")?.as_array()?;

    for level in levels {
        let level_asset = level.get("asset_id").and_then(Value::as_str)?;
        if !by_market && level_asset != instrument_id {
            continue;
        }
        let bid = level.get("best_bid").and_then(parse_price)?;
        let ask = level.get("best_ask").and_then(parse_price)?;
        return Some((level_asset.to_string(), bid, ask));
    }
    None
}

fn parse_live_data_quote(payload: &Value) -> Option<Quote> {
    let topic = payload.get("topic").and_then(Value::as_str)?;
    if topic != "crypto_prices" {
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
