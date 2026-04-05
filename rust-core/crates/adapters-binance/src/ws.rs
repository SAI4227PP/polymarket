use anyhow::{anyhow, Context, Result};
use common::Quote;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Debug, Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid: String,
    #[serde(rename = "a")]
    best_ask: String,
    #[serde(rename = "E")]
    event_time_ms: Option<u64>,
}

pub async fn stream_best_bid_ask(symbol: &str) -> Result<Quote> {
    let normalized = symbol.to_lowercase();
    let endpoint = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", normalized);
    let url = Url::parse(&endpoint).context("invalid binance websocket url")?;

    let (mut ws, _) = connect_async(url.as_str())
        .await
        .context("failed to connect to binance websocket")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("binance websocket read error")?;
        match msg {
            Message::Text(text) => {
                if let Some(quote) = parse_quote(&text)? {
                    return Ok(quote);
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong binance websocket")?;
            }
            Message::Close(_) => return Err(anyhow!("binance websocket closed")),
            _ => {}
        }
    }

    Err(anyhow!("binance websocket ended without quote"))
}

pub async fn run_quote_stream(symbol: &str, tx: watch::Sender<Option<Quote>>) -> Result<()> {
    let normalized = symbol.to_lowercase();
    let endpoint = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", normalized);
    let url = Url::parse(&endpoint).context("invalid binance websocket url")?;

    let (mut ws, _) = connect_async(url.as_str())
        .await
        .context("failed to connect to binance websocket")?;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("binance websocket read error")?;
        match msg {
            Message::Text(text) => {
                if let Some(quote) = parse_quote(&text)? {
                    let _ = tx.send(Some(quote));
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong binance websocket")?;
            }
            Message::Close(_) => return Err(anyhow!("binance websocket closed")),
            _ => {}
        }
    }

    Err(anyhow!("binance websocket ended"))
}

fn parse_quote(text: &str) -> Result<Option<Quote>> {
    let payload: BinanceBookTicker =
        serde_json::from_str(text).context("failed to parse binance message")?;

    let bid = payload
        .best_bid
        .parse::<f64>()
        .context("invalid binance best bid")?;
    let ask = payload
        .best_ask
        .parse::<f64>()
        .context("invalid binance best ask")?;

    if !(bid.is_finite() && ask.is_finite()) || ask < bid {
        return Ok(None);
    }

    Ok(Some(Quote {
        venue: "binance".to_string(),
        symbol: payload.symbol,
        bid,
        ask,
        price: (bid + ask) / 2.0,
        ts_ms: payload.event_time_ms.unwrap_or_else(now_ms),
    }))
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
