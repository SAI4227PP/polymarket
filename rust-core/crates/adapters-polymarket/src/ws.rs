use anyhow::{anyhow, bail, Context, Result};
use chrono::{Datelike, Utc};
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
    async fn from_instrument_id(instrument_id: &str) -> Result<Self> {
        if instrument_id.starts_with("0x") {
            let asset_ids = resolve_market_asset_ids(instrument_id)
                .await
                .context("failed resolving market asset ids for market-id subscription")?;
            if asset_ids.is_empty() {
                bail!("resolved market asset ids are empty");
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

fn first_non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub async fn resolve_instrument_id_from_env() -> Result<String> {
    if let Some(asset_id) = first_non_empty_env("POLYMARKET_ASSET_ID") {
        return Ok(asset_id);
    }
    if let Some(market_id) = first_non_empty_env("POLYMARKET_MARKET_ID") {
        return Ok(market_id);
    }

    if let Some(event_url) = first_non_empty_env("POLYMARKET_EVENT_URL") {
        let parsed = Url::parse(&event_url).context("invalid POLYMARKET_EVENT_URL")?;
        let parts: Vec<&str> = parsed
            .path_segments()
            .map(|s| s.collect())
            .unwrap_or_default();

        let event_idx = parts.iter().position(|p| *p == "event");
        if let Some(idx) = event_idx {
            if idx + 1 < parts.len() {
                let event_slug = parts[idx + 1];
                let preferred_market_slug = if idx + 2 < parts.len() {
                    let s = parts[idx + 2].trim();
                    if s.is_empty() { None } else { Some(s) }
                } else {
                    None
                };
                return resolve_market_id_from_event_slug(event_slug, preferred_market_slug).await;
            }
        }
        bail!("POLYMARKET_EVENT_URL must look like /event/{{event-slug}}[/market-slug]");
    }

    if let Some(event_slug) = first_non_empty_env("POLYMARKET_EVENT_SLUG") {
        return resolve_market_id_from_event_slug(&event_slug, None).await;
    }

    let auto_event_slug = auto_today_event_slug();
    resolve_market_id_from_event_slug(&auto_event_slug, None)
        .await
        .with_context(|| format!("auto-resolve failed for event slug {auto_event_slug}"))
}

async fn resolve_market_id_from_event_slug(
    event_slug: &str,
    preferred_market_slug: Option<&str>,
) -> Result<String> {
    match resolve_market_id_from_gamma_event(event_slug, preferred_market_slug).await {
        Ok(id) => Ok(id),
        Err(gamma_err) => resolve_market_id_from_activity_ws(event_slug)
            .await
            .with_context(|| {
                format!(
                    "gamma lookup failed ({gamma_err:#}); activity websocket fallback failed for event_slug={event_slug}"
                )
            }),
    }
}

async fn resolve_market_id_from_gamma_event(event_slug: &str, preferred_market_slug: Option<&str>) -> Result<String> {
    let base = std::env::var("POLYMARKET_GAMMA_API_URL")
        .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());
    let base = base.trim_end_matches('/');
    let url = format!("{base}/events/slug/{event_slug}");

    let payload: Value = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("gamma event request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("gamma event request non-success: {url}"))?
        .json()
        .await
        .with_context(|| format!("gamma event decode failed: {url}"))?;

    let markets = payload
        .get("markets")
        .and_then(Value::as_array)
        .context("gamma event payload missing markets")?;

    let pick = if let Some(mslug) = preferred_market_slug {
        markets.iter().find(|m| m.get("slug").and_then(Value::as_str) == Some(mslug))
    } else {
        markets
            .iter()
            .filter_map(|m| {
                let active = m.get("active").and_then(Value::as_bool).unwrap_or(false);
                let closed = m.get("closed").and_then(Value::as_bool).unwrap_or(false);
                let vol = m.get("volumeNum").and_then(Value::as_f64).or_else(|| {
                    m.get("volume")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse::<f64>().ok())
                }).unwrap_or(0.0);
                Some((m, if active { 2 } else { 0 } + if !closed { 1 } else { 0 }, vol))
            })
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal)))
            .map(|(m,_,_)| m)
    };

    let market = pick.context("no market found for event slug")?;

    if let Some(cid) = market.get("conditionId").and_then(Value::as_str) {
        let cid = cid.trim();
        if cid.starts_with("0x") && !cid.is_empty() {
            return Ok(cid.to_string());
        }
    }

    if let Some(mid) = market.get("id").and_then(Value::as_str) {
        let mid = mid.trim();
        if mid.starts_with("0x") && !mid.is_empty() {
            return Ok(mid.to_string());
        }
    }

    bail!("resolved market is missing a usable 0x market identifier");
}


async fn resolve_market_id_from_activity_ws(event_slug: &str) -> Result<String> {
    let url = Url::parse(POLYMARKET_WS_LIVE_DATA).context("invalid polymarket live-data ws url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .context("failed to connect to polymarket live-data websocket")?;

    let filters = serde_json::to_string(&json!({ "event_slug": event_slug }))
        .context("failed to encode activity filters")?;
    let subscribe = json!({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "activity",
                "type": "orders_matched",
                "filters": filters
            }
        ]
    });

    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to subscribe activity websocket")?;

    let deadline = Duration::from_secs(25);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > deadline {
            bail!("timed out waiting for activity stream conditionId");
        }

        let next = timeout(Duration::from_secs(WS_HEARTBEAT_SECONDS), ws.next()).await;
        let Some(msg) = (match next {
            Ok(v) => v,
            Err(_) => {
                ws.send(Message::Ping(Vec::new().into()))
                    .await
                    .context("failed to send activity keepalive ping")?;
                continue;
            }
        }) else {
            bail!("activity websocket ended before market id could be resolved");
        };

        let msg = msg.context("activity websocket read error")?;
        match msg {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(&text).context("failed to parse activity message")?;
                if let Some(mid) = parse_condition_id_from_activity_message(&payload, event_slug) {
                    return Ok(mid);
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong activity websocket")?;
            }
            Message::Close(_) => bail!("activity websocket closed"),
            _ => {}
        }
    }
}

fn parse_condition_id_from_activity_message(payload: &Value, event_slug: &str) -> Option<String> {
    let topic = payload.get("topic").and_then(Value::as_str)?;
    if topic != "activity" {
        return None;
    }

    let msg_type = payload.get("type").and_then(Value::as_str)?;
    if msg_type != "orders_matched" {
        return None;
    }

    let item = payload.get("payload")?;
    let msg_event_slug = item
        .get("eventSlug")
        .and_then(Value::as_str)
        .or_else(|| item.get("event_slug").and_then(Value::as_str));

    if msg_event_slug != Some(event_slug) {
        return None;
    }

    let candidate = item
        .get("conditionId")
        .and_then(Value::as_str)
        .or_else(|| item.get("condition_id").and_then(Value::as_str))?;

    let trimmed = candidate.trim();
    if trimmed.starts_with("0x") && !trimmed.is_empty() {
        return Some(trimmed.to_string());
    }

    None
}

fn auto_today_event_slug() -> String {
    let prefix = first_non_empty_env("POLYMARKET_AUTO_EVENT_PREFIX")
        .unwrap_or_else(|| "bitcoin-above-on".to_string());
    let now = Utc::now().date_naive();
    format!("{}-{}-{}", prefix, month_name(now.month()), now.day())
}

fn month_name(month: u32) -> &'static str {
    match month {
        1 => "january",
        2 => "february",
        3 => "march",
        4 => "april",
        5 => "may",
        6 => "june",
        7 => "july",
        8 => "august",
        9 => "september",
        10 => "october",
        11 => "november",
        12 => "december",
        _ => "unknown",
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
    let subscription = MarketSubscription::from_instrument_id(instrument_id).await?;

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
    let subscription = MarketSubscription::from_instrument_id(instrument_id).await?;

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

fn parse_market_asset_ids_from_env() -> Vec<String> {
    let raw = match std::env::var("POLYMARKET_OUTCOME_ASSET_IDS") {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for token in raw.split(',') {
        let v = token.trim();
        if !v.is_empty() && !out.iter().any(|existing| existing == v) {
            out.push(v.to_string());
        }
    }
    out
}

async fn resolve_market_asset_ids(market_id: &str) -> Result<Vec<String>> {
    let env_ids = parse_market_asset_ids_from_env();
    if !env_ids.is_empty() {
        return Ok(env_ids);
    }

    let base = std::env::var("POLYMARKET_GAMMA_API_URL")
        .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());
    let base = base.trim_end_matches('/');
    let client = reqwest::Client::new();

    let by_path = format!("{base}/markets/{market_id}");
    if let Ok(ids) = fetch_market_asset_ids_from_gamma(&client, &by_path).await {
        if !ids.is_empty() {
            return Ok(ids);
        }
    }

    let by_query = format!("{base}/markets?id={market_id}");
    if let Ok(ids) = fetch_market_asset_ids_from_gamma(&client, &by_query).await {
        if !ids.is_empty() {
            return Ok(ids);
        }
    }

    bail!("could not resolve market asset ids for market_id={market_id}");
}

async fn fetch_market_asset_ids_from_gamma(client: &reqwest::Client, url: &str) -> Result<Vec<String>> {
    let resp = client
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("gamma request failed: {url}"))?;

    if !resp.status().is_success() {
        bail!("gamma request non-success status {} for {}", resp.status(), url);
    }

    let payload: Value = resp
        .json()
        .await
        .with_context(|| format!("gamma response json decode failed: {url}"))?;

    let ids = parse_market_asset_ids_from_gamma_payload(&payload);
    if ids.is_empty() {
        bail!("gamma response had no market asset ids");
    }
    Ok(ids)
}

fn parse_market_asset_ids_from_gamma_payload(payload: &Value) -> Vec<String> {
    match payload {
        Value::Object(_) => parse_market_asset_ids_from_gamma_market(payload),
        Value::Array(items) => items
            .iter()
            .find_map(|v| {
                let ids = parse_market_asset_ids_from_gamma_market(v);
                if ids.is_empty() { None } else { Some(ids) }
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn parse_market_asset_ids_from_gamma_market(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(arr) = v.get("clobTokenIds").and_then(Value::as_array) {
        for idv in arr {
            if let Some(id) = idv.as_str() {
                let id = id.trim();
                if !id.is_empty() && !out.iter().any(|e| e == id) {
                    out.push(id.to_string());
                }
            }
        }
        return out;
    }

    if let Some(s) = v.get("clobTokenIds").and_then(Value::as_str) {
        if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
            for idv in arr {
                if let Some(id) = idv.as_str() {
                    let id = id.trim();
                    if !id.is_empty() && !out.iter().any(|e| e == id) {
                        out.push(id.to_string());
                    }
                }
            }
        }
    }
    out
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

