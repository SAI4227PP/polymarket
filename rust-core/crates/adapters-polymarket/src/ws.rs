use anyhow::{anyhow, bail, Context, Result};
use chrono::{Datelike, Utc};
use common::Quote;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time::{timeout, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const POLYMARKET_WS_MARKET_CLOB: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_WS_LIVE_DATA: &str = "wss://ws-live-data.polymarket.com/";
const WS_HEARTBEAT_SECONDS: u64 = 10;
const MARKET_WS_FIRST_QUOTE_TIMEOUT_MS_DEFAULT: u64 = 20_000;

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

#[derive(Debug, Clone, Default)]
struct OutcomeTokenMap {
    up_token: Option<String>,
    down_token: Option<String>,
}

impl OutcomeTokenMap {
    fn from_ordered(ids: &[String]) -> Self {
        Self {
            up_token: ids.first().cloned(),
            down_token: ids.get(1).cloned(),
        }
    }

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
        let mut payload = json!({
            "type": "market",
            "custom_feature_enabled": true
        });

        if !self.asset_ids.is_empty() {
            payload["assets_ids"] = json!(self.asset_ids);
        }

        payload
    }

    fn accepts_asset(&self, asset_id: &str, preferred_asset: Option<&str>) -> bool {
        if let Some(preferred) = preferred_asset {
            return asset_id == preferred;
        }
        match self.mode {
            InstrumentMode::Asset => self.asset_ids.iter().any(|a| a == asset_id),
            InstrumentMode::Market => true,
        }
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

fn ws_debug_enabled() -> bool {
    first_non_empty_env("POLYMARKET_WS_DEBUG")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn market_ws_first_quote_timeout() -> Duration {
    let timeout_ms = first_non_empty_env("POLYMARKET_MARKET_WS_FIRST_QUOTE_TIMEOUT_MS")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(MARKET_WS_FIRST_QUOTE_TIMEOUT_MS_DEFAULT)
        .max(1_000);
    Duration::from_millis(timeout_ms)
}

fn parse_ws_json_text(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str::<Value>(trimmed).ok()
}
#[derive(Debug, Clone)]
pub struct ResolvedInstrument {
    pub instrument_id: String,
    pub source: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResolvedMarketMetadata {
    pub category: Option<String>,
    pub fees_enabled: Option<bool>,
}

pub async fn resolve_instrument_from_env() -> Result<ResolvedInstrument> {
    if let Some(asset_id) = first_non_empty_env("POLYMARKET_ASSET_ID") {
        return Ok(ResolvedInstrument {
            instrument_id: asset_id,
            source: "env:POLYMARKET_ASSET_ID".to_string(),
        });
    }
    if let Some(market_id) = first_non_empty_env("POLYMARKET_MARKET_ID") {
        return Ok(ResolvedInstrument {
            instrument_id: market_id,
            source: "env:POLYMARKET_MARKET_ID".to_string(),
        });
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
                let instrument_id =
                    resolve_market_id_from_event_slug(event_slug, preferred_market_slug).await?;
                return Ok(ResolvedInstrument {
                    instrument_id,
                    source: format!("env:POLYMARKET_EVENT_URL:{}", event_slug),
                });
            }
        }
        bail!("POLYMARKET_EVENT_URL must look like /event/{{event-slug}}[/market-slug]");
    }

    if let Some(event_slug) = first_non_empty_env("POLYMARKET_EVENT_SLUG") {
        let instrument_id = resolve_market_id_from_event_slug(&event_slug, None).await?;
        return Ok(ResolvedInstrument {
            instrument_id,
            source: format!("env:POLYMARKET_EVENT_SLUG:{}", event_slug),
        });
    }

    let auto_event_slug = auto_today_event_slug();
    let instrument_id = resolve_market_id_from_event_slug(&auto_event_slug, None)
        .await
        .with_context(|| format!("auto-resolve failed for event slug {auto_event_slug}"))?;

    Ok(ResolvedInstrument {
        instrument_id,
        source: format!("auto:event-slug:{}", auto_event_slug),
    })
}

pub async fn resolve_instrument_id_from_env() -> Result<String> {
    Ok(resolve_instrument_from_env().await?.instrument_id)
}

pub async fn resolve_market_id_for_live_trading() -> Result<String> {
    if let Some(mid) = first_non_empty_env("POLYMARKET_MARKET_ID") {
        return Ok(mid);
    }

    let resolved = resolve_instrument_from_env().await?;
    if resolved.instrument_id.starts_with("0x") {
        return Ok(resolved.instrument_id);
    }

    bail!(
        "unable to resolve market id for live trading; set POLYMARKET_MARKET_ID when using asset-id subscription"
    )
}

#[derive(Debug, Clone)]
pub struct ResolvedOutcomeTokens {
    pub up_token: String,
    pub down_token: String,
}

pub async fn resolve_outcome_tokens_from_env() -> Result<ResolvedOutcomeTokens> {
    let env_ids = parse_market_asset_ids_from_env();
    if env_ids.len() >= 2 {
        return Ok(ResolvedOutcomeTokens {
            up_token: env_ids[0].clone(),
            down_token: env_ids[1].clone(),
        });
    }

    let resolved = resolve_instrument_from_env().await?;
    let event_slug = event_slug_for_subscription();
    let map = resolve_outcome_token_map(&resolved.instrument_id, &event_slug)
        .await
        .with_context(|| {
            format!(
                "failed resolving outcome tokens for instrument={} event_slug={}",
                resolved.instrument_id, event_slug
            )
        })?;

    let up_token = map
        .up_token
        .context("missing up/yes token id for live trading")?;
    let down_token = map
        .down_token
        .context("missing down/no token id for live trading")?;

    Ok(ResolvedOutcomeTokens {
        up_token,
        down_token,
    })
}

pub async fn resolve_market_metadata_from_env() -> Result<ResolvedMarketMetadata> {
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
                return resolve_market_metadata_by_event_slug(event_slug, preferred_market_slug).await;
            }
        }
    }

    if let Some(event_slug) = first_non_empty_env("POLYMARKET_EVENT_SLUG") {
        return resolve_market_metadata_by_event_slug(&event_slug, None).await;
    }

    let auto_event_slug = auto_today_event_slug();
    resolve_market_metadata_by_event_slug(&auto_event_slug, None).await
}
async fn resolve_market_id_from_event_slug(
    event_slug: &str,
    preferred_market_slug: Option<&str>,
) -> Result<String> {
    resolve_market_id_from_gamma_event(event_slug, preferred_market_slug).await
}

async fn resolve_market_id_from_gamma_event(event_slug: &str, preferred_market_slug: Option<&str>) -> Result<String> {
    let payload = fetch_gamma_event_payload_by_slug(event_slug).await?;
    let market = select_market_from_event_payload(&payload, preferred_market_slug, None)?;

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

fn gamma_api_base() -> String {
    std::env::var("POLYMARKET_GAMMA_API_URL")
        .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string())
        .trim_end_matches('/')
        .to_string()
}

async fn fetch_gamma_event_payload_by_slug(event_slug: &str) -> Result<Value> {
    let base = gamma_api_base();
    let url = format!("{base}/events/slug/{event_slug}");

    reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("gamma event request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("gamma event request non-success: {url}"))?
        .json()
        .await
        .with_context(|| format!("gamma event decode failed: {url}"))
}

fn select_market_from_event_payload<'a>(
    payload: &'a Value,
    preferred_market_slug: Option<&str>,
    preferred_market_id: Option<&str>,
) -> Result<&'a Value> {
    let markets = payload
        .get("markets")
        .and_then(Value::as_array)
        .context("gamma event payload missing markets")?;

    let pick = if let Some(market_id) = preferred_market_id {
        markets
            .iter()
            .find(|m| {
                m.get("conditionId").and_then(Value::as_str) == Some(market_id)
                    || m.get("id").and_then(Value::as_str) == Some(market_id)
            })
            .or_else(|| if markets.len() == 1 { markets.first() } else { None })
    } else if let Some(mslug) = preferred_market_slug {
        markets.iter().find(|m| m.get("slug").and_then(Value::as_str) == Some(mslug))
    } else {
        markets
            .iter()
            .filter_map(|m| {
                let active = m.get("active").and_then(Value::as_bool).unwrap_or(false);
                let closed = m.get("closed").and_then(Value::as_bool).unwrap_or(false);
                let vol = m
                    .get("volumeNum")
                    .and_then(Value::as_f64)
                    .or_else(|| {
                        m.get("volume")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<f64>().ok())
                    })
                    .unwrap_or(0.0);
                Some((m, if active { 2 } else { 0 } + if !closed { 1 } else { 0 }, vol))
            })
            .max_by(|a, b| {
                a.1.cmp(&b.1)
                    .then_with(|| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
            })
            .map(|(m, _, _)| m)
    };

    pick.context("no market found in gamma event payload")
}


fn parse_market_metadata_from_value(market: &Value) -> ResolvedMarketMetadata {
    let category = market
        .get("category")
        .or_else(|| market.get("group"))
        .or_else(|| market.get("tag"))
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let fees_enabled = market
        .get("feesEnabled")
        .or_else(|| market.get("fees_enabled"))
        .or_else(|| market.get("feeEnabled"))
        .and_then(Value::as_bool);

    ResolvedMarketMetadata {
        category,
        fees_enabled,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MarketWsTrackedMessageKind {
    SubscriptionRequest,
    SubscriptionUpdate,
    Ping,
    Pong,
    OrderbookSnapshot,
    PriceChange,
    LastTradePrice,
    TickSizeChange,
    BestBidAsk,
    NewMarket,
    MarketResolved,
    Unknown,
}

impl MarketWsTrackedMessageKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SubscriptionRequest => "subscription_request",
            Self::SubscriptionUpdate => "subscription_update",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::OrderbookSnapshot => "orderbook_snapshot",
            Self::PriceChange => "price_change",
            Self::LastTradePrice => "last_trade_price",
            Self::TickSizeChange => "tick_size_change",
            Self::BestBidAsk => "best_bid_ask",
            Self::NewMarket => "new_market",
            Self::MarketResolved => "market_resolved",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Default)]
struct MarketWsMessageTracker {
    counts: HashMap<MarketWsTrackedMessageKind, u64>,
}

impl MarketWsMessageTracker {
    fn record(
        &mut self,
        kind: MarketWsTrackedMessageKind,
        direction: &str,
        endpoint: &str,
        debug_ws: bool,
    ) {
        if !debug_ws {
            return;
        }

        let count = self
            .counts
            .entry(kind)
            .and_modify(|n| *n += 1)
            .or_insert(1);
        eprintln!(
            "polymarket ws market_track direction={} endpoint={} message={} count={}",
            direction,
            endpoint,
            kind.as_str(),
            count
        );
    }
}

fn classify_market_ws_message(
    payload: &Value,
    direction: &str,
) -> Option<MarketWsTrackedMessageKind> {
    let obj = payload.as_object()?;

    if let Some(event_type) = obj.get("event_type").and_then(Value::as_str) {
        return Some(match event_type {
            "book" => MarketWsTrackedMessageKind::OrderbookSnapshot,
            "price_change" => MarketWsTrackedMessageKind::PriceChange,
            "last_trade_price" => MarketWsTrackedMessageKind::LastTradePrice,
            "tick_size_change" => MarketWsTrackedMessageKind::TickSizeChange,
            "best_bid_ask" => MarketWsTrackedMessageKind::BestBidAsk,
            "new_market" => MarketWsTrackedMessageKind::NewMarket,
            "market_resolved" => MarketWsTrackedMessageKind::MarketResolved,
            _ => MarketWsTrackedMessageKind::Unknown,
        });
    }

    if obj.get("type").and_then(Value::as_str) == Some("market") {
        return Some(MarketWsTrackedMessageKind::SubscriptionRequest);
    }

    if obj.get("operation").and_then(Value::as_str).is_some() {
        return Some(MarketWsTrackedMessageKind::SubscriptionUpdate);
    }

    if obj.is_empty() {
        return Some(if direction == "inbound" {
            MarketWsTrackedMessageKind::Pong
        } else {
            MarketWsTrackedMessageKind::Ping
        });
    }

    None
}

fn track_market_ws_payload(
    tracker: &mut MarketWsMessageTracker,
    payload: &Value,
    subscription: &MarketSubscription,
    preferred_asset: Option<&str>,
    direction: &str,
    endpoint: &str,
    debug_ws: bool,
) {
    match payload {
        Value::Array(items) => {
            for item in items {
                if !is_market_ws_payload_relevant(item, subscription, preferred_asset, direction) {
                    continue;
                }
                if let Some(kind) = classify_market_ws_message(item, direction) {
                    tracker.record(kind, direction, endpoint, debug_ws);
                }
            }
        }
        _ => {
            if !is_market_ws_payload_relevant(payload, subscription, preferred_asset, direction) {
                return;
            }
            if let Some(kind) = classify_market_ws_message(payload, direction) {
                tracker.record(kind, direction, endpoint, debug_ws);
            }
        }
    }
}

fn is_market_ws_payload_relevant(
    payload: &Value,
    subscription: &MarketSubscription,
    preferred_asset: Option<&str>,
    direction: &str,
) -> bool {
    let Some(obj) = payload.as_object() else {
        return false;
    };

    // Connection-level control messages are always relevant to this stream.
    if obj.is_empty() {
        return matches!(direction, "inbound" | "outbound");
    }
    if obj.get("type").and_then(Value::as_str) == Some("market") {
        return true;
    }
    if obj.get("operation").and_then(Value::as_str).is_some() {
        return true;
    }

    let event_type = obj.get("event_type").and_then(Value::as_str);
    let Some(event_type) = event_type else {
        return false;
    };

    let market = obj
        .get("market")
        .and_then(Value::as_str)
        .or_else(|| obj.get("condition_id").and_then(Value::as_str));
    let market_matches = subscription.accepts_market(market);
    let market_mode = matches!(subscription.mode, InstrumentMode::Market);

    match event_type {
        "price_change" => {
            let levels = match obj.get("price_changes").and_then(Value::as_array) {
                Some(v) => v,
                None => return false,
            };
            levels.iter().any(|lvl| {
                let asset_ok = lvl
                    .get("asset_id")
                    .and_then(Value::as_str)
                    .map(|asset| subscription.accepts_asset(asset, preferred_asset))
                    .unwrap_or(false);
                asset_ok && (market_matches || market_mode)
            })
        }
        "new_market" | "market_resolved" => {
            if let Some(assets) = obj.get("assets_ids").and_then(Value::as_array) {
                let asset_ok = assets.iter().any(|v| {
                    v.as_str()
                        .map(|asset| subscription.accepts_asset(asset, preferred_asset))
                        .unwrap_or(false)
                });
                return asset_ok && (market_matches || market_mode);
            }
            market_matches
        }
        "book" | "best_bid_ask" | "last_trade_price" | "tick_size_change" => {
            let asset_ok = obj
            .get("asset_id")
            .and_then(Value::as_str)
            .map(|asset| subscription.accepts_asset(asset, preferred_asset))
            .unwrap_or(false);
            asset_ok && (market_matches || market_mode)
        }
        _ => false,
    }
}

fn payload_contains_market_resolved_event(
    payload: &Value,
    subscription: &MarketSubscription,
    preferred_asset: Option<&str>,
) -> bool {
    match payload {
        Value::Array(items) => items.iter().any(|item| {
            item.as_object()
                .and_then(|obj| obj.get("event_type"))
                .and_then(Value::as_str)
                .map(|event_type| {
                    event_type == "market_resolved"
                        && is_market_ws_payload_relevant(
                            item,
                            subscription,
                            preferred_asset,
                            "inbound",
                        )
                })
                .unwrap_or(false)
        }),
        Value::Object(obj) => obj
            .get("event_type")
            .and_then(Value::as_str)
            .map(|event_type| {
                event_type == "market_resolved"
                    && is_market_ws_payload_relevant(payload, subscription, preferred_asset, "inbound")
            })
            .unwrap_or(false),
        _ => false,
    }
}

async fn resolve_market_metadata_by_event_slug(
    event_slug: &str,
    preferred_market_slug: Option<&str>,
) -> Result<ResolvedMarketMetadata> {
    let payload = fetch_gamma_event_payload_by_slug(event_slug).await?;
    let market = select_market_from_event_payload(&payload, preferred_market_slug, None)?;
    Ok(parse_market_metadata_from_value(market))
}

pub async fn fetch_market_outcome_payload_from_env() -> Result<Value> {
    let resolved = resolve_instrument_from_env().await?;
    let event_slug = event_slug_for_subscription();
    let payload = fetch_gamma_event_payload_by_slug(&event_slug).await?;

    let preferred_market_id = if resolved.instrument_id.starts_with("0x") {
        Some(resolved.instrument_id.as_str())
    } else {
        None
    };
    let preferred_market_slug = if preferred_market_id.is_none() {
        Some(event_slug.as_str())
    } else {
        None
    };
    let market = select_market_from_event_payload(&payload, preferred_market_slug, preferred_market_id)?;
    let market_id = market
        .get("conditionId")
        .and_then(Value::as_str)
        .or_else(|| market.get("id").and_then(Value::as_str))
        .map(|s| s.to_string())
        .unwrap_or_else(|| resolved.instrument_id.clone());

    let outcomes = parse_market_outcome_labels(&market);
    let prices = parse_market_outcome_prices(&market);
    let (probability_up, probability_down) = assign_up_down_probabilities(&outcomes, &prices);
    let up_prob = probability_up.unwrap_or(0.5);
    let down_prob = probability_down.unwrap_or((1.0 - up_prob).clamp(0.0, 1.0));

    let mut winning_outcome = market
        .get("winning_outcome")
        .or_else(|| market.get("winningOutcome"))
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if winning_outcome.is_none() {
        if up_prob >= 0.999 {
            winning_outcome = Some("Yes".to_string());
        } else if down_prob >= 0.999 {
            winning_outcome = Some("No".to_string());
        }
    }

    let asset_ids = parse_market_asset_ids_from_gamma_market(&market);
    let winning_asset_id = match winning_outcome.as_ref().map(|s| s.to_ascii_lowercase()) {
        Some(ref w) if w == "yes" || w == "up" => asset_ids.first().cloned(),
        Some(ref w) if w == "no" || w == "down" => asset_ids.get(1).cloned(),
        _ => None,
    };

    let closed = market.get("closed").and_then(Value::as_bool);
    let resolved_flag = market
        .get("resolved")
        .and_then(Value::as_bool)
        .or_else(|| Some(winning_outcome.is_some() || up_prob >= 0.999 || down_prob >= 0.999));

    Ok(json!({
        "market_id": market_id,
        "resolved": resolved_flag,
        "closed": closed,
        "winning_outcome": winning_outcome,
        "winning_asset_id": winning_asset_id,
        "probability_up": up_prob,
        "probability_down": down_prob,
        "source": "official_gamma_api_markets",
        "ts_ms": now_ms(),
    }))
}

fn auto_today_event_slug() -> String {
    let prefix = first_non_empty_env("POLYMARKET_AUTO_EVENT_PREFIX")
        .unwrap_or_else(|| "btc-updown-5m".to_string());
    let mode = first_non_empty_env("POLYMARKET_AUTO_EVENT_MODE").unwrap_or_else(|| {
        if prefix.starts_with("btc-updown-5m") {
            "epoch_5m".to_string()
        } else {
            "daily_date".to_string()
        }
    });

    if mode.eq_ignore_ascii_case("epoch_5m") {
        let bucket = current_5m_bucket_unix();
        return format!("{}-{}", prefix, bucket);
    }

    let now = Utc::now().date_naive();
    format!("{}-{}-{}", prefix, month_name(now.month()), now.day())
}

fn current_5m_bucket_unix() -> i64 {
    let ts = Utc::now().timestamp();
    ts - ts.rem_euclid(300)
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
    let mut errors = Vec::new();

    let endpoints = market_ws_endpoints();

    for endpoint in endpoints {
        match stream_market_quote_from_endpoint(instrument_id, &endpoint).await {
            Ok(q) => return Ok(q),
            Err(err) => errors.push(format!("{endpoint}: {err:#}")),
        }
    }

    bail!(
        "all polymarket market websocket endpoints failed: {}",
        errors.join(" | ")
    );
}

pub async fn run_market_quote_stream(
    instrument_id: &str,
    tx: watch::Sender<Option<Quote>>,
) -> Result<()> {
    let mut errors = Vec::new();

    let endpoints = market_ws_endpoints();

    for endpoint in endpoints {
        match run_market_quote_stream_from_endpoint(instrument_id, &endpoint, tx.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) => errors.push(format!("{endpoint}: {err:#}")),
        }
    }

    bail!(
        "all polymarket market websocket endpoints failed: {}",
        errors.join(" | ")
    );
}

fn market_ws_endpoints() -> Vec<String> {
    let mut out = Vec::new();

    if let Some(configured) = first_non_empty_env("POLYMARKET_MARKET_WS_URL") {
        out.push(configured);
    }

    // Use only official CLOB market stream endpoints.
    out.push(POLYMARKET_WS_MARKET_CLOB.to_string());

    let mut deduped = Vec::new();
    for endpoint in out {
        if !deduped.iter().any(|existing| existing == &endpoint) {
            deduped.push(endpoint);
        }
    }
    deduped
}
fn event_slug_for_subscription() -> String {
    if let Some(event_slug) = first_non_empty_env("POLYMARKET_EVENT_SLUG") {
        return event_slug;
    }

    if let Some(event_url) = first_non_empty_env("POLYMARKET_EVENT_URL") {
        if let Ok(parsed) = Url::parse(&event_url) {
            let parts: Vec<&str> = parsed
                .path_segments()
                .map(|s| s.collect())
                .unwrap_or_default();
            if let Some(idx) = parts.iter().position(|p| *p == "event") {
                if idx + 1 < parts.len() {
                    return parts[idx + 1].to_string();
                }
            }
        }
    }

    auto_today_event_slug()
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
                let Some(payload) = parse_ws_json_text(&text) else {
                    continue;
                };
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
    let debug_ws = ws_debug_enabled();
    let mut tracker = MarketWsMessageTracker::default();

    let url = Url::parse(endpoint).context("invalid polymarket market websocket url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to polymarket market websocket {endpoint}"))?;

    let subscribe = subscription.subscribe_payload();
    track_market_ws_payload(
        &mut tracker,
        &subscribe,
        &subscription,
        None,
        "outbound",
        endpoint,
        debug_ws,
    );
    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to send polymarket market subscription")?;

    let preferred_asset: Option<String> = None;
    let heartbeat_interval = Duration::from_secs(WS_HEARTBEAT_SECONDS);
    let mut next_heartbeat_at = Instant::now() + heartbeat_interval;
    let first_quote_deadline = Instant::now() + market_ws_first_quote_timeout();

    loop {
        if Instant::now() >= first_quote_deadline {
            bail!("polymarket market websocket connected but yielded no quote before timeout");
        }

        if Instant::now() >= next_heartbeat_at {
            tracker.record(
                MarketWsTrackedMessageKind::Ping,
                "outbound",
                endpoint,
                debug_ws,
            );
            ws.send(Message::Text("PING".to_string().into()))
                .await
                .context("failed to send market keepalive ping")?;
            next_heartbeat_at = Instant::now() + heartbeat_interval;
        }

        let next = timeout(Duration::from_secs(WS_HEARTBEAT_SECONDS), ws.next()).await;

        let Some(msg) = (match next {
            Ok(v) => v,
            Err(_) => {
                tracker.record(
                    MarketWsTrackedMessageKind::Ping,
                    "outbound",
                    endpoint,
                    debug_ws,
                );
                ws.send(Message::Text("PING".to_string().into()))
                    .await
                    .context("failed to send market keepalive ping")?;
                next_heartbeat_at = Instant::now() + heartbeat_interval;
                continue;
            }
        }) else {
            return Err(anyhow!("polymarket market websocket ended without quote"));
        };

        let msg = msg.context("polymarket market websocket read error")?;
        match msg {
            Message::Text(text) => {
                let Some(payload) = parse_ws_json_text(&text) else {
                    continue;
                };
                track_market_ws_payload(
                    &mut tracker,
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                    "inbound",
                    endpoint,
                    debug_ws,
                );

                if let Some(quote) = parse_quote_from_payload(
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                ) {
                    return Ok(quote);
                }
            }
            Message::Ping(payload) => {
                tracker.record(
                    MarketWsTrackedMessageKind::Ping,
                    "inbound",
                    endpoint,
                    debug_ws,
                );
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong polymarket market websocket")?;
                tracker.record(
                    MarketWsTrackedMessageKind::Pong,
                    "outbound",
                    endpoint,
                    debug_ws,
                );
            }
            Message::Pong(_) => tracker.record(
                MarketWsTrackedMessageKind::Pong,
                "inbound",
                endpoint,
                debug_ws,
            ),
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
    let debug_ws = ws_debug_enabled();
    let mut tracker = MarketWsMessageTracker::default();

    let url = Url::parse(endpoint).context("invalid polymarket market websocket url")?;
    let (mut ws, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to polymarket market websocket {endpoint}"))?;

    let subscribe = subscription.subscribe_payload();
    track_market_ws_payload(
        &mut tracker,
        &subscribe,
        &subscription,
        None,
        "outbound",
        endpoint,
        debug_ws,
    );
    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to send polymarket market subscription")?;

    let mut preferred_asset: Option<String> = None;
    let heartbeat_interval = Duration::from_secs(WS_HEARTBEAT_SECONDS);
    let mut next_heartbeat_at = Instant::now() + heartbeat_interval;
    let first_quote_deadline = Instant::now() + market_ws_first_quote_timeout();
    let mut has_seen_quote = false;

    loop {
        if !has_seen_quote && Instant::now() >= first_quote_deadline {
            bail!("polymarket market websocket connected but yielded no quote before timeout");
        }

        if Instant::now() >= next_heartbeat_at {
            tracker.record(
                MarketWsTrackedMessageKind::Ping,
                "outbound",
                endpoint,
                debug_ws,
            );
            ws.send(Message::Text("PING".to_string().into()))
                .await
                .context("failed to send market keepalive ping")?;
            next_heartbeat_at = Instant::now() + heartbeat_interval;
        }

        let next = timeout(Duration::from_secs(WS_HEARTBEAT_SECONDS), ws.next()).await;

        let Some(msg) = (match next {
            Ok(v) => v,
            Err(_) => {
                tracker.record(
                    MarketWsTrackedMessageKind::Ping,
                    "outbound",
                    endpoint,
                    debug_ws,
                );
                ws.send(Message::Text("PING".to_string().into()))
                    .await
                    .context("failed to send market keepalive ping")?;
                next_heartbeat_at = Instant::now() + heartbeat_interval;
                continue;
            }
        }) else {
            return Err(anyhow!("polymarket market websocket ended"));
        };

        let msg = msg.context("polymarket market websocket read error")?;
        match msg {
            Message::Text(text) => {
                let Some(payload) = parse_ws_json_text(&text) else {
                    continue;
                };
                track_market_ws_payload(
                    &mut tracker,
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                    "inbound",
                    endpoint,
                    debug_ws,
                );

                if payload_contains_market_resolved_event(
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                ) {
                    eprintln!(
                        "polymarket market_resolved received for instrument={} endpoint={}; re-resolving next market",
                        instrument_id,
                        endpoint
                    );
                    return Ok(());
                }

                if let Some(quote) = parse_quote_from_payload(
                    &payload,
                    &subscription,
                    preferred_asset.as_deref(),
                ) {
                    if preferred_asset.is_none() {
                        preferred_asset = Some(quote.symbol.clone());
                    }
                    has_seen_quote = true;
                    let _ = tx.send(Some(quote));
                }
            }
            Message::Ping(payload) => {
                tracker.record(
                    MarketWsTrackedMessageKind::Ping,
                    "inbound",
                    endpoint,
                    debug_ws,
                );
                ws.send(Message::Pong(payload))
                    .await
                    .context("failed to pong polymarket market websocket")?;
                tracker.record(
                    MarketWsTrackedMessageKind::Pong,
                    "outbound",
                    endpoint,
                    debug_ws,
                );
            }
            Message::Pong(_) => tracker.record(
                MarketWsTrackedMessageKind::Pong,
                "inbound",
                endpoint,
                debug_ws,
            ),
            Message::Close(_) => return Err(anyhow!("polymarket market websocket closed")),
            _ => {}
        }
    }
}

fn parse_market_asset_ids_from_env() -> Vec<String> {
    if should_ignore_env_asset_ids_for_auto_epoch_mode() {
        return Vec::new();
    }

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

fn should_ignore_env_asset_ids_for_auto_epoch_mode() -> bool {
    let force_static = first_non_empty_env("POLYMARKET_FORCE_STATIC_OUTCOME_ASSET_IDS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if force_static {
        return false;
    }

    let mode = first_non_empty_env("POLYMARKET_AUTO_EVENT_MODE").unwrap_or_default();
    if !mode.eq_ignore_ascii_case("epoch_5m") {
        return false;
    }

    // If user explicitly pins any target market/instrument, keep honoring static ids.
    if first_non_empty_env("POLYMARKET_ASSET_ID").is_some()
        || first_non_empty_env("POLYMARKET_MARKET_ID").is_some()
        || first_non_empty_env("POLYMARKET_EVENT_SLUG").is_some()
        || first_non_empty_env("POLYMARKET_EVENT_URL").is_some()
    {
        return false;
    }

    true
}

async fn resolve_market_asset_ids(market_id: &str) -> Result<Vec<String>> {
    let env_ids = parse_market_asset_ids_from_env();
    if !env_ids.is_empty() {
        return Ok(env_ids);
    }

    if let Ok(ids) = resolve_market_asset_ids_by_condition_id(market_id).await {
        if !ids.is_empty() {
            return Ok(ids);
        }
    }

    let event_slug = event_slug_for_subscription();
    if event_slug.trim().is_empty() {
        bail!("event slug is empty; cannot resolve market asset ids");
    }

    resolve_market_asset_ids_from_event_slug(&event_slug, market_id).await
}

async fn resolve_market_asset_ids_from_event_slug(event_slug: &str, market_id: &str) -> Result<Vec<String>> {
    let payload = fetch_gamma_event_payload_by_slug(event_slug).await?;
    let selected = select_market_from_event_payload(&payload, None, Some(market_id))?;

    let ids = parse_market_asset_ids_from_gamma_market(selected);
    if ids.is_empty() {
        bail!("matched market in event payload had no clobTokenIds");
    }
    Ok(ids)
}

async fn resolve_market_asset_ids_by_condition_id(market_id: &str) -> Result<Vec<String>> {
    let trimmed_market_id = market_id.trim();
    if trimmed_market_id.is_empty() {
        bail!("market id is empty");
    }

    let base = gamma_api_base();
    let direct_url = format!("{base}/markets/{trimmed_market_id}");
    let direct_payload = reqwest::Client::new()
        .get(&direct_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("gamma market-by-id request failed: {direct_url}"))
        .and_then(|resp| {
            resp.error_for_status()
                .with_context(|| format!("gamma market-by-id request non-success: {direct_url}"))
        })
        .and_then(|resp| async {
            resp.json::<Value>()
                .await
                .with_context(|| format!("gamma market-by-id decode failed: {direct_url}"))
        }
        .await);
    if let Ok(payload) = direct_payload {
        let ids = parse_market_asset_ids_from_gamma_market(&payload);
        if !ids.is_empty() {
            return Ok(ids);
        }
    }

    let url = format!("{base}/markets");
    let payload: Value = reqwest::Client::new()
        .get(&url)
        .query(&[
            ("condition_ids", trimmed_market_id),
            ("limit", "10"),
            ("closed", "true"),
        ])
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("gamma markets request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("gamma markets request non-success: {url}"))?
        .json()
        .await
        .with_context(|| format!("gamma markets decode failed: {url}"))?;

    let market = select_market_from_markets_payload(&payload, trimmed_market_id)
        .context("no gamma market matched provided condition id")?;
    let ids = parse_market_asset_ids_from_gamma_market(market);
    if ids.is_empty() {
        bail!("gamma market matched condition id but has no clobTokenIds");
    }
    Ok(ids)
}

fn select_market_from_markets_payload<'a>(payload: &'a Value, market_id: &str) -> Option<&'a Value> {
    let market_matches = |market: &'a Value| {
        let condition_id = market
            .get("conditionId")
            .or_else(|| market.get("condition_id"))
            .and_then(Value::as_str);
        let id = market.get("id").and_then(Value::as_str);
        condition_id == Some(market_id) || id == Some(market_id)
    };

    match payload {
        Value::Array(items) => items.iter().find(|m| market_matches(m)),
        Value::Object(map) => {
            if (map.contains_key("conditionId") || map.contains_key("condition_id") || map.contains_key("id"))
                && market_matches(payload)
            {
                return Some(payload);
            }
            map.get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.iter().find(|m| market_matches(m)))
        }
        _ => None,
    }
}

async fn fetch_market_outcome_token_map_from_gamma(
    client: &reqwest::Client,
    url: &str,
) -> Result<OutcomeTokenMap> {
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

    parse_market_outcome_token_map_from_gamma_payload(&payload)
        .context("gamma response had no outcome token map")
}
fn parse_market_asset_ids_from_gamma_market(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(arr) = v
        .get("clobTokenIds")
        .or_else(|| v.get("clob_token_ids"))
        .and_then(Value::as_array)
    {
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

    if let Some(s) = v
        .get("clobTokenIds")
        .or_else(|| v.get("clob_token_ids"))
        .and_then(Value::as_str)
    {
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

fn parse_market_outcome_token_map_from_gamma_payload(payload: &Value) -> Option<OutcomeTokenMap> {
    match payload {
        Value::Object(_) => parse_market_outcome_token_map_from_gamma_market(payload),
        Value::Array(items) => items
            .iter()
            .find_map(parse_market_outcome_token_map_from_gamma_market),
        _ => None,
    }
}

fn parse_market_outcome_token_map_from_gamma_market(v: &Value) -> Option<OutcomeTokenMap> {
    let ids = parse_market_asset_ids_from_gamma_market(v);
    if ids.is_empty() {
        return None;
    }

    let outcomes = parse_market_outcome_labels(v);
    if outcomes.is_empty() {
        return Some(OutcomeTokenMap::from_ordered(&ids));
    }

    let mut map = OutcomeTokenMap::default();
    for (idx, label) in outcomes.iter().enumerate() {
        let token = match ids.get(idx) {
            Some(t) => t.clone(),
            None => continue,
        };
        if label.eq_ignore_ascii_case("up") || label.eq_ignore_ascii_case("yes") {
            map.up_token = Some(token);
        } else if label.eq_ignore_ascii_case("down") || label.eq_ignore_ascii_case("no") {
            map.down_token = Some(token);
        }
    }

    if map.up_token.is_none() && map.down_token.is_none() {
        return Some(OutcomeTokenMap::from_ordered(&ids));
    }

    if map.up_token.is_none() {
        map.up_token = ids.first().cloned();
    }
    if map.down_token.is_none() {
        map.down_token = ids.get(1).cloned();
    }
    Some(map)
}

fn parse_market_outcome_labels(v: &Value) -> Vec<String> {
    if let Some(arr) = v.get("outcomes").and_then(Value::as_array) {
        return arr
            .iter()
            .filter_map(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }

    if let Some(s) = v.get("outcomes").and_then(Value::as_str) {
        if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
            return arr
                .iter()
                .filter_map(Value::as_str)
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect();
        }
    }

    Vec::new()
}

fn parse_market_outcome_prices(v: &Value) -> Vec<f64> {
    if let Some(arr) = v.get("outcomePrices").and_then(Value::as_array) {
        return arr
            .iter()
            .filter_map(parse_price)
            .filter(|p| p.is_finite())
            .collect();
    }

    if let Some(s) = v.get("outcomePrices").and_then(Value::as_str) {
        if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
            return arr
                .iter()
                .filter_map(parse_price)
                .filter(|p| p.is_finite())
                .collect();
        }
    }

    Vec::new()
}

fn assign_up_down_probabilities(outcomes: &[String], prices: &[f64]) -> (Option<f64>, Option<f64>) {
    if prices.is_empty() {
        return (None, None);
    }

    let mut up: Option<f64> = None;
    let mut down: Option<f64> = None;

    for (idx, label) in outcomes.iter().enumerate() {
        let p = match prices.get(idx) {
            Some(v) => *v,
            None => continue,
        };
        if label.eq_ignore_ascii_case("yes") || label.eq_ignore_ascii_case("up") {
            up = Some(p);
        } else if label.eq_ignore_ascii_case("no") || label.eq_ignore_ascii_case("down") {
            down = Some(p);
        }
    }

    if up.is_none() {
        up = prices.first().copied();
    }
    if down.is_none() {
        down = prices.get(1).copied().or_else(|| up.map(|v| (1.0 - v).clamp(0.0, 1.0)));
    }

    (up, down)
}

async fn resolve_outcome_token_map(
    instrument_id: &str,
    event_slug: &str,
) -> Option<OutcomeTokenMap> {
    let base = std::env::var("POLYMARKET_GAMMA_API_URL")
        .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());
    let base = base.trim_end_matches('/');
    let client = reqwest::Client::new();

    let by_slug_query = format!("{base}/markets?slug={event_slug}");
    if let Ok(map) = fetch_market_outcome_token_map_from_gamma(&client, &by_slug_query).await {
        return Some(map);
    }

    let by_slug_path = format!("{base}/markets/slug/{event_slug}");
    if let Ok(map) = fetch_market_outcome_token_map_from_gamma(&client, &by_slug_path).await {
        return Some(map);
    }

    if instrument_id.starts_with("0x") {
        if let Ok(ids) = resolve_market_asset_ids(instrument_id).await {
            if !ids.is_empty() {
                return Some(OutcomeTokenMap::from_ordered(&ids));
            }
        }
    }
    None
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
            let bid = best_bid_from_book_levels(bids)?;
            let ask = best_ask_from_book_levels(asks)?;
            (event_asset.to_string(), bid, ask)
        }
        "price_change" => parse_price_change_bid_ask(payload, subscription, preferred_asset)?,
        "last_trade_price" => {
            let event_market = payload.get("market").and_then(Value::as_str);
            let event_asset = payload.get("asset_id").and_then(Value::as_str)?;

            if !subscription.accepts_market(event_market)
                || !subscription.accepts_asset(event_asset, preferred_asset)
            {
                return None;
            }

            let px = payload
                .get("price")
                .and_then(parse_price)
                .or_else(|| payload.get("last_trade_price").and_then(parse_price))?;
            (event_asset.to_string(), px, px)
        }
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
            if !subscription.accepts_asset(level_asset, preferred_asset) {
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

fn best_bid_from_book_levels(levels: &[Value]) -> Option<f64> {
    levels
        .iter()
        .filter_map(|level| level.get("price").and_then(parse_price))
        .filter(|p| p.is_finite())
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

fn best_ask_from_book_levels(levels: &[Value]) -> Option<f64> {
    levels
        .iter()
        .filter_map(|level| level.get("price").and_then(parse_price))
        .filter(|p| p.is_finite())
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum UserWsTrackedMessageKind {
    SubscriptionRequest,
    SubscriptionUpdate,
    Ping,
    Pong,
    OrderEvent,
    TradeEvent,
}

impl UserWsTrackedMessageKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SubscriptionRequest => "subscription_request",
            Self::SubscriptionUpdate => "subscription_update",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::OrderEvent => "order_event",
            Self::TradeEvent => "trade_event",
        }
    }
}

#[derive(Debug, Default)]
struct UserWsMessageTracker {
    counts: HashMap<UserWsTrackedMessageKind, u64>,
}

impl UserWsMessageTracker {
    fn record(&mut self, kind: UserWsTrackedMessageKind, direction: &str) {
        let count = self
            .counts
            .entry(kind)
            .and_modify(|n| *n += 1)
            .or_insert(1);
        eprintln!(
            "polymarket ws user_track direction={} message={} count={}",
            direction,
            kind.as_str(),
            count
        );
    }
}

fn classify_user_ws_payload(payload: &Value, direction: &str) -> Option<UserWsTrackedMessageKind> {
    let obj = payload.as_object()?;

    if obj.get("type").and_then(Value::as_str) == Some("user") {
        return Some(UserWsTrackedMessageKind::SubscriptionRequest);
    }

    if obj.get("operation").and_then(Value::as_str).is_some() {
        return Some(UserWsTrackedMessageKind::SubscriptionUpdate);
    }

    if obj.is_empty() {
        return Some(if direction == "inbound" {
            UserWsTrackedMessageKind::Pong
        } else {
            UserWsTrackedMessageKind::Ping
        });
    }

    let event_type = obj
        .get("event_type")
        .and_then(Value::as_str)
        .or_else(|| obj.get("type").and_then(Value::as_str));

    match event_type {
        Some(v) if v.eq_ignore_ascii_case("order") => Some(UserWsTrackedMessageKind::OrderEvent),
        Some(v) if v.eq_ignore_ascii_case("trade") => Some(UserWsTrackedMessageKind::TradeEvent),
        _ => None,
    }
}

fn track_user_ws_inbound_payload(tracker: &mut UserWsMessageTracker, payload: &Value) {
    if let Some(kind) = classify_user_ws_payload(payload, "inbound") {
        tracker.record(kind, "inbound");
        return;
    }

    match payload {
        Value::Array(items) => {
            for item in items {
                if let Some(kind) = classify_user_ws_payload(item, "inbound") {
                    tracker.record(kind, "inbound");
                }
            }
        }
        Value::Object(map) => {
            for v in map.values() {
                if let Some(kind) = classify_user_ws_payload(v, "inbound") {
                    tracker.record(kind, "inbound");
                }
            }
        }
        _ => {}
    }
}

pub async fn run_user_event_stream(market_id: &str) -> Result<()> {
    let api_key = std::env::var("POLYMARKET_API_KEY")
        .context("missing POLYMARKET_API_KEY for user websocket stream")?;
    let api_secret = std::env::var("POLYMARKET_API_SECRET")
        .context("missing POLYMARKET_API_SECRET for user websocket stream")?;
    let passphrase = std::env::var("POLYMARKET_PASSPHRASE")
        .context("missing POLYMARKET_PASSPHRASE for user websocket stream")?;

    let ws_url = std::env::var("POLYMARKET_USER_WS_URL")
        .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_string());
    let url = Url::parse(&ws_url).context("invalid POLYMARKET_USER_WS_URL")?;

    let (mut socket, _) = connect_async(url.as_str())
        .await
        .context("failed to connect to polymarket user websocket")?;

    let mut tracker = UserWsMessageTracker::default();
    let subscribe = json!({
        "auth": {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": passphrase
        },
        "markets": [market_id],
        "type": "user"
    });
    tracker.record(UserWsTrackedMessageKind::SubscriptionRequest, "outbound");
    socket
        .send(Message::Text(subscribe.to_string().into()))
        .await
        .context("failed to subscribe to polymarket user websocket")?;

    let heartbeat_interval = Duration::from_secs(WS_HEARTBEAT_SECONDS);
    let mut next_heartbeat_at = Instant::now() + heartbeat_interval;

    loop {
        let now = Instant::now();
        if now >= next_heartbeat_at {
            tracker.record(UserWsTrackedMessageKind::Ping, "outbound");
            socket
                .send(Message::Text("PING".to_string().into()))
                .await
                .context("failed to send user websocket heartbeat ping")?;
            next_heartbeat_at = Instant::now() + heartbeat_interval;
        }

        let wait_window = next_heartbeat_at.saturating_duration_since(Instant::now());
        let next = timeout(wait_window, socket.next()).await;
        let msg = match next {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => return Err(anyhow!("user websocket read error: {e}")),
            Ok(None) => return Err(anyhow!("user websocket ended")),
            Err(_) => {
                tracker.record(UserWsTrackedMessageKind::Ping, "outbound");
                socket
                    .send(Message::Text("PING".to_string().into()))
                    .await
                    .context("failed to send user websocket heartbeat ping")?;
                next_heartbeat_at = Instant::now() + heartbeat_interval;
                continue;
            }
        };

        match msg {
            Message::Text(text) => {
                if let Some(payload) = parse_ws_json_text(&text) {
                    track_user_ws_inbound_payload(&mut tracker, &payload);
                }
            }
            Message::Ping(payload) => {
                tracker.record(UserWsTrackedMessageKind::Ping, "inbound");
                socket
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to pong user websocket")?;
                tracker.record(UserWsTrackedMessageKind::Pong, "outbound");
            }
            Message::Pong(_) => tracker.record(UserWsTrackedMessageKind::Pong, "inbound"),
            Message::Close(_) => return Err(anyhow!("user websocket closed")),
            _ => {}
        }
    }
}
