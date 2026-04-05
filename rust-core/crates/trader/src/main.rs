use anyhow::{anyhow, Context, Result};
use adapters_binance::{ws as binance_ws, BinanceClient};
use adapters_polymarket::{ws as polymarket_ws, PolymarketClient};
use common::Quote;
use execution_engine::order_manager::{self, ExecutionConfig};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use risk_engine::kill_switch::{should_kill, KillSwitchConfig, KillSwitchState};
use risk_engine::limits::{evaluate_trade, RiskInput, RiskLimits};
use serde::Serialize;
use strategy_engine::signals::{compute_signal, SignalConfig};
use tokio::sync::watch;
use tokio::time::{interval, sleep, Duration};

#[derive(Debug, Clone)]
struct PendingPosition {
    settle_at_ms: u64,
    expected_pnl_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
struct TraderState {
    ts_ms: u64,
    polymarket_mid: f64,
    binance_mid: f64,
    edge_bps: f64,
    net_edge_bps: f64,
    direction: String,
    risk_allowed: bool,
    action: String,
    execution_status: String,
    open_positions: usize,
    day_pnl_usd: f64,
    oldest_quote_age_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let pm_instrument_id = std::env::var("POLYMARKET_ASSET_ID")
        .or_else(|_| std::env::var("POLYMARKET_MARKET_ID"))
        .context("missing POLYMARKET_ASSET_ID or POLYMARKET_MARKET_ID")?;
    let bn_symbol = std::env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    let loop_ms = env_u64("LOOP_INTERVAL_MS", 1_000);
    let min_edge_bps = env_f64("MIN_EDGE_BPS", 8.0);
    let order_notional_usd = env_f64("ORDER_NOTIONAL_USD", 1.0);
    let position_ttl_ms = env_u64("POSITION_TTL_MS", 10_000);

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_state_key = std::env::var("REDIS_TRADER_STATE_KEY")
        .unwrap_or_else(|_| "trader:state:latest".to_string());

    let signal_cfg = SignalConfig {
        min_edge_bps,
        anchor_price: env_f64("BINANCE_ANCHOR_PRICE", 100_000.0),
        basis_bps: env_f64("BINANCE_BASIS_BPS", 0.0),
        target_notional_usd: order_notional_usd,
        ..SignalConfig::default()
    };

    let risk_limits = RiskLimits {
        max_notional_usd: env_f64("MAX_NOTIONAL_USD", 10.0),
        max_daily_loss_usd: env_f64("MAX_DAILY_LOSS_USD", 100.0),
        max_open_positions: env_u64("MAX_OPEN_POSITIONS", 3) as usize,
        max_leverage: env_f64("MAX_LEVERAGE", 2.5),
        max_var_budget_usd: env_f64("MAX_VAR_BUDGET_USD", 150.0),
        max_concentration_ratio: env_f64("MAX_CONCENTRATION_RATIO", 0.5),
    };

    let kill_cfg = KillSwitchConfig {
        max_drawdown_pct: env_f64("KILL_SWITCH_DRAWDOWN_PCT", 5.0),
        max_error_streak: env_u64("KILL_SWITCH_ERROR_STREAK", 5) as u32,
        max_stale_data_ms: env_u64("KILL_SWITCH_STALE_DATA_MS", 15_000),
    };

    let execution_cfg = ExecutionConfig {
        base_qty: env_f64("ORDER_QTY", 5.0),
        edge_threshold_bps: env_f64("EXECUTION_EDGE_THRESHOLD_BPS", 5.0),
        demo_wallet_balance_usd: env_f64("DEMO_WALLET_BALANCE_USD", 10.0),
        min_order_notional_usd: env_f64("MIN_ORDER_NOTIONAL_USD", 1.0),
        min_order_shares: env_f64("MIN_ORDER_SHARES", 5.0),
        ..ExecutionConfig::default()
    };

    let account_equity_usd = env_f64("ACCOUNT_EQUITY_USD", 10.0);
    let realized_vol_bps = env_f64("REALIZED_VOL_BPS", 50.0);

    let _pm = PolymarketClient::new();
    let _bn = BinanceClient::new();

    let (bn_tx, bn_rx) = watch::channel::<Option<Quote>>(None);
    let (pm_tx, pm_rx) = watch::channel::<Option<Quote>>(None);

    spawn_binance_stream(bn_symbol.clone(), bn_tx);
    spawn_polymarket_stream(pm_instrument_id.clone(), pm_tx);

    let mut redis_conn: Option<MultiplexedConnection> = None;

    let mut ticker = interval(Duration::from_millis(loop_ms));
    let mut day_pnl_usd = 0.0;
    let mut pending_positions: Vec<PendingPosition> = Vec::new();
    let mut error_streak = 0u32;
    let mut oldest_quote_age_ms = 0u64;

    loop {
        ticker.tick().await;

        settle_expired_positions(&mut pending_positions, &mut day_pnl_usd, now_ms());
        let open_positions = pending_positions.len();

        let iteration = run_iteration(
            &pm_rx,
            &bn_rx,
            signal_cfg,
            risk_limits,
            execution_cfg,
            order_notional_usd,
            day_pnl_usd,
            open_positions,
            position_ttl_ms,
            account_equity_usd,
            realized_vol_bps,
        )
        .await;

        match iteration {
            Ok(IterationOutcome { position, state }) => {
                error_streak = 0;
                oldest_quote_age_ms = state.oldest_quote_age_ms;

                if let Some(pos) = position {
                    pending_positions.push(pos);
                }

                if let Err(err) = publish_state_to_redis(
                    &redis_url,
                    &redis_state_key,
                    &state,
                    &mut redis_conn,
                )
                .await
                {
                    eprintln!("redis publish error: {err:#}");
                }
            }
            Err(err) => {
                error_streak = error_streak.saturating_add(1);
                eprintln!("iteration error: {err:#}");
            }
        }

        if should_kill(
            KillSwitchState {
                current_drawdown_pct: env_f64("CURRENT_DRAWDOWN_PCT", 0.0),
                error_streak,
                oldest_quote_age_ms,
            },
            kill_cfg,
        ) {
            return Err(anyhow!(
                "kill switch triggered drawdown_or_error_streak={} stale_ms={}",
                error_streak,
                oldest_quote_age_ms
            ));
        }

        if day_pnl_usd <= -risk_limits.max_daily_loss_usd {
            return Err(anyhow!(
                "daily loss threshold breached day_pnl_usd={} limit={} ",
                day_pnl_usd,
                -risk_limits.max_daily_loss_usd
            ));
        }
    }
}

struct IterationOutcome {
    position: Option<PendingPosition>,
    state: TraderState,
}

#[allow(clippy::too_many_arguments)]
async fn run_iteration(
    pm_rx: &watch::Receiver<Option<Quote>>,
    bn_rx: &watch::Receiver<Option<Quote>>,
    signal_cfg: SignalConfig,
    risk_limits: RiskLimits,
    execution_cfg: ExecutionConfig,
    order_notional_usd: f64,
    day_pnl_usd: f64,
    open_positions: usize,
    position_ttl_ms: u64,
    account_equity_usd: f64,
    realized_vol_bps: f64,
) -> Result<IterationOutcome> {
    let pm_quote = match pm_rx.borrow().clone() {
        Some(q) => q,
        None => {
            eprintln!("waiting for polymarket quote...");
            return Ok(IterationOutcome {
                position: None,
                state: waiting_state(day_pnl_usd, open_positions),
            });
        }
    };
    let bn_quote = match bn_rx.borrow().clone() {
        Some(q) => q,
        None => {
            eprintln!("waiting for binance quote...");
            return Ok(IterationOutcome {
                position: None,
                state: waiting_state(day_pnl_usd, open_positions),
            });
        }
    };

    let signal = compute_signal(&pm_quote, &bn_quote, signal_cfg);

    let gross_exposure_usd = (open_positions as f64 * order_notional_usd) + order_notional_usd;
    let risk = evaluate_trade(
        RiskInput {
            notional_usd: order_notional_usd,
            day_pnl_usd,
            open_positions,
            gross_exposure_usd,
            account_equity_usd,
            realized_vol_bps,
            largest_position_usd: order_notional_usd,
        },
        risk_limits,
    );

    let top_book_liquidity_usd = env_f64("TOP_BOOK_LIQUIDITY_USD", 5_000.0);
    let intent = if risk.allowed {
        order_manager::build_order_intent(
            "BTC",
            &signal,
            pm_quote.price,
            order_notional_usd,
            top_book_liquidity_usd,
            realized_vol_bps,
            execution_cfg,
        )
    } else {
        None
    };
    let filled_notional_usd = intent
        .as_ref()
        .map(|o| o.quantity * o.limit_price)
        .unwrap_or(0.0);
    let report = order_manager::submit_if_needed(intent);

    let oldest_quote_age_ms = now_ms()
        .saturating_sub(pm_quote.ts_ms)
        .max(now_ms().saturating_sub(bn_quote.ts_ms));

    println!(
        "edge_bps={:.2} net_edge_bps={:.2} dir={:?} pm_mid={:.4} bn_mid={:.2} risk_allowed={} open_positions={} day_pnl={} route={} status={}",
        signal.edge_bps,
        signal.net_edge_bps,
        signal.direction,
        pm_quote.price,
        bn_quote.price,
        risk.allowed,
        open_positions,
        day_pnl_usd,
        report.routed_path,
        report.status
    );

    let state = TraderState {
        ts_ms: now_ms(),
        polymarket_mid: pm_quote.price,
        binance_mid: bn_quote.price,
        edge_bps: signal.edge_bps,
        net_edge_bps: signal.net_edge_bps,
        direction: format!("{:?}", signal.direction),
        risk_allowed: risk.allowed,
        action: if signal.should_trade {
            "trade".to_string()
        } else {
            "hold".to_string()
        },
        execution_status: report.status.clone(),
        open_positions,
        day_pnl_usd,
        oldest_quote_age_ms,
    };

    if report.accepted {
        let expected_pnl_usd = filled_notional_usd * (signal.net_edge_bps / 10_000.0);
        return Ok(IterationOutcome {
            position: Some(PendingPosition {
                settle_at_ms: now_ms().saturating_add(position_ttl_ms),
                expected_pnl_usd,
            }),
            state,
        });
    }

    Ok(IterationOutcome {
        position: None,
        state,
    })
}

async fn publish_state_to_redis(
    redis_url: &str,
    key: &str,
    state: &TraderState,
    conn: &mut Option<MultiplexedConnection>,
) -> Result<()> {
    if conn.is_none() {
        let client = redis::Client::open(redis_url).context("invalid redis url")?;
        let c = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to redis")?;
        *conn = Some(c);
    }

    let payload = serde_json::to_string(state).context("serialize trader state")?;

    if let Some(c) = conn.as_mut() {
        let set_res: redis::RedisResult<()> = c.set(key, payload).await;
        if let Err(e) = set_res {
            *conn = None;
            return Err(anyhow!("redis set failed: {e}"));
        }
    }

    Ok(())
}

fn waiting_state(day_pnl_usd: f64, open_positions: usize) -> TraderState {
    TraderState {
        ts_ms: now_ms(),
        polymarket_mid: 0.0,
        binance_mid: 0.0,
        edge_bps: 0.0,
        net_edge_bps: 0.0,
        direction: "Flat".to_string(),
        risk_allowed: false,
        action: "waiting_quotes".to_string(),
        execution_status: "waiting".to_string(),
        open_positions,
        day_pnl_usd,
        oldest_quote_age_ms: 0,
    }
}

fn spawn_binance_stream(symbol: String, tx: watch::Sender<Option<Quote>>) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = binance_ws::run_quote_stream(&symbol, tx.clone()).await {
                eprintln!("binance stream error: {err:#}");
            }
            sleep(Duration::from_millis(750)).await;
        }
    });
}

fn spawn_polymarket_stream(instrument_id: String, tx: watch::Sender<Option<Quote>>) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = polymarket_ws::run_market_quote_stream(&instrument_id, tx.clone()).await {
                eprintln!("polymarket stream error: {err:#}");
            }
            sleep(Duration::from_millis(750)).await;
        }
    });
}

fn settle_expired_positions(
    pending_positions: &mut Vec<PendingPosition>,
    day_pnl_usd: &mut f64,
    now_ms: u64,
) {
    let mut keep = Vec::with_capacity(pending_positions.len());
    for pos in pending_positions.drain(..) {
        if pos.settle_at_ms <= now_ms {
            *day_pnl_usd += pos.expected_pnl_usd;
        } else {
            keep.push(pos);
        }
    }
    *pending_positions = keep;
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

