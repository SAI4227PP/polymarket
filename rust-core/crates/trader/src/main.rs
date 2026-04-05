use anyhow::{anyhow, Context, Result};
use adapters_binance::BinanceClient;
use adapters_polymarket::PolymarketClient;
use execution_engine::order_manager;
use risk_engine::kill_switch::{should_kill, KillSwitchConfig};
use risk_engine::limits::{evaluate_trade, RiskLimits};
use strategy_engine::signals::{compute_signal, SignalConfig};
use tokio::time::{interval, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let pm_instrument_id = std::env::var("POLYMARKET_ASSET_ID")
        .or_else(|_| std::env::var("POLYMARKET_MARKET_ID"))
        .context("missing POLYMARKET_ASSET_ID or POLYMARKET_MARKET_ID")?;
    let bn_symbol = std::env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    let loop_ms = env_u64("LOOP_INTERVAL_MS", 1_000);
    let min_edge_bps = env_f64("MIN_EDGE_BPS", 8.0);
    let order_qty = env_f64("ORDER_QTY", 10.0);
    let order_notional_usd = env_f64("ORDER_NOTIONAL_USD", 100.0);

    let signal_cfg = SignalConfig {
        min_edge_bps,
        anchor_price: env_f64("BINANCE_ANCHOR_PRICE", 100_000.0),
        basis_bps: env_f64("BINANCE_BASIS_BPS", 0.0),
        ..SignalConfig::default()
    };

    let risk_limits = RiskLimits {
        max_notional_usd: env_f64("MAX_NOTIONAL_USD", 1000.0),
        max_daily_loss_usd: env_f64("MAX_DAILY_LOSS_USD", 100.0),
        max_open_positions: env_u64("MAX_OPEN_POSITIONS", 3) as usize,
    };

    let kill_cfg = KillSwitchConfig {
        max_drawdown_pct: env_f64("KILL_SWITCH_DRAWDOWN_PCT", 5.0),
        max_error_streak: env_u64("KILL_SWITCH_ERROR_STREAK", 5) as u32,
    };

    let pm = PolymarketClient::new();
    let bn = BinanceClient::new();

    let mut ticker = interval(Duration::from_millis(loop_ms));
    let day_pnl_usd = 0.0;
    let mut open_positions = 0usize;
    let mut error_streak = 0u32;

    loop {
        ticker.tick().await;

        let iteration = run_iteration(
            &pm,
            &bn,
            &pm_instrument_id,
            &bn_symbol,
            signal_cfg,
            risk_limits,
            order_qty,
            order_notional_usd,
            day_pnl_usd,
            open_positions,
        )
        .await;

        match iteration {
            Ok(executed) => {
                error_streak = 0;
                if executed {
                    open_positions = open_positions.saturating_add(1);
                }
            }
            Err(err) => {
                error_streak = error_streak.saturating_add(1);
                eprintln!("iteration error: {err:#}");
            }
        }

        if should_kill(env_f64("CURRENT_DRAWDOWN_PCT", 0.0), error_streak, kill_cfg) {
            return Err(anyhow!(
                "kill switch triggered drawdown_or_error_streak={}",
                error_streak
            ));
        }

        if day_pnl_usd <= -risk_limits.max_daily_loss_usd {
            return Err(anyhow!("daily loss threshold breached"));
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_iteration(
    pm: &PolymarketClient,
    bn: &BinanceClient,
    pm_instrument_id: &str,
    bn_symbol: &str,
    signal_cfg: SignalConfig,
    risk_limits: RiskLimits,
    order_qty: f64,
    order_notional_usd: f64,
    day_pnl_usd: f64,
    open_positions: usize,
) -> Result<bool> {
    let (pm_quote, bn_quote) = tokio::try_join!(
        pm.best_bid_ask_mid(pm_instrument_id),
        bn.best_bid_ask_mid(bn_symbol)
    )?;

    let signal = compute_signal(&pm_quote, &bn_quote, signal_cfg);
    let risk = evaluate_trade(order_notional_usd, day_pnl_usd, open_positions, risk_limits);

    let intent = if risk.allowed {
        order_manager::build_order_intent("BTC", &signal, order_qty, pm_quote.price)
    } else {
        None
    };
    let report = order_manager::submit_if_needed(intent);

    println!(
        "edge_bps={:.2} net_edge_bps={:.2} dir={:?} pm_mid={:.4} bn_mid={:.2} risk_allowed={} route={} status={}",
        signal.edge_bps,
        signal.net_edge_bps,
        signal.direction,
        pm_quote.price,
        bn_quote.price,
        risk.allowed,
        report.routed_path,
        report.status
    );

    Ok(report.accepted)
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
