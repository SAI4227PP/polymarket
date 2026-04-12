use anyhow::{anyhow, Context, Result};
use adapters_binance::{ws as binance_ws, BinanceClient};
use adapters_polymarket::{ws as polymarket_ws, LiveFillCriteria, LiveLimitOrderRequest, LiveOrderSide, PolymarketClient};
use common::{OrderIntent, Quote, RiskDecision, TradeDirection};
use execution_engine::order_manager::{self, ExecutionConfig};
use portfolio_engine::daily::{update_daily_history, PortfolioDayStats};
use portfolio_engine::pnl::{total_pnl, unrealized_pnl};
use portfolio_engine::positions::{update_position, Position};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use risk_engine::kill_switch::{should_kill, KillSwitchConfig, KillSwitchState};
use risk_engine::limits::{evaluate_trade, RiskInput, RiskLimits};
use serde::{Deserialize, Serialize};
use serde_json::json;
use strategy_engine::signals::{compute_signal, SignalConfig};
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time::{interval, sleep, Duration};

#[derive(Debug, Clone)]
struct PendingPosition {
    id: String,
    direction: TradeDirection,
    quantity: f64,
    entry_price: f64,
    entry_notional_usd: f64,
    entry_ts_ms: u64,
    settle_at_ms: u64,
    expected_pnl_usd: f64,
}

#[derive(Debug, Clone)]
struct PendingLiveEntry {
    id: String,
    direction: TradeDirection,
    quantity: f64,
    entry_price: f64,
    entry_notional_usd: f64,
    expected_pnl_usd: f64,
    submitted_ts_ms: u64,
    settle_at_ms: u64,
    criteria: LiveFillCriteria,
    submit_status: String,
    last_reconcile_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenTrade {
    id: String,
    pair: String,
    direction: String,
    entry_side: String,
    quantity: f64,
    entry_price: f64,
    entry_probability: f64,
    entry_notional_usd: f64,
    entry_ts_ms: u64,
    settle_at_ms: u64,
    expected_pnl_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompletedTrade {
    id: String,
    pair: String,
    direction: String,
    entry_side: String,
    exit_side: String,
    quantity: f64,
    entry_price: f64,
    exit_price: f64,
    entry_probability: f64,
    exit_probability: f64,
    entry_notional_usd: f64,
    exit_notional_usd: f64,
    pnl_usd: f64,
    entry_ts_ms: u64,
    exit_ts_ms: u64,
    duration_ms: u64,
    outcome: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct TraderStateSnapshot {
    ts_ms: u64,
    day_pnl_usd: f64,
    portfolio_realized_pnl_usd: f64,
    portfolio_total_pnl_usd: f64,
    portfolio_all_time_realized_pnl_usd: f64,
    portfolio_drawdown_pct_from_peak: f64,
}

#[derive(Debug, Clone, Serialize)]
struct TraderState {
    ts_ms: u64,
    polymarket_mid: f64,
    polymarket_probability: f64,
    fair_value_probability: f64,
    binance_mid: f64,
    edge_bps: f64,
    net_edge_bps: f64,
    direction: String,
    risk_allowed: bool,
    risk_reason: String,
    action: String,
    position_signal: String,
    entry_signal: bool,
    exit_signal: bool,
    close_reason: String,
    execution_status: String,
    open_positions: usize,
    day_pnl_usd: f64,
    oldest_quote_age_ms: u64,
    portfolio_net_qty: f64,
    portfolio_avg_entry: f64,
    portfolio_realized_pnl_usd: f64,
    portfolio_unrealized_pnl_usd: f64,
    portfolio_total_pnl_usd: f64,
    portfolio_all_time_realized_pnl_usd: f64,
    portfolio_drawdown_pct_from_peak: f64,
}


#[derive(Debug, Clone)]
struct LiveTradingConfig {
    market_id: String,
    up_token_id: String,
    down_token_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let bn_symbol = std::env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let mode = std::env::var("MODE").unwrap_or_else(|_| "paper".to_string());
    let is_live_mode = mode.eq_ignore_ascii_case("live");

    let loop_ms = env_u64("LOOP_INTERVAL_MS", 1_000);
    let min_edge_bps = env_f64("MIN_EDGE_BPS", 8.0);
    let order_notional_usd = env_f64("ORDER_NOTIONAL_USD", 1.0);
    let position_ttl_ms = env_u64("POSITION_TTL_MS", 10_000);

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_state_key = std::env::var("REDIS_TRADER_STATE_KEY")
        .unwrap_or_else(|_| "trader:state:latest".to_string());
    let redis_open_trades_key = std::env::var("REDIS_OPEN_TRADES_KEY")
        .unwrap_or_else(|_| "trader:open_trades:latest".to_string());
    let redis_final_trades_key = std::env::var("REDIS_FINAL_TRADES_KEY")
        .unwrap_or_else(|_| "trader:trades:latest".to_string());
    let redis_live_btc_key = std::env::var("REDIS_LIVE_BTC_KEY")
        .unwrap_or_else(|_| "trader:live_btc:latest".to_string());
    let redis_past_outcomes_key = std::env::var("REDIS_PAST_OUTCOMES_KEY")
        .unwrap_or_else(|_| "trader:past_outcomes:latest".to_string());
    let redis_portfolio_daily_key = std::env::var("REDIS_PORTFOLIO_DAILY_KEY")
        .unwrap_or_else(|_| "trader:portfolio:daily".to_string());
    let trade_history_limit = env_u64("TRADE_HISTORY_LIMIT", 200) as usize;
    let past_outcomes_refresh_ms = env_u64("PAST_OUTCOMES_REFRESH_MS", 15_000).max(1_000);

    let mut polymarket_market_category = std::env::var("POLYMARKET_MARKET_CATEGORY")
        .unwrap_or_else(|_| "crypto".to_string())
        .to_ascii_lowercase();
    let mut polymarket_fees_enabled = env_bool("POLYMARKET_FEES_ENABLED", true);
    if let Ok(meta) = polymarket_ws::resolve_market_metadata_from_env().await {
        if let Some(c) = meta.category {
            polymarket_market_category = c.to_ascii_lowercase();
        }
        if let Some(f) = meta.fees_enabled {
            polymarket_fees_enabled = f;
        }
    }

    let polymarket_fee_rate = match std::env::var("POLYMARKET_FEE_RATE") {
        Ok(raw) => raw
            .parse::<f64>()
            .with_context(|| format!("invalid POLYMARKET_FEE_RATE value: {raw}"))?,
        Err(_) => polymarket_fee_rate_for_category(&polymarket_market_category)?,
    };

    eprintln!(
        "polymarket metadata category={} fees_enabled={} fee_rate={}",
        polymarket_market_category,
        polymarket_fees_enabled,
        polymarket_fee_rate
    );

    let base_signal_cfg = SignalConfig {
        min_edge_bps,
        anchor_price: env_f64("CHAINLINK_ANCHOR_PRICE", env_f64("BINANCE_ANCHOR_PRICE", 100_000.0)),
        basis_bps: env_f64("BINANCE_BASIS_BPS", 0.0),
        target_notional_usd: order_notional_usd,
        microstructure_model_weight: env_f64("BINANCE_SIGNAL_WEIGHT", 0.35),
        polymarket_fees_enabled,
        polymarket_fee_rate,
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

    let configured_account_equity_usd = env_f64("ACCOUNT_EQUITY_USD", 10.0);
    let runtime_wallet_balance_usd = if is_live_mode {
        env_f64(
            "LIVE_WALLET_BALANCE_USD",
            configured_account_equity_usd,
        )
    } else {
        env_f64("DEMO_WALLET_BALANCE_USD", configured_account_equity_usd)
    };
    if (runtime_wallet_balance_usd - configured_account_equity_usd).abs() > 1e-9 {
        eprintln!(
            "wallet/equity mismatch detected: ACCOUNT_EQUITY_USD={} runtime_wallet_balance_usd={} mode={} (using runtime wallet for both sizing and risk)",
            configured_account_equity_usd,
            runtime_wallet_balance_usd,
            mode
        );
    }
    let effective_account_equity_usd = runtime_wallet_balance_usd;

    let kill_cfg = KillSwitchConfig {
        max_drawdown_pct: env_f64("KILL_SWITCH_DRAWDOWN_PCT", 5.0),
        max_error_streak: env_u64("KILL_SWITCH_ERROR_STREAK", 5) as u32,
        max_stale_data_ms: env_u64("KILL_SWITCH_STALE_DATA_MS", 15_000),
    };

    let execution_cfg = ExecutionConfig {
        base_qty: env_f64("ORDER_QTY", 5.0),
        edge_threshold_bps: env_f64("EXECUTION_EDGE_THRESHOLD_BPS", 5.0),
        min_net_edge_margin_bps: env_f64("EXECUTION_MIN_NET_EDGE_MARGIN_BPS", 3.0),
        max_slippage_bps: env_f64("EXECUTION_MAX_SLIPPAGE_BPS", 80.0),
        demo_wallet_balance_usd: effective_account_equity_usd,
        min_order_notional_usd: env_f64("MIN_ORDER_NOTIONAL_USD", 1.0),
        min_order_shares: env_f64("MIN_ORDER_SHARES", 5.0),
        ..ExecutionConfig::default()
    };

    let account_equity_usd = effective_account_equity_usd;
    let realized_vol_bps = env_f64("REALIZED_VOL_BPS", 50.0);

    let pm_client = PolymarketClient::new();
    let _bn = BinanceClient::new();

    let live_order_enabled = is_live_mode && env_bool("POLYMARKET_LIVE_ORDER_ENABLED", true);
    let live_trading_cfg = if live_order_enabled {
        let market_id = polymarket_ws::resolve_market_id_for_live_trading().await?;
        let tokens = polymarket_ws::resolve_outcome_tokens_from_env().await?;
        eprintln!(
            "live order routing enabled market_id={} up_token={} down_token={}",
            market_id,
            tokens.up_token,
            tokens.down_token
        );
        Some(LiveTradingConfig {
            market_id,
            up_token_id: tokens.up_token,
            down_token_id: tokens.down_token,
        })
    } else {
        None
    };
    let (bn_tx, bn_rx) = watch::channel::<Option<Quote>>(None);
    let (pm_tx, pm_rx) = watch::channel::<Option<Quote>>(None);
    let (chainlink_tx, chainlink_rx) = watch::channel::<Option<Quote>>(None);

    spawn_binance_stream(bn_symbol.clone(), bn_tx);
    spawn_polymarket_stream(pm_tx);
    spawn_chainlink_reference_stream(chainlink_tx);
    if is_live_mode {
        spawn_polymarket_user_stream();
    }

    let mut redis_conn: Option<MultiplexedConnection> = None;

    let mut ticker = interval(Duration::from_millis(loop_ms));
    let mut day_pnl_usd = 0.0;
    let mut portfolio_realized_pnl_usd = 0.0;
    let mut pending_positions: Vec<PendingPosition> = Vec::new();
    let mut pending_live_entries: Vec<PendingLiveEntry> = Vec::new();
    let mut trade_history: Vec<CompletedTrade> = Vec::new();
    let live_reconcile_wait_timeout_ms = env_u64("POLYMARKET_LIVE_RECONCILE_WAIT_TIMEOUT_MS", 350);
    let live_reconcile_max_age_ms = env_u64("POLYMARKET_LIVE_RECONCILE_MAX_AGE_MS", 120_000);
    let mut portfolio_daily_history = match load_portfolio_daily_from_redis(
        &redis_url,
        &redis_portfolio_daily_key,
        &mut redis_conn,
    )
    .await
    {
        Ok(history) => history,
        Err(err) => {
            eprintln!("redis portfolio-daily load error: {err:#}");
            HashMap::new()
        }
    };
    let last_trader_state = match load_trader_state_from_redis(
        &redis_url,
        &redis_state_key,
        &mut redis_conn,
    )
    .await
    {
        Ok(state) => state,
        Err(err) => {
            eprintln!("redis trader-state load error: {err:#}");
            None
        }
    };

    let recovered_open_trades = match load_open_trades_from_redis(
        &redis_url,
        &redis_open_trades_key,
        &mut redis_conn,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            eprintln!("redis open-trades load error: {err:#}");
            Vec::new()
        }
    };
    pending_positions = recovered_open_trades
        .into_iter()
        .filter_map(|t| pending_position_from_open_trade(t))
        .collect();

    trade_history = match load_trade_history_from_redis(
        &redis_url,
        &redis_final_trades_key,
        &mut redis_conn,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            eprintln!("redis trade-history load error: {err:#}");
            Vec::new()
        }
    };
    trim_history(&mut trade_history, trade_history_limit);

    if let Some(state) = last_trader_state.as_ref() {
        portfolio_realized_pnl_usd = state
            .portfolio_all_time_realized_pnl_usd
            .max(state.portfolio_realized_pnl_usd);
    }

    let today_key = utc_day_string(now_ms());
    if let Some(today_stats) = portfolio_daily_history.get(&today_key) {
        day_pnl_usd = today_stats.day_pnl_usd;
    } else if let Some(state) = last_trader_state.as_ref() {
        if utc_day_string(state.ts_ms) == today_key {
            day_pnl_usd = state.day_pnl_usd;
        }
    }

    let mut error_streak = 0u32;
    let mut oldest_quote_age_ms = 0u64;
    let mut current_5m_bucket_start_ms: Option<u64> = None;
    let mut current_5m_anchor_price = base_signal_cfg.anchor_price;
    let mut last_past_outcomes_refresh_ms = 0u64;
    let allow_binance_anchor_fallback = env_bool("ALLOW_BINANCE_ANCHOR_FALLBACK", false);
    let mut kill_switch_latched = false;
    let mut transient_kill_blocked_prev = false;
    let mut daily_loss_latched = false;
    let mut current_day_key = today_key.clone();
    let min_peak_pnl_for_drawdown_pct = env_f64("KILL_SWITCH_MIN_PEAK_PNL_USD", 5.0).max(0.0);
    let mut peak_total_pnl_usd = last_trader_state
        .as_ref()
        .map(|s| s.portfolio_total_pnl_usd)
        .unwrap_or(0.0);
    // Do not restore persisted drawdown percentage directly on boot. A stale value can
    // re-latch entries immediately before we recompute drawdown from the current runtime
    // equity curve, leaving the runner stuck in no-new-entries mode after restart.
    let mut current_drawdown_pct = 0.0;

    let initial_open_trades_payload = build_open_trades(&pending_positions);
    if let Err(err) = publish_open_trades_to_redis(
        &redis_url,
        &redis_open_trades_key,
        &initial_open_trades_payload,
        &mut redis_conn,
    )
    .await
    {
        eprintln!("redis open-trades initial publish error: {err:#}");
    }

    if let Err(err) = publish_trades_to_redis(
        &redis_url,
        &redis_final_trades_key,
        &trade_history,
        &mut redis_conn,
    )
    .await
    {
        eprintln!("redis trade-history initial publish error: {err:#}");
    }

    let mut initial_past_outcomes_payload = derive_past_outcomes_payload_from_history(&trade_history, None);
    match polymarket_ws::fetch_market_outcome_payload_from_env().await {
        Ok(official_payload) => {
            initial_past_outcomes_payload = official_payload;
        }
        Err(err) => {
            initial_past_outcomes_payload =
                derive_past_outcomes_payload_from_history(&trade_history, Some(err.to_string()));
        }
    }
    if let Err(err) = publish_past_outcomes_to_redis(
        &redis_url,
        &redis_past_outcomes_key,
        &initial_past_outcomes_payload,
        &mut redis_conn,
    )
    .await
    {
        eprintln!("redis past-outcomes initial publish error: {err:#}");
    }




    loop {
        ticker.tick().await;
        let loop_day_key = utc_day_string(now_ms());
        if loop_day_key != current_day_key {
            current_day_key = loop_day_key.clone();
            day_pnl_usd = portfolio_daily_history
                .get(&loop_day_key)
                .map(|s| s.day_pnl_usd)
                .unwrap_or(0.0);
            if daily_loss_latched {
                eprintln!(
                    "daily loss latch cleared on day rollover (new day: {}, day_pnl_usd={})",
                    loop_day_key,
                    day_pnl_usd
                );
            }
            daily_loss_latched = false;
        }
        let pm_snapshot = pm_rx.borrow().clone();
        let bn_snapshot = bn_rx.borrow().clone();
        let chainlink_snapshot = chainlink_rx.borrow().clone();

        if let Some(clq) = chainlink_snapshot.as_ref() {
            let bucket = five_min_bucket_start_ms(clq.ts_ms);
            if current_5m_bucket_start_ms != Some(bucket) {
                current_5m_bucket_start_ms = Some(bucket);
                if clq.price.is_finite() && clq.price > 0.0 {
                    current_5m_anchor_price = clq.price;
                }
            }
        } else if allow_binance_anchor_fallback {
            if let Some(bnq) = bn_snapshot.as_ref() {
                let bucket = five_min_bucket_start_ms(bnq.ts_ms);
                if current_5m_bucket_start_ms != Some(bucket) {
                    current_5m_bucket_start_ms = Some(bucket);
                    if bnq.price.is_finite() && bnq.price > 0.0 {
                        current_5m_anchor_price = bnq.price;
                    }
                }
            }
        }

        let signal_cfg = SignalConfig {
            anchor_price: current_5m_anchor_price,
            ..base_signal_cfg
        };

        if is_live_mode && !pending_live_entries.is_empty() {
            let mut still_pending: Vec<PendingLiveEntry> = Vec::with_capacity(pending_live_entries.len());
            let mut reconciled_positions: Vec<PendingPosition> = Vec::new();
            for mut pending in pending_live_entries.drain(..) {
                let age_ms = now_ms().saturating_sub(pending.submitted_ts_ms);
                if age_ms >= live_reconcile_max_age_ms {
                    eprintln!(
                        "live entry reconciliation expired id={} age_ms={} status={}",
                        pending.id, age_ms, pending.submit_status
                    );
                    continue;
                }
                match pm_client
                    .wait_for_user_fill(&pending.criteria, live_reconcile_wait_timeout_ms)
                    .await
                {
                    Ok(Some(fill)) => {
                        let matched_qty = fill.matched_size.max(0.0).min(pending.quantity);
                        if matched_qty <= 0.0 {
                            pending.last_reconcile_status = format!(
                                "awaiting_user_ws_fill matched_qty_non_positive fill_status={}",
                                fill.status
                            );
                            still_pending.push(pending);
                            continue;
                        }

                        let entry_ts_ms = fill.ts_ms.max(pending.submitted_ts_ms);
                        eprintln!(
                            "live entry fill reconciled id={} matched_qty={:.6} fill_status={} fill_ts_ms={}",
                            pending.id, matched_qty, fill.status, fill.ts_ms
                        );
                        reconciled_positions.push(PendingPosition {
                            id: pending.id,
                            direction: pending.direction,
                            quantity: matched_qty,
                            entry_price: pending.entry_price,
                            entry_notional_usd: (matched_qty * pending.entry_price).max(0.0),
                            entry_ts_ms,
                            settle_at_ms: pending.settle_at_ms.max(entry_ts_ms.saturating_add(position_ttl_ms)),
                            expected_pnl_usd: pending.expected_pnl_usd,
                        });
                    }
                    Ok(None) => {
                        pending.last_reconcile_status =
                            format!("awaiting_user_ws_fill age_ms={}", age_ms);
                        still_pending.push(pending);
                    }
                    Err(err) => {
                        pending.last_reconcile_status = format!("reconcile_error {err:#}");
                        eprintln!(
                            "live entry reconcile error id={} err={err:#}",
                            pending.id
                        );
                        still_pending.push(pending);
                    }
                }
            }
            pending_live_entries = still_pending;
            if !reconciled_positions.is_empty() {
                pending_positions.extend(reconciled_positions);
            }
        }

        let open_positions = pending_positions.len();
        let effective_open_positions = pending_positions.len() + pending_live_entries.len();
        let open_direction = current_open_direction(&pending_positions).or_else(|| {
            pending_live_entries.first().map(|p| p.direction)
        });
        let current_gross_exposure_usd_filled: f64 = pending_positions
            .iter()
            .map(|p| p.entry_notional_usd.abs())
            .sum();
        let pending_live_exposure_usd: f64 = pending_live_entries
            .iter()
            .map(|p| p.entry_notional_usd.abs())
            .sum();
        let current_gross_exposure_usd = current_gross_exposure_usd_filled + pending_live_exposure_usd;
        let current_largest_filled_position_usd = pending_positions
            .iter()
            .map(|p| p.entry_notional_usd.abs())
            .fold(0.0, f64::max);
        let current_largest_pending_live_usd = pending_live_entries
            .iter()
            .map(|p| p.entry_notional_usd.abs())
            .fold(0.0, f64::max);
        let current_largest_position_usd =
            current_largest_filled_position_usd.max(current_largest_pending_live_usd);

        let kill_state = KillSwitchState {
            current_drawdown_pct,
            error_streak,
            oldest_quote_age_ms,
        };
        let kill_triggered = should_kill(kill_state, kill_cfg);
        let drawdown_breached = current_drawdown_pct >= kill_cfg.max_drawdown_pct;
        let transient_kill_blocked = kill_triggered && !drawdown_breached;

        if drawdown_breached {
            if !kill_switch_latched {
                eprintln!(
                    "kill switch latched drawdown_pct={:.2} error_streak={} stale_ms={} (no-new-entries mode)",
                    current_drawdown_pct,
                    error_streak,
                    oldest_quote_age_ms
                );
            }
            kill_switch_latched = true;
        } else if kill_switch_latched {
            eprintln!(
                "kill switch drawdown latch cleared drawdown_pct={:.2} threshold_pct={:.2}",
                current_drawdown_pct,
                kill_cfg.max_drawdown_pct
            );
            kill_switch_latched = false;
        }

        if transient_kill_blocked && !transient_kill_blocked_prev {
            eprintln!(
                "kill switch temporary block error_streak={} stale_ms={} (entries paused until recovery)",
                error_streak,
                oldest_quote_age_ms
            );
        } else if !transient_kill_blocked && transient_kill_blocked_prev {
            eprintln!("kill switch temporary block cleared (entries resumed)");
        }
        transient_kill_blocked_prev = transient_kill_blocked;

        if day_pnl_usd <= -risk_limits.max_daily_loss_usd {
            if !daily_loss_latched {
                eprintln!(
                    "daily loss latched day_pnl_usd={} limit={} (no-new-entries mode)",
                    day_pnl_usd,
                    -risk_limits.max_daily_loss_usd
                );
            }
            daily_loss_latched = true;
        }

        let entries_latched = kill_switch_latched || daily_loss_latched || transient_kill_blocked;
        let entry_block_reason = if kill_switch_latched {
            Some(format!(
                "kill_switch_latched_drawdown drawdown_pct={:.2} threshold_pct={:.2}",
                current_drawdown_pct, kill_cfg.max_drawdown_pct
            ))
        } else if daily_loss_latched {
            Some(format!(
                "daily_loss_latched day_pnl_usd={:.4} limit_usd={:.4}",
                day_pnl_usd, risk_limits.max_daily_loss_usd
            ))
        } else if transient_kill_blocked {
            Some(format!(
                "kill_switch_temporary error_streak={} stale_ms={} thresholds(error_streak={},stale_ms={})",
                error_streak,
                oldest_quote_age_ms,
                kill_cfg.max_error_streak,
                kill_cfg.max_stale_data_ms
            ))
        } else {
            None
        };

        let iteration = run_iteration(
            &pm_rx,
            &bn_rx,
            chainlink_snapshot.clone(),
            signal_cfg,
            risk_limits,
            execution_cfg,
            order_notional_usd,
            day_pnl_usd,
            effective_open_positions,
            open_direction,
            current_gross_exposure_usd,
            current_largest_position_usd,
            position_ttl_ms,
            account_equity_usd,
            realized_vol_bps,
            kill_cfg.max_stale_data_ms,
            entries_latched,
            &pm_client,
            is_live_mode,
            live_trading_cfg.as_ref(),
        )
        .await;

        match iteration {
            Ok(IterationOutcome {
                position,
                pending_live_submission,
                state,
                close_open_positions,
                close_reason,
            }) => {
                error_streak = 0;
                oldest_quote_age_ms = state.oldest_quote_age_ms;
                let mut state = state;
                if state.action == "entries_blocked_no_new_entries" {
                    if let Some(reason) = entry_block_reason.as_ref() {
                        state.risk_reason = reason.clone();
                        state.execution_status = format!(
                            "skipped:entries_blocked_no_new_entries {}",
                            reason
                        );
                    }
                }

                let mut close_open_positions = close_open_positions;
                if !pending_live_entries.is_empty() {
                    close_open_positions = false;
                }
                if close_open_positions && is_live_mode {
                    if let Some(cfg) = live_trading_cfg.as_ref() {
                        let mut close_errors = Vec::new();
                        for pos in &pending_positions {
                            match submit_live_exit_order(&pm_client, cfg, pos, state.polymarket_mid).await {
                                Ok(LiveSubmitOutcome::Filled { .. }) => {}
                                Ok(LiveSubmitOutcome::Pending { status }) => {
                                    close_errors.push(format!("{}: pending {}", pos.id, status));
                                }
                                Err(err) => {
                                    close_errors.push(format!("{}: {err:#}", pos.id));
                                }
                            }
                        }
                        if !close_errors.is_empty() {
                            close_open_positions = false;
                            state.execution_status = format!(
                                "skipped:live_close_failed {}",
                                close_errors.join(" | ")
                            );
                        }
                    } else {
                        close_open_positions = false;
                        state.execution_status =
                            "skipped:live_close_failed missing live trading config".to_string();
                    }
                }

                if close_open_positions {
                    let close_ts_ms = now_ms();
                    let closed_now = close_positions_now(
                        &mut pending_positions,
                        &mut day_pnl_usd,
                        &mut portfolio_realized_pnl_usd,
                        state.polymarket_mid,
                        close_ts_ms,
                        close_reason.as_deref().unwrap_or("exit_signal"),
                    );
                    if !closed_now.is_empty() {
                        trade_history.extend(closed_now);
                        trim_history(&mut trade_history, trade_history_limit);
                    }
                }

                if let Some(pos) = position {
                    pending_positions.push(pos);
                }
                if let Some(pending_submission) = pending_live_submission {
                    pending_live_entries.push(pending_submission);
                }
                let portfolio_position = build_portfolio_position(&pending_positions);
                let mark_price = state.polymarket_mid.clamp(0.0, 1.0);
                let unrealized_pnl_usd = unrealized_pnl(portfolio_position.avg_price, mark_price, portfolio_position.qty);
                let total_pnl_usd = total_pnl(
                    portfolio_realized_pnl_usd,
                    portfolio_position.avg_price,
                    mark_price,
                    portfolio_position.qty,
                );

                state.open_positions = pending_positions.len() + pending_live_entries.len();
                state.day_pnl_usd = day_pnl_usd;
                state.portfolio_net_qty = portfolio_position.qty;
                state.portfolio_avg_entry = portfolio_position.avg_price;
                state.portfolio_realized_pnl_usd = portfolio_realized_pnl_usd;
                state.portfolio_unrealized_pnl_usd = unrealized_pnl_usd;
                state.portfolio_total_pnl_usd = total_pnl_usd;
                state.portfolio_all_time_realized_pnl_usd = portfolio_realized_pnl_usd;
                if total_pnl_usd > peak_total_pnl_usd {
                    peak_total_pnl_usd = total_pnl_usd;
                }
                current_drawdown_pct =
                    pnl_drawdown_pct_from_peak(total_pnl_usd, peak_total_pnl_usd, min_peak_pnl_for_drawdown_pct);
                state.portfolio_drawdown_pct_from_peak = current_drawdown_pct;
                if !pending_live_entries.is_empty() {
                    let pending = &pending_live_entries[0];
                    state.action = "live_entry_submitted".to_string();
                    state.position_signal = "live_entry_submitted".to_string();
                    state.execution_status = format!(
                        "submitted:awaiting_user_ws_fill id={} submitted_ts_ms={} {}",
                        pending.id, pending.submitted_ts_ms, pending.last_reconcile_status
                    );
                }

                let day_key = utc_day_string(state.ts_ms);
                update_daily_history(
                    &mut portfolio_daily_history,
                    day_key,
                    state.portfolio_total_pnl_usd,
                    state.ts_ms,
                );

                if let Err(err) = publish_portfolio_daily_to_redis(
                    &redis_url,
                    &redis_portfolio_daily_key,
                    &portfolio_daily_history,
                    &mut redis_conn,
                )
                .await
                {
                    eprintln!("redis portfolio-daily publish error: {err:#}");
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


                if let Err(err) = publish_aux_stream_payloads_to_redis(
                    &redis_url,
                    &redis_live_btc_key,
                    chainlink_snapshot.clone(),
                    bn_snapshot.clone(),
                    &mut redis_conn,
                )
                .await
                {
                    eprintln!("redis aux-stream publish error: {err:#}");
                }
            }
            Err(err) => {
                error_streak = error_streak.saturating_add(1);
                eprintln!("iteration error: {err:#}");
            }
        }


        let open_trades_payload = build_open_trades(&pending_positions);
        if let Err(err) = publish_open_trades_to_redis(
            &redis_url,
            &redis_open_trades_key,
            &open_trades_payload,
            &mut redis_conn,
        )
        .await
        {
            eprintln!("redis open-trades publish error: {err:#}");
        }

        if let Err(err) = publish_trades_to_redis(
            &redis_url,
            &redis_final_trades_key,
            &trade_history,
            &mut redis_conn,
        )
        .await
        {
            eprintln!("redis trade-history publish error: {err:#}");
        }

        let now = now_ms();
        if now.saturating_sub(last_past_outcomes_refresh_ms) >= past_outcomes_refresh_ms {
            let past_outcomes_payload = match polymarket_ws::fetch_market_outcome_payload_from_env().await {
                Ok(official_payload) => official_payload,
                Err(err) => derive_past_outcomes_payload_from_history(&trade_history, Some(err.to_string())),
            };

            if let Err(err) = publish_past_outcomes_to_redis(
                &redis_url,
                &redis_past_outcomes_key,
                &past_outcomes_payload,
                &mut redis_conn,
            )
            .await
            {
                eprintln!("redis past-outcomes publish error: {err:#}");
            } else {
                last_past_outcomes_refresh_ms = now;
            }
        }
    }
}

struct IterationOutcome {
    position: Option<PendingPosition>,
    pending_live_submission: Option<PendingLiveEntry>,
    state: TraderState,
    close_open_positions: bool,
    close_reason: Option<String>,
}

#[allow(clippy::too_many_arguments)]
async fn run_iteration(
    pm_rx: &watch::Receiver<Option<Quote>>,
    bn_rx: &watch::Receiver<Option<Quote>>,
    chainlink_quote: Option<Quote>,
    signal_cfg: SignalConfig,
    risk_limits: RiskLimits,
    execution_cfg: ExecutionConfig,
    order_notional_usd: f64,
    day_pnl_usd: f64,
    open_positions: usize,
    open_direction: Option<TradeDirection>,
    current_gross_exposure_usd: f64,
    current_largest_position_usd: f64,
    position_ttl_ms: u64,
    account_equity_usd: f64,
    realized_vol_bps: f64,
    max_quote_age_ms: u64,
    entries_latched: bool,
    pm_client: &PolymarketClient,
    is_live_mode: bool,
    live_trading_cfg: Option<&LiveTradingConfig>,
) -> Result<IterationOutcome> {
    let pm_quote = match pm_rx.borrow().clone() {
        Some(q) => q,
        None => {
            eprintln!("waiting for polymarket quote...");
            let mut state = waiting_state(day_pnl_usd, open_positions);
            state.action = "waiting_polymarket_quote".to_string();
            state.risk_reason = "waiting_polymarket_quote".to_string();
            state.execution_status = "waiting:polymarket_quote_missing".to_string();
            return Ok(IterationOutcome {
                position: None,
                pending_live_submission: None,
                state,
                close_open_positions: false,
                close_reason: None,
            });
        }
    };
    if !is_tradeable_polymarket_quote(&pm_quote) {
        let mut state = waiting_state(day_pnl_usd, open_positions);
        state.action = "waiting_tradeable_polymarket_quote".to_string();
        state.execution_status = format!("waiting:ignored_non_tradeable_venue {}", pm_quote.venue);
        state.oldest_quote_age_ms = now_ms().saturating_sub(pm_quote.ts_ms);
        return Ok(IterationOutcome {
            position: None,
            pending_live_submission: None,
            state,
            close_open_positions: false,
            close_reason: None,
        });
    }

    let bn_quote = match bn_rx.borrow().clone().or(chainlink_quote) {
        Some(q) => q,
        None => {
            eprintln!("waiting for binance quote...");
            let mut state = waiting_state(day_pnl_usd, open_positions);
            state.action = "waiting_binance_quote".to_string();
            state.risk_reason = "waiting_binance_quote".to_string();
            state.execution_status = "waiting:binance_quote_missing".to_string();
            return Ok(IterationOutcome {
                position: None,
                pending_live_submission: None,
                state,
                close_open_positions: false,
                close_reason: None,
            });
        }
    };
    if env_bool("TRADER_DEBUG_QUOTES", false) {
        eprintln!(
            "engine quotes pm={{venue:{} symbol:{} price:{:.4} ts_ms:{}}} bn={{venue:{} symbol:{} price:{:.2} ts_ms:{}}}",
            pm_quote.venue,
            pm_quote.symbol,
            pm_quote.price,
            pm_quote.ts_ms,
            bn_quote.venue,
            bn_quote.symbol,
            bn_quote.price,
            bn_quote.ts_ms
        );
    }

    let signal = compute_signal(&pm_quote, &bn_quote, signal_cfg);
    let fair_value_probability = (pm_quote.price + (signal.edge_bps / 10_000.0)).clamp(0.0, 1.0);
    let now = now_ms();
    let pm_quote_age_ms = now.saturating_sub(pm_quote.ts_ms);
    let bn_quote_age_ms = now.saturating_sub(bn_quote.ts_ms);
    let pm_fresh = pm_quote_age_ms <= max_quote_age_ms;
    let bn_fresh = bn_quote_age_ms <= max_quote_age_ms;

    let has_open_positions = open_positions > 0;
    let reversal_signal = has_open_positions
        && open_direction
            .map(|d| signal.should_trade && signal.direction != TradeDirection::Flat && signal.direction != d)
            .unwrap_or(false);

    let top_book_liquidity_usd = env_f64("TOP_BOOK_LIQUIDITY_USD", 5_000.0);
    let no_entry_first_seconds_5m = env_u64("NO_ENTRY_FIRST_SECONDS_5M", 3).min(120);
    let blocked_first_seconds_5m = is_first_seconds_of_five_min_window(now, no_entry_first_seconds_5m);
    let no_entry_last_seconds_5m = env_u64("NO_ENTRY_LAST_SECONDS_5M", 15).min(299);
    let blocked_last_seconds_5m = is_last_seconds_of_five_min_window(now, no_entry_last_seconds_5m);
    let risk_capped_notional_usd = capped_entry_notional_usd(
        order_notional_usd,
        current_gross_exposure_usd,
        risk_limits,
        account_equity_usd,
    );

    let can_open_new_position = !has_open_positions && !entries_latched;
    let candidate_intent = if can_open_new_position
        && pm_fresh
        && bn_fresh
        && !blocked_first_seconds_5m
        && !blocked_last_seconds_5m
    {
        order_manager::build_order_intent(
            "BTC",
            &signal,
            pm_quote.price,
            risk_capped_notional_usd,
            top_book_liquidity_usd,
            realized_vol_bps,
            execution_cfg,
        )
    } else {
        None
    };

    let candidate_notional_usd = candidate_intent
        .as_ref()
        .map(|o| o.quantity * o.limit_price)
        .unwrap_or(risk_capped_notional_usd);

    let risk = if can_open_new_position {
        let projected_gross_exposure_usd = current_gross_exposure_usd + candidate_notional_usd;
        let projected_largest_position_usd = current_largest_position_usd.max(candidate_notional_usd);
        evaluate_trade(
            RiskInput {
                notional_usd: candidate_notional_usd,
                day_pnl_usd,
                open_positions,
                gross_exposure_usd: projected_gross_exposure_usd,
                account_equity_usd,
                realized_vol_bps,
                largest_position_usd: projected_largest_position_usd,
            },
            risk_limits,
        )
    } else if entries_latched && !has_open_positions {
        RiskDecision {
            allowed: false,
            reason: "entries blocked by kill switch".to_string(),
            violations: Vec::new(),
        }
    } else {
        RiskDecision {
            allowed: true,
            reason: "existing position managed by signal/quote freshness".to_string(),
            violations: Vec::new(),
        }
    };

    let intent = if can_open_new_position && risk.allowed {
        candidate_intent
    } else {
        None
    };
    let submitted_intent = intent.clone();
    let filled_notional_usd = intent
        .as_ref()
        .map(|o| o.quantity * o.limit_price)
        .unwrap_or(0.0);
    let mut report = order_manager::submit_if_needed(intent);
    let mut live_filled_qty: Option<f64> = None;
    let mut pending_live_submission: Option<PendingLiveEntry> = None;
    if report.accepted && is_live_mode {
        match (submitted_intent.as_ref(), live_trading_cfg) {
            (Some(order), Some(cfg)) => match submit_live_entry_order(pm_client, cfg, order).await {
                Ok(LiveSubmitOutcome::Filled { status, matched_qty }) => {
                    live_filled_qty = Some(matched_qty.max(0.0));
                    report.status = format!("live_order_filled {}", status);
                }
                Ok(LiveSubmitOutcome::Pending { status }) => {
                    report.status = format!("live_order_submitted {}", status);
                    let submitted_ts_ms = now_ms();
                    let expected_pnl_usd =
                        (order.quantity * order.limit_price) * (signal.net_edge_bps / 10_000.0);
                    let live_min_fill_size = env_f64("POLYMARKET_LIVE_MIN_FILL_SIZE", 0.001).max(0.0);
                    match token_id_for_direction(cfg, order.direction) {
                        Ok(token_id) => {
                            pending_live_submission = Some(PendingLiveEntry {
                                id: format!("BTC-{}", submitted_ts_ms),
                                direction: order.direction,
                                quantity: order.quantity,
                                entry_price: order.limit_price,
                                entry_notional_usd: (order.quantity * order.limit_price).max(0.0),
                                expected_pnl_usd,
                                submitted_ts_ms,
                                settle_at_ms: submitted_ts_ms.saturating_add(position_ttl_ms),
                                criteria: LiveFillCriteria {
                                    market_id: cfg.market_id.clone(),
                                    token_id: token_id.to_string(),
                                    side: LiveOrderSide::Buy,
                                    min_size: live_min_fill_size,
                                },
                                submit_status: status,
                                last_reconcile_status: "awaiting_user_ws_fill".to_string(),
                            });
                        }
                        Err(err) => {
                            report.accepted = false;
                            report.status = format!("skipped:live_order_failed {err:#}");
                        }
                    }
                }
                Err(err) => {
                    report.accepted = false;
                    report.status = format!("skipped:live_order_failed {err:#}");
                }
            },
            _ => {
                report.accepted = false;
                report.status = "skipped:live_order_config_missing".to_string();
            }
        }
    } else if report.accepted && !is_live_mode {
        report.status = format!(
            "paper_order_filled order_status=matched trade_status=CONFIRMED qty={} price={} notional={}",
            submitted_intent.as_ref().map(|o| o.quantity).unwrap_or(0.0),
            submitted_intent.as_ref().map(|o| o.limit_price).unwrap_or(0.0),
            submitted_intent
                .as_ref()
                .map(|o| o.quantity * o.limit_price)
                .unwrap_or(0.0)
        );
    }
    if !report.accepted && entries_latched && !has_open_positions {
        report.status = "skipped:entries_blocked_no_new_entries".to_string();
    } else if !report.accepted && can_open_new_position && blocked_first_seconds_5m {
        report.status = format!(
            "skipped:first_seconds_5m_guard elapsed_s<{}",
            no_entry_first_seconds_5m
        );
    } else if !report.accepted && can_open_new_position && blocked_last_seconds_5m {
        report.status = format!(
            "skipped:last_seconds_5m_guard remaining_s<={}",
            no_entry_last_seconds_5m
        );
    } else if !report.accepted
        && can_open_new_position
        && risk_capped_notional_usd < execution_cfg.min_order_notional_usd
    {
        report.status = format!(
            "skipped:risk_budget_too_small capped_notional={} min_required={}",
            risk_capped_notional_usd,
            execution_cfg.min_order_notional_usd
        );
    } else if !report.accepted && can_open_new_position && !risk.allowed {
        report.status = format!("skipped:risk_rejected {}", risk.reason);
    }

    let live_fill_confirmed = is_live_mode && live_filled_qty.is_some();
    let live_entry_submitted = is_live_mode && report.accepted && submitted_intent.is_some() && !live_fill_confirmed;
    let entry_signal = can_open_new_position && signal.should_trade && risk.allowed && report.accepted && (!is_live_mode || live_fill_confirmed);
    let hold_strong_edge_bps = env_f64("HOLD_STRONG_EDGE_BPS", (signal_cfg.min_edge_bps * 0.6).max(1.0));
    let invalidation_close_bps =
        env_f64("INVALIDATION_CLOSE_BPS", (signal_cfg.min_edge_bps * 0.8).max(4.0));
    let same_direction_hold = has_open_positions
        && open_direction
            .map(|d| signal.direction == d && signal.direction != TradeDirection::Flat)
            .unwrap_or(false);
    let strong_same_direction = same_direction_hold && signal.net_edge_bps >= hold_strong_edge_bps;
    let invalidation_deadband_hold = has_open_positions
        && !reversal_signal
        && !signal.should_trade
        && signal.net_edge_bps.abs() < invalidation_close_bps;

    let no_trade_signal = signal.direction == TradeDirection::Flat;
    let weak_edge_invalidation = signal.net_edge_bps.abs() < invalidation_close_bps;

    let seconds_left_in_5m = 300_u64.saturating_sub((now / 1_000) % 300);
    let strong_win_zone = has_open_positions
        && seconds_left_in_5m <= env_u64("NEAR_CERTAINTY_HOLD_SECONDS", 15)
        && same_direction_hold
        && signal.net_edge_bps.abs() >= hold_strong_edge_bps;

    if !report.accepted && has_open_positions && strong_same_direction {
        report.status = format!(
            "hold:strong_direction net_edge_bps={:.2} hold_threshold_bps={:.2}",
            signal.net_edge_bps, hold_strong_edge_bps
        );
    } else if !report.accepted && invalidation_deadband_hold {
        report.status = format!(
            "hold:invalidation_deadband net_edge_bps={:.2} close_threshold_bps={:.2}",
            signal.net_edge_bps, invalidation_close_bps
        );
    }

    let mut close_reason = if reversal_signal {
        Some("reverse_signal".to_string())
    } else if has_open_positions && !same_direction_hold && (weak_edge_invalidation || no_trade_signal) {
        Some("signal_invalidated".to_string())
    } else if has_open_positions && invalidation_deadband_hold {
        None
    } else {
        None
    };

    if strong_win_zone {
        close_reason = None;
    }
    let exit_signal = close_reason.is_some();

    let position_signal = if live_entry_submitted {
        "live_entry_submitted".to_string()
    } else if entry_signal {
        match signal.direction {
            TradeDirection::Up => "open_up",
            TradeDirection::Down => "open_down",
            TradeDirection::Flat => "open_flat",
        }
        .to_string()
    } else if has_open_positions && strong_same_direction {
        "hold_strong_direction".to_string()
    } else if exit_signal {
        match open_direction.unwrap_or(TradeDirection::Flat) {
            TradeDirection::Up => "close_up",
            TradeDirection::Down => "close_down",
            TradeDirection::Flat => "close",
        }
        .to_string()
    } else {
        "hold".to_string()
    };

    let action = if !pm_fresh {
        "hold_stale_polymarket"
    } else if !bn_fresh {
        "hold_stale_binance_depth"
    } else if entries_latched && !has_open_positions {
        "entries_blocked_no_new_entries"
    } else if blocked_first_seconds_5m && can_open_new_position {
        "hold_first_seconds_5m_guard"
    } else if blocked_last_seconds_5m && can_open_new_position {
        "hold_last_seconds_5m_guard"
    } else if has_open_positions && strong_win_zone {
        "hold_near_certainty"
    } else if has_open_positions && same_direction_hold {
        "hold_to_expiry"
    } else if has_open_positions && strong_same_direction {
        "hold_strong_direction"
    } else if reversal_signal {
        "close_on_reversal"
    } else if live_entry_submitted {
        "live_entry_submitted"
    } else if entry_signal {
        "entry_signal"
    } else if exit_signal {
        "exit_signal"
    } else {
        "hold"
    };

    let oldest_quote_age_ms = pm_quote_age_ms.max(bn_quote_age_ms);

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
        polymarket_probability: pm_quote.price.clamp(0.0, 1.0),
        fair_value_probability,
        binance_mid: bn_quote.price,
        edge_bps: signal.edge_bps,
        net_edge_bps: signal.net_edge_bps,
        direction: format!("{:?}", signal.direction),
        risk_allowed: risk.allowed,
        risk_reason: risk.reason.clone(),
        action: action.to_string(),
        position_signal: position_signal.clone(),
        entry_signal,
        exit_signal,
        close_reason: close_reason.clone().unwrap_or_else(|| "none".to_string()),
        execution_status: report.status.clone(),
        open_positions,
        day_pnl_usd,
        oldest_quote_age_ms,
        portfolio_net_qty: 0.0,
        portfolio_avg_entry: 0.0,
        portfolio_realized_pnl_usd: 0.0,
        portfolio_unrealized_pnl_usd: 0.0,
        portfolio_total_pnl_usd: day_pnl_usd,
        portfolio_all_time_realized_pnl_usd: 0.0,
        portfolio_drawdown_pct_from_peak: 0.0,
    };

    if live_entry_submitted {
        let mut live_state = state;
        live_state.action = "live_entry_submitted".to_string();
        live_state.position_signal = "live_entry_submitted".to_string();
        live_state.execution_status = format!("submitted:awaiting_user_ws_fill {}", report.status);
        return Ok(IterationOutcome {
            position: None,
            pending_live_submission,
            state: live_state,
            close_open_positions: false,
            close_reason: None,
        });
    }

    if report.accepted {
        let expected_pnl_usd = filled_notional_usd * (signal.net_edge_bps / 10_000.0);
        if let Some(order) = submitted_intent {
            let ts = now_ms();
            return Ok(IterationOutcome {
                position: Some(PendingPosition {
                    id: format!("BTC-{}", ts),
                    direction: order.direction,
                    quantity: live_filled_qty.unwrap_or(order.quantity),
                    entry_price: order.limit_price,
                    entry_notional_usd: live_filled_qty.unwrap_or(order.quantity) * order.limit_price,
                    entry_ts_ms: ts,
                    settle_at_ms: ts.saturating_add(position_ttl_ms),
                    expected_pnl_usd,
                }),
                pending_live_submission: None,
                state,
                close_open_positions: false,
                close_reason: None,
            });
        }
    }

    Ok(IterationOutcome {
        position: None,
        pending_live_submission,
        state,
        close_open_positions: exit_signal,
        close_reason,
    })
}


async fn load_portfolio_daily_from_redis(
    redis_url: &str,
    key: &str,
    conn: &mut Option<MultiplexedConnection>,
) -> Result<HashMap<String, PortfolioDayStats>> {
    if conn.is_none() {
        let client = redis::Client::open(redis_url).context("invalid redis url")?;
        let c = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to redis")?;
        *conn = Some(c);
    }

    if let Some(c) = conn.as_mut() {
        let get_res: redis::RedisResult<Option<String>> = c.get(key).await;
        match get_res {
            Ok(Some(raw)) => {
                let parsed = serde_json::from_str::<HashMap<String, PortfolioDayStats>>(&raw)
                    .context("deserialize portfolio daily history")?;
                Ok(parsed)
            }
            Ok(None) => Ok(HashMap::new()),
            Err(e) => {
                *conn = None;
                Err(anyhow!("redis get portfolio daily failed: {e}"))
            }
        }
    } else {
        Ok(HashMap::new())
    }
}

async fn load_trader_state_from_redis(
    redis_url: &str,
    key: &str,
    conn: &mut Option<MultiplexedConnection>,
) -> Result<Option<TraderStateSnapshot>> {
    if conn.is_none() {
        let client = redis::Client::open(redis_url).context("invalid redis url")?;
        let c = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to redis")?;
        *conn = Some(c);
    }

    if let Some(c) = conn.as_mut() {
        let get_res: redis::RedisResult<Option<String>> = c.get(key).await;
        match get_res {
            Ok(Some(raw)) => {
                let parsed = serde_json::from_str::<TraderStateSnapshot>(&raw)
                    .context("deserialize trader state snapshot")?;
                Ok(Some(parsed))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                *conn = None;
                Err(anyhow!("redis get trader state failed: {e}"))
            }
        }
    } else {
        Ok(None)
    }
}

async fn load_open_trades_from_redis(
    redis_url: &str,
    key: &str,
    conn: &mut Option<MultiplexedConnection>,
) -> Result<Vec<OpenTrade>> {
    if conn.is_none() {
        let client = redis::Client::open(redis_url).context("invalid redis url")?;
        let c = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to redis")?;
        *conn = Some(c);
    }

    if let Some(c) = conn.as_mut() {
        let get_res: redis::RedisResult<Option<String>> = c.get(key).await;
        match get_res {
            Ok(Some(raw)) => {
                let parsed = serde_json::from_str::<Vec<OpenTrade>>(&raw)
                    .context("deserialize open trades snapshot")?;
                Ok(parsed)
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => {
                *conn = None;
                Err(anyhow!("redis get open trades failed: {e}"))
            }
        }
    } else {
        Ok(Vec::new())
    }
}

async fn load_trade_history_from_redis(
    redis_url: &str,
    key: &str,
    conn: &mut Option<MultiplexedConnection>,
) -> Result<Vec<CompletedTrade>> {
    if conn.is_none() {
        let client = redis::Client::open(redis_url).context("invalid redis url")?;
        let c = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to redis")?;
        *conn = Some(c);
    }

    if let Some(c) = conn.as_mut() {
        let get_res: redis::RedisResult<Option<String>> = c.get(key).await;
        match get_res {
            Ok(Some(raw)) => {
                let parsed = serde_json::from_str::<Vec<CompletedTrade>>(&raw)
                    .context("deserialize trade-history snapshot")?;
                Ok(parsed)
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => {
                *conn = None;
                Err(anyhow!("redis get trade-history failed: {e}"))
            }
        }
    } else {
        Ok(Vec::new())
    }
}

fn pending_position_from_open_trade(t: OpenTrade) -> Option<PendingPosition> {
    let direction = parse_trade_direction(&t.direction)?;
    Some(PendingPosition {
        id: t.id,
        direction,
        quantity: t.quantity,
        entry_price: t.entry_price,
        entry_notional_usd: t.entry_notional_usd,
        entry_ts_ms: t.entry_ts_ms,
        settle_at_ms: t.settle_at_ms,
        expected_pnl_usd: t.expected_pnl_usd,
    })
}

fn parse_trade_direction(v: &str) -> Option<TradeDirection> {
    match v.trim().to_ascii_lowercase().as_str() {
        "up" => Some(TradeDirection::Up),
        "down" => Some(TradeDirection::Down),
        "flat" => Some(TradeDirection::Flat),
        _ => None,
    }
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

async fn publish_trades_to_redis(
    redis_url: &str,
    key: &str,
    trades: &[CompletedTrade],
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

    let payload = serde_json::to_string(trades).context("serialize trade history")?;

    if let Some(c) = conn.as_mut() {
        let set_res: redis::RedisResult<()> = c.set(key, payload).await;
        if let Err(e) = set_res {
            *conn = None;
            return Err(anyhow!("redis set trades failed: {e}"));
        }
    }

    Ok(())
}


async fn publish_open_trades_to_redis(
    redis_url: &str,
    key: &str,
    trades: &[OpenTrade],
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

    let payload = serde_json::to_string(trades).context("serialize open trades")?;

    if let Some(c) = conn.as_mut() {
        let set_res: redis::RedisResult<()> = c.set(key, payload).await;
        if let Err(e) = set_res {
            *conn = None;
            return Err(anyhow!("redis set open trades failed: {e}"));
        }
    }

    Ok(())
}

async fn publish_portfolio_daily_to_redis(
    redis_url: &str,
    key: &str,
    daily: &HashMap<String, PortfolioDayStats>,
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

    let payload = serde_json::to_string(daily).context("serialize portfolio daily history")?;

    if let Some(c) = conn.as_mut() {
        let set_res: redis::RedisResult<()> = c.set(key, payload).await;
        if let Err(e) = set_res {
            *conn = None;
            return Err(anyhow!("redis set portfolio daily failed: {e}"));
        }
    }

    Ok(())
}
async fn publish_aux_stream_payloads_to_redis(
    redis_url: &str,
    live_btc_key: &str,
    chainlink_quote: Option<Quote>,
    bn_quote: Option<Quote>,
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

    let live_btc_payload = if let Some(q) = chainlink_quote {
        json!({
            "source": q.venue,
            "symbol": q.symbol,
            "price": q.price,
            "ts_ms": q.ts_ms
        })
    } else if let Some(q) = bn_quote {
        json!({
            "source": q.venue,
            "symbol": q.symbol,
            "price": q.price,
            "ts_ms": q.ts_ms
        })
    } else {
        json!({ "warning": "live btc unavailable" })
    };

    if let Some(c) = conn.as_mut() {
        let live_payload = serde_json::to_string(&live_btc_payload).context("serialize live btc payload")?;

        let live_set: redis::RedisResult<()> = c.set(live_btc_key, live_payload).await;
        if let Err(e) = live_set {
            *conn = None;
            return Err(anyhow!("redis set live btc failed: {e}"));
        }
    }

    Ok(())
}

async fn publish_past_outcomes_to_redis(
    redis_url: &str,
    key: &str,
    payload: &serde_json::Value,
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

    if let Some(c) = conn.as_mut() {
        let encoded = serde_json::to_string(payload).context("serialize past outcomes payload")?;
        let set_res: redis::RedisResult<()> = c.set(key, encoded).await;
        if let Err(e) = set_res {
            *conn = None;
            return Err(anyhow!("redis set past outcomes failed: {e}"));
        }
    }

    Ok(())
}

fn derive_past_outcomes_payload_from_history(
    trade_history: &[CompletedTrade],
    warning: Option<String>,
) -> serde_json::Value {
    let mut sample_size = 0usize;
    let mut up_trades = 0usize;
    let mut down_trades = 0usize;
    let mut sum_probability_up = 0.0_f64;

    for trade in trade_history {
        let mut p_up = trade.exit_probability;
        if !p_up.is_finite() {
            continue;
        }
        p_up = p_up.clamp(0.0, 1.0);

        let direction = trade.direction.trim().to_ascii_lowercase();
        if direction == "up" {
            up_trades += 1;
        } else if direction == "down" {
            down_trades += 1;
            p_up = (1.0 - p_up).clamp(0.0, 1.0);
        } else {
            continue;
        }

        sum_probability_up += p_up;
        sample_size += 1;
    }

    let probability_up = if sample_size > 0 {
        sum_probability_up / sample_size as f64
    } else {
        0.5
    };
    let probability_down = (1.0 - probability_up).clamp(0.0, 1.0);

    let mut payload = json!({
        "probability_up": probability_up,
        "probability_down": probability_down,
        "sample_size": sample_size,
        "up_trades": up_trades,
        "down_trades": down_trades,
        "source": "derived_from_completed_trades",
        "ts_ms": now_ms(),
    });

    if let Some(w) = warning {
        let trimmed = w.trim();
        if !trimmed.is_empty() {
            payload["warning"] = json!(trimmed);
        }
    }

    payload
}
fn waiting_state(day_pnl_usd: f64, open_positions: usize) -> TraderState {
    TraderState {
        ts_ms: now_ms(),
        polymarket_mid: 0.0,
        polymarket_probability: 0.0,
        fair_value_probability: 0.0,
        binance_mid: 0.0,
        edge_bps: 0.0,
        net_edge_bps: 0.0,
        direction: "Flat".to_string(),
        risk_allowed: false,
        risk_reason: "waiting_quotes".to_string(),
        action: "waiting_quotes".to_string(),
        position_signal: "waiting".to_string(),
        entry_signal: false,
        exit_signal: false,
        close_reason: "none".to_string(),
        execution_status: "waiting".to_string(),
        open_positions,
        day_pnl_usd,
        oldest_quote_age_ms: 0,
        portfolio_net_qty: 0.0,
        portfolio_avg_entry: 0.0,
        portfolio_realized_pnl_usd: 0.0,
        portfolio_unrealized_pnl_usd: 0.0,
        portfolio_total_pnl_usd: day_pnl_usd,
        portfolio_all_time_realized_pnl_usd: 0.0,
        portfolio_drawdown_pct_from_peak: 0.0,
    }
}

fn pnl_drawdown_pct_from_peak(current_total_pnl_usd: f64, peak_total_pnl_usd: f64, min_peak_pnl_usd: f64) -> f64 {
    if !current_total_pnl_usd.is_finite() || !peak_total_pnl_usd.is_finite() {
        return 0.0;
    }
    if peak_total_pnl_usd <= 0.0 || peak_total_pnl_usd < min_peak_pnl_usd {
        return 0.0;
    }

    let giveback_usd = (peak_total_pnl_usd - current_total_pnl_usd).max(0.0);
    (giveback_usd / peak_total_pnl_usd) * 100.0
}

fn build_open_trades(pending_positions: &[PendingPosition]) -> Vec<OpenTrade> {
    pending_positions
        .iter()
        .map(|p| {
            let (direction, entry_side) = match p.direction {
                TradeDirection::Up => ("Up".to_string(), "buy".to_string()),
                TradeDirection::Down => ("Down".to_string(), "sell".to_string()),
                TradeDirection::Flat => ("Flat".to_string(), "none".to_string()),
            };
            OpenTrade {
                id: p.id.clone(),
                pair: "BTC".to_string(),
                direction,
                entry_side,
                quantity: p.quantity,
                entry_price: p.entry_price,
                entry_probability: p.entry_price,
                entry_notional_usd: p.entry_notional_usd,
                entry_ts_ms: p.entry_ts_ms,
                settle_at_ms: p.settle_at_ms,
                expected_pnl_usd: p.expected_pnl_usd,
            }
        })
        .collect()
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


fn spawn_chainlink_reference_stream(tx: watch::Sender<Option<Quote>>) {
    tokio::spawn(async move {
        loop {
            match polymarket_ws::stream_chainlink_btc_reference().await {
                Ok(q) => {
                    let _ = tx.send(Some(q));
                }
                Err(err) => {
                    eprintln!("chainlink btc reference stream error: {err:#}");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });
}

fn spawn_polymarket_stream(tx: watch::Sender<Option<Quote>>) {
    tokio::spawn(async move {
        let mut active_instrument: Option<String> = None;
        let mut active_source: Option<String> = None;

        loop {
            if active_instrument.is_none() {
                let resolved = match polymarket_ws::resolve_instrument_from_env().await {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("polymarket resolve error: {err:#}");
                        sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                };
                active_instrument = Some(resolved.instrument_id);
                active_source = Some(resolved.source);
            }

            let instrument_id = match active_instrument.as_ref() {
                Some(v) => v.clone(),
                None => {
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            let source = active_source
                .clone()
                .unwrap_or_else(|| "unknown_source".to_string());
            eprintln!(
                "polymarket market subscribe instrument={} source={}",
                instrument_id, source
            );

            let tx_clone = tx.clone();
            let instrument_for_task = instrument_id.clone();
            let stream_task = tokio::spawn(async move {
                polymarket_ws::run_market_quote_stream(&instrument_for_task, tx_clone).await
            });
            match stream_task.await {
                Ok(Ok(())) => {
                    let prev_bucket = extract_5m_bucket(&source).or_else(|| extract_5m_bucket(&instrument_id));
                    eprintln!(
                        "polymarket market resolved instrument={} bucket={:?}; moving to next market",
                        instrument_id, prev_bucket
                    );
                    active_instrument = None;
                    active_source = None;
                }
                Ok(Err(err)) => {
                    eprintln!(
                        "polymarket stream error for active instrument={} (will retry same market): {err:#}",
                        instrument_id
                    );
                }
                Err(err) => {
                    eprintln!(
                        "polymarket stream task join error for active instrument={} (will retry same market): {err}",
                        instrument_id
                    );
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
    });
}

fn spawn_polymarket_user_stream() {
    tokio::spawn(async move {
        loop {
            let market_id = match polymarket_ws::resolve_market_id_for_live_trading().await {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("polymarket user stream market-id resolve error: {err:#}");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            eprintln!("polymarket user stream subscribe market_id={}", market_id);
            match polymarket_ws::run_user_event_stream(&market_id).await {
                Ok(()) => {}
                Err(err) => eprintln!("polymarket user stream error: {err:#}"),
            }
            sleep(Duration::from_millis(750)).await;
        }
    });
}

fn extract_5m_bucket(value: &str) -> Option<i64> {
    let token = value.split('-').next_back()?.trim();
    token.parse::<i64>().ok()
}

fn build_portfolio_position(pending_positions: &[PendingPosition]) -> Position {
    let mut pos = Position::default();

    for p in pending_positions {
        let fill_qty = signed_fill_qty(p.direction, p.quantity);
        update_position(&mut pos, fill_qty, p.entry_price);
    }

    pos
}

fn signed_fill_qty(direction: TradeDirection, quantity: f64) -> f64 {
    match direction {
        TradeDirection::Up => quantity,
        TradeDirection::Down => -quantity,
        TradeDirection::Flat => 0.0,
    }
}

fn current_open_direction(pending_positions: &[PendingPosition]) -> Option<TradeDirection> {
    pending_positions.first().map(|p| p.direction)
}

fn is_tradeable_polymarket_quote(q: &Quote) -> bool {
    q.venue == "polymarket"
}

fn close_positions_now(
    pending_positions: &mut Vec<PendingPosition>,
    day_pnl_usd: &mut f64,
    portfolio_realized_pnl_usd: &mut f64,
    close_price: f64,
    close_ts_ms: u64,
    reason: &str,
) -> Vec<CompletedTrade> {
    let mut closed = Vec::with_capacity(pending_positions.len());

    for pos in pending_positions.drain(..) {
        let pnl_usd = position_pnl_usd(pos.direction, pos.quantity, pos.entry_price, close_price);
        *day_pnl_usd += pnl_usd;
        *portfolio_realized_pnl_usd += pnl_usd;
        let exit_notional_usd = (close_price * pos.quantity).max(0.0);

        closed.push(CompletedTrade {
            id: pos.id,
            pair: "BTC".to_string(),
            direction: format!("{:?}", pos.direction),
            entry_side: entry_side_for_direction(pos.direction).to_string(),
            exit_side: exit_side_for_direction(pos.direction).to_string(),
            quantity: pos.quantity,
            entry_price: pos.entry_price,
            exit_price: close_price,
            entry_probability: pos.entry_price.clamp(0.0, 1.0),
            exit_probability: close_price.clamp(0.0, 1.0),
            entry_notional_usd: pos.entry_notional_usd,
            exit_notional_usd,
            pnl_usd,
            entry_ts_ms: pos.entry_ts_ms,
            exit_ts_ms: close_ts_ms,
            duration_ms: close_ts_ms.saturating_sub(pos.entry_ts_ms),
            outcome: format!(
                "{}:{}",
                if pnl_usd >= 0.0 { "profit" } else { "loss" },
                reason
            ),
        });
    }

    closed
}

fn entry_side_for_direction(direction: TradeDirection) -> &'static str {
    match direction {
        TradeDirection::Up => "buy",
        TradeDirection::Down => "sell",
        TradeDirection::Flat => "flat",
    }
}

fn exit_side_for_direction(direction: TradeDirection) -> &'static str {
    match direction {
        TradeDirection::Up => "sell",
        TradeDirection::Down => "buy",
        TradeDirection::Flat => "flat",
    }
}
fn position_pnl_usd(direction: TradeDirection, quantity: f64, entry_price: f64, exit_price: f64) -> f64 {
    let entry = entry_price.clamp(0.0, 1.0);
    let exit = exit_price.clamp(0.0, 1.0);
    match direction {
        TradeDirection::Up => quantity * (exit - entry),
        TradeDirection::Down => quantity * (entry - exit),
        TradeDirection::Flat => 0.0,
    }
}

#[derive(Debug, Clone)]
enum LiveSubmitOutcome {
    Filled { status: String, matched_qty: f64 },
    Pending { status: String },
}

async fn submit_live_entry_order(
    pm_client: &PolymarketClient,
    cfg: &LiveTradingConfig,
    order: &OrderIntent,
) -> Result<LiveSubmitOutcome> {
    let live_min_fill_size = env_f64("POLYMARKET_LIVE_MIN_FILL_SIZE", 0.001).max(0.0);
    let token_id = token_id_for_direction(cfg, order.direction)?;
    let receipt = pm_client
        .place_limit_order_live(LiveLimitOrderRequest {
            market_id: cfg.market_id.clone(),
            token_id: token_id.to_string(),
            price: order.limit_price.clamp(0.001, 0.999),
            size: order.quantity,
            side: LiveOrderSide::Buy,
        })
        .await?;

    let wait_timeout_ms = env_u64("POLYMARKET_LIVE_FILL_WAIT_TIMEOUT_MS", 2_000);
    let fill = pm_client
        .wait_for_user_fill(
            &LiveFillCriteria {
                market_id: cfg.market_id.clone(),
                token_id: token_id.to_string(),
                side: LiveOrderSide::Buy,
                min_size: live_min_fill_size,
            },
            wait_timeout_ms,
        )
        .await?;

    Ok(match fill {
        Some(f) => LiveSubmitOutcome::Filled {
            status: format!("{} fill_status={} fill_ts_ms={}", receipt.status, f.status, f.ts_ms),
            matched_qty: f.matched_size.max(0.0),
        },
        None => LiveSubmitOutcome::Pending {
            status: format!("{} no_fill_within_{}ms", receipt.status, wait_timeout_ms),
        },
    })
}

async fn submit_live_exit_order(
    pm_client: &PolymarketClient,
    cfg: &LiveTradingConfig,
    pos: &PendingPosition,
    mark_price: f64,
) -> Result<LiveSubmitOutcome> {
    let live_min_fill_size = env_f64("POLYMARKET_LIVE_MIN_FILL_SIZE", 0.001).max(0.0);
    let token_id = token_id_for_direction(cfg, pos.direction)?;
    let receipt = pm_client
        .place_limit_order_live(LiveLimitOrderRequest {
            market_id: cfg.market_id.clone(),
            token_id: token_id.to_string(),
            price: mark_price.clamp(0.001, 0.999),
            size: pos.quantity,
            side: LiveOrderSide::Sell,
        })
        .await?;

    let wait_timeout_ms = env_u64("POLYMARKET_LIVE_FILL_WAIT_TIMEOUT_MS", 2_000);
    let fill = pm_client
        .wait_for_user_fill(
            &LiveFillCriteria {
                market_id: cfg.market_id.clone(),
                token_id: token_id.to_string(),
                side: LiveOrderSide::Sell,
                min_size: live_min_fill_size,
            },
            wait_timeout_ms,
        )
        .await?;

    Ok(match fill {
        Some(f) => LiveSubmitOutcome::Filled {
            status: format!("{} fill_status={} fill_ts_ms={}", receipt.status, f.status, f.ts_ms),
            matched_qty: f.matched_size.max(0.0),
        },
        None => LiveSubmitOutcome::Pending {
            status: format!("{} no_fill_within_{}ms", receipt.status, wait_timeout_ms),
        },
    })
}

fn token_id_for_direction<'a>(cfg: &'a LiveTradingConfig, direction: TradeDirection) -> Result<&'a str> {
    match direction {
        TradeDirection::Up => Ok(cfg.up_token_id.as_str()),
        TradeDirection::Down => Ok(cfg.down_token_id.as_str()),
        TradeDirection::Flat => Err(anyhow!("flat direction has no live token mapping")),
    }
}

fn polymarket_fee_rate_for_category(category: &str) -> Result<f64> {
    match category.trim().to_ascii_lowercase().as_str() {
        "crypto" => env_f64_required("POLYMARKET_FEE_RATE_CRYPTO"),
        "sports" => env_f64_required("POLYMARKET_FEE_RATE_SPORTS"),
        "finance" => env_f64_required("POLYMARKET_FEE_RATE_FINANCE"),
        "politics" => env_f64_required("POLYMARKET_FEE_RATE_POLITICS"),
        "mentions" => env_f64_required("POLYMARKET_FEE_RATE_MENTIONS"),
        "tech" => env_f64_required("POLYMARKET_FEE_RATE_TECH"),
        "economics" => env_f64_required("POLYMARKET_FEE_RATE_ECONOMICS"),
        "culture" => env_f64_required("POLYMARKET_FEE_RATE_CULTURE"),
        "weather" => env_f64_required("POLYMARKET_FEE_RATE_WEATHER"),
        "other" | "general" => env_f64_required("POLYMARKET_FEE_RATE_OTHER"),
        "geopolitics" => env_f64_required("POLYMARKET_FEE_RATE_GEOPOLITICS"),
        unknown => {
            eprintln!(
                "unknown polymarket category '{}', using POLYMARKET_FEE_RATE_DEFAULT",
                unknown
            );
            env_f64_required("POLYMARKET_FEE_RATE_DEFAULT")
        }
    }
}

fn trim_history(history: &mut Vec<CompletedTrade>, max_len: usize) {
    if max_len == 0 {
        history.clear();
        return;
    }
    if history.len() > max_len {
        let drop_n = history.len() - max_len;
        history.drain(0..drop_n);
    }
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_f64_required(name: &str) -> Result<f64> {
    let raw = std::env::var(name)
        .with_context(|| format!("missing required environment variable {name}"))?;
    raw.parse::<f64>()
        .with_context(|| format!("invalid {name} value: {raw}"))
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn capped_entry_notional_usd(
    requested_notional_usd: f64,
    current_gross_exposure_usd: f64,
    risk_limits: RiskLimits,
    account_equity_usd: f64,
) -> f64 {
    if !requested_notional_usd.is_finite() || requested_notional_usd <= 0.0 {
        return 0.0;
    }

    let remaining_notional_cap = (risk_limits.max_notional_usd - current_gross_exposure_usd).max(0.0);
    let leverage_cap_usd = (risk_limits.max_leverage * account_equity_usd).max(0.0);
    let remaining_leverage_cap = (leverage_cap_usd - current_gross_exposure_usd).max(0.0);

    requested_notional_usd
        .min(remaining_notional_cap)
        .min(remaining_leverage_cap)
        .max(0.0)
}

fn is_last_seconds_of_five_min_window(now_ms: u64, block_last_seconds: u64) -> bool {
    if block_last_seconds == 0 {
        return false;
    }

    let window_ms = 5 * 60 * 1000;
    let block_ms = block_last_seconds.saturating_mul(1_000).min(window_ms - 1);
    let elapsed_in_window = now_ms % window_ms;
    elapsed_in_window >= (window_ms - block_ms)
}

fn is_first_seconds_of_five_min_window(now_ms: u64, block_first_seconds: u64) -> bool {
    if block_first_seconds == 0 {
        return false;
    }

    let window_ms = 5 * 60 * 1000;
    let block_ms = block_first_seconds.saturating_mul(1_000).min(window_ms - 1);
    let elapsed_in_window = now_ms % window_ms;
    elapsed_in_window < block_ms
}

fn five_min_bucket_start_ms(ts_ms: u64) -> u64 {
    let window_ms = 5 * 60 * 1000;
    ts_ms - (ts_ms % window_ms)
}

fn utc_day_string(ts_ms: u64) -> String {
    let days_since_epoch = (ts_ms / 1_000 / 86_400) as i64;
    let (year, month, day) = civil_from_days(days_since_epoch);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year, m, d)
}
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}



