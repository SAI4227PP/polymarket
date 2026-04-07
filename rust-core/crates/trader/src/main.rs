use anyhow::{anyhow, Context, Result};
use adapters_binance::{ws as binance_ws, BinanceClient};
use adapters_polymarket::{ws as polymarket_ws, PolymarketClient};
use common::{Quote, RiskDecision, TradeDirection};
use execution_engine::order_manager::{self, ExecutionConfig};
use portfolio_engine::daily::{update_daily_history, PortfolioDayStats};
use portfolio_engine::pnl::{total_pnl, unrealized_pnl};
use portfolio_engine::positions::{update_position, Position};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use risk_engine::kill_switch::{should_kill, KillSwitchConfig, KillSwitchState};
use risk_engine::limits::{evaluate_trade, RiskInput, RiskLimits};
use serde::Serialize;
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let bn_symbol = std::env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());
    let mode = std::env::var("MODE").unwrap_or_else(|_| "paper".to_string());
    let is_paper_mode = mode.eq_ignore_ascii_case("paper");

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
    let configured_event_slug = std::env::var("POLYMARKET_EVENT_SLUG").unwrap_or_default();
    let trade_history_limit = env_u64("TRADE_HISTORY_LIMIT", 200) as usize;

    let signal_cfg = SignalConfig {
        min_edge_bps,
        anchor_price: env_f64("BINANCE_ANCHOR_PRICE", 100_000.0),
        basis_bps: env_f64("BINANCE_BASIS_BPS", 0.0),
        target_notional_usd: order_notional_usd,
        microstructure_model_weight: env_f64("BINANCE_SIGNAL_WEIGHT", 0.35),
        ..SignalConfig::default()
    };

    let risk_limits = RiskLimits {
        max_notional_usd: env_f64("MAX_NOTIONAL_USD", if is_paper_mode { 5.0 } else { 10.0 }),
        max_daily_loss_usd: env_f64("MAX_DAILY_LOSS_USD", if is_paper_mode { 20.0 } else { 100.0 }),
        max_open_positions: env_u64("MAX_OPEN_POSITIONS", if is_paper_mode { 1 } else { 3 }) as usize,
        max_leverage: env_f64("MAX_LEVERAGE", if is_paper_mode { 1.5 } else { 2.5 }),
        max_var_budget_usd: env_f64("MAX_VAR_BUDGET_USD", if is_paper_mode { 50.0 } else { 150.0 }),
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
    spawn_polymarket_stream(pm_tx);

    let mut redis_conn: Option<MultiplexedConnection> = None;

    let mut ticker = interval(Duration::from_millis(loop_ms));
    let mut day_pnl_usd = 0.0;
    let mut portfolio_realized_pnl_usd = 0.0;
    let mut pending_positions: Vec<PendingPosition> = Vec::new();
    let mut trade_history: Vec<CompletedTrade> = Vec::new();
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
    let mut last_past_probability: Option<f64> = None;
    let mut error_streak = 0u32;
    let mut oldest_quote_age_ms = 0u64;
    let enforce_position_ttl = env_bool("ENFORCE_POSITION_TTL", false);

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


    loop {
        ticker.tick().await;
        let pm_snapshot = pm_rx.borrow().clone();
        let bn_snapshot = bn_rx.borrow().clone();
        if let Some(q) = pm_snapshot.as_ref() {
            if q.venue == "polymarket-past-results" {
                last_past_probability = Some(q.price.clamp(0.0, 1.0));
            } else if q.venue == "polymarket-prior" && last_past_probability.is_none() {
                // Fallback: if the explicit past-results seed was missed between loop ticks,
                // reuse prior probability so API websocket always has a past_outcomes payload.
                last_past_probability = Some(q.price.clamp(0.0, 1.0));
            }
        }

        if enforce_position_ttl {
            let closed = settle_expired_positions(
                &mut pending_positions,
                &mut day_pnl_usd,
                &mut portfolio_realized_pnl_usd,
                now_ms(),
            );
            if !closed.is_empty() {
                trade_history.extend(closed);
                trim_history(&mut trade_history, trade_history_limit);
            }
        }
        let open_positions = pending_positions.len();
        let open_direction = current_open_direction(&pending_positions);
        let current_gross_exposure_usd: f64 = pending_positions
            .iter()
            .map(|p| p.entry_notional_usd.abs())
            .sum();
        let current_largest_position_usd = pending_positions
            .iter()
            .map(|p| p.entry_notional_usd.abs())
            .fold(0.0, f64::max);

        let iteration = run_iteration(
            &pm_rx,
            &bn_rx,
            signal_cfg,
            risk_limits,
            execution_cfg,
            order_notional_usd,
            day_pnl_usd,
            open_positions,
            open_direction,
            current_gross_exposure_usd,
            current_largest_position_usd,
            position_ttl_ms,
            account_equity_usd,
            realized_vol_bps,
            kill_cfg.max_stale_data_ms,
        )
        .await;

        match iteration {
            Ok(IterationOutcome {
                position,
                state,
                close_open_positions,
                close_reason,
            }) => {
                error_streak = 0;
                oldest_quote_age_ms = state.oldest_quote_age_ms;

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

                let mut state = state;
                let portfolio_position = build_portfolio_position(&pending_positions);
                let mark_price = state.polymarket_mid.clamp(0.0, 1.0);
                let unrealized_pnl_usd = unrealized_pnl(portfolio_position.avg_price, mark_price, portfolio_position.qty);
                let total_pnl_usd = total_pnl(
                    portfolio_realized_pnl_usd,
                    portfolio_position.avg_price,
                    mark_price,
                    portfolio_position.qty,
                );

                state.open_positions = pending_positions.len();
                state.day_pnl_usd = day_pnl_usd;
                state.portfolio_net_qty = portfolio_position.qty;
                state.portfolio_avg_entry = portfolio_position.avg_price;
                state.portfolio_realized_pnl_usd = portfolio_realized_pnl_usd;
                state.portfolio_unrealized_pnl_usd = unrealized_pnl_usd;
                state.portfolio_total_pnl_usd = total_pnl_usd;
                state.portfolio_all_time_realized_pnl_usd = portfolio_realized_pnl_usd;

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
                if let Err(err) = publish_aux_stream_payloads_to_redis(
                    &redis_url,
                    &redis_live_btc_key,
                    &redis_past_outcomes_key,
                    &configured_event_slug,
                    pm_snapshot.clone(),
                    bn_snapshot.clone(),
                    last_past_probability,
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
    close_open_positions: bool,
    close_reason: Option<String>,
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
    open_direction: Option<TradeDirection>,
    current_gross_exposure_usd: f64,
    current_largest_position_usd: f64,
    position_ttl_ms: u64,
    account_equity_usd: f64,
    realized_vol_bps: f64,
    max_quote_age_ms: u64,
) -> Result<IterationOutcome> {
    let pm_quote = match pm_rx.borrow().clone() {
        Some(q) => q,
        None => {
            eprintln!("waiting for polymarket quote...");
            return Ok(IterationOutcome {
                position: None,
                state: waiting_state(day_pnl_usd, open_positions),
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
            state,
            close_open_positions: false,
            close_reason: None,
        });
    }

    let bn_quote = match bn_rx.borrow().clone() {
        Some(q) => q,
        None => {
            eprintln!("waiting for binance quote...");
            return Ok(IterationOutcome {
                position: None,
                state: waiting_state(day_pnl_usd, open_positions),
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
    let no_entry_last_seconds_5m = env_u64("NO_ENTRY_LAST_SECONDS_5M", 15).min(299);
    let blocked_last_seconds_5m = is_last_seconds_of_five_min_window(now, no_entry_last_seconds_5m);
    let risk_capped_notional_usd = capped_entry_notional_usd(
        order_notional_usd,
        current_gross_exposure_usd,
        risk_limits,
        account_equity_usd,
    );

    let can_open_new_position = !has_open_positions;
    let candidate_intent = if can_open_new_position && pm_fresh && bn_fresh && !blocked_last_seconds_5m {
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
    if !report.accepted && can_open_new_position && blocked_last_seconds_5m {
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

    let entry_signal = can_open_new_position && signal.should_trade && risk.allowed && report.accepted;
    let hold_strong_edge_bps = env_f64("HOLD_STRONG_EDGE_BPS", (signal_cfg.min_edge_bps * 0.6).max(1.0));
    let invalidation_close_bps =
        env_f64("INVALIDATION_CLOSE_BPS", (signal_cfg.min_edge_bps * 0.8).max(4.0));
    let strong_same_direction = has_open_positions
        && open_direction
            .map(|d| signal.direction == d && signal.net_edge_bps >= hold_strong_edge_bps)
            .unwrap_or(false);
    let invalidation_deadband_hold = has_open_positions
        && !signal.should_trade
        && signal.net_edge_bps.abs() < invalidation_close_bps;

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

    let close_reason = if reversal_signal {
        Some("reverse_signal".to_string())
    } else if has_open_positions
        && !signal.should_trade
        && !strong_same_direction
        && !invalidation_deadband_hold
    {
        Some("signal_invalidated".to_string())
    } else if has_open_positions && !pm_fresh {
        Some("stale_polymarket".to_string())
    } else if has_open_positions && !bn_fresh {
        Some("stale_binance".to_string())
    } else {
        None
    };
    let exit_signal = close_reason.is_some();

    let position_signal = if entry_signal {
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
    } else if blocked_last_seconds_5m && can_open_new_position {
        "hold_last_seconds_5m_guard"
    } else if has_open_positions && strong_same_direction {
        "hold_strong_direction"
    } else if reversal_signal {
        "close_on_reversal"
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
    };

    if report.accepted {
        let expected_pnl_usd = filled_notional_usd * (signal.net_edge_bps / 10_000.0);
        if let Some(order) = submitted_intent {
            let ts = now_ms();
            return Ok(IterationOutcome {
                position: Some(PendingPosition {
                    id: format!("BTC-{}", ts),
                    direction: order.direction,
                    quantity: order.quantity,
                    entry_price: order.limit_price,
                    entry_notional_usd: order.quantity * order.limit_price,
                    entry_ts_ms: ts,
                    settle_at_ms: ts.saturating_add(position_ttl_ms),
                    expected_pnl_usd,
                }),
                state,
                close_open_positions: false,
                close_reason: None,
            });
        }
    }

    Ok(IterationOutcome {
        position: None,
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
    past_outcomes_key: &str,
    event_slug: &str,
    pm_quote: Option<Quote>,
    bn_quote: Option<Quote>,
    last_past_probability: Option<f64>,
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

    let live_btc_payload = if let Some(q) = bn_quote {
        json!({
            "source": q.venue,
            "symbol": q.symbol,
            "price": q.price,
            "ts_ms": q.ts_ms
        })
    } else {
        json!({ "warning": "live btc unavailable" })
    };

    let derived_event_slug = if !event_slug.trim().is_empty() {
        event_slug.to_string()
    } else {
        pm_quote.map(|q| q.symbol).unwrap_or_default()
    };

    let past_outcomes_payload = if let Some(p) = last_past_probability {
        json!({
            "source": "polymarket-past-results",
            "event_slug": derived_event_slug,
            "probability_up": p,
            "ts_ms": now_ms()
        })
    } else {
        json!({ "warning": "past outcomes unavailable" })
    };

    if let Some(c) = conn.as_mut() {
        let live_payload = serde_json::to_string(&live_btc_payload).context("serialize live btc payload")?;
        let past_payload = serde_json::to_string(&past_outcomes_payload).context("serialize past outcomes payload")?;

        let live_set: redis::RedisResult<()> = c.set(live_btc_key, live_payload).await;
        if let Err(e) = live_set {
            *conn = None;
            return Err(anyhow!("redis set live btc failed: {e}"));
        }

        let past_set: redis::RedisResult<()> = c.set(past_outcomes_key, past_payload).await;
        if let Err(e) = past_set {
            *conn = None;
            return Err(anyhow!("redis set past outcomes failed: {e}"));
        }
    }

    Ok(())
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
    }
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

fn spawn_polymarket_stream(tx: watch::Sender<Option<Quote>>) {
    tokio::spawn(async move {
        let mut last_instrument: Option<String> = None;
        let mut last_source: Option<String> = None;

        loop {
            let resolved = match polymarket_ws::resolve_instrument_from_env().await {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("polymarket resolve error: {err:#}");
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            };

            let instrument_id = resolved.instrument_id;
            let source = resolved.source;

            if last_instrument.as_deref() != Some(instrument_id.as_str()) {
                if let (Some(prev_market), Some(prev_source)) = (last_instrument.as_ref(), last_source.as_ref()) {
                    let prev_bucket = extract_5m_bucket(prev_source).or_else(|| extract_5m_bucket(prev_market));
                    let new_bucket = extract_5m_bucket(&source).or_else(|| extract_5m_bucket(&instrument_id));
                    eprintln!(
                        "polymarket 5m transition resolved_previous_bucket={:?} previous_market={} -> new_bucket={:?} new_market={}",
                        prev_bucket, prev_market, new_bucket, instrument_id
                    );
                }

                eprintln!(
                    "polymarket rollover subscribe instrument={} source={}",
                    instrument_id, source
                );
                last_instrument = Some(instrument_id.clone());
                last_source = Some(source.clone());
            }

            let tx_clone = tx.clone();
            let instrument_for_task = instrument_id.clone();
            let mut stream_task = tokio::spawn(async move {
                polymarket_ws::run_market_quote_stream(&instrument_for_task, tx_clone).await
            });

            let rollover_wait = next_polymarket_rollover_wait();
            tokio::pin!(rollover_wait);

            tokio::select! {
                task_res = &mut stream_task => {
                    match task_res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => eprintln!("polymarket stream error: {err:#}"),
                        Err(err) => eprintln!("polymarket stream task join error: {err}"),
                    }
                }
                _ = &mut rollover_wait => {
                    eprintln!("polymarket rollover boundary reached; re-resolving market");
                    stream_task.abort();
                    let _ = stream_task.await;
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
    });
}

fn extract_5m_bucket(value: &str) -> Option<i64> {
    let token = value.split('-').next_back()?.trim();
    token.parse::<i64>().ok()
}

async fn next_polymarket_rollover_wait() {
    let mode = std::env::var("POLYMARKET_AUTO_EVENT_MODE").unwrap_or_default();
    if mode.eq_ignore_ascii_case("epoch_5m") {
        let now_ms = now_ms();
        let period_ms = 300_000_u64;
        let next_boundary_ms = ((now_ms / period_ms) + 1) * period_ms + 1200;
        let wait_ms = next_boundary_ms.saturating_sub(now_ms).max(500);
        sleep(Duration::from_millis(wait_ms)).await;
        return;
    }

    sleep(Duration::from_secs(300)).await;
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
    matches!(q.venue.as_str(), "polymarket" | "polymarket-live-data")
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
fn settle_expired_positions(
    pending_positions: &mut Vec<PendingPosition>,
    day_pnl_usd: &mut f64,
    portfolio_realized_pnl_usd: &mut f64,
    now_ms: u64,
) -> Vec<CompletedTrade> {
    let mut keep = Vec::with_capacity(pending_positions.len());
    let mut closed = Vec::new();

    for pos in pending_positions.drain(..) {
        if pos.settle_at_ms <= now_ms {
            *day_pnl_usd += pos.expected_pnl_usd;
            *portfolio_realized_pnl_usd += pos.expected_pnl_usd;
            let exit_notional_usd = (pos.entry_notional_usd + pos.expected_pnl_usd).max(0.0);
            let exit_price = if pos.quantity > 0.0 {
                exit_notional_usd / pos.quantity
            } else {
                pos.entry_price
            };

            closed.push(CompletedTrade {
                id: pos.id,
                pair: "BTC".to_string(),
                direction: format!("{:?}", pos.direction),
                entry_side: entry_side_for_direction(pos.direction).to_string(),
                exit_side: exit_side_for_direction(pos.direction).to_string(),
                quantity: pos.quantity,
                entry_price: pos.entry_price,
                exit_price,
                entry_probability: pos.entry_price.clamp(0.0, 1.0),
                exit_probability: exit_price.clamp(0.0, 1.0),
                entry_notional_usd: pos.entry_notional_usd,
                exit_notional_usd,
                pnl_usd: pos.expected_pnl_usd,
                entry_ts_ms: pos.entry_ts_ms,
                exit_ts_ms: now_ms,
                duration_ms: now_ms.saturating_sub(pos.entry_ts_ms),
                outcome: if pos.expected_pnl_usd >= 0.0 {
                    "profit".to_string()
                } else {
                    "loss".to_string()
                },
            });
        } else {
            keep.push(pos);
        }
    }

    *pending_positions = keep;
    closed
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


#[cfg(test)]
mod tests {
    use super::*;

    fn q(venue: &str, price: f64, ts_ms: u64) -> Quote {
        Quote {
            venue: venue.to_string(),
            symbol: "BTC".to_string(),
            bid: price,
            ask: price,
            price,
            ts_ms,
        }
    }

    fn test_signal_cfg() -> SignalConfig {
        SignalConfig {
            min_edge_bps: 1.0,
            anchor_price: 100_000.0,
            basis_bps: 0.0,
            microstructure_model_weight: 1.0,
            regime_vol_bps: 0.0,
            target_notional_usd: 10.0,
            ..SignalConfig::default()
        }
    }

    fn test_risk_limits() -> RiskLimits {
        RiskLimits {
            max_notional_usd: 100.0,
            max_daily_loss_usd: 1_000.0,
            max_open_positions: 3,
            max_leverage: 10.0,
            max_var_budget_usd: 1_000.0,
            max_concentration_ratio: 0.5,
        }
    }

    fn test_execution_cfg() -> ExecutionConfig {
        ExecutionConfig {
            base_qty: 1.0,
            edge_threshold_bps: 1.0,
            demo_wallet_balance_usd: 100.0,
            min_order_notional_usd: 1.0,
            min_order_shares: 0.01,
            ..ExecutionConfig::default()
        }
    }

    #[tokio::test]
    async fn up_reverse_down_transitions_risk_and_portfolio() {
        let now = now_ms();

        let (pm_tx, pm_rx) = watch::channel::<Option<Quote>>(None);
        let (bn_tx, bn_rx) = watch::channel::<Option<Quote>>(None);

        // 1) Open UP
        pm_tx.send_replace(Some(q("polymarket", 0.20, now)));
        bn_tx.send_replace(Some(q("binance", 100_000.0, now)));

        let out1 = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            0,
            None,
            0.0,
            0.0,
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("iteration 1 should succeed");

        let up_pos = out1.position.expect("expected initial UP position");
        assert_eq!(up_pos.direction, TradeDirection::Up);

        let mut pending = vec![up_pos.clone()];
        let portfolio_up = build_portfolio_position(&pending);
        assert!(portfolio_up.qty > 0.0, "UP portfolio qty should be positive");

        // 2) Reverse signal to DOWN -> should trigger close, no new open in same cycle
        let now2 = now + 500;
        pm_tx.send_replace(Some(q("polymarket", 0.80, now2)));
        bn_tx.send_replace(Some(q("binance", 0.0, now2)));

        let out2 = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            pending.len(),
            Some(TradeDirection::Up),
            pending.iter().map(|p| p.entry_notional_usd.abs()).sum(),
            pending
                .iter()
                .map(|p| p.entry_notional_usd.abs())
                .fold(0.0, f64::max),
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("iteration 2 should succeed");

        assert!(out2.close_open_positions, "reversal should close open UP");
        assert_eq!(out2.close_reason.as_deref(), Some("reverse_signal"));
        assert!(out2.position.is_none(), "should not open DOWN in same iteration as close");

        let mut day_pnl_usd = 0.0;
        let mut portfolio_realized_pnl_usd = 0.0;
        let closed = close_positions_now(
            &mut pending,
            &mut day_pnl_usd,
            &mut portfolio_realized_pnl_usd,
            out2.state.polymarket_mid,
            now2,
            "reverse_signal",
        );
        assert_eq!(pending.len(), 0, "pending positions must be cleared after close");
        assert!(!closed.is_empty(), "should record a closed trade");
        assert_eq!(closed[0].entry_side, "buy");
        assert_eq!(closed[0].exit_side, "sell");

        let portfolio_flat = build_portfolio_position(&pending);
        assert!(portfolio_flat.qty.abs() < f64::EPSILON, "portfolio should be flat after close");

        // 3) Next cycle with DOWN signal should open DOWN
        let now3 = now2 + 500;
        pm_tx.send_replace(Some(q("polymarket", 0.80, now3)));
        bn_tx.send_replace(Some(q("binance", 0.0, now3)));

        let out3 = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            day_pnl_usd,
            0,
            None,
            0.0,
            0.0,
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("iteration 3 should succeed");

        let down_pos = out3.position.expect("expected DOWN position after reversal close");
        assert_eq!(down_pos.direction, TradeDirection::Down);

        let portfolio_down = build_portfolio_position(&[down_pos]);
        assert!(portfolio_down.qty < 0.0, "DOWN portfolio qty should be negative");
    }

    #[tokio::test]
    async fn open_position_is_not_force_closed_by_entry_risk_check() {
        let now = now_ms();
        let (pm_tx, pm_rx) = watch::channel::<Option<Quote>>(None);
        let (bn_tx, bn_rx) = watch::channel::<Option<Quote>>(None);

        pm_tx.send_replace(Some(q("polymarket", 0.20, now)));
        bn_tx.send_replace(Some(q("binance", 100_000.0, now)));

        let opened = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            0,
            None,
            0.0,
            0.0,
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("open iteration should succeed");

        let pos = opened.position.expect("expected initial UP position");
        let now2 = now + 500;
        pm_tx.send_replace(Some(q("polymarket", 0.20, now2)));
        bn_tx.send_replace(Some(q("binance", 100_000.0, now2)));

        let hold = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            1,
            Some(TradeDirection::Up),
            pos.entry_notional_usd.abs(),
            pos.entry_notional_usd.abs(),
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("hold iteration should succeed");

        assert!(!hold.close_open_positions, "existing position should not be force-closed");
        assert!(hold.close_reason.is_none(), "no risk_rejected close expected");
        assert!(hold.position.is_none(), "should not open a second position while one is live");
    }

    #[tokio::test]
    async fn ignores_non_tradeable_polymarket_seed_quotes() {
        let now = now_ms();
        let (pm_tx, pm_rx) = watch::channel::<Option<Quote>>(None);
        let (bn_tx, bn_rx) = watch::channel::<Option<Quote>>(None);

        pm_tx.send_replace(Some(q("polymarket-past-results", 0.42, now)));
        bn_tx.send_replace(Some(q("binance", 100_000.0, now)));

        let out = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            0,
            None,
            0.0,
            0.0,
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("iteration should succeed");

        assert!(out.position.is_none(), "must not trade on seed quote venues");
        assert!(!out.close_open_positions, "must not close solely on seed quote venues");
        assert_eq!(
            out.state.action,
            "waiting_tradeable_polymarket_quote",
            "state should indicate waiting for tradable quote"
        );
    }

    #[tokio::test]
    async fn invalidation_deadband_holds_instead_of_churn_close() {
        let now = now_ms();
        let (pm_tx, pm_rx) = watch::channel::<Option<Quote>>(None);
        let (bn_tx, bn_rx) = watch::channel::<Option<Quote>>(None);

        // Open a position first.
        pm_tx.send_replace(Some(q("polymarket", 0.20, now)));
        bn_tx.send_replace(Some(q("binance", 100_000.0, now)));

        let opened = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            0,
            None,
            0.0,
            0.0,
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("open iteration should succeed");

        let pos = opened.position.expect("expected position to be opened");

        // With tiny/noisy edge around current price, deadband should hold, not close.
        let now2 = now + 1_000;
        pm_tx.send_replace(Some(q("polymarket", pos.entry_price, now2)));
        bn_tx.send_replace(Some(q("binance", 100_000.0, now2)));

        let hold = run_iteration(
            &pm_rx,
            &bn_rx,
            test_signal_cfg(),
            test_risk_limits(),
            test_execution_cfg(),
            10.0,
            0.0,
            1,
            Some(pos.direction),
            pos.entry_notional_usd.abs(),
            pos.entry_notional_usd.abs(),
            5_000,
            100.0,
            30.0,
            60_000,
        )
        .await
        .expect("hold iteration should succeed");

        assert!(!hold.close_open_positions, "deadband should avoid churn close");
        assert!(hold.position.is_none(), "no second position while one is open");
    }

    #[test]
    fn caps_notional_from_risk_limits() {
        let limits = test_risk_limits();
        let capped = capped_entry_notional_usd(10.0, 95.0, limits, 100.0);
        assert!(capped <= 5.0 + f64::EPSILON, "capped={}", capped);
    }

    #[test]
    fn detects_last_seconds_of_five_min_window() {
        let window_ms = 5 * 60 * 1000;
        assert!(!is_last_seconds_of_five_min_window(window_ms - 16_000, 15));
        assert!(is_last_seconds_of_five_min_window(window_ms - 10_000, 15));
    }
}






