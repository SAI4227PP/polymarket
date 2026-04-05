use common::{Quote, Signal, TradeDirection};

use crate::arbitrage::{expected_edge_after_costs, CostModel};
use crate::fair_value::{blended_fair_value, fair_value_with_basis};

#[derive(Debug, Clone, Copy)]
pub struct SignalConfig {
    pub min_edge_bps: f64,
    pub anchor_price: f64,
    pub basis_bps: f64,
    pub costs: CostModel,
    pub spread_guard_bps: f64,
    pub max_quote_age_ms: u64,
    pub stale_penalty_bps: f64,
    pub microstructure_model_weight: f64,
    pub alpha_half_life_ms: u64,
    pub regime_vol_bps: f64,
    pub target_notional_usd: f64,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            min_edge_bps: 8.0,
            anchor_price: 100_000.0,
            basis_bps: 0.0,
            costs: CostModel::default(),
            spread_guard_bps: 30.0,
            max_quote_age_ms: 5_000,
            stale_penalty_bps: 8.0,
            microstructure_model_weight: 0.65,
            alpha_half_life_ms: 1_500,
            regime_vol_bps: 40.0,
            target_notional_usd: 100.0,
        }
    }
}

pub fn compute_signal(pm: &Quote, bn: &Quote, cfg: SignalConfig) -> Signal {
    let model_fv = fair_value_with_basis(bn, cfg.basis_bps, cfg.anchor_price);
    let market_fv = pm.price;

    let micro_bias_bps = (bn.spread_bps() - pm.spread_bps()).clamp(-10.0, 10.0) * 0.2;
    let fair_value = blended_fair_value(
        model_fv,
        market_fv,
        cfg.microstructure_model_weight,
        micro_bias_bps,
    );

    let gross_edge = fair_value - pm.price;
    let gross_edge_bps_signed = gross_edge * 10_000.0;

    let pm_spread = pm.spread_bps();
    let bn_spread = bn.spread_bps();
    let spread_penalty = ((pm_spread + bn_spread) * 0.5).max(cfg.spread_guard_bps * 0.25);

    let now_ms = now_ms();
    let oldest_quote_age_ms = quote_age_ms(now_ms, pm.ts_ms).max(quote_age_ms(now_ms, bn.ts_ms));
    let age_penalty = if oldest_quote_age_ms > cfg.max_quote_age_ms {
        cfg.stale_penalty_bps
    } else {
        0.0
    };

    let liquidity_score = (1.0 - (pm_spread / cfg.spread_guard_bps).clamp(0.0, 1.0)).clamp(0.0, 1.0);
    let gross_edge_bps = gross_edge_bps_signed.abs();

    let mut net_edge_bps = expected_edge_after_costs(
        gross_edge_bps,
        cfg.target_notional_usd,
        liquidity_score,
        oldest_quote_age_ms,
        cfg.alpha_half_life_ms,
        cfg.costs,
    );

    let vol_penalty = (cfg.regime_vol_bps / 100.0).clamp(0.0, 10.0);
    net_edge_bps -= spread_penalty + age_penalty + vol_penalty;

    let direction = if gross_edge_bps_signed > 0.0 {
        TradeDirection::Up
    } else if gross_edge_bps_signed < 0.0 {
        TradeDirection::Down
    } else {
        TradeDirection::Flat
    };

    let adaptive_threshold = cfg.min_edge_bps + (pm_spread + bn_spread) * 0.15 + vol_penalty;

    Signal {
        edge_bps: gross_edge_bps_signed,
        net_edge_bps,
        should_trade: net_edge_bps >= adaptive_threshold && direction != TradeDirection::Flat,
        direction,
    }
}

fn quote_age_ms(now_ms: u64, quote_ts_ms: u64) -> u64 {
    now_ms.saturating_sub(quote_ts_ms)
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

