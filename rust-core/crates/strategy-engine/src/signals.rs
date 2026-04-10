use common::{Quote, Signal, TradeDirection};

use crate::arbitrage::{expected_edge_after_costs, CostModel};
use crate::fair_value::{blended_fair_value, dynamic_blend_weight, fair_value_advanced};

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
    pub no_trade_band_bps: f64,
    pub max_model_market_divergence_bps: f64,
    pub divergence_penalty_bps_per_100: f64,
    pub min_liquidity_score: f64,
    pub direction_mismatch_penalty_bps: f64,
    pub fair_value_horizon_ms: u64,
    pub fair_value_vol_floor_bps: f64,
    pub blend_max_divergence_bps: f64,
    pub micro_bias_cap_bps: f64,
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
            no_trade_band_bps: 20.0,
            max_model_market_divergence_bps: 450.0,
            divergence_penalty_bps_per_100: 2.5,
            min_liquidity_score: 0.15,
            direction_mismatch_penalty_bps: 8.0,
            fair_value_horizon_ms: 300_000,
            fair_value_vol_floor_bps: 20.0,
            blend_max_divergence_bps: 350.0,
            micro_bias_cap_bps: 12.0,
        }
    }
}

pub fn compute_signal(pm: &Quote, bn: &Quote, cfg: SignalConfig) -> Signal {
    if !(pm.price.is_finite() && bn.price.is_finite()) || bn.price <= 0.0 {
        return Signal {
            edge_bps: 0.0,
            net_edge_bps: 0.0,
            should_trade: false,
            direction: TradeDirection::Flat,
        };
    }

    let now_ms = now_ms();
    let oldest_quote_age_ms = quote_age_ms(now_ms, pm.ts_ms).max(quote_age_ms(now_ms, bn.ts_ms));

    let pm_spread = pm.spread_bps();
    let bn_spread = bn.spread_bps();
    let liquidity_score = (1.0 - (pm_spread / cfg.spread_guard_bps).clamp(0.0, 1.0)).clamp(0.0, 1.0);

    let modeled_vol_bps = cfg.regime_vol_bps.abs().max(cfg.fair_value_vol_floor_bps);
    let model_fv = fair_value_advanced(
        bn.price,
        cfg.anchor_price,
        cfg.basis_bps,
        modeled_vol_bps,
        oldest_quote_age_ms,
        cfg.fair_value_horizon_ms,
    );

    let market_fv = pm.price;
    let model_market_divergence_bps = (model_fv - market_fv).abs() * 10_000.0;
    let adaptive_model_weight = dynamic_blend_weight(
        cfg.microstructure_model_weight,
        model_market_divergence_bps,
        cfg.blend_max_divergence_bps,
        liquidity_score,
    );

    let micro_bias_bps = ((bn_spread - pm_spread) * 0.2)
        .clamp(-cfg.micro_bias_cap_bps.abs(), cfg.micro_bias_cap_bps.abs());
    let fair_value = blended_fair_value(model_fv, market_fv, adaptive_model_weight, micro_bias_bps);

    let gross_edge = fair_value - pm.price;
    let gross_edge_bps_signed = gross_edge * 10_000.0;
    let gross_edge_bps = gross_edge_bps_signed.abs();

    let mut net_edge_bps = expected_edge_after_costs(
        gross_edge_bps,
        cfg.target_notional_usd,
        liquidity_score,
        oldest_quote_age_ms,
        cfg.alpha_half_life_ms,
        pm.price,
        cfg.costs,
    );

    let spread_penalty = ((pm_spread + bn_spread) * 0.5).max(cfg.spread_guard_bps * 0.25);
    net_edge_bps -= spread_penalty;

    let age_ratio = (oldest_quote_age_ms as f64 / cfg.max_quote_age_ms.max(1) as f64).clamp(0.0, 3.0);
    let age_penalty = cfg.stale_penalty_bps.max(0.0) * age_ratio;
    net_edge_bps -= age_penalty;

    let vol_penalty = (cfg.regime_vol_bps / 100.0).clamp(0.0, 10.0);
    net_edge_bps -= vol_penalty;

    let model_side = side_from_prob(model_fv, cfg.no_trade_band_bps);
    let market_side = side_from_prob(pm.price, cfg.no_trade_band_bps);
    if model_side != 0 && market_side != 0 && model_side != market_side {
        net_edge_bps -= cfg.direction_mismatch_penalty_bps.max(0.0);
    }

    let divergence_excess_bps = (model_market_divergence_bps - cfg.no_trade_band_bps.max(0.0)).max(0.0);
    net_edge_bps -= (divergence_excess_bps / 100.0) * cfg.divergence_penalty_bps_per_100.max(0.0);

    let direction = if gross_edge_bps_signed > 0.0 {
        TradeDirection::Up
    } else if gross_edge_bps_signed < 0.0 {
        TradeDirection::Down
    } else {
        TradeDirection::Flat
    };

    let adaptive_threshold = cfg.min_edge_bps + (pm_spread + bn_spread) * 0.15 + vol_penalty;
    let no_trade_band = model_side == 0 && market_side == 0;
    let stale_divergence = model_market_divergence_bps > cfg.max_model_market_divergence_bps.max(0.0);
    let too_thin_liquidity = liquidity_score < cfg.min_liquidity_score.clamp(0.0, 1.0);

    Signal {
        edge_bps: gross_edge_bps_signed,
        net_edge_bps,
        should_trade: net_edge_bps >= adaptive_threshold
            && direction != TradeDirection::Flat
            && !no_trade_band
            && !stale_divergence
            && !too_thin_liquidity,
        direction,
    }
}

fn side_from_prob(prob: f64, no_trade_band_bps: f64) -> i8 {
    let center_bps = (prob - 0.5) * 10_000.0;
    let band = no_trade_band_bps.max(0.0);
    if center_bps > band {
        1
    } else if center_bps < -band {
        -1
    } else {
        0
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
