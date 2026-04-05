#[derive(Debug, Clone, Copy)]
pub struct CostModel {
    pub taker_fees_bps: f64,
    pub maker_rebate_bps: f64,
    pub slippage_bps: f64,
    pub latency_penalty_bps: f64,
    pub adverse_selection_bps: f64,
    pub impact_coeff_bps: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            taker_fees_bps: 2.0,
            maker_rebate_bps: 0.0,
            slippage_bps: 3.0,
            latency_penalty_bps: 1.0,
            adverse_selection_bps: 1.5,
            impact_coeff_bps: 0.8,
        }
    }
}

pub fn expected_edge_after_costs(
    gross_edge_bps: f64,
    order_notional_usd: f64,
    liquidity_score: f64,
    alpha_age_ms: u64,
    alpha_half_life_ms: u64,
    costs: CostModel,
) -> f64 {
    let decayed_alpha = alpha_half_life_decay(gross_edge_bps, alpha_age_ms, alpha_half_life_ms);
    let liq = liquidity_score.clamp(0.0, 1.0);
    let impact = (order_notional_usd.max(0.0).sqrt() * costs.impact_coeff_bps) * (1.0 - liq);
    let explicit_costs = costs.taker_fees_bps + costs.slippage_bps + costs.latency_penalty_bps;

    decayed_alpha - explicit_costs - costs.adverse_selection_bps + costs.maker_rebate_bps - impact
}

pub fn alpha_half_life_decay(alpha_bps: f64, elapsed_ms: u64, half_life_ms: u64) -> f64 {
    if half_life_ms == 0 {
        return 0.0;
    }
    let decay = 2f64.powf(-(elapsed_ms as f64) / (half_life_ms as f64));
    alpha_bps * decay
}
