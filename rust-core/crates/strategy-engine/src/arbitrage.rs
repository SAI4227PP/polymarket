#[derive(Debug, Clone, Copy)]
pub struct CostModel {
    // Polymarket CLOB fee/impact model (all values in bps).
    pub taker_fees_bps: f64,
    pub maker_rebate_bps: f64,
    pub slippage_bps: f64,
    pub latency_penalty_bps: f64,
    pub adverse_selection_bps: f64,
    pub impact_coeff_bps: f64,
    pub settlement_buffer_bps: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct CostBreakdown {
    pub decayed_alpha_bps: f64,
    pub explicit_costs_bps: f64,
    pub adverse_selection_bps: f64,
    pub impact_bps: f64,
    pub maker_rebate_credit_bps: f64,
    pub net_edge_bps: f64,
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
            settlement_buffer_bps: 1.0,
        }
    }
}

pub fn expected_edge_after_costs(
    gross_edge_bps: f64,
    order_notional_usd: f64,
    liquidity_score: f64,
    alpha_age_ms: u64,
    alpha_half_life_ms: u64,
    polymarket_price: f64,
    costs: CostModel,
) -> f64 {
    cost_breakdown(
        gross_edge_bps,
        order_notional_usd,
        liquidity_score,
        alpha_age_ms,
        alpha_half_life_ms,
        polymarket_price,
        costs,
    )
    .net_edge_bps
}

pub fn cost_breakdown(
    gross_edge_bps: f64,
    order_notional_usd: f64,
    liquidity_score: f64,
    alpha_age_ms: u64,
    alpha_half_life_ms: u64,
    polymarket_price: f64,
    costs: CostModel,
) -> CostBreakdown {
    let decayed_alpha_bps = alpha_half_life_decay(gross_edge_bps, alpha_age_ms, alpha_half_life_ms);
    let liq = liquidity_score.clamp(0.0, 1.0);

    // Polymarket contracts are in [0,1]. Infer shares from notional and price.
    let price = polymarket_price.clamp(0.05, 0.95);
    let estimated_shares = order_notional_usd.max(0.0) / price;

    // Assume taker-like entry+exit for conservative net edge budgeting.
    let round_trip_fees_bps = costs.taker_fees_bps.max(0.0) * 2.0;
    let slippage_bps = costs.slippage_bps.max(0.0) * (1.0 + (1.0 - liq));
    let latency_bps = costs.latency_penalty_bps.max(0.0);
    let settlement_bps = costs.settlement_buffer_bps.max(0.0);
    let explicit_costs_bps = round_trip_fees_bps + slippage_bps + latency_bps + settlement_bps;

    // Extremes (near 0 or 1) are more fragile for quick reversals.
    let extremity = ((price - 0.5).abs() / 0.5).clamp(0.0, 1.0);
    let adverse_selection_bps = costs.adverse_selection_bps.max(0.0)
        * (1.0 + 0.5 * (1.0 - liq) + 0.5 * extremity);

    // Impact rises with size in shares and illiquidity.
    let impact_bps = costs.impact_coeff_bps.max(0.0)
        * estimated_shares.sqrt()
        * (1.0 - liq).powi(2)
        / 10.0;

    // Rebate benefit should decay when fill probability is poor.
    let maker_fill_prob = liq.sqrt().clamp(0.0, 1.0);
    let maker_rebate_credit_bps = costs.maker_rebate_bps.max(0.0) * maker_fill_prob;

    let net_edge_bps = decayed_alpha_bps
        - explicit_costs_bps
        - adverse_selection_bps
        - impact_bps
        + maker_rebate_credit_bps;

    CostBreakdown {
        decayed_alpha_bps,
        explicit_costs_bps,
        adverse_selection_bps,
        impact_bps,
        maker_rebate_credit_bps,
        net_edge_bps,
    }
}

pub fn alpha_half_life_decay(alpha_bps: f64, elapsed_ms: u64, half_life_ms: u64) -> f64 {
    if half_life_ms == 0 {
        return 0.0;
    }
    let decay = 2f64.powf(-(elapsed_ms as f64) / (half_life_ms as f64));
    alpha_bps * decay
}
