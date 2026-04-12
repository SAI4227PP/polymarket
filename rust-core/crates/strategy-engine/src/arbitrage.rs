#[derive(Debug, Clone, Copy)]
pub struct CostModel {
    // Additional non-fee execution frictions (bps).
    pub slippage_bps: f64,
    pub latency_penalty_bps: f64,
    pub adverse_selection_bps: f64,
    pub impact_coeff_bps: f64,
    pub settlement_buffer_bps: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct CostBreakdown {
    pub decayed_alpha_bps: f64,
    pub taker_fee_bps: f64,
    pub explicit_costs_bps: f64,
    pub adverse_selection_bps: f64,
    pub impact_bps: f64,
    pub net_edge_bps: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
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
    polymarket_fees_enabled: bool,
    polymarket_fee_rate: f64,
    costs: CostModel,
) -> f64 {
    cost_breakdown(
        gross_edge_bps,
        order_notional_usd,
        liquidity_score,
        alpha_age_ms,
        alpha_half_life_ms,
        polymarket_price,
        polymarket_fees_enabled,
        polymarket_fee_rate,
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
    polymarket_fees_enabled: bool,
    polymarket_fee_rate: f64,
    costs: CostModel,
) -> CostBreakdown {
    let decayed_alpha_bps = alpha_half_life_decay(gross_edge_bps, alpha_age_ms, alpha_half_life_ms);
    let liq = liquidity_score.clamp(0.0, 1.0);

    // Polymarket contracts are priced in [0,1] USDC/share.
    let price = polymarket_price.clamp(0.01, 0.99);
    let notional = order_notional_usd.max(0.0);

    // Official Polymarket taker fee model on fee-enabled markets:
    // fee_usdc = C * fee_rate * p * (1 - p)
    // where C = shares, p = probability price.
    let taker_fee_bps = if polymarket_fees_enabled && notional > 0.0 {
        let shares = notional / price;
        let fee_usdc = shares * polymarket_fee_rate.max(0.0) * price * (1.0 - price);
        (fee_usdc / notional) * 10_000.0
    } else {
        0.0
    };

    let slippage_bps = costs.slippage_bps.max(0.0) * (1.0 + (1.0 - liq));
    let latency_bps = costs.latency_penalty_bps.max(0.0);
    let settlement_bps = costs.settlement_buffer_bps.max(0.0);
    let explicit_costs_bps = taker_fee_bps + slippage_bps + latency_bps + settlement_bps;

    let extremity = ((price - 0.5).abs() / 0.5).clamp(0.0, 1.0);
    let adverse_selection_bps = costs.adverse_selection_bps.max(0.0)
        * (1.0 + 0.5 * (1.0 - liq) + 0.5 * extremity);

    // Impact rises with size in shares and illiquidity.
    let shares = if price > 0.0 { notional / price } else { 0.0 };
    let impact_bps = costs.impact_coeff_bps.max(0.0)
        * shares.sqrt()
        * (1.0 - liq).powi(2)
        / 10.0;

    let net_edge_bps = decayed_alpha_bps - explicit_costs_bps - adverse_selection_bps - impact_bps;

    CostBreakdown {
        decayed_alpha_bps,
        taker_fee_bps,
        explicit_costs_bps,
        adverse_selection_bps,
        impact_bps,
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
