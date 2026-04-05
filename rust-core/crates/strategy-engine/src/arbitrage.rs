#[derive(Debug, Clone, Copy)]
pub struct CostModel {
    pub taker_fees_bps: f64,
    pub slippage_bps: f64,
    pub latency_penalty_bps: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            taker_fees_bps: 2.0,
            slippage_bps: 3.0,
            latency_penalty_bps: 1.0,
        }
    }
}

pub fn expected_edge_after_costs(gross_edge_bps: f64, costs: CostModel) -> f64 {
    gross_edge_bps - costs.taker_fees_bps - costs.slippage_bps - costs.latency_penalty_bps
}
