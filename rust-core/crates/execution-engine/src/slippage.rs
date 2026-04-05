#[derive(Debug, Clone, Copy)]
pub struct SlippageModel {
    pub base_bps: f64,
    pub impact_coeff_bps_per_usd: f64,
}

impl Default for SlippageModel {
    fn default() -> Self {
        Self {
            base_bps: 2.0,
            impact_coeff_bps_per_usd: 0.0005,
        }
    }
}

pub fn estimate_slippage_bps(order_notional_usd: f64, model: SlippageModel) -> f64 {
    model.base_bps + (order_notional_usd.max(0.0) * model.impact_coeff_bps_per_usd)
}

pub fn apply_slippage(price: f64, slippage_bps: f64, is_buy: bool) -> f64 {
    let multiplier = if is_buy {
        1.0 + slippage_bps / 10_000.0
    } else {
        1.0 - slippage_bps / 10_000.0
    };
    price * multiplier
}
