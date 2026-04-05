#[derive(Debug, Clone, Copy)]
pub struct SlippageModel {
    pub base_bps: f64,
    pub linear_impact_bps_per_usd: f64,
    pub sqrt_impact_coeff_bps: f64,
    pub vol_coeff_bps: f64,
}

impl Default for SlippageModel {
    fn default() -> Self {
        Self {
            base_bps: 2.0,
            linear_impact_bps_per_usd: 0.0002,
            sqrt_impact_coeff_bps: 0.35,
            vol_coeff_bps: 0.02,
        }
    }
}

pub fn estimate_slippage_bps(
    order_notional_usd: f64,
    top_book_liquidity_usd: f64,
    realized_vol_bps: f64,
    model: SlippageModel,
) -> f64 {
    let notional = order_notional_usd.max(0.0);
    let liq = top_book_liquidity_usd.max(1.0);
    let participation = (notional / liq).clamp(0.0, 10.0);

    let linear = notional * model.linear_impact_bps_per_usd;
    let sqrt = notional.sqrt() * model.sqrt_impact_coeff_bps;
    let vol = realized_vol_bps.max(0.0) * model.vol_coeff_bps;

    model.base_bps + (linear + sqrt) * (1.0 + participation) + vol
}

pub fn apply_slippage(price: f64, slippage_bps: f64, is_buy: bool) -> f64 {
    let multiplier = if is_buy {
        1.0 + slippage_bps / 10_000.0
    } else {
        1.0 - slippage_bps / 10_000.0
    };
    price * multiplier
}
