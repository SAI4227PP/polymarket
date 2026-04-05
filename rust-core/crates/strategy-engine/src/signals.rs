use common::{Quote, Signal, TradeDirection};

use crate::arbitrage::{expected_edge_after_costs, CostModel};
use crate::fair_value::fair_value_with_basis;

#[derive(Debug, Clone, Copy)]
pub struct SignalConfig {
    pub min_edge_bps: f64,
    pub anchor_price: f64,
    pub basis_bps: f64,
    pub costs: CostModel,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            min_edge_bps: 8.0,
            anchor_price: 100_000.0,
            basis_bps: 0.0,
            costs: CostModel::default(),
        }
    }
}

pub fn compute_signal(pm: &Quote, bn: &Quote, cfg: SignalConfig) -> Signal {
    let fair_value = fair_value_with_basis(bn, cfg.basis_bps, cfg.anchor_price);
    let gross_edge = fair_value - pm.price;
    let gross_edge_bps = gross_edge * 10_000.0;
    let net_edge_bps = expected_edge_after_costs(gross_edge_bps.abs(), cfg.costs);

    let direction = if gross_edge_bps > 0.0 {
        TradeDirection::BuyPolymarketSellBinance
    } else if gross_edge_bps < 0.0 {
        TradeDirection::SellPolymarketBuyBinance
    } else {
        TradeDirection::Flat
    };

    Signal {
        edge_bps: gross_edge_bps,
        net_edge_bps,
        should_trade: net_edge_bps >= cfg.min_edge_bps && direction != TradeDirection::Flat,
        direction,
    }
}
