use common::{ExecutionReport, OrderIntent, Signal, TradeDirection};

use crate::router::route_order;
use crate::slippage::{apply_slippage, estimate_slippage_bps, SlippageModel};

#[derive(Debug, Clone, Copy)]
pub struct ExecutionConfig {
    pub base_qty: f64,
    pub edge_threshold_bps: f64,
    pub min_net_edge_margin_bps: f64,
    pub max_slippage_bps: f64,
    pub slippage_model: SlippageModel,
    pub demo_wallet_balance_usd: f64,
    pub min_order_notional_usd: f64,
    pub min_order_shares: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            base_qty: 5.0,
            edge_threshold_bps: 5.0,
            min_net_edge_margin_bps: 3.0,
            max_slippage_bps: 80.0,
            slippage_model: SlippageModel::default(),
            demo_wallet_balance_usd: 10.0,
            min_order_notional_usd: 1.0,
            min_order_shares: 5.0,
        }
    }
}

pub fn build_order_intent(
    pair: &str,
    signal: &Signal,
    ref_price: f64,
    order_notional_usd: f64,
    top_book_liquidity_usd: f64,
    realized_vol_bps: f64,
    cfg: ExecutionConfig,
) -> Option<OrderIntent> {
    if !signal.should_trade || signal.direction == TradeDirection::Flat || cfg.base_qty <= 0.0 {
        return None;
    }
    if ref_price <= 0.0 || cfg.demo_wallet_balance_usd <= 0.0 {
        return None;
    }

    let min_required_edge = cfg.edge_threshold_bps + cfg.min_net_edge_margin_bps.max(0.0);
    if signal.net_edge_bps < min_required_edge {
        return None;
    }

    let scaled_qty = size_from_edge_bps(signal.net_edge_bps, cfg.base_qty, cfg.edge_threshold_bps);
    if scaled_qty <= 0.0 {
        return None;
    }

    let base_target_notional_usd = order_notional_usd.max(cfg.min_order_notional_usd);
    // Pre-size notional so minimum-share orders can pass when affordable.
    let pre_slippage_notional_usd = base_target_notional_usd
        .max(cfg.min_order_shares * ref_price)
        .min(cfg.demo_wallet_balance_usd);
    if pre_slippage_notional_usd < cfg.min_order_notional_usd {
        return None;
    }

    let slippage_bps = estimate_slippage_bps(
        pre_slippage_notional_usd,
        top_book_liquidity_usd,
        realized_vol_bps,
        cfg.slippage_model,
    );
    if slippage_bps > cfg.max_slippage_bps.max(0.0) {
        return None;
    }

    let is_buy = signal.direction == TradeDirection::Up;
    let limit_price = apply_slippage(ref_price, slippage_bps, is_buy);
    if limit_price <= 0.0 {
        return None;
    }

    let required_notional_usd = cfg
        .min_order_notional_usd
        .max(cfg.min_order_shares * limit_price);
    let budget_notional_usd = pre_slippage_notional_usd
        .max(required_notional_usd)
        .min(cfg.demo_wallet_balance_usd);
    if budget_notional_usd < required_notional_usd {
        return None;
    }

    let qty_by_notional = budget_notional_usd / limit_price;
    let qty_by_wallet = cfg.demo_wallet_balance_usd / limit_price;
    let desired_qty = scaled_qty.max(cfg.min_order_shares);
    let final_qty = desired_qty.min(qty_by_notional).min(qty_by_wallet);

    if final_qty < cfg.min_order_shares {
        return None;
    }

    let final_notional = final_qty * limit_price;
    if final_notional < cfg.min_order_notional_usd || final_notional > cfg.demo_wallet_balance_usd {
        return None;
    }

    Some(OrderIntent {
        pair: pair.to_string(),
        direction: signal.direction,
        quantity: final_qty,
        limit_price,
    })
}

pub fn submit_if_needed(intent: Option<OrderIntent>) -> ExecutionReport {
    match intent {
        Some(order) => ExecutionReport {
            accepted: true,
            status: format!(
                "accepted qty={} price={} notional={}",
                order.quantity,
                order.limit_price,
                order.quantity * order.limit_price
            ),
            routed_path: route_order(order.direction).to_string(),
        },
        None => ExecutionReport {
            accepted: false,
            status: "skipped".to_string(),
            routed_path: "flat".to_string(),
        },
    }
}

pub fn size_from_edge_bps(net_edge_bps: f64, base_qty: f64, threshold_bps: f64) -> f64 {
    if base_qty <= 0.0 || threshold_bps <= 0.0 {
        return 0.0;
    }
    let score = (net_edge_bps / threshold_bps).clamp(0.0, 3.0);
    base_qty * (0.25 + 0.25 * score)
}




