use common::{ExecutionReport, OrderIntent, Signal, TradeDirection};

use crate::router::route_order;

pub fn build_order_intent(pair: &str, signal: &Signal, qty: f64, ref_price: f64) -> Option<OrderIntent> {
    if !signal.should_trade || signal.direction == TradeDirection::Flat || qty <= 0.0 {
        return None;
    }

    Some(OrderIntent {
        pair: pair.to_string(),
        direction: signal.direction,
        quantity: qty,
        limit_price: ref_price,
    })
}

pub fn submit_if_needed(intent: Option<OrderIntent>) -> ExecutionReport {
    match intent {
        Some(order) => ExecutionReport {
            accepted: true,
            status: format!("accepted qty={} price={}", order.quantity, order.limit_price),
            routed_path: route_order(order.direction).to_string(),
        },
        None => ExecutionReport {
            accepted: false,
            status: "skipped".to_string(),
            routed_path: "flat".to_string(),
        },
    }
}
