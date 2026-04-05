pub fn route_order(edge_bps: f64) -> &'static str {
    if edge_bps >= 0.0 {
        "buy_polymarket_sell_binance"
    } else {
        "sell_polymarket_buy_binance"
    }
}
