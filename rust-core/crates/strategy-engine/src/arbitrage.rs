pub fn expected_edge_after_costs(edge_bps: f64, fees_bps: f64, slippage_bps: f64) -> f64 {
    edge_bps - fees_bps - slippage_bps
}
