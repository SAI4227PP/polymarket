pub fn map_market_to_symbol(market_id: &str) -> String {
    format!("PM-{}", market_id)
}
