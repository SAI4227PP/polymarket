use common::TradeDirection;

pub fn route_order(direction: TradeDirection) -> &'static str {
    match direction {
        TradeDirection::Up => "polymarket_yes",
        TradeDirection::Down => "polymarket_no",
        TradeDirection::Flat => "flat",
    }
}
