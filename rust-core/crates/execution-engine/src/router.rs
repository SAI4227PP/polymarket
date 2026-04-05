use common::TradeDirection;

pub fn route_order(direction: TradeDirection) -> &'static str {
    match direction {
        TradeDirection::Up => "up",
        TradeDirection::Down => "down",
        TradeDirection::Flat => "flat",
    }
}

