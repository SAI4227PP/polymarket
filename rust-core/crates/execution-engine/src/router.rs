use common::TradeDirection;

pub fn route_order(direction: TradeDirection) -> &'static str {
    match direction {
        TradeDirection::BuyPolymarketSellBinance => "buy_polymarket_sell_binance",
        TradeDirection::SellPolymarketBuyBinance => "sell_polymarket_buy_binance",
        TradeDirection::Flat => "flat",
    }
}
