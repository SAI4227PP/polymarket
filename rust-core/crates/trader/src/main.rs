use anyhow::Result;
use adapters_binance::BinanceClient;
use adapters_polymarket::PolymarketClient;
use execution_engine::{order_manager, router};
use risk_engine::limits;
use strategy_engine::signals;

#[tokio::main]
async fn main() -> Result<()> {
    let pm_asset_id = std::env::var("POLYMARKET_ASSET_ID")
        .unwrap_or_else(|_| "REPLACE_WITH_BTC_ASSET_ID".to_string());
    let bn_symbol = std::env::var("BINANCE_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    let pm = PolymarketClient::new();
    let bn = BinanceClient::new();

    let (pm_quote, bn_quote) = tokio::try_join!(
        pm.best_bid_ask_mid(&pm_asset_id),
        bn.best_bid_ask_mid(&bn_symbol)
    )?;

    let signal = signals::compute_signal(&pm_quote, &bn_quote, 5.0);
    let risk = limits::check_notional_limit(100.0, 1000.0);

    if signal.should_trade && risk.allowed {
        let path = router::route_order(signal.edge_bps);
        let result = order_manager::submit_if_needed(&signal).unwrap_or("no-order");
        println!(
            "trade path={path} status={result} edge_bps={} pm={} bn={}",
            signal.edge_bps, pm_quote.price, bn_quote.price
        );
    } else {
        println!(
            "no trade edge_bps={} risk_allowed={} reason={} pm={} bn={}",
            signal.edge_bps, risk.allowed, risk.reason, pm_quote.price, bn_quote.price
        );
    }

    Ok(())
}
