use common::Quote;

use crate::signals::{compute_signal, SignalConfig};

fn q(price: f64) -> Quote {
    Quote {
        venue: "test".to_string(),
        symbol: "BTC".to_string(),
        bid: price,
        ask: price,
        price,
        ts_ms: 0,
    }
}

#[test]
fn signal_trades_when_edge_exceeds_threshold() {
    let pm = q(0.40);
    let bn = q(90000.0);
    let cfg = SignalConfig {
        min_edge_bps: 5.0,
        anchor_price: 100000.0,
        basis_bps: 0.0,
        ..SignalConfig::default()
    };

    let s = compute_signal(&pm, &bn, cfg);
    assert!(s.should_trade);
}

#[test]
fn signal_does_not_trade_when_edge_small() {
    let pm = q(0.50);
    let bn = q(50000.0);
    let cfg = SignalConfig {
        min_edge_bps: 500.0,
        anchor_price: 100000.0,
        basis_bps: 0.0,
        ..SignalConfig::default()
    };

    let s = compute_signal(&pm, &bn, cfg);
    assert!(!s.should_trade);
}
