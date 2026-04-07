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
    let bn = q(100000.0);
    let cfg = SignalConfig {
        min_edge_bps: 500.0,
        anchor_price: 100000.0,
        basis_bps: 0.0,
        ..SignalConfig::default()
    };

    let s = compute_signal(&pm, &bn, cfg);
    assert!(!s.should_trade);
}

#[test]
fn signal_skips_when_both_probs_near_fair_coin() {
    let pm = q(0.5006);
    let bn = q(100005.0);
    let cfg = SignalConfig {
        min_edge_bps: 2.0,
        anchor_price: 100000.0,
        no_trade_band_bps: 20.0,
        ..SignalConfig::default()
    };

    let s = compute_signal(&pm, &bn, cfg);
    assert!(!s.should_trade);
}

#[test]
fn signal_skips_on_extreme_model_market_divergence() {
    let pm = q(0.20);
    let bn = q(100100.0);
    let cfg = SignalConfig {
        min_edge_bps: 1.0,
        anchor_price: 100000.0,
        max_model_market_divergence_bps: 150.0,
        ..SignalConfig::default()
    };

    let s = compute_signal(&pm, &bn, cfg);
    assert!(!s.should_trade);
}
