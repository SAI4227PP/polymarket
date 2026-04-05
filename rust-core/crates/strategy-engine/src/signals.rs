use common::{Quote, Signal};

pub fn compute_signal(pm: &Quote, bn: &Quote, min_edge_bps: f64) -> Signal {
    let bn_norm = bn.price / 100000.0;
    let edge = bn_norm - pm.price;
    let edge_bps = edge * 10_000.0;
    Signal {
        edge_bps,
        should_trade: edge_bps.abs() >= min_edge_bps,
    }
}
