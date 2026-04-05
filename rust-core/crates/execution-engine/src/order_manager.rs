use common::Signal;

pub fn submit_if_needed(signal: &Signal) -> Option<&'static str> {
    if signal.should_trade {
        Some("order-submitted")
    } else {
        None
    }
}
