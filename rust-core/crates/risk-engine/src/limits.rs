use common::RiskDecision;

pub fn check_notional_limit(notional: f64, max_notional: f64) -> RiskDecision {
    if notional <= max_notional {
        RiskDecision {
            allowed: true,
            reason: "within notional".to_string(),
        }
    } else {
        RiskDecision {
            allowed: false,
            reason: "notional limit exceeded".to_string(),
        }
    }
}
