use common::RiskDecision;

#[derive(Debug, Clone, Copy)]
pub struct RiskLimits {
    pub max_notional_usd: f64,
    pub max_daily_loss_usd: f64,
    pub max_open_positions: usize,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_notional_usd: 1000.0,
            max_daily_loss_usd: 100.0,
            max_open_positions: 3,
        }
    }
}

pub fn evaluate_trade(
    notional_usd: f64,
    day_pnl_usd: f64,
    open_positions: usize,
    limits: RiskLimits,
) -> RiskDecision {
    let mut violations = Vec::new();

    if notional_usd > limits.max_notional_usd {
        violations.push("notional limit exceeded".to_string());
    }
    if day_pnl_usd <= -limits.max_daily_loss_usd {
        violations.push("daily loss limit exceeded".to_string());
    }
    if open_positions >= limits.max_open_positions {
        violations.push("max open positions reached".to_string());
    }

    let allowed = violations.is_empty();
    RiskDecision {
        allowed,
        reason: if allowed {
            "within limits".to_string()
        } else {
            violations.join(", ")
        },
        violations,
    }
}
