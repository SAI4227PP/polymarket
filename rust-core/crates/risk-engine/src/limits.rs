use common::RiskDecision;

#[derive(Debug, Clone, Copy)]
pub struct RiskLimits {
    pub max_notional_usd: f64,
    pub max_daily_loss_usd: f64,
    pub max_open_positions: usize,
    pub max_leverage: f64,
    pub max_var_budget_usd: f64,
    pub max_concentration_ratio: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct RiskInput {
    pub notional_usd: f64,
    pub day_pnl_usd: f64,
    pub open_positions: usize,
    pub gross_exposure_usd: f64,
    pub account_equity_usd: f64,
    pub realized_vol_bps: f64,
    pub largest_position_usd: f64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_notional_usd: 1000.0,
            max_daily_loss_usd: 100.0,
            max_open_positions: 3,
            max_leverage: 2.5,
            max_var_budget_usd: 150.0,
            max_concentration_ratio: 0.5,
        }
    }
}

pub fn evaluate_trade(input: RiskInput, limits: RiskLimits) -> RiskDecision {
    let mut violations = Vec::new();

    if !input.notional_usd.is_finite() || input.notional_usd <= 0.0 {
        violations.push("invalid notional".to_string());
    }
    if !input.account_equity_usd.is_finite() || input.account_equity_usd <= 0.0 {
        violations.push("invalid account equity".to_string());
    }
    if !input.gross_exposure_usd.is_finite() || input.gross_exposure_usd < 0.0 {
        violations.push("invalid gross exposure".to_string());
    }
    if !input.realized_vol_bps.is_finite() || input.realized_vol_bps < 0.0 {
        violations.push("invalid realized volatility".to_string());
    }

    if input.notional_usd > limits.max_notional_usd {
        violations.push("notional limit exceeded".to_string());
    }
    if input.day_pnl_usd <= -limits.max_daily_loss_usd {
        violations.push("daily loss limit exceeded".to_string());
    }
    if input.open_positions >= limits.max_open_positions {
        violations.push("max open positions reached".to_string());
    }

    let leverage = input.gross_exposure_usd / input.account_equity_usd.max(1e-9);
    if leverage > limits.max_leverage {
        violations.push("leverage threshold exceeded".to_string());
    }

    let var_proxy_usd = input.gross_exposure_usd * (input.realized_vol_bps / 10_000.0) * 2.33;
    if var_proxy_usd > limits.max_var_budget_usd {
        violations.push("var budget exceeded".to_string());
    }

    let concentration = if input.gross_exposure_usd > 0.0 {
        (input.largest_position_usd.abs() / input.gross_exposure_usd.abs()).clamp(0.0, 1.0)
    } else {
        0.0
    };
    if concentration > limits.max_concentration_ratio {
        violations.push("concentration limit exceeded".to_string());
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
