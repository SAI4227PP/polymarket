use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioDayStats {
    pub day: String,
    pub start_total_pnl_usd: f64,
    pub current_total_pnl_usd: f64,
    pub day_pnl_usd: f64,
    pub day_max_up_usd: f64,
    pub day_max_drop_usd: f64,
    pub updated_at_ms: u64,
}

pub fn update_daily_history(
    history: &mut HashMap<String, PortfolioDayStats>,
    day: String,
    total_pnl_usd: f64,
    updated_at_ms: u64,
) -> PortfolioDayStats {
    let total = if total_pnl_usd.is_finite() { total_pnl_usd } else { 0.0 };

    let stats = history.entry(day.clone()).or_insert_with(|| PortfolioDayStats {
        day: day.clone(),
        start_total_pnl_usd: total,
        current_total_pnl_usd: total,
        day_pnl_usd: 0.0,
        day_max_up_usd: 0.0,
        day_max_drop_usd: 0.0,
        updated_at_ms,
    });

    stats.current_total_pnl_usd = total;
    stats.day_pnl_usd = total - stats.start_total_pnl_usd;
    if stats.day_pnl_usd > stats.day_max_up_usd {
        stats.day_max_up_usd = stats.day_pnl_usd;
    }
    if stats.day_pnl_usd < stats.day_max_drop_usd {
        stats.day_max_drop_usd = stats.day_pnl_usd;
    }
    stats.updated_at_ms = updated_at_ms;

    stats.clone()
}

#[cfg(test)]
mod tests {
    use super::{update_daily_history, PortfolioDayStats};
    use std::collections::HashMap;

    fn approx_eq(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "left={} right={}", a, b);
    }

    #[test]
    fn tracks_day_pnl_and_intraday_extremes() {
        let mut history: HashMap<String, PortfolioDayStats> = HashMap::new();

        let s1 = update_daily_history(&mut history, "2026-04-07".to_string(), 10.0, 1);
        approx_eq(s1.day_pnl_usd, 0.0);

        let s2 = update_daily_history(&mut history, "2026-04-07".to_string(), 12.5, 2);
        approx_eq(s2.day_pnl_usd, 2.5);
        approx_eq(s2.day_max_up_usd, 2.5);
        approx_eq(s2.day_max_drop_usd, 0.0);

        let s3 = update_daily_history(&mut history, "2026-04-07".to_string(), 8.0, 3);
        approx_eq(s3.day_pnl_usd, -2.0);
        approx_eq(s3.day_max_up_usd, 2.5);
        approx_eq(s3.day_max_drop_usd, -2.0);
    }

    #[test]
    fn non_finite_total_is_treated_as_zero() {
        let mut history: HashMap<String, PortfolioDayStats> = HashMap::new();
        let s = update_daily_history(&mut history, "2026-04-07".to_string(), f64::NAN, 1);
        approx_eq(s.start_total_pnl_usd, 0.0);
        approx_eq(s.current_total_pnl_usd, 0.0);
    }
}

