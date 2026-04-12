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


