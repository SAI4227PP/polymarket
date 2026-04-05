#[derive(Debug, Clone, Copy)]
pub struct KillSwitchConfig {
    pub max_drawdown_pct: f64,
    pub max_error_streak: u32,
    pub max_stale_data_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct KillSwitchState {
    pub current_drawdown_pct: f64,
    pub error_streak: u32,
    pub oldest_quote_age_ms: u64,
}

impl Default for KillSwitchConfig {
    fn default() -> Self {
        Self {
            max_drawdown_pct: 5.0,
            max_error_streak: 5,
            max_stale_data_ms: 15_000,
        }
    }
}

pub fn should_kill(state: KillSwitchState, cfg: KillSwitchConfig) -> bool {
    state.current_drawdown_pct >= cfg.max_drawdown_pct
        || state.error_streak >= cfg.max_error_streak
        || state.oldest_quote_age_ms >= cfg.max_stale_data_ms
}
