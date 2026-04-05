#[derive(Debug, Clone, Copy)]
pub struct KillSwitchConfig {
    pub max_drawdown_pct: f64,
    pub max_error_streak: u32,
}

impl Default for KillSwitchConfig {
    fn default() -> Self {
        Self {
            max_drawdown_pct: 5.0,
            max_error_streak: 5,
        }
    }
}

pub fn should_kill(current_drawdown_pct: f64, error_streak: u32, cfg: KillSwitchConfig) -> bool {
    current_drawdown_pct >= cfg.max_drawdown_pct || error_streak >= cfg.max_error_streak
}
