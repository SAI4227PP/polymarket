pub fn should_kill(current_drawdown_pct: f64, kill_threshold_pct: f64) -> bool {
    current_drawdown_pct >= kill_threshold_pct
}
