use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CombatRules {
    pub pvp_enabled: bool,
    pub fight_timer: Duration,
    pub white_skull_timer: Duration,
}

impl Default for CombatRules {
    fn default() -> Self {
        Self {
            pvp_enabled: true,
            fight_timer: Duration::from_secs(60),
            white_skull_timer: Duration::from_secs(60),
        }
    }
}
