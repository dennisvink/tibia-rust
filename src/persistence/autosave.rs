use crate::persistence::store::SaveStore;
use crate::world::state::WorldState;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutosaveConfig {
    pub interval_seconds: u64,
}

impl AutosaveConfig {
    pub fn interval(self) -> Option<Duration> {
        if self.interval_seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(self.interval_seconds.max(1)))
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutosaveState {
    interval: Option<Duration>,
    next_due: Option<Instant>,
}

impl AutosaveState {
    pub fn new(config: AutosaveConfig, now: Instant) -> Self {
        let interval = config.interval();
        let next_due = interval.map(|interval| now + interval);
        Self { interval, next_due }
    }

    pub fn due(&self, now: Instant) -> bool {
        self.next_due.map_or(false, |next| now >= next)
    }

    pub fn mark_saved(&mut self, now: Instant) {
        if let Some(interval) = self.interval {
            self.next_due = Some(now + interval);
        }
    }
}

#[derive(Debug, Default)]
pub struct AutosaveReport {
    pub saved_players: usize,
    pub player_errors: Vec<String>,
    pub house_owner_error: Option<String>,
}

pub fn autosave_world(
    world: &WorldState,
    store: &SaveStore,
    root: &Path,
) -> AutosaveReport {
    let mut report = AutosaveReport::default();
    for player in world.players.values() {
        let snapshot = world.player_for_save(player);
        match store.save_player(&snapshot) {
            Ok(()) => report.saved_players += 1,
            Err(err) => report.player_errors.push(err),
        }
    }
    if let Err(err) = world.save_house_owners(root) {
        report.house_owner_error = Some(err);
    }
    report
}
