use crate::combat::damage::DamageType;
use crate::world::time::GameTick;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConditionKind {
    Poison,
    Fire,
    Energy,
    Drown,
    Freeze,
    Curse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConditionTick {
    pub kind: ConditionKind,
    pub damage_type: DamageType,
    pub attempted_damage: u32,
    pub applied_damage: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConditionInstance {
    pub kind: ConditionKind,
    pub damage_type: DamageType,
    pub tick_damage: u32,
    pub interval_ticks: u64,
    pub next_tick: GameTick,
    pub expires_at: GameTick,
}

impl ConditionInstance {
    pub fn new(
        kind: ConditionKind,
        damage_type: DamageType,
        tick_damage: u32,
        interval_ticks: u64,
        start_tick: GameTick,
        duration_ticks: u64,
    ) -> Self {
        let interval_ticks = interval_ticks.max(1);
        let duration_ticks = duration_ticks.max(1);
        let expires_at = GameTick(start_tick.0.saturating_add(duration_ticks));
        Self {
            kind,
            damage_type,
            tick_damage,
            interval_ticks,
            next_tick: start_tick,
            expires_at,
        }
    }

    pub fn apply_until(&mut self, now: GameTick) -> Option<u32> {
        if now < self.next_tick {
            return None;
        }
        let last_tick = if now >= self.expires_at {
            self.expires_at
        } else {
            now
        };
        if last_tick < self.next_tick {
            return None;
        }
        let available = last_tick.0.saturating_sub(self.next_tick.0);
        let ticks = (available / self.interval_ticks).saturating_add(1);
        let damage = self.tick_damage.saturating_mul(ticks.min(u64::from(u32::MAX)) as u32);
        self.next_tick = GameTick(
            self.next_tick
                .0
                .saturating_add(self.interval_ticks.saturating_mul(ticks)),
        );
        Some(damage)
    }

    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }

    pub fn merge_from(&mut self, other: ConditionInstance) {
        if self.kind != other.kind {
            return;
        }
        if other.expires_at > self.expires_at {
            self.expires_at = other.expires_at;
        }
        if other.next_tick < self.next_tick {
            self.next_tick = other.next_tick;
        }
        self.tick_damage = self.tick_damage.max(other.tick_damage);
        self.interval_ticks = self.interval_ticks.min(other.interval_ticks.max(1));
        self.damage_type = other.damage_type;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn condition_ticks_accumulate_when_skipped() {
        let start = GameTick(10);
        let mut condition = ConditionInstance::new(
            ConditionKind::Poison,
            DamageType::Earth,
            3,
            2,
            start,
            5,
        );
        assert_eq!(condition.apply_until(GameTick(14)), Some(9));
        assert_eq!(condition.apply_until(GameTick(14)), None);
        assert!(condition.is_expired(GameTick(15)));
    }

    #[test]
    fn condition_ticks_follow_interval() {
        let start = GameTick(10);
        let mut condition = ConditionInstance::new(
            ConditionKind::Fire,
            DamageType::Fire,
            4,
            2,
            start,
            5,
        );
        assert_eq!(condition.apply_until(GameTick(10)), Some(4));
        assert_eq!(condition.apply_until(GameTick(11)), None);
        assert_eq!(condition.apply_until(GameTick(12)), Some(4));
        assert_eq!(condition.apply_until(GameTick(14)), Some(4));
        assert!(condition.is_expired(GameTick(15)));
    }
}
