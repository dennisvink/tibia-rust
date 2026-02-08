use crate::combat::damage::DamageType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub health: u32,
    pub max_health: u32,
    pub mana: u32,
    pub max_mana: u32,
    pub soul: u32,
    pub capacity: u32,
    pub resistances: DamageResistances,
}

impl Stats {
    pub fn base_for_profession(profession: u8) -> Self {
        let (mana, soul) = match profession {
            11 | 12 => (0, 200),
            13 | 14 => (0, 200),
            1 | 2 | 3 | 4 => (0, 100),
            _ => (0, 100),
        };
        let health = 150;
        let capacity = 400;
        Self {
            health,
            max_health: health,
            mana,
            max_mana: mana,
            soul,
            capacity,
            resistances: DamageResistances::default(),
        }
    }

    pub fn apply_damage(&mut self, damage_type: DamageType, amount: u32) -> u32 {
        let adjusted = self.resistances.apply(damage_type, amount);
        self.apply_raw_damage(adjusted)
    }

    pub fn apply_raw_damage(&mut self, amount: u32) -> u32 {
        let applied = amount.min(self.health);
        self.health = self.health.saturating_sub(applied);
        applied
    }

    pub fn apply_heal(&mut self, amount: u32) -> u32 {
        if self.max_health == 0 {
            return 0;
        }
        let before = self.health;
        let max = self.max_health;
        let new = before.saturating_add(amount).min(max);
        self.health = new;
        new.saturating_sub(before)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DamageResistances {
    percents: [i16; DamageType::COUNT],
}

impl DamageResistances {
    pub fn from_array(values: [i16; DamageType::COUNT]) -> Self {
        Self { percents: values }
    }

    pub fn to_array(&self) -> [i16; DamageType::COUNT] {
        self.percents
    }

    pub fn apply(&self, damage_type: DamageType, amount: u32) -> u32 {
        let Some(index) = damage_type.index() else {
            return amount;
        };
        let percent = self.percents[index];
        let percent = percent.clamp(-100, 100) as i64;
        let base = amount as i64;
        let adjusted = base - (base * percent / 100);
        adjusted.clamp(0, i64::from(u32::MAX)) as u32
    }
}

impl Default for DamageResistances {
    fn default() -> Self {
        Self {
            percents: [0; DamageType::COUNT],
        }
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::base_for_profession(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resistances_with_physical(value: i16) -> DamageResistances {
        let mut values = [0i16; DamageType::COUNT];
        values[0] = value;
        DamageResistances::from_array(values)
    }

    #[test]
    fn damage_resistance_reduces_damage() {
        let res = resistances_with_physical(50);
        assert_eq!(res.apply(DamageType::Physical, 100), 50);
    }

    #[test]
    fn damage_resistance_increases_damage_for_negative_values() {
        let res = resistances_with_physical(-50);
        assert_eq!(res.apply(DamageType::Physical, 100), 150);
    }

    #[test]
    fn damage_resistance_clamps_percent() {
        let res = resistances_with_physical(150);
        assert_eq!(res.apply(DamageType::Physical, 100), 0);
    }
}
