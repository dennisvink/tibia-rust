#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamageType {
    Physical,
    Energy,
    Earth,
    Fire,
    LifeDrain,
    ManaDrain,
    Drown,
    Ice,
    Holy,
    Death,
    Unknown(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DamageScaleFlags {
    pub clamp_upper: bool,
    pub clamp_lower: bool,
}

impl DamageScaleFlags {
    pub const NONE: Self = Self {
        clamp_upper: false,
        clamp_lower: false,
    };
}

impl DamageType {
    pub const COUNT: usize = 10;

    pub fn from_mask(mask: u16) -> Self {
        match mask {
            1 => Self::Physical,
            2 => Self::Energy,
            4 => Self::Earth,
            8 => Self::Fire,
            16 => Self::LifeDrain,
            32 => Self::ManaDrain,
            64 => Self::Drown,
            128 => Self::Ice,
            256 => Self::Holy,
            512 => Self::Death,
            other => Self::Unknown(other),
        }
    }

    pub fn mask(self) -> u16 {
        match self {
            Self::Physical => 1,
            Self::Energy => 2,
            Self::Earth => 4,
            Self::Fire => 8,
            Self::LifeDrain => 16,
            Self::ManaDrain => 32,
            Self::Drown => 64,
            Self::Ice => 128,
            Self::Holy => 256,
            Self::Death => 512,
            Self::Unknown(mask) => mask,
        }
    }

    pub fn index(self) -> Option<usize> {
        match self {
            Self::Physical => Some(0),
            Self::Energy => Some(1),
            Self::Earth => Some(2),
            Self::Fire => Some(3),
            Self::LifeDrain => Some(4),
            Self::ManaDrain => Some(5),
            Self::Drown => Some(6),
            Self::Ice => Some(7),
            Self::Holy => Some(8),
            Self::Death => Some(9),
            Self::Unknown(_) => None,
        }
    }
}

pub fn compute_damage(
    base_damage: i32,
    variance: i32,
    skill_a: i32,
    skill_b: i32,
    flags: DamageScaleFlags,
    random_offset: i32,
) -> i32 {
    let mut damage = base_damage;
    if variance != 0 {
        let offset = random_offset.clamp(-variance, variance);
        damage = damage.saturating_add(offset);
    }
    let mut factor = skill_a.saturating_mul(2).saturating_add(skill_b.saturating_mul(3));
    if flags.clamp_upper && factor >= 101 {
        factor = 100;
    }
    if flags.clamp_lower && factor <= 99 {
        factor = 100;
    }
    let scaled = (i64::from(damage) * i64::from(factor)) / 100;
    scaled.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_damage_applies_variance() {
        let damage = compute_damage(10, 4, 50, 0, DamageScaleFlags::NONE, 3);
        assert_eq!(damage, 13);
    }

    #[test]
    fn compute_damage_clamps_upper_factor() {
        let flags = DamageScaleFlags {
            clamp_upper: true,
            clamp_lower: false,
        };
        let damage = compute_damage(100, 0, 50, 1, flags, 0);
        assert_eq!(damage, 100);
    }

    #[test]
    fn compute_damage_clamps_lower_factor() {
        let flags = DamageScaleFlags {
            clamp_upper: false,
            clamp_lower: true,
        };
        let damage = compute_damage(10, 0, 30, 0, flags, 0);
        assert_eq!(damage, 10);
    }
}
