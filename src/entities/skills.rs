#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillType {
    Fist,
    Club,
    Sword,
    Axe,
    Distance,
    Shielding,
    Fishing,
    Magic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillLevel {
    pub level: u16,
    pub progress: u8,
}

impl Default for SkillLevel {
    fn default() -> Self {
        Self { level: 10, progress: 0 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSet {
    pub fist: SkillLevel,
    pub club: SkillLevel,
    pub sword: SkillLevel,
    pub axe: SkillLevel,
    pub distance: SkillLevel,
    pub shielding: SkillLevel,
    pub fishing: SkillLevel,
    pub magic: SkillLevel,
}

impl Default for SkillSet {
    fn default() -> Self {
        Self {
            fist: SkillLevel::default(),
            club: SkillLevel::default(),
            sword: SkillLevel::default(),
            axe: SkillLevel::default(),
            distance: SkillLevel::default(),
            shielding: SkillLevel::default(),
            fishing: SkillLevel::default(),
            magic: SkillLevel { level: 0, progress: 0 },
        }
    }
}

impl SkillSet {
    pub fn get(&self, skill: SkillType) -> SkillLevel {
        match skill {
            SkillType::Fist => self.fist,
            SkillType::Club => self.club,
            SkillType::Sword => self.sword,
            SkillType::Axe => self.axe,
            SkillType::Distance => self.distance,
            SkillType::Shielding => self.shielding,
            SkillType::Fishing => self.fishing,
            SkillType::Magic => self.magic,
        }
    }
}

pub fn skill_id_for_type(skill: SkillType) -> u32 {
    match skill {
        SkillType::Magic => 1,
        SkillType::Shielding => 6,
        SkillType::Distance => 7,
        SkillType::Sword => 8,
        SkillType::Club => 9,
        SkillType::Axe => 10,
        SkillType::Fist => 11,
        SkillType::Fishing => 13,
    }
}

pub fn skill_exp_for_level(level: i32, base: i32) -> Option<i64> {
    if !(1..=0x1f4).contains(&level) || base <= 0 {
        return None;
    }
    let level = i64::from(level);
    let base = i64::from(base);
    let numerator = (level * (level - 6) + 17) * level - 12;
    Some(numerator / 3 * base)
}

pub fn skill_progress_from_values(values: &[i32; RAW_SKILL_FIELDS]) -> u8 {
    let level = values[SKILL_FIELD_ACT];
    let exp_current = i64::from(values[SKILL_FIELD_EXP]);
    let exp_next = i64::from(values[SKILL_FIELD_NEXT_LEVEL]);
    let exp_base = values[SKILL_FIELD_DELTA];
    let exp_level = match skill_exp_for_level(level, exp_base) {
        Some(value) => value,
        None => return 0,
    };
    if exp_next <= exp_level || exp_current < exp_level {
        return 0;
    }
    let denom = exp_next - exp_level;
    if denom <= 0 {
        return 0;
    }
    let numer = exp_current - exp_level;
    let progress = (numer.saturating_mul(100) / denom).clamp(0, 100);
    progress as u8
}

pub fn apply_skill_progress_values(values: &mut [i32; RAW_SKILL_FIELDS], level: u16, progress: u8) {
    let base = values[SKILL_FIELD_DELTA];
    let Some(exp_level) = skill_exp_for_level(i32::from(level), base) else {
        return;
    };
    let Some(exp_next) = skill_exp_for_level(i32::from(level.saturating_add(1)), base) else {
        return;
    };
    let exp_diff = exp_next.saturating_sub(exp_level);
    let progress = i64::from(progress.clamp(0, 100));
    let exp_current = exp_level.saturating_add(exp_diff.saturating_mul(progress) / 100);
    values[SKILL_FIELD_EXP] = exp_current.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    values[SKILL_FIELD_NEXT_LEVEL] = exp_next.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
}

pub const RAW_SKILL_FIELDS: usize = 14;
pub const SKILL_FIELD_ACT: usize = 0;
pub const SKILL_FIELD_MAX: usize = 1;
pub const SKILL_FIELD_MIN: usize = 2;
pub const SKILL_FIELD_DACT: usize = 3;
pub const SKILL_FIELD_MDACT: usize = 4;
pub const SKILL_FIELD_CYCLE: usize = 5;
pub const SKILL_FIELD_MAX_CYCLE: usize = 6;
pub const SKILL_FIELD_COUNT: usize = 7;
pub const SKILL_FIELD_MAX_COUNT: usize = 8;
pub const SKILL_FIELD_ADD_LEVEL: usize = 9;
pub const SKILL_FIELD_EXP: usize = 10;
pub const SKILL_FIELD_FACTOR_PERCENT: usize = 11;
pub const SKILL_FIELD_NEXT_LEVEL: usize = 12;
pub const SKILL_FIELD_DELTA: usize = 13;

pub const SKILL_FED: u32 = 14;
pub const SKILL_LIGHT: u32 = 15;
pub const SKILL_ILLUSION: u32 = 16;
pub const SKILL_POISON: u32 = 17;
pub const SKILL_BURNING: u32 = 18;
pub const SKILL_ENERGY: u32 = 19;
pub const SKILL_DRUNKEN: u32 = 20;
pub const SKILL_MANASHIELD: u32 = 21;
pub const SKILL_SOUL: u32 = 22;

pub fn default_skill_row_values() -> [i32; RAW_SKILL_FIELDS] {
    let mut values = [0i32; RAW_SKILL_FIELDS];
    values[SKILL_FIELD_MAX] = i32::MAX;
    values[SKILL_FIELD_FACTOR_PERCENT] = 1000;
    values[SKILL_FIELD_NEXT_LEVEL] = i32::MAX;
    values[SKILL_FIELD_DELTA] = i32::MAX;
    values
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRow {
    pub skill_id: u32,
    pub values: [i32; RAW_SKILL_FIELDS],
}

impl SkillRow {
    pub fn new(skill_id: u32, values: [i32; RAW_SKILL_FIELDS]) -> Self {
        Self { skill_id, values }
    }
}
