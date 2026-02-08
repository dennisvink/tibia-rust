use crate::combat::damage::DamageType;
use crate::entities::creature::Outfit;
use crate::entities::item::ItemStack;
use crate::scripting::monster::{load_monster_script, MonsterLootEntry, MonsterScript};
use crate::scripting::raid::{load_raid_script, RaidCount, RaidPosition, RaidScript, RaidSpawn};
use crate::world::item_types::ItemTypeIndex;
use crate::world::position::{Position, PositionDelta};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Default)]
pub struct MonsterIndex {
    pub scripts: HashMap<String, MonsterScript>,
    pub raids: HashMap<String, RaidScript>,
    pub race_index: HashMap<i64, String>,
}

#[derive(Debug, Default)]
pub struct MonsterValidationReport {
    pub monster_files: usize,
    pub raid_files: usize,
    pub parsed_monsters: usize,
    pub parsed_raids: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaidSpawnPlan {
    pub delay: i64,
    pub race_number: i64,
    pub race_name: Option<String>,
    pub positions: Vec<Position>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterLootTable {
    pub entries: Vec<MonsterLootEntry>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MonsterFlags {
    pub distance_fighting: bool,
    pub kick_boxes: bool,
    pub kick_creatures: bool,
    pub no_burning: bool,
    pub no_convince: bool,
    pub no_energy: bool,
    pub no_hit: bool,
    pub no_illusion: bool,
    pub no_life_drain: bool,
    pub no_paralyze: bool,
    pub no_poison: bool,
    pub no_summon: bool,
    pub see_invisible: bool,
    pub unpushable: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MonsterSkills {
    pub level: i32,
    pub magic: i32,
    pub fist: i32,
    pub club: i32,
    pub sword: i32,
    pub axe: i32,
    pub distance: i32,
    pub shielding: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct LootRng {
    state: u64,
}

impl MonsterIndex {
    pub fn name_by_race(&self, race_number: i64) -> Option<&str> {
        self.race_index
            .get(&race_number)
            .map(String::as_str)
    }

    pub fn script_by_name(&self, name: &str) -> Option<&MonsterScript> {
        let name = name.trim();
        if name.is_empty() {
            return None;
        }
        let normalized = normalize_monster_name(name);
        for (key, script) in &self.scripts {
            if normalize_monster_name(key) == normalized {
                return Some(script);
            }
            if let Some(script_name) = script.name.as_deref() {
                if normalize_monster_name(script_name) == normalized {
                    return Some(script);
                }
            }
        }
        None
    }

    pub fn race_by_name(&self, name: &str) -> Option<i64> {
        self.script_by_name(name).and_then(|script| script.race_number())
    }

    pub fn script_by_race(&self, race_number: i64) -> Option<&MonsterScript> {
        let name = self.race_index.get(&race_number)?;
        self.scripts.get(name)
    }

    pub fn outfit_by_race(&self, race_number: i64) -> Option<Outfit> {
        self.script_by_race(race_number)
            .and_then(|script| script.outfit())
    }
}

fn normalize_monster_name(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

impl MonsterLootTable {
    pub fn roll(&self, rng: &mut LootRng, item_types: Option<&ItemTypeIndex>) -> Vec<ItemStack> {
        let mut drops = Vec::new();
        for entry in &self.entries {
            if !rng.roll_per_mille(entry.chance) {
                continue;
            }
            let stackable = item_types
                .and_then(|index| index.get(entry.type_id))
                .map(|item| item.stackable)
                .unwrap_or(false);
            if entry.count == 0 {
                continue;
            }
            let count = rng.roll_range(1, entry.count);
            if stackable {
                drops.push(ItemStack { id: crate::entities::item::ItemId::next(),
                    type_id: entry.type_id,
                    count,
                    attributes: Vec::new(),
                    contents: Vec::new(),
                });
            } else {
                for _ in 0..count {
                    drops.push(ItemStack { id: crate::entities::item::ItemId::next(),
                        type_id: entry.type_id,
                        count: 1,
                        attributes: Vec::new(),
                        contents: Vec::new(),
                    });
                }
            }
        }
        drops
    }
}

impl MonsterFlags {
    pub fn from_list(list: &[String]) -> Self {
        let mut flags = Self::default();
        for entry in list {
            match entry.as_str() {
                "DistanceFighting" | "Distance" => flags.distance_fighting = true,
                "KickBoxes" => flags.kick_boxes = true,
                "KickCreatures" => flags.kick_creatures = true,
                "NoBurning" => flags.no_burning = true,
                "NoConvince" => flags.no_convince = true,
                "NoEnergy" => flags.no_energy = true,
                "NoHit" => flags.no_hit = true,
                "NoIllusion" => flags.no_illusion = true,
                "NoLifeDrain" => flags.no_life_drain = true,
                "NoParalyze" => flags.no_paralyze = true,
                "NoPoison" => flags.no_poison = true,
                "NoSummon" => flags.no_summon = true,
                "SeeInvisible" => flags.see_invisible = true,
                "Unpushable" => flags.unpushable = true,
                _ => {}
            }
        }
        flags
    }

    pub fn blocks_damage(&self, damage_type: DamageType) -> bool {
        match damage_type {
            DamageType::Physical => self.no_hit,
            DamageType::Energy => self.no_energy,
            DamageType::Earth => self.no_poison,
            DamageType::Fire => self.no_burning,
            DamageType::LifeDrain => self.no_life_drain,
            _ => false,
        }
    }
}

impl MonsterSkills {
    pub fn from_script(script: &MonsterScript) -> Self {
        let value = |name| script.skill_value(name).unwrap_or(0) as i32;
        Self {
            level: value("Level"),
            magic: value("MagicLevel"),
            fist: value("FistFighting"),
            club: value("ClubFighting"),
            sword: value("SwordFighting"),
            axe: value("AxeFighting"),
            distance: value("DistanceFighting"),
            shielding: value("Shielding"),
        }
    }

    pub fn melee_skill(self, flags: MonsterFlags) -> i32 {
        if flags.distance_fighting && self.distance > 0 {
            return self.distance;
        }
        self.fist
            .max(self.club)
            .max(self.sword)
            .max(self.axe)
    }
}

impl LootRng {
    pub fn from_seed(seed: u64) -> Self {
        let seed = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state: seed }
    }

    pub fn roll_per_mille(&mut self, chance: u16) -> bool {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let bucket = (self.state >> 32) as u32 % 1000;
        bucket <= u32::from(chance.min(1000))
    }

    pub fn roll_range(&mut self, min: u16, max: u16) -> u16 {
        let (min, max) = if min >= max { (min, min) } else { (min, max) };
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let span = u64::from(max - min) + 1;
        let value = ((self.state >> 32) as u64) % span;
        min + value as u16
    }
}

impl Default for MonsterLootTable {
    fn default() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Default for LootRng {
    fn default() -> Self {
        Self::from_seed(0x9e3779b97f4a7c15)
    }
}

pub fn build_loot_table(script: &MonsterScript) -> Result<MonsterLootTable, String> {
    Ok(MonsterLootTable {
        entries: script.loot_entries()?,
    })
}

pub fn resolve_raid_spawns(
    index: &MonsterIndex,
    raid: &RaidScript,
    seed: u64,
) -> Result<Vec<RaidSpawnPlan>, String> {
    let mut rng = RaidRng::from_seed(seed);
    raid.spawns
        .iter()
        .map(|spawn| resolve_spawn(index, spawn, &mut rng))
        .collect()
}

fn resolve_spawn(
    index: &MonsterIndex,
    spawn: &RaidSpawn,
    rng: &mut RaidRng,
) -> Result<RaidSpawnPlan, String> {
    let delay = spawn.delay.unwrap_or(0);
    let position = spawn
        .position
        .as_ref()
        .ok_or_else(|| "raid spawn missing Position".to_string())?;
    let race_number = spawn
        .race
        .ok_or_else(|| "raid spawn missing Race".to_string())?;
    let count = resolve_spawn_count(spawn.count.as_ref(), rng)?;
    let spread = spawn.spread.unwrap_or(0).max(0);

    let base = raid_position_to_position(position);
    let mut positions = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let pos = if spread > 0 {
            let offset = roll_spread_offset(rng, spread);
            base.offset(offset).unwrap_or(base)
        } else {
            base
        };
        positions.push(pos);
    }

    Ok(RaidSpawnPlan {
        delay,
        race_number,
        race_name: index.name_by_race(race_number).map(str::to_string),
        positions,
        message: spawn.message.clone(),
    })
}

fn resolve_spawn_count(count: Option<&RaidCount>, rng: &mut RaidRng) -> Result<i64, String> {
    match count {
        None => Ok(1),
        Some(count) => {
            if count.min <= 0 || count.max <= 0 {
                return Err("raid spawn Count must be positive".to_string());
            }
            if count.min > count.max {
                return Err("raid spawn Count min exceeds max".to_string());
            }
            Ok(rng.roll_range_i64(count.min, count.max))
        }
    }
}

fn raid_position_to_position(position: &RaidPosition) -> Position {
    Position {
        x: position.x,
        y: position.y,
        z: position.z,
    }
}

fn roll_spread_offset(rng: &mut RaidRng, spread: i64) -> PositionDelta {
    let spread = spread.min(i64::from(i16::MAX));
    let dx = rng.roll_range_i64(-spread, spread) as i16;
    let dy = rng.roll_range_i64(-spread, spread) as i16;
    PositionDelta { dx, dy, dz: 0 }
}

#[derive(Debug, Clone, Copy)]
struct RaidRng {
    state: u64,
}

impl RaidRng {
    fn from_seed(seed: u64) -> Self {
        let seed = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state: seed }
    }

    fn roll_range_i64(&mut self, min: i64, max: i64) -> i64 {
        if min >= max {
            return min;
        }
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let span = (max - min + 1) as u64;
        let value = ((self.state >> 32) as u64) % span;
        min + value as i64
    }
}

pub fn load_monsters(dir: &Path) -> Result<MonsterIndex, String> {
    let entries = fs::read_dir(dir)
        .map_err(|err| format!("failed to read monster dir {}: {}", dir.display(), err))?;
    let mut scripts = HashMap::new();
    let mut raids = HashMap::new();
    let mut race_index = HashMap::new();

    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read monster dir entry: {}", err))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext != "mon" && ext != "evt" {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| format!("monster script path missing stem: {}", path.display()))?;
        if ext == "evt" {
            let raid = match load_raid_script(&path) {
                Ok(raid) => raid,
                Err(err) => {
                    eprintln!("tibia: raid script skipped: {}", err);
                    continue;
                }
            };
            if raids.contains_key(stem) {
                eprintln!(
                    "tibia: duplicate raid script key {}, skipping {}",
                    stem,
                    path.display()
                );
                continue;
            }
            raids.insert(stem.to_string(), raid);
        } else {
            let script = match load_monster_script(&path) {
                Ok(script) => script,
                Err(err) => {
                    eprintln!("tibia: monster script skipped: {}", err);
                    continue;
                }
            };
            if scripts.contains_key(stem) {
                eprintln!(
                    "tibia: duplicate monster script key {}, skipping {}",
                    stem,
                    path.display()
                );
                continue;
            }
            let race_number = script.race_number();
            if let Some(race_number) = race_number {
                if race_index.contains_key(&race_number) {
                    eprintln!(
                        "tibia: duplicate monster race number {} for {}, skipping {}",
                        race_number,
                        stem,
                        path.display()
                    );
                    continue;
                }
            }
            if let Some(race_number) = race_number {
                race_index.insert(race_number, stem.to_string());
            }
            scripts.insert(stem.to_string(), script);
        }
    }

    Ok(MonsterIndex {
        scripts,
        raids,
        race_index,
    })
}

pub fn validate_monsters(dir: &Path) -> MonsterValidationReport {
    let mut report = MonsterValidationReport::default();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            report.errors.push(format!(
                "failed to read monster dir {}: {}",
                dir.display(),
                err
            ));
            return report;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                report
                    .errors
                    .push(format!("failed to read monster dir entry: {}", err));
                continue;
            }
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext != "mon" && ext != "evt" {
            continue;
        }
        if ext == "evt" {
            report.raid_files += 1;
            match load_raid_script(&path) {
                Ok(_) => report.parsed_raids += 1,
                Err(err) => report.errors.push(format!(
                    "raid script {}: {}",
                    path.display(),
                    err
                )),
            }
        } else {
            report.monster_files += 1;
            match load_monster_script(&path) {
                Ok(_) => report.parsed_monsters += 1,
                Err(err) => report.errors.push(format!(
                    "monster script {}: {}",
                    path.display(),
                    err
                )),
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn resolve_raid_spawns_respects_spread_and_count() {
        let mut index = MonsterIndex::default();
        index.race_index.insert(105, "Badger".to_string());

        let spawn = RaidSpawn {
            delay: Some(1),
            position: Some(RaidPosition { x: 100, y: 200, z: 7 }),
            spread: Some(2),
            race: Some(105),
            count: Some(RaidCount { min: 3, max: 3 }),
            message: Some("Badgers!".to_string()),
            fields: Vec::new(),
        };
        let raid = RaidScript {
            raid_type: Some("SmallRaid".to_string()),
            interval: Some(120),
            spawns: vec![spawn],
            fields: Vec::new(),
        };

        let plans = resolve_raid_spawns(&index, &raid, 1).expect("resolve");
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert_eq!(plan.delay, 1);
        assert_eq!(plan.race_number, 105);
        assert_eq!(plan.race_name.as_deref(), Some("Badger"));
        assert_eq!(plan.positions.len(), 3);

        let base_x = 100i32;
        let base_y = 200i32;
        for pos in &plan.positions {
            let dx = pos.x as i32 - base_x;
            let dy = pos.y as i32 - base_y;
            assert!(dx.abs() <= 2);
            assert!(dy.abs() <= 2);
            assert_eq!(pos.z, 7);
        }
    }

    #[test]
    fn resolve_raid_spawn_requires_position() {
        let index = MonsterIndex::default();
        let spawn = RaidSpawn {
            delay: Some(1),
            position: None,
            spread: None,
            race: Some(1),
            count: None,
            message: None,
            fields: Vec::new(),
        };
        let raid = RaidScript {
            raid_type: None,
            interval: None,
            spawns: vec![spawn],
            fields: Vec::new(),
        };
        let err = resolve_raid_spawns(&index, &raid, 1).expect_err("resolve");
        assert!(err.contains("missing Position"));
    }

    #[test]
    fn monster_loot_table_rolls_per_mille_chance() {
        let table = MonsterLootTable {
            entries: vec![
                MonsterLootEntry {
                    type_id: crate::entities::item::ItemTypeId(3031),
                    count: 10,
                    chance: 1000,
                },
                MonsterLootEntry {
                    type_id: crate::entities::item::ItemTypeId(3492),
                    count: 1,
                    chance: 0,
                },
            ],
        };
        let mut rng = LootRng::from_seed(1);
        let drops = table.roll(&mut rng, None);
        assert_eq!(
            drops,
            vec![
                ItemStack { id: crate::entities::item::ItemId::next(),
                    type_id: crate::entities::item::ItemTypeId(3031),
                    count: 1,
                    attributes: Vec::new(),
                    contents: Vec::new(),
                },
                ItemStack { id: crate::entities::item::ItemId::next(),
                    type_id: crate::entities::item::ItemTypeId(3031),
                    count: 1,
                    attributes: Vec::new(),
                    contents: Vec::new(),
                },
            ]
        );
    }

    #[test]
    fn load_monsters_from_assets() {
        let asset_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mon_dir = asset_root.join("mon");
        let index = load_monsters(&mon_dir).expect("monster load");
        assert!(!index.scripts.is_empty());
        assert!(!index.race_index.is_empty());
    }
}
