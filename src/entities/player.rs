use crate::entities::creature::{CreatureId, Outfit, DEFAULT_OUTFIT};
use crate::combat::damage::DamageType;
use crate::entities::effects::{
    DrunkenEffect,
    LightEffect,
    MagicShieldEffect,
    OutfitEffect,
    SpeedEffect,
    StrengthEffect,
};
use crate::entities::inventory::{Inventory, InventorySlot};
use crate::entities::item::{ItemStack, ItemTypeId};
use crate::entities::skills::{SkillRow, SkillSet};
use crate::entities::spells::{Spell, SpellGroupId, SpellId};
use crate::entities::stats::Stats;
use crate::world::position::{Direction, Position};
use crate::world::time::{Cooldown, GameClock, GameTick};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use crate::world::viewport::{Viewport, ViewportSize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlayerId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkullState {
    None,
    White,
    Red,
    Black,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PvpStatus {
    pub skull: SkullState,
    pub skull_expires_at: Option<GameTick>,
    pub fight_expires_at: Option<GameTick>,
    pub last_attack: Option<GameTick>,
}

impl Default for PvpStatus {
    fn default() -> Self {
        Self {
            skull: SkullState::None,
            skull_expires_at: None,
            fight_expires_at: None,
            last_attack: None,
        }
    }
}

impl PvpStatus {
    fn refresh(&mut self, now: GameTick) {
        if let Some(until) = self.fight_expires_at {
            if now >= until {
                self.fight_expires_at = None;
            }
        }
        if let Some(until) = self.skull_expires_at {
            if now >= until {
                self.skull_expires_at = None;
                self.skull = SkullState::None;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FightModes {
    pub attack_mode: u8,
    pub chase_mode: u8,
    pub secure_mode: bool,
}

impl Default for FightModes {
    fn default() -> Self {
        Self {
            attack_mode: 1,
            chase_mode: 0,
            secure_mode: false,
        }
    }
}

impl FightModes {
    pub fn from_client(attack_mode: u8, chase_mode: u8, secure_mode: u8) -> Self {
        Self {
            attack_mode: attack_mode.clamp(1, 3),
            chase_mode: chase_mode.min(1),
            secure_mode: secure_mode != 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerState {
    pub id: PlayerId,
    pub name: String,
    pub guild_id: Option<u32>,
    pub guild_name: Option<String>,
    pub race: u8,
    pub level: u16,
    pub experience: u64,
    pub profession: u8,
    pub premium: bool,
    pub is_gm: bool,
    pub is_test_god: bool,
    pub skills: SkillSet,
    pub raw_skills: Vec<SkillRow>,
    pub learning_points: u8,
    pub stats: Stats,
    pub base_speed: u16,
    pub position: Position,
    pub start_position: Position,
    pub direction: Direction,
    pub viewport: Viewport,
    pub original_outfit: Outfit,
    pub current_outfit: Outfit,
    pub outfit_effect: Option<OutfitEffect>,
    pub last_login: u64,
    pub last_logout: u64,
    pub playerkiller_end: u64,
    pub murders: Vec<u64>,
    pub inventory: Inventory,
    pub inventory_containers: HashMap<InventorySlot, Vec<ItemStack>>,
    pub quest_values: HashMap<u16, i32>,
    pub depots: HashMap<u16, Vec<ItemStack>>,
    pub active_depot: Option<ActiveDepot>,
    pub buddies: HashSet<PlayerId>,
    pub party_id: Option<u32>,
    pub open_containers: HashMap<u8, OpenContainer>,
    pub npc_topics: HashMap<CreatureId, i64>,
    pub npc_vars: HashMap<CreatureId, HashMap<String, i64>>,
    pub conditions: Vec<ConditionInstance>,
    pub drunken_effect: Option<DrunkenEffect>,
    pub strength_effect: Option<StrengthEffect>,
    pub speed_effect: Option<SpeedEffect>,
    pub light_effect: Option<LightEffect>,
    pub magic_shield_effect: Option<MagicShieldEffect>,
    pub known_spells: HashSet<SpellId>,
    pub spell_cooldowns: HashMap<SpellId, Cooldown>,
    pub group_cooldowns: HashMap<SpellGroupId, Cooldown>,
    pub move_cooldown: Cooldown,
    pub attack_cooldown: Cooldown,
    pub defend_cooldown: Cooldown,
    pub food_expires_at: Option<GameTick>,
    pub food_hp_cooldown: Cooldown,
    pub food_mana_cooldown: Cooldown,
    pub pvp: PvpStatus,
    pub fight_modes: FightModes,
    pub attack_target: Option<CreatureId>,
    pub follow_target: Option<CreatureId>,
    pub autowalk_steps: VecDeque<Direction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveDepot {
    pub depot_id: u16,
    pub locker_position: Position,
    pub locker_type: ItemTypeId,
    pub capacity: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenContainer {
    pub container_id: u8,
    pub item_type: ItemTypeId,
    pub name: String,
    pub capacity: u8,
    pub has_parent: bool,
    pub parent_container_id: Option<u8>,
    pub parent_slot: Option<u8>,
    pub source_slot: Option<InventorySlot>,
    pub source_position: Option<Position>,
    pub source_stack_pos: Option<u8>,
    pub items: Vec<ItemStack>,
}

impl PlayerState {
    pub fn new(id: PlayerId, name: String, position: Position) -> Self {
        let viewport = Viewport::from_center(position, ViewportSize::default());
        Self {
            id,
            name,
            guild_id: None,
            guild_name: None,
            race: 0,
            level: 1,
            experience: 0,
            profession: 0,
            premium: true,
            is_gm: false,
            is_test_god: false,
            skills: SkillSet::default(),
            raw_skills: Vec::new(),
            learning_points: 0,
            stats: Stats::default(),
            base_speed: 220,
            position,
            start_position: position,
            direction: Direction::South,
            viewport,
            original_outfit: DEFAULT_OUTFIT,
            current_outfit: DEFAULT_OUTFIT,
            outfit_effect: None,
            last_login: 0,
            last_logout: 0,
            playerkiller_end: 0,
            murders: Vec::new(),
            inventory: Inventory::default(),
            inventory_containers: HashMap::new(),
            quest_values: HashMap::new(),
            depots: HashMap::new(),
            active_depot: None,
            buddies: HashSet::new(),
            party_id: None,
            open_containers: HashMap::new(),
            npc_topics: HashMap::new(),
            npc_vars: HashMap::new(),
            conditions: Vec::new(),
            drunken_effect: None,
            strength_effect: None,
            speed_effect: None,
            light_effect: None,
            magic_shield_effect: None,
            known_spells: HashSet::new(),
            spell_cooldowns: HashMap::new(),
            group_cooldowns: HashMap::new(),
            move_cooldown: Cooldown::new(GameTick(0)),
            attack_cooldown: Cooldown::new(GameTick(0)),
            defend_cooldown: Cooldown::new(GameTick(0)),
            food_expires_at: None,
            food_hp_cooldown: Cooldown::new(GameTick(0)),
            food_mana_cooldown: Cooldown::new(GameTick(0)),
            pvp: PvpStatus::default(),
            fight_modes: FightModes::default(),
            attack_target: None,
            follow_target: None,
            autowalk_steps: VecDeque::new(),
        }
    }

    pub fn next_container_id(&self) -> Option<u8> {
        for id in 0u8..16 {
            if !self.open_containers.contains_key(&id) {
                return Some(id);
            }
        }
        None
    }

    pub fn clamp_outfits(&mut self) {
        let (first, last) = self.outfit_bounds();
        self.current_outfit = clamp_outfit(self.current_outfit, first, last);
        self.original_outfit = clamp_outfit(self.original_outfit, first, last);
    }

    pub fn clamp_outfit(&self, outfit: Outfit) -> Outfit {
        let (first, last) = self.outfit_bounds();
        clamp_outfit(outfit, first, last)
    }

    fn outfit_bounds(&self) -> (u16, u16) {
        let (first, last) = if self.race == 0 {
            (128u16, 131u16)
        } else {
            (136u16, 139u16)
        };
        let last = if self.premium {
            last.saturating_add(3)
        } else {
            last
        };
        (first, last)
    }

    pub fn open_container(&mut self, container: OpenContainer) -> Result<(), String> {
        if self.open_containers.contains_key(&container.container_id) {
            return Err("container already open".to_string());
        }
        self.open_containers
            .insert(container.container_id, container);
        Ok(())
    }

    pub fn close_container(&mut self, container_id: u8) -> bool {
        self.open_containers.remove(&container_id).is_some()
    }

    pub fn move_to(&mut self, position: Position, direction: Direction) {
        self.position = position;
        self.direction = direction;
        self.viewport = Viewport::from_center(position, ViewportSize::default());
    }

    pub fn add_experience(&mut self, amount: u32) {
        self.experience = self.experience.saturating_add(u64::from(amount));
        self.sync_level_skill_row();
    }

    pub fn add_condition(&mut self, condition: ConditionInstance) {
        if let Some(existing) = self
            .conditions
            .iter_mut()
            .find(|current| current.kind == condition.kind)
        {
            existing.merge_from(condition);
            return;
        }
        self.conditions.push(condition);
    }

    pub fn clear_condition(&mut self, kind: crate::combat::conditions::ConditionKind) {
        self.conditions.retain(|condition| condition.kind != kind);
    }

    pub fn learn_spell(&mut self, spell_id: SpellId) {
        self.known_spells.insert(spell_id);
    }

    pub fn npc_topic(&self, npc_id: CreatureId) -> i64 {
        self.npc_topics.get(&npc_id).copied().unwrap_or(0)
    }

    pub fn set_npc_topic(&mut self, npc_id: CreatureId, topic: i64) {
        if topic == 0 {
            self.npc_topics.remove(&npc_id);
        } else {
            self.npc_topics.insert(npc_id, topic);
        }
    }

    pub fn npc_var(&self, npc_id: CreatureId, key: &str) -> Option<i64> {
        let key = key.trim().to_ascii_lowercase();
        self.npc_vars
            .get(&npc_id)
            .and_then(|vars| vars.get(&key).copied())
    }

    pub fn set_npc_var(&mut self, npc_id: CreatureId, key: &str, value: i64) {
        let key = key.trim().to_ascii_lowercase();
        self.npc_vars.entry(npc_id).or_default().insert(key, value);
    }

    pub fn clear_npc_vars(&mut self, npc_id: CreatureId) {
        self.npc_vars.remove(&npc_id);
    }

    pub fn knows_spell(&self, spell_id: SpellId) -> bool {
        self.known_spells.contains(&spell_id)
    }

    fn sync_level_skill_row(&mut self) {
        let Some(row) = self.raw_skills.iter_mut().find(|row| row.skill_id == 0) else {
            return;
        };
        row.values[0] = i32::from(self.level);
        row.values[10] = self.experience.min(i32::MAX as u64) as i32;
        if let Some(exp_next) = exp_for_level(i32::from(self.level) + 1, row.values[13]) {
            row.values[12] = exp_next.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        }
    }

    pub fn tick_conditions(&mut self, now: GameTick) -> Vec<ConditionTick> {
        let mut ticks = Vec::new();
        for condition in &mut self.conditions {
            if let Some(damage) = condition.apply_until(now) {
                let (applied, _) = if self.magic_shield_effect.is_none() {
                    let applied = self.stats.apply_raw_damage(damage);
                    (applied, 0)
                } else {
                    let absorbed = damage.min(self.stats.mana);
                    if absorbed > 0 {
                        self.stats.mana = self.stats.mana.saturating_sub(absorbed);
                    }
                    let remaining = damage.saturating_sub(absorbed);
                    let applied = if remaining > 0 {
                        self.stats.apply_raw_damage(remaining)
                    } else {
                        0
                    };
                    (applied, absorbed)
                };
                ticks.push(ConditionTick {
                    kind: condition.kind,
                    damage_type: condition.damage_type,
                    attempted_damage: damage,
                    applied_damage: applied,
                });
            }
        }
        self.conditions.retain(|condition| !condition.is_expired(now));
        ticks
    }

    pub fn apply_damage_with_magic_shield(
        &mut self,
        _damage_type: DamageType,
        amount: u32,
    ) -> (u32, u32) {
        if amount == 0 {
            return (0, 0);
        }
        if self.magic_shield_effect.is_none() {
            let applied = self.stats.apply_raw_damage(amount);
            return (applied, 0);
        }
        let absorbed = amount.min(self.stats.mana);
        if absorbed > 0 {
            self.stats.mana = self.stats.mana.saturating_sub(absorbed);
        }
        let remaining = amount.saturating_sub(absorbed);
        let applied = if remaining > 0 {
            self.stats.apply_raw_damage(remaining)
        } else {
            0
        };
        (applied, absorbed)
    }

    pub fn check_spell_requirements(
        &self,
        spell: &Spell,
        clock: &GameClock,
    ) -> Result<(), String> {
        self.check_spell_requirements_internal(spell, clock, true)
    }

    pub fn check_spell_requirements_no_costs(
        &self,
        spell: &Spell,
        clock: &GameClock,
    ) -> Result<(), String> {
        self.check_spell_requirements_internal(spell, clock, false)
    }

    fn check_spell_requirements_internal(
        &self,
        spell: &Spell,
        clock: &GameClock,
        require_costs: bool,
    ) -> Result<(), String> {
        if self.level < spell.level_required {
            return Err("spell cast failed: level too low".to_string());
        }
        if self.skills.magic.level < u16::from(spell.magic_level_required) {
            return Err("spell cast failed: magic level too low".to_string());
        }
        if require_costs {
            if self.stats.mana < u32::from(spell.mana_cost) {
                return Err("spell cast failed: insufficient mana".to_string());
            }
            if self.stats.soul < u32::from(spell.soul_cost) {
                return Err("spell cast failed: insufficient soul".to_string());
            }
        }
        if let Some(cooldown) = self.spell_cooldowns.get(&spell.id) {
            if !cooldown.is_ready(clock) {
                return Err("spell cast failed: spell cooldown".to_string());
            }
        }
        if let Some(group) = spell.group {
            if let Some(cooldown) = self.group_cooldowns.get(&group) {
                if !cooldown.is_ready(clock) {
                    return Err("spell cast failed: group cooldown".to_string());
                }
            }
        }
        Ok(())
    }

    pub fn spend_spell_costs(&mut self, spell: &Spell) -> Result<(), String> {
        let mana_cost = u32::from(spell.mana_cost);
        let soul_cost = u32::from(spell.soul_cost);
        if self.stats.mana < mana_cost {
            return Err("spell cast failed: insufficient mana".to_string());
        }
        if self.stats.soul < soul_cost {
            return Err("spell cast failed: insufficient soul".to_string());
        }
        self.stats.mana -= mana_cost;
        self.stats.soul -= soul_cost;
        Ok(())
    }

    pub fn trigger_spell_cooldowns(&mut self, spell: &Spell, clock: &GameClock) {
        if spell.cooldown_ms > 0 {
            let cooldown = Cooldown::from_duration_from_now(
                clock,
                std::time::Duration::from_millis(u64::from(spell.cooldown_ms)),
            );
            self.spell_cooldowns.insert(spell.id, cooldown);
        }
        if let Some(group) = spell.group {
            if spell.group_cooldown_ms > 0 {
                let cooldown = Cooldown::from_duration_from_now(
                    clock,
                    std::time::Duration::from_millis(u64::from(spell.group_cooldown_ms)),
                );
                self.group_cooldowns.insert(group, cooldown);
            }
        }
    }

    pub fn mark_in_combat(&mut self, clock: &GameClock, duration: Duration) {
        let now = clock.now();
        self.pvp.refresh(now);
        self.pvp.last_attack = Some(now);
        if duration.is_zero() {
            return;
        }
        let ticks = clock.ticks_from_duration_round_up(duration);
        let deadline = GameTick(now.0.saturating_add(ticks));
        let next = match self.pvp.fight_expires_at {
            Some(current) => current.max(deadline),
            None => deadline,
        };
        self.pvp.fight_expires_at = Some(next);
    }

    pub fn mark_white_skull(&mut self, clock: &GameClock, duration: Duration) {
        let now = clock.now();
        self.pvp.refresh(now);
        if duration.is_zero() {
            return;
        }
        let ticks = clock.ticks_from_duration_round_up(duration);
        let deadline = GameTick(now.0.saturating_add(ticks));
        let next = match self.pvp.skull_expires_at {
            Some(current) => current.max(deadline),
            None => deadline,
        };
        self.pvp.skull = SkullState::White;
        self.pvp.skull_expires_at = Some(next);
    }

    pub fn in_combat(&mut self, clock: &GameClock) -> bool {
        let now = clock.now();
        self.pvp.refresh(now);
        self.pvp
            .fight_expires_at
            .map(|deadline| now < deadline)
            .unwrap_or(false)
    }
}

fn exp_for_level(level: i32, base: i32) -> Option<i64> {
    if !(1..=0x1f4).contains(&level) || base <= 0 {
        return None;
    }
    let level = i64::from(level);
    let base = i64::from(base);
    let numerator = (level * (level - 6) + 17) * level - 12;
    Some(numerator / 3 * base)
}

fn clamp_outfit(mut outfit: Outfit, first: u16, last: u16) -> Outfit {
    if outfit.look_type < first || outfit.look_type > last {
        outfit.look_type = first;
        outfit.look_item = 0;
    }
    outfit.head = outfit.head.min(132);
    outfit.body = outfit.body.min(132);
    outfit.legs = outfit.legs.min(132);
    outfit.feet = outfit.feet.min(132);
    outfit.addons = outfit.addons.min(3);
    outfit
}
use crate::combat::conditions::{ConditionInstance, ConditionTick};
