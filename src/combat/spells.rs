use crate::entities::creature::{CreatureId, Outfit};
use crate::entities::player::PlayerId;
use crate::world::position::Position;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpellTargetId {
    Player(PlayerId),
    Monster(CreatureId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpellHit {
    pub target: SpellTargetId,
    pub attempted_damage: u32,
    pub applied_damage: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellSpeedUpdate {
    pub id: u32,
    pub speed: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellLightUpdate {
    pub id: u32,
    pub level: u8,
    pub color: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpellTextEffect {
    pub position: Position,
    pub color: u8,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellOutfitUpdate {
    pub id: u32,
    pub outfit: Outfit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpellCastMessage {
    pub message_type: u8,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpellCastReport {
    pub positions: Vec<Position>,
    pub hits: Vec<SpellHit>,
    pub text_effects: Vec<SpellTextEffect>,
    pub messages: Vec<SpellCastMessage>,
    pub speed_updates: Vec<SpellSpeedUpdate>,
    pub light_updates: Vec<SpellLightUpdate>,
    pub outfit_updates: Vec<SpellOutfitUpdate>,
    pub refresh_map: bool,
}
