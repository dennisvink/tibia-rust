use crate::entities::stats::Stats;
use crate::world::position::Position;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CreatureId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatureKind {
    Player,
    Npc,
    Monster,
    Summon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outfit {
    pub look_type: u16,
    pub head: u8,
    pub body: u8,
    pub legs: u8,
    pub feet: u8,
    pub addons: u8,
    pub look_item: u16,
}

pub const DEFAULT_OUTFIT: Outfit = Outfit {
    look_type: 128,
    head: 40,
    body: 40,
    legs: 40,
    feet: 40,
    addons: 0,
    look_item: 0,
};

impl Default for Outfit {
    fn default() -> Self {
        Self {
            look_type: 0,
            head: 0,
            body: 0,
            legs: 0,
            feet: 0,
            addons: 0,
            look_item: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Creature {
    pub id: CreatureId,
    pub name: String,
    pub kind: CreatureKind,
    pub position: Position,
    pub stats: Stats,
    pub speed: u16,
    pub outfit: Outfit,
}
