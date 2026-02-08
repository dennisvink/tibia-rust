use crate::entities::creature::Outfit;
use crate::world::time::GameTick;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutfitEffect {
    pub outfit: Outfit,
    pub expires_at: GameTick,
    pub original: Outfit,
}

impl OutfitEffect {
    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpeedEffect {
    pub speed: u16,
    pub expires_at: GameTick,
    pub original_speed: u16,
}

impl SpeedEffect {
    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrunkenEffect {
    pub intensity: u8,
    pub expires_at: GameTick,
}

impl DrunkenEffect {
    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrengthEffect {
    pub delta: i16,
    pub expires_at: GameTick,
}

impl StrengthEffect {
    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LightEffect {
    pub level: u8,
    pub color: u8,
    pub expires_at: GameTick,
}

impl LightEffect {
    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MagicShieldEffect {
    pub expires_at: GameTick,
}

impl MagicShieldEffect {
    pub fn is_expired(&self, now: GameTick) -> bool {
        now >= self.expires_at
    }
}
