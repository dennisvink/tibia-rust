use crate::combat::damage::{DamageScaleFlags, DamageType};
use crate::combat::conditions::ConditionKind;
use crate::entities::creature::Outfit;
use crate::entities::item::ItemTypeId;
use crate::entities::spell_definitions::builtin_spells;
use crate::telemetry::logging;
use std::collections::HashMap;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpellId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpellGroupId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpellKind {
    Instant,
    Rune,
    Conjure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpellTarget {
    SelfOnly,
    Creature,
    Position,
    Area,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpellShape {
    Area { radius: u8 },
    Line { length: u8 },
    Cone { range: u8, angle_degrees: u16 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpellEffectKind {
    Damage,
    Healing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellEffect {
    pub shape: SpellShape,
    pub kind: SpellEffectKind,
    pub damage_type: DamageType,
    pub min_damage: u32,
    pub max_damage: u32,
    pub include_caster: bool,
    pub base_damage: Option<i32>,
    pub variance: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SummonSpellEffect {
    pub race_number: Option<i64>,
    pub count: u8,
    pub convince: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellHasteEffect {
    pub shape: SpellShape,
    pub speed_delta: i16,
    pub speed_percent: Option<i16>,
    pub duration_ms: u32,
    pub include_caster: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellLightEffect {
    pub level: u8,
    pub color: u8,
    pub duration_ms: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellDispelEffect {
    pub shape: SpellShape,
    pub remove_magic_fields: bool,
    pub remove_items: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellFieldEffect {
    pub shape: SpellShape,
    pub field_kind: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellMagicShieldEffect {
    pub duration_ms: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpellOutfitEffect {
    Apply { outfit: Outfit, duration_ms: u32 },
    Cancel,
    CreatureName { duration_ms: u32 },
    Chameleon { duration_ms: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellChallengeEffect {
    pub radius: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellLevitateEffect;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpellRaiseDeadEffect {
    pub creature_name: String,
    pub radius: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellConjureEffect {
    pub item_type_id: ItemTypeId,
    pub count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellConditionCureEffect {
    pub kind: ConditionKind,
    pub shape: SpellShape,
    pub include_caster: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellMagicRopeEffect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellFindPersonEffect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpellEnchantStaffEffect {
    pub source_type_id: ItemTypeId,
    pub enchanted_type_id: ItemTypeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spell {
    pub id: SpellId,
    pub name: String,
    pub words: String,
    pub kind: SpellKind,
    pub rune_type_id: Option<ItemTypeId>,
    pub target: SpellTarget,
    pub group: Option<SpellGroupId>,
    pub mana_cost: u16,
    pub soul_cost: u8,
    pub level_required: u16,
    pub magic_level_required: u8,
    pub cooldown_ms: u32,
    pub group_cooldown_ms: u32,
    pub damage_scale_flags: DamageScaleFlags,
    pub effect: Option<SpellEffect>,
    pub summon: Option<SummonSpellEffect>,
    pub haste: Option<SpellHasteEffect>,
    pub light: Option<SpellLightEffect>,
    pub dispel: Option<SpellDispelEffect>,
    pub field: Option<SpellFieldEffect>,
    pub magic_shield: Option<SpellMagicShieldEffect>,
    pub outfit: Option<SpellOutfitEffect>,
    pub challenge: Option<SpellChallengeEffect>,
    pub levitate: Option<SpellLevitateEffect>,
    pub raise_dead: Option<SpellRaiseDeadEffect>,
    pub conjure: Option<SpellConjureEffect>,
    pub antidote: Option<SpellConditionCureEffect>,
    pub magic_rope: Option<SpellMagicRopeEffect>,
    pub find_person: Option<SpellFindPersonEffect>,
    pub enchant_staff: Option<SpellEnchantStaffEffect>,
}

#[derive(Debug, Default, Clone)]
pub struct SpellBook {
    spells: HashMap<SpellId, Spell>,
    by_words: HashMap<String, SpellId>,
    by_rune_type: HashMap<ItemTypeId, SpellId>,
    by_syllables: HashMap<Vec<u8>, SpellId>,
}

impl SpellBook {
    pub fn insert(&mut self, spell: Spell) -> Result<(), String> {
        if self.spells.contains_key(&spell.id) {
            return Err(format!("spell {:?} already exists", spell.id));
        }
        let key = spell.words.to_ascii_lowercase();
        if let Some(existing) = self.by_words.get(&key) {
            return Err(format!(
                "spell words {} already used by {:?}",
                spell.words, existing
            ));
        }
        if spell.kind != SpellKind::Rune && spell.rune_type_id.is_some() {
            return Err("non-rune spell cannot define a rune item".to_string());
        }
        if let Some(rune_type) = spell.rune_type_id {
            if let Some(existing) = self.by_rune_type.get(&rune_type) {
                return Err(format!(
                    "rune item {:?} already mapped to {:?}",
                    rune_type, existing
                ));
            }
            self.by_rune_type.insert(rune_type, spell.id);
        }
        let syllables = spell_syllables_from_words(&spell.words);
        if !syllables.is_empty() {
            if let Some(existing) = self.by_syllables.get(&syllables) {
                return Err(format!(
                    "spell syllables {:?} already used by {:?}",
                    syllables, existing
                ));
            }
            self.by_syllables.insert(syllables, spell.id);
        }
        self.by_words.insert(key, spell.id);
        self.spells.insert(spell.id, spell);
        Ok(())
    }

    pub fn get(&self, id: SpellId) -> Option<&Spell> {
        self.spells.get(&id)
    }

    pub fn get_by_words(&self, words: &str) -> Option<&Spell> {
        let key = words.to_ascii_lowercase();
        self.by_words
            .get(&key)
            .and_then(|id| self.spells.get(id))
    }

    pub fn get_by_input(&self, words: &str) -> Option<&Spell> {
        if let Some(spell) = self.get_by_words(words) {
            log_spell_debug(&format!(
                "spell lookup words='{}' matched={:?} exact",
                words, spell.id
            ));
            return Some(spell);
        }
        let syllables = spell_syllables_from_input(words);
        let spell_id = self.by_syllables.get(&syllables).copied();
        let spell = spell_id.and_then(|id| self.spells.get(&id));
        log_spell_debug(&format!(
            "spell lookup words='{}' syllables={:?} matched={:?}",
            words, syllables, spell_id
        ));
        spell
    }

    pub fn get_by_rune_item(&self, rune_type: ItemTypeId) -> Option<&Spell> {
        let spell_id = self.by_rune_type.get(&rune_type).copied();
        let spell = spell_id.and_then(|id| self.spells.get(&id));
        log_spell_debug(&format!(
            "spell lookup rune={:?} matched={:?}",
            rune_type, spell_id
        ));
        spell
    }

    pub fn iter(&self) -> impl Iterator<Item = &Spell> {
        self.spells.values()
    }

    pub fn len(&self) -> usize {
        self.spells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spells.is_empty()
    }
}

pub fn register_builtin_spells(spellbook: &mut SpellBook) -> Result<(), String> {
    for spell in builtin_spells() {
        spellbook.insert(spell)?;
    }
    for error in validate_spellbook(spellbook) {
        logging::log_game(&format!("spell validation: {error}"));
    }
    Ok(())
}

pub fn validate_spellbook(spellbook: &SpellBook) -> Vec<String> {
    let mut errors = Vec::new();
    for spell in spellbook.iter() {
        if spell.kind == SpellKind::Rune && spell.rune_type_id.is_none() {
            errors.push(format!(
                "rune spell {:?} ({}) missing rune item id",
                spell.id, spell.name
            ));
        }
        if spell.words.trim().is_empty() {
            errors.push(format!(
                "spell {:?} ({}) missing words",
                spell.id, spell.name
            ));
        }
    }
    errors
}

fn spell_debug_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("TIBIA_SPELL_DEBUG")
            .ok()
            .map(|value| {
                let value = value.trim().to_ascii_lowercase();
                matches!(value.as_str(), "1" | "true" | "yes" | "on")
            })
            .unwrap_or(false)
    })
}

fn log_spell_debug(message: &str) {
    if spell_debug_enabled() {
        logging::log_game(message);
    }
}

const SPELL_SYLLABLES: [&str; 51] = [
    "", "al", "ad", "ex", "ut", "om", "para", "ana", "evo", "ori", "mort", "lux", "liber",
    "vita", "flam", "pox", "hur", "moe", "ani", "ina", "eta", "amo", "hora", "gran", "cogni",
    "res", "mas", "vis", "som", "aqua", "frigo", "tera", "ura", "sio", "grav", "ito", "pan",
    "vid", "isa", "iva", "con", "", "", "", "", "", "", "", "", "", "",
];

const UNKNOWN_SYLLABLE_INDEX: u8 = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpellWordToken {
    pub text: String,
    pub quoted: bool,
}

pub(crate) fn spell_word_tokens(words: &str) -> Vec<SpellWordToken> {
    let mut tokens = Vec::new();
    let mut chars = words.chars().peekable();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        let quoted = ch == '"';
        if quoted {
            chars.next();
        }
        let mut token = String::new();
        if quoted {
            while let Some(next) = chars.next() {
                if next == '"' {
                    break;
                }
                token.push(next);
            }
        } else {
            while let Some(next) = chars.peek().copied() {
                if next.is_whitespace() {
                    break;
                }
                token.push(next);
                chars.next();
            }
        }
        if token.is_empty() {
            continue;
        }
        tokens.push(SpellWordToken { text: token, quoted });
        if tokens.len() >= 9 {
            break;
        }
    }
    tokens
}

pub(crate) fn spell_syllables_from_words(words: &str) -> Vec<u8> {
    spell_syllables_from_tokens(words)
}

fn spell_syllables_from_input(words: &str) -> Vec<u8> {
    spell_syllables_from_tokens(words)
}

fn spell_syllables_from_tokens(words: &str) -> Vec<u8> {
    let mut syllables = Vec::new();
    for token in spell_word_tokens(words) {
        append_syllables_from_token(&token.text, &mut syllables);
        if syllables.len() >= 9 {
            break;
        }
    }
    syllables
}

fn append_syllables_from_token(token: &str, out: &mut Vec<u8>) {
    let mut remaining = token.to_ascii_lowercase();
    while !remaining.is_empty() {
        let mut matched: Option<(u8, usize)> = None;
        for (index, syllable) in SPELL_SYLLABLES.iter().enumerate() {
            if syllable.is_empty() {
                continue;
            }
            if remaining.starts_with(syllable) {
                let len = syllable.len();
                if matched.map_or(true, |(_, best_len)| len > best_len) {
                    matched = Some((index as u8, len));
                }
            }
        }
        let Some((index, len)) = matched else {
            out.push(UNKNOWN_SYLLABLE_INDEX);
            return;
        };
        out.push(index);
        remaining = remaining.split_off(len);
        if out.len() >= 9 {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_spell(id: u16, words: &str, kind: SpellKind) -> Spell {
        Spell {
            id: SpellId(id),
            name: format!("Spell {}", id),
            words: words.to_string(),
            kind,
            rune_type_id: None,
            target: SpellTarget::SelfOnly,
            group: None,
            mana_cost: 0,
            soul_cost: 0,
            level_required: 0,
            magic_level_required: 0,
            cooldown_ms: 0,
            group_cooldown_ms: 0,
            damage_scale_flags: DamageScaleFlags::NONE,
            effect: None,
            summon: None,
            haste: None,
            light: None,
            dispel: None,
            field: None,
            magic_shield: None,
            outfit: None,
            challenge: None,
            levitate: None,
            raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
        }
    }

    #[test]
    fn spell_syllables_from_words_handles_quotes() {
        let syllables = spell_syllables_from_words("exura sio \"name\"");
        assert_eq!(syllables, vec![3, 32, 33, UNKNOWN_SYLLABLE_INDEX]);
    }

    #[test]
    fn spell_syllables_from_input_handles_concatenation() {
        let syllables = spell_syllables_from_input("exuragran");
        assert_eq!(syllables, vec![3, 32, 23]);
    }

    #[test]
    fn spell_syllables_from_input_defaults_unknown_to_index() {
        let syllables = spell_syllables_from_input("exura unknown");
        assert_eq!(syllables, vec![3, 32, UNKNOWN_SYLLABLE_INDEX]);
    }

    #[test]
    fn spellbook_falls_back_to_syllable_matching() {
        let mut book = SpellBook::default();
        let spell = make_spell(1, "exura gran", SpellKind::Instant);
        book.insert(spell).expect("insert spell");
        let found = book.get_by_input("exuragran");
        assert!(found.is_some());
    }

    #[test]
    fn spellbook_maps_rune_item_ids() {
        let mut book = SpellBook::default();
        let mut spell = make_spell(2, "adori", SpellKind::Rune);
        spell.rune_type_id = Some(ItemTypeId(112));
        book.insert(spell).expect("insert rune");
        let found = book.get_by_rune_item(ItemTypeId(112));
        assert!(found.is_some());
    }

    #[test]
    fn builtin_spell_words_resolve() {
        let mut book = SpellBook::default();
        register_builtin_spells(&mut book).expect("register builtin");

        for spell in builtin_spells() {
            if spell.words.trim().is_empty() {
                continue;
            }
            let by_words = book
                .get_by_words(&spell.words)
                .expect("lookup by words");
            assert_eq!(by_words.id, spell.id);

            let by_input = book
                .get_by_input(&spell.words)
                .expect("lookup by input");
            assert_eq!(by_input.id, spell.id);
        }
    }
}
