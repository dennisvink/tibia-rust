use crate::combat::conditions::ConditionKind;
use crate::combat::damage::{DamageScaleFlags, DamageType};
use crate::entities::creature::Outfit;
use crate::entities::item::ItemTypeId;
use crate::entities::spells::{
    spell_syllables_from_words, Spell, SpellChallengeEffect, SpellConjureEffect,
    SpellConditionCureEffect, SpellDispelEffect, SpellEffect, SpellEffectKind, SpellEnchantStaffEffect,
    SpellFieldEffect, SpellFindPersonEffect, SpellHasteEffect, SpellKind, SpellLevitateEffect,
    SpellLightEffect, SpellMagicRopeEffect, SpellMagicShieldEffect, SpellOutfitEffect,
    SpellRaiseDeadEffect, SpellShape, SpellTarget, SummonSpellEffect,
};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

pub fn builtin_spells() -> Vec<Spell> {
    let mut spells = Vec::new();
    let mut fallback_id = 1000u16;
    const LIGHT_COLOR: u8 = 215;
    fn normalize_spell_words(words: &str) -> String {
        words
            .replace("{0:-name}", "\"name\"")
            .replace("{0:-creature}", "\"creature\"")
            .replace("{0:-up|down}", "\"up|down\"")
    }
    #[derive(Clone)]
    struct SpellListEntry {
        id: u16,
        name: String,
        words: String,
        syllables: String,
        syllable_indexes: Vec<u8>,
        kind: Option<SpellKind>,
        level: Option<u16>,
        mana: Option<u16>,
        soul: Option<u16>,
        group: Option<crate::entities::spells::SpellGroupId>,
    }
    fn parse_group_id(group: &str) -> Option<crate::entities::spells::SpellGroupId> {
        match group {
            "GROUP_ATTACK" => Some(crate::entities::spells::SpellGroupId(1)),
            "GROUP_HEALING" => Some(crate::entities::spells::SpellGroupId(2)),
            "GROUP_SUPPORT" => Some(crate::entities::spells::SpellGroupId(3)),
            "GROUP_POWERSTRIKES" => Some(crate::entities::spells::SpellGroupId(4)),
            _ => None,
        }
    }
    fn normalize_csv_field(field: &str) -> String {
        let mut value = field.trim().to_string();
        if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
            value = value[1..value.len() - 1].to_string();
        }
        value.replace("\"\"", "\"")
    }
    fn spell_list_entries() -> Vec<SpellListEntry> {
        const CSV: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_list.csv"));
        let mut entries = Vec::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 11 {
                continue;
            }
            let id = fields[0].trim().parse::<u16>().ok();
            let Some(id) = id else {
                continue;
            };
            let syllables = fields[1].trim().to_string();
            let syllable_indexes = fields[2]
                .split_whitespace()
                .filter_map(|value| value.parse::<u8>().ok())
                .collect::<Vec<u8>>();
            let name = fields[4].trim().to_string();
            let words = normalize_spell_words(&normalize_csv_field(fields[5]));
            let kind = match fields[6].trim() {
                "TYPE_INSTANT" => Some(SpellKind::Instant),
                "TYPE_RUNE" => Some(SpellKind::Rune),
                "TYPE_CONJURE" => Some(SpellKind::Conjure),
                _ => None,
            };
            let level = fields[7].trim().parse::<u16>().ok();
            let mana = fields[8].trim().parse::<u16>().ok();
            let soul = fields[9].trim().parse::<u16>().ok();
            let group = parse_group_id(fields[10].trim());
            entries.push(SpellListEntry {
                id,
                name,
                words,
                syllables,
                syllable_indexes,
                kind,
                level,
                mana,
                soul,
                group,
            });
        }
        entries
    }
    fn apply_spell_list_metadata(spells: &mut [Spell]) {
        const CSV: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_list.csv"));
        #[derive(Clone, Copy)]
        struct SpellListMeta {
            level: Option<u16>,
            mana: Option<u16>,
            soul: Option<u16>,
            group: Option<crate::entities::spells::SpellGroupId>,
            cooldown_ms: Option<u32>,
            group_cooldown_ms: Option<u32>,
        }
        fn normalize_csv_words(words: &str) -> String {
            normalize_spell_words(&normalize_csv_field(words))
        }
        let mut by_words: HashMap<String, SpellListMeta> = HashMap::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 18 {
                continue;
            }
            let words = normalize_csv_words(fields[5].trim());
            if words.is_empty() {
                continue;
            }
            let level = fields[7].trim().parse::<u16>().ok();
            let mana = fields[8].trim().parse::<u16>().ok();
            let soul = fields[9].trim().parse::<u16>().ok();
            let group = parse_group_id(fields[10].trim());
            let cooldown_ms = fields[11].trim().parse::<u32>().ok();
            let group_cooldown_ms = fields[12].trim().parse::<u32>().ok();
            by_words.insert(
                words.to_ascii_lowercase(),
                SpellListMeta {
                    level,
                    mana,
                    soul,
                    group,
                    cooldown_ms,
                    group_cooldown_ms,
                },
            );
        }
        for spell in spells {
            let key = normalize_spell_words(&spell.words).to_ascii_lowercase();
            if let Some(meta) = by_words.get(&key) {
                if let Some(level) = meta.level {
                    spell.level_required = level;
                }
                if let Some(mana) = meta.mana {
                    spell.mana_cost = mana;
                }
                if let Some(soul) = meta.soul {
                    spell.soul_cost = u8::try_from(soul).unwrap_or(u8::MAX);
                }
                if spell.group.is_none() {
                    spell.group = meta.group;
                }
                if spell.cooldown_ms == 0 {
                    if let Some(cooldown_ms) = meta.cooldown_ms {
                        spell.cooldown_ms = cooldown_ms;
                    }
                }
                if spell.group_cooldown_ms == 0 {
                    if let Some(group_cooldown_ms) = meta.group_cooldown_ms {
                        spell.group_cooldown_ms = group_cooldown_ms;
                    }
                }
            }
        }
    }
    fn apply_flash_cooldowns(spells: &mut [Spell]) {
        const CSV: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/flash_spells.csv"));
        #[derive(Clone, Copy)]
        struct FlashCooldown {
            group: Option<crate::entities::spells::SpellGroupId>,
            cooldown_ms: u32,
            group_cooldown_ms: u32,
        }
        let mut by_words: HashMap<String, FlashCooldown> = HashMap::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 17 {
                continue;
            }
            let words = normalize_spell_words(fields[2].trim());
            let group = parse_group_id(fields[3].trim());
            let cooldown_ms = fields[14].trim().parse::<u32>().unwrap_or(0);
            let group_cooldown_ms = fields[15].trim().parse::<u32>().unwrap_or(0);
            by_words.insert(
                words,
                FlashCooldown {
                    group,
                    cooldown_ms,
                    group_cooldown_ms,
                },
            );
        }
        for spell in spells {
            let key = normalize_spell_words(&spell.words);
            if let Some(entry) = by_words.get(&key) {
                if spell.cooldown_ms == 0 {
                    spell.cooldown_ms = entry.cooldown_ms;
                }
                if spell.group.is_none() {
                    spell.group = entry.group;
                }
                if spell.group_cooldown_ms == 0 && entry.group.is_some() {
                    spell.group_cooldown_ms = entry.group_cooldown_ms;
                }
            }
        }
    }
    fn apply_game_orig_metadata(spells: &mut [Spell]) {
        const CSV: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_init_fields.csv"));
        const RUNE_BASE_TYPE_ID: u16 = 3147;
        #[derive(Clone, Copy)]
        struct GameOrigMeta {
            level: Option<u16>,
            mana: Option<u16>,
            soul: Option<u8>,
            magic_level: Option<u8>,
            rune_class: Option<u8>,
            rune_subtype: Option<u8>,
            flags: Option<u16>,
        }
        let mut header_indexes: HashMap<&str, usize> = HashMap::new();
        let mut by_id: HashMap<u16, GameOrigMeta> = HashMap::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 {
                header_indexes = line
                    .split(',')
                    .enumerate()
                    .map(|(idx, name)| (name.trim(), idx))
                    .collect();
                continue;
            }
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            let Some(spell_id_idx) = header_indexes.get("spell_id").copied() else {
                continue;
            };
            let Some(spell_id) = fields
                .get(spell_id_idx)
                .and_then(|value| value.trim().parse::<u16>().ok())
            else {
                continue;
            };
            let parse_u16 = |name: &str| -> Option<u16> {
                header_indexes
                    .get(name)
                    .and_then(|idx| fields.get(*idx))
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty())
                    .and_then(|value| value.parse::<u16>().ok())
            };
            let level = parse_u16("offset_0x10");
            let mana = parse_u16("offset_0x18");
            let soul = parse_u16("offset_0x1c").and_then(|value| u8::try_from(value).ok());
            let magic_level =
                parse_u16("offset_0x12").and_then(|value| u8::try_from(value).ok());
            let rune_class = parse_u16("offset_0xa").and_then(|value| u8::try_from(value).ok());
            let rune_subtype = parse_u16("offset_0xb").and_then(|value| u8::try_from(value).ok());
            let flags = parse_u16("offset_0x14");
            if level.is_some()
                || mana.is_some()
                || soul.is_some()
                || magic_level.is_some()
                || rune_class.is_some()
                || rune_subtype.is_some()
                || flags.is_some()
            {
                by_id.insert(
                    spell_id,
                    GameOrigMeta {
                        level,
                        mana,
                        soul,
                        magic_level,
                        rune_class,
                        rune_subtype,
                        flags,
                    },
                );
            }
        }
        for spell in spells {
            let Some(meta) = by_id.get(&spell.id.0) else {
                continue;
            };
            if spell.level_required == 0 {
                if let Some(level) = meta.level {
                    spell.level_required = level;
                }
            }
            if spell.mana_cost == 0 {
                if let Some(mana) = meta.mana {
                    spell.mana_cost = mana;
                }
            }
            if spell.soul_cost == 0 {
                if let Some(soul) = meta.soul {
                    spell.soul_cost = soul;
                }
            }
            if spell.magic_level_required == 0 {
                if let Some(magic_level) = meta.magic_level {
                    spell.magic_level_required = magic_level;
                }
            }
            if spell.rune_type_id.is_none() {
                if let (Some(class), Some(subtype)) = (meta.rune_class, meta.rune_subtype) {
                    if class == 79 && subtype > 0 {
                        spell.rune_type_id = Some(ItemTypeId(
                            RUNE_BASE_TYPE_ID.saturating_add(u16::from(subtype)),
                        ));
                    }
                }
            }
            if let Some(flags) = meta.flags {
                spell.damage_scale_flags = crate::combat::damage::DamageScaleFlags {
                    clamp_upper: flags & 0x4 != 0,
                    clamp_lower: flags & 0x8 != 0,
                };
            }
        }
    }
    fn apply_spell_effect_metadata(spells: &mut [Spell]) {
        const CSV: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_effect_metadata.csv"));
        #[derive(Clone, Copy)]
        struct EffectMeta {
            base_damage: i32,
            variance: i32,
        }
        let mut by_id: HashMap<u16, EffectMeta> = HashMap::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 5 {
                continue;
            }
            let spell_id = fields[0].trim().parse::<u16>().ok();
            let base_damage = fields[3].trim().parse::<i32>().ok();
            let variance = fields[4].trim().parse::<i32>().ok();
            let (Some(spell_id), Some(base_damage), Some(variance)) =
                (spell_id, base_damage, variance)
            else {
                continue;
            };
            by_id.insert(spell_id, EffectMeta { base_damage, variance });
        }
        for spell in spells {
            let Some(meta) = by_id.get(&spell.id.0) else {
                continue;
            };
            let Some(effect) = spell.effect.as_mut() else {
                continue;
            };
            let min = meta.base_damage.saturating_sub(meta.variance);
            let max = meta.base_damage.saturating_add(meta.variance);
            let min = min.max(0) as u32;
            let max = max.max(0) as u32;
            effect.min_damage = min;
            effect.max_damage = max;
            effect.base_damage = Some(meta.base_damage);
            effect.variance = Some(meta.variance);
        }
    }
    fn apply_rune_crosswalk(spells: &mut [Spell]) {
        const CSV: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/data/spells/spell_rune_crosswalk.csv"
        ));
        let mut by_id: HashMap<u16, ItemTypeId> = HashMap::new();
        let mut by_words: HashMap<String, ItemTypeId> = HashMap::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 4 {
                continue;
            }
            let spell_id = fields[0].trim().parse::<u16>().ok();
            let words = normalize_spell_words(&normalize_csv_field(fields[1]));
            let rune_id = fields[3].trim().parse::<u16>().ok();
            let Some(rune_id) = rune_id else {
                continue;
            };
            let rune_id = ItemTypeId(rune_id);
            if let Some(spell_id) = spell_id {
                by_id.insert(spell_id, rune_id);
            }
            if !words.is_empty() {
                by_words.insert(words.to_ascii_lowercase(), rune_id);
            }
        }
        for spell in spells {
            if spell.kind != SpellKind::Rune || spell.rune_type_id.is_some() {
                continue;
            }
            if let Some(rune_id) = by_id.get(&spell.id.0).copied() {
                spell.rune_type_id = Some(rune_id);
                continue;
            }
            let key = normalize_spell_words(&spell.words).to_ascii_lowercase();
            if let Some(rune_id) = by_words.get(&key).copied() {
                spell.rune_type_id = Some(rune_id);
            }
        }
    }
    fn apply_placeholder_spell_effects(spells: &mut [Spell]) {
        fn amount_from_mana(mana_cost: u16, divisor: u16) -> u32 {
            let divisor = divisor.max(1) as u32;
            let amount = (mana_cost as u32) / divisor;
            amount.max(1)
        }
        fn has_token(tokens: &[&str], token: &str) -> bool {
            tokens.iter().any(|value| *value == token)
        }
        fn damage_type_for_words(words: &str, tokens: &[&str]) -> DamageType {
            match words {
                "adori vita vis" => DamageType::Death,
                "exevo mort hur" => DamageType::Energy,
                _ => {
                    if has_token(tokens, "mort") {
                        DamageType::Death
                    } else if has_token(tokens, "san") {
                        DamageType::Holy
                    } else if has_token(tokens, "frigo") {
                        DamageType::Ice
                    } else if has_token(tokens, "flam") {
                        DamageType::Fire
                    } else if has_token(tokens, "tera") || has_token(tokens, "pox") {
                        DamageType::Earth
                    } else if has_token(tokens, "vis") {
                        DamageType::Energy
                    } else {
                        DamageType::Physical
                    }
                }
            }
        }
        fn apply_effect(
            spell: &mut Spell,
            target: SpellTarget,
            effect: SpellEffect,
        ) {
            if spell.effect.is_some() {
                return;
            }
            spell.target = target;
            spell.effect = Some(effect);
        }
        fn apply_damage_effect(
            spell: &mut Spell,
            target: SpellTarget,
            shape: SpellShape,
            damage_type: DamageType,
            mana_divisor: u16,
        ) {
            apply_effect(
                spell,
                target,
                SpellEffect {
                    shape,
                    kind: SpellEffectKind::Damage,
                    damage_type,
                    min_damage: amount_from_mana(spell.mana_cost, mana_divisor),
                    max_damage: amount_from_mana(spell.mana_cost, mana_divisor),
                    include_caster: false,
                    base_damage: None,
                    variance: None,
                },
            );
        }
        fn apply_field_effect(
            spell: &mut Spell,
            target: SpellTarget,
            shape: SpellShape,
            field_kind: u8,
        ) {
            if spell.field.is_some() {
                return;
            }
            spell.target = target;
            spell.field = Some(SpellFieldEffect { shape, field_kind });
        }
        for spell in spells {
            if spell.kind == SpellKind::Conjure {
                continue;
            }
            match spell.words.as_str() {
                "exori" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 2 },
                        kind: SpellEffectKind::Damage,
                        damage_type: DamageType::Physical,
                        min_damage: amount_from_mana(spell.mana_cost, 4),
                        max_damage: amount_from_mana(spell.mana_cost, 4),
                        include_caster: false,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "exori gran" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 2 },
                        kind: SpellEffectKind::Damage,
                        damage_type: DamageType::Physical,
                        min_damage: amount_from_mana(spell.mana_cost, 4),
                        max_damage: amount_from_mana(spell.mana_cost, 4),
                        include_caster: false,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "exori mas" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 2 },
                        kind: SpellEffectKind::Damage,
                        damage_type: DamageType::Physical,
                        min_damage: amount_from_mana(spell.mana_cost, 4),
                        max_damage: amount_from_mana(spell.mana_cost, 4),
                        include_caster: false,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "adori flam" => apply_effect(
                    spell,
                    SpellTarget::Position,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 1 },
                        kind: SpellEffectKind::Damage,
                        damage_type: DamageType::Fire,
                        min_damage: amount_from_mana(spell.mana_cost, 4),
                        max_damage: amount_from_mana(spell.mana_cost, 4),
                        include_caster: false,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "adori gran flam" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    DamageType::Fire,
                    4,
                ),
                "adori mas flam" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    DamageType::Fire,
                    4,
                ),
                "adori gran mort" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Death,
                    4,
                ),
                "adori" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Energy,
                    4,
                ),
                "adori min vis" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Energy,
                    4,
                ),
                "adori vis" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Energy,
                    4,
                ),
                "adori gran" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Energy,
                    4,
                ),
                "adori vita vis" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Death,
                    4,
                ),
                "adori san" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Holy,
                    4,
                ),
                "adori frigo" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Ice,
                    4,
                ),
                "adori tera" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    DamageType::Earth,
                    4,
                ),
                "adori mas frigo" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    DamageType::Ice,
                    4,
                ),
                "adori mas tera" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    DamageType::Earth,
                    4,
                ),
                "adori mas vis" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    DamageType::Energy,
                    4,
                ),
                "adevo mas hur" => apply_damage_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 1 },
                    DamageType::Physical,
                    4,
                ),
                "adevo res flam" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Fire,
                    4,
                ),
                "adevo res pox" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Earth,
                    4,
                ),
                "exori con" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Physical,
                    4,
                ),
                "exori flam" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Fire,
                    4,
                ),
                "exori frigo" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Ice,
                    4,
                ),
                "exori mort" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Death,
                    4,
                ),
                "exori san" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Holy,
                    4,
                ),
                "exori tera" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Earth,
                    4,
                ),
                "exori vis" => apply_damage_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellShape::Area { radius: 0 },
                    DamageType::Energy,
                    4,
                ),
                "exevo flam hur" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Cone {
                        range: 4,
                        angle_degrees: 45,
                    },
                    DamageType::Fire,
                    4,
                ),
                "exevo mort hur" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Cone {
                        range: 5,
                        angle_degrees: 30,
                    },
                    DamageType::Energy,
                    4,
                ),
                "exevo vis lux" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Line { length: 5 },
                    DamageType::Energy,
                    4,
                ),
                "exevo gran vis lux" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Line { length: 8 },
                    DamageType::Energy,
                    4,
                ),
                "exevo gran mas flam" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Area { radius: 3 },
                    DamageType::Fire,
                    4,
                ),
                "exevo gran mas vis" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Area { radius: 6 },
                    DamageType::Physical,
                    4,
                ),
                "exevo gran mas frigo" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Area { radius: 3 },
                    DamageType::Ice,
                    4,
                ),
                "exevo gran mas tera" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Area { radius: 3 },
                    DamageType::Earth,
                    4,
                ),
                "exevo mas san" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Area { radius: 2 },
                    DamageType::Holy,
                    4,
                ),
                "exevo gran mas pox" => apply_damage_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellShape::Area { radius: 8 },
                    DamageType::Earth,
                    4,
                ),
                "exura" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 0 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "exura gran" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 0 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "exura vita" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 0 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "exura sio \"name\"" => apply_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 0 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "exura gran mas res" => apply_effect(
                    spell,
                    SpellTarget::SelfOnly,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 1 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "adura gran" => apply_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 0 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "adura vita" => apply_effect(
                    spell,
                    SpellTarget::Creature,
                    SpellEffect {
                        shape: SpellShape::Area { radius: 0 },
                        kind: SpellEffectKind::Healing,
                        damage_type: DamageType::Holy,
                        min_damage: amount_from_mana(spell.mana_cost, 2),
                        max_damage: amount_from_mana(spell.mana_cost, 2),
                        include_caster: true,
                        base_damage: None,
                        variance: None,
                    },
                ),
                "adevo grav flam" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    1,
                ),
                "adevo grav pox" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    2,
                ),
                "adevo grav vis" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    3,
                ),
                "adevo mas flam" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    1,
                ),
                "adevo mas pox" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    2,
                ),
                "adevo mas vis" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 2 },
                    3,
                ),
                "adevo mas grav flam" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Line { length: 5 },
                    1,
                ),
                "adevo mas grav pox" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Line { length: 5 },
                    2,
                ),
                "adevo mas grav vis" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Line { length: 7 },
                    3,
                ),
                "adevo grav tera" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    4,
                ),
                "exevo grav vita" => apply_field_effect(
                    spell,
                    SpellTarget::Position,
                    SpellShape::Area { radius: 0 },
                    5,
                ),
                _ => {}
            }
            if spell.effect.is_some() {
                continue;
            }
            let tokens: Vec<&str> = spell.words.split_whitespace().collect();
            if has_token(&tokens, "exevo") {
                let damage_type = damage_type_for_words(spell.words.as_str(), &tokens);
                if has_token(&tokens, "lux") {
                    let length = if has_token(&tokens, "gran") { 6 } else { 4 };
                    apply_damage_effect(
                        spell,
                        SpellTarget::SelfOnly,
                        SpellShape::Line { length },
                        damage_type,
                        4,
                    );
                } else if has_token(&tokens, "hur") {
                    let range = if has_token(&tokens, "gran") { 5 } else { 4 };
                    apply_damage_effect(
                        spell,
                        SpellTarget::SelfOnly,
                        SpellShape::Cone {
                            range,
                            angle_degrees: 45,
                        },
                        damage_type,
                        4,
                    );
                } else if has_token(&tokens, "mas") {
                    let radius = if has_token(&tokens, "gran") { 3 } else { 2 };
                    apply_damage_effect(
                        spell,
                        SpellTarget::SelfOnly,
                        SpellShape::Area { radius },
                        damage_type,
                        4,
                    );
                }
            }
        }
    }

    fn apply_support_spell_effects(spells: &mut [Spell]) {
        fn apply_support_spell_metadata(spells: &mut [Spell]) {
            const CSV: &str = include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/data/spells/spell_support_metadata.csv"
            ));
            const LIGHT_DURATION_MS_PER_UNIT: u32 = 1_000;
            const HASTE_DURATION_MS_PER_UNIT: u32 = 60_000;
            #[derive(Clone, Copy, Default)]
            struct SupportMeta {
                light_level: Option<u8>,
                light_duration_ms: Option<u32>,
                haste_speed_percent: Option<i16>,
                haste_duration_ms: Option<u32>,
            }
            let mut by_id: HashMap<u16, SupportMeta> = HashMap::new();
            for (idx, line) in CSV.lines().enumerate() {
                if idx == 0 || line.trim().is_empty() {
                    continue;
                }
                let fields: Vec<&str> = line.split(',').collect();
                if fields.len() < 7 {
                    continue;
                }
                let spell_id = fields[0].trim().parse::<u16>().ok();
                let effect = fields[2].trim();
                let Some(spell_id) = spell_id else {
                    continue;
                };
                let entry = by_id.entry(spell_id).or_default();
                match effect {
                    "light" => {
                        let level = fields[3]
                            .trim()
                            .parse::<u8>()
                            .ok();
                        let duration = fields[4]
                            .trim()
                            .parse::<u32>()
                            .ok()
                            .and_then(|value| value.checked_mul(LIGHT_DURATION_MS_PER_UNIT));
                        if let Some(level) = level {
                            entry.light_level = Some(level);
                        }
                        if let Some(duration) = duration {
                            entry.light_duration_ms = Some(duration);
                        }
                    }
                    "haste" => {
                        let speed_percent = fields[5]
                            .trim()
                            .parse::<i16>()
                            .ok();
                        let duration = fields[6]
                            .trim()
                            .parse::<u32>()
                            .ok()
                            .and_then(|value| value.checked_mul(HASTE_DURATION_MS_PER_UNIT));
                        if let Some(speed_percent) = speed_percent {
                            entry.haste_speed_percent = Some(speed_percent);
                        }
                        if let Some(duration) = duration {
                            entry.haste_duration_ms = Some(duration);
                        }
                    }
                    _ => {}
                }
            }
            for spell in spells {
                let Some(meta) = by_id.get(&spell.id.0) else {
                    continue;
                };
                if let Some(level) = meta.light_level {
                    let duration_ms = meta.light_duration_ms.unwrap_or(0);
                    if let Some(light) = spell.light.as_mut() {
                        light.level = level;
                        if duration_ms > 0 {
                            light.duration_ms = duration_ms;
                        }
                    } else if duration_ms > 0 {
                        spell.light = Some(SpellLightEffect {
                            level,
                            color: LIGHT_COLOR,
                            duration_ms,
                        });
                    }
                }
                if meta.haste_speed_percent.is_some() || meta.haste_duration_ms.is_some() {
                    if let Some(haste) = spell.haste.as_mut() {
                        if let Some(speed_percent) = meta.haste_speed_percent {
                            haste.speed_percent = Some(speed_percent);
                        }
                        if let Some(duration_ms) = meta.haste_duration_ms {
                            if duration_ms > 0 {
                                haste.duration_ms = duration_ms;
                            }
                        }
                    }
                }
            }
        }

        let conjure_items: HashMap<&'static str, SpellConjureEffect> = HashMap::from([
            ("exevo con", SpellConjureEffect { item_type_id: ItemTypeId(3447), count: 10 }),
            ("exevo con mort", SpellConjureEffect { item_type_id: ItemTypeId(3446), count: 5 }),
            ("exevo con pox", SpellConjureEffect { item_type_id: ItemTypeId(3448), count: 7 }),
            ("exevo con flam", SpellConjureEffect { item_type_id: ItemTypeId(3449), count: 8 }),
            ("exevo con vis", SpellConjureEffect { item_type_id: ItemTypeId(3450), count: 10 }),
            ("exevo pan", SpellConjureEffect { item_type_id: ItemTypeId(3577), count: 1 }),
        ]);

        apply_support_spell_metadata(spells);

        for spell in spells {
            if spell.conjure.is_none() {
                if let Some(effect) = conjure_items.get(spell.words.as_str()) {
                    spell.conjure = Some(*effect);
                    spell.target = SpellTarget::SelfOnly;
                }
            }

            match spell.words.as_str() {
                "exana pox" => {
                    if spell.antidote.is_none() {
                        spell.target = SpellTarget::SelfOnly;
                        spell.antidote = Some(SpellConditionCureEffect {
                            kind: ConditionKind::Poison,
                            shape: SpellShape::Area { radius: 0 },
                            include_caster: true,
                        });
                    }
                }
                "adana pox" => {
                    if spell.antidote.is_none() {
                        spell.target = SpellTarget::Creature;
                        spell.antidote = Some(SpellConditionCureEffect {
                            kind: ConditionKind::Poison,
                            shape: SpellShape::Area { radius: 0 },
                            include_caster: true,
                        });
                    }
                }
                "exiva \"name\"" => {
                    spell.find_person.get_or_insert(SpellFindPersonEffect);
                }
                "exani tera" => {
                    if spell.magic_rope.is_none() {
                        spell.target = SpellTarget::Position;
                        spell.magic_rope = Some(SpellMagicRopeEffect);
                    }
                }
                "exeta vis" => {
                    if spell.enchant_staff.is_none() {
                        spell.target = SpellTarget::SelfOnly;
                        spell.enchant_staff = Some(SpellEnchantStaffEffect {
                            source_type_id: ItemTypeId(3289),
                            enchanted_type_id: ItemTypeId(3321),
                        });
                    }
                }
                _ => {}
            }
        }
    }
    fn push_spell(
        spells: &mut Vec<Spell>,
        fallback_id: &mut u16,
        id_by_words: &HashMap<String, u16>,
        id_by_syllables: &HashMap<Vec<u8>, u16>,
        used_ids: &mut HashSet<u16>,
        defined_words: &mut HashSet<String>,
        name: &str,
        words: &str,
        kind: SpellKind,
        level: u16,
        mana: u16,
    ) {
        let id = spell_id_for_words(
            words,
            id_by_words,
            id_by_syllables,
            used_ids,
            fallback_id,
        );
        let spell = Spell {
            id: crate::entities::spells::SpellId(id),
            name: name.to_string(),
            words: words.to_string(),
            kind,
            rune_type_id: None,
            target: SpellTarget::SelfOnly,
            group: None,
            mana_cost: mana,
            soul_cost: 0,
            level_required: level,
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
        };
        defined_words.insert(normalize_spell_words(words).to_ascii_lowercase());
        spells.push(spell);
    }
    fn spell_id_for_words(
        words: &str,
        id_by_words: &HashMap<String, u16>,
        id_by_syllables: &HashMap<Vec<u8>, u16>,
        used_ids: &mut HashSet<u16>,
        fallback_id: &mut u16,
    ) -> u16 {
        let normalized = normalize_spell_words(words).to_ascii_lowercase();
        if let Some(id) = id_by_words.get(&normalized).copied() {
            if used_ids.insert(id) {
                return id;
            }
        }
        let syllables = spell_syllables_from_words(words);
        if let Some(id) = id_by_syllables.get(&syllables).copied() {
            if used_ids.insert(id) {
                return id;
            }
        }
        let id = *fallback_id;
        *fallback_id = fallback_id.saturating_add(1);
        used_ids.insert(id);
        id
    }
    fn default_target_for_kind(kind: SpellKind) -> SpellTarget {
        match kind {
            SpellKind::Rune => SpellTarget::Position,
            SpellKind::Instant | SpellKind::Conjure => SpellTarget::SelfOnly,
        }
    }
    let spell_list_entries = spell_list_entries();
    let mut id_by_words = HashMap::new();
    let mut id_by_syllables = HashMap::new();
    for entry in &spell_list_entries {
        if !entry.words.is_empty() {
            id_by_words.insert(entry.words.to_ascii_lowercase(), entry.id);
        }
        if !entry.syllable_indexes.is_empty() {
            id_by_syllables.insert(entry.syllable_indexes.clone(), entry.id);
        }
    }
    let mut used_ids: HashSet<u16> = HashSet::new();
    let mut defined_words: HashSet<String> = HashSet::new();

    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "adana mort",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Animate Dead".to_string(),
        words: "adana mort".to_string(),
        kind: SpellKind::Rune,
        rune_type_id: None,
        target: SpellTarget::Position,
        group: None,
        mana_cost: 600,
        soul_cost: 0,
        level_required: 27,
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
        raise_dead: Some(SpellRaiseDeadEffect {
            creature_name: "skeleton".to_string(),
            radius: 1,
        }),
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("adana mort".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Antidote",
        "exana pox",
        SpellKind::Instant,
        10,
        30,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Antidote Rune",
        "adana pox",
        SpellKind::Rune,
        15,
        200,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Berserk",
        "exori",
        SpellKind::Instant,
        35,
        0,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "exana ina",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Cancel Invisibility".to_string(),
        words: "exana ina".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 200,
        soul_cost: 0,
        level_required: 26,
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
        outfit: Some(SpellOutfitEffect::Cancel),
        challenge: None,
        levitate: None,
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("exana ina".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "exeta res",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Challenge".to_string(),
        words: "exeta res".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 30,
        soul_cost: 0,
        level_required: 20,
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
        challenge: Some(SpellChallengeEffect { radius: 5 }),
        levitate: None,
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("exeta res".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "adevo ina",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Chameleon".to_string(),
        words: "adevo ina".to_string(),
        kind: SpellKind::Rune,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 600,
        soul_cost: 0,
        level_required: 27,
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
        outfit: Some(SpellOutfitEffect::Chameleon { duration_ms: 200_000 }),
        challenge: None,
        levitate: None,
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("adevo ina".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Conjure Arrow",
        "exevo con",
        SpellKind::Conjure,
        13,
        100,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Conjure Bolt",
        "exevo con mort",
        SpellKind::Conjure,
        17,
        140,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "adeta sio",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Convince Creature".to_string(),
        words: "adeta sio".to_string(),
        kind: SpellKind::Rune,
        rune_type_id: None,
        target: SpellTarget::Creature,
        group: None,
        mana_cost: 200,
        soul_cost: 0,
        level_required: 16,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: Some(SummonSpellEffect {
            race_number: None,
            count: 1,
            convince: true,
        }),
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
    };
    defined_words.insert("adeta sio".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utevo res ina \"creature\"",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Creature Illusion".to_string(),
        words: "utevo res ina \"creature\"".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 100,
        soul_cost: 0,
        level_required: 23,
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
        outfit: Some(SpellOutfitEffect::CreatureName { duration_ms: 200_000 }),
        challenge: None,
        levitate: None,
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert(
        normalize_spell_words("utevo res ina \"creature\"").to_ascii_lowercase(),
    );
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "adito tera",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Desintegrate".to_string(),
        words: "adito tera".to_string(),
        kind: SpellKind::Rune,
        rune_type_id: None,
        target: SpellTarget::Position,
        group: None,
        mana_cost: 200,
        soul_cost: 0,
        level_required: 21,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: None,
        light: None,
        dispel: Some(SpellDispelEffect {
            shape: SpellShape::Area { radius: 0 },
            remove_magic_fields: false,
            remove_items: true,
        }),
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
    };
    defined_words.insert("adito tera".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "adito grav",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Destroy Field".to_string(),
        words: "adito grav".to_string(),
        kind: SpellKind::Rune,
        rune_type_id: None,
        target: SpellTarget::Position,
        group: None,
        mana_cost: 120,
        soul_cost: 0,
        level_required: 17,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: None,
        light: None,
        dispel: Some(SpellDispelEffect {
            shape: SpellShape::Area { radius: 0 },
            remove_magic_fields: true,
            remove_items: false,
        }),
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
    };
    defined_words.insert("adito grav".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Enchant Staff",
        "exeta vis",
        SpellKind::Instant,
        41,
        80,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Energy Beam",
        "exevo vis lux",
        SpellKind::Instant,
        23,
        100,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Energy Field",
        "adevo grav vis",
        SpellKind::Rune,
        18,
        320,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Energy Strike",
        "exori vis",
        SpellKind::Instant,
        12,
        20,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Energy Wall",
        "adevo mas grav vis",
        SpellKind::Rune,
        41,
        1000,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Energy Wave",
        "exevo mort hur",
        SpellKind::Instant,
        38,
        250,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Energybomb",
        "adevo mas vis",
        SpellKind::Rune,
        37,
        880,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Envenom",
        "adevo res pox",
        SpellKind::Rune,
        21,
        400,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Explosion",
        "adevo mas hur",
        SpellKind::Rune,
        31,
        720,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Explosive Arrow",
        "exevo con flam",
        SpellKind::Conjure,
        25,
        290,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Find Person",
        "exiva \"name\"",
        SpellKind::Instant,
        8,
        20,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Fire Field",
        "adevo grav flam",
        SpellKind::Rune,
        15,
        240,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Fire Wall",
        "adevo mas grav flam",
        SpellKind::Rune,
        33,
        780,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Fire Wave",
        "exevo flam hur",
        SpellKind::Instant,
        18,
        80,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Fireball",
        "adori flam",
        SpellKind::Rune,
        17,
        160,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Firebomb",
        "adevo mas flam",
        SpellKind::Rune,
        27,
        600,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Flame Strike",
        "exori flam",
        SpellKind::Instant,
        12,
        20,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Food",
        "exevo pan",
        SpellKind::Conjure,
        14,
        120,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Force Strike",
        "exori mort",
        SpellKind::Instant,
        11,
        20,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Great Energy Beam",
        "exevo gran vis lux",
        SpellKind::Instant,
        29,
        200,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Great Fireball",
        "adori gran flam",
        SpellKind::Rune,
        23,
        480,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utevo gran lux",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Great Light".to_string(),
        words: "utevo gran lux".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 60,
        soul_cost: 0,
        level_required: 13,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: None,
        light: Some(SpellLightEffect {
            level: 8,
            color: LIGHT_COLOR,
            duration_ms: 1_000_000,
        }),
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
    };
    defined_words.insert("utevo gran lux".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utani hur",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Haste".to_string(),
        words: "utani hur".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 60,
        soul_cost: 0,
        level_required: 14,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: Some(SpellHasteEffect {
            shape: SpellShape::Area { radius: 0 },
            speed_delta: 100,
            speed_percent: None,
            duration_ms: 120_000,
            include_caster: true,
        }),
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
    };
    defined_words.insert("utani hur".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Heal Friend",
        "exura sio \"name\"",
        SpellKind::Instant,
        18,
        70,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Heavy Magic Missile",
        "adori gran",
        SpellKind::Rune,
        25,
        280,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Intense Healing",
        "exura gran",
        SpellKind::Instant,
        11,
        40,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Intense Healing Rune",
        "adura gran",
        SpellKind::Rune,
        15,
        240,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utana vid",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Invisible".to_string(),
        words: "utana vid".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 440,
        soul_cost: 0,
        level_required: 35,
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
        outfit: Some(SpellOutfitEffect::Apply {
            outfit: Outfit::default(),
            duration_ms: 200_000,
        }),
        challenge: None,
        levitate: None,
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("utana vid".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "exani hur \"up|down\"",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Levitate".to_string(),
        words: "exani hur \"up|down\"".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 50,
        soul_cost: 0,
        level_required: 12,
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
        levitate: Some(SpellLevitateEffect),
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert(
        normalize_spell_words("exani hur \"up|down\"").to_ascii_lowercase(),
    );
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utevo lux",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Light".to_string(),
        words: "utevo lux".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 20,
        soul_cost: 0,
        level_required: 8,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: None,
        light: Some(SpellLightEffect {
            level: 6,
            color: LIGHT_COLOR,
            duration_ms: 500_000,
        }),
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
    };
    defined_words.insert("utevo lux".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Light Healing",
        "exura",
        SpellKind::Instant,
        9,
        25,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Light Magic Missile",
        "adori",
        SpellKind::Rune,
        15,
        120,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Magic Rope",
        "exani tera",
        SpellKind::Instant,
        9,
        20,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utamo vita",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Magic Shield".to_string(),
        words: "utamo vita".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 50,
        soul_cost: 0,
        level_required: 14,
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
        magic_shield: Some(SpellMagicShieldEffect {
            duration_ms: 200_000,
        }),
        outfit: None,
        challenge: None,
        levitate: None,
        raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("utamo vita".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Magic Wall",
        "adevo grav tera",
        SpellKind::Rune,
        32,
        750,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Mass Healing",
        "exura gran mas res",
        SpellKind::Instant,
        36,
        150,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "adana ani",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Paralyze".to_string(),
        words: "adana ani".to_string(),
        kind: SpellKind::Rune,
        rune_type_id: None,
        target: SpellTarget::Creature,
        group: None,
        mana_cost: 1400,
        soul_cost: 0,
        level_required: 54,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: Some(SpellHasteEffect {
            shape: SpellShape::Area { radius: 0 },
            speed_delta: -100,
            speed_percent: None,
            duration_ms: 10_000,
            include_caster: false,
        }),
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
    };
    defined_words.insert("adana ani".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Poison Bomb",
        "adevo mas pox",
        SpellKind::Rune,
        25,
        520,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Poison Field",
        "adevo grav pox",
        SpellKind::Rune,
        14,
        200,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Poison Storm",
        "exevo gran mas pox",
        SpellKind::Instant,
        50,
        600,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Poison Wall",
        "adevo mas grav pox",
        SpellKind::Rune,
        29,
        640,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Poisoned Arrow",
        "exevo con pox",
        SpellKind::Conjure,
        16,
        130,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Power Bolt",
        "exevo con vis",
        SpellKind::Conjure,
        59,
        800,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Soulfire",
        "adevo res flam",
        SpellKind::Rune,
        27,
        600,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utani gran hur",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Strong Haste".to_string(),
        words: "utani gran hur".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 100,
        soul_cost: 0,
        level_required: 20,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: Some(SpellHasteEffect {
            shape: SpellShape::Area { radius: 0 },
            speed_delta: 200,
            speed_percent: None,
            duration_ms: 120_000,
            include_caster: true,
        }),
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
    };
    defined_words.insert("utani gran hur".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Sudden Death",
        "adori vita vis",
        SpellKind::Rune,
        25,
        880,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utevo res \"creature\"",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Summon Creature".to_string(),
        words: "utevo res \"creature\"".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 0,
        soul_cost: 0,
        level_required: 25,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: Some(SummonSpellEffect {
            race_number: None,
            count: 1,
            convince: false,
        }),
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
    };
    defined_words.insert(
        normalize_spell_words("utevo res \"creature\"").to_ascii_lowercase(),
    );
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Ultimate Explosion",
        "exevo gran mas vis",
        SpellKind::Instant,
        60,
        1200,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Ultimate Healing",
        "exura vita",
        SpellKind::Instant,
        20,
        160,
    );
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Ultimate Healing Rune",
        "adura vita",
        SpellKind::Rune,
        24,
        400,
    );
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "utevo vis lux",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Ultimate Light".to_string(),
        words: "utevo vis lux".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 140,
        soul_cost: 0,
        level_required: 26,
        magic_level_required: 0,
        cooldown_ms: 0,
        group_cooldown_ms: 0,
        damage_scale_flags: DamageScaleFlags::NONE,
        effect: None,
        summon: None,
        haste: None,
        light: Some(SpellLightEffect {
            level: 9,
            color: LIGHT_COLOR,
            duration_ms: 2_000_000,
        }),
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
    };
    defined_words.insert("utevo vis lux".to_string());
    spells.push(spell);
    let spell = Spell {
        id: crate::entities::spells::SpellId(spell_id_for_words(
            "exana mas mort",
            &id_by_words,
            &id_by_syllables,
            &mut used_ids,
            &mut fallback_id,
        )),
        name: "Undead Legion".to_string(),
        words: "exana mas mort".to_string(),
        kind: SpellKind::Instant,
        rune_type_id: None,
        target: SpellTarget::SelfOnly,
        group: None,
        mana_cost: 500,
        soul_cost: 0,
        level_required: 30,
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
        raise_dead: Some(SpellRaiseDeadEffect {
            creature_name: "skeleton".to_string(),
            radius: 3,
        }),
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
    };
    defined_words.insert("exana mas mort".to_string());
    spells.push(spell);
    push_spell(
        &mut spells,
        &mut fallback_id,
        &id_by_words,
        &id_by_syllables,
        &mut used_ids,
        &mut defined_words,
        "Wild Growth",
        "exevo grav vita",
        SpellKind::Instant,
        27,
        220,
    );

    for entry in &spell_list_entries {
        let words = if entry.words.is_empty() {
            entry.syllables.as_str()
        } else {
            entry.words.as_str()
        };
        if words.is_empty() {
            continue;
        }
        let normalized = normalize_spell_words(words).to_ascii_lowercase();
        if defined_words.contains(&normalized) || used_ids.contains(&entry.id) {
            continue;
        }
        let Some(kind) = entry.kind else {
            continue;
        };
        let name = if entry.name.is_empty() {
            format!("Spell {}", entry.id)
        } else {
            entry.name.clone()
        };
        let spell = Spell {
            id: crate::entities::spells::SpellId(entry.id),
            name,
            words: words.to_string(),
            kind,
            rune_type_id: None,
            target: default_target_for_kind(kind),
            group: entry.group,
            mana_cost: entry.mana.unwrap_or(0),
            soul_cost: entry
                .soul
                .and_then(|value| u8::try_from(value).ok())
                .unwrap_or(0),
            level_required: entry.level.unwrap_or(0),
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
        };
        defined_words.insert(normalized);
        used_ids.insert(entry.id);
        spells.push(spell);
    }

    let rune_item_ids: HashMap<&'static str, ItemTypeId> = HashMap::from([
        ("adana ani", ItemTypeId(3165)),
        ("adana mort", ItemTypeId(3203)),
        ("adana pox", ItemTypeId(3153)),
        ("adeta sio", ItemTypeId(3177)),
        ("adevo grav flam", ItemTypeId(3188)),
        ("adevo grav pox", ItemTypeId(3172)),
        ("adevo grav tera", ItemTypeId(3180)),
        ("adevo grav vis", ItemTypeId(3164)),
        ("adevo ina", ItemTypeId(3178)),
        ("adevo mas flam", ItemTypeId(3192)),
        ("adevo mas grav flam", ItemTypeId(3190)),
        ("adevo mas grav pox", ItemTypeId(3176)),
        ("adevo mas grav vis", ItemTypeId(3166)),
        ("adevo mas hur", ItemTypeId(3200)),
        ("adevo mas pox", ItemTypeId(3173)),
        ("adevo mas vis", ItemTypeId(3149)),
        ("adevo res flam", ItemTypeId(3195)),
        ("adevo res pox", ItemTypeId(3179)),
        ("adito grav", ItemTypeId(3148)),
        ("adito tera", ItemTypeId(3197)),
        ("adori", ItemTypeId(3174)),
        ("adori flam", ItemTypeId(3189)),
        ("adori gran", ItemTypeId(3198)),
        ("adori gran flam", ItemTypeId(3191)),
        ("adori vita vis", ItemTypeId(3155)),
        ("adura gran", ItemTypeId(3152)),
        ("adura vita", ItemTypeId(3160)),
    ]);
    for spell in &mut spells {
        if spell.kind == SpellKind::Rune {
            if let Some(rune_id) = rune_item_ids.get(spell.words.as_str()) {
                spell.rune_type_id = Some(*rune_id);
            }
        }
    }

    apply_flash_cooldowns(&mut spells);
    apply_spell_list_metadata(&mut spells);
    apply_game_orig_metadata(&mut spells);
    apply_rune_crosswalk(&mut spells);
    apply_placeholder_spell_effects(&mut spells);
    apply_spell_effect_metadata(&mut spells);
    apply_support_spell_effects(&mut spells);
    spells
}

pub fn spell_level_by_id(spell_id: crate::entities::spells::SpellId) -> Option<u16> {
    static LEVELS: OnceLock<HashMap<u16, u16>> = OnceLock::new();
    let levels = LEVELS.get_or_init(|| {
        const CSV: &str =
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_list.csv"));
        let mut map = HashMap::new();
        for (idx, line) in CSV.lines().enumerate() {
            if idx == 0 || line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split(',').collect();
            if fields.len() < 8 {
                continue;
            }
            let id = match fields[0].trim().parse::<u16>() {
                Ok(id) => id,
                Err(_) => continue,
            };
            let level = fields[7].trim().parse::<u16>().ok();
            if let Some(level) = level {
                map.insert(id, level);
            }
        }
        map
    });
    levels.get(&spell_id.0).copied()
}
