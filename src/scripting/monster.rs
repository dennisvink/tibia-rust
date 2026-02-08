use crate::combat::damage::DamageType;
use crate::entities::creature::Outfit;
use crate::entities::item::ItemTypeId;
use crate::scripting::value::{parse_value, ScriptValue};
use std::path::Path;

#[derive(Debug, Default)]
pub struct MonsterDefinition {
    pub name: Option<String>,
    pub fields: Vec<(String, ScriptValue)>,
}

pub type MonsterScript = MonsterDefinition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterLootEntry {
    pub type_id: ItemTypeId,
    pub count: u16,
    pub chance: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterSkillEntry {
    pub name: String,
    pub values: MonsterSkillValues,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonsterSkillValues {
    pub current: i64,
    pub base: i64,
    pub cap: i64,
    pub progress: i64,
    pub tries: i64,
    pub extra: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterSpell {
    pub target: MonsterSpellTarget,
    pub effect: MonsterSpellEffect,
    pub chance: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonsterSpellTarget {
    Actor(Vec<ScriptValue>),
    Victim(Vec<ScriptValue>),
    Origin(Vec<ScriptValue>),
    Destination(Vec<ScriptValue>),
    Angle(Vec<ScriptValue>),
    Unknown { name: String, args: Vec<ScriptValue> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonsterSpellEffect {
    Damage {
        mask: u16,
        damage_type: DamageType,
        args: Vec<ScriptValue>,
    },
    Healing { args: Vec<ScriptValue> },
    Summon { args: Vec<ScriptValue> },
    Speed { args: Vec<ScriptValue> },
    Outfit { args: Vec<ScriptValue> },
    Field { args: Vec<ScriptValue> },
    Drunken { args: Vec<ScriptValue> },
    Strength { args: Vec<ScriptValue> },
    Unknown { name: String, args: Vec<ScriptValue> },
}

impl MonsterDefinition {
    pub fn field_value(&self, key: &str) -> Option<&ScriptValue> {
        self.fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(key))
            .map(|(_, value)| value)
    }

    pub fn race_number(&self) -> Option<i64> {
        match self.field_value("RaceNumber")? {
            ScriptValue::Number(value) => Some(*value),
            ScriptValue::Ident(value) => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn experience(&self) -> Option<u32> {
        parse_script_u32(self.field_value("Experience")?).ok()
    }

    pub fn article(&self) -> Option<String> {
        parse_script_string(self.field_value("Article")?)
    }

    pub fn blood(&self) -> Option<String> {
        parse_script_string(self.field_value("Blood")?)
    }

    pub fn summon_cost(&self) -> Option<u32> {
        parse_script_u32(self.field_value("SummonCost")?).ok()
    }

    pub fn hitpoints(&self) -> Option<u32> {
        self.skill_value("HitPoints")
    }

    pub fn flee_threshold(&self) -> Option<u32> {
        parse_script_u32(self.field_value("FleeThreshold")?).ok()
    }

    pub fn lose_target(&self) -> Option<u32> {
        parse_script_u32(self.field_value("LoseTarget")?).ok()
    }

    pub fn attack(&self) -> Option<u32> {
        parse_script_u32(self.field_value("Attack")?).ok()
    }

    pub fn defend(&self) -> Option<u32> {
        parse_script_u32(self.field_value("Defend")?).ok()
    }

    pub fn armor(&self) -> Option<u32> {
        parse_script_u32(self.field_value("Armor")?).ok()
    }

    pub fn poison(&self) -> Option<u32> {
        parse_script_u32(self.field_value("Poison")?).ok()
    }

    pub fn strategy(&self) -> Option<[u8; 4]> {
        let value = self.field_value("Strategy")?;
        let parts = match value {
            ScriptValue::Tuple(parts) | ScriptValue::List(parts) => parts,
            _ => return None,
        };
        if parts.len() < 4 {
            return None;
        }
        let mut strategy = [0u8; 4];
        for (slot, part) in strategy.iter_mut().zip(parts.iter().take(4)) {
            let value = parse_script_u16(part).ok()?;
            *slot = value.min(u16::from(u8::MAX)) as u8;
        }
        Some(strategy)
    }

    pub fn loot_entries(&self) -> Result<Vec<MonsterLootEntry>, String> {
        let value = match self.field_value("Inventory") {
            Some(value) => value,
            None => return Ok(Vec::new()),
        };
        let ScriptValue::List(items) = value else {
            return Err("Inventory expects a list".to_string());
        };
        let mut entries = Vec::with_capacity(items.len());
        for item in items {
            entries.push(parse_loot_entry(item)?);
        }
        Ok(entries)
    }

    pub fn corpse_ids(&self) -> Result<Vec<u16>, String> {
        if let Some(value) = self.field_value("Corpse") {
            return parse_u16_list(value);
        }
        if let Some(value) = self.field_value("Corpses") {
            return parse_u16_list(value);
        }
        Ok(Vec::new())
    }

    pub fn flags(&self) -> Result<Vec<String>, String> {
        let value = match self.field_value("Flags") {
            Some(value) => value,
            None => return Ok(Vec::new()),
        };
        parse_string_list(value)
    }

    pub fn talk_lines(&self) -> Result<Vec<String>, String> {
        let value = match self.field_value("Talk") {
            Some(value) => value,
            None => return Ok(Vec::new()),
        };
        parse_string_list(value)
    }

    pub fn skills(&self) -> Result<Vec<MonsterSkillEntry>, String> {
        let value = match self.field_value("Skills") {
            Some(value) => value,
            None => return Ok(Vec::new()),
        };
        let ScriptValue::List(entries) = value else {
            return Err("Skills expects a list".to_string());
        };
        let mut skills = Vec::with_capacity(entries.len());
        for entry in entries {
            let ScriptValue::Tuple(parts) = entry else {
                return Err("Skills entry expects tuple".to_string());
            };
            if parts.len() < 2 {
                return Err("Skills entry expects at least 2 values".to_string());
            }
            let name = match &parts[0] {
                ScriptValue::Ident(name) | ScriptValue::String(name) => name.clone(),
                _ => return Err("Skills entry expects name".to_string()),
            };
            let values = parse_skill_values(&parts[1..])?;
            skills.push(MonsterSkillEntry { name, values });
        }
        Ok(skills)
    }

    pub fn spells(&self) -> Result<Vec<MonsterSpell>, String> {
        let value = match self.field_value("Spells") {
            Some(value) => value,
            None => return Ok(Vec::new()),
        };
        let ScriptValue::List(entries) = value else {
            return Err("Spells expects a list".to_string());
        };
        let mut spells = Vec::with_capacity(entries.len());
        for entry in entries {
            let raw = match entry {
                ScriptValue::Ident(value) | ScriptValue::String(value) => value,
                _ => return Err("Spells entry expects string".to_string()),
            };
            spells.push(parse_spell_entry(raw)?);
        }
        Ok(spells)
    }

    pub fn outfit(&self) -> Option<Outfit> {
        let value = self.field_value("Outfit")?;
        parse_outfit(value).ok()
    }

    pub fn skill_value(&self, skill: &str) -> Option<u32> {
        let value = self.field_value("Skills")?;
        let ScriptValue::List(entries) = value else {
            return None;
        };
        for entry in entries {
            let ScriptValue::Tuple(parts) = entry else {
                continue;
            };
            if parts.len() < 2 {
                continue;
            }
            let name = match &parts[0] {
                ScriptValue::Ident(name) | ScriptValue::String(name) => name.as_str(),
                _ => continue,
            };
            if !name.eq_ignore_ascii_case(skill) {
                continue;
            }
            let values = parse_skill_values(&parts[1..]).ok()?;
            return u32::try_from(values.current).ok();
        }
        None
    }
}

pub fn load_monster_script(path: &Path) -> Result<MonsterDefinition, String> {
    let bytes = std::fs::read(path).map_err(|err| {
        format!(
            "failed to read monster script {}: {}",
            path.display(),
            err
        )
    })?;
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            let bytes = err.into_bytes();
            eprintln!(
                "tibia: monster script contained invalid UTF-8; decoding latin-1: {}",
                path.display()
            );
            bytes.iter().map(|b| *b as char).collect()
        }
    };
    parse_monster_script(&content)
        .map_err(|err| format!("monster script {}: {}", path.display(), err))
}

pub fn parse_monster_script(content: &str) -> Result<MonsterDefinition, String> {
    let mut script = MonsterDefinition::default();

    for (line_no, line) in coalesce_lines(content).into_iter().enumerate() {
        let line_no = line_no + 1;
        for assignment in split_assignments(&line) {
            let Some((key, value)) = assignment.split_once('=') else {
                continue;
            };
            let key = key.trim().to_string();
            let value = value.trim();
            let parsed = parse_value(value)
                .map_err(|err| format!("line {} invalid value: {}", line_no, err))?;
            if key.eq_ignore_ascii_case("Name") {
                if let ScriptValue::String(name) = &parsed {
                    script.name = Some(name.clone());
                }
            }
            script.fields.push((key, parsed));
        }
    }

    Ok(script)
}

fn coalesce_lines(content: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut buffer = String::new();
    let mut depth: i32 = 0;
    let mut in_quotes = false;

    for raw_line in content.lines() {
        let line = strip_inline_comment(raw_line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if !buffer.is_empty() {
            buffer.push(' ');
        }
        buffer.push_str(line);

        for ch in line.chars() {
            match ch {
                '"' => in_quotes = !in_quotes,
                '{' | '(' if !in_quotes => depth += 1,
                '}' | ')' if !in_quotes => depth -= 1,
                _ => {}
            }
        }

        if depth <= 0 && !in_quotes {
            lines.push(buffer.clone());
            buffer.clear();
            depth = 0;
        }
    }

    if !buffer.is_empty() {
        lines.push(buffer);
    }

    lines
}

fn strip_inline_comment(line: &str) -> String {
    let mut in_quotes = false;
    for (idx, ch) in line.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '#' if !in_quotes => return line[..idx].to_string(),
            _ => {}
        }
    }
    line.to_string()
}

fn split_assignments(line: &str) -> Vec<String> {
    let mut starts = Vec::new();
    let mut in_quotes = false;
    let chars: Vec<char> = line.chars().collect();

    for (idx, ch) in chars.iter().enumerate() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '=' if !in_quotes => {
                let mut key_end = idx;
                while key_end > 0 && chars[key_end - 1].is_whitespace() {
                    key_end -= 1;
                }
                let mut key_start = key_end;
                while key_start > 0
                    && (chars[key_start - 1].is_ascii_alphanumeric()
                        || chars[key_start - 1] == '_')
                {
                    key_start -= 1;
                }
                if key_start < key_end
                    && (key_start == 0 || chars[key_start - 1].is_whitespace())
                {
                    starts.push(key_start);
                }
            }
            _ => {}
        }
    }

    if starts.is_empty() {
        return vec![line.trim().to_string()];
    }

    let mut segments = Vec::new();
    for (idx, start) in starts.iter().enumerate() {
        let end = starts
            .get(idx + 1)
            .copied()
            .unwrap_or_else(|| line.len());
        segments.push(line[*start..end].trim().to_string());
    }
    segments
}

fn parse_loot_entry(value: &ScriptValue) -> Result<MonsterLootEntry, String> {
    let ScriptValue::Tuple(parts) = value else {
        return Err("Inventory entry expects tuple".to_string());
    };
    if parts.len() != 3 {
        return Err("Inventory entry expects 3 values".to_string());
    }
    let type_id = parse_script_u16(&parts[0])?;
    let count = parse_script_u16(&parts[1])?;
    let chance = parse_script_u16(&parts[2])?;
    Ok(MonsterLootEntry {
        type_id: ItemTypeId(type_id),
        count,
        chance,
    })
}

fn parse_skill_values(parts: &[ScriptValue]) -> Result<MonsterSkillValues, String> {
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        values.push(parse_script_i64(part)?);
    }
    if values.len() < 6 {
        return Err("Skills entry expects 6 values".to_string());
    }
    Ok(MonsterSkillValues {
        current: values[0],
        base: values[1],
        cap: values[2],
        progress: values[3],
        tries: values[4],
        extra: values[5],
    })
}

fn parse_outfit(value: &ScriptValue) -> Result<Outfit, String> {
    let ScriptValue::Tuple(parts) = value else {
        return Err("Outfit expects tuple".to_string());
    };
    if parts.is_empty() {
        return Err("Outfit expects at least look_type".to_string());
    }
    let look_type = parse_script_u16(&parts[0])?;
    let mut outfit = Outfit {
        look_type,
        head: 0,
        body: 0,
        legs: 0,
        feet: 0,
        addons: 0,
        look_item: 0,
    };
    if parts.len() >= 2 {
        if let Some(colors) = parse_outfit_colors(&parts[1]) {
            outfit.head = colors[0];
            outfit.body = colors[1];
            outfit.legs = colors[2];
            outfit.feet = colors[3];
        } else if look_type == 0 {
            if let Ok(look_item) = parse_script_u16(&parts[1]) {
                outfit.look_item = look_item;
            }
        }
    }
    Ok(outfit)
}

fn parse_outfit_colors(value: &ScriptValue) -> Option<[u8; 4]> {
    match value {
        ScriptValue::Tuple(parts) | ScriptValue::List(parts) => {
            if parts.len() < 4 {
                return None;
            }
            let mut colors = [0u8; 4];
            for (idx, part) in parts.iter().take(4).enumerate() {
                let component = parse_script_u16(part).ok()?;
                colors[idx] = u8::try_from(component).ok()?;
            }
            Some(colors)
        }
        ScriptValue::Ident(value) => {
            let mut colors = [0u8; 4];
            let mut iter = value.split('-');
            for slot in &mut colors {
                let raw = iter.next()?;
                let component = raw.trim().parse::<i64>().ok()?;
                if !(0..=255).contains(&component) {
                    return None;
                }
                *slot = component as u8;
            }
            Some(colors)
        }
        _ => None,
    }
}

fn parse_string_list(value: &ScriptValue) -> Result<Vec<String>, String> {
    let ScriptValue::List(entries) = value else {
        return Err("value expects list".to_string());
    };
    let mut items = Vec::with_capacity(entries.len());
    for entry in entries {
        items.push(
            parse_script_string(entry)
                .ok_or_else(|| "value expects string".to_string())?,
        );
    }
    Ok(items)
}

fn parse_script_string(value: &ScriptValue) -> Option<String> {
    match value {
        ScriptValue::String(value) | ScriptValue::Ident(value) => Some(value.clone()),
        _ => None,
    }
}

fn parse_u16_list(value: &ScriptValue) -> Result<Vec<u16>, String> {
    match value {
        ScriptValue::Number(_) => Ok(vec![parse_script_u16(value)?]),
        ScriptValue::Ident(raw) | ScriptValue::String(raw) => parse_u16_list_from_text(raw),
        ScriptValue::Tuple(parts) | ScriptValue::List(parts) => {
            let mut values = Vec::with_capacity(parts.len());
            for part in parts {
                values.push(parse_script_u16(part)?);
            }
            Ok(values)
        }
    }
}

fn parse_u16_list_from_text(raw: &str) -> Result<Vec<u16>, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = trimmed
            .parse::<u16>()
            .map_err(|_| "value expects u16".to_string())?;
        values.push(value);
    }
    if values.is_empty() {
        return Err("value expects u16 list".to_string());
    }
    Ok(values)
}

fn parse_spell_entry(entry: &str) -> Result<MonsterSpell, String> {
    let entry = entry.trim();
    let (target_part, effect_part) = entry
        .split_once("->")
        .ok_or_else(|| "spell entry expects '->'".to_string())?;
    let (effect_part, chance_part) = effect_part
        .rsplit_once(':')
        .ok_or_else(|| "spell entry expects ':'".to_string())?;
    let chance = chance_part
        .trim()
        .parse::<u32>()
        .map_err(|_| "spell entry expects chance".to_string())?;

    let (target_name, target_args) = split_name_and_args(target_part)?;
    let (effect_name, effect_args) = split_name_and_args(effect_part)?;

    let target = parse_spell_target(&target_name, target_args);
    let effect = parse_spell_effect(&effect_name, effect_args)?;

    Ok(MonsterSpell {
        target,
        effect,
        chance,
    })
}

fn split_name_and_args(value: &str) -> Result<(String, Vec<ScriptValue>), String> {
    let value = value.trim();
    let Some(open) = value.find('(') else {
        return Ok((value.to_string(), Vec::new()));
    };
    let close = value
        .rfind(')')
        .ok_or_else(|| "spell entry expects ')'".to_string())?;
    if close < open {
        return Err("spell entry has invalid args".to_string());
    }
    let name = value[..open].trim();
    let raw_args = value[open..=close].trim();
    let parsed = parse_value(raw_args)
        .map_err(|_| "spell entry has invalid args".to_string())?;
    let args = match parsed {
        ScriptValue::Tuple(parts) | ScriptValue::List(parts) => parts,
        other => vec![other],
    };
    Ok((name.to_string(), args))
}

fn parse_spell_target(name: &str, args: Vec<ScriptValue>) -> MonsterSpellTarget {
    if name.eq_ignore_ascii_case("Actor") {
        return MonsterSpellTarget::Actor(args);
    }
    if name.eq_ignore_ascii_case("Victim") {
        return MonsterSpellTarget::Victim(args);
    }
    if name.eq_ignore_ascii_case("Origin") {
        return MonsterSpellTarget::Origin(args);
    }
    if name.eq_ignore_ascii_case("Destination") {
        return MonsterSpellTarget::Destination(args);
    }
    if name.eq_ignore_ascii_case("Angle") {
        return MonsterSpellTarget::Angle(args);
    }
    MonsterSpellTarget::Unknown {
        name: name.to_string(),
        args,
    }
}

fn parse_spell_effect(name: &str, args: Vec<ScriptValue>) -> Result<MonsterSpellEffect, String> {
    if name.eq_ignore_ascii_case("Damage") {
        let mask = args
            .get(0)
            .ok_or_else(|| "Damage expects mask".to_string())
            .and_then(parse_script_u16)?;
        return Ok(MonsterSpellEffect::Damage {
            mask,
            damage_type: DamageType::from_mask(mask),
            args,
        });
    }
    if name.eq_ignore_ascii_case("Healing") {
        return Ok(MonsterSpellEffect::Healing { args });
    }
    if name.eq_ignore_ascii_case("Summon") {
        return Ok(MonsterSpellEffect::Summon { args });
    }
    if name.eq_ignore_ascii_case("Speed") {
        return Ok(MonsterSpellEffect::Speed { args });
    }
    if name.eq_ignore_ascii_case("Outfit") {
        return Ok(MonsterSpellEffect::Outfit { args });
    }
    if name.eq_ignore_ascii_case("Field") {
        return Ok(MonsterSpellEffect::Field { args });
    }
    if name.eq_ignore_ascii_case("Drunken") {
        return Ok(MonsterSpellEffect::Drunken { args });
    }
    if name.eq_ignore_ascii_case("Strength") {
        return Ok(MonsterSpellEffect::Strength { args });
    }
    Ok(MonsterSpellEffect::Unknown {
        name: name.to_string(),
        args,
    })
}

fn parse_script_u16(value: &ScriptValue) -> Result<u16, String> {
    let raw = parse_script_i64(value)?;
    if raw < 0 {
        return Err("value must be non-negative".to_string());
    }
    u16::try_from(raw).map_err(|_| "value out of range".to_string())
}

fn parse_script_u32(value: &ScriptValue) -> Result<u32, String> {
    let raw = parse_script_i64(value)?;
    if raw < 0 {
        return Err("value must be non-negative".to_string());
    }
    u32::try_from(raw).map_err(|_| "value out of range".to_string())
}

fn parse_script_i64(value: &ScriptValue) -> Result<i64, String> {
    match value {
        ScriptValue::Number(number) => Ok(*number),
        ScriptValue::Ident(ident) => ident
            .parse::<i64>()
            .map_err(|_| "value expects number".to_string()),
        _ => Err("value expects number".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_monster_script_reads_race_number() {
        let input = r#"
RaceNumber = 16
Name = "bear"
"#;
        let script = parse_monster_script(input).expect("parse");
        assert_eq!(script.name.as_deref(), Some("bear"));
        assert_eq!(script.race_number(), Some(16));
    }

    #[test]
    fn parse_monster_script_reads_experience_and_inventory() {
        let input = r#"
RaceNumber = 16
Experience = 42
Inventory = {(3031, 10, 999), (3492, 1, 5)}
"#;
        let script = parse_monster_script(input).expect("parse");
        assert_eq!(script.experience(), Some(42));
        let loot = script.loot_entries().expect("loot");
        assert_eq!(
            loot,
            vec![
                MonsterLootEntry {
                    type_id: ItemTypeId(3031),
                    count: 10,
                    chance: 999,
                },
                MonsterLootEntry {
                    type_id: ItemTypeId(3492),
                    count: 1,
                    chance: 5,
                },
            ]
        );
    }

    #[test]
    fn parse_monster_script_reads_hitpoints_from_skills() {
        let input = r#"
Skills = {(HitPoints, 80, 0, 80, 0, 0, 0), (FistFighting, 15, 0, 0, 0, 0, 0)}
"#;
        let script = parse_monster_script(input).expect("parse");
        assert_eq!(script.hitpoints(), Some(80));
    }

    #[test]
    fn parse_monster_script_reads_flee_and_lose_target() {
        let input = r#"
RaceNumber = 16
FleeThreshold = 10
LoseTarget = 5
"#;
        let script = parse_monster_script(input).expect("parse");
        assert_eq!(script.flee_threshold(), Some(10));
        assert_eq!(script.lose_target(), Some(5));
    }

    #[test]
    fn parse_monster_script_reads_corpses_and_flags() {
        let input = r#"
Corpses = 4240, 4247
Flags = {KickBoxes, NoSummon}
"#;
        let script = parse_monster_script(input).expect("parse");
        assert_eq!(script.corpse_ids().expect("corpses"), vec![4240, 4247]);
        assert_eq!(
            script.flags().expect("flags"),
            vec!["KickBoxes".to_string(), "NoSummon".to_string()]
        );
    }

    #[test]
    fn parse_monster_script_reads_talk_lines() {
        let input = "Talk = {\"#Y Hi\", \"Hello\"}\n";
        let script = parse_monster_script(input).expect("parse");
        assert_eq!(
            script.talk_lines().expect("talk"),
            vec!["#Y Hi".to_string(), "Hello".to_string()]
        );
    }

    #[test]
    fn parse_monster_script_reads_spell_entries() {
        let input = r#"
Spells = {Victim (7, 9, 0) -> Damage (1, 25, 5) : 10}
"#;
        let script = parse_monster_script(input).expect("parse");
        let spells = script.spells().expect("spells");
        assert_eq!(spells.len(), 1);
        assert_eq!(spells[0].chance, 10);
        match &spells[0].target {
            MonsterSpellTarget::Victim(args) => {
                assert_eq!(
                    args,
                    &vec![
                        ScriptValue::Number(7),
                        ScriptValue::Number(9),
                        ScriptValue::Number(0),
                    ]
                );
            }
            other => panic!("unexpected target {:?}", other),
        }
        match &spells[0].effect {
            MonsterSpellEffect::Damage {
                mask,
                damage_type,
                args,
            } => {
                assert_eq!(*mask, 1);
                assert_eq!(*damage_type, DamageType::Physical);
                assert_eq!(
                    args,
                    &vec![
                        ScriptValue::Number(1),
                        ScriptValue::Number(25),
                        ScriptValue::Number(5),
                    ]
                );
            }
            other => panic!("unexpected effect {:?}", other),
        }
    }
}
