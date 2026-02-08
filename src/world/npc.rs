use crate::entities::creature::{Outfit, DEFAULT_OUTFIT};
use crate::scripting::npc::{load_npc_script, NpcAction, NpcCondition, NpcScript};
use crate::scripting::value::ScriptValue;
use crate::world::position::Position;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpcDefinition {
    pub script_key: String,
    pub name: String,
    pub home: Option<Position>,
    pub radius: Option<u16>,
    pub outfit: Outfit,
}

#[derive(Debug, Default)]
pub struct NpcIndex {
    pub scripts: HashMap<String, NpcScript>,
    pub definitions: Vec<NpcDefinition>,
}

#[derive(Debug, Default)]
pub struct NpcValidationReport {
    pub files: usize,
    pub parsed: usize,
    pub errors: Vec<String>,
}

pub fn load_npcs(dir: &Path) -> Result<NpcIndex, String> {
    let entries = fs::read_dir(dir)
        .map_err(|err| format!("failed to read npc dir {}: {}", dir.display(), err))?;
    let mut scripts = HashMap::new();
    let mut definitions = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read npc dir entry: {}", err))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext != "npc" && ext != "ndb" {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| format!("npc script path missing stem: {}", path.display()))?;
        let script = match load_npc_script(&path) {
            Ok(script) => script,
            Err(err) => {
                eprintln!("tibia: npc script skipped: {}", err);
                continue;
            }
        };
        let name = script
            .name
            .clone()
            .unwrap_or_else(|| stem.to_string());
        let home = script.field("Home").and_then(parse_position);
        let radius = script.field("Radius").and_then(parse_u16);
        let outfit = script
            .field("Outfit")
            .and_then(|value| parse_outfit(value).ok())
            .unwrap_or(DEFAULT_OUTFIT);
        definitions.push(NpcDefinition {
            script_key: stem.to_string(),
            name,
            home,
            radius,
            outfit,
        });
        if scripts.insert(stem.to_string(), script).is_some() {
            return Err(format!("duplicate npc script key {}", stem));
        }
    }

    Ok(NpcIndex {
        scripts,
        definitions,
    })
}

pub fn validate_npcs(dir: &Path) -> NpcValidationReport {
    let mut report = NpcValidationReport::default();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            report
                .errors
                .push(format!("failed to read npc dir {}: {}", dir.display(), err));
            return report;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                report
                    .errors
                    .push(format!("failed to read npc dir entry: {}", err));
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
        if ext != "npc" && ext != "ndb" {
            continue;
        }
        report.files += 1;
        match load_npc_script(&path) {
            Ok(_) => report.parsed += 1,
            Err(err) => report
                .errors
                .push(format!("npc script {}: {}", path.display(), err)),
        }
    }

    report
}

fn parse_u16(value: &ScriptValue) -> Option<u16> {
    match value {
        ScriptValue::Number(number) => u16::try_from(*number).ok(),
        _ => None,
    }
}

fn parse_position(value: &ScriptValue) -> Option<Position> {
    let items = match value {
        ScriptValue::List(items) => items,
        ScriptValue::Tuple(items) => items,
        _ => return None,
    };
    if items.len() != 3 {
        return None;
    }
    let x = match items.get(0)? {
        ScriptValue::Number(number) => u16::try_from(*number).ok()?,
        _ => return None,
    };
    let y = match items.get(1)? {
        ScriptValue::Number(number) => u16::try_from(*number).ok()?,
        _ => return None,
    };
    let z = match items.get(2)? {
        ScriptValue::Number(number) => u8::try_from(*number).ok()?,
        _ => return None,
    };
    Some(Position { x, y, z })
}

fn parse_outfit(value: &ScriptValue) -> Result<Outfit, String> {
    let ScriptValue::Tuple(parts) = value else {
        return Err("Outfit expects tuple".to_string());
    };
    if parts.is_empty() {
        return Err("Outfit expects at least look_type".to_string());
    }
    let look_type = parse_u16(&parts[0]).ok_or_else(|| "Outfit expects look_type".to_string())?;
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
            if let Some(look_item) = parse_u16(&parts[1]) {
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
                let component = parse_u16(part)?;
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

pub fn npc_reply_for_message(
    script: &NpcScript,
    message: &str,
    player_name: &str,
) -> Option<String> {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    let tokens = tokenize_message(&normalized);
    for rule in &script.behaviour {
        if !rule_matches_message(rule, &normalized, &tokens) {
            continue;
        }
        if let Some(reply) = rule_reply(rule) {
            return Some(reply.replace("%N", player_name));
        }
    }
    None
}

fn tokenize_message(message: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in message.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn rule_matches_message(rule: &crate::scripting::npc::NpcBehaviourRule, message: &str, tokens: &[String]) -> bool {
    let mut string_conditions = Vec::new();
    for condition in &rule.parsed_conditions {
        if let NpcCondition::String(value) = condition {
            string_conditions.push(value);
        }
    }
    if string_conditions.is_empty() {
        return false;
    }
    for raw in string_conditions {
        let value = raw.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        if let Some(stripped) = value.strip_suffix('$') {
            if message != stripped {
                return false;
            }
            continue;
        }
        if !tokens.iter().any(|token| token == &value) {
            return false;
        }
    }
    true
}

fn rule_reply(rule: &crate::scripting::npc::NpcBehaviourRule) -> Option<String> {
    for action in &rule.parsed_actions {
        if let NpcAction::Say(text) = action {
            return Some(text.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_position_accepts_list() {
        let value = ScriptValue::List(vec![
            ScriptValue::Number(100),
            ScriptValue::Number(200),
            ScriptValue::Number(7),
        ]);
        let position = parse_position(&value).expect("position");
        assert_eq!(position, Position { x: 100, y: 200, z: 7 });
    }

    #[test]
    fn load_npcs_from_assets() {
        let asset_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let npc_dir = asset_root.join("npc");
        let index = load_npcs(&npc_dir).expect("npc load");
        assert!(!index.scripts.is_empty());
        assert!(!index.definitions.is_empty());
    }
}
