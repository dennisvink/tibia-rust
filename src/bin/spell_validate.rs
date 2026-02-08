use std::collections::HashMap;
use tibia::entities::spells::{register_builtin_spells, SpellBook};

#[derive(Clone, Copy)]
struct SpellMeta {
    level: Option<u16>,
    mana: Option<u16>,
    soul: Option<u16>,
}

fn normalize_spell_words(words: &str) -> String {
    words
        .replace("{0:-name}", "\"name\"")
        .replace("{0:-creature}", "\"creature\"")
        .replace("{0:-up|down}", "\"up|down\"")
}

fn normalize_csv_field(field: &str) -> String {
    let mut value = field.trim().to_string();
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        value = value[1..value.len() - 1].to_string();
    }
    value.replace("\"\"", "\"")
}

fn load_spell_metadata() -> HashMap<String, SpellMeta> {
    const CSV: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_list.csv"));
    let mut by_words = HashMap::new();
    for (idx, line) in CSV.lines().enumerate() {
        if idx == 0 || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 10 {
            continue;
        }
        let words = normalize_spell_words(&normalize_csv_field(fields[5]));
        if words.is_empty() {
            continue;
        }
        let level = fields[7].trim().parse::<u16>().ok();
        let mana = fields[8].trim().parse::<u16>().ok();
        let soul = fields[9].trim().parse::<u16>().ok();
        by_words.insert(
            words.to_ascii_lowercase(),
            SpellMeta { level, mana, soul },
        );
    }
    by_words
}

fn main() -> Result<(), String> {
    let mut spellbook = SpellBook::default();
    register_builtin_spells(&mut spellbook)?;
    let meta = load_spell_metadata();

    let mut mismatches = Vec::new();
    let mut missing = Vec::new();

    for spell in spellbook.iter() {
        let key = normalize_spell_words(&spell.words).to_ascii_lowercase();
        let Some(entry) = meta.get(&key) else {
            missing.push(spell.words.clone());
            continue;
        };
        if let Some(level) = entry.level {
            if spell.level_required != level {
                mismatches.push(format!(
                    "{} level: server={} csv={}",
                    spell.words, spell.level_required, level
                ));
            }
        }
        if let Some(mana) = entry.mana {
            if spell.mana_cost != mana {
                mismatches.push(format!(
                    "{} mana: server={} csv={}",
                    spell.words, spell.mana_cost, mana
                ));
            }
        }
        if let Some(soul) = entry.soul {
            if spell.soul_cost != soul as u8 {
                mismatches.push(format!(
                    "{} soul: server={} csv={}",
                    spell.words, spell.soul_cost, soul
                ));
            }
        }
    }

    println!("spell metadata check:");
    println!("- spells: {}", spellbook.len());
    println!("- csv entries: {}", meta.len());
    println!("- missing csv entries: {}", missing.len());
    println!("- mismatches: {}", mismatches.len());
    if !missing.is_empty() {
        println!("missing csv words:");
        for word in missing {
            println!("- {}", word);
        }
    }
    if !mismatches.is_empty() {
        println!("mismatches:");
        for mismatch in mismatches {
            println!("- {}", mismatch);
        }
        return Err("spell metadata mismatches detected".to_string());
    }

    Ok(())
}
