use tibia::entities::spells::{register_builtin_spells, Spell, SpellBook, SpellGroupId};

fn group_label(group: Option<SpellGroupId>) -> &'static str {
    match group {
        Some(SpellGroupId(1)) => "attack",
        Some(SpellGroupId(2)) => "healing",
        Some(SpellGroupId(3)) => "support",
        Some(SpellGroupId(4)) => "powerstrikes",
        Some(_) => "other",
        None => "none",
    }
}

fn spell_has_payload(spell: &Spell) -> bool {
    spell.effect.is_some()
        || spell.summon.is_some()
        || spell.haste.is_some()
        || spell.light.is_some()
        || spell.dispel.is_some()
        || spell.field.is_some()
        || spell.magic_shield.is_some()
        || spell.outfit.is_some()
        || spell.challenge.is_some()
        || spell.levitate.is_some()
        || spell.raise_dead.is_some()
        || spell.conjure.is_some()
        || spell.antidote.is_some()
        || spell.magic_rope.is_some()
        || spell.find_person.is_some()
        || spell.enchant_staff.is_some()
}

fn main() -> Result<(), String> {
    let mut spellbook = SpellBook::default();
    register_builtin_spells(&mut spellbook)?;

    let mut missing = Vec::new();
    for spell in spellbook.iter() {
        if !spell_has_payload(spell) {
            missing.push(spell);
        }
    }

    println!("spell payload audit:");
    println!("- spells: {}", spellbook.len());
    println!("- missing payloads: {}", missing.len());
    for spell in missing {
        println!(
            "- {} ({}) kind={:?} group={}",
            spell.words,
            spell.name,
            spell.kind,
            group_label(spell.group)
        );
    }

    Ok(())
}
