use tibia::entities::spell_definitions::builtin_spells;
use tibia::entities::spells::SpellKind;

fn count_spell_list() -> (usize, usize, usize, usize) {
    const CSV: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/spells/spell_list.csv"));
    let mut total = 0usize;
    let mut rune = 0usize;
    let mut instant = 0usize;
    let mut conjure = 0usize;
    for (idx, line) in CSV.lines().enumerate() {
        if idx == 0 || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 7 {
            continue;
        }
        total += 1;
        match fields[6].trim() {
            "TYPE_RUNE" => rune += 1,
            "TYPE_INSTANT" => instant += 1,
            "TYPE_CONJURE" => conjure += 1,
            _ => {}
        }
    }
    (total, rune, instant, conjure)
}

fn main() {
    let spells = builtin_spells();
    let builtin_total = spells.len();
    let builtin_rune = spells.iter().filter(|spell| spell.kind == SpellKind::Rune).count();
    let builtin_instant = spells
        .iter()
        .filter(|spell| spell.kind == SpellKind::Instant)
        .count();
    let builtin_conjure = spells
        .iter()
        .filter(|spell| spell.kind == SpellKind::Conjure)
        .count();
    let (list_total, list_rune, list_instant, list_conjure) = count_spell_list();

    println!("builtin total: {}", builtin_total);
    println!(
        "builtin rune/instant/conjure: {}/{}/{}",
        builtin_rune, builtin_instant, builtin_conjure
    );
    println!("game.orig total: {}", list_total);
    println!(
        "game.orig rune/instant/conjure: {}/{}/{}",
        list_rune, list_instant, list_conjure
    );
    println!(
        "delta rune/instant/conjure: {}/{}/{}",
        builtin_rune as isize - list_rune as isize,
        builtin_instant as isize - list_instant as isize,
        builtin_conjure as isize - list_conjure as isize
    );
}
