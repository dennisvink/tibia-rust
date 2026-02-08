use crate::entities::creature::Outfit;
use crate::entities::inventory::{Inventory, InventorySlot};
use crate::entities::item::{ItemAttribute, ItemStack, ItemTypeId};
use crate::entities::player::{PlayerId, PlayerState};
use crate::entities::skills::{
    apply_skill_progress_values,
    default_skill_row_values,
    skill_exp_for_level,
    skill_progress_from_values,
    SkillLevel,
    SkillRow,
    SkillSet,
    RAW_SKILL_FIELDS,
};
use crate::entities::spells::SpellId;
use crate::entities::stats::{DamageResistances, Stats};
use crate::world::position::{Direction, Position};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct SaveStore {
    root: PathBuf,
}

#[derive(Debug, Default)]
pub struct SaveValidationReport {
    pub player_files: usize,
    pub parsed: usize,
    pub skipped: usize,
    pub errors: Vec<String>,
    pub missing_dir: bool,
}

impl SaveStore {
    pub fn from_root(root: &Path) -> Self {
        Self {
            root: root.join("save"),
        }
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn load_player(&self, id: PlayerId) -> Result<Option<PlayerState>, String> {
        let path = self.player_path(id);
        let backup_path = self.player_backup_path(id);
        let legacy_backup_path = self.player_legacy_backup_path(id);
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return self.load_player_from_backups(id, &backup_path, &legacy_backup_path);
            }
            Err(err) => {
                return Err(format!(
                    "player save read failed for {}: {}",
                    path.display(),
                    err
                ))
            }
        };
        let parsed = match PlayerSave::parse(&data) {
            Ok(parsed) => parsed,
            Err(err) => {
                if let Some(fallback) =
                    self.load_player_from_backups(id, &backup_path, &legacy_backup_path)?
                {
                    eprintln!(
                        "tibia: save parse failed for {}, using backup: {}",
                        path.display(),
                        err
                    );
                    return Ok(Some(fallback));
                }
                return Err(err);
            }
        };
        if let Some(saved_id) = parsed.id {
            if saved_id != id {
                return Err(format!(
                    "player save id mismatch: expected {:?}, got {:?}",
                    id, saved_id
                ));
            }
        }
        Ok(Some(parsed.into_state(id)?))
    }

    pub fn save_player(&self, player: &PlayerState) -> Result<(), String> {
        fs::create_dir_all(self.player_dir()).map_err(|err| {
            format!(
                "player save dir create failed for {}: {}",
                self.player_dir().display(),
                err
            )
        })?;
        let path = self.player_path(player.id);
        let backup_path = self.player_backup_path(player.id);
        let data = PlayerSave::from_state(player).serialize();
        if path.exists() {
            fs::copy(&path, &backup_path).map_err(|err| {
                format!(
                    "player save backup failed for {}: {}",
                    backup_path.display(),
                    err
                )
            })?;
        }
        fs::write(&path, data).map_err(|err| {
            format!("player save write failed for {}: {}", path.display(), err)
        })
    }

    pub fn validate_player_saves(&self) -> SaveValidationReport {
        let player_dir = self.player_dir();
        let entries = match fs::read_dir(&player_dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut report = SaveValidationReport::default();
                report.missing_dir = true;
                return report;
            }
            Err(err) => {
                return SaveValidationReport {
                    errors: vec![format!(
                        "player save dir read failed for {}: {}",
                        player_dir.display(),
                        err
                    )],
                    ..SaveValidationReport::default()
                };
            }
        };

        let mut report = SaveValidationReport::default();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    report.errors.push(format!(
                        "player save dir entry failed for {}: {}",
                        player_dir.display(),
                        err
                    ));
                    continue;
                }
            };
            let path = entry.path();
            let is_save = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("sav"))
                .unwrap_or(false);
            if !is_save {
                report.skipped += 1;
                continue;
            }
            report.player_files += 1;
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                report
                    .errors
                    .push(format!("player save file name missing stem: {}", path.display()));
                continue;
            };
            let id: u32 = match stem.parse() {
                Ok(id) => id,
                Err(_) => {
                    report.errors.push(format!(
                        "player save file name is not numeric: {}",
                        path.display()
                    ));
                    continue;
                }
            };
            let data = match fs::read_to_string(&path) {
                Ok(data) => data,
                Err(err) => {
                    report.errors.push(format!(
                        "player save read failed for {}: {}",
                        path.display(),
                        err
                    ));
                    continue;
                }
            };
            let parsed = match PlayerSave::parse(&data) {
                Ok(parsed) => parsed,
                Err(err) => {
                    report.errors.push(format!(
                        "player save parse failed for {}: {}",
                        path.display(),
                        err
                    ));
                    continue;
                }
            };
            if let Some(saved_id) = parsed.id {
                if saved_id.0 != id {
                    report.errors.push(format!(
                        "player save id mismatch for {}: expected {}, got {}",
                        path.display(),
                        id,
                        saved_id.0
                    ));
                    continue;
                }
            }
            if let Err(err) = parsed.into_state(PlayerId(id)) {
                report.errors.push(format!(
                    "player save state invalid for {}: {}",
                    path.display(),
                    err
                ));
                continue;
            }
            report.parsed += 1;
        }

        report
    }

    fn player_dir(&self) -> PathBuf {
        self.root.join("players")
    }

    fn player_path(&self, id: PlayerId) -> PathBuf {
        self.player_dir().join(format!("{}.sav", id.0))
    }

    fn player_backup_path(&self, id: PlayerId) -> PathBuf {
        self.player_dir().join(format!("{}.sav#", id.0))
    }

    fn player_legacy_backup_path(&self, id: PlayerId) -> PathBuf {
        self.player_dir().join(format!("{}.sav.bak", id.0))
    }

    fn load_player_from_backup(
        &self,
        id: PlayerId,
        backup_path: &Path,
    ) -> Result<Option<PlayerState>, String> {
        let data = match fs::read_to_string(backup_path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(format!(
                    "player backup read failed for {}: {}",
                    backup_path.display(),
                    err
                ))
            }
        };
        let parsed = PlayerSave::parse(&data)?;
        if let Some(saved_id) = parsed.id {
            if saved_id != id {
                return Err(format!(
                    "player save id mismatch: expected {:?}, got {:?}",
                    id, saved_id
                ));
            }
        }
        Ok(Some(parsed.into_state(id)?))
    }

    fn load_player_from_backups(
        &self,
        id: PlayerId,
        primary_backup: &Path,
        legacy_backup: &Path,
    ) -> Result<Option<PlayerState>, String> {
        if let Some(state) = self.load_player_from_backup(id, primary_backup)? {
            return Ok(Some(state));
        }
        self.load_player_from_backup(id, legacy_backup)
    }
}

#[derive(Debug, Default)]
struct PlayerSave {
    version: u32,
    id: Option<PlayerId>,
    name: Option<String>,
    guild_id: Option<u32>,
    guild_name: Option<String>,
    race: Option<u8>,
    level: Option<u16>,
    experience: Option<u64>,
    profession: Option<u8>,
    premium: Option<bool>,
    position: Option<Position>,
    start_position: Option<Position>,
    current_position: Option<Position>,
    direction: Option<Direction>,
    original_outfit: Option<Outfit>,
    current_outfit: Option<Outfit>,
    last_login: Option<u64>,
    last_logout: Option<u64>,
    playerkiller_end: Option<u64>,
    stats: Option<Stats>,
    skills: Option<SkillSet>,
    raw_skills: Vec<SkillRow>,
    inventory: Vec<(InventorySlot, ItemStack)>,
    inventory_containers: HashMap<InventorySlot, Vec<ItemStack>>,
    quest_values: HashMap<u16, i32>,
    depots: HashMap<u16, Vec<ItemStack>>,
    known_spells: HashSet<SpellId>,
    murders: Vec<u64>,
    buddies: Vec<PlayerId>,
}

const KEY_ID: &str = "ID              = ";
const KEY_NAME: &str = "Name            = ";
const KEY_RACE: &str = "Race            = ";
const KEY_PROFESSION: &str = "Profession      = ";
const KEY_GUILD_ID: &str = "GuildId         = ";
const KEY_GUILD_NAME: &str = "GuildName       = ";
const KEY_ORIGINAL_OUTFIT: &str = "OriginalOutfit  = ";
const KEY_CURRENT_OUTFIT: &str = "CurrentOutfit   = ";
const KEY_LAST_LOGIN: &str = "LastLogin       = ";
const KEY_LAST_LOGOUT: &str = "LastLogout      = ";
const KEY_START_POSITION: &str = "StartPosition   = ";
const KEY_CURRENT_POSITION: &str = "CurrentPosition = ";
const KEY_PLAYERKILLER_END: &str = "PlayerkillerEnd = ";
const KEY_SKILL: &str = "Skill = (";
const KEY_SPELLS: &str = "Spells      = {";
const KEY_QUEST_VALUES: &str = "QuestValues = {";
const KEY_MURDERS: &str = "Murders     = {";
const KEY_BUDDIES: &str = "Buddies     = {";
const KEY_INVENTORY: &str = "Inventory   = {";
const KEY_DEPOTS: &str = "Depots      = {";
const KEY_CONTENT: &str = " Content=";
const MAX_SKILL_ID: u32 = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaveSection {
    None,
    Inventory,
    Containers,
    Depots,
}

impl PlayerSave {
    fn from_state(player: &PlayerState) -> Self {
        let mut inventory = Vec::new();
        for index in 0..10 {
            let Some(slot) = InventorySlot::from_index(index) else {
                continue;
            };
            if let Some(item) = player.inventory.slot(slot) {
                inventory.push((slot, item.clone()));
            }
        }
        let raw_skills = skill_rows_from_player(player);
        let mut buddies: Vec<PlayerId> = player.buddies.iter().copied().collect();
        buddies.sort_by_key(|id| id.0);
        Self {
            version: 1,
            id: Some(player.id),
            name: Some(player.name.clone()),
            guild_id: player.guild_id,
            guild_name: player.guild_name.clone(),
            race: Some(player.race),
            level: Some(player.level),
            experience: Some(player.experience),
            profession: Some(player.profession),
            premium: Some(player.premium),
            position: Some(player.position),
            start_position: Some(player.start_position),
            current_position: Some(player.position),
            direction: Some(player.direction),
            original_outfit: Some(player.original_outfit),
            current_outfit: Some(player.current_outfit),
            last_login: Some(player.last_login),
            last_logout: Some(player.last_logout),
            playerkiller_end: Some(player.playerkiller_end),
            stats: Some(player.stats),
            skills: Some(player.skills.clone()),
            raw_skills,
            inventory,
            inventory_containers: player.inventory_containers.clone(),
            quest_values: player.quest_values.clone(),
            depots: player.depots.clone(),
            known_spells: player.known_spells.clone(),
            murders: player.murders.clone(),
            buddies,
        }
    }

    fn skill_rows_for_save(&self) -> Vec<SkillRow> {
        if !self.raw_skills.is_empty() {
            return filter_and_sort_skill_rows(self.raw_skills.clone());
        }
        if let Some(skills) = self.skills.as_ref() {
            return filter_and_sort_skill_rows(skill_rows_from_skillset(skills));
        }
        Vec::new()
    }

    fn serialize(&self) -> String {
        self.serialize_original()
    }

    fn serialize_original(&self) -> String {
        let mut lines = Vec::new();
        lines.push("# tibia player save v1".to_string());
        let id = self.id.unwrap_or(PlayerId(0));
        let name = self.name.as_deref().unwrap_or("");
        let race = self.race.unwrap_or(0);
        let profession = self.profession.unwrap_or(0);
        let original_outfit = self.original_outfit.unwrap_or_default();
        let current_outfit = self.current_outfit.unwrap_or_default();
        let last_login = self.last_login.unwrap_or(0);
        let last_logout = self.last_logout.unwrap_or(0);
        let playerkiller_end = self.playerkiller_end.unwrap_or(0);
        let start_position = self
            .start_position
            .or(self.position)
            .or(self.current_position)
            .unwrap_or(Position { x: 0, y: 0, z: 0 });
        let current_position = self
            .current_position
            .or(self.position)
            .unwrap_or(Position { x: 0, y: 0, z: 0 });

        lines.push(format!("{}{}", KEY_ID, id.0));
        lines.push(format!("{}{}", KEY_NAME, escape_string(name)));
        lines.push(format!("{}{}", KEY_RACE, race));
        lines.push(format!("{}{}", KEY_PROFESSION, profession));
        if let Some(guild_id) = self.guild_id {
            if guild_id > 0 {
                lines.push(format!("{}{}", KEY_GUILD_ID, guild_id));
            }
        }
        if let Some(guild_name) = self.guild_name.as_ref() {
            let trimmed = guild_name.trim();
            if !trimmed.is_empty() {
                lines.push(format!("{}{}", KEY_GUILD_NAME, escape_string(trimmed)));
            }
        }
        lines.push(format!(
            "{}{}",
            KEY_ORIGINAL_OUTFIT,
            format_outfit(original_outfit)
        ));
        lines.push(format!(
            "{}{}",
            KEY_CURRENT_OUTFIT,
            format_outfit(current_outfit)
        ));
        lines.push(format!("{}{}", KEY_LAST_LOGIN, last_login));
        lines.push(format!("{}{}", KEY_LAST_LOGOUT, last_logout));
        lines.push(format!(
            "{}{},{},{}",
            KEY_START_POSITION, start_position.x, start_position.y, start_position.z
        ));
        lines.push(format!(
            "{}{},{},{}",
            KEY_CURRENT_POSITION,
            current_position.x,
            current_position.y,
            current_position.z
        ));
        lines.push(format!("{}{}", KEY_PLAYERKILLER_END, playerkiller_end));
        lines.push(String::new());

        let skill_rows = self.skill_rows_for_save();
        for row in skill_rows {
            let mut values = Vec::with_capacity(1 + RAW_SKILL_FIELDS);
            values.push(row.skill_id.to_string());
            values.extend(row.values.iter().map(|value| value.to_string()));
            lines.push(format!("{}{})", KEY_SKILL, values.join(",")));
        }
        lines.push(String::new());

        let mut spell_ids: Vec<_> = self.known_spells.iter().copied().collect();
        spell_ids.sort_by_key(|spell| spell.0);
        lines.push(format!(
            "{}{}{}",
            KEY_SPELLS,
            spell_ids
                .iter()
                .map(|spell| spell.0.to_string())
                .collect::<Vec<_>>()
                .join(","),
            "}"
        ));

        let mut quest_keys: Vec<_> = self.quest_values.keys().copied().collect();
        quest_keys.sort_unstable();
        let quest_pairs = quest_keys
            .into_iter()
            .filter_map(|key| self.quest_values.get(&key).map(|value| (key, *value)))
            .map(|(key, value)| format!("({},{})", key, value))
            .collect::<Vec<_>>()
            .join(",");
        lines.push(format!("{}{}{}", KEY_QUEST_VALUES, quest_pairs, "}"));

        let murder_list = self
            .murders
            .iter()
            .map(|entry| entry.to_string())
            .collect::<Vec<_>>()
            .join(",");
        lines.push(format!("{}{}{}", KEY_MURDERS, murder_list, "}"));

        let mut buddy_ids = self.buddies.clone();
        buddy_ids.sort_by_key(|id| id.0);
        let buddy_list = buddy_ids
            .iter()
            .map(|entry| entry.0.to_string())
            .collect::<Vec<_>>()
            .join(",");
        lines.push(format!("{}{}{}", KEY_BUDDIES, buddy_list, "}"));
        lines.push(String::new());

        lines.push(KEY_INVENTORY.to_string());
        let mut inventory_slots: Vec<_> = self.inventory.iter().collect();
        inventory_slots.sort_by_key(|(slot, _)| slot_index(*slot));
        for (slot, item) in inventory_slots {
            let contents = self.inventory_containers.get(slot).map(|items| items.as_slice());
            let entry = format_item_entry(item, contents);
            lines.push(format!(
                "{}{}{{{}}}",
                slot_index(*slot),
                KEY_CONTENT,
                entry
            ));
        }
        lines.push("}".to_string());
        lines.push(String::new());

        lines.push(KEY_DEPOTS.to_string());
        let mut depot_ids: Vec<_> = self.depots.keys().copied().collect();
        depot_ids.sort_unstable();
        for depot_id in depot_ids {
            if let Some(items) = self.depots.get(&depot_id) {
                if items.is_empty() {
                    continue;
                }
                let content = format_content_items(items);
                lines.push(format!("{}{}{}", depot_id, KEY_CONTENT, content));
            }
        }
        lines.push("}".to_string());
        lines.push(String::new());
        lines.push("# legacy".to_string());
        if let Some(premium) = self.premium {
            lines.push(format!("# premium={}", if premium { 1 } else { 0 }));
        }
        if let Some(direction) = self.direction {
            lines.push(format!("# direction={}", direction_to_str(direction)));
        }
        lines.join("\n")
    }

    fn parse(data: &str) -> Result<Self, String> {
        if data
            .lines()
            .any(|line| line.trim_start().starts_with("version="))
        {
            return Self::parse_legacy(data);
        }
        Self::parse_original(data)
    }

    fn parse_original(data: &str) -> Result<Self, String> {
        let mut save = PlayerSave::default();
        let mut section = SaveSection::None;
        for (line_number, line) in data.lines().enumerate() {
            let raw_line = line.trim_end();
            let trimmed = raw_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
            if let Some(line) = trimmed.strip_prefix('#').map(|line| line.trim_start()) {
                if let Some((key, value)) = line.split_once('=') {
                    match key {
                        "premium" => save.premium = Some(parse_bool(value, "premium")?),
                        "direction" => save.direction = Some(parse_direction(value)?),
                        "stats" => {
                            let parts = parse_u32_list(value, "stats")?;
                            if parts.len() != 6 {
                                return Err("stats expects 6 entries".to_string());
                            }
                            let resistances = save
                                .stats
                                .as_ref()
                                .map(|stats| stats.resistances)
                                .unwrap_or_else(DamageResistances::default);
                            let stats = Stats {
                                health: parts[0],
                                max_health: parts[1],
                                mana: parts[2],
                                max_mana: parts[3],
                                soul: parts[4],
                                capacity: parts[5],
                                resistances,
                            };
                            save.stats = Some(stats);
                        }
                        _ => {}
                    }
                }
            }
            continue;
        }
            match section {
                SaveSection::Inventory => {
                    if trimmed == "}" {
                        section = SaveSection::None;
                        continue;
                    }
                    if let Some(value) = trimmed.strip_prefix("Content=") {
                        save_parse_inventory_line(value, &mut save)?;
                        continue;
                    }
                    if let Some((slot_index, content)) = parse_indexed_content_line(trimmed) {
                        let items = parse_braced_content_items(content)?;
                        let slot_index = usize::try_from(slot_index)
                            .map_err(|_| "inventory slot out of range".to_string())?;
                        let slot = InventorySlot::from_index(slot_index)
                            .ok_or_else(|| "inventory slot out of range".to_string())?;
                        if let Some(item) = items.first() {
                            save.inventory.push((slot, content_item_to_stack(item)));
                            if !item.contents.is_empty() {
                                save.inventory_containers
                                    .insert(slot, content_items_to_stacks(&item.contents));
                            }
                        }
                        continue;
                    }
                }
                SaveSection::Containers => {
                    if trimmed == "}" {
                        section = SaveSection::None;
                        continue;
                    }
                    if let Some(value) = trimmed.strip_prefix("Content=") {
                        save_parse_container_line(value, &mut save)?;
                        continue;
                    }
                }
                SaveSection::Depots => {
                    if trimmed == "}" {
                        section = SaveSection::None;
                        continue;
                    }
                    if let Some(value) = trimmed.strip_prefix("Content=") {
                        save_parse_depot_line(value, &mut save)?;
                        continue;
                    }
                    if let Some((depot_id, content)) = parse_indexed_content_line(trimmed) {
                        let items = parse_braced_content_items(content)?;
                        if !items.is_empty() {
                            let depot_id = to_u16(u32::from(depot_id), "depot id")?;
                            save.depots
                                .insert(depot_id, content_items_to_stacks(&items));
                        }
                        continue;
                    }
                }
                SaveSection::None => {}
            }

            if let Some((key, value)) = parse_assignment_line(raw_line, line_number + 1)? {
                match key.as_str() {
                    "id" => {
                        let id = parse_u32(value, "ID")?;
                        save.id = Some(PlayerId(id));
                    }
                    "name" => {
                        save.name = Some(parse_string_value(value, "Name")?);
                    }
                    "race" => {
                        save.race = Some(parse_u8(value, "Race")?);
                    }
                    "profession" => {
                        save.profession = Some(parse_u8(value, "Profession")?);
                    }
                    "guildid" => {
                        save.guild_id = Some(parse_u32(value, "GuildId")?);
                    }
                    "guildname" => {
                        save.guild_name = Some(parse_string_value(value, "GuildName")?);
                    }
                    "originaloutfit" => {
                        save.original_outfit = Some(parse_outfit(value)?);
                    }
                    "currentoutfit" => {
                        save.current_outfit = Some(parse_outfit(value)?);
                    }
                    "lastlogin" => {
                        save.last_login = Some(parse_u64(value, "LastLogin")?);
                    }
                    "lastlogout" => {
                        save.last_logout = Some(parse_u64(value, "LastLogout")?);
                    }
                    "startposition" => {
                        save.start_position = Some(parse_position(value)?);
                    }
                    "currentposition" => {
                        save.current_position = Some(parse_position(value)?);
                    }
                    "playerkillerend" => {
                        save.playerkiller_end = Some(parse_u64(value, "PlayerkillerEnd")?);
                    }
                    "skill" => {
                        save.raw_skills.push(parse_skill_row(value, line_number + 1)?);
                    }
                    "spells" => {
                        for id in parse_braced_u16_list(value, "Spells")? {
                            save.known_spells.insert(SpellId(id));
                        }
                    }
                    "questvalues" => {
                        for (key, value) in parse_braced_pairs(value, "QuestValues")? {
                            save.quest_values.insert(key, value);
                        }
                    }
                    "murders" => {
                        save.murders = parse_braced_u64_list(value, "Murders")?;
                    }
                    "buddies" => {
                        let ids = parse_braced_u32_list(value, "Buddies")?;
                        save.buddies = ids.into_iter().map(PlayerId).collect();
                    }
                    "inventory" => {
                        if value.trim() != "{" {
                            return Err(format!("save line {} invalid inventory header", line_number + 1));
                        }
                        section = SaveSection::Inventory;
                    }
                    "containers" => {
                        if value.trim() != "{" {
                            return Err(format!("save line {} invalid containers header", line_number + 1));
                        }
                        section = SaveSection::Containers;
                    }
                    "depots" => {
                        if value.trim() != "{" {
                            return Err(format!("save line {} invalid depots header", line_number + 1));
                        }
                        section = SaveSection::Depots;
                    }
                    other => {
                        return Err(format!("save line {} unexpected identifier '{}'", line_number + 1, other));
                    }
                }
                continue;
            }
            return Err(format!(
                "save line {} unexpected content '{}'",
                line_number + 1,
                trimmed
            ));
        }
        Ok(save)
    }

    fn parse_legacy(data: &str) -> Result<Self, String> {
        let mut save = PlayerSave::default();
        for (line_number, line) in data.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("save line {} missing '='", line_number + 1))?;
            match key {
                "version" => {
                    save.version = parse_u32(value, "version")?;
                }
                "id" => {
                    let id = parse_u32(value, "id")?;
                    save.id = Some(PlayerId(id));
                }
                "name" => {
                    save.name = Some(unescape_string(value)?);
                }
                "level" => {
                    save.level = Some(parse_u16(value, "level")?);
                }
                "experience" => {
                    save.experience = Some(parse_u64(value, "experience")?);
                }
                "profession" => {
                    save.profession = Some(parse_u8(value, "profession")?);
                }
                "premium" => {
                    save.premium = Some(parse_bool(value, "premium")?);
                }
                "position" => {
                    let parts = parse_u32_list(value, "position")?;
                    if parts.len() != 3 {
                        return Err("position expects 3 entries".to_string());
                    }
                    let x = u16::try_from(parts[0]).map_err(|_| "position x out of range".to_string())?;
                    let y = u16::try_from(parts[1]).map_err(|_| "position y out of range".to_string())?;
                    let z = u8::try_from(parts[2]).map_err(|_| "position z out of range".to_string())?;
                    save.position = Some(Position { x, y, z });
                }
                "direction" => {
                    save.direction = Some(parse_direction(value)?);
                }
                "stats" => {
                    let parts = parse_u32_list(value, "stats")?;
                    if parts.len() != 6 {
                        return Err("stats expects 6 entries".to_string());
                    }
                    let resistances = save
                        .stats
                        .as_ref()
                        .map(|stats| stats.resistances)
                        .unwrap_or_else(DamageResistances::default);
                    let stats = Stats {
                        health: parts[0],
                        max_health: parts[1],
                        mana: parts[2],
                        max_mana: parts[3],
                        soul: parts[4],
                        capacity: parts[5],
                        resistances,
                    };
                    save.stats = Some(stats);
                }
                "inventory" => {
                    let parts = parse_u32_list(value, "inventory")?;
                    if parts.len() != 3 {
                        return Err("inventory expects 3 entries".to_string());
                    }
                    let slot_index = usize::try_from(parts[0])
                        .map_err(|_| "inventory slot out of range".to_string())?;
                    let slot = InventorySlot::from_index(slot_index)
                        .ok_or_else(|| "inventory slot out of range".to_string())?;
                    let item = ItemStack { id: crate::entities::item::ItemId::next(),
                        type_id: ItemTypeId(to_u16(parts[1], "inventory type")?),
                        count: to_u16(parts[2], "inventory count")?,
                        attributes: Vec::new(),
                        contents: Vec::new(),
                    };
                    save.inventory.push((slot, item));
                }
                "quest" => {
                    let parts = parse_i32_list(value, "quest")?;
                    if parts.len() != 2 {
                        return Err("quest expects 2 entries".to_string());
                    }
                    if parts[0] < 0 || parts[0] > i32::from(u16::MAX) {
                        return Err("quest id out of range".to_string());
                    }
                    save.quest_values
                        .insert(parts[0] as u16, parts[1] as i32);
                }
                "depot" => {
                    let parts = parse_u32_list(value, "depot")?;
                    if parts.len() != 3 {
                        return Err("depot expects 3 entries".to_string());
                    }
                    let depot_id = to_u16(parts[0], "depot id")?;
                    let entry = save
                        .depots
                        .entry(depot_id)
                        .or_insert_with(Vec::new);
                    entry.push(ItemStack { id: crate::entities::item::ItemId::next(),
                        type_id: ItemTypeId(to_u16(parts[1], "depot type")?),
                        count: to_u16(parts[2], "depot count")?,
                        attributes: Vec::new(),
                        contents: Vec::new(),
                    });
                }
                "spell" => {
                    let id = parse_u16(value, "spell")?;
                    save.known_spells.insert(SpellId(id));
                }
                _ => {}
            }
        }
        Ok(save)
    }

    fn into_state(self, id: PlayerId) -> Result<PlayerState, String> {
        let name = self.name.ok_or_else(|| "save missing name".to_string())?;
        let position = self
            .current_position
            .or(self.position)
            .or(self.start_position)
            .ok_or_else(|| "save missing position".to_string())?;
        let mut player = PlayerState::new(id, name, position);
        if let Some(level) = self.level {
            player.level = level;
        }
        if let Some(experience) = self.experience {
            player.experience = experience;
        }
        if let Some(profession) = self.profession {
            player.profession = profession;
        }
        if let Some(race) = self.race {
            player.race = race;
        }
        if let Some(guild_id) = self.guild_id {
            if guild_id > 0 {
                player.guild_id = Some(guild_id);
            }
        }
        if let Some(guild_name) = self.guild_name {
            let trimmed = guild_name.trim();
            if !trimmed.is_empty() {
                player.guild_name = Some(trimmed.to_string());
            }
        }
        if let Some(premium) = self.premium {
            player.premium = premium;
        }
        if let Some(direction) = self.direction {
            player.direction = direction;
        }
        if let Some(stats) = self.stats {
            player.stats = stats;
        }
        if !self.raw_skills.is_empty() {
            player.skills = skillset_from_rows(&self.raw_skills);
        } else if let Some(skills) = self.skills {
            player.skills = skills;
        }
        if let Some((level, experience)) = level_and_experience_from_rows(&self.raw_skills) {
            player.level = level;
            player.experience = experience;
        }
        let applied_stat_skills = apply_stat_skills_from_rows(&self.raw_skills, &mut player);
        if !applied_stat_skills && self.stats.is_none() {
            player.stats = stats_from_level_and_profession(player.level, player.profession);
        }
        if player.stats.health == 0 && player.stats.max_health == 0 {
            let defaults = Stats::default();
            player.stats.health = defaults.health;
            player.stats.max_health = defaults.max_health;
        } else if player.stats.health == 0 && player.stats.max_health > 0 {
            player.stats.health = player.stats.max_health;
        }
        if player.stats.max_mana == 0 && player.stats.mana > 0 {
            player.stats.max_mana = player.stats.mana;
        }
        if player.stats.mana > player.stats.max_mana {
            player.stats.mana = player.stats.max_mana;
        }
        if let Some(start_position) = self.start_position {
            player.start_position = start_position;
        }
        if let Some(original_outfit) = self.original_outfit {
            player.original_outfit = original_outfit;
        }
        if let Some(current_outfit) = self.current_outfit {
            player.current_outfit = current_outfit;
        }
        if let Some(last_login) = self.last_login {
            player.last_login = last_login;
        }
        if let Some(last_logout) = self.last_logout {
            player.last_logout = last_logout;
        }
        if let Some(playerkiller_end) = self.playerkiller_end {
            player.playerkiller_end = playerkiller_end;
        }
        player.raw_skills = self.raw_skills;
        player.murders = self.murders;

        let mut inventory = Inventory::default();
        for (slot, item) in self.inventory {
            inventory.set_slot(slot, Some(item));
        }
        player.inventory = inventory;
        player.inventory_containers = self.inventory_containers;
        player.quest_values = self.quest_values;
        player.depots = self.depots;
        player.known_spells = self.known_spells;
        player.buddies = self.buddies.into_iter().collect();
        Ok(player)
    }
}

fn escape_string(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn unescape_string(input: &str) -> Result<String, String> {
    let mut output = String::new();
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            return Err("string escape sequence truncated".to_string());
        };
        match next {
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            '\\' => output.push('\\'),
            other => {
                output.push(other);
            }
        }
    }
    Ok(output)
}

fn parse_u8(value: &str, label: &str) -> Result<u8, String> {
    value
        .trim()
        .parse::<u8>()
        .map_err(|_| format!("{} parse failed", label))
}

fn parse_u16(value: &str, label: &str) -> Result<u16, String> {
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("{} parse failed", label))
}

fn parse_u32(value: &str, label: &str) -> Result<u32, String> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("{} parse failed", label))
}

fn parse_u64(value: &str, label: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{} parse failed", label))
}

fn parse_i32(value: &str, label: &str) -> Result<i32, String> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| format!("{} parse failed", label))
}

fn parse_string_value(value: &str, label: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.starts_with('"') {
        let (parsed, end) = parse_quoted_string_literal(trimmed, 0)?;
        if !trimmed[end..].trim().is_empty() {
            return Err(format!("{} trailing characters", label));
        }
        return Ok(parsed);
    }
    unescape_string(trimmed)
}

fn to_u16(value: u32, label: &str) -> Result<u16, String> {
    u16::try_from(value).map_err(|_| format!("{} out of range", label))
}

fn to_u8(value: u32, label: &str) -> Result<u8, String> {
    u8::try_from(value).map_err(|_| format!("{} out of range", label))
}

fn parse_bool(value: &str, label: &str) -> Result<bool, String> {
    match value.trim() {
        "1" => Ok(true),
        "0" => Ok(false),
        _ => Err(format!("{} parse failed", label)),
    }
}

fn parse_assignment_line(
    line: &str,
    line_number: usize,
) -> Result<Option<(String, &str)>, String> {
    let mut idx = skip_ws(line, 0);
    if idx >= line.len() {
        return Ok(None);
    }
    let bytes = line.as_bytes();
    if !is_key_char(bytes[idx]) {
        return Ok(None);
    }
    let start = idx;
    while idx < line.len() && is_key_char(bytes[idx]) {
        idx += 1;
    }
    let key = line[start..idx].to_ascii_lowercase();
    idx = skip_ws(line, idx);
    if idx >= line.len() || bytes[idx] != b'=' {
        return Err(format!("save line {} missing '='", line_number));
    }
    idx += 1;
    Ok(Some((key, &line[idx..])))
}

fn parse_u32_list(value: &str, label: &str) -> Result<Vec<u32>, String> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|entry| {
            entry
                .trim()
                .parse::<u32>()
                .map_err(|_| format!("{} parse failed", label))
        })
        .collect()
}

fn parse_i32_list(value: &str, label: &str) -> Result<Vec<i32>, String> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|entry| {
            entry
                .trim()
                .parse::<i32>()
                .map_err(|_| format!("{} parse failed", label))
        })
        .collect()
}

fn parse_direction(value: &str) -> Result<Direction, String> {
    let trimmed = value.trim();
    let lowered = trimmed.to_ascii_lowercase();
    match lowered.as_str() {
        "north" => Ok(Direction::North),
        "east" => Ok(Direction::East),
        "south" => Ok(Direction::South),
        "west" => Ok(Direction::West),
        "northeast" => Ok(Direction::Northeast),
        "northwest" => Ok(Direction::Northwest),
        "southeast" => Ok(Direction::Southeast),
        "southwest" => Ok(Direction::Southwest),
        _ => Err("direction parse failed".to_string()),
    }
}

fn direction_to_str(direction: Direction) -> &'static str {
    match direction {
        Direction::North => "North",
        Direction::East => "East",
        Direction::South => "South",
        Direction::West => "West",
        Direction::Northeast => "Northeast",
        Direction::Northwest => "Northwest",
        Direction::Southeast => "Southeast",
        Direction::Southwest => "Southwest",
    }
}

fn slot_index(slot: InventorySlot) -> usize {
    match slot {
        InventorySlot::Head => 0,
        InventorySlot::Necklace => 1,
        InventorySlot::Backpack => 2,
        InventorySlot::Armor => 3,
        InventorySlot::RightHand => 4,
        InventorySlot::LeftHand => 5,
        InventorySlot::Legs => 6,
        InventorySlot::Feet => 7,
        InventorySlot::Ring => 8,
        InventorySlot::Ammo => 9,
    }
}

fn format_outfit(outfit: Outfit) -> String {
    format!(
        "({},{},{},{},{},{},{})",
        outfit.look_type,
        outfit.head,
        outfit.body,
        outfit.legs,
        outfit.feet,
        outfit.addons,
        outfit.look_item
    )
}

fn parse_outfit(value: &str) -> Result<Outfit, String> {
    let trimmed = value.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(trimmed);
    let parts = parse_u32_list(inner, "outfit")?;
    if parts.len() != 6 && parts.len() != 7 {
        return Err("outfit expects 6 or 7 entries".to_string());
    }
    Ok(Outfit {
        look_type: to_u16(parts[0], "outfit look_type")?,
        head: to_u8(parts[1], "outfit head")?,
        body: to_u8(parts[2], "outfit body")?,
        legs: to_u8(parts[3], "outfit legs")?,
        feet: to_u8(parts[4], "outfit feet")?,
        addons: to_u8(parts[5], "outfit addons")?,
        look_item: if parts.len() == 7 {
            to_u16(parts[6], "outfit look_item")?
        } else {
            0
        },
    })
}

fn parse_position(value: &str) -> Result<Position, String> {
    let trimmed = value.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(trimmed);
    let parts = parse_u32_list(inner, "position")?;
    if parts.len() != 3 {
        return Err("position expects 3 entries".to_string());
    }
    let x = u16::try_from(parts[0]).map_err(|_| "position x out of range".to_string())?;
    let y = u16::try_from(parts[1]).map_err(|_| "position y out of range".to_string())?;
    let z = u8::try_from(parts[2]).map_err(|_| "position z out of range".to_string())?;
    Ok(Position { x, y, z })
}

fn parse_braced_u16_list(value: &str, label: &str) -> Result<Vec<u16>, String> {
    let inner = value
        .trim()
        .strip_suffix('}')
        .ok_or_else(|| format!("{} missing '}}'", label))?
        .trim();
    let inner = inner.strip_prefix('{').unwrap_or(inner).trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|entry| parse_u16(entry, label))
        .collect()
}

fn parse_braced_u64_list(value: &str, label: &str) -> Result<Vec<u64>, String> {
    let inner = value
        .trim()
        .strip_suffix('}')
        .ok_or_else(|| format!("{} missing '}}'", label))?
        .trim();
    let inner = inner.strip_prefix('{').unwrap_or(inner).trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|entry| parse_u64(entry, label))
        .collect()
}

fn parse_braced_u32_list(value: &str, label: &str) -> Result<Vec<u32>, String> {
    let inner = value
        .trim()
        .strip_suffix('}')
        .ok_or_else(|| format!("{} missing '}}'", label))?
        .trim();
    let inner = inner.strip_prefix('{').unwrap_or(inner).trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|entry| parse_u32(entry, label))
        .collect()
}

fn parse_braced_pairs(value: &str, label: &str) -> Result<Vec<(u16, i32)>, String> {
    let inner = value
        .trim()
        .strip_suffix('}')
        .ok_or_else(|| format!("{} missing '}}'", label))?;
    let inner = inner.trim();
    let inner = inner.strip_prefix('{').unwrap_or(inner).trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    let entries = inner.split("),(");
    let mut pairs = Vec::new();
    for entry in entries {
        let entry = entry.trim().trim_start_matches('(').trim_end_matches(')');
        let parts: Vec<_> = entry.split(',').collect();
        if parts.len() != 2 {
            return Err(format!("{} entry expects 2 values", label));
        }
        let key = parse_u16(parts[0], label)?;
        let value = parse_i32(parts[1], label)?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

fn parse_skill_row(value: &str, line_number: usize) -> Result<SkillRow, String> {
    let trimmed = value.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| format!("save line {} malformed skill row", line_number))?;
    let parts = parse_i32_list(inner, "Skill")?;
    if parts.len() != 1 + RAW_SKILL_FIELDS {
        return Err(format!(
            "save line {} expects {} skill values",
            line_number,
            1 + RAW_SKILL_FIELDS
        ));
    }
    let skill_id = u32::try_from(parts[0]).map_err(|_| "skill id out of range".to_string())?;
    if skill_id > MAX_SKILL_ID {
        return Err("skill id out of range".to_string());
    }
    let mut values = [0i32; RAW_SKILL_FIELDS];
    for (index, slot) in values.iter_mut().enumerate() {
        *slot = parts[index + 1];
    }
    Ok(SkillRow::new(skill_id, values))
}

fn save_parse_inventory_line(value: &str, save: &mut PlayerSave) -> Result<(), String> {
    let parts = parse_u32_list(value, "inventory")?;
    if parts.len() != 3 {
        return Err("inventory expects 3 entries".to_string());
    }
    let slot_index =
        usize::try_from(parts[0]).map_err(|_| "inventory slot out of range".to_string())?;
    let slot = InventorySlot::from_index(slot_index)
        .ok_or_else(|| "inventory slot out of range".to_string())?;
    let item = ItemStack { id: crate::entities::item::ItemId::next(),
        type_id: ItemTypeId(to_u16(parts[1], "inventory type")?),
        count: to_u16(parts[2], "inventory count")?,
        attributes: Vec::new(),
        contents: Vec::new(),
    };
    save.inventory.push((slot, item));
    Ok(())
}

fn save_parse_container_line(value: &str, save: &mut PlayerSave) -> Result<(), String> {
    let parts = parse_u32_list(value, "container")?;
    if parts.len() != 3 {
        return Err("container expects 3 entries".to_string());
    }
    let slot_index =
        usize::try_from(parts[0]).map_err(|_| "container slot out of range".to_string())?;
    let slot = InventorySlot::from_index(slot_index)
        .ok_or_else(|| "container slot out of range".to_string())?;
    let item = ItemStack { id: crate::entities::item::ItemId::next(),
        type_id: ItemTypeId(to_u16(parts[1], "container type")?),
        count: to_u16(parts[2], "container count")?,
        attributes: Vec::new(),
        contents: Vec::new(),
    };
    save.inventory_containers
        .entry(slot)
        .or_insert_with(Vec::new)
        .push(item);
    Ok(())
}

fn save_parse_depot_line(value: &str, save: &mut PlayerSave) -> Result<(), String> {
    let parts = parse_u32_list(value, "depot")?;
    if parts.len() != 3 {
        return Err("depot expects 3 entries".to_string());
    }
    let depot_id = to_u16(parts[0], "depot id")?;
    let entry = save.depots.entry(depot_id).or_insert_with(Vec::new);
    entry.push(ItemStack { id: crate::entities::item::ItemId::next(),
        type_id: ItemTypeId(to_u16(parts[1], "depot type")?),
        count: to_u16(parts[2], "depot count")?,
        attributes: Vec::new(),
        contents: Vec::new(),
    });
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedContentItem {
    type_id: ItemTypeId,
    count: u16,
    attributes: Vec<ItemAttribute>,
    contents: Vec<ParsedContentItem>,
}

fn format_item_entry(item: &ItemStack, contents: Option<&[ItemStack]>) -> String {
    let mut parts = Vec::new();
    parts.push(item.type_id.0.to_string());
    if item.count != 1 {
        parts.push(format!("Amount={}", item.count));
    }
    parts.extend(format_item_attributes(&item.attributes));
    let contents = match contents {
        Some(items) => Some(items),
        None => {
            if item.contents.is_empty() {
                None
            } else {
                Some(item.contents.as_slice())
            }
        }
    };
    if let Some(contents) = contents {
        if !contents.is_empty() {
            parts.push(format!("Content={}", format_content_items(contents)));
        }
    }
    parts.join(" ")
}

fn format_content_items(items: &[ItemStack]) -> String {
    let entries = items
        .iter()
        .map(|item| format_item_entry(item, None))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{}}}", entries)
}

fn format_item_attributes(attributes: &[ItemAttribute]) -> Vec<String> {
    let mut parts = Vec::new();
    for attribute in attributes {
        match attribute {
            ItemAttribute::String(text) => {
                parts.push(format!("String={}", format_attribute_value(text)));
            }
            ItemAttribute::DynamicString(text) => {
                parts.push(format!("String={}", format_attribute_value(text)));
            }
            ItemAttribute::ChestQuestNumber(value) => {
                parts.push(format!("ChestQuestNumber={value}"));
            }
            ItemAttribute::ContainerLiquidType(value) => {
                parts.push(format!("ContainerLiquidType={value}"));
            }
            ItemAttribute::PoolLiquidType(value) => {
                parts.push(format!("PoolLiquidType={value}"));
            }
            ItemAttribute::RemainingExpireTime(value) => {
                parts.push(format!("RemainingExpireTime={value}"));
            }
            ItemAttribute::KeyholeNumber(value) => {
                parts.push(format!("KeyholeNumber={value}"));
            }
            ItemAttribute::DoorQuestNumber(value) => {
                parts.push(format!("DoorQuestNumber={value}"));
            }
            ItemAttribute::DoorQuestValue(value) => {
                parts.push(format!("DoorQuestValue={value}"));
            }
            ItemAttribute::Level(value) => {
                parts.push(format!("Level={value}"));
            }
            ItemAttribute::RemainingUses(value) => {
                parts.push(format!("RemainingUses={value}"));
            }
            ItemAttribute::KeyNumber(value) => {
                parts.push(format!("KeyNumber={value}"));
            }
            ItemAttribute::SavedExpireTime(value) => {
                parts.push(format!("SavedExpireTime={value}"));
            }
            ItemAttribute::AbsTeleportDestination(value) => {
                parts.push(format!("AbsTeleportDestination={value}"));
            }
            ItemAttribute::Responsible(value) => {
                parts.push(format!("Responsible={value}"));
            }
            ItemAttribute::Amount(_) | ItemAttribute::Charges(_) => {}
            ItemAttribute::Unknown { key, value } => {
                parts.push(format!("{key}={}", format_attribute_value(value)));
            }
        }
    }
    parts
}

fn format_attribute_value(value: &str) -> String {
    format!("\"{}\"", escape_string(value))
}

fn parse_indexed_content_line(line: &str) -> Option<(u16, &str)> {
    let content_idx = line.find("Content=")?;
    let (prefix, rest) = line.split_at(content_idx);
    let prefix = prefix.trim();
    if prefix.is_empty() || !prefix.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let index = prefix.parse::<u16>().ok()?;
    let value = rest["Content=".len()..].trim();
    Some((index, value))
}

fn parse_braced_content_items(value: &str) -> Result<Vec<ParsedContentItem>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if !trimmed.starts_with('{') {
        return Err("Content missing '{'".to_string());
    }
    let end = find_matching_brace(trimmed, 0)?;
    let inner = trimmed[1..end].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    parse_content_items(inner)
}

fn parse_content_items(content: &str) -> Result<Vec<ParsedContentItem>, String> {
    let mut items = Vec::new();
    for entry in split_top_level_items(content) {
        if entry.trim().is_empty() {
            continue;
        }
        items.push(parse_content_item_entry(entry)?);
    }
    Ok(items)
}

fn parse_content_item_entry(entry: &str) -> Result<ParsedContentItem, String> {
    let mut idx = 0usize;
    let bytes = entry.as_bytes();
    idx = skip_ws(entry, idx);
    let start = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if start == idx {
        return Err("missing item id".to_string());
    }
    let type_id = entry[start..idx]
        .parse::<u16>()
        .map_err(|_| "invalid item id".to_string())?;

    let mut item = ParsedContentItem {
        type_id: ItemTypeId(type_id),
        count: 1,
        attributes: Vec::new(),
        contents: Vec::new(),
    };

    while idx < bytes.len() {
        idx = skip_ws(entry, idx);
        if idx >= bytes.len() {
            break;
        }

        if entry[idx..].starts_with("Content") {
            idx += "Content".len();
            idx = skip_ws(entry, idx);
            if idx < bytes.len() && bytes[idx] == b'=' {
                idx += 1;
            }
            idx = skip_ws(entry, idx);
            if idx >= bytes.len() || bytes[idx] != b'{' {
                return Err("Content missing '{'".to_string());
            }
            let end = find_matching_brace(entry, idx)?;
            let inner = entry[idx + 1..end].trim();
            if !inner.is_empty() {
                item.contents = parse_content_items(inner)?;
            }
            idx = end + 1;
            continue;
        }

        let key_start = idx;
        while idx < bytes.len() && is_key_char(bytes[idx]) {
            idx += 1;
        }
        if key_start == idx {
            idx += 1;
            continue;
        }
        let key = entry[key_start..idx].trim();
        idx = skip_ws(entry, idx);
        if idx >= bytes.len() || bytes[idx] != b'=' {
            continue;
        }
        idx += 1;
        idx = skip_ws(entry, idx);
        if idx >= bytes.len() {
            break;
        }

        let (value, next) = if bytes[idx] == b'"' {
            parse_quoted_string_literal(entry, idx)?
        } else {
            let value_start = idx;
            while idx < bytes.len() && !bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            (entry[value_start..idx].to_string(), idx)
        };
        match key {
            "Amount" | "Charges" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid item amount".to_string())?;
                item.count = parsed.max(1);
            }
            "String" => {
                item.attributes.push(ItemAttribute::String(value));
            }
            "ChestQuestNumber" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid ChestQuestNumber".to_string())?;
                item.attributes
                    .push(ItemAttribute::ChestQuestNumber(parsed));
            }
            "ContainerLiquidType" => {
                let parsed = value
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| "invalid ContainerLiquidType".to_string())?;
                item.attributes
                    .push(ItemAttribute::ContainerLiquidType(parsed));
            }
            "PoolLiquidType" => {
                let parsed = value
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| "invalid PoolLiquidType".to_string())?;
                item.attributes.push(ItemAttribute::PoolLiquidType(parsed));
            }
            "RemainingExpireTime" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid RemainingExpireTime".to_string())?;
                item.attributes
                    .push(ItemAttribute::RemainingExpireTime(parsed));
            }
            "KeyholeNumber" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid KeyholeNumber".to_string())?;
                item.attributes.push(ItemAttribute::KeyholeNumber(parsed));
            }
            "DoorQuestNumber" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid DoorQuestNumber".to_string())?;
                item.attributes.push(ItemAttribute::DoorQuestNumber(parsed));
            }
            "DoorQuestValue" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid DoorQuestValue".to_string())?;
                item.attributes.push(ItemAttribute::DoorQuestValue(parsed));
            }
            "Level" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid Level".to_string())?;
                item.attributes.push(ItemAttribute::Level(parsed));
            }
            "RemainingUses" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid RemainingUses".to_string())?;
                item.attributes.push(ItemAttribute::RemainingUses(parsed));
            }
            "KeyNumber" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid KeyNumber".to_string())?;
                item.attributes.push(ItemAttribute::KeyNumber(parsed));
            }
            "SavedExpireTime" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid SavedExpireTime".to_string())?;
                item.attributes
                    .push(ItemAttribute::SavedExpireTime(parsed));
            }
            "AbsTeleportDestination" => {
                let parsed = value
                    .trim()
                    .parse::<i32>()
                    .map_err(|_| "invalid AbsTeleportDestination".to_string())?;
                item.attributes
                    .push(ItemAttribute::AbsTeleportDestination(parsed));
            }
            "Responsible" => {
                let parsed = value
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| "invalid Responsible".to_string())?;
                item.attributes.push(ItemAttribute::Responsible(parsed));
            }
            _ => {
                item.attributes.push(ItemAttribute::Unknown {
                    key: key.to_string(),
                    value,
                });
            }
        }
        idx = next;
    }

    Ok(item)
}

fn content_item_to_stack(item: &ParsedContentItem) -> ItemStack {
    ItemStack { id: crate::entities::item::ItemId::next(),
        type_id: item.type_id,
        count: item.count,
        attributes: item.attributes.clone(),
        contents: item.contents.iter().map(content_item_to_stack).collect(),
    }
}

fn content_items_to_stacks(items: &[ParsedContentItem]) -> Vec<ItemStack> {
    items.iter().map(content_item_to_stack).collect()
}

fn split_top_level_items(content: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut in_quotes = false;
    let mut escape = false;
    let mut depth = 0i32;
    let mut start = 0usize;

    for (idx, ch) in content.char_indices() {
        if in_quotes {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_quotes = false;
            }
            continue;
        }

        match ch {
            '"' => in_quotes = true,
            '{' => depth += 1,
            '}' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ',' if depth == 0 => {
                parts.push(content[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start < content.len() {
        parts.push(content[start..].trim());
    }
    parts
}

fn find_matching_brace(input: &str, open_idx: usize) -> Result<usize, String> {
    let mut in_quotes = false;
    let mut escape = false;
    let mut depth = 0i32;

    for (idx, ch) in input.char_indices().skip(open_idx) {
        if in_quotes {
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == '"' {
                in_quotes = false;
            }
            continue;
        }

        match ch {
            '"' => in_quotes = true,
            '{' => depth += 1,
            '}' => {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(idx);
                    }
                }
            }
            _ => {}
        }
    }

    Err("missing '}'".to_string())
}

fn is_key_char(value: u8) -> bool {
    (value as char).is_ascii_alphanumeric() || value == b'_'
}

fn skip_ws(input: &str, mut idx: usize) -> usize {
    while idx < input.len() && input.as_bytes()[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn parse_quoted_string_literal(input: &str, start: usize) -> Result<(String, usize), String> {
    let bytes = input.as_bytes();
    if start >= bytes.len() || bytes[start] != b'"' {
        return Err("missing opening quote".to_string());
    }
    let mut out = String::new();
    let mut idx = start + 1;
    let mut escape = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if escape {
            let decoded = match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                other => other,
            };
            out.push(decoded);
            escape = false;
            idx += 1;
            continue;
        }
        if ch == '\\' {
            escape = true;
            idx += 1;
            continue;
        }
        if ch == '"' {
            return Ok((out, idx + 1));
        }
        out.push(ch);
        idx += 1;
    }
    Err("unterminated quoted string".to_string())
}

fn filter_and_sort_skill_rows(mut rows: Vec<SkillRow>) -> Vec<SkillRow> {
    rows.retain(|row| row.values[2] != i32::MIN);
    rows.sort_by_key(|row| row.skill_id);
    rows
}

fn skill_rows_from_skillset(skills: &SkillSet) -> Vec<SkillRow> {
    let mut rows = Vec::new();
    for skill_id in 0..=MAX_SKILL_ID {
        rows.push(SkillRow::new(skill_id, default_skill_row_values()));
    }
    let mut index_by_id = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        index_by_id.insert(row.skill_id, index);
    }
    let mapping = [
        (1, skills.magic),
        (6, skills.shielding),
        (7, skills.distance),
        (8, skills.sword),
        (9, skills.club),
        (10, skills.axe),
        (11, skills.fist),
        (13, skills.fishing),
    ];
    for (skill_id, skill) in mapping {
        if let Some(index) = index_by_id.get(&skill_id).copied() {
            let row = &mut rows[index];
            row.values[0] = i32::from(skill.level);
            row.values[13] = 10;
            apply_skill_progress_values(&mut row.values, skill.level, skill.progress);
        }
    }
    rows
}

fn skillset_from_rows(rows: &[SkillRow]) -> SkillSet {
    let mut skills = SkillSet::default();
    for row in rows {
        if row.values[2] == i32::MIN {
            continue;
        }
        let level = row.values.get(0).copied().unwrap_or(0).max(0) as u16;
        let progress = skill_progress_from_values(&row.values);
        let skill_level = SkillLevel { level, progress };
        match row.skill_id {
            1 => skills.magic = skill_level,
            6 => skills.shielding = skill_level,
            7 => skills.distance = skill_level,
            8 => skills.sword = skill_level,
            9 => skills.club = skill_level,
            10 => skills.axe = skill_level,
            11 => skills.fist = skill_level,
            13 => skills.fishing = skill_level,
            _ => {}
        }
    }
    skills
}

#[derive(Debug, Clone, Copy)]
struct StatGains {
    health: u32,
    mana: u32,
    capacity: u32,
}

fn stat_gains_for_profession(profession: u8) -> StatGains {
    match profession {
        1 | 11 => StatGains {
            health: 15,
            mana: 5,
            capacity: 25,
        },
        2 | 12 => StatGains {
            health: 10,
            mana: 15,
            capacity: 20,
        },
        3 | 4 | 13 | 14 => StatGains {
            health: 5,
            mana: 30,
            capacity: 10,
        },
        _ => StatGains {
            health: 5,
            mana: 5,
            capacity: 10,
        },
    }
}

fn stats_from_level_and_profession(level: u16, profession: u8) -> Stats {
    let gains = stat_gains_for_profession(profession);
    let mut stats = Stats::base_for_profession(profession);
    let levels = u32::from(level.saturating_sub(1));
    stats.max_health = stats
        .max_health
        .saturating_add(levels.saturating_mul(gains.health));
    stats.max_mana = stats
        .max_mana
        .saturating_add(levels.saturating_mul(gains.mana));
    stats.capacity = stats
        .capacity
        .saturating_add(levels.saturating_mul(gains.capacity));
    stats.health = stats.max_health;
    stats.mana = stats.max_mana;
    stats
}

fn apply_stat_skills_from_rows(rows: &[SkillRow], player: &mut PlayerState) -> bool {
    let mut applied = false;
    let mut has_health = false;
    let mut has_mana = false;
    for row in rows {
        if row.values[2] == i32::MIN {
            continue;
        }
        match row.skill_id {
            2 => {
                let max_value = row.values[0].max(0) as u32;
                let current_value = row.values[1].max(0) as u32;
                player.stats.max_health = max_value;
                player.stats.health = current_value.min(max_value);
                has_health = true;
                applied = true;
            }
            3 => {
                let max_value = row.values[0].max(0) as u32;
                let current_value = row.values[1].max(0) as u32;
                player.stats.max_mana = max_value;
                player.stats.mana = current_value.min(max_value);
                has_mana = true;
                applied = true;
            }
            4 => {
                let value = row.values[0].clamp(0, i32::from(u16::MAX));
                player.base_speed = value as u16;
                applied = true;
            }
            5 => {
                let value = row.values[0].max(0) as u32;
                player.stats.capacity = value;
                applied = true;
            }
            _ => {}
        }
    }
    if has_health && (player.stats.health == 0 || player.stats.health > player.stats.max_health) {
        player.stats.health = player.stats.max_health;
    }
    if has_mana && (player.stats.mana == 0 || player.stats.mana > player.stats.max_mana) {
        player.stats.mana = player.stats.max_mana;
    }
    applied
}

pub(crate) fn skill_rows_from_player(player: &PlayerState) -> Vec<SkillRow> {
    if !player.raw_skills.is_empty() {
        return player.raw_skills.clone();
    }

    let mut rows = Vec::new();
    for skill_id in 0..=MAX_SKILL_ID {
        rows.push(SkillRow::new(skill_id, default_skill_row_values()));
    }
    let mut index_by_id = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        index_by_id.insert(row.skill_id, index);
    }

    ensure_level_skill_row(&mut rows, &mut index_by_id, player);
    ensure_combat_skill_rows(&mut rows, &mut index_by_id, player);
    ensure_stat_skill_rows(&mut rows, &mut index_by_id, player);

    rows
}

fn ensure_level_skill_row(
    rows: &mut Vec<SkillRow>,
    index_by_id: &mut HashMap<u32, usize>,
    player: &PlayerState,
) {
    let index = ensure_skill_row(rows, index_by_id, 0);
    let row = &mut rows[index];
    row.values[0] = i32::from(player.level);
    row.values[10] = player
        .experience
        .min(i32::MAX as u64) as i32;
    row.values[13] = 10;
    if let Some(exp_next) = skill_exp_for_level(i32::from(player.level) + 1, row.values[13]) {
        row.values[12] = exp_next.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    }
}

fn ensure_combat_skill_rows(
    rows: &mut Vec<SkillRow>,
    index_by_id: &mut HashMap<u32, usize>,
    player: &PlayerState,
) {
    let mapping = [
        (1, player.skills.magic),
        (6, player.skills.shielding),
        (7, player.skills.distance),
        (8, player.skills.sword),
        (9, player.skills.club),
        (10, player.skills.axe),
        (11, player.skills.fist),
        (13, player.skills.fishing),
    ];
    for (skill_id, skill) in mapping {
        let index = ensure_skill_row(rows, index_by_id, skill_id);
        let row = &mut rows[index];
        row.values[0] = i32::from(skill.level);
        row.values[13] = 10;
        apply_skill_progress_values(&mut row.values, skill.level, skill.progress);
    }
}

fn ensure_stat_skill_rows(
    rows: &mut Vec<SkillRow>,
    index_by_id: &mut HashMap<u32, usize>,
    player: &PlayerState,
) {
    set_stat_skill_value(
        rows,
        index_by_id,
        2,
        i32::try_from(player.stats.max_health).unwrap_or(i32::MAX),
    );
    set_stat_skill_value(
        rows,
        index_by_id,
        3,
        i32::try_from(player.stats.max_mana).unwrap_or(i32::MAX),
    );
    set_stat_skill_value(rows, index_by_id, 4, i32::from(player.base_speed));
    set_stat_skill_value(
        rows,
        index_by_id,
        5,
        i32::try_from(player.stats.capacity).unwrap_or(i32::MAX),
    );
    ensure_magic_defense_row(rows, index_by_id);
}

fn set_stat_skill_value(
    rows: &mut Vec<SkillRow>,
    index_by_id: &mut HashMap<u32, usize>,
    skill_id: u32,
    value: i32,
) {
    let index = ensure_skill_row(rows, index_by_id, skill_id);
    let row = &mut rows[index];
    row.values = default_stat_skill_values(skill_id, value);
}

fn ensure_magic_defense_row(rows: &mut Vec<SkillRow>, index_by_id: &mut HashMap<u32, usize>) {
    let index = ensure_skill_row(rows, index_by_id, 12);
    let row = &mut rows[index];
    let mut values = [0i32; RAW_SKILL_FIELDS];
    values[1] = i32::MAX;
    values[11] = 1000;
    values[12] = i32::MAX;
    values[13] = i32::MAX;
    row.values = values;
}

fn default_stat_skill_values(skill_id: u32, value: i32) -> [i32; RAW_SKILL_FIELDS] {
    let mut values = [0i32; RAW_SKILL_FIELDS];
    values[0] = value;
    values[1] = value;
    match skill_id {
        2 => values[10] = 5,
        3 => values[10] = 30,
        4 => {
            values[3] = 20;
            values[8] = 10;
            values[9] = 10;
            values[10] = 1;
        }
        5 => values[10] = 10,
        _ => {}
    }
    values
}

fn ensure_skill_row(
    rows: &mut Vec<SkillRow>,
    index_by_id: &mut HashMap<u32, usize>,
    skill_id: u32,
) -> usize {
    if let Some(index) = index_by_id.get(&skill_id).copied() {
        return index;
    }
    let values = default_skill_row_values();
    let index = rows.len();
    rows.push(SkillRow::new(skill_id, values));
    index_by_id.insert(skill_id, index);
    index
}

fn level_and_experience_from_rows(rows: &[SkillRow]) -> Option<(u16, u64)> {
    let row = rows
        .iter()
        .find(|row| row.skill_id == 0 && row.values[2] != i32::MIN)?;
    let level = row.values.get(0).copied().unwrap_or(0).max(1) as u16;
    let exp_base = row.values.get(13).copied().unwrap_or(0);
    let exp_level = skill_exp_for_level(i32::from(level), exp_base).unwrap_or(0);
    let exp_current = row.values.get(10).copied().unwrap_or(0) as i64;
    let exp_current = exp_current.max(exp_level).max(0) as u64;
    Some((level, exp_current))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::item::{ItemStack, ItemTypeId};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_store() -> SaveStore {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("tibia-save-test-{}", suffix));
        SaveStore::new(root)
    }

    #[test]
    fn save_and_load_player_roundtrip() {
        let store = temp_store();
        let id = PlayerId(42);
        let mut player = PlayerState::new(
            id,
            "Test".to_string(),
            Position { x: 100, y: 200, z: 7 },
        );
        player.level = 5;
        player.experience = 1234;
        player.profession = 2;
        player.premium = true;
        player.stats.health = 70;
        player.inventory.set_slot(
            InventorySlot::Head,
            Some(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(100),
                count: 2,
                attributes: Vec::new(),
                contents: Vec::new(),
            }),
        );
        player.quest_values.insert(10, 5);
        player
            .depots
            .insert(
                1,
                vec![ItemStack { id: crate::entities::item::ItemId::next(),
                    type_id: ItemTypeId(200),
                    count: 3,
                    attributes: Vec::new(),
                    contents: Vec::new(),
                }],
            );
        player.known_spells.insert(SpellId(7));

        store.save_player(&player).expect("save");
        let loaded = store.load_player(id).expect("load").expect("player");
        assert_eq!(loaded.id, player.id);
        assert_eq!(loaded.name, player.name);
        assert_eq!(loaded.position, player.position);
        assert_eq!(loaded.level, player.level);
        assert_eq!(loaded.experience, player.experience);
        assert_eq!(loaded.profession, player.profession);
        assert_eq!(loaded.premium, player.premium);
        assert_eq!(loaded.stats, player.stats);
        assert_eq!(loaded.skills, player.skills);
        assert_eq!(loaded.inventory.slot(InventorySlot::Head), player.inventory.slot(InventorySlot::Head));
        assert_eq!(loaded.quest_values, player.quest_values);
        assert_eq!(loaded.depots, player.depots);
        assert_eq!(loaded.known_spells, player.known_spells);

        let _ = fs::remove_dir_all(store.root);
    }
}
