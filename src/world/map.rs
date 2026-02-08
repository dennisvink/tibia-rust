use crate::entities::item::{ItemAttribute, ItemStack, ItemTypeId};
use crate::world::position::Position;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const SECTOR_TILE_SIZE: u16 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SectorCoord {
    pub x: u16,
    pub y: u16,
    pub z: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectorBounds {
    pub min: SectorCoord,
    pub max: SectorCoord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tile {
    pub position: crate::world::position::Position,
    pub items: Vec<ItemStack>,
    pub item_details: Vec<MapItem>,
    pub refresh: bool,
    pub protection_zone: bool,
    pub no_logout: bool,
    pub annotations: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapItem {
    pub type_id: ItemTypeId,
    pub count: u16,
    pub attributes: Vec<ItemAttribute>,
    pub contents: Vec<MapItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapSector {
    pub coord: SectorCoord,
    pub path: PathBuf,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Map {
    pub name: String,
    pub sector_bounds: Option<SectorBounds>,
    pub sectors: Vec<MapSector>,
    pub tiles: HashMap<Position, Tile>,
}

impl Map {
    pub fn tile_count(&self) -> usize {
        self.tiles.len()
    }

    pub fn has_tile(&self, position: Position) -> bool {
        self.tiles.contains_key(&position)
    }

    pub fn tile(&self, position: Position) -> Option<&Tile> {
        self.tiles.get(&position)
    }

    pub fn tile_mut(&mut self, position: Position) -> Option<&mut Tile> {
        self.tiles.get_mut(&position)
    }

    pub fn sector_count(&self) -> usize {
        self.sectors.len()
    }

    pub fn sector_for_position(&self, position: Position) -> SectorCoord {
        SectorCoord {
            x: position.x / SECTOR_TILE_SIZE,
            y: position.y / SECTOR_TILE_SIZE,
            z: position.z,
        }
    }

    pub fn has_sector(&self, coord: SectorCoord) -> bool {
        self.sectors.iter().any(|sector| sector.coord == coord)
    }
}

pub fn load_sector_index(map_dir: &Path) -> Result<Map, String> {
    let mut sectors = Vec::new();

    let entries = std::fs::read_dir(map_dir)
        .map_err(|err| format!("failed to read map dir {}: {}", map_dir.display(), err))?;

    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read map dir entry: {}", err))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sec") {
            continue;
        }

        let file_stem = match path.file_stem().and_then(|stem| stem.to_str()) {
            Some(stem) => stem,
            None => continue,
        };

        let mut parts = file_stem.split('-');
        let x = match parts.next().and_then(|value| value.parse::<u16>().ok()) {
            Some(value) => value,
            None => continue,
        };
        let y = match parts.next().and_then(|value| value.parse::<u16>().ok()) {
            Some(value) => value,
            None => continue,
        };
        let z = match parts.next().and_then(|value| value.parse::<u8>().ok()) {
            Some(value) => value,
            None => continue,
        };
        if parts.next().is_some() {
            continue;
        }

        sectors.push(MapSector {
            coord: SectorCoord { x, y, z },
            path,
        });
    }

    let sector_bounds = compute_sector_bounds(&sectors);

    Ok(Map {
        name: "map".to_string(),
        sector_bounds,
        sectors,
        tiles: HashMap::new(),
    })
}

pub fn load_map(map_dir: &Path) -> Result<Map, String> {
    let mut map = load_sector_index(map_dir)?;
    map.tiles = load_sector_tiles(&map.sectors)?;
    Ok(map)
}

fn load_sector_tiles(sectors: &[MapSector]) -> Result<HashMap<Position, Tile>, String> {
    let mut tiles = HashMap::new();

    for sector in sectors {
        let bytes = std::fs::read(&sector.path).map_err(|err| {
            format!(
                "failed to read sector {}: {}",
                sector.path.display(),
                err
            )
        })?;
        let content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(err) => {
                let bytes = err.into_bytes();
                eprintln!(
                    "warning: sector {} contained invalid UTF-8; decoding latin-1",
                    sector.path.display()
                );
                bytes.iter().map(|b| *b as char).collect()
            }
        };
        parse_sector_content(&content, sector, &mut tiles)?;
    }

    Ok(tiles)
}

fn parse_sector_content(
    content: &str,
    sector: &MapSector,
    tiles: &mut HashMap<Position, Tile>,
) -> Result<(), String> {
    for (line_no, raw_line) in content.lines().enumerate() {
        let line_no = line_no + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (coord, remainder) = line
            .split_once(':')
            .ok_or_else(|| format!("sector {} line {} missing ':'", sector.path.display(), line_no))?;
        let (local_x, local_y) = parse_tile_coord(coord.trim()).map_err(|err| {
            format!(
                "sector {} line {} invalid coord: {}",
                sector.path.display(),
                line_no,
                err
            )
        })?;
        let entry = parse_tile_entry(remainder).map_err(|err| {
            format!(
                "sector {} line {} invalid content: {}",
                sector.path.display(),
                line_no,
                err
            )
        })?;
        if entry.items.is_empty()
            && entry.annotations.is_empty()
            && entry.tags.is_empty()
            && !entry.no_logout
        {
            continue;
        }

        let position = sector_position(sector.coord, local_x, local_y).map_err(|err| {
            format!(
                "sector {} line {} invalid position: {}",
                sector.path.display(),
                line_no,
                err
            )
        })?;

        let tile = Tile {
            position,
            items: entry.items,
            item_details: entry.item_details,
            refresh: entry.refresh,
            protection_zone: entry.protection_zone,
            no_logout: entry.no_logout,
            annotations: entry.annotations,
            tags: entry.tags,
        };
        tiles.insert(position, tile);
    }

    Ok(())
}

fn parse_tile_coord(coord: &str) -> Result<(u16, u16), String> {
    let (x_raw, y_raw) = coord
        .split_once('-')
        .ok_or_else(|| "missing '-'".to_string())?;
    let x = x_raw
        .trim()
        .parse::<u16>()
        .map_err(|_| "invalid x".to_string())?;
    let y = y_raw
        .trim()
        .parse::<u16>()
        .map_err(|_| "invalid y".to_string())?;
    if x >= SECTOR_TILE_SIZE || y >= SECTOR_TILE_SIZE {
        return Err("coord out of range".to_string());
    }
    Ok((x, y))
}

struct TileEntry {
    items: Vec<ItemStack>,
    item_details: Vec<MapItem>,
    refresh: bool,
    protection_zone: bool,
    no_logout: bool,
    annotations: Vec<String>,
    tags: Vec<String>,
}

fn parse_tile_entry(remainder: &str) -> Result<TileEntry, String> {
    let mut entry = TileEntry {
        items: Vec::new(),
        item_details: Vec::new(),
        refresh: false,
        protection_zone: false,
        no_logout: false,
        annotations: Vec::new(),
        tags: Vec::new(),
    };
    let Some((start, end)) = find_content_span(remainder)? else {
        parse_tile_prefix(remainder, &mut entry)?;
        return Ok(entry);
    };
    parse_tile_prefix(&remainder[..start], &mut entry)?;
    let inner = remainder[start + 1..end].trim();
    if !inner.is_empty() {
        let (items, annotations) = parse_content_items(inner)?;
        entry.items = items
            .iter()
            .map(|item| ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: item.type_id,
                count: item.count,
                attributes: Vec::new(),
                contents: Vec::new(),
            })
            .collect();
        entry.item_details = items;
        entry.annotations = annotations;
    }

    Ok(entry)
}

fn parse_tile_prefix(prefix: &str, entry: &mut TileEntry) -> Result<(), String> {
    let mut tokens = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let mut escape = false;

    for (idx, ch) in prefix.char_indices() {
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
            ',' => {
                tokens.push(prefix[start..idx].trim().to_string());
                start = idx + 1;
            }
            _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn load_sample_sector_tiles_from_assets() {
        let asset_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let map_dir = asset_root.join("map");
        let map = load_sector_index(&map_dir).expect("sector index");
        let sector = map
            .sectors
            .iter()
            .filter_map(|sector| fs::metadata(&sector.path).ok().map(|m| (sector, m.len())))
            .max_by_key(|(_, len)| *len)
            .map(|(sector, _)| sector)
            .or_else(|| map.sectors.first())
            .expect("map sectors")
            .clone();

        let tiles = load_sector_tiles(&[sector.clone()]).expect("sector tiles");
        assert!(
            !tiles.is_empty(),
            "expected tiles in {}",
            sector.path.display()
        );
    }
}
    if start < prefix.len() {
        tokens.push(prefix[start..].trim().to_string());
    }

    for token in tokens {
        if token.is_empty() {
            continue;
        }
        match token {
            ref value if value.eq_ignore_ascii_case("Refresh") => entry.refresh = true,
            ref value if value.eq_ignore_ascii_case("ProtectionZone") => entry.protection_zone = true,
            ref value if value.eq_ignore_ascii_case("NoLogout") => entry.no_logout = true,
            _ => entry.tags.push(token),
        }
    }

    Ok(())
}

fn find_content_span(input: &str) -> Result<Option<(usize, usize)>, String> {
    let Some(content_idx) = input.find("Content") else {
        return Ok(None);
    };
    let after = &input[content_idx..];
    let brace_rel = after
        .find('{')
        .ok_or_else(|| "missing '{'".to_string())?;
    let brace_start = content_idx + brace_rel;
    let brace_end = find_matching_brace(input, brace_start)?;
    Ok(Some((brace_start, brace_end)))
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

fn parse_content_items(content: &str) -> Result<(Vec<MapItem>, Vec<String>), String> {
    let mut items = Vec::new();
    let mut annotations = Vec::new();
    for entry in split_top_level_items(content) {
        if entry.trim().is_empty() {
            continue;
        }
        match parse_content_item_entry(entry) {
            Ok((item, mut item_notes)) => {
                annotations.append(&mut item_notes);
                items.push(item);
            }
            Err(err) => {
                if entry.trim().starts_with("String") {
                    let (value, _) = parse_string_only(entry)?;
                    annotations.push(value);
                    continue;
                }
                return Err(err);
            }
        }
    }
    Ok((items, annotations))
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

fn parse_content_item_entry(entry: &str) -> Result<(MapItem, Vec<String>), String> {
    let mut idx = 0usize;
    let bytes = entry.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
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

    let mut item = MapItem {
        type_id: ItemTypeId(type_id),
        count: 1,
        attributes: Vec::new(),
        contents: Vec::new(),
    };
    let mut annotations = Vec::new();

    while idx < bytes.len() {
        idx = skip_ws(entry, idx);
        if idx >= bytes.len() {
            break;
        }

        if entry[idx..].starts_with("String") {
            idx += "String".len();
            idx = skip_ws(entry, idx);
            if idx < bytes.len() && bytes[idx] == b'=' {
                idx += 1;
            }
            idx = skip_ws(entry, idx);
            let (value, next) = parse_quoted_string_literal(entry, idx)?;
            annotations.push(value.clone());
            item.attributes.push(ItemAttribute::String(value));
            idx = next;
            continue;
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
                let (nested_items, mut nested_notes) = parse_content_items(inner)?;
                item.contents = nested_items;
                annotations.append(&mut nested_notes);
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
        let key = entry[key_start..idx].trim().to_string();
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

        match key.as_str() {
            "ChestQuestNumber" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid ChestQuestNumber".to_string())?;
                item.attributes.push(ItemAttribute::ChestQuestNumber(parsed));
            }
            "ContainerLiquidType" => {
                let parsed = value
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| "invalid ContainerLiquidType".to_string())?;
                item.attributes
                    .push(ItemAttribute::ContainerLiquidType(parsed));
            }
            "Amount" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid Amount".to_string())?;
                item.attributes.push(ItemAttribute::Amount(parsed));
            }
            "PoolLiquidType" => {
                let parsed = value
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| "invalid PoolLiquidType".to_string())?;
                item.attributes
                    .push(ItemAttribute::PoolLiquidType(parsed));
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
                item.attributes
                    .push(ItemAttribute::DoorQuestNumber(parsed));
            }
            "DoorQuestValue" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid DoorQuestValue".to_string())?;
                item.attributes
                    .push(ItemAttribute::DoorQuestValue(parsed));
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
                item.attributes
                    .push(ItemAttribute::RemainingUses(parsed));
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
            "Charges" => {
                let parsed = value
                    .trim()
                    .parse::<u16>()
                    .map_err(|_| "invalid Charges".to_string())?;
                item.attributes.push(ItemAttribute::Charges(parsed));
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
                if key == "String" {
                    annotations.push(value.clone());
                    item.attributes.push(ItemAttribute::String(value));
                } else {
                    item.attributes.push(ItemAttribute::Unknown { key, value });
                }
            }
        }
        idx = next;
    }

    Ok((item, annotations))
}

fn parse_string_only(entry: &str) -> Result<(String, usize), String> {
    let mut idx = 0usize;
    idx = skip_ws(entry, idx);
    if entry[idx..].starts_with("String") {
        idx += "String".len();
        idx = skip_ws(entry, idx);
        if idx < entry.len() && entry.as_bytes()[idx] == b'=' {
            idx += 1;
        }
        idx = skip_ws(entry, idx);
        return parse_quoted_string_literal(entry, idx);
    }
    Err("string entry missing String".to_string())
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
            let expanded = expand_double_escapes(&out);
            return Ok((expanded, idx + 1));
        }
        out.push(ch);
        idx += 1;
    }

    Err("missing closing quote".to_string())
}

fn expand_double_escapes(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
                continue;
            }
        }
        out.push(ch);
    }
    out
}

fn sector_position(coord: SectorCoord, local_x: u16, local_y: u16) -> Result<Position, String> {
    let base_x = coord
        .x
        .checked_mul(SECTOR_TILE_SIZE)
        .ok_or_else(|| "sector x overflow".to_string())?;
    let base_y = coord
        .y
        .checked_mul(SECTOR_TILE_SIZE)
        .ok_or_else(|| "sector y overflow".to_string())?;
    let x = base_x
        .checked_add(local_x)
        .ok_or_else(|| "position x overflow".to_string())?;
    let y = base_y
        .checked_add(local_y)
        .ok_or_else(|| "position y overflow".to_string())?;
    Ok(Position { x, y, z: coord.z })
}

fn compute_sector_bounds(sectors: &[MapSector]) -> Option<SectorBounds> {
    let first = sectors.first()?;
    let mut min = first.coord;
    let mut max = first.coord;

    for sector in sectors.iter().skip(1) {
        min.x = min.x.min(sector.coord.x);
        min.y = min.y.min(sector.coord.y);
        min.z = min.z.min(sector.coord.z);

        max.x = max.x.max(sector.coord.x);
        max.y = max.y.max(sector.coord.y);
        max.z = max.z.max(sector.coord.z);
    }

    Some(SectorBounds { min, max })
}
