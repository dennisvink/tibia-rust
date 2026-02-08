use crate::world::map::{SectorBounds, SectorCoord};
use crate::world::position::Position;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapDat {
    pub sector_bounds: Option<SectorBounds>,
    pub newbie_start: Option<Position>,
    pub veteran_start: Option<Position>,
    pub refreshed_cylinders: Option<u16>,
    pub marks: Vec<MapMark>,
    pub depots: Vec<MapDepot>,
    pub towns: Vec<MapTown>,
}

impl MapDat {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|err| format!("failed to read map.dat {}: {}", path.display(), err))?;
        let content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(err) => {
                eprintln!(
                    "tibia: map.dat contained invalid UTF-8; decoding lossy: {}",
                    path.display()
                );
                String::from_utf8_lossy(&err.into_bytes()).into_owned()
            }
        };
        parse_map_dat(&content)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapMark {
    pub name: String,
    pub position: Position,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapDepot {
    pub id: u16,
    pub name: String,
    pub capacity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapTown {
    pub id: u16,
    pub name: String,
    pub temple_position: Option<Position>,
}

#[derive(Default)]
struct PartialSector {
    x: Option<u16>,
    y: Option<u16>,
    z: Option<u8>,
}

fn parse_map_dat(content: &str) -> Result<MapDat, String> {
    let mut sector_min = PartialSector::default();
    let mut sector_max = PartialSector::default();
    let mut newbie_start = None;
    let mut veteran_start = None;
    let mut refreshed_cylinders = None;
    let mut marks = Vec::new();
    let mut depots = Vec::new();
    let mut towns = Vec::new();

    for (index, raw_line) in content.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();

        match key {
            "SectorXMin" => {
                sector_min.x = Some(parse_u16(value, line_no, key)?);
            }
            "SectorYMin" => {
                sector_min.y = Some(parse_u16(value, line_no, key)?);
            }
            "SectorZMin" => {
                sector_min.z = Some(parse_u8(value, line_no, key)?);
            }
            "SectorXMax" => {
                sector_max.x = Some(parse_u16(value, line_no, key)?);
            }
            "SectorYMax" => {
                sector_max.y = Some(parse_u16(value, line_no, key)?);
            }
            "SectorZMax" => {
                sector_max.z = Some(parse_u8(value, line_no, key)?);
            }
            "NewbieStart" => {
                newbie_start = Some(parse_position_value(value, line_no, key)?);
            }
            "VeteranStart" => {
                veteran_start = Some(parse_position_value(value, line_no, key)?);
            }
            "RefreshedCylinders" => {
                refreshed_cylinders = Some(parse_u16(value, line_no, key)?);
            }
            "Mark" => {
                marks.push(parse_mark(value, line_no)?);
            }
            "Depot" => {
                depots.push(parse_depot(value, line_no)?);
            }
            "Town" => {
                towns.push(parse_town(value, line_no)?);
            }
            _ => {}
        }
    }

    let sector_bounds = build_sector_bounds(sector_min, sector_max)?;
    let towns = if towns.is_empty() {
        derive_towns(&depots, &marks)
    } else {
        towns
    };

    Ok(MapDat {
        sector_bounds,
        newbie_start,
        veteran_start,
        refreshed_cylinders,
        marks,
        depots,
        towns,
    })
}

fn build_sector_bounds(
    min: PartialSector,
    max: PartialSector,
) -> Result<Option<SectorBounds>, String> {
    let min_complete = min.x.is_some() || min.y.is_some() || min.z.is_some();
    let max_complete = max.x.is_some() || max.y.is_some() || max.z.is_some();

    if !min_complete && !max_complete {
        return Ok(None);
    }

    let min = SectorCoord {
        x: min.x.ok_or_else(|| "map.dat missing SectorXMin".to_string())?,
        y: min.y.ok_or_else(|| "map.dat missing SectorYMin".to_string())?,
        z: min.z.ok_or_else(|| "map.dat missing SectorZMin".to_string())?,
    };
    let max = SectorCoord {
        x: max.x.ok_or_else(|| "map.dat missing SectorXMax".to_string())?,
        y: max.y.ok_or_else(|| "map.dat missing SectorYMax".to_string())?,
        z: max.z.ok_or_else(|| "map.dat missing SectorZMax".to_string())?,
    };

    Ok(Some(SectorBounds { min, max }))
}

fn parse_u16(raw: &str, line_no: usize, key: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("map.dat {} invalid u16 at line {}", key, line_no))
}

fn parse_u8(raw: &str, line_no: usize, key: &str) -> Result<u8, String> {
    raw.parse::<u8>()
        .map_err(|_| format!("map.dat {} invalid u8 at line {}", key, line_no))
}

fn parse_position_value(raw: &str, line_no: usize, key: &str) -> Result<Position, String> {
    let start = raw.find('[').ok_or_else(|| {
        format!("map.dat {} missing '[' at line {}", key, line_no)
    })?;
    let end = raw.find(']').ok_or_else(|| {
        format!("map.dat {} missing ']' at line {}", key, line_no)
    })?;
    if end <= start {
        return Err(format!(
            "map.dat {} invalid position at line {}",
            key, line_no
        ));
    }
    let values = &raw[start + 1..end];
    parse_position(values, line_no, key)
}

fn parse_position(values: &str, line_no: usize, key: &str) -> Result<Position, String> {
    let mut parts = values.split(',').map(|part| part.trim());
    let x = parts
        .next()
        .ok_or_else(|| format!("map.dat {} missing x at line {}", key, line_no))?
        .parse::<u16>()
        .map_err(|_| format!("map.dat {} invalid x at line {}", key, line_no))?;
    let y = parts
        .next()
        .ok_or_else(|| format!("map.dat {} missing y at line {}", key, line_no))?
        .parse::<u16>()
        .map_err(|_| format!("map.dat {} invalid y at line {}", key, line_no))?;
    let z = parts
        .next()
        .ok_or_else(|| format!("map.dat {} missing z at line {}", key, line_no))?
        .parse::<u8>()
        .map_err(|_| format!("map.dat {} invalid z at line {}", key, line_no))?;
    if parts.next().is_some() {
        return Err(format!(
            "map.dat {} extra values at line {}",
            key, line_no
        ));
    }
    Ok(Position { x, y, z })
}

fn parse_mark(value: &str, line_no: usize) -> Result<MapMark, String> {
    let value = value.trim();
    if !value.starts_with('(') || !value.ends_with(')') {
        return Err(format!("map.dat Mark invalid tuple at line {}", line_no));
    }
    let inner = &value[1..value.len() - 1];
    let (name, remainder) = parse_quoted_component(inner, line_no, "Mark")?;
    let remainder = remainder
        .trim_start()
        .strip_prefix(',')
        .ok_or_else(|| format!("map.dat Mark missing ',' at line {}", line_no))?
        .trim_start();
    let position = parse_position_value(remainder, line_no, "Mark")?;
    Ok(MapMark { name, position })
}

fn parse_depot(value: &str, line_no: usize) -> Result<MapDepot, String> {
    let value = value.trim();
    if !value.starts_with('(') || !value.ends_with(')') {
        return Err(format!("map.dat Depot invalid tuple at line {}", line_no));
    }
    let inner = &value[1..value.len() - 1];
    let (id_raw, remainder) = split_first_component(inner)
        .ok_or_else(|| format!("map.dat Depot missing id at line {}", line_no))?;
    let id = id_raw
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("map.dat Depot invalid id at line {}", line_no))?;
    let remainder = remainder
        .trim_start()
        .strip_prefix(',')
        .ok_or_else(|| format!("map.dat Depot missing name at line {}", line_no))?
        .trim_start();
    let (name, remainder) = parse_quoted_component(remainder, line_no, "Depot")?;
    let remainder = remainder
        .trim_start()
        .strip_prefix(',')
        .ok_or_else(|| format!("map.dat Depot missing capacity at line {}", line_no))?
        .trim_start();
    let capacity = remainder
        .parse::<u32>()
        .map_err(|_| format!("map.dat Depot invalid capacity at line {}", line_no))?;
    Ok(MapDepot {
        id,
        name,
        capacity,
    })
}

fn parse_town(value: &str, line_no: usize) -> Result<MapTown, String> {
    let value = value.trim();
    if !value.starts_with('(') || !value.ends_with(')') {
        return Err(format!("map.dat Town invalid tuple at line {}", line_no));
    }
    let inner = &value[1..value.len() - 1];
    let (id_raw, remainder) = split_first_component(inner)
        .ok_or_else(|| format!("map.dat Town missing id at line {}", line_no))?;
    let id = id_raw
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("map.dat Town invalid id at line {}", line_no))?;
    let remainder = remainder
        .trim_start()
        .strip_prefix(',')
        .ok_or_else(|| format!("map.dat Town missing name at line {}", line_no))?
        .trim_start();
    let (name, remainder) = parse_quoted_component(remainder, line_no, "Town")?;
    let remainder = remainder
        .trim_start()
        .strip_prefix(',')
        .ok_or_else(|| format!("map.dat Town missing temple position at line {}", line_no))?
        .trim_start();
    let temple_position = parse_position_value(remainder, line_no, "Town")?;
    Ok(MapTown {
        id,
        name,
        temple_position: Some(temple_position),
    })
}

fn derive_towns(depots: &[MapDepot], marks: &[MapMark]) -> Vec<MapTown> {
    if depots.is_empty() {
        return Vec::new();
    }
    let mut mark_lookup = HashMap::new();
    for mark in marks {
        let key = normalize_town_key(&mark.name);
        if key.is_empty() {
            continue;
        }
        mark_lookup.entry(key).or_insert(mark.position);
    }
    depots
        .iter()
        .map(|depot| {
            let key = normalize_town_key(&depot.name);
            let temple_position = mark_lookup.get(&key).copied();
            MapTown {
                id: depot.id,
                name: depot.name.clone(),
                temple_position,
            }
        })
        .collect()
}

fn normalize_town_key(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        }
    }
    out
}

fn parse_quoted_component<'a>(
    input: &'a str,
    line_no: usize,
    label: &str,
) -> Result<(String, &'a str), String> {
    let start = input
        .find('"')
        .ok_or_else(|| format!("map.dat {} missing quote at line {}", label, line_no))?;
    let rest = &input[start + 1..];
    let end = rest.find('"').ok_or_else(|| {
        format!("map.dat {} missing closing quote at line {}", label, line_no)
    })?;
    Ok((rest[..end].to_string(), &rest[end + 1..]))
}

fn split_first_component(input: &str) -> Option<(&str, &str)> {
    let mut depth = 0i32;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            ',' if depth == 0 => return Some((&input[..idx], &input[idx..])),
            _ => {}
        }
    }
    if input.is_empty() {
        None
    } else {
        Some((input, ""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FuzzRng(u64);

    impl FuzzRng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            (self.0 >> 32) as u32
        }

        fn next_u8(&mut self) -> u8 {
            (self.next_u32() & 0xff) as u8
        }

        fn gen_ascii_line(&mut self, len: usize) -> String {
            let mut out = String::with_capacity(len);
            for _ in 0..len {
                let byte = (self.next_u8() % 95) + 0x20;
                out.push(byte as char);
            }
            out
        }
    }

    #[test]
    fn fuzz_parse_map_dat() {
        let mut rng = FuzzRng::new(0x1234_5678_9abc_def0);
        let mut content = String::new();
        for i in 0..200 {
            if i % 20 == 0 {
                content.push_str("SectorXMin = 1\nSectorYMin = 1\nSectorZMin = 7\n");
                content.push_str("SectorXMax = 2\nSectorYMax = 2\nSectorZMax = 7\n");
                content.push_str("NewbieStart = [100,200,7]\n");
                continue;
            }
            let len = (rng.next_u32() % 80) as usize;
            content.push_str(&rng.gen_ascii_line(len));
            content.push('\n');
        }
        let _ = parse_map_dat(&content);
    }
}
