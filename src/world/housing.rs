use crate::world::position::Position;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HouseArea {
    pub id: u16,
    pub name: String,
    pub rent_per_square: u16,
    pub depot_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct House {
    pub id: u32,
    pub name: String,
    pub description: String,
    pub rent_offset: u32,
    pub area_id: u16,
    pub guild_house: bool,
    pub exit: Position,
    pub center: Position,
    pub fields: Vec<Position>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HouseOwner {
    pub id: u32,
    pub owner: u32,
    pub last_transition: u64,
    pub paid_until: u64,
    pub guests: Vec<String>,
    pub subowners: Vec<String>,
}

pub fn load_house_areas(path: &Path) -> Result<Vec<HouseArea>, String> {
    let bytes = std::fs::read(path)
        .map_err(|err| format!("failed to read houseareas.dat {}: {}", path.display(), err))?;
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            eprintln!(
                "tibia: houseareas.dat contained invalid UTF-8; decoding lossy: {}",
                path.display()
            );
            String::from_utf8_lossy(&err.into_bytes()).into_owned()
        }
    };
    parse_house_areas(&content)
}

pub fn load_houses(path: &Path) -> Result<Vec<House>, String> {
    let bytes = std::fs::read(path)
        .map_err(|err| format!("failed to read houses.dat {}: {}", path.display(), err))?;
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            eprintln!(
                "tibia: houses.dat contained invalid UTF-8; decoding lossy: {}",
                path.display()
            );
            String::from_utf8_lossy(&err.into_bytes()).into_owned()
        }
    };
    parse_houses(&content)
}

pub fn load_house_owners(path: &Path) -> Result<Vec<HouseOwner>, String> {
    let bytes = std::fs::read(path)
        .map_err(|err| format!("failed to read owners.dat {}: {}", path.display(), err))?;
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            eprintln!(
                "tibia: owners.dat contained invalid UTF-8; decoding lossy: {}",
                path.display()
            );
            String::from_utf8_lossy(&err.into_bytes()).into_owned()
        }
    };
    parse_house_owners(&content)
}

pub fn save_house_owners(path: &Path, owners: &[HouseOwner]) -> Result<(), String> {
    let data = serialize_house_owners(owners)?;
    std::fs::write(path, data)
        .map_err(|err| format!("failed to write owners.dat {}: {}", path.display(), err))
}

fn parse_house_areas(content: &str) -> Result<Vec<HouseArea>, String> {
    let mut areas = Vec::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line = strip_inline_comment(raw_line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !line.starts_with("Area") {
            continue;
        }
        let line_no = index + 1;
        let start = line.find('(').ok_or_else(|| {
            format!("houseareas.dat missing '(' at line {}", line_no)
        })?;
        let end = line.rfind(')').ok_or_else(|| {
            format!("houseareas.dat missing ')' at line {}", line_no)
        })?;
        if end <= start {
            return Err(format!("houseareas.dat invalid tuple at line {}", line_no));
        }
        let tuple = &line[start + 1..end];
        let parts = split_top_level(tuple);
        if parts.len() != 4 {
            return Err(format!(
                "houseareas.dat expected 4 values at line {}, got {}",
                line_no,
                parts.len()
            ));
        }
        let id = parse_u16(parts[0], line_no, "Area")?;
        let name = parse_quoted(parts[1], line_no, "Area")?;
        let rent_per_square = parse_u16(parts[2], line_no, "Area")?;
        let depot_id = parse_u16(parts[3], line_no, "Area")?;
        areas.push(HouseArea {
            id,
            name,
            rent_per_square,
            depot_id,
        });
    }
    Ok(areas)
}

fn parse_houses(content: &str) -> Result<Vec<House>, String> {
    let mut houses = Vec::new();
    let mut current = HouseBuilder::default();
    let mut have_current = false;
    let mut lines = content.lines().enumerate().peekable();

    while let Some((index, raw_line)) = lines.next() {
        let mut line = strip_inline_comment(raw_line);
        line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("Fields") && !line.contains('}') {
            while let Some((_, continuation)) = lines.next() {
                let cleaned = strip_inline_comment(continuation);
                line.push_str(cleaned.trim());
                if line.contains('}') {
                    break;
                }
            }
        }

        line = strip_inline_comment(&line);
        line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let line_no = index + 1;
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();

        if key == "ID" {
            if have_current {
                houses.push(current.build()?);
                current = HouseBuilder::default();
            }
            have_current = true;
            current.id = Some(parse_u32(value, line_no, "ID")?);
            continue;
        }

        match key {
            "Name" => current.name = Some(parse_quoted(value, line_no, "Name")?),
            "Description" => current.description = Some(parse_quoted(value, line_no, "Description")?),
            "RentOffset" => current.rent_offset = Some(parse_u32(value, line_no, "RentOffset")?),
            "Area" => current.area_id = Some(parse_u16(value, line_no, "Area")?),
            "GuildHouse" => current.guild_house = Some(parse_bool(value, line_no, "GuildHouse")?),
            "Exit" => current.exit = Some(parse_position_value(value, line_no, "Exit")?),
            "Center" => current.center = Some(parse_position_value(value, line_no, "Center")?),
            "Fields" => current.fields = parse_positions_list(value, line_no)?,
            _ => {}
        }
    }

    if have_current {
        houses.push(current.build()?);
    }

    Ok(houses)
}

fn parse_house_owners(content: &str) -> Result<Vec<HouseOwner>, String> {
    let mut owners = Vec::new();
    let mut current = OwnerBuilder::default();
    let mut have_current = false;

    for (index, raw_line) in content.lines().enumerate() {
        let line = strip_inline_comment(raw_line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        let line_no = index + 1;

        if key == "ID" {
            if have_current {
                owners.push(current.build()?);
                current = OwnerBuilder::default();
            }
            have_current = true;
            current.id = Some(parse_u32(value, line_no, "ID")?);
            continue;
        }

        match key {
            "Owner" => current.owner = Some(parse_u32(value, line_no, "Owner")?),
            "LastTransition" => {
                current.last_transition = Some(parse_u64(value, line_no, "LastTransition")?)
            }
            "PaidUntil" => current.paid_until = Some(parse_u64(value, line_no, "PaidUntil")?),
            "Guests" => current.guests = parse_string_list(value, line_no, "Guests")?,
            "Subowners" => current.subowners = parse_string_list(value, line_no, "Subowners")?,
            _ => {}
        }
    }

    if have_current {
        owners.push(current.build()?);
    }

    Ok(owners)
}

fn serialize_house_owners(owners: &[HouseOwner]) -> Result<String, String> {
    let mut out = String::new();
    for (index, owner) in owners.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str(&format!("ID = {}\n", owner.id));
        out.push_str(&format!("Owner = {}\n", owner.owner));
        out.push_str(&format!("LastTransition = {}\n", owner.last_transition));
        out.push_str(&format!("PaidUntil = {}\n", owner.paid_until));
        out.push_str("Guests = ");
        out.push_str(&serialize_string_list(&owner.guests)?);
        out.push('\n');
        out.push_str("Subowners = ");
        out.push_str(&serialize_string_list(&owner.subowners)?);
        out.push('\n');
    }
    Ok(out)
}

#[derive(Default)]
struct HouseBuilder {
    id: Option<u32>,
    name: Option<String>,
    description: Option<String>,
    rent_offset: Option<u32>,
    area_id: Option<u16>,
    guild_house: Option<bool>,
    exit: Option<Position>,
    center: Option<Position>,
    fields: Vec<Position>,
}

impl HouseBuilder {
    fn build(self) -> Result<House, String> {
        Ok(House {
            id: self.id.ok_or_else(|| "houses.dat missing ID".to_string())?,
            name: self
                .name
                .ok_or_else(|| "houses.dat missing Name".to_string())?,
            description: self
                .description
                .ok_or_else(|| "houses.dat missing Description".to_string())?,
            rent_offset: self
                .rent_offset
                .ok_or_else(|| "houses.dat missing RentOffset".to_string())?,
            area_id: self
                .area_id
                .ok_or_else(|| "houses.dat missing Area".to_string())?,
            guild_house: self
                .guild_house
                .ok_or_else(|| "houses.dat missing GuildHouse".to_string())?,
            exit: self
                .exit
                .ok_or_else(|| "houses.dat missing Exit".to_string())?,
            center: self
                .center
                .ok_or_else(|| "houses.dat missing Center".to_string())?,
            fields: self.fields,
        })
    }
}

#[derive(Default)]
struct OwnerBuilder {
    id: Option<u32>,
    owner: Option<u32>,
    last_transition: Option<u64>,
    paid_until: Option<u64>,
    guests: Vec<String>,
    subowners: Vec<String>,
}

impl OwnerBuilder {
    fn build(self) -> Result<HouseOwner, String> {
        Ok(HouseOwner {
            id: self.id.ok_or_else(|| "owners.dat missing ID".to_string())?,
            owner: self
                .owner
                .ok_or_else(|| "owners.dat missing Owner".to_string())?,
            last_transition: self
                .last_transition
                .ok_or_else(|| "owners.dat missing LastTransition".to_string())?,
            paid_until: self
                .paid_until
                .ok_or_else(|| "owners.dat missing PaidUntil".to_string())?,
            guests: self.guests,
            subowners: self.subowners,
        })
    }
}

fn parse_u16(raw: &str, line_no: usize, label: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("{} invalid u16 at line {}", label, line_no))
}

fn parse_u32(raw: &str, line_no: usize, label: &str) -> Result<u32, String> {
    raw.parse::<u32>()
        .map_err(|_| format!("{} invalid u32 at line {}", label, line_no))
}

fn parse_u64(raw: &str, line_no: usize, label: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|_| format!("{} invalid u64 at line {}", label, line_no))
}

fn parse_bool(raw: &str, line_no: usize, label: &str) -> Result<bool, String> {
    match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{} invalid bool at line {}", label, line_no)),
    }
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

fn parse_quoted(raw: &str, line_no: usize, label: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        Ok(raw[1..raw.len() - 1].to_string())
    } else {
        Err(format!("{} expected quoted string at line {}", label, line_no))
    }
}

fn parse_position_value(raw: &str, line_no: usize, label: &str) -> Result<Position, String> {
    let raw = raw.trim();
    if !raw.starts_with('[') || !raw.ends_with(']') {
        return Err(format!(
            "{} expected [x,y,z] at line {}",
            label, line_no
        ));
    }
    let inner = &raw[1..raw.len() - 1];
    parse_position(inner, line_no, label)
}

fn parse_position(raw: &str, line_no: usize, label: &str) -> Result<Position, String> {
    let mut parts = raw.split(',').map(|part| part.trim());
    let x = parts
        .next()
        .ok_or_else(|| format!("{} missing x at line {}", label, line_no))?
        .parse::<u16>()
        .map_err(|_| format!("{} invalid x at line {}", label, line_no))?;
    let y = parts
        .next()
        .ok_or_else(|| format!("{} missing y at line {}", label, line_no))?
        .parse::<u16>()
        .map_err(|_| format!("{} invalid y at line {}", label, line_no))?;
    let z = parts
        .next()
        .ok_or_else(|| format!("{} missing z at line {}", label, line_no))?
        .parse::<u8>()
        .map_err(|_| format!("{} invalid z at line {}", label, line_no))?;
    if parts.next().is_some() {
        return Err(format!("{} extra values at line {}", label, line_no));
    }
    Ok(Position { x, y, z })
}

fn parse_positions_list(raw: &str, line_no: usize) -> Result<Vec<Position>, String> {
    let raw = raw.trim();
    if !raw.starts_with('{') || !raw.ends_with('}') {
        return Err(format!("Fields expected braces at line {}", line_no));
    }
    let inner = &raw[1..raw.len() - 1];
    let mut positions = Vec::new();
    let mut in_bracket = false;
    let mut buffer = String::new();

    for ch in inner.chars() {
        match ch {
            '[' => {
                in_bracket = true;
                buffer.clear();
            }
            ']' => {
                if !in_bracket {
                    continue;
                }
                let position = parse_position(&buffer, line_no, "Fields")?;
                positions.push(position);
                in_bracket = false;
            }
            _ => {
                if in_bracket {
                    buffer.push(ch);
                }
            }
        }
    }

    if in_bracket {
        return Err(format!("Fields missing closing bracket at line {}", line_no));
    }

    Ok(positions)
}

fn parse_string_list(raw: &str, line_no: usize, label: &str) -> Result<Vec<String>, String> {
    let raw = raw.trim();
    if raw == "{}" {
        return Ok(Vec::new());
    }
    if !raw.starts_with('{') || !raw.ends_with('}') {
        return Err(format!("{} expected braces at line {}", label, line_no));
    }
    let inner = &raw[1..raw.len() - 1].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    let parts = split_top_level(inner);
    let mut values = Vec::new();
    for part in parts {
        if part.is_empty() {
            continue;
        }
        values.push(parse_quoted(part, line_no, label)?);
    }
    Ok(values)
}

fn serialize_string_list(values: &[String]) -> Result<String, String> {
    if values.is_empty() {
        return Ok("{}".to_string());
    }
    let mut out = String::from("{");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&format_quoted(value)?);
    }
    out.push('}');
    Ok(out)
}

fn format_quoted(value: &str) -> Result<String, String> {
    if value.contains('"') {
        return Err("owners.dat names cannot contain '\"'".to_string());
    }
    if value.contains('\n') || value.contains('\r') {
        return Err("owners.dat names cannot contain newlines".to_string());
    }
    Ok(format!("\"{}\"", value))
}

fn split_top_level(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut in_quotes = false;
    let mut start = 0usize;

    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                parts.push(input[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }

    if start <= input.len() {
        parts.push(input[start..].trim());
    }

    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn house_owner_roundtrip() {
        let owners = vec![
            HouseOwner {
                id: 1,
                owner: 42,
                last_transition: 100,
                paid_until: 200,
                guests: vec!["Guest One".to_string(), "Guest Two".to_string()],
                subowners: vec![],
            },
            HouseOwner {
                id: 2,
                owner: 84,
                last_transition: 300,
                paid_until: 400,
                guests: vec![],
                subowners: vec!["Sub One".to_string()],
            },
        ];

        let serialized = serialize_house_owners(&owners).expect("serialize");
        let parsed = parse_house_owners(&serialized).expect("parse");
        assert_eq!(parsed, owners);
    }
}
