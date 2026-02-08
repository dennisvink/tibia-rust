use crate::entities::item::ItemTypeId;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorChange {
    Up,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAttribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectType {
    pub id: ItemTypeId,
    pub name: String,
    pub flags: Vec<String>,
    pub attributes: Vec<ObjectAttribute>,
}

impl ObjectType {
    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags
            .iter()
            .any(|value| value.eq_ignore_ascii_case(flag))
    }

    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|attr| attr.key.eq_ignore_ascii_case(key))
            .map(|attr| attr.value.as_str())
    }

    pub fn attribute_u16(&self, key: &str) -> Option<u16> {
        self.attribute(key)
            .and_then(|value| value.trim().parse::<u16>().ok())
    }

    pub fn body_position(&self) -> Option<u8> {
        self.attribute_u16("BodyPosition")
            .and_then(|value| u8::try_from(value).ok())
    }

    pub fn blocks_movement(&self) -> bool {
        self.has_flag("Unpass")
    }

    pub fn is_movable(&self) -> bool {
        self.has_flag("Take")
    }

    pub fn ground_speed(&self) -> Option<u16> {
        self.attribute_u16("Waypoints")
            .or_else(|| self.attribute_u16("Speed"))
            .or_else(|| self.attribute_u16("GroundSpeed"))
    }

    pub fn floor_change_hint(&self) -> Option<FloorChange> {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectTypeIndex {
    types: HashMap<ItemTypeId, ObjectType>,
}

impl ObjectTypeIndex {
    pub fn get(&self, id: ItemTypeId) -> Option<&ObjectType> {
        self.types.get(&id)
    }

    pub fn insert(&mut self, entry: ObjectType) -> Result<(), String> {
        if self.types.contains_key(&entry.id) {
            return Err(format!("object type {:?} already exists", entry.id));
        }
        self.types.insert(entry.id, entry);
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ItemTypeId, &ObjectType)> {
        self.types.iter()
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }
}

pub fn load_object_types(path: &Path) -> Result<ObjectTypeIndex, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read objects.srv {}: {}", path.display(), err))?;
    parse_object_types(&content)
}

fn parse_object_types(content: &str) -> Result<ObjectTypeIndex, String> {
    let mut index = ObjectTypeIndex::default();
    let mut current: Option<ObjectType> = None;

    for (line_no, raw_line) in content.lines().enumerate() {
        let line_no = line_no + 1;
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(trimmed) = strip_comment(line) {
            line = trimmed.trim();
        }
        if line.is_empty() {
            continue;
        }

        if line.starts_with("TypeID") {
            if let Some(entry) = current.take() {
                insert_type(&mut index, entry, line_no)?;
            }
            let id = parse_type_id(line, line_no)?;
            current = Some(ObjectType {
                id: ItemTypeId(id),
                name: String::new(),
                flags: Vec::new(),
                attributes: Vec::new(),
            });
            continue;
        }

        let Some(entry) = current.as_mut() else {
            return Err(format!(
                "objects.srv line {} has entry before TypeID",
                line_no
            ));
        };

        let (key, value) = parse_assignment(line, line_no)?;
        match key {
            "Name" => {
                entry.name = parse_quoted_string(value, line_no)?;
            }
            "Flags" => {
                entry.flags = parse_braced_list(value, line_no)?;
            }
            "Attributes" => {
                entry.attributes = parse_attributes(value, line_no)?;
            }
            _ => {}
        }
    }

    if let Some(entry) = current.take() {
        insert_type(&mut index, entry, content.lines().count())?;
    }

    Ok(index)
}

fn strip_comment(line: &str) -> Option<&str> {
    line.find('#').map(|idx| &line[..idx]).or(Some(line))
}

fn parse_type_id(line: &str, line_no: usize) -> Result<u16, String> {
    let (_, value) = parse_assignment(line, line_no)?;
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("objects.srv line {} invalid TypeID", line_no))
}

fn parse_assignment<'a>(line: &'a str, line_no: usize) -> Result<(&'a str, &'a str), String> {
    let (key, value) = line
        .split_once('=')
        .ok_or_else(|| format!("objects.srv line {} missing '='", line_no))?;
    Ok((key.trim(), value.trim()))
}

fn parse_quoted_string(value: &str, line_no: usize) -> Result<String, String> {
    let value = value.trim();
    if !value.starts_with('"') {
        return Ok(value.to_string());
    }
    let rest = &value[1..];
    let end = rest.find('"').ok_or_else(|| {
        format!(
            "objects.srv line {} missing closing quote in Name",
            line_no
        )
    })?;
    Ok(rest[..end].to_string())
}

fn parse_braced_list(value: &str, line_no: usize) -> Result<Vec<String>, String> {
    let start = value
        .find('{')
        .ok_or_else(|| format!("objects.srv line {} missing '{{'", line_no))?;
    let end = value
        .rfind('}')
        .ok_or_else(|| format!("objects.srv line {} missing '}}'", line_no))?;
    if end <= start {
        return Err(format!("objects.srv line {} invalid braces", line_no));
    }
    let inner = value[start + 1..end].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(inner
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect())
}

fn parse_attributes(value: &str, line_no: usize) -> Result<Vec<ObjectAttribute>, String> {
    let entries = parse_braced_list(value, line_no)?;
    let mut attributes = Vec::new();
    for entry in entries {
        let mut parts = entry.splitn(2, '=');
        let key = parts.next().unwrap_or("").trim();
        let value = parts.next().unwrap_or("").trim();
        if key.is_empty() {
            continue;
        }
        attributes.push(ObjectAttribute {
            key: key.to_string(),
            value: value.to_string(),
        });
    }
    Ok(attributes)
}

fn insert_type(
    index: &mut ObjectTypeIndex,
    entry: ObjectType,
    line_no: usize,
) -> Result<(), String> {
    if index.types.contains_key(&entry.id) {
        return Err(format!(
            "objects.srv line {} duplicate TypeID {}",
            line_no, entry.id.0
        ));
    }
    index.types.insert(entry.id, entry);
    Ok(())
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

        fn gen_ascii(&mut self, len: usize) -> String {
            let mut out = String::with_capacity(len);
            for _ in 0..len {
                let byte = (self.next_u8() % 95) + 0x20;
                out.push(byte as char);
            }
            out
        }
    }

    #[test]
    fn fuzz_parse_object_types() {
        let mut rng = FuzzRng::new(0xa11c_e55e_0000_0001);
        let mut content = String::new();
        for i in 0..200 {
            if i % 15 == 0 {
                content.push_str("TypeID = 100\nName = \"Test\"\nFlags = {Unpass}\n");
                continue;
            }
            let len = (rng.next_u32() % 120) as usize;
            content.push_str(&rng.gen_ascii(len));
            content.push('\n');
        }
        let _ = parse_object_types(&content);
    }
}
