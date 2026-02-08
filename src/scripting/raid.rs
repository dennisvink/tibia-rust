use crate::scripting::value::{parse_value, ScriptValue};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaidPosition {
    pub x: u16,
    pub y: u16,
    pub z: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaidCount {
    pub min: i64,
    pub max: i64,
}

#[derive(Debug, Default)]
pub struct RaidSpawn {
    pub delay: Option<i64>,
    pub position: Option<RaidPosition>,
    pub spread: Option<i64>,
    pub race: Option<i64>,
    pub count: Option<RaidCount>,
    pub message: Option<String>,
    pub fields: Vec<(String, ScriptValue)>,
}

#[derive(Debug, Default)]
pub struct RaidScript {
    pub raid_type: Option<String>,
    pub interval: Option<i64>,
    pub spawns: Vec<RaidSpawn>,
    pub fields: Vec<(String, ScriptValue)>,
}

pub fn load_raid_script(path: &Path) -> Result<RaidScript, String> {
    let bytes = std::fs::read(path).map_err(|err| {
        format!(
            "failed to read raid script {}: {}",
            path.display(),
            err
        )
    })?;
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            let bytes = err.into_bytes();
            eprintln!(
                "tibia: raid script contained invalid UTF-8; decoding latin-1: {}",
                path.display()
            );
            bytes.iter().map(|b| *b as char).collect()
        }
    };
    parse_raid_script(&content).map_err(|err| format!("raid script {}: {}", path.display(), err))
}

pub fn parse_raid_script(content: &str) -> Result<RaidScript, String> {
    let mut script = RaidScript::default();
    let mut current = RaidSpawn::default();

    for (line_no, line) in coalesce_lines(content).into_iter().enumerate() {
        let line_no = line_no + 1;
        for assignment in split_assignments(&line) {
            let Some((key, value)) = assignment.split_once('=') else {
                continue;
            };
            let key = key.trim();
            let value = value.trim();
            let parsed = parse_value(value)
                .map_err(|err| format!("line {} invalid value: {}", line_no, err))?;

            if key.eq_ignore_ascii_case("Type") {
                script.raid_type = Some(value_to_string(&parsed));
                script.fields.push((key.to_string(), parsed));
                continue;
            }
            if key.eq_ignore_ascii_case("Interval") {
                let interval = parse_number(&parsed)
                    .ok_or_else(|| format!("line {} invalid Interval", line_no))?;
                script.interval = Some(interval);
                script.fields.push((key.to_string(), parsed));
                continue;
            }

            if key.eq_ignore_ascii_case("Delay") && !spawn_is_empty(&current) {
                script.spawns.push(current);
                current = RaidSpawn::default();
            }

            apply_spawn_field(&mut current, key, parsed, line_no)?;
        }
    }

    if !spawn_is_empty(&current) {
        script.spawns.push(current);
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

fn spawn_is_empty(spawn: &RaidSpawn) -> bool {
    spawn.delay.is_none()
        && spawn.position.is_none()
        && spawn.spread.is_none()
        && spawn.race.is_none()
        && spawn.count.is_none()
        && spawn.message.is_none()
        && spawn.fields.is_empty()
}

fn apply_spawn_field(
    spawn: &mut RaidSpawn,
    key: &str,
    value: ScriptValue,
    line_no: usize,
) -> Result<(), String> {
    if key.eq_ignore_ascii_case("Delay") {
        let delay = parse_number(&value).ok_or_else(|| {
            format!("line {} invalid Delay value: {:?}", line_no, value)
        })?;
        spawn.delay = Some(delay);
    } else if key.eq_ignore_ascii_case("Position") {
        let position = parse_position(&value).ok_or_else(|| {
            format!("line {} invalid Position value: {:?}", line_no, value)
        })?;
        spawn.position = Some(position);
    } else if key.eq_ignore_ascii_case("Spread") {
        let spread = parse_number(&value).ok_or_else(|| {
            format!("line {} invalid Spread value: {:?}", line_no, value)
        })?;
        spawn.spread = Some(spread);
    } else if key.eq_ignore_ascii_case("Race") {
        let race = parse_number(&value).ok_or_else(|| {
            format!("line {} invalid Race value: {:?}", line_no, value)
        })?;
        spawn.race = Some(race);
    } else if key.eq_ignore_ascii_case("Count") {
        let count = parse_count(&value).ok_or_else(|| {
            format!("line {} invalid Count value: {:?}", line_no, value)
        })?;
        spawn.count = Some(count);
    } else if key.eq_ignore_ascii_case("Message") {
        spawn.message = Some(value_to_string(&value));
    }

    spawn.fields.push((key.to_string(), value));
    Ok(())
}

fn parse_position(value: &ScriptValue) -> Option<RaidPosition> {
    let values = match value {
        ScriptValue::List(values) | ScriptValue::Tuple(values) => values,
        _ => return None,
    };
    if values.len() != 3 {
        return None;
    }
    let x = parse_number(&values[0])?;
    let y = parse_number(&values[1])?;
    let z = parse_number(&values[2])?;
    if x < 0 || x > i64::from(u16::MAX) {
        return None;
    }
    if y < 0 || y > i64::from(u16::MAX) {
        return None;
    }
    if z < 0 || z > i64::from(u8::MAX) {
        return None;
    }
    Some(RaidPosition {
        x: x as u16,
        y: y as u16,
        z: z as u8,
    })
}

fn parse_count(value: &ScriptValue) -> Option<RaidCount> {
    let values = match value {
        ScriptValue::List(values) | ScriptValue::Tuple(values) => values,
        _ => return None,
    };
    if values.len() != 2 {
        return None;
    }
    let min = parse_number(&values[0])?;
    let max = parse_number(&values[1])?;
    Some(RaidCount { min, max })
}

fn parse_number(value: &ScriptValue) -> Option<i64> {
    match value {
        ScriptValue::Number(number) => Some(*number),
        ScriptValue::Ident(ident) => ident.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_to_string(value: &ScriptValue) -> String {
    match value {
        ScriptValue::String(value) => value.clone(),
        ScriptValue::Ident(value) => value.clone(),
        ScriptValue::Number(value) => value.to_string(),
        ScriptValue::Tuple(values) | ScriptValue::List(values) => values
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(","),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_raid_script_extracts_spawns() {
        let input = r#"
Type = SmallRaid
Interval = 120
Delay = 1
Position = [32681,31598,7]
Spread = 6
Race = 105
Count = (9,18)
Message = "Badgers!"
Delay = 2
Position = [32671,31623,7]
Spread = 6
Race = 105
Count = (9,18)
"#;

        let script = parse_raid_script(input).expect("parse");
        assert_eq!(script.raid_type.as_deref(), Some("SmallRaid"));
        assert_eq!(script.interval, Some(120));
        assert_eq!(script.spawns.len(), 2);
        assert_eq!(
            script.spawns[0].position,
            Some(RaidPosition {
                x: 32681,
                y: 31598,
                z: 7
            })
        );
        assert_eq!(
            script.spawns[0].count,
            Some(RaidCount { min: 9, max: 18 })
        );
        assert_eq!(script.spawns[0].message.as_deref(), Some("Badgers!"));
    }
}
