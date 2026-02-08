use crate::world::position::Position;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterHome {
    pub race_number: i64,
    pub position: Position,
    pub radius: u16,
    pub amount: u16,
    pub regen: u16,
    pub act_monsters: u16,
    pub timer: i32,
}

pub fn load_monster_homes(path: &Path) -> Result<Vec<MonsterHome>, String> {
    let bytes = fs::read(path)
        .map_err(|err| format!("failed to read {}: {}", path.display(), err))?;
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(err) => {
            let bytes = err.into_bytes();
            eprintln!(
                "tibia: monster homes contained invalid UTF-8; decoding lossy: {}",
                path.display()
            );
            String::from_utf8_lossy(&bytes).into_owned()
        }
    };
    let mut homes = Vec::new();
    for (line_no, raw_line) in content.lines().enumerate() {
        let line_no = line_no + 1;
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        if line == "0" {
            break;
        }
        if !line
            .chars()
            .next()
            .map(|ch| ch.is_ascii_digit() || ch == '-')
            .unwrap_or(false)
        {
            continue;
        }
        let home = parse_monster_home(line, line_no)?;
        homes.push(home);
    }
    Ok(homes)
}

fn strip_comment(line: &str) -> &str {
    if let Some(idx) = line.find('#') {
        &line[..idx]
    } else {
        line
    }
}

fn parse_monster_home(line: &str, line_no: usize) -> Result<MonsterHome, String> {
    let mut parts = line.split_whitespace();
    let race_number = parse_i64(parts.next(), "race", line_no)?;
    let x = parse_u16(parts.next(), "x", line_no)?;
    let y = parse_u16(parts.next(), "y", line_no)?;
    let z = parse_u8(parts.next(), "z", line_no)?;
    let radius = parse_u16(parts.next(), "radius", line_no)?;
    let amount = parse_u16(parts.next(), "amount", line_no)?;
    let regen = parse_u16(parts.next(), "regen", line_no)?;
    Ok(MonsterHome {
        race_number,
        position: Position { x, y, z },
        radius,
        amount,
        regen,
        act_monsters: 0,
        timer: 0,
    })
}

fn parse_i64(value: Option<&str>, label: &str, line_no: usize) -> Result<i64, String> {
    let value = value.ok_or_else(|| format!("monster.db line {} missing {}", line_no, label))?;
    value
        .parse::<i64>()
        .map_err(|_| format!("monster.db line {} invalid {}", line_no, label))
}

fn parse_u16(value: Option<&str>, label: &str, line_no: usize) -> Result<u16, String> {
    let value = value.ok_or_else(|| format!("monster.db line {} missing {}", line_no, label))?;
    value
        .parse::<u16>()
        .map_err(|_| format!("monster.db line {} invalid {}", line_no, label))
}

fn parse_u8(value: Option<&str>, label: &str, line_no: usize) -> Result<u8, String> {
    let value = value.ok_or_else(|| format!("monster.db line {} missing {}", line_no, label))?;
    value
        .parse::<u8>()
        .map_err(|_| format!("monster.db line {} invalid {}", line_no, label))
}
