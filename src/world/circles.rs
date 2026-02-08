use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Circles {
    pub width: usize,
    pub height: usize,
    pub max_radius: u8,
    pub cells: Vec<u8>,
}

impl Circles {
    pub fn load(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|err| format!("failed to read circles.dat {}: {}", path.display(), err))?;
        parse_circles(&content)
    }

    pub fn cell(&self, x: usize, y: usize) -> Option<u8> {
        if x >= self.width || y >= self.height {
            return None;
        }
        self.cells.get(y * self.width + x).copied()
    }
}

fn parse_circles(content: &str) -> Result<Circles, String> {
    let mut lines = content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty());

    let header = lines
        .next()
        .ok_or_else(|| "circles.dat missing header".to_string())?;

    let mut header_parts = header.split_whitespace();
    let width = header_parts
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| "circles.dat header missing width".to_string())?;
    let height = header_parts
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| "circles.dat header missing height".to_string())?;
    let max_radius = header_parts
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or_else(|| "circles.dat header missing max radius".to_string())?;
    if header_parts.next().is_some() {
        return Err("circles.dat header has extra values".to_string());
    }

    let mut cells = Vec::with_capacity(width.saturating_mul(height));
    for (line_index, line) in lines.enumerate() {
        let mut row = Vec::new();
        for part in line.split_whitespace() {
            let value = part.parse::<u8>().map_err(|_| {
                format!("circles.dat invalid value at line {}", line_index + 2)
            })?;
            row.push(value);
        }
        if row.is_empty() {
            continue;
        }
        if row.len() != width {
            return Err(format!(
                "circles.dat expected {} columns, got {} at line {}",
                width,
                row.len(),
                line_index + 2
            ));
        }
        cells.extend_from_slice(&row);
    }

    if cells.len() != width * height {
        return Err(format!(
            "circles.dat expected {} values, got {}",
            width * height,
            cells.len()
        ));
    }

    Ok(Circles {
        width,
        height,
        max_radius,
        cells,
    })
}
