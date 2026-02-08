use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemDat {
    pub objects: Option<u32>,
    pub cache_size: Option<u32>,
}

impl MemDat {
    pub fn load(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|err| format!("failed to read mem.dat {}: {}", path.display(), err))?;
        parse_mem_dat(&content)
    }
}

fn parse_mem_dat(content: &str) -> Result<MemDat, String> {
    let mut mem = MemDat {
        objects: None,
        cache_size: None,
    };

    for (line_no, raw_line) in content.lines().enumerate() {
        let line_no = line_no + 1;
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((before, _)) = line.split_once('#') {
            line = before.trim();
            if line.is_empty() {
                continue;
            }
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("mem.dat line {} missing '='", line_no))?;
        let key = key.trim();
        let value = value.trim();
        let parsed = value
            .parse::<u32>()
            .map_err(|_| format!("mem.dat line {} invalid number", line_no))?;
        match key {
            "Objects" => mem.objects = Some(parsed),
            "CacheSize" => mem.cache_size = Some(parsed),
            _ => {}
        }
    }

    Ok(mem)
}
