use std::fs;
use std::path::Path;

#[derive(Debug, Default)]
pub struct AssetSummary {
    pub dat_files: usize,
    pub map_files: usize,
    pub npc_files: usize,
    pub mon_files: usize,
    pub save_files: usize,
}

pub fn scan(root: &Path) -> Result<AssetSummary, String> {
    Ok(AssetSummary {
        dat_files: count_dir(root.join("dat"))?,
        map_files: count_dir(root.join("map"))?,
        npc_files: count_dir(root.join("npc"))?,
        mon_files: count_dir(root.join("mon"))?,
        save_files: count_dir(root.join("save"))?,
    })
}

fn count_dir(path: impl AsRef<Path>) -> Result<usize, String> {
    let path = path.as_ref();
    let entries = fs::read_dir(path)
        .map_err(|err| format!("failed to read {}: {}", path.display(), err))?;

    let mut count = 0usize;
    for entry in entries {
        if entry.is_ok() {
            count += 1;
        }
    }

    Ok(count)
}
