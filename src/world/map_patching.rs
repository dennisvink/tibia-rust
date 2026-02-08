use crate::world::position::Position;
use crate::entities::item::ItemStack;
use std::collections::HashSet;
use std::path::PathBuf;

/// Coordinates for a map sector
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SectorCoord {
    pub x: u16,
    pub y: u16,
    pub z: u8,
}

impl SectorCoord {
    pub fn new(x: u16, y: u16, z: u8) -> Self {
        SectorCoord { x, y, z }
    }
}

/// Tile flags for map sectors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TileFlags {
    pub refresh: bool,
    pub no_logout: bool,
    pub protection_zone: bool,
}

impl TileFlags {
    pub fn is_empty(&self) -> bool {
        !self.refresh && !self.no_logout && !self.protection_zone
    }
}

/// A single tile in a sector
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tile {
    pub flags: TileFlags,
    pub items: Vec<ItemStack>,
}

impl Tile {
    pub fn empty() -> Self {
        Tile {
            flags: TileFlags::default(),
            items: Vec::new(),
        }
    }
    
    pub fn is_empty(&self) -> bool {
        self.flags.is_empty() && self.items.is_empty()
    }
    
    pub fn from_patch(flags: TileFlags, items: Vec<ItemStack>) -> Self {
        Tile { flags, items }
    }
}

impl Default for Tile {
    fn default() -> Self {
        Self::empty()
    }
}

/// Sector data containing 32x32 tiles
#[derive(Debug, Clone)]
pub struct SectorData {
    pub tiles: Vec<Tile>,
}

impl SectorData {
    pub fn new() -> Self {
        SectorData {
            tiles: vec![Tile::empty(); 32 * 32],
        }
    }
    
    pub fn get_tile(&self, x: u8, y: u8) -> Option<&Tile> {
        if x >= 32 || y >= 32 {
            return None;
        }
        self.tiles.get((y as usize) * 32 + (x as usize))
    }
    
    pub fn get_tile_mut(&mut self, x: u8, y: u8) -> Option<&mut Tile> {
        if x >= 32 || y >= 32 {
            return None;
        }
        self.tiles.get_mut((y as usize) * 32 + (x as usize))
    }
}

impl Default for SectorData {
    fn default() -> Self {
        Self::new()
    }
}

/// Patch instruction for updating a single tile
#[derive(Debug, Clone)]
pub struct PatchInstruction {
    pub offset_x: u8,
    pub offset_y: u8,
    pub flags: TileFlags,
    pub items: Vec<ItemStack>,
}

/// House system for detecting and preserving house tiles
#[derive(Debug, Clone)]
pub struct HouseSystem {
    house_tiles: HashSet<Position>,
}

impl HouseSystem {
    pub fn new() -> Self {
        HouseSystem {
            house_tiles: HashSet::new(),
        }
    }
    
    /// Add a house tile position
    pub fn add_house_tile(&mut self, pos: Position) {
        self.house_tiles.insert(pos);
    }
    
    /// Remove a house tile position
    pub fn remove_house_tile(&mut self, pos: Position) {
        self.house_tiles.remove(&pos);
    }
    
    /// Clear all house tiles
    pub fn clear(&mut self) {
        self.house_tiles.clear();
    }
    
    /// Check if position is a house field
    pub fn is_house_field(&self, pos: Position) -> bool {
        self.house_tiles.contains(&pos)
    }
    
    /// Check if position is adjacent to a house tile
    pub fn is_adjacent_to_house(&self, pos: Position) -> bool {
        let offsets = [
            (0.0, 1.0), (0.0, -1.0), (1.0, 0.0), (-1.0, 0.0),
            (1.0, 1.0), (1.0, -1.0), (-1.0, 1.0), (-1.0, -1.0),
        ];
        
        for (dx, dy) in &offsets {
            let new_x = i32::from(pos.x) + *dx as i32;
            let new_y = i32::from(pos.y) + *dy as i32;
            
            if new_x < 0 || new_x > i32::from(u16::MAX) {
                continue;
            }
            if new_y < 0 || new_y > i32::from(u16::MAX) {
                continue;
            }
            
            let adjacent = Position {
                x: new_x as u16,
                y: new_y as u16,
                z: pos.z,
            };
            
            if self.house_tiles.contains(&adjacent) {
                return true;
            }
        }
        
        false
    }
    
    /// Clean house field (remove from house system)
    pub fn clean_house_field(&mut self, pos: Position) {
        self.house_tiles.remove(&pos);
    }
}

impl Default for HouseSystem {
    fn default() -> Self {
        Self::new()
    }
}

/// Map patcher for updating sectors while preserving house data
#[derive(Debug, Clone)]
pub struct MapPatcher {
    map_path: PathBuf,
    house_system: HouseSystem,
}

impl MapPatcher {
    pub fn new(map_path: PathBuf) -> Self {
        MapPatcher {
            map_path,
            house_system: HouseSystem::new(),
        }
    }
    
    pub fn with_house_system(mut self, house_system: HouseSystem) -> Self {
        self.house_system = house_system;
        self
    }
    
    /// Get a reference to the house system
    pub fn house_system(&self) -> &HouseSystem {
        &self.house_system
    }
    
    /// Get a mutable reference to the house system
    pub fn house_system_mut(&mut self) -> &mut HouseSystem {
        &mut self.house_system
    }
    
    /// Patch a sector with given instructions
    pub fn patch_sector(
        &mut self,
        sector: SectorCoord,
        patches: Vec<PatchInstruction>,
        full_sector: bool,
        save_houses: bool,
    ) -> Result<(), PatchError> {
        // Step 1: Load current sector data
        let mut current_data = self.load_sector(sector)?;
        
        // Step 2: Apply patches (skipping house fields if save_houses)
        let mut patched_fields: HashSet<(u8, u8)> = HashSet::new();
        
        for patch in &patches {
            let pos = (patch.offset_x, patch.offset_y);
            
            // Check if field belongs to house
            let world_pos = Position {
                x: (sector.x as i32 * 32 + patch.offset_x as i32) as u16,
                y: (sector.y as i32 * 32 + patch.offset_y as i32) as u16,
                z: sector.z,
            };
            
            let is_house_field = self.house_system.is_house_field(world_pos)
                || self.house_system.is_adjacent_to_house(world_pos);
            
            if is_house_field && save_houses {
                continue; // Skip house fields
            }
            
            // Clean house data if not saving
            if is_house_field {
                self.house_system.clean_house_field(world_pos);
            }
            
            // Apply patch
            if let Some(tile) = current_data.get_tile_mut(patch.offset_x, patch.offset_y) {
                *tile = Tile::from_patch(patch.flags.clone(), patch.items.clone());
                patched_fields.insert(pos);
            }
        }
        
        // Step 3: Full sector patch (clear non-patched fields)
        if full_sector {
            for y in 0u8..32 {
                for x in 0u8..32 {
                    if patched_fields.contains(&(x, y)) {
                        continue;
                    }
                    
                    let world_pos = Position {
                        x: (sector.x as i32 * 32 + x as i32) as u16,
                        y: (sector.y as i32 * 32 + y as i32) as u16,
                        z: sector.z,
                    };
                    
                    let is_house_field = self.house_system.is_house_field(world_pos);
                    
                    if is_house_field && save_houses {
                        continue;
                    }
                    
                    // Clear tile
                    if let Some(tile) = current_data.get_tile_mut(x, y) {
                        *tile = Tile::empty();
                    }
                }
            }
        }
        
        // Step 4: Save patched sector
        self.save_sector(sector, &current_data)?;
        
        Ok(())
    }
    
    /// Load a sector from disk
    fn load_sector(&self, sector: SectorCoord) -> Result<SectorData, PatchError> {
        let path = self.map_path.join(format!(
            "{:04x}-{:04x}-{:02x}.sec", sector.x, sector.y, sector.z
        ));
        
        let mut tiles = vec![Tile::empty(); 32 * 32];
        
        // Try to load existing sector file
        if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| PatchError::ReadError(path.clone(), e.to_string()))?;
            
            self.parse_sector_content(&content, &mut tiles)?;
        }
        
        Ok(SectorData { tiles })
    }
    
    /// Parse sector file content
    fn parse_sector_content(&self, content: &str, tiles: &mut [Tile]) -> Result<(), PatchError> {
        for line in content.lines() {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            
            // Parse: "X-Y: Flags, Content={...}"
            if let Some((coord_part, data_part)) = line.split_once(':') {
                let (x, y) = self.parse_coordinate(coord_part)?;
                let (flags, items) = self.parse_tile_data(data_part)?;
                
                if let Some(tile) = tiles.get_mut((y as usize) * 32 + (x as usize)) {
                    *tile = Tile::from_patch(flags, items);
                }
            }
        }
        
        Ok(())
    }
    
    /// Parse coordinate "X-Y"
    fn parse_coordinate(&self, coord: &str) -> Result<(u8, u8), PatchError> {
        let parts: Vec<&str> = coord.trim().split('-').collect();
        if parts.len() != 2 {
            return Err(PatchError::ParseError(format!(
                "Invalid coordinate format: {}", coord
            )));
        }
        
        let x: u8 = parts[0]
            .trim()
            .parse()
            .map_err(|_| PatchError::ParseError(format!("Invalid X coordinate: {}", parts[0])))?;
        let y: u8 = parts[1]
            .trim()
            .parse()
            .map_err(|_| PatchError::ParseError(format!("Invalid Y coordinate: {}", parts[1])))?;
        
        if x >= 32 || y >= 32 {
            return Err(PatchError::ParseError(format!(
                "Coordinate out of range: {},{} (must be < 32)", x, y
            )));
        }
        
        Ok((x, y))
    }
    
    /// Parse tile data (flags and items)
    fn parse_tile_data(&self, data: &str) -> Result<(TileFlags, Vec<ItemStack>), PatchError> {
        let mut flags = TileFlags::default();
        let mut items = Vec::new();
        
        let parts: Vec<&str> = data.split(',').map(|s| s.trim()).collect();
        
        for part in &parts {
            if part.eq_ignore_ascii_case("Refresh") {
                flags.refresh = true;
            } else if part.eq_ignore_ascii_case("NoLogout") {
                flags.no_logout = true;
            } else if part.eq_ignore_ascii_case("ProtectionZone") {
                flags.protection_zone = true;
            } else if part.starts_with("Content=") {
                // Parse content - simplified for now
                // Full implementation would parse nested structure
                let content = part["Content=".len()..].trim();
                if !content.starts_with('{') || !content.ends_with('}') {
                    continue;
                }
                // Parse items from content string
                items = self.parse_items_content(&content[1..content.len()-1])?;
            }
        }
        
        Ok((flags, items))
    }
    
    /// Parse items from content string
    fn parse_items_content(&self, content: &str) -> Result<Vec<ItemStack>, PatchError> {
        // Simplified implementation - returns empty for now
        // Full implementation would parse item types and counts
        let mut items = Vec::new();
        
        for item_str in content.split(',') {
            let item_str = item_str.trim();
            if item_str.is_empty() {
                continue;
            }
            
            // Parse item ID from format "ID" or "ID(Amount)"
            let parts: Vec<&str> = item_str.split('(').collect();
            if parts.is_empty() {
                continue;
            }
            
            let item_id: u16 = parts[0]
                .trim()
                .parse()
                .map_err(|_| PatchError::ParseError(format!("Invalid item ID: {}", parts[0])))?;
            
            let amount = if parts.len() > 1 {
                parts[1]
                    .trim_end_matches(')')
                    .trim()
                    .parse()
                    .unwrap_or(1)
            } else {
                1
            };
            
            items.push(ItemStack::new(
                crate::entities::item::ItemTypeId(item_id),
                amount,
            ));
        }
        
        Ok(items)
    }
    
    /// Save a sector to disk
    fn save_sector(&self, sector: SectorCoord, data: &SectorData) -> Result<(), PatchError> {
        let path = self.map_path.join(format!(
            "{:04x}-{:04x}-{:02x}.sec", sector.x, sector.y, sector.z
        ));
        
        let mut output = String::new();
        
        // Write header
        output.push_str("# Tibia - graphical Multi-User-Dungeon\n");
        output.push_str(&format!("# Data for sector {}/{}/{}\n",
            sector.x, sector.y, sector.z));
        output.push('\n');
        
        // Write tiles
        for y in 0u8..32 {
            for x in 0u8..32 {
                let tile = &data.tiles[(y as usize) * 32 + (x as usize)];
                
                if tile.is_empty() {
                    continue;
                }
                
                output.push_str(&format!("{}-{}: ", x, y));
                
                let mut flag_parts = Vec::new();
                if tile.flags.refresh {
                    flag_parts.push("Refresh");
                }
                if tile.flags.no_logout {
                    flag_parts.push("NoLogout");
                }
                if tile.flags.protection_zone {
                    flag_parts.push("ProtectionZone");
                }
                
                output.push_str(&flag_parts.join(", "));
                
                if !tile.items.is_empty() {
                    if !flag_parts.is_empty() {
                        output.push_str(", ");
                    }
                    output.push_str(&self.serialize_items(&tile.items));
                }
                
                output.push('\n');
            }
        }
        
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PatchError::WriteError(path.clone(), e.to_string()))?;
        }
        
        // Write to temp file first
        let temp_path = format!("{}.tmp", path.display());
        std::fs::write(&temp_path, output)
            .map_err(|e| PatchError::WriteError(path.clone(), e.to_string()))?;
        
        // Replace original file
        std::fs::rename(&temp_path, &path)
            .map_err(|e| PatchError::WriteError(path.clone(), e.to_string()))?;
        
        Ok(())
    }
    
    /// Serialize items to sector file format
    fn serialize_items(&self, items: &[ItemStack]) -> String {
        let mut content = String::from("Content={");
        
        let item_strs: Vec<String> = items
            .iter()
            .map(|item| {
                if item.count > 1 {
                    format!("{}({})", item.type_id.0, item.count)
                } else {
                    format!("{}", item.type_id.0)
                }
            })
            .collect();
        
        content.push_str(&item_strs.join(", "));
        content.push('}');
        
        content
    }
}

/// Errors that can occur during map patching
#[derive(Debug, Clone)]
pub enum PatchError {
    ReadError(PathBuf, String),
    WriteError(PathBuf, String),
    ParseError(String),
}

impl std::fmt::Display for PatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatchError::ReadError(path, msg) => {
                write!(f, "Failed to read {}: {}", path.display(), msg)
            }
            PatchError::WriteError(path, msg) => {
                write!(f, "Failed to write {}: {}", path.display(), msg)
            }
            PatchError::ParseError(msg) => {
                write!(f, "Parse error: {}", msg)
            }
        }
    }
}

impl std::error::Error for PatchError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::item::ItemTypeId;
    use std::fs;

    fn temp_patcher() -> MapPatcher {
        let temp_dir = std::env::temp_dir().join(format!("tibia-patcher-test-{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();
        
        MapPatcher::new(temp_dir.join("map"))
    }

    #[test]
    fn sector_coord_creation() {
        let coord = SectorCoord::new(100, 200, 7);
        assert_eq!(coord.x, 100);
        assert_eq!(coord.y, 200);
        assert_eq!(coord.z, 7);
    }

    #[test]
    fn tile_empty() {
        let tile = Tile::empty();
        assert!(tile.is_empty());
    }

    #[test]
    fn tile_with_items() {
        let tile = Tile::from_patch(
            TileFlags { refresh: true, no_logout: false, protection_zone: false },
            vec![ItemStack::new(ItemTypeId(100), 1)],
        );
        assert!(!tile.is_empty());
        assert!(tile.flags.refresh);
        assert_eq!(tile.items.len(), 1);
    }

    #[test]
    fn sector_data_get_tile() {
        let data = SectorData::new();
        assert!(data.get_tile(0, 0).is_some());
        assert!(data.get_tile(31, 31).is_some());
        assert!(data.get_tile(32, 0).is_none());
        assert!(data.get_tile(0, 32).is_none());
    }

    #[test]
    fn sector_data_set_tile() {
        let mut data = SectorData::new();
        let new_tile = Tile::from_patch(
            TileFlags::default(),
            vec![ItemStack::new(ItemTypeId(100), 1)],
        );
        
        *data.get_tile_mut(10, 15).unwrap() = new_tile.clone();
        
        let retrieved = data.get_tile(10, 15).unwrap();
        assert_eq!(retrieved.items.len(), 1);
    }

    #[test]
    fn house_system_add_remove() {
        let mut house = HouseSystem::new();
        let pos = Position { x: 100, y: 200, z: 7 };
        
        assert!(!house.is_house_field(pos));
        house.add_house_tile(pos);
        assert!(house.is_house_field(pos));
        house.remove_house_tile(pos);
        assert!(!house.is_house_field(pos));
    }

    #[test]
    fn house_system_adjacent() {
        let mut house = HouseSystem::new();
        let house_pos = Position { x: 100, y: 200, z: 7 };
        house.add_house_tile(house_pos);
        
        assert!(house.is_adjacent_to_house(Position { x: 100, y: 201, z: 7 }));
        assert!(house.is_adjacent_to_house(Position { x: 101, y: 200, z: 7 }));
        assert!(house.is_adjacent_to_house(Position { x: 101, y: 201, z: 7 }));
        assert!(!house.is_adjacent_to_house(Position { x: 102, y: 202, z: 7 }));
    }

    #[test]
    fn parse_coordinate_valid() {
        let patcher = temp_patcher();
        let (x, y) = patcher.parse_coordinate("10-15").unwrap();
        assert_eq!(x, 10);
        assert_eq!(y, 15);
    }

    #[test]
    fn parse_coordinate_invalid() {
        let patcher = temp_patcher();
        assert!(patcher.parse_coordinate("10").is_err());
        assert!(patcher.parse_coordinate("10-15-20").is_err());
        assert!(patcher.parse_coordinate("35-15").is_err());
    }

    #[test]
    fn serialize_items() {
        let patcher = temp_patcher();
        let items = vec![
            ItemStack::new(ItemTypeId(100), 1),
            ItemStack::new(ItemTypeId(200), 5),
        ];
        
        let serialized = patcher.serialize_items(&items);
        assert!(serialized.contains("100"));
        assert!(serialized.contains("200(5)"));
    }

    #[test]
    fn patch_sector_simple() {
        let patcher = temp_patcher();
        let sector = SectorCoord::new(100, 200, 7);
        
        let patches = vec![
            PatchInstruction {
                offset_x: 10,
                offset_y: 15,
                flags: TileFlags { refresh: true, no_logout: false, protection_zone: false },
                items: vec![ItemStack::new(ItemTypeId(100), 1)],
            },
        ];
        
        let result = patcher.patch_sector(sector, patches, false, false);
        assert!(result.is_ok());
    }

    #[test]
    fn patch_sector_with_house_preservation() {
        let mut patcher = temp_patcher();
        
        // Add house tile
        let house_pos = Position { x: 3210, y: 6415, z: 7 };
        patcher.house_system_mut().add_house_tile(house_pos);
        
        let sector = SectorCoord::new(100, 200, 7);
        
        let patches = vec![
            PatchInstruction {
                offset_x: 10,
                offset_y: 15,
                flags: TileFlags::default(),
                items: vec![ItemStack::new(ItemTypeId(100), 1)],
            },
        ];
        
        // Patch with house preservation
        let result = patcher.patch_sector(sector, patches.clone(), false, true);
        assert!(result.is_ok());
        
        // House tile should still be in system
        assert!(patcher.house_system().is_house_field(house_pos));
    }

    #[test]
    fn patch_sector_full_sector() {
        let patcher = temp_patcher();
        let sector = SectorCoord::new(100, 200, 7);
        
        let patches = vec![
            PatchInstruction {
                offset_x: 5,
                offset_y: 5,
                flags: TileFlags::default(),
                items: vec![ItemStack::new(ItemTypeId(100), 1)],
            },
        ];
        
        // Full sector patch - should clear all except patched tile
        let result = patcher.patch_sector(sector, patches, true, false);
        assert!(result.is_ok());
    }
}
