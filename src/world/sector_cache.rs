use crate::world::map_patching::{SectorCoord, SectorData, TileFlags, Tile};
use crate::entities::item::ItemStack;
use crate::world::position::Position;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

/// Sector cache entry with access tracking
#[derive(Debug, Clone)]
pub struct CachedSector {
    pub data: Arc<SectorData>,
    pub access_count: u64,
}

impl CachedSector {
    pub fn new(data: SectorData) -> Self {
        CachedSector {
            data: Arc::new(data),
            access_count: 0,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub loads: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64) / (total as f64)
        }
    }
}

/// Sector cache with LRU eviction
pub struct SectorCache {
    cache: LruCache<SectorCoord, CachedSector>,
    backing_path: PathBuf,
    stats: CacheStats,
}

impl SectorCache {
    /// Create a new sector cache with specified capacity
    pub fn new(capacity: usize, backing_path: PathBuf) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap();
        SectorCache {
            cache: LruCache::new(capacity),
            backing_path,
            stats: CacheStats::default(),
        }
    }
    
    /// Get a sector, loading from disk if not cached
    pub fn get_sector(&mut self, sector: SectorCoord) -> Result<Arc<SectorData>, CacheError> {
        // Check cache
        if let Some(cached) = self.cache.get_mut(&sector) {
            cached.access_count += 1;
            self.stats.hits += 1;
            return Ok(Arc::clone(&cached.data));
        }
        
        // Cache miss - load from disk
        self.stats.misses += 1;
        let sector_data = self.load_sector(sector)?;
        let cached = CachedSector::new(sector_data);
        
        // Add to cache (may evict oldest entry)
        let evicted = self.cache.put(sector, cached).is_some();
        if evicted {
            self.stats.evictions += 1;
        }
        
        self.stats.loads += 1;
        
        // Return the newly cached sector
        Ok(Arc::clone(&self.cache.get(&sector).unwrap().data))
    }
    
    /// Get a tile at a specific position
    pub fn get_tile(&mut self, pos: Position) -> Result<Tile, CacheError> {
        let sector = SectorCoord::from(pos);
        let sector_data = self.get_sector(sector)?;
        
        let offset_x = (pos.x % 32) as u8;
        let offset_y = (pos.y % 32) as u8;
        
        sector_data
            .get_tile(offset_x, offset_y)
            .map(|tile| tile.clone())
            .ok_or(CacheError::InvalidPosition(pos))
    }
    
    /// Load a sector from disk
    fn load_sector(&self, sector: SectorCoord) -> Result<SectorData, CacheError> {
        let path = self.backing_path.join(format!(
            "{:04x}-{:04x}-{:02x}.sec", sector.x, sector.y, sector.z
        ));
        
        if !path.exists() {
            // Return empty sector if file doesn't exist
            return Ok(SectorData::new());
        }
        
        let content = std::fs::read_to_string(&path)
            .map_err(|e| CacheError::ReadError(path.clone(), e.to_string()))?;
        
        let mut tiles = vec![Tile::empty(); 32 * 32];
        self.parse_sector_content(&content, &mut tiles)?;
        
        Ok(SectorData { tiles })
    }
    
    /// Parse sector file content
    fn parse_sector_content(&self, content: &str, tiles: &mut [Tile]) -> Result<(), CacheError> {
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
                    *tile = Tile { flags, items };
                }
            }
        }
        
        Ok(())
    }
    
    /// Parse coordinate "X-Y"
    fn parse_coordinate(&self, coord: &str) -> Result<(u8, u8), CacheError> {
        let parts: Vec<&str> = coord.trim().split('-').collect();
        if parts.len() != 2 {
            return Err(CacheError::ParseError(format!(
                "Invalid coordinate format: {}", coord
            )));
        }
        
        let x: u8 = parts[0]
            .trim()
            .parse()
            .map_err(|_| CacheError::ParseError(format!("Invalid X coordinate: {}", parts[0])))?;
        let y: u8 = parts[1]
            .trim()
            .parse()
            .map_err(|_| CacheError::ParseError(format!("Invalid Y coordinate: {}", parts[1])))?;
        
        if x >= 32 || y >= 32 {
            return Err(CacheError::ParseError(format!(
                "Coordinate out of range: {},{} (must be < 32)", x, y
            )));
        }
        
        Ok((x, y))
    }
    
    /// Parse tile data (flags and items)
    fn parse_tile_data(&self, data: &str) -> Result<(TileFlags, Vec<ItemStack>), CacheError> {
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
                let content = part["Content=".len()..].trim();
                if !content.starts_with('{') || !content.ends_with('}') {
                    continue;
                }
                items = self.parse_items_content(&content[1..content.len()-1])?;
            }
        }
        
        Ok((flags, items))
    }
    
    /// Parse items from content string
    fn parse_items_content(&self, content: &str) -> Result<Vec<ItemStack>, CacheError> {
        let mut items = Vec::new();
        
        for item_str in content.split(',') {
            let item_str = item_str.trim();
            if item_str.is_empty() {
                continue;
            }
            
            let parts: Vec<&str> = item_str.split('(').collect();
            if parts.is_empty() {
                continue;
            }
            
            let item_id: u16 = parts[0]
                .trim()
                .parse()
                .map_err(|_| CacheError::ParseError(format!("Invalid item ID: {}", parts[0])))?;
            
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
    
    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }
    
    /// Reset cache statistics
    pub fn reset_stats(&mut self) {
        self.stats = CacheStats::default();
    }
    
    /// Clear the entire cache
    pub fn clear(&mut self) {
        self.cache.clear();
    }
    
    /// Get current cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    
    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}


/// Convert Position to SectorCoord
impl From<Position> for SectorCoord {
    fn from(pos: Position) -> Self {
        SectorCoord {
            x: pos.x / 32,
            y: pos.y / 32,
            z: pos.z,
        }
    }
}

/// Errors that can occur during cache operations
#[derive(Debug, Clone)]
pub enum CacheError {
    ReadError(PathBuf, String),
    ParseError(String),
    InvalidPosition(Position),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::ReadError(path, msg) => {
                write!(f, "Failed to read {}: {}", path.display(), msg)
            }
            CacheError::ParseError(msg) => {
                write!(f, "Parse error: {}", msg)
            }
            CacheError::InvalidPosition(pos) => {
                write!(f, "Invalid position: ({}, {}, {})", pos.x, pos.y, pos.z)
            }
        }
    }
}

impl std::error::Error for CacheError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    
    fn create_test_sector(path: &Path) {
        let content = r#"# Test sector
10-15: Refresh, Content={100}
5-20: NoLogout, Content={200, 300(5)}
"#;
        fs::write(path, content).unwrap();
    }
    
    fn temp_cache() -> SectorCache {
        let temp_dir = std::env::temp_dir().join(format!("tibia-cache-test-{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();
        
        // Create test sector
        let sector_path = temp_dir.join("0000-0000-07.sec");
        create_test_sector(&sector_path);
        
        SectorCache::new(10, temp_dir)
    }
    
    #[test]
    fn cache_creation() {
        let cache = temp_cache();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }
    
    #[test]
    fn sector_lookup_first_access() {
        let mut cache = temp_cache();
        let sector = SectorCoord::new(0, 0, 7);
        
        let result = cache.get_sector(sector);
        assert!(result.is_ok());
        
        assert_eq!(cache.stats.misses, 1);
        assert_eq!(cache.stats.hits, 0);
        assert_eq!(cache.len(), 1);
    }
    
    #[test]
    fn sector_lookup_cached() {
        let mut cache = temp_cache();
        let sector = SectorCoord::new(0, 0, 7);
        
        // First access - miss
        cache.get_sector(sector).unwrap();
        
        // Second access - hit
        cache.get_sector(sector).unwrap();
        
        assert_eq!(cache.stats.misses, 1);
        assert_eq!(cache.stats.hits, 1);
        assert_eq!(cache.len(), 1);
    }
    
    #[test]
    fn cache_eviction() {
        let temp_dir = std::env::temp_dir().join(format!("tibia-cache-evict-{}", std::process::id()));
        fs::create_dir_all(&temp_dir).unwrap();
        
        // Create multiple test sectors
        for i in 0..5 {
            let path = temp_dir.join(format!("{:04x}-0000-07.sec", i));
            create_test_sector(&path);
        }
        
        let mut cache = SectorCache::new(3, temp_dir);
        
        // Load 5 sectors (capacity is 3)
        for i in 0..5 {
            cache.get_sector(SectorCoord::new(i, 0, 7)).unwrap();
        }
        
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.stats.evictions, 2);
    }
    
    #[test]
    fn hit_rate_calculation() {
        let mut cache = temp_cache();
        let sector = SectorCoord::new(0, 0, 7);
        
        // First access - miss
        cache.get_sector(sector).unwrap();
        
        // Multiple hits
        for _ in 0..10 {
            cache.get_sector(sector).unwrap();
        }
        
        let hit_rate = cache.stats().hit_rate();
        assert!((hit_rate - 0.909).abs() < 0.01); // ~91%
    }
    
    #[test]
    fn reset_stats() {
        let mut cache = temp_cache();
        let sector = SectorCoord::new(0, 0, 7);
        
        cache.get_sector(sector).unwrap();
        cache.get_sector(sector).unwrap();
        
        assert_eq!(cache.stats.hits, 1);
        assert_eq!(cache.stats.misses, 1);
        
        cache.reset_stats();
        
        assert_eq!(cache.stats.hits, 0);
        assert_eq!(cache.stats.misses, 0);
    }
    
    #[test]
    fn clear_cache() {
        let mut cache = temp_cache();
        
        cache.get_sector(SectorCoord::new(0, 0, 7)).unwrap();
        cache.get_sector(SectorCoord::new(1, 0, 7)).unwrap();
        
        assert_eq!(cache.len(), 2);
        
        cache.clear();
        
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }
}
