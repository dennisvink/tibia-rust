use serde::{Deserialize, Serialize};
use crate::world::position::Position;

/// A rectangular zone in the game world
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Zone {
    pub name: String,
    pub x1: i32,
    pub y1: i32,
    pub z1: i32,
    pub x2: i32,
    pub y2: i32,
    pub z2: i32,
}

impl Zone {
    /// Create a new zone
    pub fn new(name: impl Into<String>, x1: i32, y1: i32, z1: i32, x2: i32, y2: i32, z2: i32) -> Self {
        // Ensure coordinates are ordered correctly
        let (x1, x2) = if x1 <= x2 { (x1, x2) } else { (x2, x1) };
        let (y1, y2) = if y1 <= y2 { (y1, y2) } else { (y2, y1) };
        let (z1, z2) = if z1 <= z2 { (z1, z2) } else { (z2, z1) };
        
        Zone {
            name: name.into(),
            x1, y1, z1,
            x2, y2, z2,
        }
    }
    
    /// Check if a position is within this zone
    pub fn contains(&self, position: Position) -> bool {
        let x = position.x as i32;
        let y = position.y as i32;
        let z = position.z as i32;
        
        x >= self.x1 && x <= self.x2
            && y >= self.y1 && y <= self.y2
            && z >= self.z1 && z <= self.z2
    }
}

/// Configuration for premium area detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PremiumConfig {
    /// Zones that are free to play (non-premium)
    pub free_play_zones: Vec<Zone>,
    /// Zones that require premium access
    pub premium_only_zones: Vec<Zone>,
}

impl PremiumConfig {
    /// Create a new premium configuration
    pub fn new() -> Self {
        PremiumConfig {
            free_play_zones: Vec::new(),
            premium_only_zones: Vec::new(),
        }
    }
    
    /// Add a free-play zone
    pub fn add_free_play_zone(&mut self, zone: Zone) {
        self.free_play_zones.push(zone);
    }
    
    /// Add a premium-only zone
    pub fn add_premium_zone(&mut self, zone: Zone) {
        self.premium_only_zones.push(zone);
    }
}

impl Default for PremiumConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Checker for premium area access
#[derive(Debug, Clone)]
pub struct PremiumAreaChecker {
    config: PremiumConfig,
}

impl PremiumAreaChecker {
    /// Create a new premium area checker
    pub fn new(config: PremiumConfig) -> Self {
        PremiumAreaChecker { config }
    }
    
    /// Check if a position is in a premium area
    /// Returns true if premium is required, false if free to play
    pub fn is_premium_area(&self, position: Position) -> bool {
        // Check if in free-play zone
        for zone in &self.config.free_play_zones {
            if zone.contains(position) {
                return false;
            }
        }
        
        // Check if explicitly marked as premium-only
        for zone in &self.config.premium_only_zones {
            if zone.contains(position) {
                return true;
            }
        }
        
        // Default: premium area (everything outside free-play zones)
        true
    }
    
    /// Check if a position is in a free-play area
    pub fn is_free_play_area(&self, position: Position) -> bool {
        !self.is_premium_area(position)
    }
    
    /// Get the name of the zone containing a position, if any
    pub fn get_zone_name(&self, position: Position) -> Option<&String> {
        // Check free-play zones first
        for zone in &self.config.free_play_zones {
            if zone.contains(position) {
                return Some(&zone.name);
            }
        }
        
        // Check premium zones
        for zone in &self.config.premium_only_zones {
            if zone.contains(position) {
                return Some(&zone.name);
            }
        }
        
        None
    }
}

/// Default implementation that matches the reference server's behavior
impl Default for PremiumAreaChecker {
    fn default() -> Self {
        // Create a configuration that matches the reference implementation
        let mut config = PremiumConfig::new();
        
        // The reference creates a free-to-play zone in the south-west
        // using the complex boolean expression. We'll create an equivalent zone.
        // The reference defines non-premium area roughly at:
        // SectorX range: ~1022-1035
        // SectorY range: ~1008-1031
        // Each sector is 32x32 tiles
        
        // Convert to tile coordinates
        let x1 = 1022 * 32;
        let x2 = 1035 * 32;
        let y1 = 1008 * 32;
        let y2 = 1031 * 32;
        
        config.add_free_play_zone(Zone::new(
            "Free Play Area",
            x1, y1, 0,
            x2, y2, 7,
        ));
        
        PremiumAreaChecker::new(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_contains_point() {
        let zone = Zone::new("Test", 0, 0, 0, 100, 100, 7);
        
        assert!(zone.contains(Position { x: 0, y: 0, z: 0 }));
        assert!(zone.contains(Position { x: 50, y: 50, z: 3 }));
        assert!(zone.contains(Position { x: 100, y: 100, z: 7 }));
        
        assert!(!zone.contains(Position { x: 101, y: 50, z: 3 }));
        assert!(!zone.contains(Position { x: 50, y: 101, z: 3 }));
        assert!(!zone.contains(Position { x: 50, y: 50, z: 8 }));
    }
    
    #[test]
    fn zone_handles_reversed_coordinates() {
        let zone = Zone::new("Test", 100, 100, 7, 0, 0, 0);
        
        // Should work the same as properly ordered coordinates
        assert!(zone.contains(Position { x: 0, y: 0, z: 0 }));
        assert!(zone.contains(Position { x: 50, y: 50, z: 3 }));
        assert!(zone.contains(Position { x: 100, y: 100, z: 7 }));
    }
    
    #[test]
    fn free_play_zone_detection() {
        let mut config = PremiumConfig::new();
        config.add_free_play_zone(Zone::new(
            "Starter Town",
            0, 0, 0,
            100, 100, 7,
        ));
        
        let checker = PremiumAreaChecker::new(config);
        
        assert!(!checker.is_premium_area(Position { x: 50, y: 50, z: 0 }));
        assert!(checker.is_free_play_area(Position { x: 50, y: 50, z: 0 }));
    }
    
    #[test]
    fn premium_zone_detection() {
        let mut config = PremiumConfig::new();
        config.add_premium_zone(Zone::new(
            "Dragon Lair",
            1000, 1000, 0,
            1100, 1100, 10,
        ));
        
        let checker = PremiumAreaChecker::new(config);
        
        assert!(checker.is_premium_area(Position { x: 1050, y: 1050, z: 5 }));
        assert!(!checker.is_free_play_area(Position { x: 1050, y: 1050, z: 5 }));
    }
    
    #[test]
    fn default_is_premium_outside_zones() {
        let config = PremiumConfig::new();
        let checker = PremiumAreaChecker::new(config);
        
        // Outside any zone should be premium by default
        assert!(checker.is_premium_area(Position { x: 500, y: 500, z: 7 }));
    }
    
    #[test]
    fn zone_name_retrieval() {
        let mut config = PremiumConfig::new();
        config.add_free_play_zone(Zone::new(
            "Free Zone",
            0, 0, 0,
            100, 100, 7,
        ));
        config.add_premium_zone(Zone::new(
            "Premium Zone",
            1000, 1000, 0,
            1100, 1100, 10,
        ));
        
        let checker = PremiumAreaChecker::new(config);
        
        assert_eq!(
            checker.get_zone_name(Position { x: 50, y: 50, z: 0 }),
            Some(&String::from("Free Zone"))
        );
        assert_eq!(
            checker.get_zone_name(Position { x: 1050, y: 1050, z: 5 }),
            Some(&String::from("Premium Zone"))
        );
        assert_eq!(
            checker.get_zone_name(Position { x: 500, y: 500, z: 7 }),
            None
        );
    }
    
    #[test]
    fn free_play_takes_precedence() {
        let mut config = PremiumConfig::new();
        
        // Overlapping zones - free play should take precedence
        config.add_free_play_zone(Zone::new(
            "Free",
            0, 0, 0,
            100, 100, 7,
        ));
        config.add_premium_zone(Zone::new(
            "Premium",
            50, 50, 0,
            150, 150, 7,
        ));
        
        let checker = PremiumAreaChecker::new(config);
        
        // In overlap, free play should win
        assert!(!checker.is_premium_area(Position { x: 75, y: 75, z: 3 }));
    }
    
    #[test]
    fn z_level_respected() {
        let config = PremiumConfig::new();
        let checker = PremiumAreaChecker::new(config);
        
        // Same x,y but different z should work correctly
        assert!(checker.is_premium_area(Position { x: 50, y: 50, z: 0 }));
        assert!(checker.is_premium_area(Position { x: 50, y: 50, z: 7 }));
        assert!(checker.is_premium_area(Position { x: 50, y: 50, z: 15 }));
    }
}