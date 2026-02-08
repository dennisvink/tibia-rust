use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use crate::entities::item::ItemId;

/// Cron entry for a timed event
#[derive(Clone, Copy, Debug)]
pub struct CronEntry {
    pub object_id: ItemId,
    pub target_round: u32,
}

/// Min-heap by target_round (earliest first)
impl Ord for CronEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior in BinaryHeap (which is max-heap)
        other.target_round.cmp(&self.target_round)
            .then_with(|| self.object_id.0.cmp(&other.object_id.0))
    }
}

impl PartialOrd for CronEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CronEntry {
    fn eq(&self, other: &Self) -> bool {
        self.object_id == other.object_id && self.target_round == other.target_round
    }
}

impl Eq for CronEntry {}

/// Cron system for managing timed events and object expiration
#[derive(Debug)]
pub struct CronSystem {
    heap: BinaryHeap<CronEntry>,
    object_index: HashMap<ItemId, CronEntry>,  // Object ID -> entry for quick lookup
}

impl Default for CronSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl CronSystem {
    pub fn new() -> Self {
        CronSystem {
            heap: BinaryHeap::new(),
            object_index: HashMap::new(),
        }
    }
    
    /// Add object to cron system with a delay
    pub fn set(&mut self, object_id: ItemId, delay: u32, current_round: u32) {
        let entry = CronEntry {
            object_id,
            target_round: current_round + delay,
        };
        
        // Store in index
        self.object_index.insert(object_id, entry.clone());
        
        // Add to heap
        self.heap.push(entry);
    }
    
    /// Check if any event is ready (but don't remove it)
    pub fn check(&mut self, current_round: u32) -> Option<ItemId> {
        loop {
            let entry = self.heap.peek()?;
            let current = self.object_index.get(&entry.object_id);
            match current {
                Some(active) if active.target_round == entry.target_round => {
                    if entry.target_round <= current_round {
                        return Some(entry.object_id);
                    }
                    return None;
                }
                _ => {
                    self.heap.pop();
                    continue;
                }
            }
        }
    }
    
    /// Pop and return next ready event
    pub fn pop_ready(&mut self, current_round: u32) -> Option<ItemId> {
        loop {
            let entry = self.heap.peek()?;
            let current = self.object_index.get(&entry.object_id);
            match current {
                Some(active) if active.target_round == entry.target_round => {
                    if entry.target_round <= current_round {
                        let entry = self.heap.pop()?;
                        self.object_index.remove(&entry.object_id);
                        return Some(entry.object_id);
                    }
                    return None;
                }
                _ => {
                    self.heap.pop();
                    continue;
                }
            }
        }
    }
    
    /// Remove object from cron system and return remaining rounds
    pub fn stop(&mut self, object_id: ItemId, current_round: u32) -> Option<u32> {
        let entry = self.object_index.remove(&object_id)?;
        let remaining = if entry.target_round > current_round {
            entry.target_round - current_round
        } else {
            1
        };
        Some(remaining)
    }
    
    /// Get remaining time for object in rounds
    pub fn get_remaining(&self, object_id: ItemId, current_round: u32) -> Option<u32> {
        let entry = self.object_index.get(&object_id)?;
        let remaining = if entry.target_round > current_round {
            entry.target_round - current_round
        } else {
            1
        };
        Some(remaining)
    }
    
    /// Change delay for existing entry (returns true if updated)
    pub fn change(&mut self, object_id: ItemId, new_delay: u32, current_round: u32) -> bool {
        let existed = self.object_index.contains_key(&object_id);
        self.set(object_id, new_delay, current_round);
        existed
    }
    
    /// Get number of active entries
    pub fn len(&self) -> usize {
        self.object_index.len()
    }
    
    /// Check if system is empty
    pub fn is_empty(&self) -> bool {
        self.object_index.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_system_basic_operations() {
        let mut cron = CronSystem::new();
        let current_round = 1000;
        
        let item1 = ItemId(1);
        let item2 = ItemId(2);
        
        // Add two events
        cron.set(item1, 10, current_round);  // Ready at 1010
        cron.set(item2, 5, current_round);   // Ready at 1005
        
        assert_eq!(cron.len(), 2);
        
        // Check: item2 should be ready first
        assert_eq!(cron.check(1004), None);
        assert_eq!(cron.check(1005), Some(item2));
        
        // Pop ready event
        assert_eq!(cron.pop_ready(1005), Some(item2));
        assert_eq!(cron.len(), 1);
        
        // Item1 not ready yet
        assert_eq!(cron.check(1009), None);
        assert_eq!(cron.pop_ready(1009), None);
        
        // Now item1 is ready
        assert_eq!(cron.pop_ready(1010), Some(item1));
        assert_eq!(cron.len(), 0);
    }
    
    #[test]
    fn cron_system_get_remaining() {
        let mut cron = CronSystem::new();
        let current_round = 1000;
        let item = ItemId(1);
        
        cron.set(item, 10, current_round);
        
        assert_eq!(cron.get_remaining(item, 1005), Some(5));
        assert_eq!(cron.get_remaining(item, 1010), Some(1));
        assert_eq!(cron.get_remaining(item, 1015), Some(1));
    }
    
    #[test]
    fn cron_system_stop() {
        let mut cron = CronSystem::new();
        let current_round = 1000;
        let item = ItemId(1);
        
        cron.set(item, 10, current_round);
        assert_eq!(cron.stop(item, current_round), Some(10));
        assert_eq!(cron.stop(item, current_round), None);  // Already removed
        assert_eq!(cron.len(), 0);
    }
    
    #[test]
    fn cron_system_change_delay() {
        let mut cron = CronSystem::new();
        let current_round = 1000;
        let item = ItemId(1);
        
        cron.set(item, 10, current_round);
        assert_eq!(cron.get_remaining(item, 1000), Some(10));
        
        assert!(cron.change(item, 20, current_round));
        assert_eq!(cron.get_remaining(item, 1000), Some(20));
    }
    
    #[test]
    fn cron_system_multiple_events_same_time() {
        let mut cron = CronSystem::new();
        let current_round = 1000;
        
        cron.set(ItemId(1), 5, current_round);
        cron.set(ItemId(2), 5, current_round);
        cron.set(ItemId(3), 5, current_round);
        
        // All should be ready at 1005
        let mut ready = Vec::new();
        while let Some(item) = cron.pop_ready(1005) {
            ready.push(item);
        }
        
        assert_eq!(ready.len(), 3);
        assert!(ready.contains(&ItemId(1)));
        assert!(ready.contains(&ItemId(2)));
        assert!(ready.contains(&ItemId(3)));
    }
    
    #[test]
    fn cron_system_pop_ready_only_returns_ready() {
        let mut cron = CronSystem::new();
        let current_round = 1000;
        
        cron.set(ItemId(1), 5, current_round);   // Ready at 1005
        cron.set(ItemId(2), 15, current_round);  // Ready at 1015
        
        // Only first item ready
        assert_eq!(cron.pop_ready(1005), Some(ItemId(1)));
        assert_eq!(cron.pop_ready(1005), None);
        
        // Second item still not ready
        assert_eq!(cron.pop_ready(1010), None);
        
        // Now second item ready
        assert_eq!(cron.pop_ready(1015), Some(ItemId(2)));
    }
}
