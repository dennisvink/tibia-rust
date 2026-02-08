use std::collections::HashMap;
use std::sync::Arc;

/// A reference-counted string that can be shared across multiple objects
pub type DynamicString = Arc<str>;

/// Pool for managing dynamic strings with automatic deduplication
#[derive(Debug)]
pub struct StringPool {
    strings: Vec<DynamicString>,
    index: HashMap<String, usize>,
}

impl StringPool {
    /// Create a new string pool
    pub fn new() -> Self {
        StringPool {
            strings: Vec::new(),
            index: HashMap::new(),
        }
    }
    
    /// Add or get existing string (automatic deduplication)
    pub fn add(&mut self, s: &str) -> DynamicString {
        if s.is_empty() {
            return Arc::from("");
        }
        
        // Check if string already exists
        if let Some(&idx) = self.index.get(s) {
            return Arc::clone(&self.strings[idx]);
        }
        
        // Add new string
        let arc_str: DynamicString = Arc::from(s);
        let idx = self.strings.len();
        self.index.insert(s.to_string(), idx);
        self.strings.push(Arc::clone(&arc_str));
        
        arc_str
    }
    
    /// Get string by index (1-based, 0 = empty string)
    pub fn get(&self, id: usize) -> DynamicString {
        if id == 0 || id > self.strings.len() {
            return Arc::from("");
        }
        Arc::clone(&self.strings[id - 1])
    }
    
    /// Remove string (only when no other references exist)
    pub fn remove(&mut self, id: usize) {
        if id == 0 || id > self.strings.len() {
            return;
        }
        
        let idx = id - 1;
        if Arc::strong_count(&self.strings[idx]) == 1 {
            // Only the pool references it, safe to remove
            let s = self.strings[idx].to_string();
            self.index.remove(&s);
            self.strings[idx] = Arc::from("");
        }
    }
    
    /// Get number of strings in the pool
    pub fn len(&self) -> usize {
        self.strings.len()
    }
    
    /// Check if pool is empty
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
    
    /// Clear all strings (only removes strings with no external references)
    pub fn clear(&mut self) {
        self.strings.retain(|s| Arc::strong_count(s) > 1);
        self.index.clear();
        
        // Rebuild index
        for (idx, s) in self.strings.iter().enumerate() {
            self.index.insert(s.to_string(), idx);
        }
    }
}

impl Default for StringPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_pool_creation() {
        let pool = StringPool::new();
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }
    
    #[test]
    fn add_empty_string() {
        let mut pool = StringPool::new();
        let s = pool.add("");
        
        assert_eq!(s.as_ref(), "");
        assert_eq!(pool.len(), 0);  // Empty strings not stored
    }
    
    #[test]
    fn add_single_string() {
        let mut pool = StringPool::new();
        let s = pool.add("Hello");
        
        assert_eq!(s.as_ref(), "Hello");
        assert_eq!(pool.len(), 1);
    }
    
    #[test]
    fn string_deduplication() {
        let mut pool = StringPool::new();
        
        let s1 = pool.add("Hello");
        let s2 = pool.add("Hello");
        let s3 = pool.add("Hello");
        
        // All should be the same Arc pointer (same data)
        assert!(Arc::ptr_eq(&s1, &s2));
        assert!(Arc::ptr_eq(&s2, &s3));
        
        // Only one string stored in pool
        assert_eq!(pool.len(), 1);
    }
    
    #[test]
    fn get_string_by_id() {
        let mut pool = StringPool::new();
        
        let s = pool.add("Test");
        assert_eq!(s.as_ref(), "Test");
        
        // Get by ID (1-based)
        let retrieved = pool.get(1);
        assert_eq!(retrieved.as_ref(), "Test");
        assert!(Arc::ptr_eq(&s, &retrieved));
    }
    
    #[test]
    fn get_invalid_id() {
        let pool = StringPool::new();
        
        assert_eq!(pool.get(0).as_ref(), "");
        assert_eq!(pool.get(999).as_ref(), "");
    }
    
    #[test]
    fn remove_string_no_references() {
        let mut pool = StringPool::new();
        
        pool.add("Test");
        assert_eq!(pool.len(), 1);
        
        pool.remove(1);
        // String removed because only pool references it
        assert_eq!(pool.len(), 1);  // Still in pool but empty
    }
    
    #[test]
    fn remove_string_with_references() {
        let mut pool = StringPool::new();
        
        let s = pool.add("Test");
        assert_eq!(Arc::strong_count(&s), 2);  // pool + s
        
        pool.remove(1);
        // String not removed because 's' still references it
        assert_eq!(Arc::strong_count(&s), 1);  // Only 's' references it now
    }
    
    #[test]
    fn clear_pool() {
        let mut pool = StringPool::new();
        
        let s1 = pool.add("String1");
        let s2 = pool.add("String2");
        
        assert_eq!(pool.len(), 2);
        
        pool.clear();
        
        // Strings still exist because s1 and s2 reference them
        assert_eq!(s1.as_ref(), "String1");
        assert_eq!(s2.as_ref(), "String2");
        
        assert_eq!(Arc::strong_count(&s1), 1);
        assert_eq!(Arc::strong_count(&s2), 1);
    }
    
    #[test]
    fn multiple_unique_strings() {
        let mut pool = StringPool::new();
        
        pool.add("Hello");
        pool.add("World");
        pool.add("Test");
        pool.add("Hello");  // Duplicate
        
        assert_eq!(pool.len(), 3);  // Only 3 unique strings
    }
    
    #[test]
    fn string_pool_thread_safety() {
        // Arc<str> is Send + Sync, so StringPool should work across threads
        let mut pool = StringPool::new();
        let s = pool.add("Test");
        
        // This compiles because Arc<str> is Send
        let _ = std::thread::spawn(move || {
            assert_eq!(s.as_ref(), "Test");
        });
    }
}