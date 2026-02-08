use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemId(pub u32);

static NEXT_ITEM_ID: AtomicU32 = AtomicU32::new(1);

impl ItemId {
    pub fn next() -> Self {
        let id = NEXT_ITEM_ID.fetch_add(1, Ordering::Relaxed);
        ItemId(id)
    }

    pub fn is_assigned(self) -> bool {
        self.0 != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemTypeId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Ground,
    Container,
    Weapon,
    Armor,
    Consumable,
    Rune,
    Misc,
}

use crate::entities::dynamic_string::{DynamicString, StringPool};

#[derive(Debug, Clone)]
pub struct ItemStack {
    pub id: ItemId,
    pub type_id: ItemTypeId,
    pub count: u16,
    pub attributes: Vec<ItemAttribute>,
    pub contents: Vec<ItemStack>,
}

impl Default for ItemStack {
    fn default() -> Self {
        Self {
            id: ItemId::next(),
            type_id: ItemTypeId(0),
            count: 0,
            attributes: Vec::new(),
            contents: Vec::new(),
        }
    }
}

impl PartialEq for ItemStack {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
            && self.count == other.count
            && self.attributes == other.attributes
            && self.contents == other.contents
    }
}

impl Eq for ItemStack {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemAttribute {
    String(String),
    DynamicString(DynamicString),
    ChestQuestNumber(u16),
    ContainerLiquidType(u8),
    Amount(u16),
    PoolLiquidType(u8),
    RemainingExpireTime(u16),
    KeyholeNumber(u16),
    DoorQuestNumber(u16),
    DoorQuestValue(u16),
    Level(u16),
    RemainingUses(u16),
    KeyNumber(u16),
    SavedExpireTime(u16),
    Charges(u16),
    AbsTeleportDestination(i32),
    Responsible(u32),
    Unknown { key: String, value: String },
}

impl ItemStack {
    pub fn new(type_id: ItemTypeId, count: u16) -> Self {
        Self {
            id: ItemId::next(),
            type_id,
            count,
            attributes: Vec::new(),
            contents: Vec::new(),
        }
    }
    
    /// Set text attribute using a string pool (for books, signs, etc.)
    pub fn set_text(&mut self, pool: &mut StringPool, text: &str) {
        // Remove existing DynamicString attribute
        self.attributes.retain(|attr| !matches!(attr, ItemAttribute::DynamicString(_)));
        
        if !text.is_empty() {
            let dynamic_string = pool.add(text);
            self.attributes.push(ItemAttribute::DynamicString(dynamic_string));
        }
    }
    
    /// Get text attribute
    pub fn get_text(&self) -> &str {
        for attr in &self.attributes {
            if let ItemAttribute::DynamicString(s) = attr {
                return s.as_ref();
            }
        }
        ""
    }
    
    /// Set editor attribute using a string pool (for writable books)
    pub fn set_editor(&mut self, pool: &mut StringPool, text: &str) {
        // Remove existing editor attributes (both String and DynamicString)
        self.attributes.retain(|attr| {
            !matches!(attr, ItemAttribute::String(_)) && 
            !matches!(attr, ItemAttribute::DynamicString(_))
        });
        
        if !text.is_empty() {
            let dynamic_string = pool.add(text);
            self.attributes.push(ItemAttribute::DynamicString(dynamic_string));
        }
    }
    
    /// Get editor attribute
    pub fn get_editor(&self) -> &str {
        for attr in &self.attributes {
            if let ItemAttribute::DynamicString(s) = attr {
                return s.as_ref();
            }
            if let ItemAttribute::String(s) = attr {
                return s.as_str();
            }
        }
        ""
    }
    
    /// Check if item has text
    pub fn has_text(&self) -> bool {
        self.attributes.iter().any(|attr| matches!(attr, ItemAttribute::DynamicString(_)))
    }
    
    /// Clear text attribute
    pub fn clear_text(&mut self) {
        self.attributes.retain(|attr| !matches!(attr, ItemAttribute::DynamicString(_)));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub id: ItemId,
    pub type_id: ItemTypeId,
    pub kind: ItemKind,
    pub count: u16,
    pub charges: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_stack_set_text() {
        let mut pool = StringPool::new();
        let mut item = ItemStack::new(ItemTypeId(1), 1);
        
        item.set_text(&mut pool, "Hello World");
        assert_eq!(item.get_text(), "Hello World");
        assert!(item.has_text());
    }
    
    #[test]
    fn item_stack_set_empty_text() {
        let mut pool = StringPool::new();
        let mut item = ItemStack::new(ItemTypeId(1), 1);
        
        item.set_text(&mut pool, "");
        assert_eq!(item.get_text(), "");
        assert!(!item.has_text());
    }
    
    #[test]
    fn item_stack_clear_text() {
        let mut pool = StringPool::new();
        let mut item = ItemStack::new(ItemTypeId(1), 1);
        
        item.set_text(&mut pool, "Some text");
        assert!(item.has_text());
        
        item.clear_text();
        assert!(!item.has_text());
        assert_eq!(item.get_text(), "");
    }
    
    #[test]
    fn item_stack_string_deduplication() {
        let mut pool = StringPool::new();
        let mut item1 = ItemStack::new(ItemTypeId(1), 1);
        let mut item2 = ItemStack::new(ItemTypeId(2), 1);
        
        item1.set_text(&mut pool, "Shared text");
        item2.set_text(&mut pool, "Shared text");
        
        assert_eq!(item1.get_text(), "Shared text");
        assert_eq!(item2.get_text(), "Shared text");
        assert_eq!(pool.len(), 1);  // Only one unique string
    }
    
    #[test]
    fn item_stack_set_editor() {
        let mut pool = StringPool::new();
        let mut item = ItemStack::new(ItemTypeId(1), 1);
        
        item.set_editor(&mut pool, "Editable text");
        assert_eq!(item.get_editor(), "Editable text");
    }
    
    #[test]
    fn item_shared_text_between_items() {
        let mut pool = StringPool::new();
        let mut book1 = ItemStack::new(ItemTypeId(1), 1);
        let mut book2 = ItemStack::new(ItemTypeId(2), 1);
        let mut book3 = ItemStack::new(ItemTypeId(3), 1);
        
        // All books get the same text
        book1.set_text(&mut pool, "Chapter 1");
        book2.set_text(&mut pool, "Chapter 1");
        book3.set_text(&mut pool, "Chapter 1");
        
        assert_eq!(book1.get_text(), "Chapter 1");
        assert_eq!(book2.get_text(), "Chapter 1");
        assert_eq!(book3.get_text(), "Chapter 1");
        
        // All share the same underlying string
        assert_eq!(pool.len(), 1);
    }
    
    #[test]
    fn item_replace_text() {
        let mut pool = StringPool::new();
        let mut item = ItemStack::new(ItemTypeId(1), 1);
        
        item.set_text(&mut pool, "Original text");
        assert_eq!(item.get_text(), "Original text");
        
        item.set_text(&mut pool, "New text");
        assert_eq!(item.get_text(), "New text");
        
        // Pool should have 2 strings now
        assert_eq!(pool.len(), 2);
    }
}
