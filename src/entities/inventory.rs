use crate::entities::item::{ItemStack, ItemTypeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InventorySlot {
    Head,
    Necklace,
    Backpack,
    Armor,
    RightHand,
    LeftHand,
    Legs,
    Feet,
    Ring,
    Ammo,
}

impl InventorySlot {
    const COUNT: usize = 10;

    pub fn index(self) -> usize {
        match self {
            InventorySlot::Head => 0,
            InventorySlot::Necklace => 1,
            InventorySlot::Backpack => 2,
            InventorySlot::Armor => 3,
            InventorySlot::RightHand => 4,
            InventorySlot::LeftHand => 5,
            InventorySlot::Legs => 6,
            InventorySlot::Feet => 7,
            InventorySlot::Ring => 8,
            InventorySlot::Ammo => 9,
        }
    }

    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(InventorySlot::Head),
            1 => Some(InventorySlot::Necklace),
            2 => Some(InventorySlot::Backpack),
            3 => Some(InventorySlot::Armor),
            4 => Some(InventorySlot::RightHand),
            5 => Some(InventorySlot::LeftHand),
            6 => Some(InventorySlot::Legs),
            7 => Some(InventorySlot::Feet),
            8 => Some(InventorySlot::Ring),
            9 => Some(InventorySlot::Ammo),
            _ => None,
        }
    }
}

pub const INVENTORY_SLOTS: [InventorySlot; 10] = [
    InventorySlot::Head,
    InventorySlot::Necklace,
    InventorySlot::Backpack,
    InventorySlot::Armor,
    InventorySlot::RightHand,
    InventorySlot::LeftHand,
    InventorySlot::Legs,
    InventorySlot::Feet,
    InventorySlot::Ring,
    InventorySlot::Ammo,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inventory {
    slots: Vec<Option<ItemStack>>,
}

impl Default for Inventory {
    fn default() -> Self {
        Self {
            slots: vec![None; InventorySlot::COUNT],
        }
    }
}

impl Inventory {
    pub fn slot(&self, slot: InventorySlot) -> Option<&ItemStack> {
        self.slots.get(slot.index()).and_then(|entry| entry.as_ref())
    }

    pub fn slot_mut(&mut self, slot: InventorySlot) -> Option<&mut ItemStack> {
        self.slots
            .get_mut(slot.index())
            .and_then(|entry| entry.as_mut())
    }

    pub fn set_slot(&mut self, slot: InventorySlot, item: Option<ItemStack>) {
        if let Some(entry) = self.slots.get_mut(slot.index()) {
            *entry = item;
        }
    }

    pub fn count_type(&self, type_id: ItemTypeId) -> u16 {
        self.slots
            .iter()
            .filter_map(|entry| entry.as_ref())
            .filter(|entry| entry.type_id == type_id)
            .fold(0u16, |acc, entry| acc.saturating_add(entry.count))
    }

    pub fn add_item(
        &mut self,
        item: ItemStack,
        stackable: bool,
    ) -> Result<InventorySlot, String> {
        if item.count == 0 {
            return Err("cannot add zero-count item".to_string());
        }
        if stackable {
            for (index, entry) in self.slots.iter_mut().enumerate() {
                if let Some(existing) = entry.as_mut() {
                    if existing.type_id == item.type_id && existing.attributes == item.attributes {
                        let total = existing.count as u32 + item.count as u32;
                        if total > u16::MAX as u32 {
                            return Err("inventory stack overflow".to_string());
                        }
                        existing.count = total as u16;
                        return InventorySlot::from_index(index)
                            .ok_or_else(|| "inventory slot index out of range".to_string());
                    }
                }
            }
        }

        if let Some((index, entry)) = self
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, entry)| entry.is_none())
        {
            *entry = Some(item);
            return InventorySlot::from_index(index)
                .ok_or_else(|| "inventory slot index out of range".to_string());
        }

        Err("inventory full".to_string())
    }

    pub fn add_item_to_slot(
        &mut self,
        slot: InventorySlot,
        item: ItemStack,
        stackable: bool,
    ) -> Result<(), String> {
        if item.count == 0 {
            return Err("cannot add zero-count item".to_string());
        }
        let entry = self
            .slots
            .get_mut(slot.index())
            .ok_or_else(|| "inventory slot index out of range".to_string())?;
        if let Some(existing) = entry.as_mut() {
            if stackable && existing.type_id == item.type_id && existing.attributes == item.attributes {
                let total = existing.count as u32 + item.count as u32;
                if total > u16::MAX as u32 {
                    return Err("inventory stack overflow".to_string());
                }
                existing.count = total as u16;
                return Ok(());
            }
            return Err("inventory slot occupied".to_string());
        }
        *entry = Some(item);
        Ok(())
    }

    pub fn remove_item(
        &mut self,
        slot: InventorySlot,
        count: u16,
    ) -> Result<ItemStack, String> {
        if count == 0 {
            return Err("cannot remove zero-count item".to_string());
        }
        let entry = self
            .slots
            .get_mut(slot.index())
            .ok_or_else(|| "inventory slot index out of range".to_string())?;
        let Some(existing) = entry.as_mut() else {
            return Err("inventory slot empty".to_string());
        };
        if count > existing.count {
            return Err("inventory slot has insufficient count".to_string());
        }
        if count == existing.count {
            let removed = entry.take().expect("entry present");
            return Ok(removed);
        }

        existing.count -= count;
        Ok(ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: existing.type_id,
            count,
            attributes: existing.attributes.clone(),
            contents: Vec::new(),
        })
    }

    pub fn move_item(
        &mut self,
        from: InventorySlot,
        to: InventorySlot,
        count: u16,
        stackable: bool,
    ) -> Result<(), String> {
        if count == 0 {
            return Err("cannot move zero-count item".to_string());
        }
        if from == to {
            return Err("cannot move item to same slot".to_string());
        }

        let from_index = from.index();
        let to_index = to.index();
        if from_index >= self.slots.len() || to_index >= self.slots.len() {
            return Err("inventory slot index out of range".to_string());
        }

        let (from_slot, to_slot) = if from_index < to_index {
            let (left, right) = self.slots.split_at_mut(to_index);
            (&mut left[from_index], &mut right[0])
        } else {
            let (left, right) = self.slots.split_at_mut(from_index);
            (&mut right[0], &mut left[to_index])
        };

        let from_item = from_slot
            .as_ref()
            .ok_or_else(|| "inventory slot empty".to_string())?
            .clone();
        if count > from_item.count {
            return Err("inventory slot has insufficient count".to_string());
        }

        if stackable {
            if let Some(dest) = to_slot.as_mut() {
                if dest.type_id == from_item.type_id && dest.attributes == from_item.attributes {
                    let total = dest.count as u32 + count as u32;
                    if total > u16::MAX as u32 {
                        return Err("inventory stack overflow".to_string());
                    }
                    dest.count = total as u16;
                    if count == from_item.count {
                        *from_slot = None;
                    } else if let Some(entry) = from_slot.as_mut() {
                        entry.count = from_item.count - count;
                    }
                    return Ok(());
                }
                if count != from_item.count {
                    return Err("cannot split stack onto occupied slot".to_string());
                }
                std::mem::swap(from_slot, to_slot);
                return Ok(());
            }

            let moved = ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: from_item.type_id,
                count,
                attributes: from_item.attributes.clone(),
                contents: from_item.contents.clone(),
            };
            if count == from_item.count {
                *from_slot = None;
            } else if let Some(entry) = from_slot.as_mut() {
                entry.count = from_item.count - count;
            }
            *to_slot = Some(moved);
            return Ok(());
        }

        if count != from_item.count {
            return Err("cannot split non-stackable item".to_string());
        }
        if to_slot.is_some() {
            std::mem::swap(from_slot, to_slot);
        } else {
            *to_slot = from_slot.take();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_item_splits_stack_into_empty_slot() {
        let mut inventory = Inventory::default();
        let item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(1),
            count: 10,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        inventory.set_slot(InventorySlot::Head, Some(item));

        inventory
            .move_item(InventorySlot::Head, InventorySlot::Ring, 3, true)
            .expect("split move");

        let head = inventory.slot(InventorySlot::Head).expect("head item");
        let ring = inventory.slot(InventorySlot::Ring).expect("ring item");
        assert_eq!(head.count, 7);
        assert_eq!(ring.count, 3);
        assert_eq!(head.type_id, ring.type_id);
    }

    #[test]
    fn move_item_merges_stack_into_existing_slot() {
        let mut inventory = Inventory::default();
        let item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(2),
            count: 5,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        let other = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(2),
            count: 4,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        inventory.set_slot(InventorySlot::Head, Some(item));
        inventory.set_slot(InventorySlot::Ring, Some(other));

        inventory
            .move_item(InventorySlot::Head, InventorySlot::Ring, 3, true)
            .expect("merge move");

        let head = inventory.slot(InventorySlot::Head).expect("head item");
        let ring = inventory.slot(InventorySlot::Ring).expect("ring item");
        assert_eq!(head.count, 2);
        assert_eq!(ring.count, 7);
        assert_eq!(head.type_id, ring.type_id);
    }

    #[test]
    fn move_item_rejects_split_onto_occupied_different_type() {
        let mut inventory = Inventory::default();
        let item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(3),
            count: 5,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        let other = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(4),
            count: 1,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        inventory.set_slot(InventorySlot::Head, Some(item));
        inventory.set_slot(InventorySlot::Ring, Some(other));

        let err = inventory
            .move_item(InventorySlot::Head, InventorySlot::Ring, 3, true)
            .expect_err("should not split onto occupied slot");
        assert_eq!(err, "cannot split stack onto occupied slot");
    }
}
