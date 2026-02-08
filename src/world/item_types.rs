use crate::entities::item::{ItemKind, ItemTypeId};
use crate::world::object_types::{ObjectType, ObjectTypeIndex};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemType {
    pub id: ItemTypeId,
    pub name: String,
    pub kind: ItemKind,
    pub stackable: bool,
    pub has_count: bool,
    pub container_capacity: Option<u16>,
    pub takeable: bool,
    pub is_expiring: bool,
    pub expire_stop: bool,
    pub expire_time_secs: Option<u32>,
    pub expire_target: Option<ItemTypeId>,
}

#[derive(Debug, Default, Clone)]
pub struct ItemTypeIndex {
    types: HashMap<ItemTypeId, ItemType>,
}

impl ItemTypeIndex {
    pub fn get(&self, id: ItemTypeId) -> Option<&ItemType> {
        self.types.get(&id)
    }

    pub fn insert(&mut self, item: ItemType) -> Result<(), String> {
        if self.types.contains_key(&item.id) {
            return Err(format!("item type {:?} already exists", item.id));
        }
        self.types.insert(item.id, item);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }
}

pub fn build_item_types(objects: &ObjectTypeIndex) -> ItemTypeIndex {
    let mut index = ItemTypeIndex::default();
    for (id, object) in objects.iter() {
        let kind = item_kind_from_object(object);
        let stackable = object.has_flag("Cumulative");
        let has_count = stackable
            || object.has_flag("LiquidContainer")
            || object.has_flag("LiquidSource")
            || object.has_flag("Rune");
        let takeable = object.has_flag("Take");
        let container_capacity = if object.has_flag("Container") {
            object.attribute_u16("Capacity")
        } else {
            None
        };
        let is_expiring = object.has_flag("Expire");
        let expire_stop = object.has_flag("ExpireStop");
        let (expire_time_secs, expire_target) = if is_expiring {
            let expire_time = object.attribute_u16("TotalExpireTime").map(u32::from);
            let expire_target = object
                .attribute_u16("ExpireTarget")
                .and_then(|value| if value == 0 { None } else { Some(ItemTypeId(value)) });
            (expire_time, expire_target)
        } else {
            (None, None)
        };
        index.types.insert(
            *id,
            ItemType {
                id: *id,
                name: object.name.clone(),
                kind,
                stackable,
                has_count,
                container_capacity,
                takeable,
                is_expiring,
                expire_stop,
                expire_time_secs,
                expire_target,
            },
        );
    }
    index
}

fn item_kind_from_object(object: &ObjectType) -> ItemKind {
    if object.has_flag("Weapon")
        || object.attribute("WeaponType").is_some()
        || object.attribute("WeaponAttackValue").is_some()
        || object.attribute("WandAttackStrength").is_some()
        || object.attribute("ThrowAttackValue").is_some()
    {
        return ItemKind::Weapon;
    }
    if object.has_flag("Armor") || object.attribute("ArmorValue").is_some() {
        return ItemKind::Armor;
    }
    if object.has_flag("Rune") {
        return ItemKind::Rune;
    }
    if object.has_flag("Container") {
        return ItemKind::Container;
    }
    if object.has_flag("Food") || object.has_flag("Drink") {
        return ItemKind::Consumable;
    }
    if object.has_flag("Bank") || object.has_flag("Bottom") {
        return ItemKind::Ground;
    }
    ItemKind::Misc
}
