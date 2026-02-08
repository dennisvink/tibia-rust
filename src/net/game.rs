#![allow(dead_code)]

use crate::entities::creature::{Outfit, DEFAULT_OUTFIT};
use crate::entities::inventory::InventorySlot;
use crate::entities::item::{Item, ItemStack, ItemTypeId};
use crate::entities::player::{PlayerId, SkullState};
use crate::net::packet::PacketWriter;
use crate::world::item_types::ItemTypeIndex;
use crate::world::position::{Direction, Position};
use crate::world::state::{MonsterInstance, NpcInstance, ShopItemView, ShopSellEntry, WorldState};
use std::collections::HashMap;

pub const OPCODE_GRAPHICAL_EFFECT: u8 = 0x83;
pub const OPCODE_TEXTUAL_EFFECT: u8 = 0x84;
pub const OPCODE_MISSILE_EFFECT: u8 = 0x85;
pub const OPCODE_CREATURE_MARK: u8 = 0x86;
pub const OPCODE_CREATURE_HEALTH: u8 = 0x8c;
pub const OPCODE_CREATURE_LIGHT: u8 = 0x8d;
pub const OPCODE_CREATURE_OUTFIT: u8 = 0x8e;
pub const OPCODE_CREATURE_SPEED: u8 = 0x8f;
pub const OPCODE_CREATURE_SKULL: u8 = 0x90;
pub const OPCODE_CREATURE_PARTY: u8 = 0x91;
pub const OPCODE_PLAYER_DATA: u8 = 0xa0;
pub const OPCODE_PLAYER_SKILLS: u8 = 0xa1;
pub const OPCODE_PLAYER_STATE: u8 = 0xa2;
pub const OPCODE_CLEAR_TARGET: u8 = 0xa3;
pub const OPCODE_INIT_GAME: u8 = 0x0a;
pub const OPCODE_RIGHTS: u8 = 0x0b;
pub const OPCODE_PING: u8 = 0x1e;
pub const OPCODE_WORLD_LIGHT: u8 = 0x82;
pub const OPCODE_OUTFIT_DIALOG_LEGACY_MALE: u8 = 0x80;
pub const OPCODE_OUTFIT_DIALOG_LEGACY_FEMALE: u8 = 0x88;
pub const OPCODE_MAP_DESCRIPTION: u8 = 0x64;
pub const OPCODE_MAP_ROW_NORTH: u8 = 0x65;
pub const OPCODE_MAP_ROW_EAST: u8 = 0x66;
pub const OPCODE_MAP_ROW_SOUTH: u8 = 0x67;
pub const OPCODE_MAP_ROW_WEST: u8 = 0x68;
pub const OPCODE_FIELD_DATA: u8 = 0x69;
pub const OPCODE_TILE_ADD_THING: u8 = 0x6a;
pub const OPCODE_TILE_CHANGE_THING: u8 = 0x6b;
pub const OPCODE_TILE_REMOVE_THING: u8 = 0x6c;
pub const OPCODE_MOVE_CREATURE: u8 = 0x6d;
pub const OPCODE_OPEN_CONTAINER: u8 = 0x6e;
pub const OPCODE_CLOSE_CONTAINER: u8 = 0x6f;
pub const OPCODE_CONTAINER_ADD: u8 = 0x70;
pub const OPCODE_CONTAINER_UPDATE: u8 = 0x71;
pub const OPCODE_CONTAINER_REMOVE: u8 = 0x72;
pub const OPCODE_SHOP_OPEN: u8 = 0x7a;
pub const OPCODE_SHOP_SELL_LIST: u8 = 0x7b;
pub const OPCODE_SHOP_CLOSE: u8 = 0x7c;
pub const OPCODE_TRADE_OFFER: u8 = 0x7d;
pub const OPCODE_TRADE_COUNTER: u8 = 0x7e;
pub const OPCODE_TRADE_CLOSE: u8 = 0x7f;
pub const OPCODE_FLOOR_CHANGE_UP: u8 = 0xbe;
pub const OPCODE_FLOOR_CHANGE_DOWN: u8 = 0xbf;
pub const OPCODE_TALK: u8 = 0xaa;
pub const OPCODE_MESSAGE: u8 = 0xb4;
pub const OPCODE_SNAPBACK: u8 = 0xb5;
pub const OPCODE_CHANNEL_LIST: u8 = 0xab;
pub const OPCODE_OPEN_CHANNEL: u8 = 0xac;
pub const OPCODE_PRIVATE_CHANNEL: u8 = 0xad;
pub const OPCODE_OPEN_OWN_CHANNEL: u8 = 0xb2;
pub const OPCODE_CLOSE_CHANNEL: u8 = 0xb3;
pub const OPCODE_OPEN_REQUEST_QUEUE: u8 = 0xae;
pub const OPCODE_DELETE_REQUEST: u8 = 0xaf;
pub const OPCODE_FINISH_REQUEST: u8 = 0xb0;
pub const OPCODE_CLOSE_REQUEST: u8 = 0xb1;
pub const OPCODE_BUDDY_DATA: u8 = 0xd2;
pub const OPCODE_BUDDY_STATUS_ONLINE: u8 = 0xd3;
pub const OPCODE_BUDDY_STATUS_OFFLINE: u8 = 0xd4;
pub const OPCODE_INVENTORY_SET: u8 = 0x78;
pub const OPCODE_INVENTORY_RESET: u8 = 0x79;
pub const OPCODE_EDIT_TEXT: u8 = 0x96;
pub const OPCODE_EDIT_LIST: u8 = 0x97;
pub const OPCODE_OUTFIT_DIALOG: u8 = 0xc8;

const MAP_WIDTH: u8 = 18;
const MAP_HEIGHT: u8 = 14;
const MAX_FLOOR: i32 = 15;
const GROUND_LAYER: i32 = 7;
const UNDERGROUND_LAYER: i32 = 2;
const MAX_TILE_THINGS: usize = 10;
const MAX_CONTAINER_ITEMS: usize = 0x24;
const CREATURE_MARKER_NEW: u16 = 0x0061;
const CREATURE_MARKER_KNOWN: u16 = 0x0062;
const CREATURE_MARKER_TURN: u16 = 0x0063;
const DEFAULT_SPEED: u16 = 220;

const TALK_TYPES_POSITION: [u8; 5] = [0x01, 0x02, 0x03, 0x10, 0x11];
const TALK_TYPES_CHANNEL: [u8; 4] = [0x05, 0x0a, 0x0c, 0x0e];
const TALK_TYPES_TEXT: [u8; 6] = [0x04, 0x06, 0x07, 0x08, 0x09, 0x0b];

#[derive(Debug, Clone, Copy)]
pub struct ItemCodec<'a> {
    item_types: Option<&'a ItemTypeIndex>,
}

#[derive(Debug, Clone, Copy)]
pub enum TalkPayload<'a> {
    Position { position: Position, text: &'a str },
    Channel { channel_id: u16, text: &'a str },
    Text { text: &'a str, arg: Option<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelEntry {
    pub id: u16,
    pub name: String,
}

impl<'a> ItemCodec<'a> {
    pub fn new(item_types: Option<&'a ItemTypeIndex>) -> Self {
        Self { item_types }
    }

    pub fn write_item_stack(&self, writer: &mut PacketWriter, stack: &ItemStack) {
        writer.write_u16_le(stack.type_id.0);
        if self.should_write_count(stack.type_id, stack.count) {
            write_item_count(writer, stack.count);
        }
    }

    pub fn write_item(&self, writer: &mut PacketWriter, item: &Item) {
        writer.write_u16_le(item.type_id.0);
        if self.should_write_count(item.type_id, item.count) {
            write_item_count(writer, item.count);
            return;
        }
        if let Some(charges) = item.charges {
            write_item_count(writer, charges);
        }
    }

    fn should_write_count(&self, type_id: ItemTypeId, count: u16) -> bool {
        let has_count = self
            .item_types
            .and_then(|types| types.get(type_id))
            .map(|item| item.has_count)
            .unwrap_or(false);
        has_count || count > 1
    }
}

pub fn write_graphical_effect(
    writer: &mut PacketWriter,
    position: Position,
    effect_id: u16,
) {
    writer.write_u8(OPCODE_GRAPHICAL_EFFECT);
    write_position(writer, position);
    write_item_count(writer, effect_id);
}

pub fn write_textual_effect(
    writer: &mut PacketWriter,
    position: Position,
    color: u8,
    message: &str,
) {
    writer.write_u8(OPCODE_TEXTUAL_EFFECT);
    write_position(writer, position);
    writer.write_u8(color);
    writer.write_string_str(message);
}

pub fn write_edit_text(
    writer: &mut PacketWriter,
    window_id: u32,
    item_type: ItemTypeId,
    max_len: u16,
    text: &str,
    author: &str,
    date: &str,
) {
    writer.write_u8(OPCODE_EDIT_TEXT);
    writer.write_u32_le(window_id);
    writer.write_u16_le(item_type.0);
    writer.write_u16_le(max_len);
    writer.write_string_str(text);
    writer.write_string_str(author);
    writer.write_string_str(date);
}

pub fn write_edit_list(writer: &mut PacketWriter, list_type: u8, list_id: u32, text: &str) {
    writer.write_u8(OPCODE_EDIT_LIST);
    writer.write_u8(list_type);
    writer.write_u32_le(list_id);
    writer.write_string_str(text);
}

pub fn write_outfit_dialog(writer: &mut PacketWriter, outfit: Outfit) {
    writer.write_u8(OPCODE_OUTFIT_DIALOG);
    writer.write_u16_le(outfit.look_type);
    writer.write_u8(outfit.head);
    writer.write_u8(outfit.body);
    writer.write_u8(outfit.legs);
    writer.write_u8(outfit.feet);
    writer.write_u8(outfit.addons);
}

pub fn write_outfit_dialog_legacy(writer: &mut PacketWriter, opcode: u8, base_outfit_id: u16) {
    writer.write_u8(opcode);
    writer.write_u8(0);
    writer.write_u16_le(base_outfit_id);
}

pub fn write_missile_effect(
    writer: &mut PacketWriter,
    from: Position,
    to: Position,
    missile_id: u8,
) {
    writer.write_u8(OPCODE_MISSILE_EFFECT);
    write_position(writer, from);
    write_position(writer, to);
    writer.write_u8(missile_id);
}

pub fn write_creature_mark(
    writer: &mut PacketWriter,
    creature_id: u32,
    mark: u8,
) {
    writer.write_u8(OPCODE_CREATURE_MARK);
    writer.write_u32_le(creature_id);
    writer.write_u8(mark);
}

pub fn write_world_light(
    writer: &mut PacketWriter,
    level: u8,
    color: u8,
) {
    writer.write_u8(OPCODE_WORLD_LIGHT);
    writer.write_u8(level);
    writer.write_u8(color);
}

pub fn write_creature_health(
    writer: &mut PacketWriter,
    creature_id: u32,
    health: u8,
) {
    writer.write_u8(OPCODE_CREATURE_HEALTH);
    writer.write_u32_le(creature_id);
    writer.write_u8(health);
}

pub fn write_creature_light(
    writer: &mut PacketWriter,
    creature_id: u32,
    level: u8,
    color: u8,
) {
    writer.write_u8(OPCODE_CREATURE_LIGHT);
    writer.write_u32_le(creature_id);
    writer.write_u8(level);
    writer.write_u8(color);
}

pub fn write_creature_outfit(
    writer: &mut PacketWriter,
    creature_id: u32,
    outfit: Outfit,
) {
    writer.write_u8(OPCODE_CREATURE_OUTFIT);
    writer.write_u32_le(creature_id);
    write_outfit(writer, &outfit_snapshot(outfit));
}

pub fn write_creature_speed(
    writer: &mut PacketWriter,
    creature_id: u32,
    speed: u16,
) {
    writer.write_u8(OPCODE_CREATURE_SPEED);
    writer.write_u32_le(creature_id);
    writer.write_u16_le(speed);
}

pub fn write_creature_skull(
    writer: &mut PacketWriter,
    creature_id: u32,
    skull: u8,
) {
    writer.write_u8(OPCODE_CREATURE_SKULL);
    writer.write_u32_le(creature_id);
    writer.write_u8(skull);
}

pub fn write_creature_party(
    writer: &mut PacketWriter,
    creature_id: u32,
    party_mark: u8,
) {
    writer.write_u8(OPCODE_CREATURE_PARTY);
    writer.write_u32_le(creature_id);
    writer.write_u8(party_mark);
}

pub fn write_player_data(
    writer: &mut PacketWriter,
    player: &crate::entities::player::PlayerState,
    capacity: u32,
) {
    writer.write_u8(OPCODE_PLAYER_DATA);
    writer.write_u16_le(player.stats.health.min(u32::from(u16::MAX)) as u16);
    writer.write_u16_le(player.stats.max_health.min(u32::from(u16::MAX)) as u16);
    writer.write_u32_le(capacity);
    writer.write_u32_le(player.experience.min(u64::from(u32::MAX)) as u32);
    writer.write_u16_le(player.level);
    writer.write_u8(0);
    writer.write_u16_le(player.stats.mana.min(u32::from(u16::MAX)) as u16);
    writer.write_u16_le(player.stats.max_mana.min(u32::from(u16::MAX)) as u16);
    writer.write_u8(player.skills.magic.level.min(u16::from(u8::MAX)) as u8);
    writer.write_u8(player.skills.magic.progress);
    writer.write_u8(player.stats.soul.min(u32::from(u8::MAX)) as u8);
    writer.write_u16_le(0);
}

pub fn write_player_skills(
    writer: &mut PacketWriter,
    player: &crate::entities::player::PlayerState,
) {
    writer.write_u8(OPCODE_PLAYER_SKILLS);
    let skills = [
        player.skills.fist,
        player.skills.club,
        player.skills.sword,
        player.skills.axe,
        player.skills.distance,
        player.skills.shielding,
        player.skills.fishing,
    ];
    for skill in skills {
        writer.write_u8(skill.level.min(u16::from(u8::MAX)) as u8);
        writer.write_u8(skill.progress);
    }
}

pub fn write_player_state(writer: &mut PacketWriter, state: u8) {
    writer.write_u8(OPCODE_PLAYER_STATE);
    writer.write_u8(state);
}

pub fn write_clear_target(writer: &mut PacketWriter) {
    writer.write_u8(OPCODE_CLEAR_TARGET);
}

pub fn write_talk(
    writer: &mut PacketWriter,
    speaker_id: u32,
    name: &str,
    talk_type: u8,
    payload: TalkPayload<'_>,
) -> Result<(), String> {
    writer.write_u8(OPCODE_TALK);
    writer.write_u32_le(speaker_id);
    writer.write_string_str(name);
    writer.write_u8(talk_type);
    if TALK_TYPES_POSITION.contains(&talk_type) {
        let TalkPayload::Position { position, text } = payload else {
            return Err("talk payload requires position".to_string());
        };
        write_position(writer, position);
        writer.write_string_str(text);
        return Ok(());
    }
    if TALK_TYPES_CHANNEL.contains(&talk_type) {
        let TalkPayload::Channel { channel_id, text } = payload else {
            return Err("talk payload requires channel id".to_string());
        };
        writer.write_u16_le(channel_id);
        writer.write_string_str(text);
        return Ok(());
    }
    if TALK_TYPES_TEXT.contains(&talk_type) {
        let TalkPayload::Text { text, arg } = payload else {
            return Err("talk payload requires text".to_string());
        };
        if talk_type == 0x06 {
            let arg = arg.ok_or_else(|| "talk payload missing arg".to_string())?;
            writer.write_u32_le(arg);
        }
        writer.write_string_str(text);
        return Ok(());
    }
    Err(format!("unsupported talk type 0x{talk_type:02x}"))
}

pub fn write_channel_list(writer: &mut PacketWriter, channels: &[ChannelEntry]) {
    writer.write_u8(OPCODE_CHANNEL_LIST);
    writer.write_u16_le(channels.len() as u16);
    for entry in channels {
        writer.write_u16_le(entry.id);
        writer.write_string_str(&entry.name);
    }
}

pub fn write_open_channel(writer: &mut PacketWriter, channel_id: u16, name: &str) {
    writer.write_u8(OPCODE_OPEN_CHANNEL);
    writer.write_u16_le(channel_id);
    writer.write_string_str(name);
}

pub fn write_private_channel(writer: &mut PacketWriter, name: &str) {
    writer.write_u8(OPCODE_PRIVATE_CHANNEL);
    writer.write_string_str(name);
}

pub fn write_open_own_channel(writer: &mut PacketWriter, channel_id: u16, name: &str) {
    writer.write_u8(OPCODE_OPEN_OWN_CHANNEL);
    writer.write_u16_le(channel_id);
    writer.write_string_str(name);
}

pub fn write_close_channel(writer: &mut PacketWriter, channel_id: u16) {
    writer.write_u8(OPCODE_CLOSE_CHANNEL);
    writer.write_u16_le(channel_id);
}

pub fn write_open_request_queue(writer: &mut PacketWriter) {
    writer.write_u8(OPCODE_OPEN_REQUEST_QUEUE);
    writer.write_u16_le(0x0003);
}

fn write_request_text(writer: &mut PacketWriter, text: &str) {
    let bytes = text.as_bytes();
    let len = bytes.len().min(u16::MAX as usize);
    writer.write_u16_le(len as u16);
    writer.write_bytes(&bytes[..len]);
}

pub fn write_delete_request(writer: &mut PacketWriter, name: &str) {
    writer.write_u8(OPCODE_DELETE_REQUEST);
    write_request_text(writer, name);
}

pub fn write_finish_request(writer: &mut PacketWriter, name: &str) {
    writer.write_u8(OPCODE_FINISH_REQUEST);
    write_request_text(writer, name);
}

pub fn write_close_request(writer: &mut PacketWriter) {
    writer.write_u8(OPCODE_CLOSE_REQUEST);
}

pub fn write_message(writer: &mut PacketWriter, message_type: u8, text: &str) {
    writer.write_u8(OPCODE_MESSAGE);
    writer.write_u8(message_type);
    writer.write_string_str(text);
}

pub fn write_snapback(writer: &mut PacketWriter, mode: u8) {
    writer.write_u8(OPCODE_SNAPBACK);
    writer.write_u8(mode);
}

pub fn write_buddy_data(writer: &mut PacketWriter, buddy_id: u32, name: &str, online: bool) {
    writer.write_u8(OPCODE_BUDDY_DATA);
    writer.write_u32_le(buddy_id);
    writer.write_string_str(name);
    writer.write_u8(if online { 1 } else { 0 });
}

pub fn write_buddy_status(writer: &mut PacketWriter, buddy_id: u32, online: bool) {
    let opcode = if online {
        OPCODE_BUDDY_STATUS_ONLINE
    } else {
        OPCODE_BUDDY_STATUS_OFFLINE
    };
    writer.write_u8(opcode);
    writer.write_u32_le(buddy_id);
}

pub fn write_position(writer: &mut PacketWriter, position: Position) {
    writer.write_u16_le(position.x);
    writer.write_u16_le(position.y);
    writer.write_u8(position.z);
}

pub fn write_init_game(writer: &mut PacketWriter, player_id: u32, has_rights: bool) {
    writer.write_u8(OPCODE_INIT_GAME);
    writer.write_u32_le(player_id);
    writer.write_u16_le(0);
    writer.write_u8(if has_rights { 1 } else { 0 });
}

pub fn write_rights(writer: &mut PacketWriter, mask: u8) {
    writer.write_u8(OPCODE_RIGHTS);
    writer.write_u8(mask);
}

pub fn write_ping(writer: &mut PacketWriter) {
    writer.write_u8(OPCODE_PING);
}

pub fn write_inventory_set(
    writer: &mut PacketWriter,
    slot: InventorySlot,
    item: &ItemStack,
    item_types: Option<&ItemTypeIndex>,
) {
    writer.write_u8(OPCODE_INVENTORY_SET);
    writer.write_u8(inventory_slot_index(slot));
    ItemCodec::new(item_types).write_item_stack(writer, item);
}

pub fn write_inventory_reset(writer: &mut PacketWriter, slot: InventorySlot) {
    writer.write_u8(OPCODE_INVENTORY_RESET);
    writer.write_u8(inventory_slot_index(slot));
}

pub fn write_open_container(
    writer: &mut PacketWriter,
    open: &crate::entities::player::OpenContainer,
    item_types: Option<&ItemTypeIndex>,
) {
    writer.write_u8(OPCODE_OPEN_CONTAINER);
    writer.write_u8(open.container_id);
    writer.write_u16_le(open.item_type.0);
    writer.write_string_str(&open.name);
    writer.write_u8(open.capacity);
    writer.write_u8(if open.has_parent { 1 } else { 0 });
    let size = open.items.len().min(MAX_CONTAINER_ITEMS);
    writer.write_u8(size as u8);
    let codec = ItemCodec::new(item_types);
    for item in open.items.iter().take(size) {
        codec.write_item_stack(writer, item);
    }
}

pub fn write_close_container(writer: &mut PacketWriter, container_id: u8) {
    writer.write_u8(OPCODE_CLOSE_CONTAINER);
    writer.write_u8(container_id);
}

pub fn write_container_add(
    writer: &mut PacketWriter,
    container_id: u8,
    item: &ItemStack,
    item_types: Option<&ItemTypeIndex>,
) {
    writer.write_u8(OPCODE_CONTAINER_ADD);
    writer.write_u8(container_id);
    ItemCodec::new(item_types).write_item_stack(writer, item);
}

pub fn write_container_update(
    writer: &mut PacketWriter,
    container_id: u8,
    slot: u8,
    item: &ItemStack,
    item_types: Option<&ItemTypeIndex>,
) {
    writer.write_u8(OPCODE_CONTAINER_UPDATE);
    writer.write_u8(container_id);
    writer.write_u8(slot);
    ItemCodec::new(item_types).write_item_stack(writer, item);
}

pub fn write_container_remove(writer: &mut PacketWriter, container_id: u8, slot: u8) {
    writer.write_u8(OPCODE_CONTAINER_REMOVE);
    writer.write_u8(container_id);
    writer.write_u8(slot);
}

pub fn write_shop_open(writer: &mut PacketWriter, items: &[ShopItemView]) {
    writer.write_u8(OPCODE_SHOP_OPEN);
    let count = items.len().min(u8::MAX as usize);
    writer.write_u8(count as u8);
    for item in items.iter().take(count) {
        write_shop_item(writer, item);
    }
}

pub fn write_shop_sell_list(writer: &mut PacketWriter, money: u32, entries: &[ShopSellEntry]) {
    writer.write_u8(OPCODE_SHOP_SELL_LIST);
    writer.write_u32_le(money);
    let count = entries.len().min(u8::MAX as usize);
    writer.write_u8(count as u8);
    for entry in entries.iter().take(count) {
        writer.write_u16_le(entry.type_id.0);
        writer.write_u8(entry.count);
    }
}

pub fn write_shop_close(writer: &mut PacketWriter) {
    writer.write_u8(OPCODE_SHOP_CLOSE);
}

fn write_shop_item(writer: &mut PacketWriter, item: &ShopItemView) {
    writer.write_u16_le(item.type_id.0);
    writer.write_u8(item.sub_type);
    writer.write_string_str(&item.description);
    writer.write_u32_le(item.weight);
    writer.write_u32_le(item.buy_price);
    writer.write_u32_le(item.sell_price);
}

pub fn write_trade_offer(
    writer: &mut PacketWriter,
    counter_offer: bool,
    name: &str,
    items: &[ItemStack],
    item_types: Option<&ItemTypeIndex>,
) {
    writer.write_u8(if counter_offer {
        OPCODE_TRADE_COUNTER
    } else {
        OPCODE_TRADE_OFFER
    });
    writer.write_string_str(name);
    let count = items.len().min(u8::MAX as usize);
    writer.write_u8(count as u8);
    let codec = ItemCodec::new(item_types);
    for item in items.iter().take(count) {
        codec.write_item_stack(writer, item);
    }
}

pub fn write_trade_close(writer: &mut PacketWriter) {
    writer.write_u8(OPCODE_TRADE_CLOSE);
}

pub fn write_empty_map_description(writer: &mut PacketWriter, position: Position) {
    writer.write_u8(OPCODE_MAP_DESCRIPTION);
    write_position(writer, position);
    let (start, end, step) = floor_range(position.z);
    let tile_count = u16::from(MAP_WIDTH) * u16::from(MAP_HEIGHT);
    let skip = u8::try_from(tile_count.saturating_sub(1)).unwrap_or(u8::MAX);
    let marker = 0xff00u16 | u16::from(skip);
    let mut floor = start;
    while floor != end + step {
        writer.write_u16_le(marker);
        floor += step;
    }
}

#[derive(Debug, Clone)]
struct OutfitSnapshot {
    look_type: u8,
    head: u8,
    body: u8,
    legs: u8,
    feet: u8,
    addons: u8,
    look_item: u16,
}

#[derive(Debug, Clone)]
struct CreatureSnapshot {
    id: u32,
    name: String,
    health_percent: u8,
    direction: u8,
    outfit: OutfitSnapshot,
    light_level: u8,
    light_color: u8,
    speed: u16,
    skull: u8,
    party_mark: u8,
    known: bool,
    removed_id: u32,
}

#[derive(Debug, Clone)]
enum MapThing {
    Item(ItemStack),
    Creature(CreatureSnapshot),
}

pub fn write_map_description(
    writer: &mut PacketWriter,
    world: &WorldState,
    position: Position,
    viewer_id: PlayerId,
) {
    writer.write_u8(OPCODE_MAP_DESCRIPTION);
    write_position(writer, position);
    let codec = ItemCodec::new(world.item_types.as_ref());
    let creatures = collect_creatures(world, viewer_id);
    let origin = map_origin(position);
    write_map_floors(
        writer,
        world,
        &codec,
        &creatures,
        origin,
        MAP_WIDTH,
        MAP_HEIGHT,
        position.z,
    );
}

pub fn write_floor_change(
    writer: &mut PacketWriter,
    world: &WorldState,
    position: Position,
    moving_up: bool,
    viewer_id: PlayerId,
) {
    writer.write_u8(if moving_up {
        OPCODE_FLOOR_CHANGE_UP
    } else {
        OPCODE_FLOOR_CHANGE_DOWN
    });
    let Some((start, end, step)) = floor_change_range(position.z, moving_up) else {
        return;
    };
    let codec = ItemCodec::new(world.item_types.as_ref());
    let creatures = collect_creatures(world, viewer_id);
    let origin = map_origin(position);
    write_map_floor_range(
        writer,
        world,
        &codec,
        &creatures,
        origin,
        MAP_WIDTH,
        MAP_HEIGHT,
        position.z,
        start,
        end,
        step,
    );
}

pub fn write_map_row(
    writer: &mut PacketWriter,
    world: &WorldState,
    opcode: u8,
    position: Position,
    viewer_id: PlayerId,
) -> Result<(), String> {
    let (origin, width, height) = map_row_origin(position, opcode)?;
    writer.write_u8(opcode);
    let codec = ItemCodec::new(world.item_types.as_ref());
    let creatures = collect_creatures(world, viewer_id);
    write_map_floors(
        writer,
        world,
        &codec,
        &creatures,
        origin,
        width,
        height,
        position.z,
    );
    Ok(())
}

pub fn write_field_data(
    writer: &mut PacketWriter,
    world: &WorldState,
    position: Position,
    viewer_id: PlayerId,
) {
    writer.write_u8(OPCODE_FIELD_DATA);
    write_position(writer, position);
    let codec = ItemCodec::new(world.item_types.as_ref());
    let creatures = collect_creatures(world, viewer_id);
    let things = build_tile_things(world, &creatures, position);
    if things.is_empty() {
        writer.write_u16_le(0xff00);
        return;
    }
    for thing in things {
        write_map_thing(writer, &codec, &thing);
    }
    writer.write_u16_le(0xff00);
}

pub fn write_move_creature(
    writer: &mut PacketWriter,
    from: Position,
    stack_pos: u8,
    to: Position,
) {
    writer.write_u8(OPCODE_MOVE_CREATURE);
    write_position(writer, from);
    writer.write_u8(stack_pos);
    write_position(writer, to);
}

pub fn write_tile_remove(writer: &mut PacketWriter, position: Position, stack_pos: u8) {
    writer.write_u8(OPCODE_TILE_REMOVE_THING);
    write_position(writer, position);
    writer.write_u8(stack_pos);
}

pub fn write_tile_add_npc(writer: &mut PacketWriter, position: Position, npc: &NpcInstance) {
    writer.write_u8(OPCODE_TILE_ADD_THING);
    write_position(writer, position);
    let creature = snapshot_npc(npc);
    write_creature(writer, &creature);
}

pub fn write_tile_add_monster(
    writer: &mut PacketWriter,
    position: Position,
    monster: &MonsterInstance,
) {
    writer.write_u8(OPCODE_TILE_ADD_THING);
    write_position(writer, position);
    let creature = snapshot_monster(monster);
    write_creature(writer, &creature);
}

pub fn write_creature_turn(
    writer: &mut PacketWriter,
    position: Position,
    stack_pos: u8,
    creature_id: u32,
    direction: Direction,
) {
    writer.write_u8(OPCODE_TILE_CHANGE_THING);
    write_position(writer, position);
    writer.write_u8(stack_pos);
    writer.write_u16_le(CREATURE_MARKER_TURN);
    writer.write_u32_le(creature_id);
    writer.write_u8(direction_to_u8(direction));
}

pub fn creature_stack_pos(
    world: &WorldState,
    position: Position,
    creature_id: u32,
) -> u8 {
    let items_len = world
        .map
        .tile(position)
        .map(|tile| tile.items.len())
        .unwrap_or(0);
    let mut creature_ids = Vec::new();
    for player in world.players.values() {
        if player.position == position {
            creature_ids.push(player.id.0);
        }
    }
    for npc in world.npcs.values() {
        if npc.position == position {
            creature_ids.push(npc.id.0);
        }
    }
    for monster in world.monsters.values() {
        if monster.position == position {
            creature_ids.push(monster.id.0);
        }
    }
    if !creature_ids.iter().any(|id| *id == creature_id) {
        creature_ids.push(creature_id);
    }
    creature_ids.sort_unstable();
    let index = creature_ids
        .iter()
        .position(|id| *id == creature_id)
        .unwrap_or(0);
    let stack = items_len.saturating_add(index);
    stack.min(u8::MAX as usize) as u8
}

fn collect_creatures(
    world: &WorldState,
    viewer_id: PlayerId,
) -> HashMap<Position, Vec<CreatureSnapshot>> {
    let mut map: HashMap<Position, Vec<CreatureSnapshot>> = HashMap::new();
    for player in world.players.values() {
        let party_mark = world.party_mark_for_viewer(viewer_id, player.id);
        let snapshot = snapshot_player(player, party_mark);
        map.entry(player.position).or_default().push(snapshot);
    }
    for npc in world.npcs.values() {
        let snapshot = snapshot_npc(npc);
        map.entry(npc.position).or_default().push(snapshot);
    }
    for monster in world.monsters.values() {
        let snapshot = snapshot_monster(monster);
        map.entry(monster.position).or_default().push(snapshot);
    }
    for creatures in map.values_mut() {
        creatures.sort_by_key(|creature| creature.id);
    }
    map
}

fn snapshot_player(
    player: &crate::entities::player::PlayerState,
    party_mark: u8,
) -> CreatureSnapshot {
    let mut outfit = player.current_outfit;
    if outfit.look_type == 0 {
        outfit = DEFAULT_OUTFIT;
    }
    let (light_level, light_color) = player
        .light_effect
        .map(|effect| (effect.level, effect.color))
        .unwrap_or((0, 0));
    CreatureSnapshot {
        id: player.id.0,
        name: player.name.clone(),
        health_percent: health_percent(player.stats.health, player.stats.max_health),
        direction: direction_to_u8(player.direction),
        outfit: outfit_snapshot(outfit),
        light_level,
        light_color,
        speed: player
            .speed_effect
            .map(|effect| effect.speed)
            .unwrap_or(DEFAULT_SPEED),
        skull: skull_to_u8(player.pvp.skull),
        party_mark,
        known: false,
        removed_id: 0,
    }
}

fn snapshot_monster(
    monster: &crate::world::state::MonsterInstance,
) -> CreatureSnapshot {
    let outfit = monster.outfit;
    CreatureSnapshot {
        id: monster.id.0,
        name: monster.name.clone(),
        health_percent: health_percent(monster.stats.health, monster.stats.max_health),
        direction: direction_to_u8(monster.direction),
        outfit: outfit_snapshot(outfit),
        light_level: 0,
        light_color: 0,
        speed: monster.speed,
        skull: 0,
        party_mark: 0,
        known: false,
        removed_id: 0,
    }
}

fn snapshot_npc(npc: &crate::world::state::NpcInstance) -> CreatureSnapshot {
    CreatureSnapshot {
        id: npc.id.0,
        name: npc.name.clone(),
        health_percent: 100,
        direction: direction_to_u8(npc.direction),
        outfit: outfit_snapshot(npc.outfit),
        light_level: 0,
        light_color: 0,
        speed: DEFAULT_SPEED,
        skull: 0,
        party_mark: 0,
        known: false,
        removed_id: 0,
    }
}

fn write_map_floors(
    writer: &mut PacketWriter,
    world: &WorldState,
    codec: &ItemCodec<'_>,
    creatures: &HashMap<Position, Vec<CreatureSnapshot>>,
    origin: Position,
    width: u8,
    height: u8,
    player_z: u8,
) {
    let (start, end, step) = floor_range(player_z);
    write_map_floor_range(
        writer, world, codec, creatures, origin, width, height, player_z, start, end, step,
    );
}

fn write_map_floor_range(
    writer: &mut PacketWriter,
    world: &WorldState,
    codec: &ItemCodec<'_>,
    creatures: &HashMap<Position, Vec<CreatureSnapshot>>,
    origin: Position,
    width: u8,
    height: u8,
    player_z: u8,
    start: i32,
    end: i32,
    step: i32,
) {
    let mut z = start;
    while z != end + step {
        if !(0..=MAX_FLOOR).contains(&z) {
            break;
        }
        let z_u8 = z as u8;
        let offset = floor_offset(player_z, z_u8);
        write_floor_tiles(writer, world, codec, creatures, origin, width, height, z_u8, offset);
        z += step;
    }
}

fn write_floor_tiles(
    writer: &mut PacketWriter,
    world: &WorldState,
    codec: &ItemCodec<'_>,
    creatures: &HashMap<Position, Vec<CreatureSnapshot>>,
    origin: Position,
    width: u8,
    height: u8,
    z: u8,
    offset: i32,
) {
    let total = usize::from(width) * usize::from(height);
    let mut tiles: Vec<Option<Vec<MapThing>>> = Vec::with_capacity(total);
    for index in 0..total {
        let (dx, dy) = index_to_coord(index, height);
        let Some(position) = map_position(origin, dx, dy, z, offset) else {
            tiles.push(None);
            continue;
        };
        let things = build_tile_things(world, creatures, position);
        if things.is_empty() {
            tiles.push(None);
        } else {
            tiles.push(Some(things));
        }
    }

    let mut index = 0usize;
    while index < total {
        if tiles[index].is_none() {
            let mut run = 1usize;
            while index + run < total && tiles[index + run].is_none() && run < 256 {
                run += 1;
            }
            let skip = (run - 1) as u16;
            writer.write_u16_le(0xff00 | skip);
            index += run;
            continue;
        }

        let things = tiles[index].as_ref().expect("tile things");
        for thing in things {
            write_map_thing(writer, codec, thing);
        }

        let mut skip = 0usize;
        while skip < 255 && index + 1 + skip < total && tiles[index + 1 + skip].is_none() {
            skip += 1;
        }
        writer.write_u16_le(0xff00 | (skip as u16));
        index += 1 + skip;
    }
}

fn build_tile_things(
    world: &WorldState,
    creatures: &HashMap<Position, Vec<CreatureSnapshot>>,
    position: Position,
) -> Vec<MapThing> {
    let mut things = Vec::new();
    if let Some(tile) = world.map.tile(position) {
        for item in &tile.items {
            things.push(MapThing::Item(item.clone()));
        }
    }
    if let Some(entries) = creatures.get(&position) {
        for creature in entries {
            things.push(MapThing::Creature(creature.clone()));
        }
    }
    if things.len() > MAX_TILE_THINGS {
        things.truncate(MAX_TILE_THINGS);
    }
    things
}

fn write_map_thing(writer: &mut PacketWriter, codec: &ItemCodec<'_>, thing: &MapThing) {
    match thing {
        MapThing::Item(item) => codec.write_item_stack(writer, item),
        MapThing::Creature(creature) => write_creature(writer, creature),
    }
}

fn write_creature(writer: &mut PacketWriter, creature: &CreatureSnapshot) {
    let marker = if creature.known {
        CREATURE_MARKER_KNOWN
    } else {
        CREATURE_MARKER_NEW
    };
    writer.write_u16_le(marker);
    if !creature.known {
        writer.write_u32_le(creature.removed_id);
    }
    writer.write_u32_le(creature.id);
    if !creature.known {
        writer.write_string_str(&creature.name);
    }
    writer.write_u8(creature.health_percent);
    writer.write_u8(creature.direction);
    write_outfit(writer, &creature.outfit);
    writer.write_u8(creature.light_level);
    writer.write_u8(creature.light_color);
    writer.write_u16_le(creature.speed);
    writer.write_u8(creature.skull);
    writer.write_u8(creature.party_mark);
}

fn write_outfit(writer: &mut PacketWriter, outfit: &OutfitSnapshot) {
    writer.write_u8(outfit.look_type);
    if outfit.look_type != 0 {
        writer.write_u8(outfit.head);
        writer.write_u8(outfit.body);
        writer.write_u8(outfit.legs);
        writer.write_u8(outfit.feet);
        writer.write_u8(outfit.addons);
    } else {
        writer.write_u16_le(outfit.look_item);
    }
}

fn outfit_snapshot(outfit: Outfit) -> OutfitSnapshot {
    if outfit.look_type > u16::from(u8::MAX) {
        return OutfitSnapshot {
            look_type: 0,
            head: 0,
            body: 0,
            legs: 0,
            feet: 0,
            addons: 0,
            look_item: 0,
        };
    }
    let look_type = outfit.look_type as u8;
    if look_type == 0 {
        OutfitSnapshot {
            look_type: 0,
            head: 0,
            body: 0,
            legs: 0,
            feet: 0,
            addons: 0,
            look_item: outfit.look_item,
        }
    } else {
        OutfitSnapshot {
            look_type,
            head: outfit.head,
            body: outfit.body,
            legs: outfit.legs,
            feet: outfit.feet,
            addons: outfit.addons,
            look_item: 0,
        }
    }
}

fn health_percent(health: u32, max_health: u32) -> u8 {
    let max = max_health.max(1);
    (health.saturating_mul(100) / max).min(100) as u8
}

fn skull_to_u8(skull: SkullState) -> u8 {
    match skull {
        SkullState::None => 0,
        SkullState::White => 1,
        SkullState::Red => 2,
        SkullState::Black => 3,
    }
}

fn direction_to_u8(direction: Direction) -> u8 {
    match direction {
        Direction::North => 0,
        Direction::East => 1,
        Direction::South => 2,
        Direction::West => 3,
        Direction::Northeast => 4,
        Direction::Southeast => 5,
        Direction::Southwest => 6,
        Direction::Northwest => 7,
    }
}

fn map_row_origin(position: Position, opcode: u8) -> Result<(Position, u8, u8), String> {
    match opcode {
        OPCODE_MAP_ROW_NORTH => Ok((
            Position {
                x: position.x.saturating_sub(8),
                y: position.y.saturating_sub(6),
                z: position.z,
            },
            MAP_WIDTH,
            1,
        )),
        OPCODE_MAP_ROW_EAST => Ok((
            Position {
                x: position.x.saturating_add(9),
                y: position.y.saturating_sub(6),
                z: position.z,
            },
            1,
            MAP_HEIGHT,
        )),
        OPCODE_MAP_ROW_SOUTH => Ok((
            Position {
                x: position.x.saturating_sub(8),
                y: position.y.saturating_add(7),
                z: position.z,
            },
            MAP_WIDTH,
            1,
        )),
        OPCODE_MAP_ROW_WEST => Ok((
            Position {
                x: position.x.saturating_sub(8),
                y: position.y.saturating_sub(6),
                z: position.z,
            },
            1,
            MAP_HEIGHT,
        )),
        _ => Err("unsupported map row opcode".to_string()),
    }
}

fn map_origin(position: Position) -> Position {
    let offset_x = u16::from((MAP_WIDTH - 1) / 2);
    let offset_y = u16::from((MAP_HEIGHT - 1) / 2);
    Position {
        x: position.x.saturating_sub(offset_x),
        y: position.y.saturating_sub(offset_y),
        z: position.z,
    }
}

fn map_position(
    origin: Position,
    dx: u8,
    dy: u8,
    z: u8,
    offset: i32,
) -> Option<Position> {
    let x = i32::from(origin.x) + i32::from(dx) + offset;
    let y = i32::from(origin.y) + i32::from(dy) + offset;
    if x < 0 || y < 0 {
        return None;
    }
    if x > i32::from(u16::MAX) || y > i32::from(u16::MAX) {
        return None;
    }
    Some(Position {
        x: x as u16,
        y: y as u16,
        z,
    })
}

pub fn position_in_viewport(center: Position, position: Position) -> bool {
    if !floor_visible(center.z, position.z) {
        return false;
    }
    let origin = map_origin(center);
    let offset = floor_offset(center.z, position.z);
    let dx = i32::from(position.x) - i32::from(origin.x) - offset;
    let dy = i32::from(position.y) - i32::from(origin.y) - offset;
    dx >= 0
        && dy >= 0
        && dx < i32::from(MAP_WIDTH)
        && dy < i32::from(MAP_HEIGHT)
}

fn index_to_coord(index: usize, height: u8) -> (u8, u8) {
    let height = usize::from(height);
    let dx = index / height;
    let dy = index % height;
    (dx as u8, dy as u8)
}

fn write_item_count(writer: &mut PacketWriter, count: u16) {
    writer.write_u8(count.min(u16::from(u8::MAX)) as u8);
}

fn inventory_slot_index(slot: InventorySlot) -> u8 {
    match slot {
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

fn floor_range(player_z: u8) -> (i32, i32, i32) {
    let z = i32::from(player_z);
    if z > GROUND_LAYER {
        let start = z - 2;
        let end = (z + 2).min(MAX_FLOOR);
        (start, end, 1)
    } else {
        (GROUND_LAYER, 0, -1)
    }
}

fn floor_change_range(player_z: u8, moving_up: bool) -> Option<(i32, i32, i32)> {
    let z = i32::from(player_z);
    if moving_up {
        if z == GROUND_LAYER {
            Some((GROUND_LAYER - UNDERGROUND_LAYER, 0, -1))
        } else if z > GROUND_LAYER {
            let start = z - 2;
            Some((start, start, -1))
        } else {
            None
        }
    } else if z == GROUND_LAYER + 1 {
        Some((z, z + UNDERGROUND_LAYER, 1))
    } else if z > GROUND_LAYER + 1 && z + 2 <= MAX_FLOOR {
        let start = z + 2;
        Some((start, start, 1))
    } else {
        None
    }
}

fn floor_visible(player_z: u8, z: u8) -> bool {
    let (start, end, step) = floor_range(player_z);
    let z = i32::from(z);
    if step > 0 {
        z >= start && z <= end
    } else {
        z <= start && z >= end
    }
}

fn floor_offset(player_z: u8, z: u8) -> i32 {
    i32::from(player_z) - i32::from(z)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::packet::PacketWriter;
    use crate::world::item_types::{ItemType, ItemTypeIndex};

    #[test]
    fn write_graphical_effect_packet() {
        let mut writer = PacketWriter::new();
        write_graphical_effect(
            &mut writer,
            Position { x: 100, y: 200, z: 7 },
            15,
        );
        assert_eq!(
            writer.as_slice(),
            &[0x83, 0x64, 0x00, 0xc8, 0x00, 0x07, 0x0f]
        );
    }

    #[test]
    fn write_textual_effect_packet() {
        let mut writer = PacketWriter::new();
        write_textual_effect(
            &mut writer,
            Position { x: 1, y: 2, z: 3 },
            0x05,
            "hi",
        );
        assert_eq!(
            writer.as_slice(),
            &[0x84, 0x01, 0x00, 0x02, 0x00, 0x03, 0x05, 0x02, 0x00, b'h', b'i']
        );
    }

    #[test]
    fn write_missile_effect_packet() {
        let mut writer = PacketWriter::new();
        write_missile_effect(
            &mut writer,
            Position { x: 5, y: 6, z: 7 },
            Position { x: 8, y: 9, z: 10 },
            0x2a,
        );
        assert_eq!(
            writer.as_slice(),
            &[
                0x85, 0x05, 0x00, 0x06, 0x00, 0x07, 0x08, 0x00, 0x09, 0x00, 0x0a, 0x2a
            ]
        );
    }

    #[test]
    fn write_move_creature_packet() {
        let mut writer = PacketWriter::new();
        write_move_creature(
            &mut writer,
            Position { x: 1, y: 2, z: 3 },
            4,
            Position { x: 5, y: 6, z: 7 },
        );
        assert_eq!(
            writer.as_slice(),
            &[0x6d, 0x01, 0x00, 0x02, 0x00, 0x03, 0x04, 0x05, 0x00, 0x06, 0x00, 0x07]
        );
    }

    #[test]
    fn position_in_viewport_matches_floor_offset() {
        let center = Position { x: 100, y: 200, z: 7 };
        assert!(position_in_viewport(
            center,
            Position {
                x: 100,
                y: 200,
                z: 7
            }
        ));
        assert!(position_in_viewport(
            center,
            Position {
                x: 100,
                y: 200,
                z: 6
            }
        ));
        assert!(position_in_viewport(
            center,
            Position {
                x: 101,
                y: 201,
                z: 6
            }
        ));
    }

    #[test]
    fn write_creature_status_packets() {
        let mut writer = PacketWriter::new();
        write_creature_health(&mut writer, 0x01020304, 0x63);
        write_creature_light(&mut writer, 0x0a0b0c0d, 0x01, 0x02);
        write_creature_speed(&mut writer, 0x0e0f1011, 0x2233);
        write_creature_skull(&mut writer, 0x12131415, 0x06);
        write_creature_party(&mut writer, 0x16171819, 0x07);
        assert_eq!(
            writer.as_slice(),
            &[
                0x8c, 0x04, 0x03, 0x02, 0x01, 0x63, 0x8d, 0x0d, 0x0c, 0x0b, 0x0a,
                0x01, 0x02, 0x8f, 0x11, 0x10, 0x0f, 0x0e, 0x33, 0x22, 0x90, 0x15,
                0x14, 0x13, 0x12, 0x06, 0x91, 0x19, 0x18, 0x17, 0x16, 0x07
            ]
        );
    }

    #[test]
    fn write_message_packet() {
        let mut writer = PacketWriter::new();
        write_message(&mut writer, 0x14, "Idle warning");
        assert_eq!(
            writer.as_slice(),
            &[
                0xb4, 0x14, 0x0c, 0x00, b'I', b'd', b'l', b'e', b' ', b'w', b'a', b'r', b'n',
                b'i', b'n', b'g'
            ]
        );
    }

    #[test]
    fn write_item_stack_writes_count_for_stackables() {
        let mut item_types = ItemTypeIndex::default();
        item_types
            .insert(ItemType {
                id: ItemTypeId(0x1234),
                name: "Gold".to_string(),
                kind: crate::entities::item::ItemKind::Misc,
                stackable: true,
                has_count: true,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert item");
        let codec = ItemCodec::new(Some(&item_types));
        let mut writer = PacketWriter::new();
        codec.write_item_stack(
            &mut writer,
            &ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(0x1234),
                count: 10,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
        );
        assert_eq!(writer.as_slice(), &[0x34, 0x12, 0x0a]);
    }

    #[test]
    fn write_item_stack_clamps_count_to_u8() {
        let mut item_types = ItemTypeIndex::default();
        item_types
            .insert(ItemType {
                id: ItemTypeId(0x2222),
                name: "Bolt".to_string(),
                kind: crate::entities::item::ItemKind::Misc,
                stackable: true,
                has_count: true,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert item");
        let codec = ItemCodec::new(Some(&item_types));
        let mut writer = PacketWriter::new();
        codec.write_item_stack(
            &mut writer,
            &ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(0x2222),
                count: 300,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
        );
        assert_eq!(writer.as_slice(), &[0x22, 0x22, 0xff]);
    }

    #[test]
    fn write_item_stack_omits_count_for_singular_non_stackable() {
        let mut item_types = ItemTypeIndex::default();
        item_types
            .insert(ItemType {
                id: ItemTypeId(0x3333),
                name: "Sword".to_string(),
                kind: crate::entities::item::ItemKind::Weapon,
                stackable: false,
                has_count: false,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert item");
        let codec = ItemCodec::new(Some(&item_types));
        let mut writer = PacketWriter::new();
        codec.write_item_stack(
            &mut writer,
            &ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(0x3333),
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
        );
        assert_eq!(writer.as_slice(), &[0x33, 0x33]);
    }

    #[test]
    fn write_item_writes_charges_for_non_stackable() {
        let codec = ItemCodec::new(None);
        let mut writer = PacketWriter::new();
        codec.write_item(
            &mut writer,
            &Item {
                id: crate::entities::item::ItemId(1),
                type_id: ItemTypeId(0x4444),
                kind: crate::entities::item::ItemKind::Rune,
                count: 1,
                charges: Some(7),
            },
        );
        assert_eq!(writer.as_slice(), &[0x44, 0x44, 0x07]);
    }

    #[test]
    fn write_inventory_set_packet() {
        let mut writer = PacketWriter::new();
        write_inventory_set(
            &mut writer,
            InventorySlot::Head,
            &ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(0x1234),
                count: 2,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
            None,
        );
        assert_eq!(writer.as_slice(), &[0x78, 0x00, 0x34, 0x12, 0x02]);
    }

    #[test]
    fn write_inventory_reset_packet() {
        let mut writer = PacketWriter::new();
        write_inventory_reset(&mut writer, InventorySlot::RightHand);
        assert_eq!(writer.as_slice(), &[0x79, 0x04]);
    }

    #[test]
    fn floor_change_range_surface_to_underground() {
        assert_eq!(floor_change_range(8, false), Some((8, 10, 1)));
        assert_eq!(floor_change_range(7, true), Some((5, 0, -1)));
    }

    #[test]
    fn floor_change_range_underground_adjacent() {
        assert_eq!(floor_change_range(9, false), Some((11, 11, 1)));
        assert_eq!(floor_change_range(9, true), Some((7, 7, -1)));
    }

    #[test]
    fn floor_change_range_noop_outside_bounds() {
        assert_eq!(floor_change_range(6, true), None);
        assert_eq!(floor_change_range(15, false), None);
    }

    #[test]
    fn write_open_container_packet() {
        let mut item_types = ItemTypeIndex::default();
        item_types
            .insert(ItemType {
                id: ItemTypeId(0x1000),
                name: "Gold".to_string(),
                kind: crate::entities::item::ItemKind::Misc,
                stackable: true,
                has_count: true,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert item");
        let open = crate::entities::player::OpenContainer {
            container_id: 2,
            item_type: ItemTypeId(0x2000),
            name: "Backpack".to_string(),
            capacity: 10,
            has_parent: false,
            parent_container_id: None,
            parent_slot: None,
            source_slot: None,
            source_position: None,
            source_stack_pos: None,
            items: vec![ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(0x1000),
                count: 3,
                attributes: Vec::new(),
                contents: Vec::new(),
            }],
        };
        let mut writer = PacketWriter::new();
        write_open_container(&mut writer, &open, Some(&item_types));
        assert_eq!(
            writer.as_slice(),
            &[
                0x6e, 0x02, 0x00, 0x20, 0x08, 0x00, b'B', b'a', b'c', b'k', b'p', b'a',
                b'c', b'k', 0x0a, 0x00, 0x01, 0x00, 0x10, 0x03
            ]
        );
    }

    #[test]
    fn write_close_container_packet() {
        let mut writer = PacketWriter::new();
        write_close_container(&mut writer, 3);
        assert_eq!(writer.as_slice(), &[0x6f, 0x03]);
    }
}
