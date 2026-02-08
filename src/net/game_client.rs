use crate::admin::commands::{parse_admin_command, AdminCommand};
use crate::combat::spells::SpellCastReport;
use crate::entities::inventory::InventorySlot;
use crate::entities::item::{ItemKind, ItemTypeId};
use crate::entities::creature::{CreatureId, Outfit};
use crate::entities::player::{FightModes, PlayerId};
use crate::net::packet::PacketReader;
use crate::telemetry::logging;
use crate::world::position::{Direction, Position};
use crate::world::state::{
    ContainerSource, ContainerUpdate, LogoutBlockReason, MoveUseOutcome, UseObjectSource, WorldState,
};
use crate::world::time::GameClock;

pub const OPCODE_CTALK: u8 = 0x96;
pub const OPCODE_USE_OBJECT: u8 = 0x82;
pub const OPCODE_USE_OBJECT_ON: u8 = 0x83;
pub const OPCODE_USE_ON_CREATURE: u8 = 0x84;
pub const OPCODE_ROTATE_ITEM: u8 = 0x85;
pub const OPCODE_LOOK: u8 = 0x8c;
pub const OPCODE_LOOK_AT_CREATURE: u8 = 0x8d;
pub const OPCODE_CLOSE_CONTAINER: u8 = 0x87;
pub const OPCODE_UP_CONTAINER: u8 = 0x88;
pub const OPCODE_MOVE_OBJECT: u8 = 0x78;
pub const OPCODE_SHOP_LOOK: u8 = 0x79;
pub const OPCODE_SHOP_BUY: u8 = 0x7a;
pub const OPCODE_SHOP_SELL: u8 = 0x7b;
pub const OPCODE_SHOP_CLOSE: u8 = 0x7c;
pub const OPCODE_TRADE_REQUEST: u8 = 0x7d;
pub const OPCODE_TRADE_LOOK: u8 = 0x7e;
pub const OPCODE_TRADE_ACCEPT: u8 = 0x7f;
pub const OPCODE_TRADE_CLOSE: u8 = 0x80;
pub const OPCODE_PING: u8 = 0x1e;
pub const OPCODE_LOGOUT: u8 = 0x14;
pub const OPCODE_FIGHT_MODES: u8 = 0xa0;
pub const OPCODE_ATTACK: u8 = 0xa1;
pub const OPCODE_FOLLOW: u8 = 0xa2;
pub const OPCODE_PARTY_INVITE: u8 = 0xa3;
pub const OPCODE_PARTY_JOIN: u8 = 0xa4;
pub const OPCODE_PARTY_REVOKE: u8 = 0xa5;
pub const OPCODE_PARTY_PASS_LEADERSHIP: u8 = 0xa6;
pub const OPCODE_PARTY_LEAVE: u8 = 0xa7;
pub const OPCODE_PARTY_SHARE_EXP: u8 = 0xa8;
pub const OPCODE_AUTO_WALK: u8 = 0x64;
pub const OPCODE_MOVE_NORTH: u8 = 0x65;
pub const OPCODE_MOVE_EAST: u8 = 0x66;
pub const OPCODE_MOVE_SOUTH: u8 = 0x67;
pub const OPCODE_MOVE_WEST: u8 = 0x68;
pub const OPCODE_STOP_AUTO_WALK: u8 = 0x69;
pub const OPCODE_MOVE_NORTHEAST: u8 = 0x6a;
pub const OPCODE_MOVE_SOUTHEAST: u8 = 0x6b;
pub const OPCODE_MOVE_SOUTHWEST: u8 = 0x6c;
pub const OPCODE_MOVE_NORTHWEST: u8 = 0x6d;
pub const OPCODE_TURN_NORTH: u8 = 0x6f;
pub const OPCODE_TURN_EAST: u8 = 0x70;
pub const OPCODE_TURN_SOUTH: u8 = 0x71;
pub const OPCODE_TURN_WEST: u8 = 0x72;
pub const OPCODE_CLOSE_NPC_DIALOG: u8 = 0x9e;
pub const OPCODE_EDIT_TEXT: u8 = 0x89;
pub const OPCODE_EDIT_LIST: u8 = 0x8a;
pub const OPCODE_REQUEST_CHANNELS: u8 = 0x97;
pub const OPCODE_OPEN_CHANNEL: u8 = 0x98;
pub const OPCODE_CLOSE_CHANNEL: u8 = 0x99;
pub const OPCODE_OPEN_PRIVATE_CHANNEL: u8 = 0x9a;
pub const OPCODE_PROCESS_REQUEST: u8 = 0x9b;
pub const OPCODE_REMOVE_REQUEST: u8 = 0x9c;
pub const OPCODE_CANCEL_REQUEST: u8 = 0x9d;
pub const OPCODE_CANCEL: u8 = 0xbe;
pub const OPCODE_REFRESH_FIELD: u8 = 0xc9;
pub const OPCODE_REFRESH_CONTAINER: u8 = 0xca;
pub const OPCODE_GET_OUTFIT: u8 = 0xd2;
pub const OPCODE_SET_OUTFIT: u8 = 0xd3;
pub const OPCODE_ADD_BUDDY: u8 = 0xdc;
pub const OPCODE_REMOVE_BUDDY: u8 = 0xdd;
pub const OPCODE_CREATE_PRIVATE_CHANNEL: u8 = 0xaa;
pub const OPCODE_INVITE_CHANNEL: u8 = 0xab;
pub const OPCODE_EXCLUDE_CHANNEL: u8 = 0xac;
pub const OPCODE_BUG_REPORT: u8 = 0xe6;
pub const OPCODE_RULE_VIOLATION: u8 = 0xe7;
pub const OPCODE_DEBUG_ASSERT: u8 = 0xe8;
pub const OPCODE_VIOLATION_REPORT: u8 = 0xf2;

const MAX_EDIT_TEXT_LEN: usize = 4096;
const MAX_REPORT_TEXT_LEN: usize = 4096;
const VIOLATION_ACTION_STATEMENT: u8 = 0x06;

const MAX_TALK_MESSAGE_LEN: usize = 512;
const MAX_TALK_RECIPIENT_LEN: usize = 64;
const MAX_CHANNEL_NAME_LEN: usize = 64;
const MAX_REQUEST_NAME_LEN: usize = 0x1e;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CTalkMessage {
    pub talk_type: u8,
    pub channel_id: Option<u16>,
    pub recipient: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemLocation {
    Position(Position),
    Inventory(InventorySlot),
    Container { container_id: u8, slot: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemUseTarget {
    None,
    Position {
        position: Position,
        type_id: ItemTypeId,
        stack_pos: u8,
    },
    Creature(PlayerId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShopRequest {
    Look {
        item_type: ItemTypeId,
        count: u8,
    },
    Buy {
        item_type: ItemTypeId,
        count: u8,
        amount: u8,
        ignore_capacity: bool,
        buy_with_backpack: bool,
    },
    Sell {
        item_type: ItemTypeId,
        count: u8,
        amount: u8,
    },
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeRequest {
    Request {
        position: Position,
        item_type: ItemTypeId,
        stack_pos: u8,
        partner_id: CreatureId,
    },
    Look {
        counter_offer: bool,
        index: u8,
    },
    Accept,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartyRequest {
    Invite { creature_id: CreatureId },
    Join { creature_id: CreatureId },
    Revoke { creature_id: CreatureId },
    PassLeadership { creature_id: CreatureId },
    Leave,
    ShareExp { enabled: bool, unknown: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemUseRequest {
    pub opcode: u8,
    pub item_type: ItemTypeId,
    pub from: ItemLocation,
    pub from_stack: u8,
    pub target: ItemUseTarget,
    pub container_id: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookTarget {
    Position {
        position: Position,
        type_id: ItemTypeId,
        stack_pos: u8,
    },
    Creature(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookRequest {
    pub target: LookTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveItemRequest {
    pub item_type: ItemTypeId,
    pub from: ItemLocation,
    pub from_stack: u8,
    pub to: ItemLocation,
    pub count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientPacketOutcome {
    SpellCast(SpellCastReport),
    MoveUse(MoveUseOutcome),
    Look(LookRequest),
    MoveItem {
        refresh_map: bool,
        refresh_positions: Vec<Position>,
        container_updates: Vec<ContainerUpdate>,
    },
    RefreshField(Position),
    RefreshContainer(u8),
    OpenContainer(crate::entities::player::OpenContainer),
    CloseContainer(u8),
    Logout(LogoutRequestOutcome),
    Admin(AdminOutcome),
    Talk(CTalkMessage),
    BuddyAdd { name: String },
    BuddyRemove { buddy_id: PlayerId },
    EditText { text_id: u32, text: String },
    EditList {
        list_type: u8,
        list_id: u32,
        text: String,
    },
    ChannelListRequest,
    OpenChannel { channel_id: u16 },
    CloseChannel { channel_id: u16 },
    OpenPrivateChannel { name: String },
    CreatePrivateChannel,
    InviteToChannel { name: String },
    ExcludeFromChannel { name: String },
    RequestProcess { name: String },
    RequestRemove { name: String },
    RequestCancel,
    Shop(ShopRequest),
    Trade(TradeRequest),
    Party(PartyRequest),
    OutfitRequest,
    OutfitSet { outfit: Outfit },
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogoutRequestOutcome {
    Allowed,
    Blocked(LogoutBlockReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminOutcome {
    DisconnectSelf,
    OnlineList(Vec<String>),
    Log(String),
    Restart,
    Shutdown,
}

pub fn parse_ctalk_packet(data: &[u8]) -> Result<CTalkMessage, String> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| "ctalk packet missing opcode".to_string())?;
    if opcode != OPCODE_CTALK {
        return Err(format!("unexpected ctalk opcode: 0x{opcode:02x}"));
    }
    parse_ctalk_payload(&mut reader)
}

pub fn parse_ctalk_payload(reader: &mut PacketReader) -> Result<CTalkMessage, String> {
    let talk_type = reader
        .read_u8()
        .ok_or_else(|| "ctalk payload missing talk type".to_string())?;
    if reader.remaining() == 0 {
        return Err("ctalk payload missing message".to_string());
    }

    if let Some((advanced, message)) = try_message_only(reader) {
        *reader = advanced;
        return Ok(CTalkMessage {
            talk_type,
            channel_id: None,
            recipient: None,
            message,
        });
    }

    if let Some((advanced, channel_id, message)) = try_channel_message(reader) {
        *reader = advanced;
        return Ok(CTalkMessage {
            talk_type,
            channel_id: Some(channel_id),
            recipient: None,
            message,
        });
    }

    if let Some((advanced, recipient, message)) = try_private_message(reader) {
        *reader = advanced;
        return Ok(CTalkMessage {
            talk_type,
            channel_id: None,
            recipient: Some(recipient),
            message,
        });
    }

    Err("ctalk payload has unexpected layout".to_string())
}

pub fn handle_client_packet(
    world: &mut WorldState,
    caster_id: PlayerId,
    data: &[u8],
    clock: &GameClock,
) -> Result<ClientPacketOutcome, String> {
    let opcode = data
        .first()
        .copied()
        .ok_or_else(|| "client packet missing opcode".to_string())?;
    match opcode {
        OPCODE_FIGHT_MODES => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "fight modes packet missing opcode".to_string())?;
            if opcode != OPCODE_FIGHT_MODES {
                return Err(format!("unexpected fight mode opcode: 0x{opcode:02x}"));
            }
            let attack_mode = reader
                .read_u8()
                .ok_or_else(|| "fight modes packet missing attack mode".to_string())?;
            let chase_mode = reader
                .read_u8()
                .ok_or_else(|| "fight modes packet missing chase mode".to_string())?;
            let secure_mode = reader
                .read_u8()
                .ok_or_else(|| "fight modes packet missing secure mode".to_string())?;
            if let Some(player) = world.players.get_mut(&caster_id) {
                player.fight_modes = FightModes::from_client(attack_mode, chase_mode, secure_mode);
            }
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_CTALK => {
            let talk = parse_ctalk_packet(data)?;
            if let Some(outcome) = handle_admin_talk(world, caster_id, &talk)? {
                return Ok(ClientPacketOutcome::Admin(outcome));
            }
            let report = try_cast_spell_from_talk(world, caster_id, &talk, clock)?;
            Ok(report
                .map(ClientPacketOutcome::SpellCast)
                .unwrap_or(ClientPacketOutcome::Talk(talk)))
        }
        OPCODE_LOOK => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "look packet missing opcode".to_string())?;
            if opcode != OPCODE_LOOK {
                return Err(format!("unexpected look opcode: 0x{opcode:02x}"));
            }
            let position = read_position(&mut reader)
                .ok_or_else(|| "look packet missing position".to_string())?;
            let type_id = reader
                .read_u16_le()
                .ok_or_else(|| "look packet missing type id".to_string())?;
            let stack_pos = reader
                .read_u8()
                .ok_or_else(|| "look packet missing stack pos".to_string())?;
            if reader.remaining() != 0 {
                return Err("look packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Look(LookRequest {
                target: LookTarget::Position {
                    position,
                    type_id: ItemTypeId(type_id),
                    stack_pos,
                },
            }))
        }
        OPCODE_LOOK_AT_CREATURE => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "look creature packet missing opcode".to_string())?;
            if opcode != OPCODE_LOOK_AT_CREATURE {
                return Err(format!(
                    "unexpected look creature opcode: 0x{opcode:02x}"
                ));
            }
            let creature_id = reader
                .read_u32_le()
                .ok_or_else(|| "look creature packet missing creature id".to_string())?;
            if reader.remaining() != 0 {
                return Err("look creature packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Look(LookRequest {
                target: LookTarget::Creature(creature_id),
            }))
        }
        OPCODE_MOVE_OBJECT => {
            let request = parse_move_item_packet(data)?;
            let mut refresh_positions = Vec::new();
            if let ItemLocation::Position(position) = &request.from {
                refresh_positions.push(*position);
            }
            if let ItemLocation::Position(position) = &request.to {
                if !refresh_positions.contains(position) {
                    refresh_positions.push(*position);
                }
            }
            let mut container_updates = Vec::new();
            let refresh_map = match (request.from, request.to) {
                (ItemLocation::Inventory(from), ItemLocation::Inventory(to)) => {
                    container_updates =
                        world.move_inventory_item(caster_id, from, to, request.count)?;
                    false
                }
                (ItemLocation::Inventory(from), ItemLocation::Position(position)) => {
                    world.drop_to_tile(caster_id, position, from, request.count)?;
                    true
                }
                (ItemLocation::Position(position), ItemLocation::Inventory(to)) => {
                    container_updates = world.pickup_to_inventory_slot(
                        caster_id,
                        position,
                        request.item_type,
                        request.count,
                        to,
                    )?;
                    true
                }
                (ItemLocation::Position(from), ItemLocation::Position(to)) => {
                    world.move_item_between_tiles(
                        caster_id,
                        from,
                        to,
                        request.item_type,
                        request.count,
                    )?;
                    true
                }
                (ItemLocation::Container { container_id, slot }, ItemLocation::Inventory(to)) => {
                    container_updates = world.move_container_item_to_inventory_slot(
                        caster_id,
                        container_id,
                        slot,
                        request.count,
                        request.item_type,
                        to,
                    )?;
                    false
                }
                (ItemLocation::Inventory(from), ItemLocation::Container { container_id, slot }) => {
                    container_updates = world.move_inventory_item_to_container(
                        caster_id,
                        from,
                        request.count,
                        request.item_type,
                        container_id,
                        slot,
                    )?;
                    false
                }
                (ItemLocation::Container { container_id, slot }, ItemLocation::Position(position)) => {
                    container_updates = world.move_container_item_to_tile(
                        caster_id,
                        container_id,
                        slot,
                        request.count,
                        request.item_type,
                        position,
                    )?;
                    true
                }
                (ItemLocation::Position(position), ItemLocation::Container { container_id, slot }) => {
                    container_updates = world.move_tile_item_to_container(
                        caster_id,
                        position,
                        request.item_type,
                        request.count,
                        request.from_stack,
                        container_id,
                        slot,
                    )?;
                    true
                }
                (
                    ItemLocation::Container { container_id: from_container, slot: from_slot },
                    ItemLocation::Container { container_id: to_container, slot: to_slot },
                ) => {
                    container_updates = world.move_container_item_between_containers(
                        caster_id,
                        from_container,
                        from_slot,
                        request.count,
                        request.item_type,
                        to_container,
                        to_slot,
                    )?;
                    false
                }
            };
            Ok(ClientPacketOutcome::MoveItem {
                refresh_map,
                refresh_positions,
                container_updates,
            })
        }
        OPCODE_SHOP_LOOK => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "shop look packet missing opcode".to_string())?;
            if opcode != OPCODE_SHOP_LOOK {
                return Err(format!("unexpected shop look opcode: 0x{opcode:02x}"));
            }
            let item_type = reader
                .read_u16_le()
                .ok_or_else(|| "shop look packet missing item type".to_string())?;
            let count = reader
                .read_u8()
                .ok_or_else(|| "shop look packet missing count".to_string())?;
            if reader.remaining() != 0 {
                return Err("shop look packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Shop(ShopRequest::Look {
                item_type: ItemTypeId(item_type),
                count,
            }))
        }
        OPCODE_SHOP_BUY => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "shop buy packet missing opcode".to_string())?;
            if opcode != OPCODE_SHOP_BUY {
                return Err(format!("unexpected shop buy opcode: 0x{opcode:02x}"));
            }
            let item_type = reader
                .read_u16_le()
                .ok_or_else(|| "shop buy packet missing item type".to_string())?;
            let count = reader
                .read_u8()
                .ok_or_else(|| "shop buy packet missing count".to_string())?;
            let amount = reader
                .read_u8()
                .ok_or_else(|| "shop buy packet missing amount".to_string())?;
            let ignore_capacity = reader
                .read_u8()
                .ok_or_else(|| "shop buy packet missing capacity flag".to_string())?
                == 0x01;
            let buy_with_backpack = reader
                .read_u8()
                .ok_or_else(|| "shop buy packet missing backpack flag".to_string())?
                == 0x01;
            if reader.remaining() != 0 {
                return Err("shop buy packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Shop(ShopRequest::Buy {
                item_type: ItemTypeId(item_type),
                count,
                amount,
                ignore_capacity,
                buy_with_backpack,
            }))
        }
        OPCODE_SHOP_SELL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "shop sell packet missing opcode".to_string())?;
            if opcode != OPCODE_SHOP_SELL {
                return Err(format!("unexpected shop sell opcode: 0x{opcode:02x}"));
            }
            let item_type = reader
                .read_u16_le()
                .ok_or_else(|| "shop sell packet missing item type".to_string())?;
            let count = reader
                .read_u8()
                .ok_or_else(|| "shop sell packet missing count".to_string())?;
            let amount = reader
                .read_u8()
                .ok_or_else(|| "shop sell packet missing amount".to_string())?;
            if reader.remaining() != 0 {
                return Err("shop sell packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Shop(ShopRequest::Sell {
                item_type: ItemTypeId(item_type),
                count,
                amount,
            }))
        }
        OPCODE_SHOP_CLOSE => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "shop close packet missing opcode".to_string())?;
            if opcode != OPCODE_SHOP_CLOSE {
                return Err(format!("unexpected shop close opcode: 0x{opcode:02x}"));
            }
            if reader.remaining() != 0 {
                return Err("shop close packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Shop(ShopRequest::Close))
        }
        OPCODE_TRADE_REQUEST => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "trade request packet missing opcode".to_string())?;
            if opcode != OPCODE_TRADE_REQUEST {
                return Err(format!(
                    "unexpected trade request opcode: 0x{opcode:02x}"
                ));
            }
            let position = read_position(&mut reader)
                .ok_or_else(|| "trade request packet missing position".to_string())?;
            let item_type = reader
                .read_u16_le()
                .ok_or_else(|| "trade request packet missing item type".to_string())?;
            let stack_pos = reader
                .read_u8()
                .ok_or_else(|| "trade request packet missing stack pos".to_string())?;
            let partner_id = reader
                .read_u32_le()
                .ok_or_else(|| "trade request packet missing partner id".to_string())?;
            if reader.remaining() != 0 {
                return Err("trade request packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Trade(TradeRequest::Request {
                position,
                item_type: ItemTypeId(item_type),
                stack_pos,
                partner_id: CreatureId(partner_id),
            }))
        }
        OPCODE_TRADE_LOOK => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "trade look packet missing opcode".to_string())?;
            if opcode != OPCODE_TRADE_LOOK {
                return Err(format!("unexpected trade look opcode: 0x{opcode:02x}"));
            }
            let counter_offer = reader
                .read_u8()
                .ok_or_else(|| "trade look packet missing counter flag".to_string())?
                == 0x01;
            let index = reader
                .read_u8()
                .ok_or_else(|| "trade look packet missing index".to_string())?;
            if reader.remaining() != 0 {
                return Err("trade look packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Trade(TradeRequest::Look {
                counter_offer,
                index,
            }))
        }
        OPCODE_TRADE_ACCEPT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "trade accept packet missing opcode".to_string())?;
            if opcode != OPCODE_TRADE_ACCEPT {
                return Err(format!("unexpected trade accept opcode: 0x{opcode:02x}"));
            }
            if reader.remaining() != 0 {
                return Err("trade accept packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Trade(TradeRequest::Accept))
        }
        OPCODE_TRADE_CLOSE => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "trade close packet missing opcode".to_string())?;
            if opcode != OPCODE_TRADE_CLOSE {
                return Err(format!("unexpected trade close opcode: 0x{opcode:02x}"));
            }
            if reader.remaining() != 0 {
                return Err("trade close packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Trade(TradeRequest::Close))
        }
        OPCODE_USE_OBJECT | OPCODE_USE_OBJECT_ON | OPCODE_USE_ON_CREATURE => {
            let request = parse_item_use_packet(data)?;
            let report = try_cast_rune_from_use(world, caster_id, &request, clock)?;
            if let Some(report) = report {
                return Ok(ClientPacketOutcome::SpellCast(report));
            }
            let source = use_source_from_location(&request.from);
            if request.opcode == OPCODE_USE_OBJECT {
                if let Some(outcome) =
                    world.try_consume_food(caster_id, source, request.item_type, clock)?
                {
                    return Ok(ClientPacketOutcome::MoveUse(outcome));
                }
                match request.from {
                    ItemLocation::Inventory(slot) => {
                        let open_request = if let Some(player) = world.players.get(&caster_id) {
                            if let Some(item) = player.inventory.slot(slot) {
                                if item.type_id == request.item_type {
                                    let has_container_state =
                                        player.inventory_containers.contains_key(&slot);
                                    Some((item.type_id, has_container_state))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        if let Some((item_type_id, has_container_state)) = open_request {
                            let has_contents = world
                                .players
                                .get(&caster_id)
                                .and_then(|player| player.inventory.slot(slot))
                                .map(|item| !item.contents.is_empty())
                                .unwrap_or(false);
                            let can_open = match world
                                .item_types
                                .as_ref()
                                .and_then(|item_types| item_types.get(item_type_id))
                            {
                                Some(item_type) => {
                                    item_type.kind == ItemKind::Container
                                        || has_container_state
                                        || has_contents
                                }
                                None => has_container_state || has_contents,
                            };
                            if can_open {
                                if let Some(container_id) = request.container_id {
                                    if container_id >= 16 {
                                        return Ok(ClientPacketOutcome::Ignored);
                                    }
                                }
                                let source = ContainerSource::InventorySlot(slot);
                                if let Some(existing) = world
                                    .find_open_container_id_for_player_source(caster_id, source)
                                {
                                    let closed =
                                        world.close_container_for_player(caster_id, existing)?;
                                    if closed {
                                        return Ok(ClientPacketOutcome::CloseContainer(existing));
                                    }
                                }
                                if let Ok(open) = world.open_container_for_player(
                                    caster_id,
                                    item_type_id,
                                    source,
                                    request.container_id,
                                ) {
                                    return Ok(ClientPacketOutcome::OpenContainer(open));
                                }
                            }
                        }
                        if let Some(edit) =
                            world.open_text_edit_for_inventory(caster_id, slot, request.item_type)
                        {
                            return Ok(ClientPacketOutcome::MoveUse(MoveUseOutcome {
                                matched_rule: None,
                                ignored_actions: Vec::new(),
                                effects: Vec::new(),
                                texts: Vec::new(),
                                edit_texts: vec![edit],
                                edit_lists: Vec::new(),
                                messages: Vec::new(),
                                damages: Vec::new(),
                                quest_updates: Vec::new(),
                                logout_users: Vec::new(),
                                refresh_positions: Vec::new(),
                                inventory_updates: Vec::new(),
                                container_updates: Vec::new(),
                            }));
                        }
                    }
                    ItemLocation::Container { container_id, slot } => {
                        let open_candidate = (|| {
                            let player = world.players.get(&caster_id)?;
                            let container = player.open_containers.get(&container_id)?;
                            let item = container.items.get(slot as usize)?;
                            if item.type_id != request.item_type {
                                return None;
                            }
                            let can_open = world
                                .item_types
                                .as_ref()
                                .and_then(|item_types| item_types.get(item.type_id))
                                .map(|item_type| {
                                    item_type.kind == ItemKind::Container || !item.contents.is_empty()
                                })
                                .unwrap_or(!item.contents.is_empty());
                            if can_open { Some(item.type_id) } else { None }
                        })();
                        if let Some(item_type_id) = open_candidate {
                            if let Some(container_id) = request.container_id {
                                if container_id >= 16 {
                                    return Ok(ClientPacketOutcome::Ignored);
                                }
                            }
                            let source = ContainerSource::Container {
                                container_id,
                                slot,
                            };
                            if let Some(existing) =
                                world.find_open_container_id_for_player_source(caster_id, source)
                            {
                                let closed =
                                    world.close_container_for_player(caster_id, existing)?;
                                if closed {
                                    return Ok(ClientPacketOutcome::CloseContainer(existing));
                                }
                            }
                            if let Ok(open) = world.open_container_for_player(
                                caster_id,
                                item_type_id,
                                source,
                                request.container_id,
                            ) {
                                return Ok(ClientPacketOutcome::OpenContainer(open));
                            }
                        }
                        if let Some(edit) = world.open_text_edit_for_container(
                            caster_id,
                            container_id,
                            slot,
                            request.item_type,
                        ) {
                            return Ok(ClientPacketOutcome::MoveUse(MoveUseOutcome {
                                matched_rule: None,
                                ignored_actions: Vec::new(),
                                effects: Vec::new(),
                                texts: Vec::new(),
                                edit_texts: vec![edit],
                                edit_lists: Vec::new(),
                                messages: Vec::new(),
                                damages: Vec::new(),
                                quest_updates: Vec::new(),
                                logout_users: Vec::new(),
                                refresh_positions: Vec::new(),
                                inventory_updates: Vec::new(),
                                container_updates: Vec::new(),
                            }));
                        }
                    }
                    ItemLocation::Position(position) => {
                        let open_candidate = (|| {
                            let tile = world.map.tile(position)?;
                            let index = request.from_stack as usize;
                            let item = tile.items.get(index)?;
                            if item.type_id != request.item_type {
                                return None;
                            }
                            let can_open = world
                                .item_types
                                .as_ref()
                                .and_then(|item_types| item_types.get(item.type_id))
                                .map(|item_type| {
                                    item_type.kind == ItemKind::Container || !item.contents.is_empty()
                                })
                                .unwrap_or(!item.contents.is_empty());
                            if can_open { Some(item.type_id) } else { None }
                        })();
                        if let Some(item_type_id) = open_candidate {
                            if let Some(container_id) = request.container_id {
                                if container_id >= 16 {
                                    return Ok(ClientPacketOutcome::Ignored);
                                }
                            }
                            let source = ContainerSource::Map {
                                position,
                                stack_pos: request.from_stack,
                            };
                            if let Some(existing) =
                                world.find_open_container_id_for_player_source(caster_id, source)
                            {
                                let closed =
                                    world.close_container_for_player(caster_id, existing)?;
                                if closed {
                                    return Ok(ClientPacketOutcome::CloseContainer(existing));
                                }
                            }
                            if let Ok(open) = world.open_container_for_player(
                                caster_id,
                                item_type_id,
                                source,
                                request.container_id,
                            ) {
                                return Ok(ClientPacketOutcome::OpenContainer(open));
                            }
                        }
                        let outcome = world.use_object_with_clock(
                            caster_id,
                            position,
                            request.item_type,
                            Some(clock),
                        )?;
                        return Ok(ClientPacketOutcome::MoveUse(outcome));
                    }
                }
            } else if request.opcode == OPCODE_USE_OBJECT_ON {
                let ItemUseTarget::Position { position, type_id, .. } = request.target else {
                    return Err("use on missing target position".to_string());
                };
                let outcome = world.use_object_on_position_with_clock(
                    caster_id,
                    source,
                    request.item_type,
                    position,
                    type_id,
                    Some(clock),
                )?;
                return Ok(ClientPacketOutcome::MoveUse(outcome));
            }
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_ROTATE_ITEM => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "rotate item packet missing opcode".to_string())?;
            if opcode != OPCODE_ROTATE_ITEM {
                return Err(format!("unexpected rotate item opcode: 0x{opcode:02x}"));
            }
            let position = read_position(&mut reader)
                .ok_or_else(|| "rotate item packet missing position".to_string())?;
            let item_type = reader
                .read_u16_le()
                .ok_or_else(|| "rotate item packet missing item type".to_string())?;
            let stack_pos = reader
                .read_u8()
                .ok_or_else(|| "rotate item packet missing stack pos".to_string())?;
            if reader.remaining() != 0 {
                return Err("rotate item packet has trailing bytes".to_string());
            }
            if let ItemLocation::Position(position) = decode_item_location(position) {
                if world.rotate_item(
                    caster_id,
                    position,
                    stack_pos,
                    ItemTypeId(item_type),
                )? {
                    return Ok(ClientPacketOutcome::MoveItem {
                        refresh_map: true,
                        refresh_positions: vec![position],
                        container_updates: Vec::new(),
                    });
                }
            }
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_CLOSE_CONTAINER | OPCODE_UP_CONTAINER => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "container packet missing opcode".to_string())?;
            if opcode != OPCODE_CLOSE_CONTAINER && opcode != OPCODE_UP_CONTAINER {
                return Err(format!("unexpected container opcode: 0x{opcode:02x}"));
            }
            let container_id = reader
                .read_u8()
                .ok_or_else(|| "container packet missing id".to_string())?;
            if opcode == OPCODE_CLOSE_CONTAINER {
                let closed = world.close_container_for_player(caster_id, container_id)?;
                if closed {
                    return Ok(ClientPacketOutcome::CloseContainer(container_id));
                }
                return Ok(ClientPacketOutcome::Ignored);
            }
            if let Some(open) = world.up_container_for_player(caster_id, container_id)? {
                return Ok(ClientPacketOutcome::OpenContainer(open));
            }
            let closed = world.close_container_for_player(caster_id, container_id)?;
            if closed {
                return Ok(ClientPacketOutcome::CloseContainer(container_id));
            }
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_LOGOUT => match world.request_logout(caster_id, Some(clock)) {
            Ok(()) => Ok(ClientPacketOutcome::Logout(LogoutRequestOutcome::Allowed)),
            Err(reason) => Ok(ClientPacketOutcome::Logout(LogoutRequestOutcome::Blocked(reason))),
        },
        OPCODE_ATTACK | OPCODE_FOLLOW => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "target packet missing opcode".to_string())?;
            let target_id = reader
                .read_u32_le()
                .ok_or_else(|| "target packet missing creature id".to_string())?;
            if reader.remaining() != 0 {
                return Err("target packet has trailing bytes".to_string());
            }
            let target = if target_id == 0 {
                None
            } else {
                let candidate = CreatureId(target_id);
                if world.creature_exists(candidate) {
                    Some(candidate)
                } else {
                    None
                }
            };
            match opcode {
                OPCODE_ATTACK => world.set_player_attack_target(caster_id, target),
                OPCODE_FOLLOW => world.set_player_follow_target(caster_id, target),
                _ => return Err(format!("unexpected target opcode: 0x{opcode:02x}")),
            }
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_PARTY_INVITE
        | OPCODE_PARTY_JOIN
        | OPCODE_PARTY_REVOKE
        | OPCODE_PARTY_PASS_LEADERSHIP => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "party packet missing opcode".to_string())?;
            let creature_id = reader
                .read_u32_le()
                .ok_or_else(|| "party packet missing creature id".to_string())?;
            if reader.remaining() != 0 {
                return Err("party packet has trailing bytes".to_string());
            }
            let creature_id = CreatureId(creature_id);
            let request = match opcode {
                OPCODE_PARTY_INVITE => PartyRequest::Invite { creature_id },
                OPCODE_PARTY_JOIN => PartyRequest::Join { creature_id },
                OPCODE_PARTY_REVOKE => PartyRequest::Revoke { creature_id },
                OPCODE_PARTY_PASS_LEADERSHIP => PartyRequest::PassLeadership { creature_id },
                _ => return Err(format!("unexpected party opcode: 0x{opcode:02x}")),
            };
            Ok(ClientPacketOutcome::Party(request))
        }
        OPCODE_PARTY_LEAVE => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "party leave packet missing opcode".to_string())?;
            if opcode != OPCODE_PARTY_LEAVE {
                return Err(format!("unexpected party leave opcode: 0x{opcode:02x}"));
            }
            if reader.remaining() != 0 {
                return Err("party leave packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Party(PartyRequest::Leave))
        }
        OPCODE_PARTY_SHARE_EXP => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "party share packet missing opcode".to_string())?;
            if opcode != OPCODE_PARTY_SHARE_EXP {
                return Err(format!("unexpected party share opcode: 0x{opcode:02x}"));
            }
            let enabled = reader
                .read_u8()
                .ok_or_else(|| "party share packet missing flag".to_string())?
                == 0x01;
            let unknown = reader
                .read_u8()
                .ok_or_else(|| "party share packet missing unknown byte".to_string())?;
            if reader.remaining() != 0 {
                return Err("party share packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::Party(PartyRequest::ShareExp {
                enabled,
                unknown,
            }))
        }
        OPCODE_AUTO_WALK => {
            let steps = parse_auto_walk_steps(data)?;
            world.set_player_autowalk(caster_id, steps);
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_STOP_AUTO_WALK => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "stop auto-walk packet missing opcode".to_string())?;
            if opcode != OPCODE_STOP_AUTO_WALK {
                return Err(format!(
                    "unexpected stop auto-walk opcode: 0x{opcode:02x}"
                ));
            }
            if reader.remaining() != 0 {
                return Err("stop auto-walk packet has trailing bytes".to_string());
            }
            world.clear_player_autowalk(caster_id);
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_PING => Ok(ClientPacketOutcome::Ignored),
        OPCODE_CANCEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "cancel packet missing opcode".to_string())?;
            if opcode != OPCODE_CANCEL {
                return Err(format!("unexpected cancel opcode: 0x{opcode:02x}"));
            }
            if reader.remaining() != 0 {
                return Err("cancel packet has trailing bytes".to_string());
            }
            world.clear_player_autowalk(caster_id);
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_CLOSE_NPC_DIALOG => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "close npc packet missing opcode".to_string())?;
            if opcode != OPCODE_CLOSE_NPC_DIALOG {
                return Err(format!("unexpected close npc opcode: 0x{opcode:02x}"));
            }
            if reader.remaining() != 0 {
                return Err("close npc packet has trailing bytes".to_string());
            }
            world.close_npc_dialog(caster_id);
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_EDIT_TEXT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "edit text packet missing opcode".to_string())?;
            if opcode != OPCODE_EDIT_TEXT {
                return Err(format!("unexpected edit text opcode: 0x{opcode:02x}"));
            }
            let text_id = reader
                .read_u32_le()
                .ok_or_else(|| "edit text packet missing id".to_string())?;
            let text = reader
                .read_string_lossy(MAX_EDIT_TEXT_LEN)
                .ok_or_else(|| "edit text packet missing text".to_string())?;
            if reader.remaining() != 0 {
                return Err("edit text packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::EditText { text_id, text })
        }
        OPCODE_EDIT_LIST => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "edit list packet missing opcode".to_string())?;
            if opcode != OPCODE_EDIT_LIST {
                return Err(format!("unexpected edit list opcode: 0x{opcode:02x}"));
            }
            let list_type = reader
                .read_u8()
                .ok_or_else(|| "edit list packet missing list type".to_string())?;
            let list_id = reader
                .read_u32_le()
                .ok_or_else(|| "edit list packet missing id".to_string())?;
            let text = reader
                .read_string_lossy(MAX_EDIT_TEXT_LEN)
                .ok_or_else(|| "edit list packet missing text".to_string())?;
            if reader.remaining() != 0 {
                return Err("edit list packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::EditList {
                list_type,
                list_id,
                text,
            })
        }
        OPCODE_REQUEST_CHANNELS => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "channel list packet missing opcode".to_string())?;
            if opcode != OPCODE_REQUEST_CHANNELS {
                return Err(format!(
                    "unexpected channel list opcode: 0x{opcode:02x}"
                ));
            }
            if reader.remaining() != 0 {
                return Err("channel list packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::ChannelListRequest)
        }
        OPCODE_OPEN_CHANNEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "open channel packet missing opcode".to_string())?;
            if opcode != OPCODE_OPEN_CHANNEL {
                return Err(format!(
                    "unexpected open channel opcode: 0x{opcode:02x}"
                ));
            }
            let channel_id = reader
                .read_u16_le()
                .ok_or_else(|| "open channel packet missing channel id".to_string())?;
            if reader.remaining() != 0 {
                return Err("open channel packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::OpenChannel { channel_id })
        }
        OPCODE_CLOSE_CHANNEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "close channel packet missing opcode".to_string())?;
            if opcode != OPCODE_CLOSE_CHANNEL {
                return Err(format!(
                    "unexpected close channel opcode: 0x{opcode:02x}"
                ));
            }
            let channel_id = reader
                .read_u16_le()
                .ok_or_else(|| "close channel packet missing channel id".to_string())?;
            if reader.remaining() != 0 {
                return Err("close channel packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::CloseChannel { channel_id })
        }
        OPCODE_OPEN_PRIVATE_CHANNEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "open private channel packet missing opcode".to_string())?;
            if opcode != OPCODE_OPEN_PRIVATE_CHANNEL {
                return Err(format!(
                    "unexpected open private channel opcode: 0x{opcode:02x}"
                ));
            }
            let name = reader
                .read_string_lossy(MAX_CHANNEL_NAME_LEN)
                .ok_or_else(|| "open private channel packet missing name".to_string())?;
            if reader.remaining() != 0 {
                return Err("open private channel packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::OpenPrivateChannel { name })
        }
        OPCODE_GET_OUTFIT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "get outfit packet missing opcode".to_string())?;
            if opcode != OPCODE_GET_OUTFIT {
                return Err(format!(
                    "unexpected get outfit opcode: 0x{opcode:02x}"
                ));
            }
            if reader.remaining() != 0 {
                return Err("get outfit packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::OutfitRequest)
        }
        OPCODE_SET_OUTFIT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "set outfit packet missing opcode".to_string())?;
            if opcode != OPCODE_SET_OUTFIT {
                return Err(format!(
                    "unexpected set outfit opcode: 0x{opcode:02x}"
                ));
            }
            let look_type = reader
                .read_u16_le()
                .ok_or_else(|| "set outfit packet missing look type".to_string())?;
            let head = reader
                .read_u8()
                .ok_or_else(|| "set outfit packet missing head".to_string())?;
            let body = reader
                .read_u8()
                .ok_or_else(|| "set outfit packet missing body".to_string())?;
            let legs = reader
                .read_u8()
                .ok_or_else(|| "set outfit packet missing legs".to_string())?;
            let feet = reader
                .read_u8()
                .ok_or_else(|| "set outfit packet missing feet".to_string())?;
            let addons = if reader.remaining() > 0 {
                reader
                    .read_u8()
                    .ok_or_else(|| "set outfit packet missing addons".to_string())?
            } else {
                0
            };
            if reader.remaining() != 0 {
                return Err("set outfit packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::OutfitSet {
                outfit: Outfit {
                    look_type,
                    head,
                    body,
                    legs,
                    feet,
                    addons,
                    look_item: 0,
                },
            })
        }
        OPCODE_ADD_BUDDY => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "add buddy packet missing opcode".to_string())?;
            if opcode != OPCODE_ADD_BUDDY {
                return Err(format!("unexpected add buddy opcode: 0x{opcode:02x}"));
            }
            let name = reader
                .read_string_lossy(0)
                .ok_or_else(|| "add buddy packet missing name".to_string())?;
            if reader.remaining() != 0 {
                return Err("add buddy packet has trailing bytes".to_string());
            }
            if name.trim().is_empty() {
                return Err("add buddy packet missing name".to_string());
            }
            if name.len() > 32 {
                return Err("add buddy name exceeds 32 characters".to_string());
            }
            Ok(ClientPacketOutcome::BuddyAdd { name })
        }
        OPCODE_REMOVE_BUDDY => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "remove buddy packet missing opcode".to_string())?;
            if opcode != OPCODE_REMOVE_BUDDY {
                return Err(format!(
                    "unexpected remove buddy opcode: 0x{opcode:02x}"
                ));
            }
            let buddy_id = reader
                .read_u32_le()
                .ok_or_else(|| "remove buddy packet missing buddy id".to_string())?;
            if reader.remaining() != 0 {
                return Err("remove buddy packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::BuddyRemove {
                buddy_id: PlayerId(buddy_id),
            })
        }
        OPCODE_CREATE_PRIVATE_CHANNEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "create private channel packet missing opcode".to_string())?;
            if opcode != OPCODE_CREATE_PRIVATE_CHANNEL {
                return Err(format!(
                    "unexpected create private channel opcode: 0x{opcode:02x}"
                ));
            }
            if reader.remaining() != 0 {
                return Err("create private channel packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::CreatePrivateChannel)
        }
        OPCODE_INVITE_CHANNEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "invite channel packet missing opcode".to_string())?;
            if opcode != OPCODE_INVITE_CHANNEL {
                return Err(format!(
                    "unexpected invite channel opcode: 0x{opcode:02x}"
                ));
            }
            let name = reader
                .read_string_lossy(MAX_CHANNEL_NAME_LEN)
                .ok_or_else(|| "invite channel packet missing name".to_string())?;
            if reader.remaining() != 0 {
                return Err("invite channel packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::InviteToChannel { name })
        }
        OPCODE_EXCLUDE_CHANNEL => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "exclude channel packet missing opcode".to_string())?;
            if opcode != OPCODE_EXCLUDE_CHANNEL {
                return Err(format!(
                    "unexpected exclude channel opcode: 0x{opcode:02x}"
                ));
            }
            let name = reader
                .read_string_lossy(MAX_CHANNEL_NAME_LEN)
                .ok_or_else(|| "exclude channel packet missing name".to_string())?;
            if reader.remaining() != 0 {
                return Err("exclude channel packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::ExcludeFromChannel { name })
        }
        OPCODE_PROCESS_REQUEST => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "process request packet missing opcode".to_string())?;
            if opcode != OPCODE_PROCESS_REQUEST {
                return Err(format!(
                    "unexpected process request opcode: 0x{opcode:02x}"
                ));
            }
            let name = reader
                .read_string_lossy(MAX_REQUEST_NAME_LEN)
                .ok_or_else(|| "process request packet missing name".to_string())?;
            if reader.remaining() != 0 {
                return Err("process request packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::RequestProcess { name })
        }
        OPCODE_REMOVE_REQUEST => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "remove request packet missing opcode".to_string())?;
            if opcode != OPCODE_REMOVE_REQUEST {
                return Err(format!(
                    "unexpected remove request opcode: 0x{opcode:02x}"
                ));
            }
            let name = reader
                .read_string_lossy(MAX_REQUEST_NAME_LEN)
                .ok_or_else(|| "remove request packet missing name".to_string())?;
            if reader.remaining() != 0 {
                return Err("remove request packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::RequestRemove { name })
        }
        OPCODE_CANCEL_REQUEST => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "cancel request packet missing opcode".to_string())?;
            if opcode != OPCODE_CANCEL_REQUEST {
                return Err(format!(
                    "unexpected cancel request opcode: 0x{opcode:02x}"
                ));
            }
            if reader.remaining() != 0 {
                return Err("cancel request packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::RequestCancel)
        }
        OPCODE_BUG_REPORT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "bug report packet missing opcode".to_string())?;
            if opcode != OPCODE_BUG_REPORT {
                return Err(format!("unexpected bug report opcode: 0x{opcode:02x}"));
            }
            let comment = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "bug report packet missing comment".to_string())?;
            if reader.remaining() != 0 {
                return Err("bug report packet has trailing bytes".to_string());
            }
            let name = world
                .players
                .get(&caster_id)
                .map(|player| player.name.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            if !comment.trim().is_empty() {
                logging::log_error(&format!("bug report from {}: {}", name, comment));
            }
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_RULE_VIOLATION => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "violation packet missing opcode".to_string())?;
            if opcode != OPCODE_RULE_VIOLATION {
                return Err(format!(
                    "unexpected violation opcode: 0x{opcode:02x}"
                ));
            }
            let target = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "violation packet missing target".to_string())?;
            let reason = reader
                .read_u8()
                .ok_or_else(|| "violation packet missing reason".to_string())?;
            let action = reader
                .read_u8()
                .ok_or_else(|| "violation packet missing action".to_string())?;
            let comment = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "violation packet missing comment".to_string())?;
            let statement = if action == VIOLATION_ACTION_STATEMENT {
                String::new()
            } else {
                reader
                    .read_string_lossy(MAX_REPORT_TEXT_LEN)
                    .ok_or_else(|| "violation packet missing statement".to_string())?
            };
            let channel_id = reader
                .read_u16_le()
                .ok_or_else(|| "violation packet missing channel id".to_string())?;
            let ip_banish = reader
                .read_u8()
                .ok_or_else(|| "violation packet missing ip ban flag".to_string())?;
            if reader.remaining() != 0 {
                return Err("violation packet has trailing bytes".to_string());
            }
            let name = world
                .players
                .get(&caster_id)
                .map(|player| player.name.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            let mut details = format!(
                "violation report from {}: target={}, reason={}, action={}, channel={}, ip_banish={}",
                name,
                target,
                reason,
                action,
                channel_id,
                ip_banish != 0
            );
            if !comment.trim().is_empty() {
                details.push_str(&format!(", comment=\"{}\"", comment));
            }
            if !statement.trim().is_empty() {
                details.push_str(&format!(", statement=\"{}\"", statement));
            }
            logging::log_error(&details);
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_DEBUG_ASSERT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "debug assert packet missing opcode".to_string())?;
            if opcode != OPCODE_DEBUG_ASSERT {
                return Err(format!(
                    "unexpected debug assert opcode: 0x{opcode:02x}"
                ));
            }
            let assert_line = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "debug assert packet missing line".to_string())?;
            let report_date = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "debug assert packet missing date".to_string())?;
            let description = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "debug assert packet missing description".to_string())?;
            let comment = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "debug assert packet missing comment".to_string())?;
            if reader.remaining() != 0 {
                return Err("debug assert packet has trailing bytes".to_string());
            }
            let name = world
                .players
                .get(&caster_id)
                .map(|player| player.name.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            logging::log_error(&format!(
                "client assert from {}: line=\"{}\" date=\"{}\" desc=\"{}\" comment=\"{}\"",
                name, assert_line, report_date, description, comment
            ));
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_VIOLATION_REPORT => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "violation report packet missing opcode".to_string())?;
            if opcode != OPCODE_VIOLATION_REPORT {
                return Err(format!(
                    "unexpected violation report opcode: 0x{opcode:02x}"
                ));
            }
            let report_type = reader
                .read_u8()
                .ok_or_else(|| "violation report packet missing report type".to_string())?;
            let rule_violation = reader
                .read_u8()
                .ok_or_else(|| "violation report packet missing rule violation".to_string())?;
            let violator_name = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "violation report packet missing violator name".to_string())?;
            let comment = reader
                .read_string_lossy(MAX_REPORT_TEXT_LEN)
                .ok_or_else(|| "violation report packet missing comment".to_string())?;
            let mut translation = String::new();
            let mut counter = None;
            if report_type == 0x00 || report_type == 0x01 {
                translation = reader
                    .read_string_lossy(MAX_REPORT_TEXT_LEN)
                    .ok_or_else(|| "violation report packet missing translation".to_string())?;
                if report_type == 0x01 {
                    counter = Some(
                        reader
                            .read_u32_le()
                            .ok_or_else(|| "violation report packet missing counter".to_string())?,
                    );
                }
            }
            if reader.remaining() != 0 {
                return Err("violation report packet has trailing bytes".to_string());
            }
            let name = world
                .players
                .get(&caster_id)
                .map(|player| player.name.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            let mut details = format!(
                "violation report from {}: type={}, rule={}, violator=\"{}\"",
                name, report_type, rule_violation, violator_name
            );
            if !comment.trim().is_empty() {
                details.push_str(&format!(", comment=\"{}\"", comment));
            }
            if !translation.trim().is_empty() {
                details.push_str(&format!(", translation=\"{}\"", translation));
            }
            if let Some(counter) = counter {
                details.push_str(&format!(", counter={}", counter));
            }
            logging::log_error(&details);
            Ok(ClientPacketOutcome::Ignored)
        }
        OPCODE_REFRESH_FIELD => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "refresh field packet missing opcode".to_string())?;
            if opcode != OPCODE_REFRESH_FIELD {
                return Err(format!("unexpected refresh field opcode: 0x{opcode:02x}"));
            }
            let position = read_position(&mut reader)
                .ok_or_else(|| "refresh field packet missing position".to_string())?;
            if reader.remaining() != 0 {
                return Err("refresh field packet has trailing bytes".to_string());
            }
            Ok(ClientPacketOutcome::RefreshField(position))
        }
        OPCODE_REFRESH_CONTAINER => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "refresh container packet missing opcode".to_string())?;
            if opcode != OPCODE_REFRESH_CONTAINER {
                return Err(format!(
                    "unexpected refresh container opcode: 0x{opcode:02x}"
                ));
            }
            let container_id = reader
                .read_u8()
                .ok_or_else(|| "refresh container packet missing container id".to_string())?;
            if reader.remaining() != 0 {
                return Err("refresh container packet has trailing bytes".to_string());
            }
            if container_id > 0x0f {
                return Ok(ClientPacketOutcome::Ignored);
            }
            Ok(ClientPacketOutcome::RefreshContainer(container_id))
        }
        OPCODE_TURN_NORTH | OPCODE_TURN_EAST | OPCODE_TURN_SOUTH | OPCODE_TURN_WEST => {
            let mut reader = PacketReader::new(data);
            let opcode = reader
                .read_u8()
                .ok_or_else(|| "turn packet missing opcode".to_string())?;
            let direction = turn_direction_for_opcode(opcode)
                .ok_or_else(|| format!("unexpected turn opcode: 0x{opcode:02x}"))?;
            if reader.remaining() != 0 {
                return Err("turn packet has trailing bytes".to_string());
            }
            world.turn_player(caster_id, direction)?;
            Ok(ClientPacketOutcome::Ignored)
        }
        _ => {
            if let Some(direction) = movement_direction_for_opcode(opcode) {
                world.clear_player_autowalk(caster_id);
                if let Err(err) = world.move_player(caster_id, direction, clock) {
                    if err != "movement blocked: cooldown" {
                        return Err(err);
                    }
                }
            }
            Ok(ClientPacketOutcome::Ignored)
        }
    }
}

fn movement_direction_for_opcode(opcode: u8) -> Option<Direction> {
    match opcode {
        OPCODE_MOVE_NORTH => Some(Direction::North),
        OPCODE_MOVE_EAST => Some(Direction::East),
        OPCODE_MOVE_SOUTH => Some(Direction::South),
        OPCODE_MOVE_WEST => Some(Direction::West),
        OPCODE_MOVE_NORTHEAST => Some(Direction::Northeast),
        OPCODE_MOVE_NORTHWEST => Some(Direction::Northwest),
        OPCODE_MOVE_SOUTHEAST => Some(Direction::Southeast),
        OPCODE_MOVE_SOUTHWEST => Some(Direction::Southwest),
        _ => None,
    }
}

fn turn_direction_for_opcode(opcode: u8) -> Option<Direction> {
    match opcode {
        OPCODE_TURN_NORTH => Some(Direction::North),
        OPCODE_TURN_EAST => Some(Direction::East),
        OPCODE_TURN_SOUTH => Some(Direction::South),
        OPCODE_TURN_WEST => Some(Direction::West),
        _ => None,
    }
}

fn parse_auto_walk_steps(data: &[u8]) -> Result<Vec<Direction>, String> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| "auto-walk packet missing opcode".to_string())?;
    if opcode != OPCODE_AUTO_WALK {
        return Err(format!("unexpected auto-walk opcode: 0x{opcode:02x}"));
    }
    let step_count = reader
        .read_u8()
        .ok_or_else(|| "auto-walk packet missing step count".to_string())?;
    let mut steps = Vec::with_capacity(step_count as usize);
    for _ in 0..step_count {
        let raw = reader
            .read_u8()
            .ok_or_else(|| "auto-walk packet missing step".to_string())?;
        let direction = direction_for_path_step(raw)
            .ok_or_else(|| format!("auto-walk step had invalid direction {raw}"))?;
        steps.push(direction);
    }
    Ok(steps)
}

fn direction_for_path_step(step: u8) -> Option<Direction> {
    match step {
        1 => Some(Direction::East),
        2 => Some(Direction::Northeast),
        3 => Some(Direction::North),
        4 => Some(Direction::Northwest),
        5 => Some(Direction::West),
        6 => Some(Direction::Southwest),
        7 => Some(Direction::South),
        8 => Some(Direction::Southeast),
        _ => None,
    }
}

fn handle_admin_talk(
    world: &mut WorldState,
    caster_id: PlayerId,
    talk: &CTalkMessage,
) -> Result<Option<AdminOutcome>, String> {
    if talk.channel_id.is_some() || talk.recipient.is_some() {
        return Ok(None);
    }
    let Some(command) = parse_admin_command(&talk.message)? else {
        return Ok(None);
    };
    let is_gm = world
        .players
        .get(&caster_id)
        .map(|player| player.is_gm)
        .unwrap_or(false);
    if !is_gm {
        return Ok(Some(AdminOutcome::Log(
            "You do not have admin rights.".to_string(),
        )));
    }

    let outcome = match command {
        AdminCommand::Online => {
            let mut names: Vec<String> = world.players.values().map(|player| player.name.clone()).collect();
            names.sort_by(|a, b| a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()));
            AdminOutcome::OnlineList(names)
        }
        AdminCommand::MoveUseAudit => {
            match world.run_moveuse_audit_for_player(caster_id) {
                Ok(summary) => AdminOutcome::Log(summary),
                Err(err) => AdminOutcome::Log(format!("moveuse audit failed: {}", err)),
            }
        }
        AdminCommand::Kick { target } => {
            if let Some(target_name) = target {
                let Some(player) = world.players.get(&caster_id) else {
                    return Err(format!("unknown player {:?}", caster_id));
                };
                if !target_name.eq_ignore_ascii_case(&player.name) {
                    return Ok(Some(AdminOutcome::Log(format!(
                        "admin kick for '{}' ignored (only self-kick supported)",
                        target_name
                    ))));
                }
            }
            AdminOutcome::DisconnectSelf
        }
        AdminCommand::Shutdown => AdminOutcome::Shutdown,
        AdminCommand::Restart => AdminOutcome::Restart,
        AdminCommand::Teleport { position } => {
            match world.teleport_player_admin(caster_id, position) {
                Ok(()) => AdminOutcome::Log(format!(
                    "teleported to ({},{},{})",
                    position.x, position.y, position.z
                )),
                Err(err) => AdminOutcome::Log(format!("teleport failed: {}", err)),
            }
        }
        AdminCommand::Where => {
            let player = world
                .players
                .get(&caster_id)
                .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
            AdminOutcome::Log(format!(
                "position ({},{},{})",
                player.position.x, player.position.y, player.position.z
            ))
        }
        AdminCommand::Unknown(name) => {
            AdminOutcome::Log(format!("unknown admin command '{}'", name))
        }
    };

    Ok(Some(outcome))
}

pub fn try_cast_spell_from_talk(
    world: &mut WorldState,
    caster_id: PlayerId,
    talk: &CTalkMessage,
    clock: &GameClock,
) -> Result<Option<SpellCastReport>, String> {
    if talk.channel_id.is_some() || talk.recipient.is_some() {
        return Ok(None);
    }

    let direction = world.players.get(&caster_id).map(|player| player.direction);
    match world.cast_spell_words(caster_id, &talk.message, None, direction, clock) {
        Ok(report) => Ok(Some(report)),
        Err(err) if err == "spell cast failed: unknown words" => Ok(None),
        Err(err) if err == "spell cast failed: empty words" => Ok(None),
        Err(err) => Err(err),
    }
}

pub fn parse_item_use_packet(data: &[u8]) -> Result<ItemUseRequest, String> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| "item use packet missing opcode".to_string())?;
    parse_item_use_payload(opcode, &mut reader)
}

pub fn parse_move_item_packet(data: &[u8]) -> Result<MoveItemRequest, String> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| "move item packet missing opcode".to_string())?;
    if opcode != OPCODE_MOVE_OBJECT {
        return Err(format!("unexpected move item opcode: 0x{opcode:02x}"));
    }
    let from_position = read_position(&mut reader)
        .ok_or_else(|| "move item payload missing from position".to_string())?;
    let item_type = reader
        .read_u16_le()
        .ok_or_else(|| "move item payload missing item type".to_string())?;
    let from_stack = reader
        .read_u8()
        .ok_or_else(|| "move item payload missing from stack".to_string())?;
    let to_position = read_position(&mut reader)
        .ok_or_else(|| "move item payload missing to position".to_string())?;
    let mut count = reader
        .read_u8()
        .ok_or_else(|| "move item payload missing count".to_string())? as u16;
    if count == 0 {
        count = 1;
    }
    if reader.remaining() != 0 {
        return Err("move item payload has trailing bytes".to_string());
    }

    Ok(MoveItemRequest {
        item_type: ItemTypeId(item_type),
        from: decode_item_location(from_position),
        from_stack,
        to: decode_item_location(to_position),
        count,
    })
}

pub fn parse_item_use_payload(
    opcode: u8,
    reader: &mut PacketReader,
) -> Result<ItemUseRequest, String> {
    let from_position = read_position(reader)
        .ok_or_else(|| "item use payload missing from position".to_string())?;
    let item_type = reader
        .read_u16_le()
        .ok_or_else(|| "item use payload missing item type".to_string())?;
    let from_stack = reader
        .read_u8()
        .ok_or_else(|| "item use payload missing from stack".to_string())?;

    let from = decode_item_location(from_position);
    let target = match opcode {
        OPCODE_USE_OBJECT => ItemUseTarget::None,
        OPCODE_USE_OBJECT_ON => {
            let to_position = read_position(reader)
                .ok_or_else(|| "use on payload missing target position".to_string())?;
            let target_type = reader
                .read_u16_le()
                .ok_or_else(|| "use on payload missing target type".to_string())?;
            let target_stack = reader
                .read_u8()
                .ok_or_else(|| "use on payload missing target stack".to_string())?;
            ItemUseTarget::Position {
                position: to_position,
                type_id: ItemTypeId(target_type),
                stack_pos: target_stack,
            }
        }
        OPCODE_USE_ON_CREATURE => {
            let creature_id = reader
                .read_u32_le()
                .ok_or_else(|| "use on creature payload missing creature id".to_string())?;
            ItemUseTarget::Creature(PlayerId(creature_id))
        }
        _ => {
            return Err(format!("unexpected use opcode: 0x{opcode:02x}"));
        }
    };

    let mut container_id = None;
    if opcode == OPCODE_USE_OBJECT {
        if reader.remaining() == 1 {
            container_id = reader.read_u8();
        } else if reader.remaining() != 0 {
            return Err("item use payload has trailing bytes".to_string());
        }
    } else if reader.remaining() != 0 {
        return Err("item use payload has trailing bytes".to_string());
    }

    Ok(ItemUseRequest {
        opcode,
        item_type: ItemTypeId(item_type),
        from,
        from_stack,
        target,
        container_id,
    })
}

pub fn try_cast_rune_from_use(
    world: &mut WorldState,
    caster_id: PlayerId,
    request: &ItemUseRequest,
    clock: &GameClock,
) -> Result<Option<SpellCastReport>, String> {
    let ItemLocation::Inventory(rune_slot) = request.from else {
        return Ok(None);
    };
    let spell = match world.spellbook.get_by_rune_item(request.item_type) {
        Some(spell) => spell.clone(),
        None => return Ok(None),
    };
    let direction = world.players.get(&caster_id).map(|player| player.direction);
    let target_position = match request.target {
        ItemUseTarget::None => None,
        ItemUseTarget::Position { position, .. } => Some(position),
        ItemUseTarget::Creature(creature_id) => world
            .players
            .get(&creature_id)
            .map(|player| player.position)
            .or_else(|| {
                world
                    .monsters
                    .get(&CreatureId(creature_id.0))
                    .map(|monster| monster.position)
            })
            .or_else(|| {
                world
                    .npcs
                    .get(&CreatureId(creature_id.0))
                    .map(|npc| npc.position)
            }),
    };

    let report = world.cast_rune(
        caster_id,
        &spell,
        rune_slot,
        target_position,
        direction,
        clock,
    )?;
    Ok(Some(report))
}

fn try_message_only<'a>(reader: &PacketReader<'a>) -> Option<(PacketReader<'a>, String)> {
    let mut trial = reader.clone();
    let message = trial.read_string_lossy(MAX_TALK_MESSAGE_LEN)?;
    if trial.remaining() == 0 {
        Some((trial, message))
    } else {
        None
    }
}

fn try_channel_message<'a>(
    reader: &PacketReader<'a>,
) -> Option<(PacketReader<'a>, u16, String)> {
    let mut trial = reader.clone();
    let channel_id = trial.read_u16_le()?;
    let message = trial.read_string_lossy(MAX_TALK_MESSAGE_LEN)?;
    if trial.remaining() == 0 {
        Some((trial, channel_id, message))
    } else {
        None
    }
}

fn try_private_message<'a>(
    reader: &PacketReader<'a>,
) -> Option<(PacketReader<'a>, String, String)> {
    let mut trial = reader.clone();
    let recipient = trial.read_string_lossy(MAX_TALK_RECIPIENT_LEN)?;
    let message = trial.read_string_lossy(MAX_TALK_MESSAGE_LEN)?;
    if trial.remaining() == 0 {
        Some((trial, recipient, message))
    } else {
        None
    }
}

fn read_position(reader: &mut PacketReader) -> Option<Position> {
    let x = reader.read_u16_le()?;
    let y = reader.read_u16_le()?;
    let z = reader.read_u8()?;
    Some(Position { x, y, z })
}

fn decode_item_location(position: Position) -> ItemLocation {
    if position.x == 0xffff {
        if let Some(slot) = InventorySlot::from_index(position.y as usize) {
            return ItemLocation::Inventory(slot);
        }
        if position.y >= 0x40 {
            return ItemLocation::Container {
                container_id: (position.y - 0x40) as u8,
                slot: position.z,
            };
        }
    }
    ItemLocation::Position(position)
}

fn use_source_from_location(location: &ItemLocation) -> UseObjectSource {
    match location {
        ItemLocation::Position(position) => UseObjectSource::Map(*position),
        ItemLocation::Inventory(slot) => UseObjectSource::Inventory(*slot),
        ItemLocation::Container { container_id, slot } => UseObjectSource::Container {
            container_id: *container_id,
            slot: *slot,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combat::damage::DamageType;
    use crate::entities::player::PlayerState;
    use crate::entities::spells::{Spell, SpellEffect, SpellId, SpellKind, SpellShape, SpellTarget};
    use crate::net::packet::PacketWriter;
    use crate::world::map::Tile;
    use crate::world::position::Position;
    use crate::world::time::GameClock;
    use std::time::Duration;

    fn make_spell(words: &str) -> Spell {
        Spell {
            id: SpellId(1),
            name: "Test".to_string(),
            words: words.to_string(),
            kind: SpellKind::Instant,
            rune_type_id: None,
            target: SpellTarget::SelfOnly,
            group: None,
            mana_cost: 0,
            soul_cost: 0,
            level_required: 1,
            magic_level_required: 0,
            cooldown_ms: 0,
            group_cooldown_ms: 0,
            damage_scale_flags: crate::combat::damage::DamageScaleFlags::NONE,
            effect: Some(SpellEffect {
                shape: SpellShape::Area { radius: 0 },
                kind: crate::entities::spells::SpellEffectKind::Damage,
                damage_type: DamageType::Physical,
                min_damage: 1,
                max_damage: 1,
                include_caster: true,
                base_damage: None,
                variance: None,
            }),
            summon: None,
            haste: None,
            light: None,
            dispel: None,
            field: None,
            magic_shield: None,
            outfit: None,
            challenge: None,
            levitate: None,
            raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
        }
    }

    #[test]
    fn parse_auto_walk_steps_uses_client_encoding() {
        let data = [OPCODE_AUTO_WALK, 3, 1, 3, 8];
        let steps = parse_auto_walk_steps(&data).expect("parse");
        assert_eq!(
            steps,
            vec![Direction::East, Direction::North, Direction::Southeast]
        );
    }

    struct FuzzRng(u64);

    impl FuzzRng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            (self.0 >> 32) as u32
        }

        fn next_u8(&mut self) -> u8 {
            (self.next_u32() & 0xff) as u8
        }

        fn gen_bytes(&mut self, len: usize) -> Vec<u8> {
            let mut out = Vec::with_capacity(len);
            for _ in 0..len {
                out.push(self.next_u8());
            }
            out
        }
    }

    #[test]
    fn fuzz_client_packet_parsers() {
        let mut rng = FuzzRng::new(0x5ed4_1234_aa55_00ff);
        for _ in 0..500 {
            let len = (rng.next_u32() % 128) as usize;
            let data = rng.gen_bytes(len);
            let _ = parse_ctalk_packet(&data);
            let _ = parse_item_use_packet(&data);
        }
    }

    #[test]
    fn parse_ctalk_message_only() {
        let payload = [
            OPCODE_CTALK,
            0x01,
            0x04,
            0x00,
            b't',
            b'e',
            b's',
            b't',
        ];
        let parsed = parse_ctalk_packet(&payload).expect("ctalk parse");
        assert_eq!(parsed.talk_type, 0x01);
        assert_eq!(parsed.channel_id, None);
        assert_eq!(parsed.recipient, None);
        assert_eq!(parsed.message, "test");
    }

    #[test]
    fn parse_ctalk_channel_message() {
        let payload = [
            OPCODE_CTALK,
            0x05,
            0x34,
            0x12,
            0x02,
            0x00,
            b'h',
            b'i',
        ];
        let parsed = parse_ctalk_packet(&payload).expect("ctalk parse");
        assert_eq!(parsed.talk_type, 0x05);
        assert_eq!(parsed.channel_id, Some(0x1234));
        assert_eq!(parsed.recipient, None);
        assert_eq!(parsed.message, "hi");
    }

    #[test]
    fn parse_ctalk_private_message() {
        let payload = [
            OPCODE_CTALK,
            0x04,
            0x03,
            0x00,
            b'b',
            b'o',
            b'b',
            0x02,
            0x00,
            b'h',
            b'i',
        ];
        let parsed = parse_ctalk_packet(&payload).expect("ctalk parse");
        assert_eq!(parsed.talk_type, 0x04);
        assert_eq!(parsed.channel_id, None);
        assert_eq!(parsed.recipient, Some("bob".to_string()));
        assert_eq!(parsed.message, "hi");
    }

    #[test]
    fn cast_spell_from_talk_message() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        let position = Position { x: 100, y: 100, z: 7 };
        let player = PlayerState::new(caster_id, "Mage".to_string(), position);
        world.players.insert(caster_id, player);
        let spell = make_spell("exori");
        world.add_spell(spell.clone()).expect("spell add");
        world.teach_spell(caster_id, spell.id).expect("learn");

        let talk = CTalkMessage {
            talk_type: 0x01,
            channel_id: None,
            recipient: None,
            message: "exori".to_string(),
        };
        let clock = GameClock::new(Duration::from_millis(100));
        let report = try_cast_spell_from_talk(&mut world, caster_id, &talk, &clock)
            .expect("spell cast")
            .expect("spell should cast");
        assert_eq!(report.hits.len(), 1);
    }

    #[test]
    fn ignore_non_spell_talk() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        let position = Position { x: 100, y: 100, z: 7 };
        let player = PlayerState::new(caster_id, "Mage".to_string(), position);
        world.players.insert(caster_id, player);

        let talk = CTalkMessage {
            talk_type: 0x01,
            channel_id: None,
            recipient: None,
            message: "hello".to_string(),
        };
        let clock = GameClock::new(Duration::from_millis(100));
        let report = try_cast_spell_from_talk(&mut world, caster_id, &talk, &clock)
            .expect("talk handling");
        assert!(report.is_none());
    }

    #[test]
    fn parse_use_on_position_packet() {
        let payload = [
            OPCODE_USE_OBJECT_ON,
            0xff,
            0xff,
            0x04,
            0x00,
            0x00,
            0x34,
            0x12,
            0x00,
            0x10,
            0x00,
            0x20,
            0x00,
            0x07,
            0x78,
            0x56,
            0x00,
        ];
        let parsed = parse_item_use_packet(&payload).expect("use parse");
        assert_eq!(parsed.opcode, OPCODE_USE_OBJECT_ON);
        assert_eq!(parsed.item_type, ItemTypeId(0x1234));
        assert_eq!(parsed.from, ItemLocation::Inventory(InventorySlot::RightHand));
        assert_eq!(
            parsed.target,
            ItemUseTarget::Position {
                position: Position { x: 0x10, y: 0x20, z: 0x07 },
                type_id: ItemTypeId(0x5678),
                stack_pos: 0x00,
            }
        );
    }

    #[test]
    fn parse_use_object_packet() {
        let payload = [
            OPCODE_USE_OBJECT,
            0xff,
            0xff,
            0x04,
            0x00,
            0x00,
            0xcd,
            0xab,
            0x01,
            0x00,
        ];
        let parsed = parse_item_use_packet(&payload).expect("use parse");
        assert_eq!(parsed.opcode, OPCODE_USE_OBJECT);
        assert_eq!(parsed.item_type, ItemTypeId(0xabcd));
        assert_eq!(parsed.from, ItemLocation::Inventory(InventorySlot::RightHand));
        assert_eq!(parsed.target, ItemUseTarget::None);
        assert_eq!(parsed.container_id, Some(0));
    }

    #[test]
    fn parse_move_item_packet() {
        let payload = [
            OPCODE_MOVE_OBJECT,
            0xff,
            0xff,
            0x04,
            0x00,
            0x00,
            0x34,
            0x12,
            0x02,
            0x10,
            0x00,
            0x20,
            0x00,
            0x07,
            0x05,
        ];
        let parsed = parse_move_item_packet(&payload).expect("move parse");
        assert_eq!(parsed.item_type, ItemTypeId(0x1234));
        assert_eq!(parsed.from, ItemLocation::Inventory(InventorySlot::RightHand));
        assert_eq!(
            parsed.to,
            ItemLocation::Position(Position { x: 0x10, y: 0x20, z: 0x07 })
        );
        assert_eq!(parsed.from_stack, 0x02);
        assert_eq!(parsed.count, 5);
    }

    #[test]
    fn parse_use_on_creature_packet() {
        let payload = [
            OPCODE_USE_ON_CREATURE,
            0xff,
            0xff,
            0x04,
            0x00,
            0x00,
            0x11,
            0x22,
            0x00,
            0x44,
            0x33,
            0x22,
            0x11,
        ];
        let parsed = parse_item_use_packet(&payload).expect("use parse");
        assert_eq!(parsed.opcode, OPCODE_USE_ON_CREATURE);
        assert_eq!(parsed.item_type, ItemTypeId(0x2211));
        assert_eq!(parsed.from, ItemLocation::Inventory(InventorySlot::RightHand));
        assert_eq!(parsed.target, ItemUseTarget::Creature(PlayerId(0x11223344)));
    }

    #[test]
    fn cast_rune_from_use_packet() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 100, y: 100, z: 7 };
        let player = PlayerState::new(caster_id, "Mage".to_string(), caster_pos);
        world.players.insert(caster_id, player);
        let rune_type = ItemTypeId(0x2222);
        let mut item_types = crate::world::item_types::ItemTypeIndex::default();
        item_types
            .insert(crate::world::item_types::ItemType {
                id: rune_type,
                name: "Test rune".to_string(),
                kind: crate::entities::item::ItemKind::Rune,
                stackable: false,
                has_count: true,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert rune type");
        world.item_types = Some(item_types);
        let mut player = world.players.get_mut(&caster_id).expect("player");
        player.inventory.set_slot(
            InventorySlot::RightHand,
            Some(crate::entities::item::ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: rune_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            }),
        );
        drop(player);
        let spell = Spell {
            id: SpellId(2),
            name: "Rune".to_string(),
            words: "adori".to_string(),
            kind: SpellKind::Rune,
            rune_type_id: Some(rune_type),
            target: SpellTarget::Position,
            group: None,
            mana_cost: 0,
            soul_cost: 0,
            level_required: 1,
            magic_level_required: 0,
            cooldown_ms: 0,
            group_cooldown_ms: 0,
            damage_scale_flags: crate::combat::damage::DamageScaleFlags::NONE,
            effect: Some(SpellEffect {
                shape: SpellShape::Area { radius: 0 },
                kind: crate::entities::spells::SpellEffectKind::Damage,
                damage_type: DamageType::Physical,
                min_damage: 1,
                max_damage: 1,
                include_caster: false,
                base_damage: None,
                variance: None,
            }),
            summon: None,
            haste: None,
            light: None,
            dispel: None,
            field: None,
            magic_shield: None,
            outfit: None,
            challenge: None,
            levitate: None,
            raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
        };
        world.add_spell(spell.clone()).expect("spell add");
        world.teach_spell(caster_id, spell.id).expect("learn");

        let payload = [
            OPCODE_USE_OBJECT_ON,
            0xff,
            0xff,
            0x04,
            0x00,
            0x00,
            0x22,
            0x22,
            0x00,
            0x64,
            0x00,
            0x64,
            0x00,
            0x07,
            0x00,
            0x00,
            0x00,
        ];
        let request = parse_item_use_packet(&payload).expect("use parse");
        let clock = GameClock::new(Duration::from_millis(100));
        let report = try_cast_rune_from_use(&mut world, caster_id, &request, &clock)
            .expect("rune cast")
            .expect("rune should cast");
        assert_eq!(report.hits.len(), 1);
        let player = world.players.get(&caster_id).expect("player");
        assert!(player.inventory.slot(InventorySlot::RightHand).is_none());
    }

    #[test]
    fn handle_ctalk_packet_casts_spell() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        let position = Position { x: 100, y: 100, z: 7 };
        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Mage".to_string(), position));
        let spell = make_spell("exori");
        world.add_spell(spell.clone()).expect("spell add");
        world.teach_spell(caster_id, spell.id).expect("learn");

        let payload = [
            OPCODE_CTALK,
            0x01,
            0x05,
            0x00,
            b'e',
            b'x',
            b'o',
            b'r',
            b'i',
        ];
        let clock = GameClock::new(Duration::from_millis(100));
        let outcome = handle_client_packet(&mut world, caster_id, &payload, &clock)
            .expect("handle");
        match outcome {
            ClientPacketOutcome::SpellCast(report) => assert_eq!(report.hits.len(), 1),
            ClientPacketOutcome::Ignored => panic!("expected spell cast"),
            ClientPacketOutcome::OpenContainer(_) => panic!("unexpected container open"),
            ClientPacketOutcome::CloseContainer(_) => panic!("unexpected container close"),
            ClientPacketOutcome::Admin(_) => panic!("unexpected admin outcome"),
            ClientPacketOutcome::Talk(_) => panic!("unexpected talk outcome"),
            _ => panic!("unexpected outcome"),
        }
    }

    #[test]
    fn handle_rune_use_packet_casts_spell() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 100, y: 100, z: 7 };
        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Mage".to_string(), caster_pos));
        let rune_type = ItemTypeId(0x2222);
        let mut item_types = crate::world::item_types::ItemTypeIndex::default();
        item_types
            .insert(crate::world::item_types::ItemType {
                id: rune_type,
                name: "Test rune".to_string(),
                kind: crate::entities::item::ItemKind::Rune,
                stackable: false,
                has_count: true,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert rune type");
        world.item_types = Some(item_types);
        let mut player = world.players.get_mut(&caster_id).expect("player");
        player.inventory.set_slot(
            InventorySlot::RightHand,
            Some(crate::entities::item::ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: rune_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            }),
        );
        drop(player);
        let spell = Spell {
            id: SpellId(2),
            name: "Rune".to_string(),
            words: "adori".to_string(),
            kind: SpellKind::Rune,
            rune_type_id: Some(rune_type),
            target: SpellTarget::Position,
            group: None,
            mana_cost: 0,
            soul_cost: 0,
            level_required: 1,
            magic_level_required: 0,
            cooldown_ms: 0,
            group_cooldown_ms: 0,
            damage_scale_flags: crate::combat::damage::DamageScaleFlags::NONE,
            effect: Some(SpellEffect {
                shape: SpellShape::Area { radius: 0 },
                kind: crate::entities::spells::SpellEffectKind::Damage,
                damage_type: DamageType::Physical,
                min_damage: 1,
                max_damage: 1,
                include_caster: false,
                base_damage: None,
                variance: None,
            }),
            summon: None,
            haste: None,
            light: None,
            dispel: None,
            field: None,
            magic_shield: None,
            outfit: None,
            challenge: None,
            levitate: None,
            raise_dead: None,
            conjure: None,
            antidote: None,
            magic_rope: None,
            find_person: None,
            enchant_staff: None,
        };
        world.add_spell(spell.clone()).expect("spell add");
        world.teach_spell(caster_id, spell.id).expect("learn");

        let payload = [
            OPCODE_USE_OBJECT_ON,
            0xff,
            0xff,
            0x04,
            0x00,
            0x00,
            0x22,
            0x22,
            0x00,
            0x64,
            0x00,
            0x64,
            0x00,
            0x07,
            0x00,
            0x00,
            0x00,
        ];
        let clock = GameClock::new(Duration::from_millis(100));
        let outcome = handle_client_packet(&mut world, caster_id, &payload, &clock)
            .expect("handle");
        match outcome {
            ClientPacketOutcome::SpellCast(report) => assert_eq!(report.hits.len(), 1),
            ClientPacketOutcome::Ignored => panic!("expected rune cast"),
            ClientPacketOutcome::OpenContainer(_) => panic!("unexpected container open"),
            ClientPacketOutcome::CloseContainer(_) => panic!("unexpected container close"),
            ClientPacketOutcome::Admin(_) => panic!("unexpected admin outcome"),
            ClientPacketOutcome::Talk(_) => panic!("unexpected talk outcome"),
            _ => panic!("unexpected outcome"),
        }
    }

    #[test]
    fn admin_online_lists_players() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        world.players.insert(
            caster_id,
            PlayerState::new(caster_id, "Zed".to_string(), Position { x: 10, y: 10, z: 7 }),
        );
        let other_id = PlayerId(2);
        world.players.insert(
            other_id,
            PlayerState::new(other_id, "Alice".to_string(), Position { x: 10, y: 11, z: 7 }),
        );

        let mut writer = PacketWriter::new();
        writer.write_u8(OPCODE_CTALK);
        writer.write_u8(0x01);
        writer.write_string_str("!online");
        let payload = writer.into_vec();
        let clock = GameClock::new(Duration::from_millis(100));

        let outcome =
            handle_client_packet(&mut world, caster_id, &payload, &clock).expect("handle");
        match outcome {
            ClientPacketOutcome::Admin(AdminOutcome::OnlineList(names)) => {
                assert_eq!(names, vec!["Alice".to_string(), "Zed".to_string()]);
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[test]
    fn admin_kick_self_disconnects() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        world.players.insert(
            caster_id,
            PlayerState::new(caster_id, "Zed".to_string(), Position { x: 10, y: 10, z: 7 }),
        );

        let mut writer = PacketWriter::new();
        writer.write_u8(OPCODE_CTALK);
        writer.write_u8(0x01);
        writer.write_string_str("!kick");
        let payload = writer.into_vec();
        let clock = GameClock::new(Duration::from_millis(100));

        let outcome =
            handle_client_packet(&mut world, caster_id, &payload, &clock).expect("handle");
        assert_eq!(
            outcome,
            ClientPacketOutcome::Admin(AdminOutcome::DisconnectSelf)
        );
    }

    #[test]
    fn admin_restart_requests_restart() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        world.players.insert(
            caster_id,
            PlayerState::new(caster_id, "Zed".to_string(), Position { x: 10, y: 10, z: 7 }),
        );

        let mut writer = PacketWriter::new();
        writer.write_u8(OPCODE_CTALK);
        writer.write_u8(0x01);
        writer.write_string_str("!restart");
        let payload = writer.into_vec();
        let clock = GameClock::new(Duration::from_millis(100));

        let outcome =
            handle_client_packet(&mut world, caster_id, &payload, &clock).expect("handle");
        assert_eq!(outcome, ClientPacketOutcome::Admin(AdminOutcome::Restart));
    }

    #[test]
    fn admin_teleport_moves_player() {
        let mut world = WorldState::default();
        let caster_id = PlayerId(1);
        let origin = Position { x: 10, y: 10, z: 7 };
        let target = Position { x: 20, y: 25, z: 7 };
        world.map.tiles.insert(
            target,
            Tile {
                position: target,
                items: Vec::new(),
                item_details: Vec::new(),
                refresh: false,
                protection_zone: false,
                no_logout: false,
                annotations: Vec::new(),
                tags: Vec::new(),
            },
        );
        world.players.insert(
            caster_id,
            PlayerState::new(caster_id, "Zed".to_string(), origin),
        );

        let mut writer = PacketWriter::new();
        writer.write_u8(OPCODE_CTALK);
        writer.write_u8(0x01);
        writer.write_string_str("!tp 20 25 7");
        let payload = writer.into_vec();
        let clock = GameClock::new(Duration::from_millis(100));

        let outcome =
            handle_client_packet(&mut world, caster_id, &payload, &clock).expect("handle");
        let player = world.players.get(&caster_id).expect("player");
        assert_eq!(player.position, target);
        assert_eq!(
            outcome,
            ClientPacketOutcome::Admin(AdminOutcome::Log(
                "teleported to (20,25,7)".to_string()
            ))
        );
    }
}
