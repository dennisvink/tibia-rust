use crate::world::map::{Map, MapItem, SectorBounds, SectorCoord, Tile};
use crate::world::map_dat::MapDat;
use crate::world::mem_dat::MemDat;
use crate::world::monster_homes::{load_monster_homes, MonsterHome};
use crate::world::monsters::{
    LootRng, MonsterFlags, MonsterIndex, MonsterLootTable, MonsterSkills, RaidSpawnPlan,
};
use crate::world::moveuse::{MoveUseDatabase, MoveUseExpr, MoveUseRule, MoveUseSection};
use crate::world::npc::NpcIndex;
use crate::scripting::npc::{
    NpcAction, NpcBehaviourRule, NpcCompareOp, NpcCondition, NpcScript, NpcTradeEntry,
};
use crate::world::object_types::{FloorChange, ObjectType, ObjectTypeIndex};
use crate::telemetry::logging;
use crate::world::item_types::ItemTypeIndex;
use crate::world::circles::Circles;
use crate::world::housing::{House, HouseArea, HouseOwner};
use crate::world::area::{circle_positions, cone_positions, line_positions};
use crate::world::position::{Direction, Position, PositionDelta};
use crate::entities::inventory::{Inventory, InventorySlot, INVENTORY_SLOTS};
use crate::entities::item::{ItemAttribute, ItemId, ItemKind, ItemStack, ItemTypeId};
use crate::entities::creature::{CreatureId, Outfit, DEFAULT_OUTFIT};
use crate::entities::effects::{
    DrunkenEffect,
    LightEffect,
    MagicShieldEffect,
    OutfitEffect,
    SpeedEffect,
    StrengthEffect,
};
use crate::entities::player::{ActiveDepot, OpenContainer, PlayerId, PlayerState};
use crate::entities::spells::{
    spell_word_tokens,
    Spell,
    SpellBook,
    SpellEffect,
    SpellEffectKind,
    SpellId,
    SpellKind,
    SpellShape,
    SpellTarget,
    SpellOutfitEffect,
    SummonSpellEffect,
};
use crate::entities::skills::{
    default_skill_row_values,
    skill_exp_for_level,
    skill_id_for_type,
    skill_progress_from_values,
    SkillRow,
    SkillLevel,
    SkillType,
    SKILL_BURNING,
    SKILL_DRUNKEN,
    SKILL_ENERGY,
    SKILL_FED,
    SKILL_FIELD_ACT,
    SKILL_FIELD_COUNT,
    SKILL_FIELD_CYCLE,
    SKILL_FIELD_DELTA,
    SKILL_FIELD_FACTOR_PERCENT,
    SKILL_FIELD_MAX,
    SKILL_FIELD_MAX_COUNT,
    SKILL_FIELD_MIN,
    SKILL_FIELD_NEXT_LEVEL,
    SKILL_FIELD_EXP,
    SKILL_ILLUSION,
    SKILL_LIGHT,
    SKILL_MANASHIELD,
    SKILL_POISON,
    SKILL_SOUL,
};
use crate::entities::stats::Stats;
use crate::combat::conditions::{ConditionKind, ConditionTick};
use crate::combat::damage::{compute_damage, DamageScaleFlags, DamageType};
use crate::combat::rules::CombatRules;
use crate::combat::spells::{
    SpellCastReport,
    SpellCastMessage,
    SpellHit,
    SpellLightUpdate,
    SpellOutfitUpdate,
    SpellSpeedUpdate,
    SpellTargetId,
    SpellTextEffect,
};
use crate::scripting::monster::{MonsterSpell, MonsterSpellEffect, MonsterSpellTarget};
use crate::scripting::value::{split_top_level, ScriptValue};
use crate::persistence::store::{SaveStore, skill_rows_from_player};
use std::path::{Path, PathBuf};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::world::time::{Cooldown, GameClock, GameTick};

#[derive(Debug, Default)]
pub struct WorldState {
    root: Option<PathBuf>,
    pub map: Map,
    map_original: Option<Map>,
    pub map_dat: Option<MapDat>,
    pub mem_dat: Option<MemDat>,
    pub circles: Option<Circles>,
    pub npc_index: Option<NpcIndex>,
    pub monster_index: Option<MonsterIndex>,
    pub monster_homes: Vec<MonsterHome>,
    pub npcs: HashMap<CreatureId, NpcInstance>,
    pub monsters: HashMap<CreatureId, MonsterInstance>,
    monster_sector_index: HashMap<SectorCoord, Vec<CreatureId>>,
    monster_sector_index_count: usize,
    next_status_effect_tick: Option<GameTick>,
    pub raid_events: Vec<RaidSpawnEvent>,
    raid_schedules: HashMap<String, RaidSchedule>,
    pub house_areas: Option<Vec<HouseArea>>,
    pub houses: Option<Vec<House>>,
    pub house_owners: Option<Vec<HouseOwner>>,
    pub house_position_index: Option<HashMap<Position, usize>>,
    next_house_rent_check: Option<u64>,
    pub moveuse: Option<MoveUseDatabase>,
    pub object_types: Option<ObjectTypeIndex>,
    pub item_types: Option<ItemTypeIndex>,
    pub cron: crate::world::cron::CronSystem,
    pub players: HashMap<PlayerId, PlayerState>,
    pub offline_players: HashMap<PlayerId, PlayerState>,
    pub spellbook: SpellBook,
    pub combat_rules: CombatRules,
    pending_messages: Vec<MoveUseMessage>,
    pending_skill_updates: Vec<PlayerId>,
    pending_data_updates: Vec<PlayerId>,
    pending_turn_updates: Vec<PendingTurnUpdate>,
    pending_outfit_updates: Vec<PendingOutfitUpdate>,
    pending_map_refreshes: Vec<PendingMapRefresh>,
    pending_buddy_updates: Vec<PendingBuddyUpdate>,
    pending_party_updates: Vec<PendingPartyUpdate>,
    pending_trade_updates: Vec<PendingTradeUpdate>,
    pending_container_closes: HashMap<PlayerId, Vec<u8>>,
    pending_container_refresh: HashSet<PlayerId>,
    shop_sessions: HashMap<PlayerId, ShopSession>,
    request_queue: Vec<RequestQueueEntry>,
    request_queue_players: HashSet<PlayerId>,
    private_channels: HashMap<u16, PrivateChannel>,
    private_channel_owners: HashMap<PlayerId, u16>,
    next_private_channel_id: u16,
    parties: HashMap<u32, PartyState>,
    next_party_id: u32,
    pending_moveuse_outcomes: HashMap<PlayerId, Vec<MoveUseOutcome>>,
    trade_sessions: HashMap<u32, TradeSession>,
    trade_by_player: HashMap<PlayerId, u32>,
    next_trade_id: u32,
    next_text_edit_id: u32,
    text_edit_sessions: HashMap<u32, TextEditSession>,
    next_list_edit_id: u32,
    list_edit_sessions: HashMap<u32, ListEditSession>,
    moveuse_rng: MoveUseRng,
    loot_rng: LootRng,
    monster_rng: MonsterRng,
    npc_rng: NpcRng,
    next_npc_id: u32,
    next_monster_id: u32,
    refresh_state: Option<MapRefreshState>,
    skill_tick_last: Option<GameTick>,
    monster_home_tick_last: Option<GameTick>,
    cron_tick_last: Option<GameTick>,
    cron_tick_accum: u64,
    cron_round: u32,
    item_index: HashMap<ItemId, ItemPath>,
    item_index_dirty: bool,
}

#[derive(Debug, Clone)]
struct MapRefreshState {
    next_x: u16,
    next_y: u16,
    cooldown: Cooldown,
    cylinders_per_minute: u16,
    bounds: SectorBounds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ItemRoot {
    Tile { position: Position, index: usize },
    Inventory { player_id: PlayerId, slot: InventorySlot },
    InventoryContainer { player_id: PlayerId, slot: InventorySlot, index: usize },
    Depot { player_id: PlayerId, depot_id: u16, index: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ItemPath {
    root: ItemRoot,
    path: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MailAddress {
    name: String,
    depot_id: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MailDelivery {
    DeliveredOnline(PlayerId),
    DeliveredOffline,
    Failed,
}

#[derive(Debug, Clone)]
struct PrivateChannel {
    id: u16,
    owner: PlayerId,
    name: String,
    invited: HashSet<PlayerId>,
}

const PARTY_SHIELD_NONE: u8 = 0;
const PARTY_SHIELD_WHITE_YELLOW: u8 = 1;
const PARTY_SHIELD_WHITE_BLUE: u8 = 2;
const PARTY_SHIELD_BLUE: u8 = 3;
const PARTY_SHIELD_YELLOW: u8 = 4;
const PARTY_SHIELD_BLUE_SHARE_EXP: u8 = 5;
const PARTY_SHIELD_YELLOW_SHARE_EXP: u8 = 6;
const PARTY_SHARE_RANGE: u16 = 30;
const PARTY_SHARE_Z_RANGE: u8 = 1;
const PARTY_SHARE_MIN_LEVEL_NUMERATOR: u32 = 2;
const PARTY_SHARE_MIN_LEVEL_DENOMINATOR: u32 = 3;
const PARTY_SHARED_EXP_BONUS_PERCENT: f32 = 0.05;
const SHOP_BACKPACK_TYPE_ID: ItemTypeId = ItemTypeId(2854);
const SHOP_BACKPACK_PRICE: u32 = 20;
const SHOP_BACKPACK_CAPACITY: u32 = 20;
const SHOP_BACKPACK_WEIGHT: u32 = 1800;
const SHOP_BACKPACK_STACK_CAPACITY: u32 = 100;
const SHOP_CAPACITY_SCALE: u32 = 100;
const FOOD_SECONDS_PER_NUTRITION: u64 = 12;
const FOOD_MAX_SECONDS: u64 = 1200;
const DEPOT_CHEST_TYPE_ID: ItemTypeId = ItemTypeId(3502);
const DUSTBIN_TYPE_ID: ItemTypeId = ItemTypeId(2526);
const DEFAULT_SKILL_LIGHT_COLOR: u8 = 215;
const SOUL_REGEN_BASE_INTERVAL_SECS: i32 = 120;
const SOUL_REGEN_PROMO_INTERVAL_SECS: i32 = 15;

#[derive(Debug, Clone)]
struct PartyState {
    leader: PlayerId,
    members: Vec<PlayerId>,
    invited: HashSet<PlayerId>,
    shared_exp_active: bool,
    shared_exp_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartyMarkUpdate {
    pub target_id: PlayerId,
    pub mark: u8,
}

#[derive(Debug, Clone)]
struct PendingPartyUpdate {
    player_id: PlayerId,
    update: PartyMarkUpdate,
}

#[derive(Debug, Clone)]
pub enum TradeUpdate {
    Offer {
        counter: bool,
        name: String,
        items: Vec<ItemStack>,
    },
    Close,
}

#[derive(Debug, Clone)]
struct PendingTradeUpdate {
    player_id: PlayerId,
    update: TradeUpdate,
}

#[derive(Debug, Clone)]
pub(crate) struct RequestQueueEntry {
    pub(crate) player_id: PlayerId,
    pub(crate) name: String,
}

#[derive(Debug, Clone)]
struct TradeSession {
    requester: PlayerId,
    partner: PlayerId,
    offer_requester: Vec<TradeItem>,
    offer_partner: Vec<TradeItem>,
    requester_accepted: bool,
    partner_accepted: bool,
}

#[derive(Debug, Clone)]
struct TradeItem {
    item: ItemStack,
    position: Position,
    stack_pos: u8,
}

#[derive(Debug, Clone)]
struct ShopSessionItem {
    type_id: ItemTypeId,
    sub_type: u8,
    buy_price: u32,
    sell_price: u32,
}

#[derive(Debug, Clone)]
struct ShopSession {
    items: Vec<ShopSessionItem>,
}

#[derive(Debug, Clone)]
enum TextEditTarget {
    Position { position: Position, stack_index: usize },
    Inventory { slot: InventorySlot },
    Container { container_id: u8, slot: u8 },
}

#[derive(Debug, Clone)]
struct TextEditSession {
    player_id: PlayerId,
    item_type: ItemTypeId,
    target: TextEditTarget,
    max_len: u16,
    can_write: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HouseListKind {
    Guests,
    Subowners,
}

#[derive(Debug, Clone)]
enum ListEditTarget {
    Door {
        position: Position,
        stack_index: usize,
        item_type: ItemTypeId,
    },
    House {
        house_id: u32,
        kind: HouseListKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HouseAccessLevel {
    None,
    Guest,
    Subowner,
    Owner,
}

#[derive(Debug, Clone)]
struct ListEditSession {
    player_id: PlayerId,
    list_type: u8,
    target: ListEditTarget,
    max_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterInstance {
    pub id: CreatureId,
    pub race_number: i64,
    pub summoner: Option<PlayerId>,
    pub summoned: bool,
    pub home_id: Option<usize>,
    pub name: String,
    pub position: Position,
    pub direction: Direction,
    pub outfit: Outfit,
    pub stats: Stats,
    pub experience: u32,
    pub loot: MonsterLootTable,
    pub inventory: Inventory,
    pub inventory_containers: HashMap<InventorySlot, Vec<ItemStack>>,
    pub corpse_ids: Vec<ItemTypeId>,
    pub flags: MonsterFlags,
    pub skills: MonsterSkills,
    pub attack: u32,
    pub defend: u32,
    pub armor: u32,
    pub poison: u32,
    pub strategy: [u8; 4],
    pub spells: Vec<MonsterSpell>,
    pub flee_threshold: u32,
    pub lose_target_distance: u16,
    pub target: Option<PlayerId>,
    pub damage_by: HashMap<PlayerId, u32>,
    pub speed: u16,
    pub outfit_effect: Option<OutfitEffect>,
    pub speed_effect: Option<SpeedEffect>,
    pub strength_effect: Option<StrengthEffect>,
    pub move_cooldown: Cooldown,
    pub combat_cooldown: Cooldown,
    pub talk_lines: Vec<String>,
    pub talk_cooldown: Cooldown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DefendSelection {
    defend: u32,
    armor: u32,
    slot: Option<InventorySlot>,
    item_type: Option<ItemTypeId>,
    skill: SkillType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AttackSelection {
    attack: u32,
    range: u16,
    skill: SkillType,
    damage_type: DamageType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpcInstance {
    pub id: CreatureId,
    pub script_key: String,
    pub name: String,
    pub position: Position,
    pub direction: Direction,
    pub home: Position,
    pub outfit: Outfit,
    pub radius: u16,
    pub focused: Option<PlayerId>,
    pub focus_expires_at: Option<GameTick>,
    pub queue: VecDeque<PlayerId>,
    pub move_cooldown: Cooldown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpcTalkResponse {
    pub npc_id: CreatureId,
    pub name: String,
    pub position: Position,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpcTalkOutcome {
    pub responses: Vec<NpcTalkResponse>,
    pub effects: Vec<MoveUseEffect>,
    pub containers_dirty: bool,
    pub shop: Option<ShopOpenResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShopItemView {
    pub type_id: ItemTypeId,
    pub sub_type: u8,
    pub description: String,
    pub weight: u32,
    pub buy_price: u32,
    pub sell_price: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShopSellEntry {
    pub type_id: ItemTypeId,
    pub count: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShopSellList {
    pub money: u32,
    pub entries: Vec<ShopSellEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShopOpenResult {
    pub items: Vec<ShopItemView>,
    pub sell_list: ShopSellList,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerUpdate {
    Add { container_id: u8, item: ItemStack },
    Update { container_id: u8, slot: u8, item: ItemStack },
    Remove { container_id: u8, slot: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerSource {
    InventorySlot(InventorySlot),
    Container { container_id: u8, slot: u8 },
    Map { position: Position, stack_pos: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuddyEntry {
    pub id: PlayerId,
    pub name: String,
    pub online: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelInviteResult {
    Invited {
        channel_id: u16,
        channel_name: String,
        invitee_id: PlayerId,
        invitee_name: String,
    },
    AlreadyInvited { invitee_name: String },
    SelfInvite,
    NoChannel,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelExcludeResult {
    Excluded {
        channel_id: u16,
        invitee_id: PlayerId,
        invitee_name: String,
    },
    NotInvited { invitee_name: String },
    SelfExclude,
    NoChannel,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuddyStatusUpdate {
    pub buddy_id: PlayerId,
    pub online: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuddyAddResult {
    Added(BuddyEntry),
    AlreadyPresent(BuddyEntry),
    NotFound,
    SelfBuddy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterReward {
    pub experience: u32,
    pub drops: Vec<ItemStack>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterVisualEffect {
    pub position: Position,
    pub effect_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombatVisualEffect {
    pub position: Position,
    pub effect_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterMissileEffect {
    pub from: Position,
    pub to: Position,
    pub missile_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonsterTalk {
    pub monster_id: CreatureId,
    pub name: String,
    pub position: Position,
    pub talk_type: u8,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatureOutfitUpdate {
    pub id: u32,
    pub outfit: Outfit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatureSpeedUpdate {
    pub id: u32,
    pub speed: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatureLightUpdate {
    pub id: u32,
    pub level: u8,
    pub color: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlayerHitMarker {
    pub player_id: PlayerId,
    pub attacker_id: CreatureId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatureTurnUpdate {
    pub id: u32,
    pub position: Position,
    pub direction: Direction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatureStep {
    pub id: CreatureId,
    pub from: Position,
    pub to: Position,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MonsterTickOutcome {
    pub moved: usize,
    pub moves: Vec<CreatureStep>,
    pub effects: Vec<MonsterVisualEffect>,
    pub missiles: Vec<MonsterMissileEffect>,
    pub talks: Vec<MonsterTalk>,
    pub player_hits: Vec<PlayerId>,
    pub player_hit_marks: Vec<PlayerHitMarker>,
    pub monster_updates: Vec<CreatureId>,
    pub outfit_updates: Vec<CreatureOutfitUpdate>,
    pub speed_updates: Vec<CreatureSpeedUpdate>,
    pub refresh_map: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PlayerCombatOutcome {
    pub effects: Vec<CombatVisualEffect>,
    pub monster_updates: Vec<CreatureId>,
    pub player_updates: Vec<PlayerId>,
    pub refresh_map: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CreatureStatusUpdates {
    pub outfit_updates: Vec<CreatureOutfitUpdate>,
    pub speed_updates: Vec<CreatureSpeedUpdate>,
    pub light_updates: Vec<CreatureLightUpdate>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FoodRegenOutcome {
    pub data_updates: Vec<PlayerId>,
    pub health_updates: Vec<PlayerId>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SkillTimerOutcome {
    pub data_updates: Vec<PlayerId>,
    pub health_updates: Vec<PlayerId>,
    pub status_updates: CreatureStatusUpdates,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingTurnUpdate {
    player_id: PlayerId,
    update: CreatureTurnUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingOutfitUpdate {
    player_id: PlayerId,
    update: CreatureOutfitUpdate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingMapRefresh {
    player_id: PlayerId,
    position: Position,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingBuddyUpdate {
    player_id: PlayerId,
    update: BuddyStatusUpdate,
}

#[derive(Debug, Clone, Copy)]
enum MonsterSpellTargetMeta {
    Actor { effect_id: u16 },
    Victim {
        range: u16,
        missile_id: u8,
        effect_id: u16,
    },
    Origin { radius: u8, effect_id: u16 },
    Destination {
        range: u16,
        missile_id: u8,
        radius: u8,
        effect_id: u16,
    },
    Angle {
        angle: u16,
        range: u8,
        effect_id: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaidSpawnEvent {
    pub at: GameTick,
    pub plan: RaidSpawnPlan,
}

#[derive(Debug, Clone, Copy)]
struct RaidSchedule {
    interval_ticks: u64,
    next_at: GameTick,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpellCostMode {
    Standard,
    Rune,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogoutBlockReason {
    ProtectionZone,
    NoLogoutZone,
    InFight,
}

impl LogoutBlockReason {
    fn as_error(self) -> &'static str {
        match self {
            LogoutBlockReason::ProtectionZone => "logout blocked: protection zone",
            LogoutBlockReason::NoLogoutZone => "logout blocked: no-logout zone",
            LogoutBlockReason::InFight => "logout blocked: in fight",
        }
    }
}

const MONSTER_MOVE_INTERVAL_TICKS: u64 = 5;
const MONSTER_MOVE_ATTEMPTS: usize = 4;
const MONSTER_ACQUIRE_RANGE: u16 = 8;
const MONSTER_ACTIVE_RANGE: u16 = 12;
const MONSTER_COMBAT_INTERVAL_TICKS: u64 = 20;
const MONSTER_MELEE_RANGE: u16 = 1;
const PLAYER_SUMMON_LIMIT: usize = 2;
const MONSTER_FIELD_TICK_DAMAGE: u32 = 5;
const MONSTER_TALK_MIN_TICKS: u64 = 100;
const MONSTER_TALK_MAX_TICKS: u64 = 300;
const PLAYER_COMBAT_INTERVAL_TICKS: u64 = 20;
const PLAYER_MELEE_RANGE: u16 = 1;
const PLAYER_THROW_RANGE: u16 = 2;
const PLAYER_DISTANCE_RANGE: u16 = 3;
const PLAYER_ATTACK_EFFECT_BLOOD_ID: u16 = 1;
const PLAYER_ATTACK_EFFECT_BLOCK_ID: u16 = 4;
const SKILL_TRAINING_POINTS: u8 = 30;
const MESSAGE_EVENT: u8 = 0x14;
const DEFEND_COOLDOWN_MS: u64 = 2000;
const DRUNKEN_CHANCE_PER_LEVEL: u32 = 10;
const FIRE_FIELD_TYPE_ID: u16 = 2118;
const POISON_FIELD_TYPE_ID: u16 = 2121;
const ENERGY_FIELD_TYPE_ID: u16 = 2122;
const MAGIC_WALL_TYPE_ID: u16 = 2128;
const WILD_GROWTH_TYPE_ID: u16 = 2130;
const NPC_MOVE_INTERVAL_TICKS: u64 = 20;
const NPC_MOVE_ATTEMPTS: usize = 4;
const NPC_ACTIVE_RANGE: u16 = 10;
const ACTIVE_Z_RANGE_UNDERGROUND: u8 = 2;
const MAX_FLOOR: u8 = 15;
const NPC_TALK_RANGE: u16 = 4;
const NPC_FOCUS_TIMEOUT_SECS: u64 = 30;
const NPC_ID_BASE: u32 = 0x4000_0000;
const DEFAULT_GROUND_SPEED: u16 = 150;
const DEFAULT_MONSTER_SPEED: u16 = 220;
const DEFAULT_CONTAINER_MEANING: u16 = 14;
const REFRESH_INTERVAL_SECS: u64 = 60;
const HOUSE_RENT_CHECK_SECS: u64 = 60;
const SECTOR_TILE_SIZE: u16 = 32;
const DEFAULT_TEXT_MAX_LEN: u16 = 1024;
const MAX_EDIT_LIST_LEN: usize = 0x0fa0;
const EDIT_LIST_TYPE_HOUSE_GUEST: u8 = 0;
const EDIT_LIST_TYPE_HOUSE_SUBOWNER: u8 = 1;
const EDIT_LIST_TYPE_NAME_DOOR: u8 = 3;
const GUILD_CHANNEL_ID: u16 = 0x00;
const PRIVATE_CHANNEL_ID_START: u16 = 0x1000;
const DEFAULT_CHANNELS: &[(u16, &str)] = &[
    (0x04, "Game-Chat"),
    (0x05, "Trade"),
    (0x07, "RL-Chat"),
    (0x09, "Help"),
];

fn init_refresh_state(map_dat: Option<&MapDat>, map: &Map) -> Option<MapRefreshState> {
    let refreshed_cylinders = map_dat
        .and_then(|dat| dat.refreshed_cylinders)
        .unwrap_or(0);
    if refreshed_cylinders == 0 {
        return None;
    }
    let bounds = map_dat
        .and_then(|dat| dat.sector_bounds)
        .or(map.sector_bounds)?;
    Some(MapRefreshState {
        next_x: bounds.min.x,
        next_y: bounds.min.y,
        cooldown: Cooldown::new(GameTick(0)),
        cylinders_per_minute: refreshed_cylinders,
        bounds,
    })
}

impl WorldState {
    pub fn load(root: &Path) -> Result<Self, String> {
        let map_dir = root.join("map");
        let map = crate::world::map::load_map(&map_dir)?;
        let map_original = match crate::world::map::load_map(&root.join("origmap")) {
            Ok(original) => Some(original),
            Err(_) => Some(map.clone()),
        };
        let map_dat_path = root.join("dat").join("map.dat");
        let map_dat = match MapDat::load(&map_dat_path) {
            Ok(map_dat) => Some(map_dat),
            Err(err) => {
                eprintln!("tibia: map.dat read skipped: {}", err);
                None
            }
        };
        let mem_dat_path = root.join("dat").join("mem.dat");
        let mem_dat = match MemDat::load(&mem_dat_path) {
            Ok(mem_dat) => Some(mem_dat),
            Err(err) => {
                eprintln!("tibia: mem.dat read skipped: {}", err);
                None
            }
        };
        let circles_path = root.join("dat").join("circles.dat");
        let circles = match Circles::load(&circles_path) {
            Ok(circles) => Some(circles),
            Err(err) => {
                eprintln!("tibia: circles.dat read skipped: {}", err);
                None
            }
        };
        let npc_dir = root.join("npc");
        let npc_index = match crate::world::npc::load_npcs(&npc_dir) {
            Ok(npcs) => Some(npcs),
            Err(err) => {
                eprintln!("tibia: npc scripts read skipped: {}", err);
                None
            }
        };
        let mon_dir = root.join("mon");
        let monster_index = match crate::world::monsters::load_monsters(&mon_dir) {
            Ok(monsters) => Some(monsters),
            Err(err) => {
                eprintln!("tibia: monster scripts read skipped: {}", err);
                None
            }
        };
        let monster_homes_path = root.join("dat").join("monster.db");
        let (monster_homes, monster_homes_source) = match load_monster_homes(&monster_homes_path) {
            Ok(homes) => (homes, monster_homes_path.to_string_lossy().into_owned()),
            Err(err) => {
                let fallback_path = root.join("dat").join("newmon.db");
                match load_monster_homes(&fallback_path) {
                    Ok(homes) => {
                        eprintln!(
                            "tibia: monster.db read skipped ({}), using newmon.db",
                            err
                        );
                        (homes, fallback_path.to_string_lossy().into_owned())
                    }
                    Err(fallback_err) => {
                        eprintln!(
                            "tibia: monster.db read skipped: {} (fallback failed: {})",
                            err, fallback_err
                        );
                        (Vec::new(), "none".to_string())
                    }
                }
            }
        };
        let homes_msg = format!(
            "tibia: loaded {} monster homes from {}",
            monster_homes.len(),
            monster_homes_source
        );
        println!("{homes_msg}");
        logging::log_game(&homes_msg);
        let house_areas_path = root.join("dat").join("houseareas.dat");
        let house_areas = match crate::world::housing::load_house_areas(&house_areas_path) {
            Ok(areas) => Some(areas),
            Err(err) => {
                eprintln!("tibia: houseareas.dat read skipped: {}", err);
                None
            }
        };
        let houses_path = root.join("dat").join("houses.dat");
        let houses = match crate::world::housing::load_houses(&houses_path) {
            Ok(houses) => Some(houses),
            Err(err) => {
                eprintln!("tibia: houses.dat read skipped: {}", err);
                None
            }
        };
        let house_position_index = houses.as_ref().map(|houses| {
            let mut index = HashMap::new();
            for (house_index, house) in houses.iter().enumerate() {
                for position in &house.fields {
                    index.insert(*position, house_index);
                }
            }
            index
        });
        let owners_path = root.join("dat").join("owners.dat");
        let house_owners = match crate::world::housing::load_house_owners(&owners_path) {
            Ok(owners) => Some(owners),
            Err(err) => {
                eprintln!("tibia: owners.dat read skipped: {}", err);
                None
            }
        };
        let moveuse_path = root.join("dat").join("moveuse.dat");
        let moveuse = match crate::world::moveuse::load_moveuse(&moveuse_path) {
            Ok(moveuse) => Some(moveuse),
            Err(err) => {
                eprintln!("tibia: moveuse.dat read skipped: {}", err);
                None
            }
        };
        let objects_path = root.join("dat").join("objects.srv");
        let object_types = match crate::world::object_types::load_object_types(&objects_path) {
            Ok(object_types) => Some(object_types),
            Err(err) => {
                eprintln!("tibia: objects.srv read skipped: {}", err);
                None
            }
        };
        let item_types = object_types
            .as_ref()
            .map(crate::world::item_types::build_item_types);
        let refresh_state = init_refresh_state(map_dat.as_ref(), &map);
        let mut world = Self {
            root: Some(root.to_path_buf()),
            map,
            map_original,
            map_dat,
            mem_dat,
            circles,
            npc_index,
            monster_index,
            monster_homes,
            npcs: HashMap::new(),
            monsters: HashMap::new(),
            monster_sector_index: HashMap::new(),
            monster_sector_index_count: 0,
            next_status_effect_tick: None,
            raid_events: Vec::new(),
            raid_schedules: HashMap::new(),
            house_areas,
            houses,
            house_owners,
            house_position_index,
            next_house_rent_check: None,
            moveuse,
            object_types,
            item_types,
            cron: crate::world::cron::CronSystem::new(),
            players: HashMap::new(),
            offline_players: HashMap::new(),
            spellbook: SpellBook::default(),
            combat_rules: CombatRules::default(),
            pending_messages: Vec::new(),
            pending_skill_updates: Vec::new(),
            pending_data_updates: Vec::new(),
            pending_turn_updates: Vec::new(),
            pending_outfit_updates: Vec::new(),
            pending_map_refreshes: Vec::new(),
            pending_buddy_updates: Vec::new(),
            pending_party_updates: Vec::new(),
            pending_trade_updates: Vec::new(),
            pending_container_closes: HashMap::new(),
            pending_container_refresh: HashSet::new(),
            shop_sessions: HashMap::new(),
            request_queue: Vec::new(),
            request_queue_players: HashSet::new(),
            private_channels: HashMap::new(),
            private_channel_owners: HashMap::new(),
            next_private_channel_id: PRIVATE_CHANNEL_ID_START,
            parties: HashMap::new(),
            next_party_id: 1,
            pending_moveuse_outcomes: HashMap::new(),
            trade_sessions: HashMap::new(),
            trade_by_player: HashMap::new(),
            next_trade_id: 1,
            next_text_edit_id: 1,
            text_edit_sessions: HashMap::new(),
            next_list_edit_id: 1,
            list_edit_sessions: HashMap::new(),
            moveuse_rng: MoveUseRng::from_time(),
            loot_rng: LootRng::default(),
            monster_rng: MonsterRng::from_time(),
            npc_rng: NpcRng::from_time(),
            next_npc_id: NPC_ID_BASE,
            next_monster_id: 1,
            refresh_state,
            skill_tick_last: None,
            monster_home_tick_last: None,
            cron_tick_last: None,
            cron_tick_accum: 0,
            cron_round: 0,
            item_index: HashMap::new(),
            item_index_dirty: true,
        };
        if let Err(err) = crate::entities::spells::register_builtin_spells(&mut world.spellbook) {
            eprintln!("tibia: spellbook init skipped: {}", err);
        }
        if let Some(object_types) = world.object_types.as_ref() {
            world.validate_rune_spell_items(object_types);
        }
        world.spawn_npcs_from_index();
        world.spawn_monsters_from_homes();
        world.schedule_cron_for_world();
        world.rebuild_item_index();
        Ok(world)
    }

    fn queue_message(&mut self, message: MoveUseMessage) {
        self.pending_messages.push(message);
    }

    pub(crate) fn queue_player_message(
        &mut self,
        player_id: PlayerId,
        message_type: u8,
        message: String,
    ) {
        self.queue_message(MoveUseMessage {
            player_id,
            message_type,
            message,
        });
    }

    pub(crate) fn queue_player_skills_update(&mut self, player_id: PlayerId) {
        if !self.pending_skill_updates.contains(&player_id) {
            self.pending_skill_updates.push(player_id);
        }
    }

    pub(crate) fn queue_player_data_update(&mut self, player_id: PlayerId) {
        if !self.pending_data_updates.contains(&player_id) {
            self.pending_data_updates.push(player_id);
        }
    }

    pub(crate) fn take_pending_messages(&mut self, player_id: PlayerId) -> Vec<MoveUseMessage> {
        if self.pending_messages.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_messages);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        for message in pending.drain(..) {
            if message.player_id == player_id {
                ready.push(message);
            } else {
                remaining.push(message);
            }
        }
        self.pending_messages = remaining;
        ready
    }

    pub(crate) fn take_pending_skill_update(&mut self, player_id: PlayerId) -> bool {
        if let Some(index) = self
            .pending_skill_updates
            .iter()
            .position(|id| *id == player_id)
        {
            self.pending_skill_updates.swap_remove(index);
            return true;
        }
        false
    }

    pub(crate) fn take_pending_data_update(&mut self, player_id: PlayerId) -> bool {
        if let Some(index) = self
            .pending_data_updates
            .iter()
            .position(|id| *id == player_id)
        {
            self.pending_data_updates.swap_remove(index);
            return true;
        }
        false
    }

    fn queue_moveuse_outcomes(&mut self, player_id: PlayerId, outcomes: Vec<MoveUseOutcome>) {
        if outcomes.is_empty() {
            return;
        }
        let entry = self
            .pending_moveuse_outcomes
            .entry(player_id)
            .or_insert_with(Vec::new);
        entry.extend(outcomes.into_iter().filter(moveuse_outcome_has_payload));
    }

    pub(crate) fn take_pending_moveuse_outcomes(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<MoveUseOutcome> {
        self.pending_moveuse_outcomes
            .remove(&player_id)
            .unwrap_or_default()
    }

    fn queue_turn_update(&mut self, update: CreatureTurnUpdate) {
        let targets: Vec<PlayerId> = self.players.keys().copied().collect();
        for player_id in targets {
            self.pending_turn_updates.push(PendingTurnUpdate { player_id, update });
        }
    }

    fn queue_outfit_update(&mut self, update: CreatureOutfitUpdate) {
        let targets: Vec<PlayerId> = self.players.keys().copied().collect();
        for player_id in targets {
            self.pending_outfit_updates
                .push(PendingOutfitUpdate {
                    player_id,
                    update: update.clone(),
                });
        }
    }

    fn queue_map_refresh(&mut self, position: Position) {
        let targets: Vec<PlayerId> = self.players.keys().copied().collect();
        for player_id in targets {
            self.pending_map_refreshes.push(PendingMapRefresh {
                player_id,
                position,
            });
        }
    }

    pub(crate) fn take_pending_turn_updates(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<CreatureTurnUpdate> {
        if self.pending_turn_updates.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_turn_updates);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        for entry in pending.drain(..) {
            if entry.player_id == player_id {
                ready.push(entry.update);
            } else {
                remaining.push(entry);
            }
        }
        self.pending_turn_updates = remaining;
        ready
    }

    pub(crate) fn take_pending_outfit_updates(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<CreatureOutfitUpdate> {
        if self.pending_outfit_updates.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_outfit_updates);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        for entry in pending.drain(..) {
            if entry.player_id == player_id {
                ready.push(entry.update);
            } else {
                remaining.push(entry);
            }
        }
        self.pending_outfit_updates = remaining;
        ready
    }

    pub(crate) fn take_pending_map_refreshes(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<Position> {
        if self.pending_map_refreshes.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_map_refreshes);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        let mut seen = HashSet::new();
        for entry in pending.drain(..) {
            if entry.player_id == player_id {
                if seen.insert(entry.position) {
                    ready.push(entry.position);
                }
            } else {
                remaining.push(entry);
            }
        }
        self.pending_map_refreshes = remaining;
        ready
    }

    pub(crate) fn queue_buddy_status_update(&mut self, buddy_id: PlayerId, online: bool) {
        let update = BuddyStatusUpdate { buddy_id, online };
        let targets: Vec<PlayerId> = self
            .players
            .iter()
            .filter_map(|(player_id, player)| {
                if *player_id == buddy_id {
                    return None;
                }
                if player.buddies.contains(&buddy_id) {
                    Some(*player_id)
                } else {
                    None
                }
            })
            .collect();
        for player_id in targets {
            self.pending_buddy_updates
                .push(PendingBuddyUpdate { player_id, update });
        }
    }

    pub(crate) fn take_pending_buddy_updates(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<BuddyStatusUpdate> {
        if self.pending_buddy_updates.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_buddy_updates);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        for entry in pending.drain(..) {
            if entry.player_id == player_id {
                ready.push(entry.update);
            } else {
                remaining.push(entry);
            }
        }
        self.pending_buddy_updates = remaining;
        ready
    }

    fn queue_party_update(&mut self, player_id: PlayerId, update: PartyMarkUpdate) {
        self.pending_party_updates
            .push(PendingPartyUpdate { player_id, update });
    }

    pub(crate) fn take_pending_party_updates(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<PartyMarkUpdate> {
        if self.pending_party_updates.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_party_updates);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        for entry in pending.drain(..) {
            if entry.player_id == player_id {
                ready.push(entry.update);
            } else {
                remaining.push(entry);
            }
        }
        self.pending_party_updates = remaining;
        ready
    }

    fn queue_trade_update(&mut self, player_id: PlayerId, update: TradeUpdate) {
        self.pending_trade_updates
            .push(PendingTradeUpdate { player_id, update });
    }

    fn queue_trade_offer(
        &mut self,
        player_id: PlayerId,
        counter: bool,
        name: String,
        items: Vec<ItemStack>,
    ) {
        self.queue_trade_update(
            player_id,
            TradeUpdate::Offer {
                counter,
                name,
                items,
            },
        );
    }

    fn queue_trade_close(&mut self, player_id: PlayerId) {
        self.queue_trade_update(player_id, TradeUpdate::Close);
    }

    pub(crate) fn take_pending_trade_updates(
        &mut self,
        player_id: PlayerId,
    ) -> Vec<TradeUpdate> {
        if self.pending_trade_updates.is_empty() {
            return Vec::new();
        }
        let mut pending = std::mem::take(&mut self.pending_trade_updates);
        let mut remaining = Vec::new();
        let mut ready = Vec::new();
        for entry in pending.drain(..) {
            if entry.player_id == player_id {
                ready.push(entry.update);
            } else {
                remaining.push(entry);
            }
        }
        self.pending_trade_updates = remaining;
        ready
    }

    pub(crate) fn buddy_list_entries(&self, player_id: PlayerId) -> Vec<BuddyEntry> {
        let Some(player) = self.players.get(&player_id) else {
            return Vec::new();
        };
        player
            .buddies
            .iter()
            .copied()
            .map(|buddy_id| self.buddy_entry_for_id(buddy_id))
            .collect()
    }

    pub(crate) fn add_buddy_by_name(
        &mut self,
        player_id: PlayerId,
        name: &str,
    ) -> Result<BuddyAddResult, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Ok(BuddyAddResult::NotFound);
        }
        let Some(buddy_id) = self.find_player_id_by_name(trimmed)? else {
            return Ok(BuddyAddResult::NotFound);
        };
        if buddy_id == player_id {
            return Ok(BuddyAddResult::SelfBuddy);
        }
        let Some(player) = self.players.get_mut(&player_id) else {
            return Err(format!("unknown player {:?}", player_id));
        };
        if !player.buddies.insert(buddy_id) {
            let entry = self.buddy_entry_for_id(buddy_id);
            return Ok(BuddyAddResult::AlreadyPresent(entry));
        }
        let entry = self.buddy_entry_for_id(buddy_id);
        Ok(BuddyAddResult::Added(entry))
    }

    pub(crate) fn remove_buddy(
        &mut self,
        player_id: PlayerId,
        buddy_id: PlayerId,
    ) -> Result<bool, String> {
        let Some(player) = self.players.get_mut(&player_id) else {
            return Err(format!("unknown player {:?}", player_id));
        };
        Ok(player.buddies.remove(&buddy_id))
    }

    fn party_id_for_player(&self, player_id: PlayerId) -> Option<u32> {
        self.players.get(&player_id).and_then(|player| player.party_id)
    }

    fn party_for_player(&self, player_id: PlayerId) -> Option<&PartyState> {
        let party_id = self.party_id_for_player(player_id)?;
        self.parties.get(&party_id)
    }

    fn party_members(party: &PartyState) -> Vec<PlayerId> {
        let mut members = Vec::with_capacity(party.members.len() + 1);
        members.push(party.leader);
        members.extend(party.members.iter().copied());
        members
    }

    fn party_shared_exp_min_level(highest_level: u16) -> u16 {
        let highest = u32::from(highest_level);
        let numerator = highest.saturating_mul(PARTY_SHARE_MIN_LEVEL_NUMERATOR);
        let min_level = numerator
            .saturating_add(PARTY_SHARE_MIN_LEVEL_DENOMINATOR - 1)
            / PARTY_SHARE_MIN_LEVEL_DENOMINATOR;
        min_level.min(u32::from(u16::MAX)) as u16
    }

    fn party_member_in_share_range(leader: Position, member: Position) -> bool {
        let dx = i32::from(leader.x) - i32::from(member.x);
        let dy = i32::from(leader.y) - i32::from(member.y);
        let dz = i32::from(leader.z) - i32::from(member.z);
        dx.unsigned_abs() <= u32::from(PARTY_SHARE_RANGE)
            && dy.unsigned_abs() <= u32::from(PARTY_SHARE_RANGE)
            && dz.unsigned_abs() <= u32::from(PARTY_SHARE_Z_RANGE)
    }

    fn party_can_enable_shared_exp(&self, leader_id: PlayerId, members: &[PlayerId]) -> bool {
        if members.len() < 2 {
            return false;
        }
        let leader = match self.players.get(&leader_id) {
            Some(player) => player,
            None => return false,
        };
        let mut highest_level = leader.level;
        for member_id in members {
            let Some(player) = self.players.get(member_id) else {
                return false;
            };
            if player.level > highest_level {
                highest_level = player.level;
            }
        }
        let min_level = Self::party_shared_exp_min_level(highest_level);
        for member_id in members {
            let Some(player) = self.players.get(member_id) else {
                return false;
            };
            if player.level < min_level {
                return false;
            }
            if !Self::party_member_in_share_range(leader.position, player.position) {
                return false;
            }
        }
        true
    }

    fn party_shared_exp_members(&mut self, party_id: u32) -> Option<Vec<PlayerId>> {
        let (shared_exp_active, leader_id, members) = {
            let party = self.parties.get(&party_id)?;
            (
                party.shared_exp_active,
                party.leader,
                Self::party_members(party),
            )
        };
        let eligible = shared_exp_active && self.party_can_enable_shared_exp(leader_id, &members);
        if let Some(party) = self.parties.get_mut(&party_id) {
            if party.shared_exp_enabled != eligible {
                party.shared_exp_enabled = eligible;
                self.queue_party_marks_for_players(&members);
            }
        }
        if eligible {
            Some(members)
        } else {
            None
        }
    }

    fn party_shared_exp_share(experience: u32, member_count: usize) -> u32 {
        if member_count == 0 {
            return 0;
        }
        let member_count = member_count as f32;
        let experience_f = experience as f32;
        let base = experience_f / member_count;
        let bonus = experience_f * PARTY_SHARED_EXP_BONUS_PERCENT;
        (base + bonus).ceil() as u32
    }

    pub(crate) fn party_mark_for_viewer(&self, viewer_id: PlayerId, target_id: PlayerId) -> u8 {
        if viewer_id == target_id {
            let Some(party) = self.party_for_player(viewer_id) else {
                return PARTY_SHIELD_NONE;
            };
            if party.leader == viewer_id {
                return if party.shared_exp_enabled {
                    PARTY_SHIELD_YELLOW_SHARE_EXP
                } else {
                    PARTY_SHIELD_YELLOW
                };
            }
            return if party.shared_exp_enabled {
                PARTY_SHIELD_BLUE_SHARE_EXP
            } else {
                PARTY_SHIELD_BLUE
            };
        }

        if let Some(party) = self.party_for_player(viewer_id) {
            if party.leader == target_id {
                return if party.shared_exp_enabled {
                    PARTY_SHIELD_YELLOW_SHARE_EXP
                } else {
                    PARTY_SHIELD_YELLOW
                };
            }
            if party.members.contains(&target_id) {
                return if party.shared_exp_enabled {
                    PARTY_SHIELD_BLUE_SHARE_EXP
                } else {
                    PARTY_SHIELD_BLUE
                };
            }
            if party.leader == viewer_id && party.invited.contains(&target_id) {
                return PARTY_SHIELD_WHITE_BLUE;
            }
        }

        if let Some(target_party) = self.party_for_player(target_id) {
            if target_party.leader == target_id && target_party.invited.contains(&viewer_id) {
                return PARTY_SHIELD_WHITE_YELLOW;
            }
        }

        PARTY_SHIELD_NONE
    }

    fn queue_party_marks_for_players(&mut self, players: &[PlayerId]) {
        let mut unique: Vec<PlayerId> = players
            .iter()
            .copied()
            .filter(|player_id| self.players.contains_key(player_id))
            .collect();
        unique.sort_by_key(|player_id| player_id.0);
        unique.dedup();
        for viewer_id in &unique {
            for target_id in &unique {
                if viewer_id == target_id {
                    continue;
                }
                let mark = self.party_mark_for_viewer(*viewer_id, *target_id);
                self.queue_party_update(
                    *viewer_id,
                    PartyMarkUpdate {
                        target_id: *target_id,
                        mark,
                    },
                );
            }
        }
    }

    fn disband_party(&mut self, party_id: u32) {
        let Some(party) = self.parties.remove(&party_id) else {
            return;
        };
        let mut members = Vec::with_capacity(party.members.len() + 1);
        members.push(party.leader);
        members.extend(party.members.iter().copied());
        for member_id in &members {
            if let Some(player) = self.players.get_mut(member_id) {
                player.party_id = None;
            }
        }
        for member_id in &members {
            self.queue_player_message(
                *member_id,
                0x14,
                "Your party has been disbanded.".to_string(),
            );
        }
        let mut update_players = members;
        update_players.extend(party.invited.iter().copied());
        if !update_players.is_empty() {
            self.queue_party_marks_for_players(&update_players);
        }
    }

    pub(crate) fn party_invite(
        &mut self,
        leader_id: PlayerId,
        target_id: PlayerId,
    ) -> Result<(), String> {
        if leader_id == target_id {
            self.queue_player_message(leader_id, 0x14, "You cannot invite yourself.".to_string());
            return Ok(());
        }
        let Some(target_name) = self.player_name_by_id(target_id) else {
            self.queue_player_message(leader_id, 0x14, "Player not found.".to_string());
            return Ok(());
        };
        if self.party_id_for_player(target_id).is_some() {
            self.queue_player_message(
                leader_id,
                0x14,
                format!("{target_name} is already in a party."),
            );
            return Ok(());
        }
        let party_id = match self.party_id_for_player(leader_id) {
            Some(party_id) => {
                let Some(party) = self.parties.get(&party_id) else {
                    return Err("party state missing".to_string());
                };
                if party.leader != leader_id {
                    self.queue_player_message(
                        leader_id,
                        0x14,
                        "Only the party leader can invite players.".to_string(),
                    );
                    return Ok(());
                }
                party_id
            }
            None => {
                let party_id = if self.next_party_id == 0 {
                    1
                } else {
                    self.next_party_id
                };
                self.next_party_id = party_id.saturating_add(1).max(1);
                let party = PartyState {
                    leader: leader_id,
                    members: Vec::new(),
                    invited: HashSet::new(),
                    shared_exp_active: false,
                    shared_exp_enabled: false,
                };
                self.parties.insert(party_id, party);
                if let Some(player) = self.players.get_mut(&leader_id) {
                    player.party_id = Some(party_id);
                }
                party_id
            }
        };
        let Some(party) = self.parties.get_mut(&party_id) else {
            return Err("party state missing".to_string());
        };
        if !party.invited.insert(target_id) {
            self.queue_player_message(
                leader_id,
                0x14,
                format!("{target_name} is already invited."),
            );
            return Ok(());
        }
        let leader_name = self
            .player_name_by_id(leader_id)
            .unwrap_or_else(|| format!("Player {}", leader_id.0));
        self.queue_player_message(
            leader_id,
            0x14,
            format!("Invitation sent to {target_name}."),
        );
        self.queue_player_message(
            target_id,
            0x14,
            format!("{leader_name} has invited you to the party."),
        );
        self.queue_party_marks_for_players(&[leader_id, target_id]);
        Ok(())
    }

    pub(crate) fn party_join(
        &mut self,
        player_id: PlayerId,
        leader_id: PlayerId,
    ) -> Result<(), String> {
        if self.party_id_for_player(player_id).is_some() {
            self.queue_player_message(player_id, 0x14, "You are already in a party.".to_string());
            return Ok(());
        }
        let Some(party_id) = self.party_id_for_player(leader_id) else {
            self.queue_player_message(player_id, 0x14, "Party not found.".to_string());
            return Ok(());
        };
        let player_name = self
            .player_name_by_id(player_id)
            .unwrap_or_else(|| format!("Player {}", player_id.0));
        let leader_name = self
            .player_name_by_id(leader_id)
            .unwrap_or_else(|| format!("Player {}", leader_id.0));
        let mut error_message: Option<String> = None;
        let mut members = Vec::new();
        {
            let Some(party) = self.parties.get_mut(&party_id) else {
                return Err("party state missing".to_string());
            };
            if party.leader != leader_id {
                error_message = Some("Party leader not found.".to_string());
            } else if !party.invited.remove(&player_id) {
                error_message = Some("You are not invited.".to_string());
            } else {
                party.members.push(player_id);
                members = Self::party_members(party);
            }
        }
        if let Some(message) = error_message {
            self.queue_player_message(player_id, 0x14, message);
            return Ok(());
        }
        if let Some(player) = self.players.get_mut(&player_id) {
            player.party_id = Some(party_id);
        }
        self.queue_player_message(
            player_id,
            0x14,
            format!("You have joined {leader_name}'s party."),
        );
        for member_id in &members {
            if *member_id == player_id {
                continue;
            }
            self.queue_player_message(
                *member_id,
                0x14,
                format!("{player_name} has joined the party."),
            );
        }
        let mut update_players = members;
        update_players.push(player_id);
        self.party_shared_exp_members(party_id);
        self.queue_party_marks_for_players(&update_players);
        Ok(())
    }

    pub(crate) fn party_revoke(
        &mut self,
        leader_id: PlayerId,
        target_id: PlayerId,
    ) -> Result<(), String> {
        let Some(party_id) = self.party_id_for_player(leader_id) else {
            self.queue_player_message(leader_id, 0x14, "You are not in a party.".to_string());
            return Ok(());
        };
        let Some(party) = self.parties.get_mut(&party_id) else {
            return Err("party state missing".to_string());
        };
        if party.leader != leader_id {
            self.queue_player_message(
                leader_id,
                0x14,
                "Only the party leader can revoke invitations.".to_string(),
            );
            return Ok(());
        }
        if !party.invited.remove(&target_id) {
            self.queue_player_message(
                leader_id,
                0x14,
                "Invitation not found.".to_string(),
            );
            return Ok(());
        }
        let target_name = self
            .player_name_by_id(target_id)
            .unwrap_or_else(|| format!("Player {}", target_id.0));
        self.queue_player_message(
            leader_id,
            0x14,
            format!("Invitation revoked for {target_name}."),
        );
        self.queue_player_message(
            target_id,
            0x14,
            "Your party invitation was revoked.".to_string(),
        );
        self.queue_party_marks_for_players(&[leader_id, target_id]);
        Ok(())
    }

    pub(crate) fn party_pass_leadership(
        &mut self,
        leader_id: PlayerId,
        target_id: PlayerId,
    ) -> Result<(), String> {
        let Some(party_id) = self.party_id_for_player(leader_id) else {
            self.queue_player_message(leader_id, 0x14, "You are not in a party.".to_string());
            return Ok(());
        };
        let mut error_message: Option<String> = None;
        let mut members = Vec::new();
        let mut cleared_invites = Vec::new();
        {
            let Some(party) = self.parties.get_mut(&party_id) else {
                return Err("party state missing".to_string());
            };
            if party.leader != leader_id {
                error_message = Some("Only the party leader can pass leadership.".to_string());
            } else if let Some(index) = party.members.iter().position(|id| *id == target_id) {
                party.members.remove(index);
                party.members.retain(|id| *id != leader_id);
                party.members.insert(0, leader_id);
                party.leader = target_id;
                cleared_invites.extend(party.invited.drain());
                members = Self::party_members(party);
            } else {
                error_message = Some("Target is not in your party.".to_string());
            }
        }
        if let Some(message) = error_message {
            self.queue_player_message(leader_id, 0x14, message);
            return Ok(());
        }
        let new_leader_name = self
            .player_name_by_id(target_id)
            .unwrap_or_else(|| format!("Player {}", target_id.0));
        for member_id in &members {
            if *member_id == target_id {
                continue;
            }
            self.queue_player_message(
                *member_id,
                0x14,
                format!("{new_leader_name} is now the party leader."),
            );
        }
        self.queue_player_message(target_id, 0x14, "You are now the party leader.".to_string());
        let mut update_players = members.clone();
        update_players.extend(cleared_invites);
        self.queue_party_marks_for_players(&update_players);
        self.party_shared_exp_members(party_id);
        Ok(())
    }

    pub(crate) fn party_leave(
        &mut self,
        player_id: PlayerId,
        notify_self: bool,
    ) -> Result<(), String> {
        let Some(party_id) = self.party_id_for_player(player_id) else {
            if notify_self {
                self.queue_player_message(
                    player_id,
                    0x14,
                    "You are not in a party.".to_string(),
                );
            }
            return Ok(());
        };
        let player_name = self
            .player_name_by_id(player_id)
            .unwrap_or_else(|| format!("Player {}", player_id.0));
        let (leader_id, member_count, invites_empty, first_member) = {
            let Some(party) = self.parties.get(&party_id) else {
                return Err("party state missing".to_string());
            };
            (
                party.leader,
                party.members.len(),
                party.invited.is_empty(),
                party.members.first().copied(),
            )
        };
        let total_members = member_count + 1;
        let mut should_disband = false;
        let mut pass_leadership = None;
        if leader_id == player_id {
            if total_members == 1 || (total_members == 2 && invites_empty) {
                should_disband = true;
            } else {
                pass_leadership = first_member;
                if pass_leadership.is_none() {
                    should_disband = true;
                }
            }
        } else if total_members == 2 && invites_empty {
            should_disband = true;
        }
        if should_disband {
            self.disband_party(party_id);
            return Ok(());
        }
        if let Some(new_leader) = pass_leadership {
            self.party_pass_leadership(player_id, new_leader)?;
        }
        if let Some(party) = self.parties.get_mut(&party_id) {
            if let Some(index) = party.members.iter().position(|id| *id == player_id) {
                party.members.remove(index);
            }
        }
        if let Some(player) = self.players.get_mut(&player_id) {
            player.party_id = None;
        }
        if let Some(party) = self.parties.get(&party_id) {
            let members = Self::party_members(party);
            let mut update_players = Vec::with_capacity(members.len() + 1);
            update_players.push(player_id);
            update_players.extend(members.iter().copied());
            for member_id in members {
                if member_id == player_id {
                    continue;
                }
                self.queue_player_message(
                    member_id,
                    0x14,
                    format!("{player_name} has left the party."),
                );
            }
            if notify_self {
                self.queue_player_message(
                    player_id,
                    0x14,
                    "You have left the party.".to_string(),
                );
            }
            self.party_shared_exp_members(party_id);
            self.queue_party_marks_for_players(&update_players);
        } else if notify_self {
            self.queue_player_message(player_id, 0x14, "You have left the party.".to_string());
        }
        Ok(())
    }

    pub(crate) fn party_set_shared_exp(
        &mut self,
        leader_id: PlayerId,
        enabled: bool,
    ) -> Result<(), String> {
        let Some(party_id) = self.party_id_for_player(leader_id) else {
            self.queue_player_message(leader_id, 0x14, "You are not in a party.".to_string());
            return Ok(());
        };
        {
            let Some(party) = self.parties.get_mut(&party_id) else {
                return Err("party state missing".to_string());
            };
            if party.leader != leader_id {
                self.queue_player_message(
                    leader_id,
                    0x14,
                    "Only the party leader can change shared experience.".to_string(),
                );
                return Ok(());
            }
            party.shared_exp_active = enabled;
        }
        self.party_shared_exp_members(party_id);
        Ok(())
    }

    pub(crate) fn trade_request(
        &mut self,
        player_id: PlayerId,
        partner_id: PlayerId,
        position: Position,
        item_type: ItemTypeId,
        stack_pos: u8,
    ) -> Result<(), String> {
        if player_id == partner_id {
            return Err("You cannot trade with yourself.".to_string());
        }
        let Some(player) = self.players.get(&player_id) else {
            return Err(format!("unknown player {:?}", player_id));
        };
        let Some(partner) = self.players.get(&partner_id) else {
            return Err("Player not found.".to_string());
        };
        let requester_name = player.name.clone();
        let partner_name = partner.name.clone();
        if self.trade_by_player.contains_key(&player_id) {
            return Err("You are already trading.".to_string());
        }
        if self.trade_by_player.contains_key(&partner_id) {
            return Err("Player is already trading.".to_string());
        }
        let trade_item =
            self.trade_item_from_location(player_id, position, stack_pos, item_type)
                .ok_or_else(|| "Trade item not found.".to_string())?;
        let offered_item = trade_item.item.clone();
        let trade_id = if self.next_trade_id == 0 {
            self.next_trade_id = 1;
            1
        } else {
            self.next_trade_id
        };
        self.next_trade_id = trade_id.saturating_add(1).max(1);
        let session = TradeSession {
            requester: player_id,
            partner: partner_id,
            offer_requester: vec![trade_item.clone()],
            offer_partner: Vec::new(),
            requester_accepted: false,
            partner_accepted: false,
        };
        self.trade_sessions.insert(trade_id, session);
        self.trade_by_player.insert(player_id, trade_id);
        self.trade_by_player.insert(partner_id, trade_id);
        self.queue_trade_offer(player_id, false, requester_name.clone(), vec![offered_item.clone()]);
        self.queue_trade_offer(player_id, true, partner_name.clone(), Vec::new());
        self.queue_trade_offer(partner_id, false, requester_name, vec![offered_item]);
        self.queue_trade_offer(partner_id, true, partner_name, Vec::new());
        Ok(())
    }

    pub(crate) fn trade_accept(&mut self, player_id: PlayerId) -> Result<(), String> {
        let trade_id = match self.trade_by_player.get(&player_id) {
            Some(id) => *id,
            None => return Err("Trade is not open.".to_string()),
        };
        let (requester, partner, should_close) = {
            let Some(session) = self.trade_sessions.get_mut(&trade_id) else {
                return Err("Trade session missing.".to_string());
            };
            if player_id == session.requester {
                session.requester_accepted = true;
            } else if player_id == session.partner {
                session.partner_accepted = true;
            }
            (
                session.requester,
                session.partner,
                session.requester_accepted && session.partner_accepted,
            )
        };
        if should_close {
            if let Err(err) = self.complete_trade_session(trade_id) {
                self.queue_player_message(requester, 0x14, err.clone());
                self.queue_player_message(partner, 0x14, err);
                self.close_trade_session(trade_id);
                return Ok(());
            }
            self.queue_player_message(requester, 0x14, "Trade completed.".to_string());
            self.queue_player_message(partner, 0x14, "Trade completed.".to_string());
            self.close_trade_session(trade_id);
        }
        Ok(())
    }

    pub(crate) fn trade_close(&mut self, player_id: PlayerId) -> Result<(), String> {
        let trade_id = match self.trade_by_player.get(&player_id) {
            Some(id) => *id,
            None => return Ok(()),
        };
        self.close_trade_session(trade_id);
        Ok(())
    }

    pub(crate) fn open_shop_for_player(
        &mut self,
        player_id: PlayerId,
        npc_id: CreatureId,
    ) -> Result<ShopOpenResult, String> {
        let npc = self
            .npcs
            .get(&npc_id)
            .ok_or_else(|| "NPC not found.".to_string())?;
        let Some(index) = self.npc_index.as_ref() else {
            return Err("NPC scripts missing.".to_string());
        };
        let script = index
            .scripts
            .get(&npc.script_key)
            .ok_or_else(|| "NPC script missing.".to_string())?;
        if script.trade_entries.is_empty() {
            return Err("Shop list not available.".to_string());
        }

        let player_snapshot = self
            .players
            .get(&player_id)
            .cloned()
            .ok_or_else(|| "Player not found.".to_string())?;
        let mut rng = MoveUseRng::default();
        let empty_tokens = Vec::new();
        let mut ctx = NpcEvalContext {
            player: &player_snapshot,
            npc_id,
            tokens: &empty_tokens,
            message: "",
            rng: &mut rng,
            object_types: self.object_types.as_ref(),
            spellbook: &self.spellbook,
            clock: None,
            focus_owner: Some(player_id),
            is_busy: false,
            is_queued: false,
            required_ident: None,
        };
        let session_items = self.build_shop_session_items(script, &mut ctx);
        if session_items.is_empty() {
            return Err("Shop list not available.".to_string());
        }

        let items = session_items
            .iter()
            .map(|item| self.shop_item_view(item))
            .collect::<Vec<_>>();
        let session = ShopSession {
            items: session_items,
        };
        let sell_list = self.shop_sell_list(player_id, &session);
        self.shop_sessions.insert(player_id, session);
        Ok(ShopOpenResult { items, sell_list })
    }

    pub(crate) fn shop_look(
        &self,
        player_id: PlayerId,
        item_type: ItemTypeId,
        count: u8,
    ) -> Result<String, String> {
        let session = self
            .shop_sessions
            .get(&player_id)
            .ok_or_else(|| "Shop window is not open.".to_string())?;
        let item = Self::shop_session_item(session, item_type, count)
            .ok_or_else(|| "Item not available.".to_string())?;
        let description = self
            .item_types
            .as_ref()
            .and_then(|types| types.get(item.type_id))
            .map(|item| item.name.clone())
            .unwrap_or_else(|| format!("an object ({})", item.type_id.0));
        Ok(format!("You see {}.", description))
    }

    pub(crate) fn shop_buy(
        &mut self,
        player_id: PlayerId,
        item_type: ItemTypeId,
        count: u8,
        amount: u8,
        ignore_capacity: bool,
        buy_with_backpack: bool,
    ) -> Result<ShopSellList, String> {
        if amount == 0 {
            return Err("Amount must be at least 1.".to_string());
        }
        let item = {
            let session = self
                .shop_sessions
                .get(&player_id)
                .ok_or_else(|| "Shop window is not open.".to_string())?;
            Self::shop_session_item(session, item_type, count)
                .cloned()
                .ok_or_else(|| "Item not available.".to_string())?
        };
        if item.buy_price == 0 {
            return Err("Item is not for sale.".to_string());
        }
        let stackable = self.stackable_for(item.type_id);
        let backpack_capacity = self
            .item_types
            .as_ref()
            .and_then(|types| types.get(SHOP_BACKPACK_TYPE_ID))
            .and_then(|entry| entry.container_capacity)
            .map(u32::from)
            .unwrap_or(SHOP_BACKPACK_CAPACITY);
        let backpack_weight = self
            .object_types
            .as_ref()
            .and_then(|types| types.get(SHOP_BACKPACK_TYPE_ID))
            .and_then(|item| item.attribute_u16("Weight"))
            .map(u32::from)
            .unwrap_or(SHOP_BACKPACK_WEIGHT);
        let items_per_backpack = if stackable {
            backpack_capacity.saturating_mul(SHOP_BACKPACK_STACK_CAPACITY)
        } else {
            backpack_capacity
        }
        .max(1);
        let backpack_count = if buy_with_backpack {
            u32::from(amount)
                .saturating_add(items_per_backpack - 1)
                / items_per_backpack
        } else {
            0
        };
        let total = item
            .buy_price
            .checked_mul(u32::from(amount))
            .and_then(|total| total.checked_add(backpack_count.saturating_mul(SHOP_BACKPACK_PRICE)))
            .ok_or_else(|| "Total price too high.".to_string())?;
        if !ignore_capacity {
            let Some(object_types) = self.object_types.as_ref() else {
                return Err("Item weight data missing.".to_string());
            };
            let Some(player) = self.players.get(&player_id) else {
                return Err(format!("unknown player {:?}", player_id));
            };
            let current_weight = player_total_weight(player, object_types);
            let added_weight = item_type_weight(
                object_types,
                item.type_id,
                u32::from(amount),
            )
            .saturating_add(backpack_count.saturating_mul(backpack_weight));
            let capacity = player.stats.capacity.saturating_mul(SHOP_CAPACITY_SCALE);
            if current_weight.saturating_add(added_weight) > capacity {
                return Err("You do not have enough capacity.".to_string());
            }
        }
        if let Err(err) = self.remove_money_from_player(player_id, total) {
            if err.contains("insufficient") {
                return Err("You don't have enough money.".to_string());
            }
            return Err("Payment failed.".to_string());
        }
        let has_count = self
            .item_types
            .as_ref()
            .and_then(|types| types.get(item.type_id))
            .map(|entry| entry.has_count)
            .unwrap_or(false);
        let sub_type = if count > 0 { count } else { item.sub_type };
        if stackable {
            self.add_item_to_player(player_id, item.type_id, u16::from(amount))?;
        } else if has_count {
            for _ in 0..amount {
                self.add_item_to_player(player_id, item.type_id, u16::from(sub_type))?;
            }
        } else {
            for _ in 0..amount {
                self.add_item_to_player(player_id, item.type_id, 1)?;
            }
        }
        if buy_with_backpack && backpack_count > 0 {
            for _ in 0..backpack_count {
                self.add_item_to_player(player_id, SHOP_BACKPACK_TYPE_ID, 1)?;
            }
        }
        let session = self
            .shop_sessions
            .get(&player_id)
            .ok_or_else(|| "Shop session missing.".to_string())?;
        Ok(self.shop_sell_list(player_id, session))
    }

    pub(crate) fn shop_sell(
        &mut self,
        player_id: PlayerId,
        item_type: ItemTypeId,
        count: u8,
        amount: u8,
    ) -> Result<ShopSellList, String> {
        if amount == 0 {
            return Err("Amount must be at least 1.".to_string());
        }
        let item = {
            let session = self
                .shop_sessions
                .get(&player_id)
                .ok_or_else(|| "Shop window is not open.".to_string())?;
            Self::shop_session_item(session, item_type, count)
                .cloned()
                .ok_or_else(|| "Item not available.".to_string())?
        };
        if item.sell_price == 0 {
            return Err("Item is not bought by this shop.".to_string());
        }
        let total = item
            .sell_price
            .checked_mul(u32::from(amount))
            .ok_or_else(|| "Total price too high.".to_string())?;
        if self
            .remove_item_from_player(player_id, item.type_id, u16::from(amount))
            .is_err()
        {
            return Err("You do not have enough items.".to_string());
        }
        self.add_money_to_player(player_id, total)?;
        let session = self
            .shop_sessions
            .get(&player_id)
            .ok_or_else(|| "Shop session missing.".to_string())?;
        Ok(self.shop_sell_list(player_id, session))
    }

    pub(crate) fn shop_close(&mut self, player_id: PlayerId) -> bool {
        self.shop_sessions.remove(&player_id).is_some()
    }

    fn shop_session_item<'a>(
        session: &'a ShopSession,
        item_type: ItemTypeId,
        count: u8,
    ) -> Option<&'a ShopSessionItem> {
        session
            .items
            .iter()
            .find(|item| item.type_id == item_type && item.sub_type == count)
            .or_else(|| session.items.iter().find(|item| item.type_id == item_type))
    }

    fn shop_item_view(&self, item: &ShopSessionItem) -> ShopItemView {
        let description = self
            .item_types
            .as_ref()
            .and_then(|types| types.get(item.type_id))
            .map(|item| item.name.clone())
            .unwrap_or_else(|| format!("an object ({})", item.type_id.0));
        let weight = self
            .object_types
            .as_ref()
            .and_then(|types| types.get(item.type_id))
            .and_then(|item| item.attribute_u16("Weight"))
            .map(u32::from)
            .unwrap_or(0);
        ShopItemView {
            type_id: item.type_id,
            sub_type: item.sub_type,
            description,
            weight,
            buy_price: item.buy_price,
            sell_price: item.sell_price,
        }
    }

    fn shop_sell_list(&self, player_id: PlayerId, session: &ShopSession) -> ShopSellList {
        let money = self
            .players
            .get(&player_id)
            .map(|player| npc_count_money(player, self.object_types.as_ref()))
            .unwrap_or(0);
        let player = self.players.get(&player_id);
        let mut counts: HashMap<ItemTypeId, u8> = HashMap::new();
        if let Some(player) = player {
            for item in &session.items {
                if item.sell_price == 0 || counts.contains_key(&item.type_id) {
                    continue;
                }
                let count = npc_count_item(player, item.type_id)
                    .min(u32::from(u8::MAX)) as u8;
                counts.insert(item.type_id, count);
            }
        }
        let mut entries = counts
            .into_iter()
            .map(|(type_id, count)| ShopSellEntry { type_id, count })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.type_id.0);
        ShopSellList { money, entries }
    }

    fn build_shop_session_items(
        &self,
        script: &NpcScript,
        ctx: &mut NpcEvalContext<'_>,
    ) -> Vec<ShopSessionItem> {
        let mut items: HashMap<(ItemTypeId, u8), ShopSessionItem> = HashMap::new();
        for entry in &script.trade_entries {
            let Some(type_id) = entry
                .type_id
                .and_then(|value| u16::try_from(value).ok())
                .map(ItemTypeId)
            else {
                continue;
            };
            let mut vars = npc_base_vars(ctx);
            vars.entry("%1".to_string()).or_insert(1);
            if !npc_trade_entry_allowed(entry, ctx, &vars) {
                continue;
            }
            let price = entry
                .price
                .as_ref()
                .and_then(|value| npc_eval_expr(value, ctx, &vars))
                .and_then(|value| u32::try_from(value).ok());
            let Some(price) = price.filter(|value| *value > 0) else {
                continue;
            };
            let amount = entry
                .amount
                .as_ref()
                .and_then(|value| npc_eval_expr(value, ctx, &vars))
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or(1);
            let sub_type = u8::try_from(amount.max(1).min(i64::from(u8::MAX)))
                .unwrap_or(1);
            let is_sell = entry
                .conditions
                .iter()
                .any(|condition| condition.eq_ignore_ascii_case("sell"));
            let item = items
                .entry((type_id, sub_type))
                .or_insert(ShopSessionItem {
                    type_id,
                    sub_type,
                    buy_price: 0,
                    sell_price: 0,
                });
            if is_sell {
                item.sell_price = item.sell_price.max(price);
            } else {
                item.buy_price = item.buy_price.max(price);
            }
        }
        let mut list = items.into_values().collect::<Vec<_>>();
        list.sort_by_key(|item| (item.type_id.0, item.sub_type));
        list
    }

    pub(crate) fn trade_item_for_look(
        &self,
        player_id: PlayerId,
        counter_offer: bool,
        index: u8,
    ) -> Option<ItemStack> {
        let trade_id = *self.trade_by_player.get(&player_id)?;
        let session = self.trade_sessions.get(&trade_id)?;
        let items = if counter_offer {
            &session.offer_partner
        } else {
            &session.offer_requester
        };
        items.get(index as usize).map(|entry| entry.item.clone())
    }

    fn complete_trade_session(&mut self, trade_id: u32) -> Result<(), String> {
        let session = self
            .trade_sessions
            .get(&trade_id)
            .cloned()
            .ok_or_else(|| "Trade session missing.".to_string())?;
        let mut requester_items = Vec::new();
        for entry in &session.offer_requester {
            let removed = self.take_trade_item_from_location(session.requester, entry)?;
            requester_items.push(removed);
        }
        let mut partner_items = Vec::new();
        for entry in &session.offer_partner {
            let removed = self.take_trade_item_from_location(session.partner, entry)?;
            partner_items.push(removed);
        }
        for item in requester_items {
            self.add_item_stack_to_player(session.partner, item)?;
        }
        for item in partner_items {
            self.add_item_stack_to_player(session.requester, item)?;
        }
        Ok(())
    }

    fn take_trade_item_from_location(
        &mut self,
        player_id: PlayerId,
        entry: &TradeItem,
    ) -> Result<ItemStack, String> {
        let position = entry.position;
        let expected = &entry.item;
        let stackable = self.stackable_for(expected.type_id);
        if position.x == 0xffff {
            if let Some(slot) = InventorySlot::from_index(position.y as usize) {
                let player = self
                    .players
                    .get_mut(&player_id)
                    .ok_or_else(|| format!("unknown player {:?}", player_id))?;
                let existing = player
                    .inventory
                    .slot(slot)
                    .ok_or_else(|| "trade item not found".to_string())?;
                if existing.type_id != expected.type_id {
                    return Err("trade item type mismatch".to_string());
                }
                if existing.count < expected.count {
                    return Err("trade item count insufficient".to_string());
                }
                return player.inventory.remove_item(slot, expected.count);
            }
            if position.y >= 0x40 {
                let container_id = (position.y - 0x40) as u8;
                let slot = position.z;
                let removed = {
                    let player = self
                        .players
                        .get_mut(&player_id)
                        .ok_or_else(|| format!("unknown player {:?}", player_id))?;
                    let container = player
                        .open_containers
                        .get_mut(&container_id)
                        .ok_or_else(|| "container not open".to_string())?;
                    let index = slot as usize;
                    if index >= container.items.len() {
                        return Err("container slot out of range".to_string());
                    }
                    let item = &container.items[index];
                    if item.type_id != expected.type_id {
                        return Err("trade item type mismatch".to_string());
                    }
                    if item.count < expected.count {
                        return Err("trade item count insufficient".to_string());
                    }
                    let (removed, _) = take_from_container(
                        container,
                        container_id,
                        slot,
                        expected.count,
                        stackable,
                        expected.type_id,
                    )?;
                    removed
                };
                self.sync_container_contents(player_id, container_id);
                return Ok(removed);
            }
            return Err("trade item location invalid".to_string());
        }
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "trade item tile missing".to_string())?;
        let index = entry.stack_pos as usize;
        let item = tile
            .items
            .get(index)
            .ok_or_else(|| "trade item not found".to_string())?;
        if item.type_id != expected.type_id {
            return Err("trade item type mismatch".to_string());
        }
        if item.count < expected.count {
            return Err("trade item count insufficient".to_string());
        }
        take_from_tile_at(tile, index, expected.count, stackable)
    }

    fn close_trade_session(&mut self, trade_id: u32) {
        let Some(session) = self.trade_sessions.remove(&trade_id) else {
            return;
        };
        self.trade_by_player.remove(&session.requester);
        self.trade_by_player.remove(&session.partner);
        self.queue_trade_close(session.requester);
        self.queue_trade_close(session.partner);
    }

    fn trade_item_from_location(
        &self,
        player_id: PlayerId,
        position: Position,
        stack_pos: u8,
        item_type: ItemTypeId,
    ) -> Option<TradeItem> {
        let player = self.players.get(&player_id)?;
        if position.x == 0xffff {
            if let Some(slot) = InventorySlot::from_index(position.y as usize) {
                let item = player.inventory.slot(slot)?;
                if item.type_id != item_type {
                    return None;
                }
                return Some(TradeItem {
                    item: item.clone(),
                    position,
                    stack_pos,
                });
            }
            if position.y >= 0x40 {
                let container_id = (position.y - 0x40) as u8;
                let slot = position.z as usize;
                let container = player.open_containers.get(&container_id)?;
                let item = container.items.get(slot)?;
                if item.type_id != item_type {
                    return None;
                }
                return Some(TradeItem {
                    item: item.clone(),
                    position,
                    stack_pos,
                });
            }
        }
        let tile = self.map.tile(position)?;
        let index = stack_pos as usize;
        let item = tile.items.get(index)?;
        if item.type_id != item_type {
            return None;
        }
        Some(TradeItem {
            item: item.clone(),
            position,
            stack_pos,
        })
    }

    fn guild_channel_name(&self, player_id: PlayerId) -> Option<String> {
        let player = self.players.get(&player_id)?;
        if let Some(name) = player.guild_name.as_ref() {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
        if player.guild_id.unwrap_or(0) > 0 {
            return Some("Guild".to_string());
        }
        None
    }

    pub(crate) fn channel_list_for(&self, player_id: PlayerId) -> Vec<(u16, String)> {
        let mut channels: Vec<(u16, String)> = Vec::new();
        if let Some(name) = self.guild_channel_name(player_id) {
            channels.push((GUILD_CHANNEL_ID, name));
        }
        channels.extend(
            DEFAULT_CHANNELS
                .iter()
                .map(|(id, name)| (*id, (*name).to_string())),
        );
        for channel in self.private_channels.values() {
            if channel.owner == player_id || channel.invited.contains(&player_id) {
                channels.push((channel.id, channel.name.clone()));
            }
        }
        channels.sort_by(|a, b| a.0.cmp(&b.0));
        channels
    }

    pub(crate) fn channel_name_for(
        &self,
        player_id: PlayerId,
        channel_id: u16,
    ) -> Option<String> {
        if channel_id == GUILD_CHANNEL_ID {
            return self.guild_channel_name(player_id);
        }
        for (id, name) in DEFAULT_CHANNELS {
            if *id == channel_id {
                return Some((*name).to_string());
            }
        }
        let channel = self.private_channels.get(&channel_id)?;
        if channel.owner == player_id || channel.invited.contains(&player_id) {
            return Some(channel.name.clone());
        }
        None
    }

    pub(crate) fn private_channel_owner(&self, channel_id: u16) -> Option<PlayerId> {
        self.private_channels.get(&channel_id).map(|channel| channel.owner)
    }

    pub(crate) fn ensure_private_channel(
        &mut self,
        owner: PlayerId,
    ) -> Result<(u16, String), String> {
        if let Some(channel_id) = self.private_channel_owners.get(&owner).copied() {
            let channel = self
                .private_channels
                .get(&channel_id)
                .ok_or_else(|| "private channel missing".to_string())?;
            return Ok((channel.id, channel.name.clone()));
        }
        let channel_id = self
            .allocate_private_channel_id()
            .ok_or_else(|| "private channel ids exhausted".to_string())?;
        let name = self.private_channel_name(owner);
        let channel = PrivateChannel {
            id: channel_id,
            owner,
            name: name.clone(),
            invited: HashSet::new(),
        };
        self.private_channels.insert(channel_id, channel);
        self.private_channel_owners.insert(owner, channel_id);
        Ok((channel_id, name))
    }

    pub(crate) fn invite_to_private_channel(
        &mut self,
        owner: PlayerId,
        name: &str,
    ) -> Result<ChannelInviteResult, String> {
        let Some(channel_id) = self.private_channel_owners.get(&owner).copied() else {
            return Ok(ChannelInviteResult::NoChannel);
        };
        let Some(invitee_id) = self.find_player_id_by_name(name)? else {
            return Ok(ChannelInviteResult::NotFound);
        };
        if invitee_id == owner {
            return Ok(ChannelInviteResult::SelfInvite);
        }
        let invitee_name = self
            .player_name_by_id(invitee_id)
            .unwrap_or_else(|| name.to_string());
        let channel = self
            .private_channels
            .get_mut(&channel_id)
            .ok_or_else(|| "private channel missing".to_string())?;
        if !channel.invited.insert(invitee_id) {
            return Ok(ChannelInviteResult::AlreadyInvited { invitee_name });
        }
        Ok(ChannelInviteResult::Invited {
            channel_id,
            channel_name: channel.name.clone(),
            invitee_id,
            invitee_name,
        })
    }

    pub(crate) fn exclude_from_private_channel(
        &mut self,
        owner: PlayerId,
        name: &str,
    ) -> Result<ChannelExcludeResult, String> {
        let Some(channel_id) = self.private_channel_owners.get(&owner).copied() else {
            return Ok(ChannelExcludeResult::NoChannel);
        };
        let Some(invitee_id) = self.find_player_id_by_name(name)? else {
            return Ok(ChannelExcludeResult::NotFound);
        };
        if invitee_id == owner {
            return Ok(ChannelExcludeResult::SelfExclude);
        }
        let invitee_name = self
            .player_name_by_id(invitee_id)
            .unwrap_or_else(|| name.to_string());
        let channel = self
            .private_channels
            .get_mut(&channel_id)
            .ok_or_else(|| "private channel missing".to_string())?;
        if !channel.invited.remove(&invitee_id) {
            return Ok(ChannelExcludeResult::NotInvited { invitee_name });
        }
        Ok(ChannelExcludeResult::Excluded {
            channel_id,
            invitee_id,
            invitee_name,
        })
    }

    pub(crate) fn submit_request(&mut self, player_id: PlayerId) -> Result<bool, String> {
        if self.request_queue_players.contains(&player_id) {
            return Ok(false);
        }
        let player = self
            .players
            .get(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        let entry = RequestQueueEntry {
            player_id,
            name: player.name.clone(),
        };
        self.request_queue.push(entry);
        self.request_queue_players.insert(player_id);
        Ok(true)
    }

    pub(crate) fn take_request_by_name(&mut self, name: &str) -> Option<RequestQueueEntry> {
        let index = self
            .request_queue
            .iter()
            .position(|entry| entry.name.eq_ignore_ascii_case(name))?;
        let entry = self.request_queue.remove(index);
        self.request_queue_players.remove(&entry.player_id);
        Some(entry)
    }

    pub(crate) fn take_request_for_player(
        &mut self,
        player_id: PlayerId,
    ) -> Option<RequestQueueEntry> {
        let index = self
            .request_queue
            .iter()
            .position(|entry| entry.player_id == player_id)?;
        let entry = self.request_queue.remove(index);
        self.request_queue_players.remove(&entry.player_id);
        Some(entry)
    }

    pub fn set_rng_seeds(&mut self, moveuse_seed: u64, loot_seed: u64, monster_seed: u64) {
        self.moveuse_rng = MoveUseRng::from_seed(moveuse_seed);
        self.loot_rng = LootRng::from_seed(loot_seed);
        self.monster_rng = MonsterRng::from_seed(monster_seed);
        self.npc_rng = NpcRng::from_seed(monster_seed.wrapping_add(0x9e37_79b9_7f4a_7c15));
    }

    fn validate_rune_spell_items(&self, object_types: &ObjectTypeIndex) {
        for spell in self.spellbook.iter() {
            if spell.kind != SpellKind::Rune {
                continue;
            }
            let Some(rune_type) = spell.rune_type_id else {
                continue;
            };
            let Some(object_type) = object_types.get(rune_type) else {
                logging::log_game(&format!(
                    "spell validation: rune spell {:?} ({}) has missing item {:?}",
                    spell.id, spell.name, rune_type
                ));
                continue;
            };
            if !object_type.has_flag("Rune") {
                logging::log_game(&format!(
                    "spell validation: rune spell {:?} ({}) item {:?} missing Rune flag",
                    spell.id, spell.name, rune_type
                ));
            }
        }
    }

    pub fn request_logout(
        &mut self,
        user_id: PlayerId,
        clock: Option<&GameClock>,
    ) -> Result<(), LogoutBlockReason> {
        let Some(player) = self.players.get_mut(&user_id) else {
            return Ok(());
        };
        if let Some(tile) = self.map.tile(player.position) {
            if tile.protection_zone {
                return Err(LogoutBlockReason::ProtectionZone);
            }
            if tile.no_logout {
                return Err(LogoutBlockReason::NoLogoutZone);
            }
        }
        let in_fight = match clock {
            Some(clock) => player.in_combat(clock),
            None => player.pvp.fight_expires_at.is_some(),
        };
        if in_fight {
            return Err(LogoutBlockReason::InFight);
        }
        Ok(())
    }

    pub fn handle_disconnect(&mut self, player_id: PlayerId) {
        if !self.players.contains_key(&player_id) {
            return;
        }
        self.queue_buddy_status_update(player_id, false);
        self.take_request_for_player(player_id);
        let _ = self.trade_close(player_id);
        let _ = self.party_leave(player_id, false);
        if self.request_logout(player_id, None).is_ok() {
            if let Some(mut player) = self.players.remove(&player_id) {
                player.last_logout = unix_time_now();
                self.offline_players.insert(player_id, player);
            }
        }
    }

    pub fn spawn_position(&self, veteran: bool) -> Option<Position> {
        let map_dat = self.map_dat.as_ref()?;
        if veteran {
            map_dat.veteran_start.or(map_dat.newbie_start)
        } else {
            map_dat.newbie_start.or(map_dat.veteran_start)
        }
    }

    fn spawn_npcs_from_index(&mut self) {
        let Some(index) = self.npc_index.as_ref() else {
            return;
        };
        let definitions = index.definitions.clone();
        for definition in definitions {
            let Some(position) = definition.home else {
                continue;
            };
            if !self.position_in_bounds(position) {
                continue;
            }
            if !self.map.tiles.is_empty() && !self.map.has_tile(position) {
                continue;
            }
            let position = match self.find_login_position(position, 1u8, false) {
                Some(position) => position,
                None => continue,
            };
            let id = self.next_npc_id();
            let npc = NpcInstance {
                id,
                script_key: definition.script_key,
                name: definition.name,
                position,
                direction: Direction::South,
                home: position,
                outfit: definition.outfit,
                radius: definition.radius.unwrap_or(1),
                focused: None,
                focus_expires_at: None,
                queue: VecDeque::new(),
                move_cooldown: Cooldown::new(GameTick(0)),
            };
            self.npcs.insert(id, npc);
        }
    }

    fn spawn_monsters_from_homes(&mut self) {
        if self.monster_homes.is_empty() {
            let msg = "tibia: monster homes empty; skipping spawn";
            eprintln!("{msg}");
            logging::log_game(msg);
            return;
        }
        if self.monster_index.is_none() {
            let msg = "tibia: monster homes loaded but monster index missing";
            eprintln!("{msg}");
            logging::log_game(msg);
            return;
        }
        let mut spawned = 0usize;
        let mut attempts = 0usize;
        let mut errors: HashMap<String, usize> = HashMap::new();
        let mut occupied: HashSet<Position> = HashSet::new();
        for player in self.players.values() {
            occupied.insert(player.position);
        }
        for npc in self.npcs.values() {
            occupied.insert(npc.position);
        }
        for monster in self.monsters.values() {
            occupied.insert(monster.position);
        }
        let sector_set = if self.map.sectors.is_empty() {
            None
        } else {
            Some(self.map.sectors.iter().map(|sector| sector.coord).collect::<HashSet<_>>())
        };
        let home_count = self.monster_homes.len();
        for home_index in 0..home_count {
            let (race_number, position, radius, amount) = {
                let home = &self.monster_homes[home_index];
                (home.race_number, home.position, home.radius, home.amount)
            };
            if amount == 0 {
                continue;
            }
            for spawn_index in 0..amount {
                attempts += 1;
                let mut spawn_radius = i32::from(radius);
                if spawn_index == 0 {
                    spawn_radius = spawn_radius.min(1);
                } else {
                    if spawn_radius > 10 {
                        spawn_radius = 10;
                    }
                    spawn_radius = -spawn_radius;
                }
                if let Some(position) = self.search_spawn_field(
                    position,
                    spawn_radius,
                    false,
                    Some(&occupied),
                    sector_set.as_ref(),
                )
                {
                    match self.spawn_monster_by_race_with_summoner(
                        race_number,
                        position,
                        None,
                        false,
                        Some(home_index),
                    ) {
                        Ok(_) => {
                            spawned += 1;
                            if let Some(home) = self.monster_homes.get_mut(home_index) {
                                home.act_monsters = home.act_monsters.saturating_add(1);
                            }
                            occupied.insert(position);
                        }
                        Err(err) => {
                            *errors.entry(err).or_insert(0) += 1;
                        }
                    }
                } else {
                    *errors
                        .entry("monster spawn blocked: no valid position".to_string())
                        .or_insert(0) += 1;
                }
            }
            let should_start = match self.monster_homes.get(home_index) {
                Some(home) => home.timer == 0 && home.act_monsters < home.amount,
                None => false,
            };
            if should_start {
                self.start_monster_home_timer(home_index);
            }
        }
        if spawned == 0 {
            let msg = format!(
                "tibia: monster spawn failed ({} attempts across {} homes)",
                attempts,
                self.monster_homes.len()
            );
            eprintln!("{msg}");
            logging::log_game(&msg);
        } else {
            let msg = format!(
                "tibia: spawned {} monsters from {} homes ({} attempts)",
                spawned,
                self.monster_homes.len(),
                attempts
            );
            println!("{msg}");
            logging::log_game(&msg);
        }
        if !errors.is_empty() {
            let mut error_list: Vec<(String, usize)> = errors.into_iter().collect();
            error_list.sort_by(|a, b| b.1.cmp(&a.1));
            for (message, count) in error_list.into_iter().take(5) {
                let msg = format!("tibia: monster spawn skipped {}x: {}", count, message);
                eprintln!("{msg}");
                logging::log_game(&msg);
            }
        }
    }

    fn start_monster_home_timer(&mut self, home_index: usize) {
        let (act, max, regen, timer) = match self.monster_homes.get(home_index) {
            Some(home) => (home.act_monsters, home.amount, home.regen, home.timer),
            None => return,
        };
        if timer > 0 || act >= max || regen == 0 {
            return;
        }
        let players = self.players.len() as u32;
        let mut max_timer = u32::from(regen);
        if players > 800 {
            max_timer = (max_timer.saturating_mul(2)) / 5;
        } else if players > 200 {
            let divisor = (players / 2).saturating_add(100);
            max_timer = (max_timer.saturating_mul(200)) / divisor.max(1);
        }
        if max_timer == 0 {
            return;
        }
        let min_timer = max_timer / 2;
        let roll = self.monster_rng.roll_range(min_timer, max_timer);
        if let Some(home) = self.monster_homes.get_mut(home_index) {
            home.timer = roll as i32;
        }
    }

    fn notify_monster_home_death(&mut self, home_index: usize) {
        let (act, max, timer) = match self.monster_homes.get(home_index) {
            Some(home) => (home.act_monsters, home.amount, home.timer),
            None => return,
        };
        if act == 0 {
            return;
        }
        if let Some(home) = self.monster_homes.get_mut(home_index) {
            home.act_monsters = home.act_monsters.saturating_sub(1);
        }
        if act - 1 < max && timer == 0 {
            self.start_monster_home_timer(home_index);
        }
    }

    fn player_can_see_floor(player_position: Position, floor_z: u8) -> bool {
        if player_position.z <= 7 {
            floor_z <= 7
        } else {
            (i32::from(player_position.z) - i32::from(floor_z)).abs() <= 2
        }
    }

    pub fn tick_monster_homes(&mut self, clock: &GameClock) -> usize {
        let now = clock.now();
        let ticks_per_second = clock
            .ticks_from_duration_round_up(Duration::from_secs(1))
            .max(1);
        if let Some(last) = self.monster_home_tick_last {
            if now.0 / ticks_per_second == last.0 / ticks_per_second {
                return 0;
            }
        }
        if self.monster_home_tick_last == Some(now) {
            return 0;
        }
        self.monster_home_tick_last = Some(now);
        let mut spawned = 0usize;
        let sector_set = if self.map.sectors.is_empty() {
            None
        } else {
            Some(self.map.sectors.iter().map(|sector| sector.coord).collect::<HashSet<_>>())
        };
        let home_count = self.monster_homes.len();
        for home_index in 0..home_count {
            let timer = match self.monster_homes.get(home_index) {
                Some(home) => home.timer,
                None => continue,
            };
            if timer <= 0 {
                continue;
            }
            if let Some(home) = self.monster_homes.get_mut(home_index) {
                home.timer -= 1;
            }
            let (race_number, position, radius, act_monsters, amount) = match self
                .monster_homes
                .get(home_index)
            {
                Some(home) => (
                    home.race_number,
                    home.position,
                    home.radius,
                    home.act_monsters,
                    home.amount,
                ),
                None => continue,
            };
            if let Some(home) = self.monster_homes.get(home_index) {
                if home.timer > 0 {
                    continue;
                }
            }
            let mut max_radius = i32::from(radius.min(10));
            for player in self.players.values() {
                if !Self::player_can_see_floor(player.position, position.z) {
                    continue;
                }
                let dx = (i32::from(player.position.x) - i32::from(position.x)).abs();
                let dy = (i32::from(player.position.y) - i32::from(position.y)).abs();
                if dx > max_radius + 9 || dy > max_radius + 7 {
                    continue;
                }
                let radius = (dx - 9).max(dy - 7);
                if radius < max_radius {
                    max_radius = radius;
                }
            }

            if max_radius >= 0 {
                let mut spawn_radius = max_radius;
                if act_monsters == 0 {
                    spawn_radius = spawn_radius.min(1);
                } else {
                    spawn_radius = -spawn_radius;
                }
                if let Some(spawn_pos) = self.search_spawn_field(
                    position,
                    spawn_radius,
                    false,
                    None,
                    sector_set.as_ref(),
                )
                {
                    if self
                        .spawn_monster_by_race_with_summoner(
                            race_number,
                            spawn_pos,
                            None,
                            false,
                            Some(home_index),
                        )
                        .is_ok()
                    {
                        spawned += 1;
                        if let Some(home) = self.monster_homes.get_mut(home_index) {
                            home.act_monsters = home.act_monsters.saturating_add(1);
                        }
                    }
                }
            }

            let should_start = match self.monster_homes.get(home_index) {
                Some(home) => home.act_monsters < amount && home.timer == 0,
                None => false,
            };
            if should_start {
                self.start_monster_home_timer(home_index);
            }
        }
        spawned
    }

    fn search_spawn_field(
        &mut self,
        origin: Position,
        distance: i32,
        player: bool,
        occupied: Option<&HashSet<Position>>,
        sector_set: Option<&HashSet<SectorCoord>>,
    ) -> Option<Position> {
        let mut distance = distance;
        let minimize = if distance < 0 {
            distance = -distance;
            false
        } else {
            true
        };
        let distance = distance.max(0) as i32;
        let size = (distance * 2 + 1) as usize;
        if size == 0 {
            return None;
        }
        let house_id = self.house_for_position(origin).map(|house| house.id);
        let mut phases = vec![i32::MAX; size * size];
        let center = distance as usize;
        phases[center * size + center] = 0;
        let mut best: Option<Position> = None;
        let mut best_tie = -1;
        let mut expansion_phase = 0;
        loop {
            let mut expanded = false;
            let mut found = false;
            for offset_y in -distance..=distance {
                for offset_x in -distance..=distance {
                    let idx = (offset_y + distance) as usize * size
                        + (offset_x + distance) as usize;
                    if phases[idx] != expansion_phase {
                        continue;
                    }
                    let position = match origin.offset(PositionDelta {
                        dx: offset_x as i16,
                        dy: offset_y as i16,
                        dz: 0,
                    }) {
                        Some(pos) => pos,
                        None => continue,
                    };
                    if !self.position_in_bounds_with_sectors(position, sector_set) {
                        continue;
                    }
                    let tile = match self.map.tile(position) {
                        Some(tile) => tile,
                        None => continue,
                    };
                    if tile.items.is_empty() {
                        continue;
                    }
                    if let Some(home_house) = house_id {
                        if let Some(house) = self.house_for_position(position) {
                            if house.id != home_house {
                                continue;
                            }
                        }
                    } else if self.house_for_position(position).is_some() {
                        continue;
                    }
                    if !player && tile.protection_zone {
                        continue;
                    }

                    let mut expansion_possible = true;
                    let mut login_possible = true;
                    let mut login_bad = false;
                    if let Some(object_types) = self.object_types.as_ref() {
                        for item in &tile.items {
                            let Some(obj) = object_types.get(item.type_id) else {
                                continue;
                            };
                            if obj.has_flag("Unpass") {
                                if obj.has_flag("Unmove") {
                                    expansion_possible = false;
                                    login_possible = false;
                                } else {
                                    login_bad = true;
                                }
                            }
                            if obj.has_flag("Avoid") {
                                if obj.has_flag("Unmove") {
                                    expansion_possible = false;
                                    if player {
                                        login_possible = false;
                                    }
                                } else {
                                    login_bad = true;
                                }
                            }
                        }
                    }
                    let is_occupied = match occupied {
                        Some(occupied) => occupied.contains(&position),
                        None => self.position_occupied(position),
                    };
                    if is_occupied {
                        login_possible = false;
                    }
                    if player && tile.no_logout {
                        login_possible = false;
                    }

                    if expansion_possible || expansion_phase == 0 {
                        for neighbor_y in (offset_y - 1)..=(offset_y + 1) {
                            for neighbor_x in (offset_x - 1)..=(offset_x + 1) {
                                if neighbor_x < -distance
                                    || neighbor_x > distance
                                    || neighbor_y < -distance
                                    || neighbor_y > distance
                                {
                                    continue;
                                }
                                let nidx = (neighbor_y + distance) as usize * size
                                    + (neighbor_x + distance) as usize;
                                let cost = expansion_phase
                                    + (neighbor_x - offset_x).abs()
                                    + (neighbor_y - offset_y).abs();
                                if phases[nidx] > cost {
                                    phases[nidx] = cost;
                                }
                            }
                        }
                        expanded = true;
                    }

                    if login_possible {
                        let mut tie = self.monster_rng.roll_range(0, 99) as i32;
                        if !login_bad {
                            tie += 100;
                        }
                        if tie > best_tie {
                            best_tie = tie;
                            best = Some(position);
                            found = true;
                        }
                    }
                }
            }
            if (found && minimize) || !expanded {
                break;
            }
            expansion_phase += 1;
        }
        best
    }

    pub fn npc_talk_responses(
        &mut self,
        player_id: PlayerId,
        message: &str,
        clock: Option<&GameClock>,
    ) -> NpcTalkOutcome {
        let player_snapshot = match self.players.get(&player_id) {
            Some(player) => player.clone(),
            None => {
                return NpcTalkOutcome {
                    responses: Vec::new(),
                    effects: Vec::new(),
                    containers_dirty: false,
                    shop: None,
                };
            }
        };
        let player_position = player_snapshot.position;
        let normalized = message.trim().to_ascii_lowercase();
        let tokens = npc_tokenize_message(&normalized);
        let wants_trade = tokens.iter().any(|token| {
            matches!(
                token.as_str(),
                "trade" | "offer" | "buy" | "sell" | "shop"
            )
        });

        let player = &player_snapshot;
        let focus_ticks = clock.map(|clock| {
            clock.ticks_from_duration_round_up(Duration::from_secs(NPC_FOCUS_TIMEOUT_SECS))
        });
        let npc_ids: Vec<CreatureId> = self.npcs.keys().copied().collect();
        let (replies, plans, shop_target) = {
            let Some(index) = self.npc_index.as_ref() else {
                return NpcTalkOutcome {
                    responses: Vec::new(),
                    effects: Vec::new(),
                    containers_dirty: false,
                    shop: None,
                };
            };
            let mut replies = Vec::new();
            let mut plans = Vec::new();
            let mut shop_target = None;
            for npc_id in npc_ids {
                let npc_snapshot = match self.npcs.get(&npc_id) {
                    Some(npc) => npc,
                    None => continue,
                };
                if !npc_in_range(npc_snapshot.position, player_position, NPC_TALK_RANGE) {
                    continue;
                }
                let Some(script) = index.scripts.get(&npc_snapshot.script_key) else {
                    continue;
                };
                let focused = npc_snapshot.focused;
                let is_focused = focused == Some(player_id);
                let mut is_busy = focused.is_some() && !is_focused;
                let is_queued = npc_snapshot.queue.iter().any(|id| *id == player_id);
                let mut required_ident = None;
                if is_busy {
                    required_ident = Some("busy");
                } else if focused.is_none() {
                    if let Some(front) = npc_snapshot.queue.front() {
                        if *front != player_id {
                            is_busy = true;
                            required_ident = Some("busy");
                        } else {
                            required_ident = Some("address");
                        }
                    } else {
                        required_ident = Some("address");
                    }
                }
                let mut ctx = NpcEvalContext {
                    player,
                    npc_id,
                    tokens: &tokens,
                    message: &normalized,
                    rng: &mut self.moveuse_rng,
                    object_types: self.object_types.as_ref(),
                    spellbook: &self.spellbook,
                    clock,
                    focus_owner: focused,
                    is_busy,
                    is_queued,
                    required_ident,
                };
                if let Some((reply, mut plan)) = npc_reply_for_message(script, &mut ctx) {
                    if let Some(text) = reply {
                        replies.push(NpcTalkResponse {
                            npc_id,
                            name: npc_snapshot.name.clone(),
                            position: npc_snapshot.position,
                            message: text,
                        });
                    }
                    let clears_focus = plan
                        .actions
                        .iter()
                        .any(|action| matches!(action, NpcPlannedAction::ClearFocus));
                    if !is_busy && !clears_focus {
                        let expires_at = focus_ticks.map(|ticks| {
                            GameTick(
                                clock
                                    .map(|clock| clock.now().0)
                                    .unwrap_or(0)
                                    .saturating_add(ticks),
                            )
                        });
                        plan.actions.push(NpcPlannedAction::FocusPlayer { expires_at });
                    }
                    if !plan.is_empty() {
                        plans.push(plan);
                    }
                }
                if wants_trade && shop_target.is_none() && focused == Some(player_id) && !is_busy {
                    shop_target = Some(npc_id);
                }
            }
            (replies, plans, shop_target)
        };

        let (effects, containers_dirty) = if plans.is_empty() {
            (Vec::new(), false)
        } else {
            let valid_spells = collect_valid_spells(&self.spellbook, &plans);
            apply_npc_plans(self, player_id, &plans, &valid_spells)
        };
        let shop = shop_target.and_then(|npc_id| self.open_shop_for_player(player_id, npc_id).ok());

        NpcTalkOutcome {
            responses: replies,
            effects,
            containers_dirty,
            shop,
        }
    }

    pub fn save_house_owners(&self, root: &Path) -> Result<(), String> {
        let Some(owners) = self.house_owners.as_ref() else {
            return Ok(());
        };
        let path = root.join("dat").join("owners.dat");
        crate::world::housing::save_house_owners(&path, owners)
    }

    pub fn spawn_player(
        &mut self,
        id: PlayerId,
        name: String,
        veteran: bool,
    ) -> Result<Position, String> {
        if self.players.contains_key(&id) {
            return Err(format!("player {:?} already spawned", id));
        }
        let position = self
            .spawn_position(veteran)
            .ok_or_else(|| "spawn position missing".to_string())?;
        let position = self
            .find_login_position(position, 1u8, true)
            .ok_or_else(|| "spawn position blocked".to_string())?;
        let player = PlayerState::new(id, name, position);
        self.players.insert(id, player);
        Ok(position)
    }

    pub fn spawn_monster_by_race(
        &mut self,
        race_number: i64,
        position: Position,
    ) -> Result<CreatureId, String> {
        self.spawn_monster_by_race_with_summoner(race_number, position, None, false, None)
    }

    fn spawn_monster_by_race_with_summoner(
        &mut self,
        race_number: i64,
        position: Position,
        summoner: Option<PlayerId>,
        summoned: bool,
        home_id: Option<usize>,
    ) -> Result<CreatureId, String> {
        if !self.position_in_bounds(position) {
            return Err("monster spawn blocked: out of bounds".to_string());
        }
        if !self.map.tiles.is_empty() && !self.map.has_tile(position) {
            return Err("monster spawn blocked: missing tile".to_string());
        }
        if self.is_protection_zone(position) {
            return Err("monster spawn blocked: protection zone".to_string());
        }
        if let Some(tile) = self.map.tile(position) {
            if self.tile_blocks_movement(tile) {
                return Err("monster spawn blocked: tile blocked".to_string());
            }
        }

        let index = self
            .monster_index
            .as_ref()
            .ok_or_else(|| "monster index missing".to_string())?;
        let script = index
            .script_by_race(race_number)
            .ok_or_else(|| format!("unknown monster race {}", race_number))?;
        let name = script
            .name
            .clone()
            .or_else(|| index.name_by_race(race_number).map(str::to_string))
            .unwrap_or_else(|| format!("Monster{}", race_number));

        let experience = script.experience().unwrap_or(0);
        let outfit = script.outfit().unwrap_or(DEFAULT_OUTFIT);
        let loot = crate::world::monsters::build_loot_table(script).unwrap_or_default();
        let corpse_ids = script
            .corpse_ids()
            .unwrap_or_default()
            .into_iter()
            .map(ItemTypeId)
            .collect::<Vec<_>>();
        let stats = if let Some(hitpoints) = script.hitpoints() {
            let hitpoints = hitpoints.max(1);
            Stats {
                health: hitpoints,
                max_health: hitpoints,
                ..Stats::default()
            }
        } else {
            Stats::default()
        };
        let flags = script
            .flags()
            .map(|entries| MonsterFlags::from_list(&entries))
            .unwrap_or_default();
        let skills = MonsterSkills::from_script(script);
        let strategy = script.strategy().unwrap_or([100, 0, 0, 0]);
        let flee_threshold = script.flee_threshold().unwrap_or(0);
        let lose_target_distance = script
            .lose_target()
            .unwrap_or(0)
            .min(u32::from(u16::MAX)) as u16;
        let attack = script.attack().unwrap_or(0);
        let defend = script.defend().unwrap_or(0);
        let armor = script.armor().unwrap_or(0);
        let poison = script.poison().unwrap_or(0);
        let spells = script.spells().unwrap_or_default();
        let talk_lines = script.talk_lines().unwrap_or_default();
        let talk_cooldown = Cooldown::new(GameTick(0));
        let speed = script
            .skill_value("GoStrength")
            .map(|value| 100u32.saturating_add(value.saturating_mul(2)))
            .unwrap_or(u32::from(DEFAULT_MONSTER_SPEED))
            .min(u32::from(u16::MAX)) as u16;

        let id = self.next_monster_id();
        let mut monster = MonsterInstance {
            id,
            race_number,
            summoner,
            summoned,
            home_id,
            name,
            position,
            direction: Direction::South,
            outfit,
            stats,
            experience,
            loot,
            inventory: Inventory::default(),
            inventory_containers: HashMap::new(),
            corpse_ids,
            flags,
            skills,
            attack,
            defend,
            armor,
            poison,
            spells,
            strategy,
            flee_threshold,
            lose_target_distance,
            target: None,
            damage_by: HashMap::new(),
            speed,
            outfit_effect: None,
            speed_effect: None,
            strength_effect: None,
            move_cooldown: Cooldown::new(GameTick(0)),
            combat_cooldown: Cooldown::new(GameTick(0)),
            talk_lines,
            talk_cooldown,
        };
        self.populate_monster_loot(&mut monster);
        self.monsters.insert(id, monster);
        self.add_monster_to_sector_index(id, position);
        Ok(id)
    }

    fn populate_monster_loot(&mut self, monster: &mut MonsterInstance) {
        if monster.summoned || monster.loot.entries.is_empty() {
            return;
        }
        if let Some(container_type) = self.default_container_type_id() {
            let bag = ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: container_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            };
            self.schedule_cron_for_item_tree(&bag);
            monster
                .inventory
                .set_slot(InventorySlot::Backpack, Some(bag));
            monster
                .inventory_containers
                .insert(InventorySlot::Backpack, Vec::new());
        }
        let drops = monster.loot.roll(&mut self.loot_rng, self.item_types.as_ref());
        for drop in drops {
            let route_to_bag = self
                .object_types
                .as_ref()
                .and_then(|index| index.get(drop.type_id))
                .map(Self::item_routes_to_bag)
                .unwrap_or(false);
            if route_to_bag {
                let stackable = self
                    .item_types
                    .as_ref()
                    .and_then(|index| index.get(drop.type_id))
                    .map(|entry| entry.stackable)
                    .unwrap_or(false);
                self.schedule_cron_for_item_tree(&drop);
                if self
                    .insert_monster_container_item(InventorySlot::Backpack, monster, drop, stackable)
                    .is_ok()
                {
                    continue;
                }
            } else if self.create_loot_at_monster(monster, drop) {
                continue;
            }
        }
        let bag_has_items = monster
            .inventory_containers
            .get(&InventorySlot::Backpack)
            .map(|items| !items.is_empty())
            .unwrap_or(false);
        if monster.inventory.slot(InventorySlot::Backpack).is_some() && !bag_has_items {
            monster.inventory.set_slot(InventorySlot::Backpack, None);
            monster.inventory_containers.remove(&InventorySlot::Backpack);
        }
    }

    fn default_container_type_id(&self) -> Option<ItemTypeId> {
        let object_types = self.object_types.as_ref()?;
        for (id, object) in object_types.iter() {
            if object.attribute_u16("Meaning") == Some(DEFAULT_CONTAINER_MEANING) {
                return Some(*id);
            }
        }
        None
    }

    fn item_routes_to_bag(object: &ObjectType) -> bool {
        object.has_flag("Weapon")
            || object.has_flag("Shield")
            || object.has_flag("Bow")
            || object.has_flag("Throw")
            || object.has_flag("Wand")
            || object.has_flag("Wearout")
            || object.has_flag("Expire")
            || object.has_flag("ExpireStop")
    }

    fn create_loot_at_monster(&mut self, monster: &mut MonsterInstance, item: ItemStack) -> bool {
        if let Some(object_types) = self.object_types.as_ref() {
            if !self.monster_can_carry_item(monster, object_types, &item) {
                return false;
            }
        }
        let stackable = self
            .item_types
            .as_ref()
            .and_then(|index| index.get(item.type_id))
            .map(|entry| entry.stackable)
            .unwrap_or(false);
        let prefer_container = self
            .object_types
            .as_ref()
            .and_then(|index| index.get(item.type_id))
            .map(|entry| entry.has_flag("MovementEvent"))
            .unwrap_or(false);
        if prefer_container {
            if self.try_place_loot_in_monster_containers(monster, item.clone(), stackable) {
                self.schedule_cron_for_item_tree(&item);
                return true;
            }
            if monster.inventory.add_item(item.clone(), stackable).is_ok() {
                self.schedule_cron_for_item_tree(&item);
                return true;
            }
        } else {
            if monster.inventory.add_item(item.clone(), stackable).is_ok() {
                self.schedule_cron_for_item_tree(&item);
                return true;
            }
            if self.try_place_loot_in_monster_containers(monster, item.clone(), stackable) {
                self.schedule_cron_for_item_tree(&item);
                return true;
            }
        }
        false
    }

    fn try_place_loot_in_monster_containers(
        &self,
        monster: &mut MonsterInstance,
        item: ItemStack,
        stackable: bool,
    ) -> bool {
        for slot in INVENTORY_SLOTS {
            if !monster.inventory_containers.contains_key(&slot) {
                continue;
            }
            if self
                .insert_monster_container_item(slot, monster, item.clone(), stackable)
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    fn insert_monster_container_item(
        &self,
        slot: InventorySlot,
        monster: &mut MonsterInstance,
        item: ItemStack,
        stackable: bool,
    ) -> Result<(), String> {
        let Some(item_types) = self.item_types.as_ref() else {
            return Err("item types missing".to_string());
        };
        let Some(container_item) = monster.inventory.slot(slot) else {
            return Err("container slot empty".to_string());
        };
        let capacity = item_types
            .get(container_item.type_id)
            .and_then(|entry| entry.container_capacity)
            .unwrap_or(0);
        let capacity = capacity.min(u16::from(u8::MAX)) as u8;
        let items = monster
            .inventory_containers
            .get_mut(&slot)
            .ok_or_else(|| "container slot missing".to_string())?;
        insert_into_inventory_container_items(items, capacity, item, stackable)
    }

    fn monster_can_carry_item(
        &self,
        monster: &MonsterInstance,
        object_types: &ObjectTypeIndex,
        item: &ItemStack,
    ) -> bool {
        let max_weight = monster.stats.capacity.saturating_mul(100);
        let current_weight = monster_total_weight(monster, object_types);
        let added_weight = item_stack_total_weight(object_types, item);
        current_weight.saturating_add(added_weight) <= max_weight
    }

    pub fn apply_damage_to_monster(
        &mut self,
        monster_id: CreatureId,
        damage_type: DamageType,
        mut amount: u32,
        killer: Option<PlayerId>,
    ) -> Result<Option<MonsterReward>, String> {
        let dead = {
            let monster = self
                .monsters
                .get_mut(&monster_id)
                .ok_or_else(|| format!("unknown monster {:?}", monster_id))?;
            if monster.flags.blocks_damage(damage_type) {
                return Ok(None);
            }
            if damage_type == DamageType::Physical {
                amount = amount
                    .saturating_sub(monster.defend)
                    .saturating_sub(monster.armor);
            }
            let applied = monster.stats.apply_damage(damage_type, amount);
            if let Some(attacker) = killer {
                if applied > 0 {
                    let entry = monster.damage_by.entry(attacker).or_insert(0);
                    *entry = entry.saturating_add(applied);
                }
            }
            monster.stats.health == 0
        };

        if dead {
            return Ok(Some(self.defeat_monster(monster_id, killer)?));
        }

        Ok(None)
    }

    pub fn defeat_monster(
        &mut self,
        monster_id: CreatureId,
        killer: Option<PlayerId>,
    ) -> Result<MonsterReward, String> {
        let monster = self
            .monsters
            .remove(&monster_id)
            .ok_or_else(|| format!("unknown monster {:?}", monster_id))?;
        self.remove_monster_from_sector_index(monster_id, monster.position);
        if let Some(home_id) = monster.home_id {
            self.notify_monster_home_death(home_id);
        }
        let drops = monster_inventory_items(&monster);

        if let Some(corpse_id) = monster.corpse_ids.first().copied() {
            let corpse = ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: corpse_id,
                count: 1,
                attributes: Vec::new(),
                contents: drops.clone(),
            };
            self.cron_expire_item(&corpse, -1);
            let stackable = self
                .item_types
                .as_ref()
                .and_then(|index| index.get(corpse_id))
                .map(|entry| entry.stackable)
                .unwrap_or(false);
            let movable = self.item_is_movable(&corpse);
            if let Some(tile) = self.map.tile_mut(monster.position) {
                let _ = place_on_tile_with_dustbin(tile, corpse, stackable, movable);
            }
        } else {
            let mut drop_entries = Vec::new();
            for drop in &drops {
                let stackable = self
                    .item_types
                    .as_ref()
                    .and_then(|index| index.get(drop.type_id))
                    .map(|entry| entry.stackable)
                    .unwrap_or(false);
                let movable = self.item_is_movable(drop);
                self.schedule_cron_for_item_tree(drop);
                drop_entries.push((drop.clone(), stackable, movable));
            }
            if let Some(tile) = self.map.tile_mut(monster.position) {
                for (drop, stackable, movable) in drop_entries {
                    let _ = place_on_tile_with_dustbin(tile, drop, stackable, movable);
                }
            }
        }

        let experience = monster.experience;
        if let Some(killer_id) = killer {
            let party_id = self.party_id_for_player(killer_id);
            if let Some(party_id) = party_id {
                if let Some(members) = self.party_shared_exp_members(party_id) {
                    let share = Self::party_shared_exp_share(experience, members.len());
                    for member_id in members {
                        if let Some(player) = self.players.get_mut(&member_id) {
                            player.add_experience(share);
                            Self::apply_soul_regen_on_experience(player, share);
                        }
                    }
                } else if let Some(player) = self.players.get_mut(&killer_id) {
                    player.add_experience(experience);
                    Self::apply_soul_regen_on_experience(player, experience);
                }
            } else if let Some(player) = self.players.get_mut(&killer_id) {
                player.add_experience(experience);
                Self::apply_soul_regen_on_experience(player, experience);
            }
        }

        Ok(MonsterReward { experience, drops })
    }

    pub fn schedule_raid(
        &mut self,
        raid_name: &str,
        seed: u64,
        start: GameTick,
    ) -> Result<usize, String> {
        let index = self
            .monster_index
            .as_ref()
            .ok_or_else(|| "monster index missing".to_string())?;
        let raid = index
            .raids
            .get(raid_name)
            .ok_or_else(|| format!("unknown raid {}", raid_name))?;
        let plans = crate::world::monsters::resolve_raid_spawns(index, raid, seed)?;
        for plan in plans {
            let delay = plan.delay.max(0) as u64;
            let at = GameTick(start.0.saturating_add(delay));
            self.raid_events.push(RaidSpawnEvent { at, plan });
        }
        Ok(self.raid_events.len())
    }

    pub fn spawn_due_raids(&mut self, now: GameTick) -> Vec<CreatureId> {
        if self.raid_events.is_empty() {
            return Vec::new();
        }
        let mut spawned = Vec::new();
        let mut pending = Vec::new();
        let events = std::mem::take(&mut self.raid_events);
        for event in events {
            if event.at <= now {
                for position in event.plan.positions {
                    if let Ok(id) = self.spawn_monster_by_race(event.plan.race_number, position) {
                        spawned.push(id);
                    }
                }
            } else {
                pending.push(event);
            }
        }
        self.raid_events = pending;
        spawned
    }

    pub fn tick_raids(&mut self, now: GameTick, clock: &GameClock) -> Vec<CreatureId> {
        self.ensure_raid_schedules(now, clock);
        if self.raid_schedules.is_empty() {
            return Vec::new();
        }
        let mut due_raids = Vec::new();
        for (name, schedule) in self.raid_schedules.iter_mut() {
            if now >= schedule.next_at {
                due_raids.push(name.clone());
                schedule.next_at =
                    GameTick(schedule.next_at.0.saturating_add(schedule.interval_ticks));
            }
        }
        for name in due_raids {
            let seed = raid_seed(&name, now);
            if let Err(err) = self.schedule_raid(&name, seed, now) {
                eprintln!("tibia: raid schedule {} failed: {}", name, err);
            }
        }
        self.spawn_due_raids(now)
    }

    pub fn add_spell(&mut self, spell: Spell) -> Result<(), String> {
        self.spellbook.insert(spell)
    }

    pub fn teach_spell(&mut self, player_id: PlayerId, spell_id: SpellId) -> Result<(), String> {
        if self.spellbook.get(spell_id).is_none() {
            return Err(format!("unknown spell {:?}", spell_id));
        }
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        player.learn_spell(spell_id);
        Ok(())
    }

    pub fn move_player(
        &mut self,
        id: PlayerId,
        direction: Direction,
        clock: &GameClock,
    ) -> Result<Position, String> {
        let (origin, ready, speed, drunken_effect) = {
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            (
                player.position,
                player.move_cooldown.is_ready(clock),
                self.player_move_speed(player),
                player.drunken_effect,
            )
        };
        if !ready {
            return Err("movement blocked: cooldown".to_string());
        }
        let mut direction = direction;
        if let Some(effect) = drunken_effect {
            if !effect.is_expired(clock.now()) {
                let chance = u32::from(effect.intensity)
                    .saturating_mul(DRUNKEN_CHANCE_PER_LEVEL)
                    .min(100);
                if chance > 0 && self.monster_rng.roll_percent(chance) {
                    direction = self.monster_rng.roll_direction();
                }
            }
        }
        let destination = self.resolve_movement_destination(origin, direction)?;
        if let Some(house) = self.house_for_position(destination) {
            let origin_house_id = self.house_for_position(origin).map(|house| house.id);
            if origin_house_id != Some(house.id) {
                let player = self
                    .players
                    .get(&id)
                    .ok_or_else(|| format!("unknown player {:?}", id))?;
                if !self.player_can_enter_house(player, house) {
                    return Err("movement blocked: house access".to_string());
                }
            }
        }
        {
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            if !self.door_allows_player(player, destination) {
                return Err("movement blocked: door access".to_string());
            }
        }
        let tile_speed = self
            .map
            .tile(destination)
            .map(|tile| self.tile_ground_speed(tile))
            .unwrap_or(DEFAULT_GROUND_SPEED);
        let cooldown_ticks = self.movement_cooldown_ticks(tile_speed, speed, direction, clock);
        let player = self
            .players
            .get_mut(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        player.move_to(destination, direction);
        player.move_cooldown.reset_from_now_ticks(clock, cooldown_ticks);
        let separation =
            self.trigger_moveuse_tile_event(MoveUseEvent::Separation, id, origin, origin);
        let collision =
            self.trigger_moveuse_tile_event(MoveUseEvent::Collision, id, destination, destination);
        self.queue_moveuse_outcomes(id, separation);
        self.queue_moveuse_outcomes(id, collision);
        Ok(destination)
    }

    pub fn turn_player(&mut self, id: PlayerId, direction: Direction) -> Result<(), String> {
        let (position, changed) = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            if player.direction == direction {
                return Ok(());
            }
            player.direction = direction;
            (player.position, true)
        };
        if changed {
            self.queue_turn_update(CreatureTurnUpdate {
                id: id.0,
                position,
                direction,
            });
        }
        Ok(())
    }

    pub fn set_player_autowalk(&mut self, id: PlayerId, steps: Vec<Direction>) {
        if let Some(player) = self.players.get_mut(&id) {
            player.autowalk_steps = steps.into();
        }
    }

    pub fn clear_player_autowalk(&mut self, id: PlayerId) {
        if let Some(player) = self.players.get_mut(&id) {
            player.autowalk_steps.clear();
        }
    }

    pub fn creature_exists(&self, id: CreatureId) -> bool {
        self.players.contains_key(&PlayerId(id.0))
            || self.monsters.contains_key(&id)
            || self.npcs.contains_key(&id)
    }

    pub fn set_player_attack_target(&mut self, id: PlayerId, target: Option<CreatureId>) {
        if let Some(player) = self.players.get_mut(&id) {
            player.attack_target = target;
        }
    }

    pub fn set_player_follow_target(&mut self, id: PlayerId, target: Option<CreatureId>) {
        if let Some(player) = self.players.get_mut(&id) {
            player.follow_target = target;
        }
    }

    pub fn set_player_outfit(&mut self, id: PlayerId, outfit: Outfit) {
        let Some(player) = self.players.get_mut(&id) else {
            return;
        };
        let outfit = player.clamp_outfit(outfit);
        let mut update = None;
        if let Some(effect) = player.outfit_effect.as_mut() {
            effect.original = outfit;
            player.original_outfit = outfit;
        } else {
            let current = player.current_outfit;
            player.current_outfit = outfit;
            player.original_outfit = outfit;
            if current != outfit {
                update = Some(CreatureOutfitUpdate {
                    id: id.0,
                    outfit,
                });
            }
        }
        if let Some(update) = update {
            self.queue_outfit_update(update);
        }
    }

    pub fn tick_player_autowalk(&mut self, id: PlayerId, clock: &GameClock) {
        let direction = self
            .players
            .get(&id)
            .and_then(|player| player.autowalk_steps.front().copied());
        let Some(direction) = direction else {
            return;
        };
        match self.move_player(id, direction, clock) {
            Ok(_) => {
                if let Some(player) = self.players.get_mut(&id) {
                    player.autowalk_steps.pop_front();
                }
            }
            Err(err) => {
                if err != "movement blocked: cooldown" {
                    if let Some(player) = self.players.get_mut(&id) {
                        player.autowalk_steps.clear();
                    }
                }
            }
        }
    }

    pub fn tick_player_attack(
        &mut self,
        player_id: PlayerId,
        clock: &GameClock,
    ) -> PlayerCombatOutcome {
        let mut outcome = PlayerCombatOutcome::default();
        let (target_id, attack_ready, attacker_pos, attack_mode, attacker_health) = match self
            .players
            .get(&player_id)
        {
            Some(player) => (
                player.attack_target,
                player.attack_cooldown.is_ready(clock),
                player.position,
                player.fight_modes.attack_mode,
                player.stats.health,
            ),
            None => return outcome,
        };
        let Some(target_id) = target_id else {
            return outcome;
        };
        if !attack_ready || attacker_health == 0 {
            return outcome;
        }
        if self.is_protection_zone(attacker_pos) {
            return outcome;
        }

        let selection = match self.players.get(&player_id) {
            Some(player) => self.player_attack_values(player),
            None => return outcome,
        };
        if selection.attack == 0 {
            return outcome;
        }

        let (skill_level, level) = match self.players.get(&player_id) {
            Some(player) => (player.skills.get(selection.skill).level, player.level),
            None => return outcome,
        };
        let attempted_damage =
            self.roll_player_attack_damage(selection.attack, attack_mode, skill_level, level);

        let mark_attacker;
        let mark_white_skull;

        if let Some(monster_pos) = self.monsters.get(&target_id).map(|monster| monster.position) {
            if monster_pos.z != attacker_pos.z {
                return outcome;
            }
            if Self::monster_tile_distance(attacker_pos, monster_pos) > selection.range {
                return outcome;
            }
            if self.is_protection_zone(monster_pos) {
                return outcome;
            }
            self.train_player_skill(player_id, selection.skill);
            let before_health = self
                .monsters
                .get(&target_id)
                .map(|monster| monster.stats.health)
                .unwrap_or(0);
            if self
                .apply_damage_to_monster(
                    target_id,
                    selection.damage_type,
                    attempted_damage,
                    Some(player_id),
                )
                .is_err()
            {
                return outcome;
            }
            let after_health = self
                .monsters
                .get(&target_id)
                .map(|monster| monster.stats.health)
                .unwrap_or(0);
            let applied = before_health.saturating_sub(after_health);
            if applied > 0 {
                if let Some(attacker) = self.players.get_mut(&player_id) {
                    Self::grant_learning_points(attacker);
                }
            }
            if attempted_damage > 0 {
                let effect_id = if applied > 0 {
                    PLAYER_ATTACK_EFFECT_BLOOD_ID
                } else {
                    PLAYER_ATTACK_EFFECT_BLOCK_ID
                };
                outcome.effects.push(CombatVisualEffect {
                    position: monster_pos,
                    effect_id,
                });
            }
            if self.monsters.contains_key(&target_id) {
                outcome.monster_updates.push(target_id);
            } else {
                outcome.refresh_map = true;
                if let Some(player) = self.players.get_mut(&player_id) {
                    if player.attack_target == Some(target_id) {
                        player.attack_target = None;
                    }
                }
            }
            mark_attacker = attempted_damage > 0;
            mark_white_skull = false;
        } else {
            let target_player_id = PlayerId(target_id.0);
            if target_player_id == player_id {
                return outcome;
            }
            let target_pos = match self.players.get(&target_player_id) {
                Some(target) => target.position,
                None => {
                    if self.npcs.contains_key(&target_id) {
                        if let Some(player) = self.players.get_mut(&player_id) {
                            player.attack_target = None;
                        }
                    } else if let Some(player) = self.players.get_mut(&player_id) {
                        player.attack_target = None;
                    }
                    return outcome;
                }
            };
            if !self.combat_rules.pvp_enabled {
                return outcome;
            }
            if target_pos.z != attacker_pos.z {
                return outcome;
            }
            if self.is_protection_zone(target_pos) {
                return outcome;
            }
            if Self::monster_tile_distance(attacker_pos, target_pos) > selection.range {
                return outcome;
            }

            let (defend_selection, defend_ready, target_mode) =
                match self.players.get(&target_player_id) {
                    Some(target) => (
                        self.player_defense_values(target),
                        target.defend_cooldown.is_ready(clock),
                        target.fight_modes.attack_mode,
                    ),
                    None => return outcome,
                };
            let defend = defend_selection.defend;
            let armor = defend_selection.armor;
            let defend_roll = if defend_ready {
                if defend_selection.skill == SkillType::Shielding {
                    self.train_player_skill(target_player_id, SkillType::Shielding);
                }
                let scaled = Self::scale_defend_by_attack_mode(defend, target_mode);
                if scaled == 0 {
                    0
                } else {
                    let capped = scaled.min(u32::from(u16::MAX));
                    self.moveuse_rng.roll_range(0, capped)
                }
            } else {
                0
            };
            let armor_roll = if armor == 0 {
                0
            } else {
                let capped = armor.min(u32::from(u16::MAX));
                self.moveuse_rng.roll_range(0, capped)
            };
            let mitigated_damage = attempted_damage
                .saturating_sub(defend_roll)
                .saturating_sub(armor_roll);
            let reduced_damage = self.apply_player_protection_reduction(
                target_player_id,
                selection.damage_type,
                mitigated_damage,
            );
            let (applied_damage, absorbed_mana) = {
                let target = match self.players.get_mut(&target_player_id) {
                    Some(target) => target,
                    None => return outcome,
                };
                let (applied, absorbed) = target
                    .apply_damage_with_magic_shield(selection.damage_type, reduced_damage);
                if applied > 0 || absorbed > 0 {
                    target.mark_in_combat(clock, self.combat_rules.fight_timer);
                }
                (applied, absorbed)
            };
            if attempted_damage > 0 {
                let effect_id = if applied_damage > 0 {
                    PLAYER_ATTACK_EFFECT_BLOOD_ID
                } else {
                    PLAYER_ATTACK_EFFECT_BLOCK_ID
                };
                outcome.effects.push(CombatVisualEffect {
                    position: target_pos,
                    effect_id,
                });
                outcome.player_updates.push(target_player_id);
            }
            if applied_damage > 0 {
                if let Some(attacker) = self.players.get_mut(&player_id) {
                    Self::grant_learning_points(attacker);
                }
            }
            if attempted_damage > 0 && defend_ready {
                self.apply_defend_wear(target_player_id, defend_selection);
                let defend_ticks = clock.ticks_from_duration_round_up(Duration::from_millis(
                    DEFEND_COOLDOWN_MS,
                ));
                if let Some(target) = self.players.get_mut(&target_player_id) {
                    target
                        .defend_cooldown
                        .reset_from_now_ticks(clock, defend_ticks);
                }
            }
            let _ = absorbed_mana;
            mark_attacker = attempted_damage > 0;
            mark_white_skull = attempted_damage > 0;
        }

        if let Some(attacker) = self.players.get_mut(&player_id) {
            attacker
                .attack_cooldown
                .reset_from_now_ticks(clock, PLAYER_COMBAT_INTERVAL_TICKS);
            if mark_attacker {
                attacker.mark_in_combat(clock, self.combat_rules.fight_timer);
                if mark_white_skull {
                    attacker.mark_white_skull(clock, self.combat_rules.white_skull_timer);
                }
            }
        }
        outcome
    }

    pub fn close_npc_dialog(&mut self, id: PlayerId) {
        self.shop_close(id);
        let mut cleared_npcs = Vec::new();
        for (npc_id, npc) in self.npcs.iter_mut() {
            let mut cleared = false;
            if npc.focused == Some(id) {
                npc.focused = None;
                npc.focus_expires_at = None;
                cleared = true;
            }
            if npc.queue.iter().any(|queued| *queued == id) {
                npc.queue.retain(|queued| *queued != id);
                cleared = true;
            }
            if cleared {
                cleared_npcs.push(*npc_id);
            }
        }
        if let Some(player) = self.players.get_mut(&id) {
            for npc_id in cleared_npcs {
                player.set_npc_topic(npc_id, 0);
                player.clear_npc_vars(npc_id);
            }
        }
    }

    pub fn move_monster(
        &mut self,
        id: CreatureId,
        direction: Direction,
    ) -> Result<Position, String> {
        let (origin, flags) = {
            let monster = self
                .monsters
                .get(&id)
                .ok_or_else(|| format!("unknown monster {:?}", id))?;
            (monster.position, monster.flags)
        };
        let destination = self.resolve_monster_movement_destination(origin, direction, flags)?;
        let monster = self
            .monsters
            .get_mut(&id)
            .ok_or_else(|| format!("unknown monster {:?}", id))?;
        monster.position = destination;
        monster.direction = direction;
        self.update_monster_sector_index(id, origin, destination);
        Ok(destination)
    }

    pub fn move_npc(
        &mut self,
        id: CreatureId,
        direction: Direction,
    ) -> Result<Position, String> {
        let origin = self
            .npcs
            .get(&id)
            .ok_or_else(|| format!("unknown npc {:?}", id))?
            .position;
        let destination = self.resolve_movement_destination(origin, direction)?;
        let npc = self
            .npcs
            .get_mut(&id)
            .ok_or_else(|| format!("unknown npc {:?}", id))?;
        if !npc_within_wander_radius(npc.home, npc.radius, destination) {
            return Err("movement blocked: npc radius".to_string());
        }
        npc.position = destination;
        npc.direction = direction;
        Ok(destination)
    }

    fn player_move_speed(&self, player: &PlayerState) -> u16 {
        player
            .speed_effect
            .map(|effect| effect.speed)
            .unwrap_or(player.base_speed)
    }

    pub fn player_capacity_remaining(&self, player: &PlayerState) -> u32 {
        let Some(object_types) = self.object_types.as_ref() else {
            return player.stats.capacity;
        };
        let max_weight = player.stats.capacity.saturating_mul(100);
        let current_weight = player_total_weight(player, object_types);
        max_weight.saturating_sub(current_weight) / 100
    }

    pub fn open_container_for_player(
        &mut self,
        player_id: PlayerId,
        item_type: ItemTypeId,
        source: ContainerSource,
        container_id: Option<u8>,
    ) -> Result<OpenContainer, String> {
        let (
            items,
            parent_container_id,
            parent_slot,
            source_slot,
            source_position,
            source_stack,
            root_position,
        ) = {
            let player = self
                .players
                .get(&player_id)
                .ok_or_else(|| format!("unknown player {:?}", player_id))?;
            match source {
                ContainerSource::InventorySlot(slot) => {
                    let mut items = player
                        .inventory_containers
                        .get(&slot)
                        .cloned()
                        .unwrap_or_default();
                    if items.is_empty() {
                        if let Some(item) = player.inventory.slot(slot) {
                            items = item.contents.clone();
                        }
                    }
                    (items, None, None, Some(slot), None, None, None)
                }
                ContainerSource::Container { container_id, slot } => {
                    let parent = player
                        .open_containers
                        .get(&container_id)
                        .ok_or_else(|| "parent container not open".to_string())?;
                    let item = parent
                        .items
                        .get(slot as usize)
                        .ok_or_else(|| "parent container slot out of range".to_string())?;
                    (
                        item.contents.clone(),
                        Some(container_id),
                        Some(slot),
                        None,
                        None,
                        None,
                        container_root_position(player, container_id),
                    )
                }
                ContainerSource::Map { position, stack_pos } => {
                    self.ensure_player_in_range(player_id, position)?;
                    let tile = self
                        .map
                        .tile(position)
                        .ok_or_else(|| "map container tile missing".to_string())?;
                    let index = stack_pos as usize;
                    let item = tile
                        .items
                        .get(index)
                        .ok_or_else(|| "map container stack missing".to_string())?;
                    if item.type_id != item_type {
                        return Err("map container type mismatch".to_string());
                    }
                    (
                        item.contents.clone(),
                        None,
                        None,
                        None,
                        Some(position),
                        Some(stack_pos),
                        Some(position),
                    )
                }
            }
        };
        let (name, mut capacity) = match self
            .item_types
            .as_ref()
            .and_then(|types| types.get(item_type))
        {
            Some(item) => {
                if item.kind != ItemKind::Container {
                    return Err("open container failed: item is not a container".to_string());
                }
                (
                    item.name.clone(),
                    item.container_capacity
                        .unwrap_or_else(|| items.len().max(1).min(u8::MAX as usize) as u16)
                        .min(u16::from(u8::MAX)) as u8,
                )
            }
            None => {
                let fallback_capacity = items.len().max(1).min(u8::MAX as usize) as u8;
                (format!("Container {}", item_type.0), fallback_capacity)
            }
        };
        if item_type == DEPOT_CHEST_TYPE_ID {
            if let Some(player) = self.players.get(&player_id) {
                if let (Some(active), Some(root)) = (player.active_depot, root_position) {
                    if root == active.locker_position {
                        if let Some(depot_capacity) = active.capacity {
                            capacity = depot_capacity.min(u32::from(u8::MAX)) as u8;
                        }
                    }
                }
            }
        }
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        let container_id = match container_id {
            Some(container_id) => container_id,
            None => player
                .next_container_id()
                .ok_or_else(|| "open container failed: no free container ids".to_string())?,
        };
        player.open_containers.remove(&container_id);
        let open = OpenContainer {
            container_id,
            item_type,
            name,
            capacity,
            has_parent: parent_container_id.is_some(),
            parent_container_id,
            parent_slot,
            source_slot,
            source_position,
            source_stack_pos: source_stack,
            items,
        };
        player.open_containers.insert(container_id, open.clone());
        Ok(open)
    }

    pub fn find_open_container_id_for_player_source(
        &self,
        player_id: PlayerId,
        source: ContainerSource,
    ) -> Option<u8> {
        let player = self.players.get(&player_id)?;
        match source {
            ContainerSource::InventorySlot(slot) => find_container_by_slot(player, slot),
            ContainerSource::Container { container_id, slot } => {
                find_container_by_parent_slot(player, container_id, slot)
            }
            ContainerSource::Map { position, stack_pos } => {
                find_container_by_map_source(player, position, stack_pos)
            }
        }
    }

    pub fn close_out_of_range_map_containers(&mut self, player_id: PlayerId) -> Vec<u8> {
        let player_position = match self.players.get(&player_id) {
            Some(player) => player.position,
            None => return Vec::new(),
        };
        let snapshot: Vec<(u8, OpenContainer)> = match self.players.get(&player_id) {
            Some(player) => player
                .open_containers
                .iter()
                .map(|(id, container)| (*id, container.clone()))
                .collect(),
            None => return Vec::new(),
        };
        if snapshot.is_empty() {
            return Vec::new();
        }
        let mut by_id = HashMap::new();
        for (id, container) in &snapshot {
            by_id.insert(*id, container);
        }

        let mut root_out_of_range = HashSet::new();
        for (id, container) in &snapshot {
            if let Some(position) = container.source_position {
                let dx = i32::from(player_position.x) - i32::from(position.x);
                let dy = i32::from(player_position.y) - i32::from(position.y);
                let in_range = player_position.z == position.z
                    && dx.unsigned_abs() <= 1
                    && dy.unsigned_abs() <= 1;
                if !in_range {
                    root_out_of_range.insert(*id);
                }
            }
        }
        if root_out_of_range.is_empty() {
            return Vec::new();
        }

        let mut to_close: Vec<(u8, usize)> = Vec::new();
        for (id, container) in &snapshot {
            let mut current = *id;
            let mut depth = 0usize;
            while let Some(parent_id) = by_id.get(&current).and_then(|entry| entry.parent_container_id)
            {
                current = parent_id;
                depth = depth.saturating_add(1);
            }
            if root_out_of_range.contains(&current) {
                to_close.push((*id, depth));
            }
            if container.source_position.is_some() && root_out_of_range.contains(id) {
                // Root map containers are already tracked.
            }
        }
        to_close.sort_by(|a, b| b.1.cmp(&a.1));

        let mut closed = Vec::new();
        let mut seen = HashSet::new();
        for (container_id, _) in to_close {
            if seen.insert(container_id) {
                if self.close_container_for_player(player_id, container_id).unwrap_or(false) {
                    closed.push(container_id);
                }
            }
        }
        closed
    }

    fn close_depot_containers(&mut self, player_id: PlayerId, locker_position: Position) {
        let snapshot: Vec<(u8, OpenContainer)> = match self.players.get(&player_id) {
            Some(player) => player
                .open_containers
                .iter()
                .map(|(id, container)| (*id, container.clone()))
                .collect(),
            None => return,
        };
        if snapshot.is_empty() {
            return;
        }

        let mut by_id: HashMap<u8, OpenContainer> = HashMap::new();
        for (id, container) in &snapshot {
            by_id.insert(*id, container.clone());
        }

        let mut candidates: Vec<(u8, usize)> = Vec::new();
        for (id, _container) in &snapshot {
            let mut current = *id;
            let mut depth = 0usize;
            while let Some(parent_id) = by_id.get(&current).and_then(|entry| entry.parent_container_id)
            {
                current = parent_id;
                depth = depth.saturating_add(1);
            }
            if let Some(root) = by_id.get(&current).and_then(|entry| entry.source_position) {
                if root == locker_position {
                    candidates.push((*id, depth));
                }
            }
        }

        if candidates.is_empty() {
            return;
        }

        candidates.sort_by(|a, b| b.1.cmp(&a.1));
        for (container_id, _) in candidates {
            if self.close_container_for_player(player_id, container_id).unwrap_or(false) {
                self.queue_container_close(player_id, container_id);
            }
        }
    }

    fn ensure_depot_capacity_for_insert(
        &self,
        player_id: PlayerId,
        dest_container: u8,
        source_container: Option<u8>,
        item: &ItemStack,
    ) -> Result<(), String> {
        let player = self
            .players
            .get(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        let Some(active) = container_active_depot(player, dest_container) else {
            return Ok(());
        };
        if let Some(source_container) = source_container {
            if let Some(source_active) = container_active_depot(player, source_container) {
                if source_active.locker_position == active.locker_position {
                    return Ok(());
                }
            }
        }
        let Some(capacity) = active.capacity else {
            return Ok(());
        };
        let tile = self
            .map
            .tile(active.locker_position)
            .ok_or_else(|| "depot locker tile missing".to_string())?;
        let locker = tile
            .items
            .iter()
            .find(|item| item.type_id == active.locker_type)
            .ok_or_else(|| "depot locker missing".to_string())?;
        let current_count = depot_item_count(&locker.contents);
        let added_count = count_item_with_contents(item);
        if current_count.saturating_add(added_count) > capacity {
            return Err("depot capacity exceeded".to_string());
        }
        Ok(())
    }

    pub fn up_container_for_player(
        &mut self,
        player_id: PlayerId,
        container_id: u8,
    ) -> Result<Option<OpenContainer>, String> {
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        let Some(container) = player.open_containers.get(&container_id).cloned() else {
            return Ok(None);
        };
        let Some(parent_id) = container.parent_container_id else {
            return Ok(None);
        };
        let Some(parent) = player.open_containers.get(&parent_id).cloned() else {
            return Ok(None);
        };
        let mut open = parent;
        open.container_id = container_id;
        open.has_parent = open.parent_container_id.is_some();
        player.open_containers.insert(container_id, open.clone());
        Ok(Some(open))
    }

    pub fn close_container_for_player(
        &mut self,
        player_id: PlayerId,
        container_id: u8,
    ) -> Result<bool, String> {
        self.sync_container_contents(player_id, container_id);
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        Ok(player.close_container(container_id))
    }

    fn is_container_item(&self, item: &ItemStack) -> bool {
        self.item_types
            .as_ref()
            .and_then(|types| types.get(item.type_id))
            .map(|entry| entry.kind == ItemKind::Container)
            .unwrap_or(false)
    }

    fn close_open_containers_for_item(&mut self, player_id: PlayerId, item: &ItemStack) {
        let open_ids: Vec<u8> = match self.players.get(&player_id) {
            Some(player) => player
                .open_containers
                .iter()
                .filter_map(|(id, container)| {
                    if item_contains_open_container(item, container) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect(),
            None => Vec::new(),
        };
        for container_id in open_ids {
            if self
                .close_container_for_player(player_id, container_id)
                .unwrap_or(false)
            {
                self.queue_container_close(player_id, container_id);
            }
        }
    }

    fn tile_ground_speed(&self, tile: &Tile) -> u16 {
        let Some(ground) = tile.items.first() else {
            return DEFAULT_GROUND_SPEED;
        };
        let Some(types) = self.object_types.as_ref() else {
            return DEFAULT_GROUND_SPEED;
        };
        types
            .get(ground.type_id)
            .and_then(|object_type| object_type.ground_speed())
            .unwrap_or(DEFAULT_GROUND_SPEED)
    }

    fn movement_cooldown_ticks(
        &self,
        tile_speed: u16,
        creature_speed: u16,
        direction: Direction,
        clock: &GameClock,
    ) -> u64 {
        let mut tile_speed = u64::from(tile_speed.max(1));
        if direction.is_diagonal() {
            tile_speed = tile_speed.saturating_mul(3);
        }
        let creature_speed = u64::from(creature_speed.max(1));
        let raw_ms = tile_speed.saturating_mul(1000) / creature_speed;
        let duration = Duration::from_millis(raw_ms.max(1));
        let ticks = clock.ticks_from_duration_round_up(duration);
        ticks.max(1)
    }

    pub fn teleport_player_admin(
        &mut self,
        id: PlayerId,
        position: Position,
    ) -> Result<(), String> {
        if !self.position_in_bounds(position) {
            return Err("target out of bounds".to_string());
        }
        if self
            .players
            .get(&id)
            .map(|player| player.is_test_god)
            .unwrap_or(false)
        {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            player.move_to(position, player.direction);
            return Ok(());
        }
        self.teleport_player(id, position)
    }

    pub fn run_moveuse_audit_for_player(&mut self, user_id: PlayerId) -> Result<String, String> {
        let Some(player) = self.players.get(&user_id) else {
            return Err(format!("unknown player {:?}", user_id));
        };
        if !player.is_test_god {
            return Err("moveuse audit requires test_god privileges".to_string());
        }
        let base = player.position;
        let Some(moveuse) = self.moveuse.clone() else {
            return Err("moveuse.dat is not loaded".to_string());
        };
        let rules = collect_moveuse_rules(&moveuse);
        if rules.is_empty() {
            return Ok("moveuse audit PASS rules=0 actions=0 unique_actions=0".to_string());
        }

        let mut failures = Vec::new();
        let mut action_count = 0usize;
        let mut unique_actions = HashSet::new();
        for (rule_index, rule) in rules.iter().enumerate() {
            let Some(event) = parse_moveuse_event_name(&rule.event.name) else {
                failures.push(format!(
                    "line {}: unsupported event '{}'",
                    rule.line_no, rule.event.name
                ));
                continue;
            };

            for (action_index, action) in rule.actions.iter().enumerate() {
                action_count = action_count.saturating_add(1);
                unique_actions.insert(action.name.trim_start_matches('!').to_string());

                let global_index = rule_index.saturating_mul(8).saturating_add(action_index);
                let object_position = Position {
                    x: base
                        .x
                        .saturating_add(((global_index % 16) as u16).saturating_add(1)),
                    y: base
                        .y
                        .saturating_add((((global_index / 16) % 16) as u16).saturating_add(1)),
                    z: base.z,
                };
                let object2_position = Position {
                    x: object_position.x.saturating_add(1),
                    y: object_position.y,
                    z: object_position.z,
                };

                let object_type_id = moveuse_rule_obj_type(rule, "Obj1").unwrap_or(ItemTypeId(100));
                let object2_type_id =
                    moveuse_rule_obj_type(rule, "Obj2").unwrap_or(ItemTypeId(101));

                let mut single_rule = rule.clone();
                single_rule.actions = vec![action.clone()];
                self.ensure_moveuse_audit_tile(base);
                self.ensure_moveuse_audit_tile(object_position);
                self.ensure_moveuse_audit_tile(object2_position);
                self.place_moveuse_audit_item(object_position, object_type_id, 1);
                self.place_moveuse_audit_item(object2_position, object2_type_id, 1);
                self.prepare_moveuse_audit_rule(&single_rule, object_position, object2_position);

                if let Some(player) = self.players.get_mut(&user_id) {
                    player.move_to(base, player.direction);
                }

                let ctx = MoveUseContext {
                    event,
                    user_id,
                    user_position: base,
                    object_position,
                    object_type_id,
                    object_source: UseObjectSource::Map(object_position),
                    object2_position: Some(object2_position),
                    object2_type_id: Some(object2_type_id),
                };
                let mut outcome = MoveUseOutcome {
                    matched_rule: Some(rule.line_no),
                    ignored_actions: Vec::new(),
                    effects: Vec::new(),
                    texts: Vec::new(),
                    edit_texts: Vec::new(),
                    edit_lists: Vec::new(),
                    messages: Vec::new(),
                    damages: Vec::new(),
                    quest_updates: Vec::new(),
                    logout_users: Vec::new(),
                    refresh_positions: Vec::new(),
                    inventory_updates: Vec::new(),
                    container_updates: Vec::new(),
                };
                if let Err(err) = self.apply_moveuse_actions(&single_rule, &ctx, &mut outcome, None) {
                    failures.push(format!("line {} action {}: {}", rule.line_no, action.name, err));
                }
            }
        }

        if failures.is_empty() {
            return Ok(format!(
                "moveuse audit PASS rules={} actions={} unique_actions={}",
                rules.len(),
                action_count,
                unique_actions.len()
            ));
        }

        let report_path = self
            .write_moveuse_audit_report(user_id, &failures)
            .unwrap_or_else(|_| "<report write failed>".to_string());

        let preview = failures
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join(" | ");
        Ok(format!(
            "moveuse audit FAIL rules={} actions={} unique_actions={} failures={} first={} report={}",
            rules.len(),
            action_count,
            unique_actions.len(),
            failures.len(),
            preview,
            report_path
        ))
    }

    pub fn player_viewport(&self, id: PlayerId) -> Option<crate::world::viewport::Viewport> {
        self.players.get(&id).map(|player| player.viewport)
    }

    pub fn tick_conditions(&mut self, now: GameTick) -> Vec<(PlayerId, Vec<ConditionTick>)> {
        let mut results = Vec::new();
        for (id, player) in self.players.iter_mut() {
            let ticks = player.tick_conditions(now);
            if !ticks.is_empty() {
                results.push((*id, ticks));
            }
        }
        results
    }

    pub fn tick_status_effects(&mut self, now: GameTick) -> CreatureStatusUpdates {
        if let Some(next) = self.next_status_effect_tick {
            if now < next {
                return CreatureStatusUpdates::default();
            }
        } else {
            return CreatureStatusUpdates::default();
        }
        let mut updates = CreatureStatusUpdates::default();
        let mut next_tick: Option<GameTick> = None;
        for (id, player) in self.players.iter_mut() {
            if let Some(effect) = player.outfit_effect {
                if effect.is_expired(now) {
                    player.current_outfit = effect.original;
                    player.outfit_effect = None;
                    updates.outfit_updates.push(CreatureOutfitUpdate {
                        id: id.0,
                        outfit: player.current_outfit,
                    });
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = player.speed_effect {
                if effect.is_expired(now) {
                    player.speed_effect = None;
                    updates.speed_updates.push(CreatureSpeedUpdate {
                        id: id.0,
                        speed: effect.original_speed,
                    });
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = player.light_effect {
                if effect.is_expired(now) {
                    player.light_effect = None;
                    updates.light_updates.push(CreatureLightUpdate {
                        id: id.0,
                        level: 0,
                        color: 0,
                    });
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = player.magic_shield_effect {
                if effect.is_expired(now) {
                    player.magic_shield_effect = None;
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = player.drunken_effect {
                if effect.is_expired(now) {
                    player.drunken_effect = None;
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = player.strength_effect {
                if effect.is_expired(now) {
                    player.strength_effect = None;
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
        }
        for (id, monster) in self.monsters.iter_mut() {
            if let Some(effect) = monster.outfit_effect {
                if effect.is_expired(now) {
                    monster.outfit = effect.original;
                    monster.outfit_effect = None;
                    updates.outfit_updates.push(CreatureOutfitUpdate {
                        id: id.0,
                        outfit: monster.outfit,
                    });
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = monster.speed_effect {
                if effect.is_expired(now) {
                    monster.speed = effect.original_speed;
                    monster.speed_effect = None;
                    updates.speed_updates.push(CreatureSpeedUpdate {
                        id: id.0,
                        speed: monster.speed,
                    });
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
            if let Some(effect) = monster.strength_effect {
                if effect.is_expired(now) {
                    monster.strength_effect = None;
                } else {
                    Self::note_next_status_effect_tick(&mut next_tick, effect.expires_at);
                }
            }
        }
        self.next_status_effect_tick = next_tick;
        updates
    }

    fn note_next_status_effect_tick(target: &mut Option<GameTick>, expires_at: GameTick) {
        match *target {
            Some(current) => {
                if expires_at < current {
                    *target = Some(expires_at);
                }
            }
            None => {
                *target = Some(expires_at);
            }
        }
    }

    fn note_status_effect_tick(&mut self, expires_at: GameTick) {
        Self::note_next_status_effect_tick(&mut self.next_status_effect_tick, expires_at);
    }

    pub fn tick_skill_timers(&mut self, clock: &GameClock) -> SkillTimerOutcome {
        let mut outcome = SkillTimerOutcome::default();
        let ticks_per_skill = Self::skill_timer_ticks_per_second(clock);
        let now = clock.now();
        let steps = match self.skill_tick_last {
            Some(last) => {
                let elapsed = now.0.saturating_sub(last.0);
                (elapsed / ticks_per_skill) as usize
            }
            None => 0,
        };
        if self.skill_tick_last.is_none() {
            self.skill_tick_last = Some(now);
        }
        if steps > 0 {
            let last = self.skill_tick_last.unwrap_or(now);
            self.skill_tick_last = Some(GameTick(
                last.0.saturating_add((steps as u64).saturating_mul(ticks_per_skill)),
            ));
        }

        if steps == 0 {
            self.sync_skill_effects(now, ticks_per_skill, &mut outcome);
            return outcome;
        }

        for _ in 0..steps {
            for (id, player) in self.players.iter_mut() {
                if player.raw_skills.is_empty() {
                    player.raw_skills = skill_rows_from_player(player);
                }
                let dead = player.stats.health == 0;
                let position = player.position;

                let fed_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_FED, player.profession);
                if Self::row_active(&player.raw_skills[fed_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[fed_index]) {
                        let in_pz = self
                            .map
                            .tile(position)
                            .map(|tile| tile.protection_zone)
                            .unwrap_or(false);
                        if !dead && !in_pz {
                            let timer = player.raw_skills[fed_index].values[SKILL_FIELD_CYCLE];
                            let (hp_interval, mana_interval) = food_regen_intervals(player.profession);
                            let hp_secs = hp_interval.as_secs().max(1);
                            let mana_secs = mana_interval.as_secs().max(1);
                            if timer % hp_secs as i32 == 0 {
                                let applied = player.stats.apply_heal(1);
                                if applied > 0 {
                                    outcome.health_updates.push(*id);
                                    outcome.data_updates.push(*id);
                                }
                            }
                            if timer % mana_secs as i32 == 0 {
                                let before = player.stats.mana;
                                let max = player.stats.max_mana;
                                let new_mana = before.saturating_add(2).min(max);
                                player.stats.mana = new_mana;
                                if new_mana > before {
                                    outcome.data_updates.push(*id);
                                }
                            }
                        }
                    }
                    if player.raw_skills[fed_index].values[SKILL_FIELD_CYCLE] == 0 {
                        Self::clear_skill_timer(&mut player.raw_skills[fed_index]);
                    }
                }

                let soul_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_SOUL, player.profession);
                if Self::row_active(&player.raw_skills[soul_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[soul_index]) {
                        if !dead {
                            let max_soul = Stats::base_for_profession(player.profession).soul;
                            let before = player.stats.soul;
                            if before < max_soul {
                                player.stats.soul = before.saturating_add(1).min(max_soul);
                                if player.stats.soul > before {
                                    outcome.data_updates.push(*id);
                                }
                            }
                        }
                    }
                    if player.raw_skills[soul_index].values[SKILL_FIELD_CYCLE] == 0 {
                        Self::clear_skill_timer(&mut player.raw_skills[soul_index]);
                    }
                }

                let poison_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_POISON, player.profession);
                if Self::row_active(&player.raw_skills[poison_index]) {
                    if let Some(range) = Self::process_poison_timer(&mut player.raw_skills[poison_index]) {
                        let damage = range.unsigned_abs();
                        if damage > 0 && !dead {
                            let (applied, absorbed) =
                                player.apply_damage_with_magic_shield(DamageType::Earth, damage);
                            if applied > 0 {
                                outcome.health_updates.push(*id);
                            }
                            if absorbed > 0 {
                                outcome.data_updates.push(*id);
                            }
                        }
                    }
                    if player.raw_skills[poison_index].values[SKILL_FIELD_CYCLE] == 0 {
                        Self::clear_skill_timer(&mut player.raw_skills[poison_index]);
                    }
                }

                let burning_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_BURNING, player.profession);
                if Self::row_active(&player.raw_skills[burning_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[burning_index]) {
                        if !dead {
                            let (applied, absorbed) =
                                player.apply_damage_with_magic_shield(DamageType::Fire, 10);
                            if applied > 0 {
                                outcome.health_updates.push(*id);
                            }
                            if absorbed > 0 {
                                outcome.data_updates.push(*id);
                            }
                        }
                    }
                    if player.raw_skills[burning_index].values[SKILL_FIELD_CYCLE] == 0 {
                        Self::clear_skill_timer(&mut player.raw_skills[burning_index]);
                    }
                }

                let energy_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_ENERGY, player.profession);
                if Self::row_active(&player.raw_skills[energy_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[energy_index]) {
                        if !dead {
                            let (applied, absorbed) =
                                player.apply_damage_with_magic_shield(DamageType::Energy, 25);
                            if applied > 0 {
                                outcome.health_updates.push(*id);
                            }
                            if absorbed > 0 {
                                outcome.data_updates.push(*id);
                            }
                        }
                    }
                    if player.raw_skills[energy_index].values[SKILL_FIELD_CYCLE] == 0 {
                        Self::clear_skill_timer(&mut player.raw_skills[energy_index]);
                    }
                }

                let drunken_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_DRUNKEN, player.profession);
                if Self::row_active(&player.raw_skills[drunken_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[drunken_index]) {
                        if player.raw_skills[drunken_index].values[SKILL_FIELD_CYCLE] == 0 {
                            Self::clear_skill_timer(&mut player.raw_skills[drunken_index]);
                        }
                    }
                }

                let light_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_LIGHT, player.profession);
                if Self::row_active(&player.raw_skills[light_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[light_index]) {
                        if player.raw_skills[light_index].values[SKILL_FIELD_CYCLE] == 0 {
                            Self::clear_skill_timer(&mut player.raw_skills[light_index]);
                        }
                    }
                }

                let illusion_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_ILLUSION, player.profession);
                if Self::row_active(&player.raw_skills[illusion_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[illusion_index]) {
                        if player.raw_skills[illusion_index].values[SKILL_FIELD_CYCLE] == 0 {
                            Self::clear_skill_timer(&mut player.raw_skills[illusion_index]);
                        }
                    }
                }

                let shield_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_MANASHIELD, player.profession);
                if Self::row_active(&player.raw_skills[shield_index]) {
                    if let Some(_) = Self::process_base_timer(&mut player.raw_skills[shield_index]) {
                        if player.raw_skills[shield_index].values[SKILL_FIELD_CYCLE] == 0 {
                            Self::clear_skill_timer(&mut player.raw_skills[shield_index]);
                        }
                    }
                }
            }
        }

        self.sync_skill_effects(now, ticks_per_skill, &mut outcome);
        outcome
    }

    pub fn tick_food_regen(&mut self, clock: &GameClock) -> FoodRegenOutcome {
        let now = clock.now();
        let mut outcome = FoodRegenOutcome::default();
        for (id, player) in self.players.iter_mut() {
            let Some(expires_at) = player.food_expires_at else {
                continue;
            };
            if now >= expires_at {
                player.food_expires_at = None;
                continue;
            }
            if player.stats.health == 0 {
                continue;
            }
            if let Some(tile) = self.map.tile(player.position) {
                if tile.protection_zone {
                    continue;
                }
            }

            let (hp_interval, mana_interval) = food_regen_intervals(player.profession);
            let mut updated = false;
            let mut health_updated = false;

            if player.food_hp_cooldown.is_ready(clock) {
                player
                    .food_hp_cooldown
                    .reset_from_now_duration(clock, hp_interval);
                if player.stats.health < player.stats.max_health {
                    let applied = player.stats.apply_heal(1);
                    if applied > 0 {
                        updated = true;
                        health_updated = true;
                    }
                }
            }

            if player.food_mana_cooldown.is_ready(clock) {
                player
                    .food_mana_cooldown
                    .reset_from_now_duration(clock, mana_interval);
                if player.stats.mana < player.stats.max_mana {
                    let before = player.stats.mana;
                    let max = player.stats.max_mana;
                    let new_mana = before.saturating_add(2).min(max);
                    player.stats.mana = new_mana;
                    if new_mana > before {
                        updated = true;
                    }
                }
            }

            if updated {
                outcome.data_updates.push(*id);
            }
            if health_updated {
                outcome.health_updates.push(*id);
            }
        }
        outcome
    }

    fn skill_timer_ticks_per_second(clock: &GameClock) -> u64 {
        clock
            .ticks_from_duration_round_up(Duration::from_secs(1))
            .max(1)
    }

    fn skill_timer_seconds_from_ticks(duration_ticks: u64, clock: &GameClock) -> i32 {
        let per = Self::skill_timer_ticks_per_second(clock);
        if per == 0 {
            return 0;
        }
        let seconds = (duration_ticks + per - 1) / per;
        seconds.min(i32::MAX as u64) as i32
    }

    fn skill_timer_remaining_seconds(cycle: i32, count: i32, max_count: i32) -> i32 {
        if cycle <= 0 {
            return 0;
        }
        if max_count <= 0 {
            return cycle;
        }
        let count = if count <= 0 { max_count } else { count };
        let cycle = cycle.saturating_sub(1);
        cycle.saturating_mul(max_count).saturating_add(count)
    }

    fn ensure_skill_row(rows: &mut Vec<SkillRow>, skill_id: u32, profession: u8) -> usize {
        if let Some(index) = rows.iter().position(|row| row.skill_id == skill_id) {
            if skill_id == SKILL_FED && rows[index].values[SKILL_FIELD_MAX] == i32::MAX {
                rows[index].values[SKILL_FIELD_MAX] = FOOD_MAX_SECONDS.min(i32::MAX as u64) as i32;
            }
            if skill_id == SKILL_SOUL && rows[index].values[SKILL_FIELD_MAX] == i32::MAX {
                rows[index].values[SKILL_FIELD_MAX] = Stats::base_for_profession(profession).soul as i32;
            }
            return index;
        }
        let mut values = default_skill_row_values();
        if skill_id == SKILL_FED {
            values[SKILL_FIELD_MAX] = FOOD_MAX_SECONDS.min(i32::MAX as u64) as i32;
        }
        if skill_id == SKILL_SOUL {
            values[SKILL_FIELD_MAX] = Stats::base_for_profession(profession).soul as i32;
        }
        rows.push(SkillRow::new(skill_id, values));
        rows.len() - 1
    }

    fn grant_learning_points(player: &mut PlayerState) {
        player.learning_points = SKILL_TRAINING_POINTS;
    }

    fn consume_learning_point(player: &mut PlayerState) -> bool {
        if player.learning_points == 0 {
            return false;
        }
        player.learning_points = player.learning_points.saturating_sub(1);
        true
    }

    fn set_player_skill_level(player: &mut PlayerState, skill: SkillType, level: u16, progress: u8) {
        let entry = SkillLevel { level, progress };
        match skill {
            SkillType::Fist => player.skills.fist = entry,
            SkillType::Club => player.skills.club = entry,
            SkillType::Sword => player.skills.sword = entry,
            SkillType::Axe => player.skills.axe = entry,
            SkillType::Distance => player.skills.distance = entry,
            SkillType::Shielding => player.skills.shielding = entry,
            SkillType::Fishing => player.skills.fishing = entry,
            SkillType::Magic => player.skills.magic = entry,
        }
    }

    fn skill_advance_message(skill: SkillType, level: u16) -> Option<String> {
        let text = match skill {
            SkillType::Magic => format!("You advanced to magic level {}.", level),
            SkillType::Shielding => "You advanced in shielding.".to_string(),
            SkillType::Distance => "You advanced in distance fighting.".to_string(),
            SkillType::Sword => "You advanced in sword fighting.".to_string(),
            SkillType::Club => "You advanced in club fighting.".to_string(),
            SkillType::Axe => "You advanced in axe fighting.".to_string(),
            SkillType::Fist => "You advanced in fist fighting.".to_string(),
            SkillType::Fishing => "You advanced in fishing.".to_string(),
        };
        Some(text)
    }

    fn train_skill_from_row(row: &mut SkillRow, amount: i32) -> (i32, i32) {
        let mut level = row.values[SKILL_FIELD_ACT];
        if level < 1 {
            level = 1;
        }
        let mut exp_base = row.values[SKILL_FIELD_DELTA];
        if exp_base <= 0 || exp_base == i32::MAX {
            exp_base = 10;
            row.values[SKILL_FIELD_DELTA] = exp_base;
        }
        let exp_level = skill_exp_for_level(level, exp_base).unwrap_or(0);
        let mut exp_current = row.values[SKILL_FIELD_EXP] as i64;
        if exp_current < exp_level {
            exp_current = exp_level;
        }
        let exp_next = skill_exp_for_level(level.saturating_add(1), exp_base).unwrap_or(exp_level + 1);
        row.values[SKILL_FIELD_NEXT_LEVEL] = exp_next.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        exp_current = exp_current.saturating_add(i64::from(amount.max(0)));
        let mut next_level = exp_next;
        let mut new_level = level;
        while exp_current >= next_level {
            new_level = new_level.saturating_add(1);
            let exp_level_now = next_level;
            next_level = skill_exp_for_level(new_level.saturating_add(1), exp_base)
                .unwrap_or(exp_level_now + 1);
            if next_level <= exp_level_now {
                break;
            }
        }
        row.values[SKILL_FIELD_ACT] = new_level;
        row.values[SKILL_FIELD_EXP] = exp_current.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        row.values[SKILL_FIELD_NEXT_LEVEL] = next_level.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        (level, new_level)
    }

    fn train_player_skill(&mut self, player_id: PlayerId, skill: SkillType) {
        let Some(player) = self.players.get_mut(&player_id) else {
            return;
        };
        if !Self::consume_learning_point(player) {
            return;
        }
        if player.raw_skills.is_empty() {
            player.raw_skills = skill_rows_from_player(player);
        }
        let skill_id = skill_id_for_type(skill);
        let index = Self::ensure_skill_row(&mut player.raw_skills, skill_id, player.profession);
        let row = &mut player.raw_skills[index];
        let old_level = row.values[SKILL_FIELD_ACT];
        let (_prev_level, new_level) = Self::train_skill_from_row(row, 1);
        let progress = skill_progress_from_values(&row.values);
        let level_u16 = new_level.clamp(0, u16::MAX as i32) as u16;
        Self::set_player_skill_level(player, skill, level_u16, progress);

        if new_level != old_level {
            let message = Self::skill_advance_message(skill, level_u16);
            if let Some(text) = message {
                self.queue_player_message(player_id, MESSAGE_EVENT, text);
            }
        }
        if skill == SkillType::Magic {
            self.queue_player_data_update(player_id);
        } else {
            self.queue_player_skills_update(player_id);
        }
    }

    fn clear_skill_timer(row: &mut SkillRow) {
        row.values[SKILL_FIELD_CYCLE] = 0;
        row.values[SKILL_FIELD_COUNT] = 0;
        row.values[SKILL_FIELD_MAX_COUNT] = 0;
    }

    fn set_skill_timer(row: &mut SkillRow, cycle: i32, count: i32, max_count: i32) {
        row.values[SKILL_FIELD_CYCLE] = cycle;
        row.values[SKILL_FIELD_COUNT] = count;
        row.values[SKILL_FIELD_MAX_COUNT] = max_count;
    }

    fn set_poison_timer(row: &mut SkillRow, cycle: i32, count: i32, max_count: i32, factor: i32) {
        Self::set_skill_timer(row, cycle, count, max_count);
        let factor = factor.clamp(10, 1000);
        row.values[SKILL_FIELD_FACTOR_PERCENT] = factor;
    }

    fn row_active(row: &SkillRow) -> bool {
        row.values[SKILL_FIELD_MIN] != i32::MIN
    }

    fn condition_skill_id(kind: ConditionKind) -> Option<u32> {
        match kind {
            ConditionKind::Poison => Some(SKILL_POISON),
            ConditionKind::Fire => Some(SKILL_BURNING),
            ConditionKind::Energy => Some(SKILL_ENERGY),
            _ => None,
        }
    }

    fn apply_condition_skill_timer(player: &mut PlayerState, kind: ConditionKind, damage: u32) {
        if damage == 0 {
            return;
        }
        if player.raw_skills.is_empty() {
            player.raw_skills = skill_rows_from_player(player);
        }
        match kind {
            ConditionKind::Poison => {
                let index =
                    Self::ensure_skill_row(&mut player.raw_skills, SKILL_POISON, player.profession);
                let current = player.raw_skills[index].values[SKILL_FIELD_CYCLE];
                let cycle = damage.min(i32::MAX as u32) as i32;
                if cycle > current {
                    Self::set_poison_timer(&mut player.raw_skills[index], cycle, 3, 3, 50);
                }
            }
            ConditionKind::Fire => {
                let index =
                    Self::ensure_skill_row(&mut player.raw_skills, SKILL_BURNING, player.profession);
                let cycle = (damage / 10).max(1).min(i32::MAX as u32) as i32;
                Self::set_skill_timer(&mut player.raw_skills[index], cycle, 8, 8);
            }
            ConditionKind::Energy => {
                let index =
                    Self::ensure_skill_row(&mut player.raw_skills, SKILL_ENERGY, player.profession);
                let cycle = (damage / 20).max(1).min(i32::MAX as u32) as i32;
                Self::set_skill_timer(&mut player.raw_skills[index], cycle, 10, 10);
            }
            _ => {}
        }
    }

    fn apply_soul_regen_on_experience(player: &mut PlayerState, amount: u32) {
        if amount < u32::from(player.level) {
            return;
        }
        let interval = if player.profession >= 11 {
            SOUL_REGEN_PROMO_INTERVAL_SECS
        } else {
            SOUL_REGEN_BASE_INTERVAL_SECS
        };
        if interval <= 0 {
            return;
        }
        if player.raw_skills.is_empty() {
            player.raw_skills = skill_rows_from_player(player);
        }
        let index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_SOUL, player.profession);
        let cycle = player.raw_skills[index].values[SKILL_FIELD_CYCLE];
        let count = player.raw_skills[index].values[SKILL_FIELD_COUNT];
        let max_count = player.raw_skills[index].values[SKILL_FIELD_MAX_COUNT];
        let timer_value = if cycle <= 0 {
            0
        } else if max_count > 0 {
            let count = count.max(0);
            cycle.saturating_sub(1)
                .saturating_mul(max_count)
                .saturating_add(count)
        } else {
            cycle
        };
        let mut next_count = timer_value % interval;
        if next_count == 0 {
            next_count = interval;
        }
        let cycle = 240 / interval;
        Self::set_skill_timer(&mut player.raw_skills[index], cycle, next_count, interval);
    }

    fn process_base_timer(row: &mut SkillRow) -> Option<i32> {
        let cycle = row.values[SKILL_FIELD_CYCLE];
        if cycle == 0 {
            return None;
        }
        let count = row.values[SKILL_FIELD_COUNT];
        let max_count = row.values[SKILL_FIELD_MAX_COUNT];
        if count <= 0 {
            row.values[SKILL_FIELD_COUNT] = max_count;
            let range = if cycle < 0 { 1 } else { -1 };
            row.values[SKILL_FIELD_CYCLE] = cycle.saturating_add(range);
            Some(range)
        } else {
            row.values[SKILL_FIELD_COUNT] = count.saturating_sub(1);
            None
        }
    }

    fn process_poison_timer(row: &mut SkillRow) -> Option<i32> {
        let cycle = row.values[SKILL_FIELD_CYCLE];
        if cycle == 0 {
            return None;
        }
        let count = row.values[SKILL_FIELD_COUNT];
        let max_count = row.values[SKILL_FIELD_MAX_COUNT];
        if count <= 0 {
            row.values[SKILL_FIELD_COUNT] = max_count;
            let factor = row.values[SKILL_FIELD_FACTOR_PERCENT].clamp(10, 1000);
            let mut range = cycle.saturating_mul(factor) / 1000;
            if range == 0 {
                range = if cycle > 0 { 1 } else { -1 };
            }
            row.values[SKILL_FIELD_CYCLE] = cycle.saturating_sub(range);
            Some(range)
        } else {
            row.values[SKILL_FIELD_COUNT] = count.saturating_sub(1);
            None
        }
    }

    fn sync_skill_effects(
        &mut self,
        now: GameTick,
        ticks_per_skill: u64,
        outcome: &mut SkillTimerOutcome,
    ) {
        let mut next_effect_tick: Option<GameTick> = None;
        for (id, player) in self.players.iter_mut() {
            if player.raw_skills.is_empty() {
                player.raw_skills = skill_rows_from_player(player);
            }

            let fed_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_FED, player.profession);
            if Self::row_active(&player.raw_skills[fed_index]) {
                let cycle = player.raw_skills[fed_index].values[SKILL_FIELD_CYCLE];
                let count = player.raw_skills[fed_index].values[SKILL_FIELD_COUNT];
                let max_count = player.raw_skills[fed_index].values[SKILL_FIELD_MAX_COUNT];
                let remaining = Self::skill_timer_remaining_seconds(cycle, count, max_count);
                if remaining > 0 {
                    let ticks = (remaining as u64).saturating_mul(ticks_per_skill);
                    player.food_expires_at = Some(GameTick(now.0.saturating_add(ticks)));
                } else {
                    player.food_expires_at = None;
                }
            }

            let light_index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_LIGHT, player.profession);
            if Self::row_active(&player.raw_skills[light_index]) {
                let cycle = player.raw_skills[light_index].values[SKILL_FIELD_CYCLE];
                let count = player.raw_skills[light_index].values[SKILL_FIELD_COUNT];
                let max_count = player.raw_skills[light_index].values[SKILL_FIELD_MAX_COUNT];
                let remaining = Self::skill_timer_remaining_seconds(cycle, count, max_count);
                if cycle > 0 && remaining > 0 {
                    let ticks = (remaining as u64).saturating_mul(ticks_per_skill);
                    let level = cycle.clamp(0, i32::from(u8::MAX)) as u8;
                    let color = player
                        .light_effect
                        .map(|effect| effect.color)
                        .unwrap_or(DEFAULT_SKILL_LIGHT_COLOR);
                    let current = player.light_effect.map(|effect| (effect.level, effect.color));
                    player.light_effect = Some(LightEffect {
                        level,
                        color,
                        expires_at: GameTick(now.0.saturating_add(ticks)),
                    });
                    Self::note_next_status_effect_tick(
                        &mut next_effect_tick,
                        GameTick(now.0.saturating_add(ticks)),
                    );
                    if current != Some((level, color)) {
                        outcome.status_updates.light_updates.push(CreatureLightUpdate {
                            id: id.0,
                            level,
                            color,
                        });
                    }
                } else if player.light_effect.is_some() {
                    player.light_effect = None;
                    outcome.status_updates.light_updates.push(CreatureLightUpdate {
                        id: id.0,
                        level: 0,
                        color: 0,
                    });
                }
            }

            let illusion_index =
                Self::ensure_skill_row(&mut player.raw_skills, SKILL_ILLUSION, player.profession);
            if Self::row_active(&player.raw_skills[illusion_index]) {
                let cycle = player.raw_skills[illusion_index].values[SKILL_FIELD_CYCLE];
                let count = player.raw_skills[illusion_index].values[SKILL_FIELD_COUNT];
                let max_count = player.raw_skills[illusion_index].values[SKILL_FIELD_MAX_COUNT];
                let act = player.raw_skills[illusion_index].values[SKILL_FIELD_ACT];
                let remaining = Self::skill_timer_remaining_seconds(cycle, count, max_count);
                if cycle > 0 && remaining > 0 {
                    let ticks = (remaining as u64).saturating_mul(ticks_per_skill);
                    let original = player.original_outfit;
                    let outfit = player.current_outfit;
                    player.outfit_effect = Some(OutfitEffect {
                        outfit,
                        original,
                        expires_at: GameTick(now.0.saturating_add(ticks)),
                    });
                    Self::note_next_status_effect_tick(
                        &mut next_effect_tick,
                        GameTick(now.0.saturating_add(ticks)),
                    );
                } else if act <= 0 {
                    if let Some(effect) = player.outfit_effect {
                        player.current_outfit = effect.original;
                        player.outfit_effect = None;
                        outcome.status_updates.outfit_updates.push(CreatureOutfitUpdate {
                            id: id.0,
                            outfit: player.current_outfit,
                        });
                    }
                }
            }

            let shield_index =
                Self::ensure_skill_row(&mut player.raw_skills, SKILL_MANASHIELD, player.profession);
            if Self::row_active(&player.raw_skills[shield_index]) {
                let cycle = player.raw_skills[shield_index].values[SKILL_FIELD_CYCLE];
                let count = player.raw_skills[shield_index].values[SKILL_FIELD_COUNT];
                let max_count = player.raw_skills[shield_index].values[SKILL_FIELD_MAX_COUNT];
                let remaining = Self::skill_timer_remaining_seconds(cycle, count, max_count);
                if cycle > 0 && remaining > 0 {
                    let ticks = (remaining as u64).saturating_mul(ticks_per_skill);
                    player.magic_shield_effect = Some(MagicShieldEffect {
                        expires_at: GameTick(now.0.saturating_add(ticks)),
                    });
                    Self::note_next_status_effect_tick(
                        &mut next_effect_tick,
                        GameTick(now.0.saturating_add(ticks)),
                    );
                } else {
                    player.magic_shield_effect = None;
                }
            }

            let drunken_index =
                Self::ensure_skill_row(&mut player.raw_skills, SKILL_DRUNKEN, player.profession);
            if Self::row_active(&player.raw_skills[drunken_index]) {
                let cycle = player.raw_skills[drunken_index].values[SKILL_FIELD_CYCLE];
                let count = player.raw_skills[drunken_index].values[SKILL_FIELD_COUNT];
                let max_count = player.raw_skills[drunken_index].values[SKILL_FIELD_MAX_COUNT];
                let remaining = Self::skill_timer_remaining_seconds(cycle, count, max_count);
                if cycle > 0 && remaining > 0 {
                    let ticks = (remaining as u64).saturating_mul(ticks_per_skill);
                    let intensity = cycle.clamp(0, i32::from(u8::MAX)) as u8;
                    player.drunken_effect = Some(DrunkenEffect {
                        intensity,
                        expires_at: GameTick(now.0.saturating_add(ticks)),
                    });
                    Self::note_next_status_effect_tick(
                        &mut next_effect_tick,
                        GameTick(now.0.saturating_add(ticks)),
                    );
                } else {
                    player.drunken_effect = None;
                }
            }
        }
        if let Some(next) = next_effect_tick {
            Self::note_next_status_effect_tick(&mut self.next_status_effect_tick, next);
        }
    }

    pub fn tick_monsters(&mut self, clock: &GameClock) -> MonsterTickOutcome {
        let monster_ids = self.collect_active_monster_ids(MONSTER_ACTIVE_RANGE);
        let mut outcome = MonsterTickOutcome::default();
        let mut player_hits = HashSet::new();
        let mut player_hit_marks = HashSet::new();
        let mut monster_updates = HashSet::new();

        for monster_id in monster_ids {
            let (
                position,
                health,
                flee_threshold,
                lose_target_distance,
                current_target,
                move_ready,
                combat_ready,
                speed,
                attack,
                poison,
                strategy,
                talk_ready,
                flags,
                summoner,
            ) = match self.monsters.get(&monster_id) {
                Some(monster) => (
                    monster.position,
                    monster.stats.health,
                    monster.flee_threshold,
                    monster.lose_target_distance,
                    monster.target,
                    monster.move_cooldown.is_ready(clock),
                    monster.combat_cooldown.is_ready(clock),
                    monster.speed,
                    Self::monster_attack_value(monster, clock.now()),
                    monster.poison,
                    monster.strategy,
                    monster.talk_cooldown.is_ready(clock),
                    monster.flags,
                    monster.summoner,
                ),
                None => continue,
            };

            if summoner.is_some() {
                continue;
            }

            if !self.has_visible_player_in_range(position, MONSTER_ACTIVE_RANGE, flags) {
                continue;
            }

            let range = Self::monster_acquire_range(lose_target_distance);
            let target = if range == 0 {
                None
            } else {
                let damage_by = match self.monsters.get_mut(&monster_id) {
                    Some(monster) => std::mem::take(&mut monster.damage_by),
                    None => continue,
                };
                let target = self.select_monster_target(
                    position,
                    current_target,
                    range,
                    strategy,
                    &damage_by,
                    flags,
                );
                if let Some(monster) = self.monsters.get_mut(&monster_id) {
                    monster.damage_by = damage_by;
                }
                target
            };
            let target_position = target.and_then(|id| self.players.get(&id).map(|p| p.position));

            if let Some(monster) = self.monsters.get_mut(&monster_id) {
                monster.target = target;
            }

            if move_ready {
                let distance_flee = flags.distance_fighting
                    && target_position
                        .map(|pos| Self::monster_tile_distance(position, pos) <= MONSTER_MELEE_RANGE)
                        .unwrap_or(false);
                let flee = (flee_threshold > 0 && health <= flee_threshold) || distance_flee;
                let preferred = target_position.and_then(|target_pos| {
                    let mut preferred_target = target_pos;
                    if !flee {
                        let dx = i32::from(target_pos.x) - i32::from(position.x);
                        let dy = i32::from(target_pos.y) - i32::from(position.y);
                        if dx.abs() <= 1 && dy.abs() <= 1 {
                            let roll = self.monster_rng.roll_range(0, 4);
                            let delta = match roll {
                                0 => PositionDelta { dx: 1, dy: 0, dz: 0 },
                                1 => PositionDelta { dx: -1, dy: 0, dz: 0 },
                                2 => PositionDelta { dx: 0, dy: 1, dz: 0 },
                                3 => PositionDelta { dx: 0, dy: -1, dz: 0 },
                                _ => PositionDelta { dx: 0, dy: 0, dz: 0 },
                            };
                            if let Some(jittered) = target_pos.offset(delta) {
                                preferred_target = jittered;
                            }
                        }
                    }
                    Self::monster_direction(position, preferred_target, flee)
                });
                let mut directions = Vec::with_capacity(MONSTER_MOVE_ATTEMPTS);
                if let Some(direction) = preferred {
                    directions.push(direction);
                    if let Some((first, second)) = Self::direction_components(direction) {
                        directions.push(first);
                        directions.push(second);
                    }
                }
                while directions.len() < MONSTER_MOVE_ATTEMPTS {
                    directions.push(self.monster_rng.roll_direction());
                }
                if let Some(direction) = directions.first().copied() {
                    if let Some(monster) = self.monsters.get_mut(&monster_id) {
                        monster.direction = direction;
                    }
                }
                let mut moved: Option<(Position, Direction)> = None;
                for direction in directions.into_iter().take(MONSTER_MOVE_ATTEMPTS) {
                    if let Ok(destination) = self.move_monster(monster_id, direction) {
                        outcome.moved += 1;
                        moved = Some((destination, direction));
                        break;
                    }
                }
                let cooldown_ticks = if let Some((destination, direction)) = moved {
                    outcome.moves.push(CreatureStep {
                        id: monster_id,
                        from: position,
                        to: destination,
                    });
                    let tile_speed = self
                        .map
                        .tile(destination)
                        .map(|tile| self.tile_ground_speed(tile))
                        .unwrap_or(DEFAULT_GROUND_SPEED);
                    self.movement_cooldown_ticks(tile_speed, speed, direction, clock)
                } else {
                    MONSTER_MOVE_INTERVAL_TICKS
                };
                if let Some(monster) = self.monsters.get_mut(&monster_id) {
                    monster
                        .move_cooldown
                        .reset_from_now_ticks(clock, cooldown_ticks);
                }
            }

            let current_position = self
                .monsters
                .get(&monster_id)
                .map(|monster| monster.position)
                .unwrap_or(position);

            if combat_ready {
                let mut action_performed = false;
                if let Some(target_id) = target {
                    if let Some(target_pos) = target_position {
                        let has_spells = self
                            .monsters
                            .get(&monster_id)
                            .map(|monster| !monster.spells.is_empty())
                            .unwrap_or(false);
                        if has_spells {
                            let spells = match self.monsters.get_mut(&monster_id) {
                                Some(monster) => std::mem::take(&mut monster.spells),
                                None => continue,
                            };
                            action_performed = self.monster_try_cast_spell(
                                monster_id,
                                current_position,
                                target_pos,
                                &spells,
                                clock,
                                &mut outcome.effects,
                                &mut outcome.missiles,
                                &mut player_hits,
                                &mut player_hit_marks,
                                &mut monster_updates,
                                &mut outcome.outfit_updates,
                                &mut outcome.speed_updates,
                                &mut outcome.refresh_map,
                                flags,
                            );
                            if let Some(monster) = self.monsters.get_mut(&monster_id) {
                                monster.spells = spells;
                            }
                        }
                        if !action_performed
                            && Self::monster_tile_distance(current_position, target_pos)
                                <= MONSTER_MELEE_RANGE
                        {
                            action_performed = self.monster_melee_attack(
                                monster_id,
                                target_id,
                                attack,
                                poison,
                                clock,
                                &mut player_hits,
                                &mut player_hit_marks,
                                &mut monster_updates,
                            );
                        }
                    }
                }
                if action_performed {
                    if let Some(monster) = self.monsters.get_mut(&monster_id) {
                        monster.combat_cooldown.reset_from_now_ticks(
                            clock,
                            MONSTER_COMBAT_INTERVAL_TICKS,
                        );
                    }
                }
            }

            if talk_ready {
                let (talk_lines, monster_name) = match self.monsters.get_mut(&monster_id) {
                    Some(monster) => {
                        (std::mem::take(&mut monster.talk_lines), monster.name.clone())
                    }
                    None => continue,
                };
                if !talk_lines.is_empty() {
                    let delay_ticks = self.monster_talk_delay_ticks();
                    if let Some(monster) = self.monsters.get_mut(&monster_id) {
                        monster
                            .talk_cooldown
                            .reset_from_now_ticks(clock, delay_ticks);
                    }
                    if let Some(talk) =
                        Self::monster_pick_talk_line(&mut self.monster_rng, &talk_lines)
                    {
                        let (talk_type, message) = Self::monster_parse_talk_line(&talk);
                        outcome.talks.push(MonsterTalk {
                            monster_id,
                            name: monster_name,
                            position,
                            talk_type,
                            message,
                        });
                    }
                }
                if let Some(monster) = self.monsters.get_mut(&monster_id) {
                    monster.talk_lines = talk_lines;
                }
            }
        }

        outcome.player_hits = player_hits.into_iter().collect();
        outcome.player_hit_marks = player_hit_marks.into_iter().collect();
        outcome.monster_updates = monster_updates.into_iter().collect();
        outcome
    }

    fn collect_active_monster_ids(&mut self, radius: u16) -> Vec<CreatureId> {
        self.ensure_monster_sector_index();
        if self.players.is_empty() || self.monsters.is_empty() {
            return Vec::new();
        }
        let mut active = Vec::new();
        for player in self.players.values() {
            let sectors = self.sectors_in_range(player.position, radius);
            for sector in sectors {
                if let Some(ids) = self.monster_sector_index.get(&sector) {
                    active.extend(ids.iter().copied());
                }
            }
        }
        active.sort_unstable_by_key(|id| id.0);
        active.dedup();
        active
    }

    fn sectors_in_range(&self, position: Position, radius: u16) -> Vec<SectorCoord> {
        let min_x = position.x.saturating_sub(radius);
        let max_x = position.x.saturating_add(radius);
        let min_y = position.y.saturating_sub(radius);
        let max_y = position.y.saturating_add(radius);
        let min_sector_x = min_x / SECTOR_TILE_SIZE;
        let max_sector_x = max_x / SECTOR_TILE_SIZE;
        let min_sector_y = min_y / SECTOR_TILE_SIZE;
        let max_sector_y = max_y / SECTOR_TILE_SIZE;
        let mut sectors = Vec::new();
        for sx in min_sector_x..=max_sector_x {
            for sy in min_sector_y..=max_sector_y {
                sectors.push(SectorCoord {
                    x: sx,
                    y: sy,
                    z: position.z,
                });
            }
        }
        sectors
    }

    fn ensure_monster_sector_index(&mut self) {
        if self.monster_sector_index_count != self.monsters.len() {
            self.rebuild_monster_sector_index();
        }
    }

    fn rebuild_monster_sector_index(&mut self) {
        self.monster_sector_index.clear();
        self.monster_sector_index_count = 0;
        let monsters: Vec<(CreatureId, Position)> = self
            .monsters
            .iter()
            .map(|(id, monster)| (*id, monster.position))
            .collect();
        for (id, position) in monsters {
            self.add_monster_to_sector_index(id, position);
        }
    }

    fn add_monster_to_sector_index(&mut self, id: CreatureId, position: Position) {
        let sector = self.map.sector_for_position(position);
        self.monster_sector_index
            .entry(sector)
            .or_default()
            .push(id);
        self.monster_sector_index_count = self.monster_sector_index_count.saturating_add(1);
    }

    fn remove_monster_from_sector_index(&mut self, id: CreatureId, position: Position) {
        let sector = self.map.sector_for_position(position);
        if let Some(ids) = self.monster_sector_index.get_mut(&sector) {
            if let Some(index) = ids.iter().position(|entry| *entry == id) {
                ids.swap_remove(index);
                self.monster_sector_index_count =
                    self.monster_sector_index_count.saturating_sub(1);
            }
            if ids.is_empty() {
                self.monster_sector_index.remove(&sector);
            }
        }
    }

    fn update_monster_sector_index(
        &mut self,
        id: CreatureId,
        from: Position,
        to: Position,
    ) {
        let from_sector = self.map.sector_for_position(from);
        let to_sector = self.map.sector_for_position(to);
        if from_sector == to_sector {
            return;
        }
        self.remove_monster_from_sector_index(id, from);
        self.add_monster_to_sector_index(id, to);
    }

    pub fn tick_map_refresh(&mut self, clock: &GameClock) {
        let (mut cursor_x, mut cursor_y, bounds, cylinders) = {
            let Some(state) = self.refresh_state.as_mut() else {
                return;
            };
            if !state.cooldown.is_ready(clock) {
                return;
            }
            state.cooldown.reset_from_now_duration(clock, Duration::from_secs(REFRESH_INTERVAL_SECS));
            (state.next_x, state.next_y, state.bounds, state.cylinders_per_minute)
        };

        for _ in 0..cylinders {
            self.refresh_cylinder(cursor_x, cursor_y, bounds.min.z, bounds.max.z);
            advance_refresh_cursor_coords(&mut cursor_x, &mut cursor_y, bounds);
        }

        if let Some(state) = self.refresh_state.as_mut() {
            state.next_x = cursor_x;
            state.next_y = cursor_y;
        }
    }

    fn find_item_path_by_id(&mut self, id: ItemId) -> Option<ItemPath> {
        if self.item_index_dirty {
            self.rebuild_item_index();
        }
        self.item_index.get(&id).cloned()
    }

    fn rebuild_item_index(&mut self) {
        let mut index = HashMap::new();

        for (position, tile) in self.map.tiles.iter() {
            for (index_pos, item) in tile.items.iter().enumerate() {
                let root = ItemRoot::Tile {
                    position: *position,
                    index: index_pos,
                };
                let mut path = Vec::new();
                index_item_tree(&mut index, root, item, &mut path);
            }
        }

        for (player_id, player) in self.players.iter() {
            for slot in crate::entities::inventory::INVENTORY_SLOTS {
                if let Some(item) = player.inventory.slot(slot) {
                    let root = ItemRoot::Inventory {
                        player_id: *player_id,
                        slot,
                    };
                    let mut path = Vec::new();
                    index_item_tree(&mut index, root, item, &mut path);
                }

                if let Some(contents) = player.inventory_containers.get(&slot) {
                    for (index_pos, item) in contents.iter().enumerate() {
                        let root = ItemRoot::InventoryContainer {
                            player_id: *player_id,
                            slot,
                            index: index_pos,
                        };
                        let mut path = Vec::new();
                        index_item_tree(&mut index, root, item, &mut path);
                    }
                }
            }

            for (depot_id, items) in player.depots.iter() {
                for (index_pos, item) in items.iter().enumerate() {
                    let root = ItemRoot::Depot {
                        player_id: *player_id,
                        depot_id: *depot_id,
                        index: index_pos,
                    };
                    let mut path = Vec::new();
                    index_item_tree(&mut index, root, item, &mut path);
                }
            }
        }

        self.item_index = index;
        self.item_index_dirty = false;
    }

    fn ensure_item_index(&mut self) {
        if self.item_index_dirty {
            self.rebuild_item_index();
        }
    }

    fn remove_item_tree_from_index(&mut self, item: &ItemStack) {
        self.item_index.remove(&item.id);
        for child in &item.contents {
            self.remove_item_tree_from_index(child);
        }
    }

    fn index_item_tree_at_path(&mut self, root: ItemRoot, mut path: Vec<usize>, item: &ItemStack) {
        index_item_tree(&mut self.item_index, root, item, &mut path);
    }

    pub(crate) fn index_player_items(&mut self, player_id: PlayerId) {
        if self.item_index_dirty {
            self.rebuild_item_index();
            return;
        }
        let Some(player) = self.players.get(&player_id) else {
            return;
        };

        for slot in crate::entities::inventory::INVENTORY_SLOTS {
            if let Some(item) = player.inventory.slot(slot) {
                let root = ItemRoot::Inventory { player_id, slot };
                let mut path = Vec::new();
                index_item_tree(&mut self.item_index, root, item, &mut path);
            }

            if let Some(contents) = player.inventory_containers.get(&slot) {
                for (index_pos, item) in contents.iter().enumerate() {
                    let root = ItemRoot::InventoryContainer {
                        player_id,
                        slot,
                        index: index_pos,
                    };
                    let mut path = Vec::new();
                    index_item_tree(&mut self.item_index, root, item, &mut path);
                }
            }
        }

        for (depot_id, items) in player.depots.iter() {
            for (index_pos, item) in items.iter().enumerate() {
                let root = ItemRoot::Depot {
                    player_id,
                    depot_id: *depot_id,
                    index: index_pos,
                };
                let mut path = Vec::new();
                index_item_tree(&mut self.item_index, root, item, &mut path);
            }
        }
    }

    pub(crate) fn schedule_cron_for_player_items(&mut self, player_id: PlayerId) {
        let Some(player) = self.players.get(&player_id) else {
            return;
        };
        let mut items = Vec::new();
        for slot in crate::entities::inventory::INVENTORY_SLOTS {
            if let Some(item) = player.inventory.slot(slot) {
                items.push(item.clone());
            }
            if let Some(contents) = player.inventory_containers.get(&slot) {
                items.extend(contents.iter().cloned());
            }
        }
        for items_in_depot in player.depots.values() {
            items.extend(items_in_depot.iter().cloned());
        }
        for item in &items {
            self.schedule_cron_for_item_tree(item);
        }
    }

    fn item_by_path(&self, path: &ItemPath) -> Option<&ItemStack> {
        match &path.root {
            ItemRoot::Tile { position, index } => {
                let tile = self.map.tile(*position)?;
                item_from_items(&tile.items, *index, &path.path)
            }
            ItemRoot::Inventory { player_id, slot } => {
                let player = self.players.get(player_id)?;
                let item = player.inventory.slot(*slot)?;
                if path.path.is_empty() {
                    return Some(item);
                }
                let (first, rest) = path.path.split_first()?;
                item_from_items(&item.contents, *first, rest)
            }
            ItemRoot::InventoryContainer {
                player_id,
                slot,
                index,
            } => {
                let player = self.players.get(player_id)?;
                let contents = player.inventory_containers.get(slot)?;
                item_from_items(contents, *index, &path.path)
            }
            ItemRoot::Depot {
                player_id,
                depot_id,
                index,
            } => {
                let player = self.players.get(player_id)?;
                let items = player.depots.get(depot_id)?;
                item_from_items(items, *index, &path.path)
            }
        }
    }

    fn item_mut_by_path(&mut self, path: &ItemPath) -> Option<&mut ItemStack> {
        match &path.root {
            ItemRoot::Tile { position, index } => {
                let tile = self.map.tile_mut(*position)?;
                item_mut_from_items(&mut tile.items, *index, &path.path)
            }
            ItemRoot::Inventory { player_id, slot } => {
                let player = self.players.get_mut(player_id)?;
                let item = player.inventory.slot_mut(*slot)?;
                if path.path.is_empty() {
                    return Some(item);
                }
                let (first, rest) = path.path.split_first()?;
                item_mut_from_items(&mut item.contents, *first, rest)
            }
            ItemRoot::InventoryContainer {
                player_id,
                slot,
                index,
            } => {
                let player = self.players.get_mut(player_id)?;
                let contents = player.inventory_containers.get_mut(slot)?;
                item_mut_from_items(contents, *index, &path.path)
            }
            ItemRoot::Depot {
                player_id,
                depot_id,
                index,
            } => {
                let player = self.players.get_mut(player_id)?;
                let items = player.depots.get_mut(depot_id)?;
                item_mut_from_items(items, *index, &path.path)
            }
        }
    }

    pub fn tick_cron_system(&mut self, clock: &GameClock) -> usize {
        let ticks_per_second = clock.ticks_from_duration_round_up(Duration::from_secs(1));
        if ticks_per_second == 0 {
            return 0;
        }
        let now = clock.now();
        if let Some(last) = self.cron_tick_last {
            let delta = now.0.saturating_sub(last.0);
            self.cron_tick_accum = self.cron_tick_accum.saturating_add(delta);
        }
        self.cron_tick_last = Some(now);
        let elapsed_secs = self.cron_tick_accum / ticks_per_second;
        if elapsed_secs == 0 {
            return 0;
        }
        self.cron_tick_accum =
            self.cron_tick_accum.saturating_sub(elapsed_secs * ticks_per_second);
        let elapsed_secs = elapsed_secs.min(u64::from(u32::MAX)) as u32;

        let mut processed = 0usize;
        for _ in 0..elapsed_secs {
            self.cron_round = self.cron_round.saturating_add(1);
            if self.cron.check(self.cron_round).is_none() {
                continue;
            }
            processed = processed.saturating_add(self.process_cron_system());
        }

        processed
    }

    fn cron_expire_item(&mut self, item: &ItemStack, delay: i32) {
        let Some(item_types) = self.item_types.as_ref() else {
            return;
        };
        let Some(item_type) = item_types.get(item.type_id) else {
            return;
        };
        if !item_type.is_expiring {
            return;
        }
        let delay = if delay == -1 {
            item_type.expire_time_secs
        } else if delay < 0 {
            None
        } else {
            Some(delay as u32)
        };
        let Some(delay) = delay else {
            return;
        };
        self.cron.set(item.id, delay, self.cron_round);
    }

    fn schedule_cron_for_item_tree(&mut self, item: &ItemStack) {
        if let Some(item_types) = self.item_types.as_ref() {
            if let Some(item_type) = item_types.get(item.type_id) {
                if item_type.is_expiring {
                    self.cron_expire_item(item, -1);
                    if let Some(remaining) = item_remaining_expire_secs(item) {
                        let _ = self
                            .cron
                            .change(item.id, u32::from(remaining), self.cron_round);
                    }
                }
            }
        }
        for child in &item.contents {
            self.schedule_cron_for_item_tree(child);
        }
    }

    fn schedule_cron_for_world(&mut self) {
        let mut items = Vec::new();
        for tile in self.map.tiles.values() {
            items.extend(tile.items.iter().cloned());
        }
        for player in self.players.values() {
            for slot in crate::entities::inventory::INVENTORY_SLOTS {
                if let Some(item) = player.inventory.slot(slot) {
                    items.push(item.clone());
                }
                if let Some(contents) = player.inventory_containers.get(&slot) {
                    items.extend(contents.iter().cloned());
                }
            }
            for items_in_depot in player.depots.values() {
                items.extend(items_in_depot.iter().cloned());
            }
        }
        for item in &items {
            self.schedule_cron_for_item_tree(item);
        }
    }

    pub fn player_for_save(&self, player: &PlayerState) -> PlayerState {
        let mut clone = player.clone();
        self.apply_cron_remaining_to_player(&mut clone);
        clone
    }

    fn apply_cron_remaining_to_player(&self, player: &mut PlayerState) {
        for slot in crate::entities::inventory::INVENTORY_SLOTS {
            if let Some(item) = player.inventory.slot_mut(slot) {
                self.apply_cron_remaining_to_item(item);
            }
        }

        for items in player.inventory_containers.values_mut() {
            for item in items {
                self.apply_cron_remaining_to_item(item);
            }
        }

        for items in player.depots.values_mut() {
            for item in items {
                self.apply_cron_remaining_to_item(item);
            }
        }
    }

    fn apply_cron_remaining_to_item(&self, item: &mut ItemStack) {
        if let Some(remaining) = self.cron.get_remaining(item.id, self.cron_round) {
            set_remaining_expire_secs(item, remaining);
        }
        for child in &mut item.contents {
            self.apply_cron_remaining_to_item(child);
        }
    }

    fn change_itemstack_type(
        &mut self,
        item: &mut ItemStack,
        new_type: ItemTypeId,
        value: u16,
    ) -> Result<(), String> {
        let (
            old_cumulative,
            old_expire,
            old_text,
            old_expire_stop,
            old_magicfield,
            old_liquid_pool,
            old_liquid_container,
            old_key,
            old_rune,
            new_cumulative,
            new_text,
            new_rune,
            new_liquid_pool,
            new_wearout,
            new_expire_stop,
            new_magicfield,
            new_liquid_container,
            new_key,
            new_total_uses,
        ) = {
            let Some(object_types) = self.object_types.as_ref() else {
                return Err("object types missing".to_string());
            };
            let old_type = object_types
                .get(item.type_id)
                .ok_or_else(|| "old type missing".to_string())?;
            let new_type_entry = object_types
                .get(new_type)
                .ok_or_else(|| "new type missing".to_string())?;
            (
                old_type.has_flag("Cumulative"),
                old_type.has_flag("Expire"),
                old_type.has_flag("Text"),
                old_type.has_flag("ExpireStop"),
                old_type.has_flag("MagicField"),
                old_type.has_flag("LiquidPool"),
                old_type.has_flag("LiquidContainer"),
                old_type.has_flag("Key"),
                old_type.has_flag("Rune"),
                new_type_entry.has_flag("Cumulative"),
                new_type_entry.has_flag("Text"),
                new_type_entry.has_flag("Rune"),
                new_type_entry.has_flag("LiquidPool"),
                new_type_entry.has_flag("WearOut"),
                new_type_entry.has_flag("ExpireStop"),
                new_type_entry.has_flag("MagicField"),
                new_type_entry.has_flag("LiquidContainer"),
                new_type_entry.has_flag("Key"),
                new_type_entry.attribute_u16("TotalUses").unwrap_or(0),
            )
        };

        let mut amount = 0u16;
        let mut saved_expire_time = 0u32;
        let mut delay: i32 = -1;

        if old_cumulative {
            amount = item.count;
        }

        if old_expire {
            saved_expire_time = self.cron.stop(item.id, self.cron_round).unwrap_or(0);
            if new_cumulative {
                amount = 1;
            }
        }

        if old_text && !new_text {
            item.attributes.retain(|attr| {
                !matches!(attr, ItemAttribute::DynamicString(_) | ItemAttribute::String(_))
            });
        }

        if old_expire_stop {
            if let Some(saved) = item_saved_expire_secs(item) {
                if saved > 0 {
                    delay = i32::from(saved);
                }
            }
        }

        if old_magicfield {
            item.attributes
                .retain(|attr| !matches!(attr, ItemAttribute::Responsible(_)));
        }

        item.type_id = new_type;

        if new_cumulative {
            if amount == 0 {
                amount = 1;
            }
            item.count = amount;
        } else {
            item.count = 1;
        }

        if new_rune {
            let charges = item
                .attributes
                .iter()
                .find_map(|attribute| {
                    if let ItemAttribute::Charges(value) = attribute {
                        Some(*value)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if charges == 0 {
                let _ = set_itemstack_attribute_u16(item, "Charges", 1, ItemAttribute::Charges);
            }
        }

        if new_liquid_pool {
            let pool = item
                .attributes
                .iter()
                .find_map(|attribute| {
                    if let ItemAttribute::PoolLiquidType(value) = attribute {
                        Some(*value)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if pool == 0 {
                let _ = set_itemstack_attribute_u8(
                    item,
                    "PoolLiquidType",
                    1,
                    ItemAttribute::PoolLiquidType,
                );
            }
        }

        if new_wearout {
            let remaining = item
                .attributes
                .iter()
                .find_map(|attribute| {
                    if let ItemAttribute::RemainingUses(value) = attribute {
                        Some(*value)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if remaining == 0 {
                let total = new_total_uses;
                let _ = set_itemstack_attribute_u16(
                    item,
                    "RemainingUses",
                    i32::from(total),
                    ItemAttribute::RemainingUses,
                );
            }
        }

        if new_expire_stop {
            set_saved_expire_secs(item, saved_expire_time);
        }

        self.cron_expire_item(item, delay);

        if new_cumulative && !old_cumulative {
            item.count = value;
        }

        if new_magicfield && !old_magicfield {
            let _ = set_itemstack_attribute_u32(
                item,
                "Responsible",
                i32::from(value),
                ItemAttribute::Responsible,
            );
        }

        if new_liquid_pool && !old_liquid_pool {
            let _ = set_itemstack_attribute_u8(
                item,
                "PoolLiquidType",
                i32::from(value),
                ItemAttribute::PoolLiquidType,
            );
        }

        if new_liquid_container && !old_liquid_container {
            let _ = set_itemstack_attribute_u8(
                item,
                "ContainerLiquidType",
                i32::from(value),
                ItemAttribute::ContainerLiquidType,
            );
        }

        if new_key && !old_key {
            let _ = set_itemstack_attribute_u16(
                item,
                "KeyNumber",
                i32::from(value),
                ItemAttribute::KeyNumber,
            );
        }

        if new_rune && !old_rune {
            let _ = set_itemstack_attribute_u16(
                item,
                "Charges",
                i32::from(value),
                ItemAttribute::Charges,
            );
        }
        Ok(())
    }

    fn empty_container_for_expire(
        &mut self,
        path: &ItemPath,
        remainder: usize,
        is_corpse: bool,
        refresh_positions: &mut HashSet<Position>,
    ) -> Result<(), String> {
        self.ensure_item_index();
        let mut contents = match &path.root {
            ItemRoot::Inventory { player_id, slot } if path.path.is_empty() => {
                let player = self
                    .players
                    .get_mut(player_id)
                    .ok_or_else(|| "player missing".to_string())?;
                let fallback = if let Some(item) = player.inventory.slot_mut(*slot) {
                    std::mem::take(&mut item.contents)
                } else {
                    Vec::new()
                };
                player.inventory_containers.remove(slot).unwrap_or(fallback)
            }
            _ => {
                let item = self
                    .item_mut_by_path(path)
                    .ok_or_else(|| "container missing".to_string())?;
                std::mem::take(&mut item.contents)
            }
        };

        if contents.is_empty() {
            return Ok(());
        }

        let mut moved = Vec::new();
        let mut keep = Vec::new();
        if remainder >= contents.len() {
            keep = contents;
        } else {
            let split_index = contents.len() - remainder;
            let mut tail = contents.split_off(split_index);
            moved.append(&mut contents);
            keep.append(&mut tail);
        }

        if !moved.is_empty() {
            if is_corpse {
                for item in &moved {
                    self.remove_item_tree_from_index(item);
                }
            } else {
                self.move_items_to_parent(path, moved, refresh_positions)?;
            }
        }

        match &path.root {
            ItemRoot::Inventory { player_id, slot } if path.path.is_empty() => {
                let player = self
                    .players
                    .get_mut(player_id)
                    .ok_or_else(|| "player missing".to_string())?;
                if keep.is_empty() {
                    player.inventory_containers.remove(slot);
                } else {
                    player.inventory_containers.insert(*slot, keep);
                }
            }
            _ => {
                if let Some(item) = self.item_mut_by_path(path) {
                    item.contents = keep;
                }
            }
        }

        Ok(())
    }

    fn move_items_to_parent(
        &mut self,
        path: &ItemPath,
        mut moved: Vec<ItemStack>,
        refresh_positions: &mut HashSet<Position>,
    ) -> Result<(), String> {
        if moved.is_empty() {
            return Ok(());
        }
        self.ensure_item_index();
        for item in &moved {
            self.remove_item_tree_from_index(item);
        }
        let mut inserted_paths: Vec<ItemPath> = Vec::new();

        match &path.root {
            ItemRoot::Tile { position, index } => {
                let tile = self
                    .map
                    .tile_mut(*position)
                    .ok_or_else(|| "tile missing".to_string())?;
                if let Some((items, _)) = parent_in_items_mut(&mut tile.items, *index, &path.path)
                {
                    let parent_path = if path.path.is_empty() {
                        Vec::new()
                    } else {
                        path.path[..path.path.len() - 1].to_vec()
                    };
                    for item in moved.drain(..) {
                        let insert_index = items.len();
                        items.push(item);
                        if path.path.is_empty() {
                            inserted_paths.push(ItemPath {
                                root: ItemRoot::Tile {
                                    position: *position,
                                    index: insert_index,
                                },
                                path: Vec::new(),
                            });
                        } else {
                            let mut full_path = parent_path.clone();
                            full_path.push(insert_index);
                            inserted_paths.push(ItemPath {
                                root: ItemRoot::Tile {
                                    position: *position,
                                    index: *index,
                                },
                                path: full_path,
                            });
                        }
                    }
                }
                refresh_positions.insert(*position);
            }
            ItemRoot::Inventory { player_id, slot } => {
                if path.path.is_empty() {
                    let position = self
                        .players
                        .get(player_id)
                        .map(|player| player.position)
                        .ok_or_else(|| "player missing".to_string())?;
                    let tile = self
                        .map
                        .tile_mut(position)
                        .ok_or_else(|| "tile missing".to_string())?;
                    for item in moved.drain(..) {
                        let insert_index = tile.items.len();
                        tile.items.push(item);
                        inserted_paths.push(ItemPath {
                            root: ItemRoot::Tile {
                                position,
                                index: insert_index,
                            },
                            path: Vec::new(),
                        });
                    }
                    refresh_positions.insert(position);
                } else {
                    let player = self
                        .players
                        .get_mut(player_id)
                        .ok_or_else(|| "player missing".to_string())?;
                    let item = player
                        .inventory
                        .slot_mut(*slot)
                        .ok_or_else(|| "inventory slot empty".to_string())?;
                    if let Some((items, _)) = parent_contents_mut(item, &path.path) {
                        let parent_path = path.path[..path.path.len() - 1].to_vec();
                        for item in moved.drain(..) {
                            let insert_index = items.len();
                            items.push(item);
                            let mut full_path = parent_path.clone();
                            full_path.push(insert_index);
                            inserted_paths.push(ItemPath {
                                root: ItemRoot::Inventory {
                                    player_id: *player_id,
                                    slot: *slot,
                                },
                                path: full_path,
                            });
                        }
                    }
                }
            }
            ItemRoot::InventoryContainer {
                player_id,
                slot,
                index,
            } => {
                let player = self
                    .players
                    .get_mut(player_id)
                    .ok_or_else(|| "player missing".to_string())?;
                let contents = player
                    .inventory_containers
                    .get_mut(slot)
                    .ok_or_else(|| "container missing".to_string())?;
                if let Some((items, _)) = parent_in_items_mut(contents, *index, &path.path) {
                    let parent_path = path.path[..path.path.len() - 1].to_vec();
                    for item in moved.drain(..) {
                        let insert_index = items.len();
                        items.push(item);
                        let mut full_path = parent_path.clone();
                        full_path.push(insert_index);
                        inserted_paths.push(ItemPath {
                            root: ItemRoot::InventoryContainer {
                                player_id: *player_id,
                                slot: *slot,
                                index: *index,
                            },
                            path: full_path,
                        });
                    }
                }
            }
            ItemRoot::Depot {
                player_id,
                depot_id,
                index,
            } => {
                let player = self
                    .players
                    .get_mut(player_id)
                    .ok_or_else(|| "player missing".to_string())?;
                let items = player
                    .depots
                    .get_mut(depot_id)
                    .ok_or_else(|| "depot missing".to_string())?;
                if let Some((parent_items, _)) = parent_in_items_mut(items, *index, &path.path) {
                    let parent_path = path.path[..path.path.len() - 1].to_vec();
                    for item in moved.drain(..) {
                        let insert_index = parent_items.len();
                        parent_items.push(item);
                        let mut full_path = parent_path.clone();
                        full_path.push(insert_index);
                        inserted_paths.push(ItemPath {
                            root: ItemRoot::Depot {
                                player_id: *player_id,
                                depot_id: *depot_id,
                                index: *index,
                            },
                            path: full_path,
                        });
                    }
                }
            }
        }

        let mut items_to_index = Vec::new();
        for inserted in inserted_paths {
            if let Some(item) = self.item_by_path(&inserted) {
                items_to_index.push((inserted.root, inserted.path, item.clone()));
            }
        }
        for (root, path, item) in items_to_index {
            self.index_item_tree_at_path(root, path, &item);
        }

        Ok(())
    }

    fn remove_item_by_path(&mut self, path: &ItemPath) -> Option<ItemStack> {
        self.ensure_item_index();
        let removed = match &path.root {
            ItemRoot::Tile { position, index } => {
                let tile = self.map.tile_mut(*position)?;
                if path.path.is_empty() {
                    take_item_from_tile_at(tile, *index)
                } else {
                    let (items, remove_index) =
                        parent_in_items_mut(&mut tile.items, *index, &path.path)?;
                    Some(items.remove(remove_index))
                }
            }
            ItemRoot::Inventory { player_id, slot } => {
                let player = self.players.get_mut(player_id)?;
                if path.path.is_empty() {
                    let item = player.inventory.slot(*slot).cloned()?;
                    player.inventory.set_slot(*slot, None);
                    player.inventory_containers.remove(slot);
                    Some(item)
                } else {
                    let item = player.inventory.slot_mut(*slot)?;
                    let (items, remove_index) = parent_contents_mut(item, &path.path)?;
                    Some(items.remove(remove_index))
                }
            }
            ItemRoot::InventoryContainer {
                player_id,
                slot,
                index,
            } => {
                let player = self.players.get_mut(player_id)?;
                let contents = player.inventory_containers.get_mut(slot)?;
                let (items, remove_index) = parent_in_items_mut(contents, *index, &path.path)?;
                Some(items.remove(remove_index))
            }
            ItemRoot::Depot {
                player_id,
                depot_id,
                index,
            } => {
                let player = self.players.get_mut(player_id)?;
                let items = player.depots.get_mut(depot_id)?;
                let (parent_items, remove_index) = parent_in_items_mut(items, *index, &path.path)?;
                Some(parent_items.remove(remove_index))
            }
        };
        if let Some(ref item) = removed {
            self.remove_item_tree_from_index(item);
        }
        removed
    }

    fn process_cron_system(&mut self) -> usize {
        let mut processed = 0usize;
        let mut refresh_positions = HashSet::new();

        loop {
            let Some(item_id) = self.cron.check(self.cron_round) else {
                break;
            };
            let Some(path) = self.find_item_path_by_id(item_id) else {
                let _ = self.cron.stop(item_id, self.cron_round);
                continue;
            };

            let item_type_id = match self.item_mut_by_path(&path) {
                Some(item) => item.type_id,
                None => {
                    let _ = self.cron.stop(item_id, self.cron_round);
                    continue;
                }
            };
            let (is_container, is_corpse, expire_target, remainder) = {
                let Some(object_types) = self.object_types.as_ref() else {
                    let _ = self.cron.stop(item_id, self.cron_round);
                    continue;
                };
                let Some(obj_type) = object_types.get(item_type_id) else {
                    let _ = self.cron.stop(item_id, self.cron_round);
                    continue;
                };
                let expire_target = obj_type
                    .attribute_u16("ExpireTarget")
                    .map(ItemTypeId)
                    .and_then(|value| if value.0 == 0 { None } else { Some(value) });
                let remainder = expire_target
                    .and_then(|target| object_types.get(target))
                    .and_then(|target| {
                        if target.has_flag("Container") {
                            target.attribute_u16("Capacity")
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0) as usize;
                (
                    obj_type.has_flag("Container"),
                    obj_type.has_flag("Corpse"),
                    expire_target,
                    remainder,
                )
            };

            if is_container {
                let _ = self.empty_container_for_expire(
                    &path,
                    remainder,
                    is_corpse,
                    &mut refresh_positions,
                );
            }

            match expire_target {
                Some(target_type) => {
                    let item_ptr = match self.item_mut_by_path(&path) {
                        Some(item) => item as *mut ItemStack,
                        None => {
                            let _ = self.cron.stop(item_id, self.cron_round);
                            continue;
                        }
                    };
                    let change_result = unsafe {
                        self.change_itemstack_type(&mut *item_ptr, target_type, 0)
                    };
                    if let Err(err) = change_result {
                        eprintln!("cron: change failed: {}", err);
                        let _ = self.cron.stop(item_id, self.cron_round);
                        let _ = self.remove_item_by_path(&path);
                    }
                }
                None => {
                    let _ = self.cron.stop(item_id, self.cron_round);
                    let _ = self.remove_item_by_path(&path);
                }
            }

            if let ItemRoot::Tile { position, .. } = path.root {
                refresh_positions.insert(position);
            }

            processed = processed.saturating_add(1);
        }

        for position in refresh_positions {
            if let Some(tile) = self.map.tile_mut(position) {
                tile.item_details = tile.items.iter().map(map_item_for_stack).collect();
            }
            self.queue_map_refresh(position);
        }

        processed
    }

    #[allow(dead_code)]
    fn move_items_by_id_to_container(
        &mut self,
        item_ids: &[ItemId],
        dest_container_id: ItemId,
    ) -> Result<(), String> {
        if item_ids.is_empty() {
            return Ok(());
        }

        if item_ids.iter().any(|id| *id == dest_container_id) {
            return Err("cannot move container into itself".to_string());
        }

        self.ensure_item_index();
        let mut dest_path = self
            .find_item_path_by_id(dest_container_id)
            .ok_or_else(|| "destination container missing".to_string())?;

        let mut source_paths = Vec::new();
        for item_id in item_ids {
            let path = self
                .find_item_path_by_id(*item_id)
                .ok_or_else(|| "item missing".to_string())?;
            if path_is_descendant(&dest_path, &path) {
                return Err("cannot move item into its own contents".to_string());
            }
            source_paths.push(path);
        }

        let mut moved = Vec::with_capacity(item_ids.len());
        for path in source_paths {
            if let Some(item) = self.remove_item_by_path(&path) {
                moved.push(item);
            } else {
                return Err("item missing".to_string());
            }
        }

        dest_path = self
            .find_item_path_by_id(dest_container_id)
            .ok_or_else(|| "destination container missing".to_string())?;
        let dest_item_type_id = {
            let dest_item = self
                .item_mut_by_path(&dest_path)
                .ok_or_else(|| "destination container missing".to_string())?;
            dest_item.type_id
        };
        let item_types = self
            .item_types
            .as_ref()
            .ok_or_else(|| "item types missing".to_string())?;
        if !item_type_is_container(item_types, dest_item_type_id) {
            return Err("destination is not a container".to_string());
        }
        let mut inserted_paths = Vec::new();
        {
            let dest_item = self
                .item_mut_by_path(&dest_path)
                .ok_or_else(|| "destination container missing".to_string())?;
            let parent_path = dest_path.path.clone();
            for item in moved.drain(..) {
                let insert_index = dest_item.contents.len();
                dest_item.contents.push(item);
                let mut full_path = parent_path.clone();
                full_path.push(insert_index);
                inserted_paths.push(ItemPath {
                    root: dest_path.root.clone(),
                    path: full_path,
                });
            }
        }
        let mut items_to_index = Vec::new();
        for inserted in inserted_paths {
            if let Some(item) = self.item_by_path(&inserted) {
                items_to_index.push((inserted.root, inserted.path, item.clone()));
            }
        }
        for (root, path, item) in items_to_index {
            self.index_item_tree_at_path(root, path, &item);
        }
        Ok(())
    }

    pub fn tick_houses(&mut self) -> usize {
        let now = unix_time_now();
        if let Some(next_check) = self.next_house_rent_check {
            if now < next_check {
                return 0;
            }
        }
        self.next_house_rent_check = Some(now.saturating_add(HOUSE_RENT_CHECK_SECS));
        let Some(owners) = self.house_owners.as_mut() else {
            return 0;
        };
        let mut evicted = 0usize;
        for owner in owners.iter_mut() {
            if owner.owner == 0 {
                continue;
            }
            if owner.paid_until != 0 && owner.paid_until > now {
                continue;
            }
            owner.owner = 0;
            owner.paid_until = 0;
            owner.last_transition = now;
            owner.guests.clear();
            owner.subowners.clear();
            evicted = evicted.saturating_add(1);
        }
        evicted
    }

    fn refresh_cylinder(&mut self, x: u16, y: u16, z_min: u8, z_max: u8) {
        for z in z_min..=z_max {
            let coord = SectorCoord { x, y, z };
            if self.sector_refreshable(coord) {
                self.refresh_sector(coord);
            }
        }
    }

    fn sector_refreshable(&self, coord: SectorCoord) -> bool {
        let min_x = coord.x.saturating_mul(SECTOR_TILE_SIZE);
        let min_y = coord.y.saturating_mul(SECTOR_TILE_SIZE);
        let max_x = min_x.saturating_add(SECTOR_TILE_SIZE - 1);
        let max_y = min_y.saturating_add(SECTOR_TILE_SIZE - 1);
        for player in self.players.values() {
            let pos = player.position;
            if pos.x < min_x || pos.x > max_x || pos.y < min_y || pos.y > max_y {
                continue;
            }
            if pos.z <= 7 && coord.z <= 7 {
                return false;
            }
            if pos.z > 7
                && coord.z >= pos.z
                && coord.z <= pos.z.saturating_add(ACTIVE_Z_RANGE_UNDERGROUND)
            {
                return false;
            }
        }
        true
    }

    fn position_in_bounds_with_sectors(
        &self,
        position: Position,
        sector_set: Option<&HashSet<SectorCoord>>,
    ) -> bool {
        let bounds = self
            .map_dat
            .as_ref()
            .and_then(|map_dat| map_dat.sector_bounds)
            .or(self.map.sector_bounds);

        let sector = self.map.sector_for_position(position);

        if let Some(bounds) = bounds {
            if sector.x < bounds.min.x
                || sector.y < bounds.min.y
                || sector.z < bounds.min.z
                || sector.x > bounds.max.x
                || sector.y > bounds.max.y
                || sector.z > bounds.max.z
            {
                return false;
            }
        }

        if !self.map.sectors.is_empty() {
            if let Some(sector_set) = sector_set {
                if !sector_set.contains(&sector) {
                    return false;
                }
            } else if !self.map.has_sector(sector) {
                return false;
            }
        }

        true
    }

    fn refresh_sector(&mut self, coord: SectorCoord) {
        let Some(base_map) = self.map_original.as_ref() else {
            return;
        };
        let min_x = coord.x.saturating_mul(SECTOR_TILE_SIZE);
        let min_y = coord.y.saturating_mul(SECTOR_TILE_SIZE);
        let max_x = min_x.saturating_add(SECTOR_TILE_SIZE - 1);
        let max_y = min_y.saturating_add(SECTOR_TILE_SIZE - 1);

        let mut refresh_tiles: Vec<(Position, Tile)> = Vec::new();
        for y in min_y..=max_y {
            for x in min_x..=max_x {
                let pos = Position { x, y, z: coord.z };
                let Some(base_tile) = base_map.tiles.get(&pos) else {
                    continue;
                };
                if !base_tile.refresh {
                    continue;
                }
                let Some(current_tile) = self.map.tiles.get(&pos) else {
                    continue;
                };
                if !tile_has_added_items(base_tile, current_tile) {
                    continue;
                }
                refresh_tiles.push((pos, base_tile.clone()));
            }
        }

        self.ensure_item_index();
        let mut reserved_positions: HashSet<Position> = HashSet::new();
        let mut npc_moves: Vec<(CreatureId, Position)> = Vec::new();
        let mut npc_resets: Vec<CreatureId> = Vec::new();
        let mut monster_moves: Vec<(CreatureId, Position)> = Vec::new();
        let mut monsters_to_remove = Vec::new();
        let mut cron_items = Vec::new();
        for (pos, base_tile) in refresh_tiles {
            let Some(current_tile) = self.map.tiles.get(&pos) else {
                continue;
            };
            let current_items: Vec<ItemStack> = current_tile.items.iter().cloned().collect();
            for item in &current_items {
                self.remove_item_tree_from_index(item);
            }
            if self.players.values().any(|player| player.position == pos) {
                continue;
            }
            if let Some((npc_id, _)) = self.npcs.iter().find(|(_, npc)| npc.position == pos) {
                if let Some(free) =
                    self.find_free_refresh_position(pos, Some(*npc_id), &reserved_positions)
                {
                    reserved_positions.insert(free);
                    npc_moves.push((*npc_id, free));
                } else {
                    npc_resets.push(*npc_id);
                }
            }
            if let Some((monster_id, _)) = self
                .monsters
                .iter()
                .find(|(_, monster)| monster.position == pos)
            {
                if let Some(free) =
                    self.find_free_refresh_position(pos, Some(*monster_id), &reserved_positions)
                {
                    reserved_positions.insert(free);
                    monster_moves.push((*monster_id, free));
                } else {
                    monsters_to_remove.push(*monster_id);
                }
            }
            self.map.tiles.insert(pos, base_tile.clone());
            if let Some(new_tile) = self.map.tiles.get(&pos) {
                let new_items: Vec<ItemStack> = new_tile.items.iter().cloned().collect();
                for (index_pos, item) in new_items.iter().enumerate() {
                    let root = ItemRoot::Tile {
                        position: pos,
                        index: index_pos,
                    };
                    self.index_item_tree_at_path(root, Vec::new(), item);
                }
            }
            cron_items.extend(base_tile.items.iter().cloned());
        }
        for item in &cron_items {
            self.schedule_cron_for_item_tree(item);
        }

        for (npc_id, pos) in npc_moves {
            if let Some(npc) = self.npcs.get_mut(&npc_id) {
                npc.position = pos;
            }
        }
        for npc_id in npc_resets {
            let home = self.npcs.get(&npc_id).map(|npc| npc.home);
            let position = home.and_then(|home| self.find_login_position(home, 1u8, false));
            if let (Some(position), Some(npc)) = (position, self.npcs.get_mut(&npc_id)) {
                npc.position = position;
            }
        }
        for (monster_id, pos) in monster_moves {
            if let Some(monster) = self.monsters.get_mut(&monster_id) {
                let origin = monster.position;
                monster.position = pos;
                self.update_monster_sector_index(monster_id, origin, pos);
            }
        }
        for id in monsters_to_remove {
            if let Some(monster) = self.monsters.remove(&id) {
                self.remove_monster_from_sector_index(id, monster.position);
                if let Some(home_id) = monster.home_id {
                    self.notify_monster_home_death(home_id);
                }
            }
        }
    }

    pub fn tick_npcs(&mut self, clock: &GameClock) -> Vec<CreatureStep> {
        let npc_ids: Vec<CreatureId> = self.npcs.keys().copied().collect();
        let mut moves = Vec::new();
        for npc_id in npc_ids {
            let (position, home, radius, ready, focused, focus_expires_at) =
                match self.npcs.get(&npc_id) {
                Some(npc) => (
                    npc.position,
                    npc.home,
                    npc.radius,
                    npc.move_cooldown.is_ready(clock),
                    npc.focused,
                    npc.focus_expires_at,
                ),
                None => continue,
            };
            let mut clear_focus = false;
            if let Some(focused_id) = focused {
                if let Some(expires_at) = focus_expires_at {
                    if clock.now() >= expires_at {
                        clear_focus = true;
                    }
                }
                if let Some(player) = self.players.get(&focused_id) {
                    if !npc_in_range(position, player.position, NPC_TALK_RANGE) {
                        clear_focus = true;
                    }
                } else {
                    clear_focus = true;
                }
            }
            if clear_focus {
                if let Some(npc) = self.npcs.get_mut(&npc_id) {
                    npc.focused = None;
                    npc.focus_expires_at = None;
                }
            }
            if focused.is_some() && !clear_focus {
                continue;
            }
            if radius == 0 || !ready {
                continue;
            }
            if !self.has_player_in_range(position, NPC_ACTIVE_RANGE) {
                continue;
            }

            for _ in 0..NPC_MOVE_ATTEMPTS {
                let direction = self.npc_rng.roll_direction();
                let destination = match self.resolve_movement_destination(position, direction) {
                    Ok(destination) => destination,
                    Err(_) => continue,
                };
                if !npc_within_wander_radius(home, radius, destination) {
                    continue;
                }
                if let Some(npc) = self.npcs.get_mut(&npc_id) {
                    npc.direction = direction;
                }
                if let Ok(destination) = self.move_npc(npc_id, direction) {
                    moves.push(CreatureStep {
                        id: npc_id,
                        from: position,
                        to: destination,
                    });
                    break;
                }
            }
            if let Some(npc) = self.npcs.get_mut(&npc_id) {
                npc.move_cooldown
                    .reset_from_now_ticks(clock, NPC_MOVE_INTERVAL_TICKS);
            }
        }
        moves
    }

    fn monster_acquire_range(lose_target_distance: u16) -> u16 {
        if lose_target_distance == 0 {
            MONSTER_ACQUIRE_RANGE
        } else {
            lose_target_distance
        }
    }

    fn has_player_in_range(&self, position: Position, radius: u16) -> bool {
        self.players.values().any(|player| {
            player_in_active_range(position, player.position, radius)
        })
    }

    fn has_visible_player_in_range(
        &self,
        position: Position,
        radius: u16,
        flags: MonsterFlags,
    ) -> bool {
        self.players.values().any(|player| {
            player_in_active_range(position, player.position, radius)
                && Self::player_visible_to_monster(player, flags)
        })
    }

    fn player_visible_to_monster(player: &PlayerState, flags: MonsterFlags) -> bool {
        flags.see_invisible || !Self::player_is_invisible(player)
    }

    fn player_is_invisible(player: &PlayerState) -> bool {
        player.current_outfit.look_type == 0 && player.current_outfit.look_item == 0
    }

    fn select_monster_target(
        &mut self,
        position: Position,
        current_target: Option<PlayerId>,
        range: u16,
        strategy: [u8; 4],
        damage_by: &HashMap<PlayerId, u32>,
        flags: MonsterFlags,
    ) -> Option<PlayerId> {
        if let Some(target_id) = current_target {
            if let Some(player) = self.players.get(&target_id) {
                if !Self::player_visible_to_monster(player, flags) {
                    return None;
                }
                if self.is_protection_zone(player.position) {
                    return None;
                }
                if player.position.z == position.z
                    && Self::monster_tile_distance(position, player.position) <= range
                {
                    return Some(target_id);
                }
            }
        }

        let strategy = Self::monster_strategy_choice(&mut self.monster_rng, strategy);
        let mut best: Option<(PlayerId, i32, u32)> = None;
        for (id, player) in &self.players {
            if player.position.z != position.z {
                continue;
            }
            if !Self::player_visible_to_monster(player, flags) {
                continue;
            }
            if self.is_protection_zone(player.position) {
                continue;
            }
            let distance = Self::monster_tile_distance(position, player.position);
            if distance > range {
                continue;
            }
            let dx = if position.x >= player.position.x {
                position.x - player.position.x
            } else {
                player.position.x - position.x
            };
            let dy = if position.y >= player.position.y {
                position.y - player.position.y
            } else {
                player.position.y - position.y
            };
            let goodness = match strategy {
                0 => -i32::from(dx + dy),
                1 => -(player.stats.mana.min(i32::MAX as u32) as i32),
                2 => damage_by.get(id).copied().unwrap_or(0) as i32,
                _ => 0,
            };
            let tie_breaker = self.monster_rng.roll_range(0, 99);
            match best {
                None => best = Some((*id, goodness, tie_breaker)),
                Some((_, best_goodness, best_tie))
                    if goodness > best_goodness
                        || (goodness == best_goodness && tie_breaker > best_tie) =>
                {
                    best = Some((*id, goodness, tie_breaker));
                }
                _ => {}
            }
        }
        best.map(|(id, _, _)| id)
    }

    fn monster_strategy_choice(rng: &mut MonsterRng, strategy: [u8; 4]) -> u8 {
        let mut selection = rng.roll_range(0, 99) as i32;
        for (index, weight) in strategy.iter().take(3).enumerate() {
            let weight = i32::from(*weight);
            if selection < weight {
                return index as u8;
            }
            selection -= weight;
        }
        3
    }

    fn monster_tile_distance(a: Position, b: Position) -> u16 {
        let dx = if a.x >= b.x { a.x - b.x } else { b.x - a.x };
        let dy = if a.y >= b.y { a.y - b.y } else { b.y - a.y };
        dx.max(dy)
    }

    fn monster_attack_value(monster: &MonsterInstance, now: GameTick) -> u32 {
        let delta = monster
            .strength_effect
            .filter(|effect| !effect.is_expired(now))
            .map(|effect| effect.delta)
            .unwrap_or(0);
        let adjusted = monster.attack as i32 + i32::from(delta);
        adjusted.clamp(0, i32::MAX) as u32
    }

    fn scale_defend_by_attack_mode(defend: u32, attack_mode: u8) -> u32 {
        match attack_mode {
            1 => {
                // Disasm: defend - floor(defend * 4 / 10).
                let reduced = (u64::from(defend) * 4) / 10;
                defend.saturating_sub(reduced as u32)
            }
            3 => {
                // Disasm: defend + floor(defend * 8 / 10).
                let bonus = (u64::from(defend) * 8) / 10;
                defend.saturating_add(bonus as u32)
            }
            _ => defend,
        }
    }

    fn scale_attack_by_attack_mode(attack: u32, attack_mode: u8) -> u32 {
        match attack_mode {
            1 => {
                let bonus = (u64::from(attack) * 2) / 10;
                attack.saturating_add(bonus as u32)
            }
            3 => {
                let reduced = (u64::from(attack) * 4) / 10;
                attack.saturating_sub(reduced as u32)
            }
            _ => attack,
        }
    }

    fn weapon_skill_from_type(weapon_type: u16) -> SkillType {
        match weapon_type {
            1 => SkillType::Sword,
            2 => SkillType::Club,
            3 => SkillType::Axe,
            _ => SkillType::Fist,
        }
    }

    fn player_defense_values(&self, player: &PlayerState) -> DefendSelection {
        let Some(object_types) = self.object_types.as_ref() else {
            return DefendSelection {
                defend: 0,
                armor: 0,
                slot: None,
                item_type: None,
                skill: SkillType::Fist,
            };
        };
        let mut armor = 0u32;
        let mut shield_defend: Option<(u32, ItemTypeId)> = None;
        let mut weapon_defend: Option<(u32, SkillType, ItemTypeId)> = None;
        let mut throw_defend: Option<(u32, ItemTypeId)> = None;

        for slot in INVENTORY_SLOTS {
            let Some(item) = player.inventory.slot(slot) else {
                continue;
            };
            let Some(object_type) = object_types.get(item.type_id) else {
                continue;
            };
            if let Some(value) = object_type.attribute_u16("ArmorValue") {
                armor = armor.saturating_add(u32::from(value));
            }
            match slot {
                InventorySlot::LeftHand => {
                    if let Some(value) = object_type.attribute_u16("ShieldDefendValue") {
                        shield_defend = Some((u32::from(value), item.type_id));
                    }
                }
                InventorySlot::RightHand => {
                    if let Some(value) = object_type.attribute_u16("WeaponDefendValue") {
                        let skill = object_type
                            .attribute_u16("WeaponType")
                            .map(Self::weapon_skill_from_type)
                            .unwrap_or(SkillType::Fist);
                        weapon_defend = Some((u32::from(value), skill, item.type_id));
                    }
                    if let Some(value) = object_type.attribute_u16("ThrowDefendValue") {
                        throw_defend = Some((u32::from(value), item.type_id));
                    }
                }
                _ => {}
            }
        }

        let (defend, slot, item_type, skill) = if let Some((value, type_id)) = shield_defend {
            (
                value.saturating_add(u32::from(player.skills.shielding.level)),
                Some(InventorySlot::LeftHand),
                Some(type_id),
                SkillType::Shielding,
            )
        } else if let Some((value, skill, type_id)) = weapon_defend {
            (
                value.saturating_add(u32::from(player.skills.get(skill).level)),
                Some(InventorySlot::RightHand),
                Some(type_id),
                skill,
            )
        } else if let Some((value, type_id)) = throw_defend {
            (
                value.saturating_add(u32::from(player.skills.distance.level)),
                Some(InventorySlot::RightHand),
                Some(type_id),
                SkillType::Distance,
            )
        } else {
            (u32::from(player.skills.fist.level), None, None, SkillType::Fist)
        };
        DefendSelection {
            defend,
            armor,
            slot,
            item_type,
            skill,
        }
    }

    fn apply_defend_wear(&mut self, target_id: PlayerId, selection: DefendSelection) {
        let Some(slot) = selection.slot else {
            return;
        };
        let Some(item_type) = selection.item_type else {
            return;
        };
        let (total_uses, wearout_target) = {
            let Some(object_types) = self.object_types.as_ref() else {
                return;
            };
            let Some(object_type) = object_types.get(item_type) else {
                return;
            };
            let Some(total_uses) = object_type.attribute_u16("TotalUses") else {
                return;
            };
            (total_uses, object_type.attribute_u16("WearoutTarget").unwrap_or(0))
        };
        let item = match self
            .players
            .get(&target_id)
            .and_then(|player| player.inventory.slot(slot).cloned())
        {
            Some(item) => item,
            None => return,
        };
        if item.type_id != item_type {
            return;
        }
        let remaining = if item.count <= 1 && total_uses > 1 {
            total_uses
        } else {
            item.count
        };
        let next_remaining = remaining.saturating_sub(1);
        if next_remaining == 0 {
            if wearout_target > 0 {
                let mut updated = item.clone();
                let updated = if self
                    .change_itemstack_type(&mut updated, ItemTypeId(wearout_target), 0)
                    .is_ok()
                {
                    Some(updated)
                } else {
                    None
                };
                if let Some(player) = self.players.get_mut(&target_id) {
                    player.inventory.set_slot(slot, updated);
                }
            } else {
                if let Some(player) = self.players.get_mut(&target_id) {
                    player.inventory.set_slot(slot, None);
                }
            }
            return;
        }
        if let Some(player) = self.players.get_mut(&target_id) {
            player.inventory.set_slot(
                slot,
                Some(ItemStack { id: crate::entities::item::ItemId::next(),
                    type_id: item.type_id,
                    count: next_remaining,
                    attributes: item.attributes.clone(),
                    contents: item.contents.clone(),
                }),
            );
        }
    }

    fn protection_mask_for_damage(damage_type: DamageType) -> u16 {
        match damage_type {
            DamageType::Physical => 1,
            DamageType::Earth => 2,
            DamageType::Fire => 4,
            DamageType::Energy => 8,
            DamageType::LifeDrain => 0x100,
            DamageType::ManaDrain => 0x200,
            _ => 0,
        }
    }

    fn apply_player_protection_reduction(
        &mut self,
        target_id: PlayerId,
        damage_type: DamageType,
        amount: u32,
    ) -> u32 {
        if amount == 0 {
            return 0;
        }
        let mask = Self::protection_mask_for_damage(damage_type);
        if mask == 0 {
            return amount;
        }
        let mut reduced = amount;
        for slot in INVENTORY_SLOTS {
            let item = match self
                .players
                .get(&target_id)
                .and_then(|player| player.inventory.slot(slot).cloned())
            {
                Some(item) => item,
                None => continue,
            };
            let (body_position, protection_mask, reduction, total_uses, wearout_target) = {
                let Some(object_types) = self.object_types.as_ref() else {
                    return amount;
                };
                let Some(object_type) = object_types.get(item.type_id) else {
                    continue;
                };
                if !object_type.has_flag("Protection") || !object_type.has_flag("Clothes") {
                    continue;
                }
                let Some(body_position) = object_type.body_position() else {
                    continue;
                };
                let protection_mask = object_type.attribute_u16("ProtectionDamageTypes").unwrap_or(0);
                let reduction = object_type.attribute_u16("DamageReduction").unwrap_or(0);
                let total_uses = match object_type.attribute_u16("TotalUses") {
                    Some(value) => value,
                    None => continue,
                };
                let wearout_target = object_type.attribute_u16("WearoutTarget").unwrap_or(0);
                (body_position, protection_mask, reduction, total_uses, wearout_target)
            };
            let expected = match u8::try_from(slot.index().saturating_add(1)) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if body_position != expected {
                continue;
            }
            if protection_mask & mask == 0 {
                continue;
            }
            if reduction == 0 {
                continue;
            }
            let capped = reduction.min(100) as u32;
            reduced = (reduced * (100 - capped)) / 100;

            let remaining = if item.count <= 1 && total_uses > 1 {
                total_uses
            } else {
                item.count
            };
            let next_remaining = remaining.saturating_sub(1);
            if next_remaining == 0 {
                if wearout_target > 0 {
                    let mut updated = item.clone();
                    let updated = if self
                        .change_itemstack_type(&mut updated, ItemTypeId(wearout_target), 0)
                        .is_ok()
                    {
                        Some(updated)
                    } else {
                        None
                    };
                    if let Some(player) = self.players.get_mut(&target_id) {
                        player.inventory.set_slot(slot, updated);
                    }
                } else {
                    if let Some(player) = self.players.get_mut(&target_id) {
                        player.inventory.set_slot(slot, None);
                    }
                }
            } else {
                if let Some(player) = self.players.get_mut(&target_id) {
                    player.inventory.set_slot(
                        slot,
                        Some(ItemStack { id: crate::entities::item::ItemId::next(),
                            type_id: item.type_id,
                            count: next_remaining,
                            attributes: item.attributes.clone(),
                            contents: item.contents.clone(),
                        }),
                    );
                }
            }

            if reduced == 0 {
                break;
            }
        }

        reduced
    }

    fn player_attack_values(&self, player: &PlayerState) -> AttackSelection {
        let fallback_attack = u32::from(player.skills.fist.level).max(1);
        let mut selection = AttackSelection {
            attack: fallback_attack,
            range: PLAYER_MELEE_RANGE,
            skill: SkillType::Fist,
            damage_type: DamageType::Physical,
        };
        let Some(object_types) = self.object_types.as_ref() else {
            return selection;
        };

        let selection_for_item = |item: &ItemStack| -> Option<AttackSelection> {
            let object_type = object_types.get(item.type_id)?;
            if let Some(value) = object_type.attribute_u16("WeaponAttackValue") {
                let skill = object_type
                    .attribute_u16("WeaponType")
                    .map(Self::weapon_skill_from_type)
                    .unwrap_or(SkillType::Fist);
                return Some(AttackSelection {
                    attack: u32::from(value),
                    range: PLAYER_MELEE_RANGE,
                    skill,
                    damage_type: DamageType::Physical,
                });
            }
            if let Some(value) = object_type.attribute_u16("ThrowAttackValue") {
                return Some(AttackSelection {
                    attack: u32::from(value),
                    range: PLAYER_THROW_RANGE,
                    skill: SkillType::Distance,
                    damage_type: DamageType::Physical,
                });
            }
            if let Some(value) = object_type.attribute_u16("WandAttackStrength") {
                return Some(AttackSelection {
                    attack: u32::from(value),
                    range: PLAYER_DISTANCE_RANGE,
                    skill: SkillType::Magic,
                    damage_type: DamageType::Energy,
                });
            }
            None
        };

        if let Some(item) = player.inventory.slot(InventorySlot::RightHand) {
            if let Some(found) = selection_for_item(item) {
                return found;
            }
        }
        if let Some(item) = player.inventory.slot(InventorySlot::LeftHand) {
            if let Some(found) = selection_for_item(item) {
                return found;
            }
        }
        if let Some(item) = player.inventory.slot(InventorySlot::Ammo) {
            if let Some(object_type) = object_types.get(item.type_id) {
                if let Some(value) = object_type.attribute_u16("AmmoAttackValue") {
                    selection = AttackSelection {
                        attack: u32::from(value),
                        range: PLAYER_DISTANCE_RANGE,
                        skill: SkillType::Distance,
                        damage_type: DamageType::Physical,
                    };
                }
            }
        }

        selection
    }

    fn roll_player_attack_damage(
        &mut self,
        attack: u32,
        attack_mode: u8,
        skill_level: u16,
        level: u16,
    ) -> u32 {
        let scaled_attack = Self::scale_attack_by_attack_mode(attack, attack_mode);
        if scaled_attack == 0 {
            return 0;
        }
        let base_damage = self.moveuse_rng.roll_range(1, scaled_attack) as i32;
        let scaled = compute_damage(
            base_damage,
            0,
            i32::from(skill_level),
            i32::from(level),
            DamageScaleFlags::NONE,
            0,
        );
        scaled.max(0) as u32
    }

    fn monster_direction(from: Position, target: Position, flee: bool) -> Option<Direction> {
        let mut dx = i32::from(target.x) - i32::from(from.x);
        let mut dy = i32::from(target.y) - i32::from(from.y);
        if dx == 0 && dy == 0 {
            return None;
        }
        if flee {
            dx = -dx;
            dy = -dy;
        }
        let abs_dx = dx.unsigned_abs();
        let abs_dy = dy.unsigned_abs();
        let east = dx > 0;
        let west = dx < 0;
        let south = dy > 0;
        let north = dy < 0;
        let direction = if abs_dx == abs_dy && abs_dx != 0 {
            match (north, south, east, west) {
                (true, _, true, _) => Direction::Northeast,
                (true, _, _, true) => Direction::Northwest,
                (_, true, true, _) => Direction::Southeast,
                (_, true, _, true) => Direction::Southwest,
                _ => return None,
            }
        } else if abs_dx > abs_dy {
            match (east, west) {
                (true, _) => Direction::East,
                (_, true) => Direction::West,
                _ => return None,
            }
        } else {
            match (south, north) {
                (true, _) => Direction::South,
                (_, true) => Direction::North,
                _ => return None,
            }
        };
        Some(direction)
    }

    fn monster_try_cast_spell(
        &mut self,
        monster_id: CreatureId,
        monster_pos: Position,
        target_pos: Position,
        spells: &[MonsterSpell],
        clock: &GameClock,
        effects: &mut Vec<MonsterVisualEffect>,
        missiles: &mut Vec<MonsterMissileEffect>,
        player_hits: &mut HashSet<PlayerId>,
        player_hit_marks: &mut HashSet<PlayerHitMarker>,
        monster_updates: &mut HashSet<CreatureId>,
        outfit_updates: &mut Vec<CreatureOutfitUpdate>,
        speed_updates: &mut Vec<CreatureSpeedUpdate>,
        refresh_map: &mut bool,
        flags: MonsterFlags,
    ) -> bool {
        let direction = Self::monster_direction(monster_pos, target_pos, false);

        for spell in spells {
            let Some(meta) = Self::monster_spell_target_meta(&spell.target) else {
                continue;
            };
            let Some(positions) =
                self.monster_spell_positions(meta, monster_pos, target_pos, direction)
            else {
                continue;
            };
            if positions.is_empty() {
                continue;
            }
            if !self.monster_rng.roll_percent(spell.chance as u32) {
                continue;
            }

            match meta {
                MonsterSpellTargetMeta::Victim { missile_id, .. }
                | MonsterSpellTargetMeta::Destination { missile_id, .. } => {
                    if missile_id != 0 {
                        missiles.push(MonsterMissileEffect {
                            from: monster_pos,
                            to: target_pos,
                            missile_id,
                        });
                    }
                }
                _ => {}
            }

            let effect_id = match meta {
                MonsterSpellTargetMeta::Actor { effect_id }
                | MonsterSpellTargetMeta::Victim { effect_id, .. }
                | MonsterSpellTargetMeta::Origin { effect_id, .. }
                | MonsterSpellTargetMeta::Destination { effect_id, .. }
                | MonsterSpellTargetMeta::Angle { effect_id, .. } => effect_id,
            };
            if effect_id != 0 {
                for position in &positions {
                    if !self.map.tiles.is_empty() && !self.map.has_tile(*position) {
                        continue;
                    }
                    effects.push(MonsterVisualEffect {
                        position: *position,
                        effect_id,
                    });
                }
            }

            match &spell.effect {
                MonsterSpellEffect::Damage { damage_type, args, .. } => {
                    let Some((min_damage, max_damage)) = Self::spell_damage_bounds(args) else {
                        return true;
                    };
                    for target_id in self.monster_spell_targets(&positions, flags) {
                        let attempted_damage = if min_damage == max_damage {
                            min_damage
                        } else {
                            self.monster_rng.roll_range(min_damage, max_damage)
                        };
                        let target_position = match self.players.get(&target_id) {
                            Some(target) => target.position,
                            None => continue,
                        };
                        if self.is_protection_zone(target_position) {
                            continue;
                        }
                        let reduced_damage = self.apply_player_protection_reduction(
                            target_id,
                            *damage_type,
                            attempted_damage,
                        );
                        let (applied_damage, absorbed_mana) = {
                            let target = match self.players.get_mut(&target_id) {
                                Some(target) => target,
                                None => continue,
                            };
                            let (applied, absorbed) =
                                target.apply_damage_with_magic_shield(*damage_type, reduced_damage);
                            if applied > 0 || absorbed > 0 {
                                target.mark_in_combat(clock, self.combat_rules.fight_timer);
                            }
                            (applied, absorbed)
                        };
                        if applied_damage > 0 || absorbed_mana > 0 {
                            player_hits.insert(target_id);
                            player_hit_marks.insert(PlayerHitMarker {
                                player_id: target_id,
                                attacker_id: monster_id,
                            });
                        }
                        let _ = applied_damage;
                    }
                }
                MonsterSpellEffect::Healing { args } => {
                    let Some((min_heal, max_heal)) = Self::spell_heal_bounds(args) else {
                        return true;
                    };
                    let heal_amount = if min_heal == max_heal {
                        min_heal
                    } else {
                        self.monster_rng.roll_range(min_heal, max_heal)
                    };
                    let mut applied_to_monster = false;
                    if matches!(meta, MonsterSpellTargetMeta::Actor { .. }) {
                        if let Some(monster) = self.monsters.get_mut(&monster_id) {
                            monster.stats.health =
                                (monster.stats.health + heal_amount).min(monster.stats.max_health);
                            monster_updates.insert(monster_id);
                            applied_to_monster = true;
                        }
                    }
                    if !applied_to_monster {
                        for target_id in self.monster_spell_targets(&positions, flags) {
                            if let Some(target) = self.players.get_mut(&target_id) {
                                target.stats.health =
                                    (target.stats.health + heal_amount).min(target.stats.max_health);
                                player_hits.insert(target_id);
                            }
                        }
                    }
                }
                MonsterSpellEffect::Summon { args } => {
                    if let Some((race_number, count)) = Self::spell_summon_args(args) {
                        self.monster_spawn_summons(monster_pos, race_number, count);
                    }
                }
                MonsterSpellEffect::Speed { args } => {
                    let Some((delta, min_duration, max_duration)) =
                        Self::spell_speed_args(args)
                    else {
                        return true;
                    };
                    let duration = if min_duration == max_duration {
                        min_duration
                    } else {
                        self.monster_rng.roll_range(min_duration, max_duration)
                    };
                    let duration_ticks = Self::spell_duration_ticks(clock, duration);
                    if duration_ticks == 0 {
                        return true;
                    }
                    if matches!(meta, MonsterSpellTargetMeta::Actor { .. }) {
                        self.apply_monster_speed_effect(
                            monster_id,
                            delta,
                            None,
                            duration_ticks,
                            speed_updates,
                            clock.now(),
                        );
                    } else {
                        for target_id in self.monster_spell_targets(&positions, flags) {
                            self.apply_player_speed_effect(
                                target_id,
                                delta,
                                None,
                                duration_ticks,
                                speed_updates,
                                clock.now(),
                            );
                        }
                    }
                }
                MonsterSpellEffect::Outfit { args } => {
                    let Some((outfit, duration)) = Self::spell_outfit_args(args) else {
                        return true;
                    };
                    let duration_ticks = Self::spell_duration_ticks(clock, duration);
                    if duration_ticks == 0 {
                        return true;
                    }
                    if matches!(meta, MonsterSpellTargetMeta::Actor { .. }) {
                        self.apply_monster_outfit_effect(
                            monster_id,
                            outfit,
                            duration_ticks,
                            outfit_updates,
                            clock.now(),
                            false,
                        );
                    } else {
                        for target_id in self.monster_spell_targets(&positions, flags) {
                            self.apply_player_outfit_effect(
                                target_id,
                                outfit,
                                duration_ticks,
                                outfit_updates,
                                clock.now(),
                                clock,
                            );
                        }
                    }
                }
                MonsterSpellEffect::Field { args } => {
                    let Some(field_kind) = Self::spell_field_kind(args) else {
                        return true;
                    };
                    let Some(type_id) = Self::field_item_type_id(field_kind) else {
                        return true;
                    };
                    let stackable = self
                        .item_types
                        .as_ref()
                        .and_then(|index| index.get(type_id))
                        .map(|item| item.stackable)
                        .unwrap_or(false);
                    let mut stack = ItemStack { id: crate::entities::item::ItemId::next(),
                        type_id,
                        count: 1,
                        attributes: Vec::new(),
                        contents: Vec::new(),
};
                    let _ = set_itemstack_attribute_u32(
                        &mut stack,
                        "Responsible",
                        monster_id.0.min(i32::MAX as u32) as i32,
                        ItemAttribute::Responsible,
                    );
                    self.schedule_cron_for_item_tree(&stack);
                    let movable = self.item_is_movable(&stack);
                    let mut placed_any = false;
                    for position in &positions {
                        if self.is_protection_zone(*position) {
                            continue;
                        }
                        if !self.map.tiles.is_empty() && !self.map.has_tile(*position) {
                            continue;
                        }
                        let Some(tile) = self.map.tile_mut(*position) else {
                            continue;
                        };
                        if place_on_tile_with_dustbin(tile, stack.clone(), stackable, movable)
                            .is_ok()
                        {
                            placed_any = true;
                        }
                    }
                    if placed_any {
                        *refresh_map = true;
                    }
                    if let Some((kind, _)) = Self::field_condition_kind(field_kind) {
                        for target_id in self.monster_spell_targets(&positions, flags) {
                            if let Some(target) = self.players.get_mut(&target_id) {
                                Self::apply_condition_skill_timer(
                                    target,
                                    kind,
                                    MONSTER_FIELD_TICK_DAMAGE,
                                );
                            }
                        }
                    }
                }
                MonsterSpellEffect::Drunken { args } => {
                    let Some((intensity, min_duration, max_duration)) =
                        Self::spell_drunken_args(args)
                    else {
                        return true;
                    };
                    let duration = if min_duration == max_duration {
                        min_duration
                    } else {
                        self.monster_rng.roll_range(min_duration, max_duration)
                    };
                    let duration_ticks = Self::spell_duration_ticks(clock, duration);
                    if duration_ticks == 0 {
                        return true;
                    }
                    if matches!(meta, MonsterSpellTargetMeta::Actor { .. }) {
                        // Drunken has no monster-side effect implemented yet.
                    } else {
                        for target_id in self.monster_spell_targets(&positions, flags) {
                            self.apply_player_drunken_effect(
                                target_id,
                                intensity,
                                duration_ticks,
                                clock.now(),
                                clock,
                            );
                        }
                    }
                }
                MonsterSpellEffect::Strength { args } => {
                    let Some((delta, min_duration, max_duration)) =
                        Self::spell_strength_args(args)
                    else {
                        return true;
                    };
                    let duration = if min_duration == max_duration {
                        min_duration
                    } else {
                        self.monster_rng.roll_range(min_duration, max_duration)
                    };
                    let duration_ticks = Self::spell_duration_ticks(clock, duration);
                    if duration_ticks == 0 {
                        return true;
                    }
                    if matches!(meta, MonsterSpellTargetMeta::Actor { .. }) {
                        self.apply_monster_strength_effect(
                            monster_id,
                            delta,
                            duration_ticks,
                            clock.now(),
                        );
                    } else {
                        for target_id in self.monster_spell_targets(&positions, flags) {
                            self.apply_player_strength_effect(
                                target_id,
                                delta,
                                duration_ticks,
                                clock.now(),
                            );
                        }
                    }
                }
                MonsterSpellEffect::Unknown { .. } => {}
            }

            return true;
        }

        false
    }

    fn monster_melee_attack(
        &mut self,
        monster_id: CreatureId,
        target_id: PlayerId,
        attack: u32,
        poison: u32,
        clock: &GameClock,
        player_hits: &mut HashSet<PlayerId>,
        player_hit_marks: &mut HashSet<PlayerHitMarker>,
        _monster_updates: &mut HashSet<CreatureId>,
    ) -> bool {
        let (skills, flags) = match self.monsters.get(&monster_id) {
            Some(monster) => (monster.skills, monster.flags),
            None => return false,
        };
        let base_damage = if attack == 0 {
            0
        } else {
            self.monster_rng.roll_range(1, attack) as i32
        };
        let attempted_damage = if base_damage == 0 {
            0
        } else {
            let skill_a = skills.melee_skill(flags);
            let mut skill_b = skills.level;
            let skill_a = if skill_a == 0 && skill_b == 0 {
                skill_b = 0;
                50
            } else {
                skill_a
            };
            let scaled = compute_damage(
                base_damage,
                0,
                skill_a,
                skill_b,
                DamageScaleFlags::NONE,
                0,
            );
            scaled.max(0) as u32
        };
        let (defend_selection, attack_mode, defend_ready) = match self.players.get(&target_id) {
            Some(target) => (
                self.player_defense_values(target),
                target.fight_modes.attack_mode,
                target.defend_cooldown.is_ready(clock),
            ),
            None => return false,
        };
        let defend = defend_selection.defend;
        let armor = defend_selection.armor;
        let defend_roll = if defend_ready {
            if defend_selection.skill == SkillType::Shielding {
                self.train_player_skill(target_id, SkillType::Shielding);
            }
            let scaled = Self::scale_defend_by_attack_mode(defend, attack_mode);
            if scaled == 0 {
                0
            } else {
                let capped = scaled.min(u32::from(u16::MAX)) as u16;
                self.monster_rng.roll_range(0, u32::from(capped))
            }
        } else {
            0
        };
        let armor_roll = if armor == 0 {
            0
        } else {
            let capped = armor.min(u32::from(u16::MAX)) as u16;
            self.monster_rng.roll_range(0, u32::from(capped))
        };
        let mitigated_damage = attempted_damage
            .saturating_sub(defend_roll)
            .saturating_sub(armor_roll);
        let reduced_damage =
            self.apply_player_protection_reduction(target_id, DamageType::Physical, mitigated_damage);
        let (applied_damage, absorbed_mana) = {
            let target = match self.players.get_mut(&target_id) {
                Some(target) => target,
                None => return false,
            };
            let (applied, absorbed) = target
                .apply_damage_with_magic_shield(DamageType::Physical, reduced_damage);
            if applied > 0 || absorbed > 0 {
                target.mark_in_combat(clock, self.combat_rules.fight_timer);
            }
            (applied, absorbed)
        };
        if attempted_damage > 0 {
            player_hits.insert(target_id);
            player_hit_marks.insert(PlayerHitMarker {
                player_id: target_id,
                attacker_id: monster_id,
            });
        }
        if attempted_damage > 0 && defend_ready {
            self.apply_defend_wear(target_id, defend_selection);
            let defend_ticks = clock.ticks_from_duration_round_up(Duration::from_millis(
                DEFEND_COOLDOWN_MS,
            ));
            if let Some(target) = self.players.get_mut(&target_id) {
                target
                    .defend_cooldown
                    .reset_from_now_ticks(clock, defend_ticks);
            }
        }

        if poison > 0 && (applied_damage > 0 || absorbed_mana > 0) {
            let poison_half = poison / 2;
            let poison_roll = self.monster_rng.roll_range(poison_half, poison);
            if poison_roll > 0 {
                if let Some(target) = self.players.get_mut(&target_id) {
                    Self::apply_condition_skill_timer(target, ConditionKind::Poison, poison_roll);
                }
            }
        }

        true
    }

    fn monster_spell_targets(
        &self,
        positions: &[Position],
        flags: MonsterFlags,
    ) -> Vec<PlayerId> {
        let mut targets = Vec::new();
        for position in positions {
            if !self.map.tiles.is_empty() && !self.map.has_tile(*position) {
                continue;
            }
            for (id, player) in self.players.iter() {
                if !Self::player_visible_to_monster(player, flags) {
                    continue;
                }
                if self.is_protection_zone(player.position) {
                    continue;
                }
                if player.position == *position {
                    targets.push(*id);
                }
            }
        }
        targets
    }

    fn monster_talk_delay_ticks(&mut self) -> u64 {
        if MONSTER_TALK_MIN_TICKS >= MONSTER_TALK_MAX_TICKS {
            return MONSTER_TALK_MIN_TICKS;
        }
        u64::from(self.monster_rng.roll_range(
            MONSTER_TALK_MIN_TICKS as u32,
            MONSTER_TALK_MAX_TICKS as u32,
        ))
    }

    fn monster_pick_talk_line(rng: &mut MonsterRng, lines: &[String]) -> Option<String> {
        if lines.is_empty() {
            return None;
        }
        let index = if lines.len() == 1 {
            0
        } else {
            rng.roll_range(0, (lines.len() - 1) as u32) as usize
        };
        lines.get(index).cloned()
    }

    fn monster_parse_talk_line(line: &str) -> (u8, String) {
        const TALK_TYPE_MONSTER_SAY: u8 = 0x10;
        const TALK_TYPE_MONSTER_YELL: u8 = 0x11;

        let trimmed = line.trim();
        if let Some(stripped) = trimmed.strip_prefix("#Y") {
            return (TALK_TYPE_MONSTER_YELL, stripped.trim_start().to_string());
        }
        if let Some(stripped) = trimmed.strip_prefix("#W") {
            return (TALK_TYPE_MONSTER_SAY, stripped.trim_start().to_string());
        }
        (TALK_TYPE_MONSTER_SAY, trimmed.to_string())
    }

    fn monster_spell_positions(
        &self,
        meta: MonsterSpellTargetMeta,
        monster_pos: Position,
        target_pos: Position,
        direction: Option<Direction>,
    ) -> Option<Vec<Position>> {
        match meta {
            MonsterSpellTargetMeta::Actor { .. } => Some(vec![monster_pos]),
            MonsterSpellTargetMeta::Victim { range, .. } => {
                if Self::monster_tile_distance(monster_pos, target_pos) > range {
                    return None;
                }
                Some(vec![target_pos])
            }
            MonsterSpellTargetMeta::Origin { radius, .. } => {
                Some(circle_positions(self.circles.as_ref(), monster_pos, radius))
            }
            MonsterSpellTargetMeta::Destination { range, radius, .. } => {
                if Self::monster_tile_distance(monster_pos, target_pos) > range {
                    return None;
                }
                Some(circle_positions(self.circles.as_ref(), target_pos, radius))
            }
            MonsterSpellTargetMeta::Angle { angle, range, .. } => {
                let direction = direction?;
                if Self::monster_tile_distance(monster_pos, target_pos) > u16::from(range) {
                    return None;
                }
                if range == 0 {
                    return Some(Vec::new());
                }
                if angle == 0 {
                    Some(line_positions(monster_pos, direction, range))
                } else {
                    Some(cone_positions(monster_pos, direction, range, angle))
                }
            }
        }
    }

    fn monster_id_at_position(&self, position: Position) -> Option<CreatureId> {
        self.monsters
            .iter()
            .find_map(|(id, monster)| (monster.position == position).then_some(*id))
    }

    fn player_summon_count(&self, caster_id: PlayerId) -> usize {
        self.monsters
            .values()
            .filter(|monster| monster.summoner == Some(caster_id))
            .count()
    }

    fn resolve_monster_race_by_name(&self, name: &str) -> Option<i64> {
        self.monster_index
            .as_ref()
            .and_then(|index| index.race_by_name(name))
    }

    fn resolve_creature_position_by_name(&self, name: &str) -> Option<Position> {
        if let Some(player) = self
            .players
            .values()
            .find(|player| player.name.eq_ignore_ascii_case(name))
        {
            return Some(player.position);
        }
        self.monsters
            .values()
            .find(|monster| monster.name.eq_ignore_ascii_case(name))
            .map(|monster| monster.position)
    }

    fn find_person_message(&self, caster: Position, target: Position, name: &str) -> String {
        if target.z < caster.z {
            return format!("{name} is above you.");
        }
        if target.z > caster.z {
            return format!("{name} is below you.");
        }
        let dx = i32::from(target.x) - i32::from(caster.x);
        let dy = i32::from(target.y) - i32::from(caster.y);
        if dx == 0 && dy == 0 {
            return format!("{name} is standing next to you.");
        }
        let mut parts = Vec::new();
        if dy < 0 {
            parts.push("north");
        } else if dy > 0 {
            parts.push("south");
        }
        if dx > 0 {
            parts.push("east");
        } else if dx < 0 {
            parts.push("west");
        }
        let direction = parts.join("-");
        format!("{name} is to the {direction}.")
    }

    fn resolve_creature_illusion_outfit(&self, name: &str) -> Result<Outfit, String> {
        let index = self
            .monster_index
            .as_ref()
            .ok_or_else(|| "spell cast failed: creature data missing".to_string())?;
        let script = index
            .script_by_name(name)
            .ok_or_else(|| "spell cast failed: unknown creature".to_string())?;
        let flags = script
            .flags()
            .map(|entries| MonsterFlags::from_list(&entries))
            .unwrap_or_default();
        if flags.no_illusion {
            return Err("spell cast failed: creature cannot be imitated".to_string());
        }
        Ok(script.outfit().unwrap_or(DEFAULT_OUTFIT))
    }

    fn resolve_chameleon_outfit(&self, position: Position) -> Option<Outfit> {
        if let Some(player) = self.players.values().find(|player| player.position == position) {
            return Some(player.current_outfit);
        }
        if let Some(monster) = self
            .monsters
            .values()
            .find(|monster| monster.position == position)
        {
            return Some(monster.outfit);
        }
        if let Some(npc) = self.npcs.values().find(|npc| npc.position == position) {
            return Some(npc.outfit);
        }
        let tile = self.map.tile(position)?;
        let item = tile.items.last()?;
        Some(Outfit {
            look_type: 0,
            head: 0,
            body: 0,
            legs: 0,
            feet: 0,
            addons: 0,
            look_item: item.type_id.0,
        })
    }

    fn resolve_levitate_target(
        &self,
        caster_position: Position,
        direction: Option<Direction>,
        spell_args: Option<&[String]>,
    ) -> Result<Position, String> {
        let direction = direction.ok_or_else(|| "spell cast failed: direction required".to_string())?;
        let arg = spell_args
            .and_then(|args| args.first())
            .ok_or_else(|| "spell cast failed: missing parameter".to_string())?;
        let delta_z = match arg.trim().to_ascii_lowercase().as_str() {
            "up" => -1,
            "down" => 1,
            _ => return Err("spell cast failed: not possible".to_string()),
        };

        let forward = caster_position
            .step(direction)
            .ok_or_else(|| "spell cast failed: not possible".to_string())?;
        if let Some(tile) = self.map.tile(forward) {
            if !self.tile_blocks_movement(tile) {
                return Err("spell cast failed: not possible".to_string());
            }
        }

        let target = forward
            .offset(PositionDelta { dx: 0, dy: 0, dz: delta_z })
            .ok_or_else(|| "spell cast failed: not possible".to_string())?;
        if !self.position_in_bounds(target) {
            return Err("spell cast failed: not possible".to_string());
        }
        let tile = self
            .map
            .tile(target)
            .ok_or_else(|| "spell cast failed: not possible".to_string())?;
        if self.tile_blocks_movement(tile) {
            return Err("spell cast failed: not possible".to_string());
        }
        if self.tile_floor_change(tile).is_some() {
            return Err("spell cast failed: not possible".to_string());
        }
        if self.position_occupied(target) {
            return Err("spell cast failed: not possible".to_string());
        }
        Ok(target)
    }

    fn raise_dead_from_corpses(
        &mut self,
        caster_id: PlayerId,
        origin: Position,
        radius: u8,
        race_number: i64,
    ) -> bool {
        if self.race_blocks_summon(race_number) {
            return false;
        }
        let Some(object_types) = self.object_types.as_ref() else {
            return false;
        };
        let mut remaining = PLAYER_SUMMON_LIMIT.saturating_sub(self.player_summon_count(caster_id));
        if remaining == 0 {
            return false;
        }
        let positions = circle_positions(self.circles.as_ref(), origin, radius);
        let mut corpses = Vec::new();
        for position in positions {
            if remaining == 0 {
                break;
            }
            let Some(tile) = self.map.tile(position) else {
                continue;
            };
            let corpse_index = tile.items.iter().rposition(|item| {
                object_types
                    .get(item.type_id)
                    .map(|object| object.has_flag("Corpse") && object.is_movable())
                    .unwrap_or(false)
            });
            if let Some(index) = corpse_index {
                corpses.push((position, index));
                remaining = remaining.saturating_sub(1);
            }
        }

        let mut refreshed = false;
        for (position, index) in corpses {
            let spawned = match self.spawn_monster_by_race_with_summoner(
                race_number,
                position,
                Some(caster_id),
                true,
                None,
            ) {
                Ok(id) => {
                    if let Some(monster) = self.monsters.get_mut(&id) {
                        monster.target = None;
                        monster.damage_by.clear();
                        monster.talk_lines.clear();
                    }
                    Some(id)
                }
                Err(_) => None,
            };
            if spawned.is_none() {
                continue;
            }
            refreshed = true;
            if let Some(tile) = self.map.tile_mut(position) {
                if index < tile.items.len() {
                    remove_item_at_index(tile, index);
                }
            }
        }

        refreshed
    }

    fn apply_challenge(&mut self, caster_id: PlayerId, origin: Position, radius: u8) {
        let positions = circle_positions(self.circles.as_ref(), origin, radius);
        for monster in self.monsters.values_mut() {
            if monster.summoner.is_some() {
                continue;
            }
            if positions.iter().any(|pos| *pos == monster.position) {
                monster.target = Some(caster_id);
            }
        }
    }

    fn monster_spawn_summons(&mut self, origin: Position, race_number: i64, count: u8) {
        if count == 0 {
            return;
        }
        if self.race_blocks_summon(race_number) {
            return;
        }
        let positions = circle_positions(self.circles.as_ref(), origin, 1);
        let mut spawned = 0u8;
        for position in positions {
            if spawned >= count {
                break;
            }
            if self
                .spawn_monster_by_race_with_summoner(race_number, position, None, true, None)
                .is_ok()
            {
                spawned = spawned.saturating_add(1);
            }
        }
    }

    fn player_spawn_summons(
        &mut self,
        caster_id: PlayerId,
        origin: Position,
        race_number: i64,
        count: u8,
    ) -> u8 {
        if count == 0 {
            return 0;
        }
        if self.race_blocks_summon(race_number) {
            return 0;
        }
        let positions = circle_positions(self.circles.as_ref(), origin, 1);
        let mut spawned = 0u8;
        for position in positions {
            if spawned >= count {
                break;
            }
            if let Ok(id) = self.spawn_monster_by_race_with_summoner(
                race_number,
                position,
                Some(caster_id),
                true,
                None,
            ) {
                if let Some(monster) = self.monsters.get_mut(&id) {
                    monster.target = None;
                    monster.damage_by.clear();
                    monster.talk_lines.clear();
                }
                spawned = spawned.saturating_add(1);
            }
        }
        spawned
    }

    fn race_blocks_summon(&self, race_number: i64) -> bool {
        let Some(index) = self.monster_index.as_ref() else {
            return false;
        };
        let Some(script) = index.script_by_race(race_number) else {
            return false;
        };
        let flags = script.flags().unwrap_or_default();
        MonsterFlags::from_list(&flags).no_summon
    }

    fn race_blocks_convince(&self, race_number: i64) -> bool {
        let Some(index) = self.monster_index.as_ref() else {
            return false;
        };
        let Some(script) = index.script_by_race(race_number) else {
            return false;
        };
        let flags = script.flags().unwrap_or_default();
        let flags = MonsterFlags::from_list(&flags);
        flags.no_summon || flags.no_convince
    }

    fn race_summon_cost(&self, race_number: i64) -> u32 {
        let Some(index) = self.monster_index.as_ref() else {
            return 0;
        };
        let Some(script) = index.script_by_race(race_number) else {
            return 0;
        };
        script.summon_cost().unwrap_or(0)
    }

    fn monster_spell_target_meta(target: &MonsterSpellTarget) -> Option<MonsterSpellTargetMeta> {
        match target {
            MonsterSpellTarget::Actor(args) => {
                let effect_id = Self::script_value_u16(args.get(0)?)?;
                Some(MonsterSpellTargetMeta::Actor { effect_id })
            }
            MonsterSpellTarget::Victim(args) => {
                if args.len() < 3 {
                    return None;
                }
                let range = Self::script_value_u16(args.get(0)?)?;
                let missile_id = Self::script_value_u16(args.get(1)?)?;
                let effect_id = Self::script_value_u16(args.get(2)?)?;
                Some(MonsterSpellTargetMeta::Victim {
                    range,
                    missile_id: missile_id.min(u16::from(u8::MAX)) as u8,
                    effect_id,
                })
            }
            MonsterSpellTarget::Origin(args) => {
                if args.len() < 2 {
                    return None;
                }
                let radius = Self::script_value_u16(args.get(0)?)?;
                let effect_id = Self::script_value_u16(args.get(1)?)?;
                Some(MonsterSpellTargetMeta::Origin {
                    radius: radius.min(u16::from(u8::MAX)) as u8,
                    effect_id,
                })
            }
            MonsterSpellTarget::Destination(args) => {
                if args.len() < 4 {
                    return None;
                }
                let range = Self::script_value_u16(args.get(0)?)?;
                let missile_id = Self::script_value_u16(args.get(1)?)?;
                let radius = Self::script_value_u16(args.get(2)?)?;
                let effect_id = Self::script_value_u16(args.get(3)?)?;
                Some(MonsterSpellTargetMeta::Destination {
                    range,
                    missile_id: missile_id.min(u16::from(u8::MAX)) as u8,
                    radius: radius.min(u16::from(u8::MAX)) as u8,
                    effect_id,
                })
            }
            MonsterSpellTarget::Angle(args) => {
                if args.len() < 3 {
                    return None;
                }
                let angle = Self::script_value_u16(args.get(0)?)?;
                let range = Self::script_value_u16(args.get(1)?)?;
                let effect_id = Self::script_value_u16(args.get(2)?)?;
                Some(MonsterSpellTargetMeta::Angle {
                    angle,
                    range: range.min(u16::from(u8::MAX)) as u8,
                    effect_id,
                })
            }
            MonsterSpellTarget::Unknown { .. } => None,
        }
    }

    fn spell_damage_bounds(args: &[ScriptValue]) -> Option<(u32, u32)> {
        let max = Self::script_value_u32(args.get(1)?)?;
        let min = Self::script_value_u32(args.get(2).unwrap_or(&args[1]))?;
        let (min, max) = if min <= max { (min, max) } else { (max, min) };
        Some((min, max))
    }

    fn spell_heal_bounds(args: &[ScriptValue]) -> Option<(u32, u32)> {
        let max = Self::script_value_u32(args.get(0)?)?;
        let min = Self::script_value_u32(args.get(1).unwrap_or(&args[0]))?;
        let (min, max) = if min <= max { (min, max) } else { (max, min) };
        Some((min, max))
    }

    fn spell_summon_args(args: &[ScriptValue]) -> Option<(i64, u8)> {
        let race_number = Self::script_value_i64(args.get(0)?)?;
        let count = Self::script_value_u16(args.get(1).unwrap_or(&ScriptValue::Number(1)))?;
        Some((race_number, count.min(u16::from(u8::MAX)) as u8))
    }

    fn spell_speed_args(args: &[ScriptValue]) -> Option<(i16, u32, u32)> {
        let delta = Self::script_value_i64(args.get(0)?)?;
        let min = match args.get(1) {
            Some(value) => Self::script_value_u32(value)?,
            None => 0,
        };
        let max = match args.get(2) {
            Some(value) => Self::script_value_u32(value)?,
            None => min,
        };
        let (min, max) = if min <= max { (min, max) } else { (max, min) };
        let delta = delta.clamp(i64::from(i16::MIN), i64::from(i16::MAX)) as i16;
        Some((delta, min, max))
    }

    fn spell_outfit_args(args: &[ScriptValue]) -> Option<(Outfit, u32)> {
        let outfit = Self::parse_outfit_value(args.get(0)?)?;
        let duration = match args.get(1) {
            Some(value) => Self::script_value_u32(value)?,
            None => 0,
        };
        Some((outfit, duration))
    }

    fn spell_field_kind(args: &[ScriptValue]) -> Option<u16> {
        Self::script_value_u16(args.get(0)?)
    }

    fn field_item_type_id(field_kind: u16) -> Option<ItemTypeId> {
        match field_kind {
            1 => Some(ItemTypeId(FIRE_FIELD_TYPE_ID)),
            2 => Some(ItemTypeId(POISON_FIELD_TYPE_ID)),
            3 => Some(ItemTypeId(ENERGY_FIELD_TYPE_ID)),
            4 => Some(ItemTypeId(MAGIC_WALL_TYPE_ID)),
            5 => Some(ItemTypeId(WILD_GROWTH_TYPE_ID)),
            _ => None,
        }
    }

    fn field_condition_kind(field_kind: u16) -> Option<(ConditionKind, DamageType)> {
        match field_kind {
            1 => Some((ConditionKind::Fire, DamageType::Fire)),
            2 => Some((ConditionKind::Poison, DamageType::Earth)),
            3 => Some((ConditionKind::Energy, DamageType::Energy)),
            _ => None,
        }
    }

    fn spell_drunken_args(args: &[ScriptValue]) -> Option<(u8, u32, u32)> {
        let intensity = Self::script_value_u16(args.get(0)?)?;
        let min = match args.get(1) {
            Some(value) => Self::script_value_u32(value)?,
            None => 0,
        };
        let max = match args.get(2) {
            Some(value) => Self::script_value_u32(value)?,
            None => min,
        };
        let (min, max) = if min <= max { (min, max) } else { (max, min) };
        Some((intensity.min(u16::from(u8::MAX)) as u8, min, max))
    }

    fn spell_strength_args(args: &[ScriptValue]) -> Option<(i16, u32, u32)> {
        let delta = Self::script_value_i64(args.get(1)?)?;
        let min = match args.get(2) {
            Some(value) => Self::script_value_u32(value)?,
            None => 0,
        };
        let max = match args.get(3) {
            Some(value) => Self::script_value_u32(value)?,
            None => min,
        };
        let (min, max) = if min <= max { (min, max) } else { (max, min) };
        let delta = delta.clamp(i64::from(i16::MIN), i64::from(i16::MAX)) as i16;
        Some((delta, min, max))
    }

    fn script_value_i64(value: &ScriptValue) -> Option<i64> {
        match value {
            ScriptValue::Number(number) => Some(*number),
            ScriptValue::Ident(ident) => ident.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn script_value_u16(value: &ScriptValue) -> Option<u16> {
        let raw = Self::script_value_i64(value)?;
        if raw < 0 {
            return None;
        }
        u16::try_from(raw).ok()
    }

    fn script_value_u32(value: &ScriptValue) -> Option<u32> {
        let raw = Self::script_value_i64(value)?;
        if raw < 0 {
            return None;
        }
        u32::try_from(raw).ok()
    }

    fn parse_outfit_value(value: &ScriptValue) -> Option<Outfit> {
        let parts = match value {
            ScriptValue::Tuple(parts) | ScriptValue::List(parts) => parts,
            _ => return None,
        };
        if parts.is_empty() {
            return None;
        }
        let look_type = Self::script_value_u16(&parts[0])?;
        let mut outfit = Outfit {
            look_type,
            head: 0,
            body: 0,
            legs: 0,
            feet: 0,
            addons: 0,
            look_item: 0,
        };
        if parts.len() >= 2 {
            if let Some(colors) = Self::parse_outfit_colors(&parts[1]) {
                outfit.head = colors[0];
                outfit.body = colors[1];
                outfit.legs = colors[2];
                outfit.feet = colors[3];
            } else if look_type == 0 {
                if let Some(look_item) = Self::script_value_u16(&parts[1]) {
                    outfit.look_item = look_item;
                }
            }
        }
        Some(outfit)
    }

    fn parse_outfit_colors(value: &ScriptValue) -> Option<[u8; 4]> {
        match value {
            ScriptValue::Tuple(parts) | ScriptValue::List(parts) => {
                if parts.len() < 4 {
                    return None;
                }
                let mut colors = [0u8; 4];
                for (idx, part) in parts.iter().take(4).enumerate() {
                    let component = Self::script_value_u16(part)?;
                    colors[idx] = u8::try_from(component).ok()?;
                }
                Some(colors)
            }
            ScriptValue::Ident(value) | ScriptValue::String(value) => {
                let mut colors = [0u8; 4];
                let mut iter = value.split('-');
                for slot in &mut colors {
                    let raw = iter.next()?;
                    let component = raw.trim().parse::<i64>().ok()?;
                    if !(0..=255).contains(&component) {
                        return None;
                    }
                    *slot = component as u8;
                }
                Some(colors)
            }
            _ => None,
        }
    }

    fn spell_duration_ticks(clock: &GameClock, seconds: u32) -> u64 {
        if seconds == 0 {
            return 0;
        }
        clock.ticks_from_duration_round_up(Duration::from_secs(u64::from(seconds)))
    }

    fn apply_player_outfit_effect(
        &mut self,
        player_id: PlayerId,
        outfit: Outfit,
        duration_ticks: u64,
        updates: &mut Vec<CreatureOutfitUpdate>,
        now: GameTick,
        clock: &GameClock,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        let push_update = {
            let player = match self.players.get_mut(&player_id) {
                Some(player) => player,
                None => return,
            };
            let original = player
                .outfit_effect
                .map(|effect| effect.original)
                .unwrap_or(player.current_outfit);
            let current = player.current_outfit;
            player.current_outfit = outfit;
            player.outfit_effect = Some(OutfitEffect {
                outfit,
                original,
                expires_at,
            });
            current != outfit
        };
        if duration_ticks > 0 {
            self.note_status_effect_tick(expires_at);
        }
        if push_update {
            updates.push(CreatureOutfitUpdate {
                id: player_id.0,
                outfit,
            });
        }
        if duration_ticks > 0 {
            let duration_seconds = Self::skill_timer_seconds_from_ticks(duration_ticks, clock);
            if duration_seconds > 0 {
                let player = match self.players.get_mut(&player_id) {
                    Some(player) => player,
                    None => return,
                };
                if player.raw_skills.is_empty() {
                    player.raw_skills = skill_rows_from_player(player);
                }
                let index = Self::ensure_skill_row(
                    &mut player.raw_skills,
                    SKILL_ILLUSION,
                    player.profession,
                );
                Self::set_skill_timer(
                    &mut player.raw_skills[index],
                    1,
                    duration_seconds,
                    duration_seconds,
                );
            }
        }
    }

    fn clear_player_outfit_effect(
        &mut self,
        player_id: PlayerId,
        updates: &mut Vec<CreatureOutfitUpdate>,
    ) {
        let player = match self.players.get_mut(&player_id) {
            Some(player) => player,
            None => return,
        };
        let Some(effect) = player.outfit_effect else {
            return;
        };
        let current = player.current_outfit;
        player.current_outfit = effect.original;
        player.outfit_effect = None;
        if current != effect.original {
            updates.push(CreatureOutfitUpdate {
                id: player_id.0,
                outfit: effect.original,
            });
        }
        if !player.raw_skills.is_empty() {
            if let Some(index) = player.raw_skills.iter().position(|row| row.skill_id == SKILL_ILLUSION) {
                Self::clear_skill_timer(&mut player.raw_skills[index]);
            }
        }
    }

    fn apply_monster_outfit_effect(
        &mut self,
        monster_id: CreatureId,
        outfit: Outfit,
        duration_ticks: u64,
        updates: &mut Vec<CreatureOutfitUpdate>,
        now: GameTick,
        respect_no_illusion: bool,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        let push_update = {
            let monster = match self.monsters.get_mut(&monster_id) {
                Some(monster) => monster,
                None => return,
            };
            if respect_no_illusion && monster.flags.no_illusion {
                return;
            }
            let original = monster
                .outfit_effect
                .map(|effect| effect.original)
                .unwrap_or(monster.outfit);
            let current = monster.outfit;
            monster.outfit = outfit;
            monster.outfit_effect = Some(OutfitEffect {
                outfit,
                original,
                expires_at,
            });
            current != outfit
        };
        if duration_ticks > 0 {
            self.note_status_effect_tick(expires_at);
        }
        if push_update {
            updates.push(CreatureOutfitUpdate {
                id: monster_id.0,
                outfit,
            });
        }
    }

    fn apply_player_speed_effect(
        &mut self,
        player_id: PlayerId,
        delta: i16,
        percent: Option<i16>,
        duration_ticks: u64,
        updates: &mut Vec<CreatureSpeedUpdate>,
        now: GameTick,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        let mut new_speed_update = None;
        {
            let player = match self.players.get_mut(&player_id) {
                Some(player) => player,
                None => return,
            };
            let original_speed = player
                .speed_effect
                .map(|effect| effect.original_speed)
                .unwrap_or(player.base_speed);
            let current_speed = player
                .speed_effect
                .map(|effect| effect.speed)
                .unwrap_or(player.base_speed);
            let base_speed = original_speed;
            let delta = match percent {
                Some(percent) => {
                    let scaled = (i32::from(base_speed) * i32::from(percent)) / 100;
                    scaled.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16
                }
                None => delta,
            };
            let adjusted = i32::from(base_speed) + i32::from(delta);
            let new_speed = adjusted.clamp(1, i32::from(u16::MAX)) as u16;
            player.speed_effect = Some(SpeedEffect {
                speed: new_speed,
                original_speed,
                expires_at,
            });
            if current_speed != new_speed {
                new_speed_update = Some(new_speed);
            }
        }
        if duration_ticks > 0 {
            self.note_status_effect_tick(expires_at);
        }
        if let Some(new_speed) = new_speed_update {
            updates.push(CreatureSpeedUpdate {
                id: player_id.0,
                speed: new_speed,
            });
        }
    }

    fn apply_player_light_effect(
        &mut self,
        player_id: PlayerId,
        level: u8,
        color: u8,
        duration_ticks: u64,
        updates: &mut Vec<CreatureLightUpdate>,
        now: GameTick,
        clock: &GameClock,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        let push_update = {
            let player = match self.players.get_mut(&player_id) {
                Some(player) => player,
                None => return,
            };
            let current = player
                .light_effect
                .map(|effect| (effect.level, effect.color));
            player.light_effect = Some(LightEffect {
                level,
                color,
                expires_at,
            });
            current != Some((level, color))
        };
        if duration_ticks > 0 {
            self.note_status_effect_tick(expires_at);
        }
        if push_update {
            updates.push(CreatureLightUpdate {
                id: player_id.0,
                level,
                color,
            });
        }
        if duration_ticks > 0 {
            let duration_seconds = Self::skill_timer_seconds_from_ticks(duration_ticks, clock);
            if duration_seconds > 0 && level > 0 {
                let player = match self.players.get_mut(&player_id) {
                    Some(player) => player,
                    None => return,
                };
                if player.raw_skills.is_empty() {
                    player.raw_skills = skill_rows_from_player(player);
                }
                let index =
                    Self::ensure_skill_row(&mut player.raw_skills, SKILL_LIGHT, player.profession);
                let interval = duration_seconds
                    .saturating_div(i32::from(level.max(1)))
                    .max(1);
                Self::set_skill_timer(
                    &mut player.raw_skills[index],
                    i32::from(level),
                    interval,
                    interval,
                );
            }
        }
    }

    fn apply_player_magic_shield_effect(
        &mut self,
        player_id: PlayerId,
        duration_ticks: u64,
        now: GameTick,
        clock: &GameClock,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        {
            let player = match self.players.get_mut(&player_id) {
                Some(player) => player,
                None => return,
            };
            player.magic_shield_effect = Some(MagicShieldEffect { expires_at });
        }
        if duration_ticks > 0 {
            self.note_status_effect_tick(expires_at);
        }
        if duration_ticks > 0 {
            let duration_seconds = Self::skill_timer_seconds_from_ticks(duration_ticks, clock);
            if duration_seconds > 0 {
                let player = match self.players.get_mut(&player_id) {
                    Some(player) => player,
                    None => return,
                };
                if player.raw_skills.is_empty() {
                    player.raw_skills = skill_rows_from_player(player);
                }
                let index = Self::ensure_skill_row(
                    &mut player.raw_skills,
                    SKILL_MANASHIELD,
                    player.profession,
                );
                Self::set_skill_timer(
                    &mut player.raw_skills[index],
                    1,
                    duration_seconds,
                    duration_seconds,
                );
            }
        }
    }

    fn apply_monster_speed_effect(
        &mut self,
        monster_id: CreatureId,
        delta: i16,
        percent: Option<i16>,
        duration_ticks: u64,
        updates: &mut Vec<CreatureSpeedUpdate>,
        now: GameTick,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        let mut new_speed_update = None;
        {
            let monster = match self.monsters.get_mut(&monster_id) {
                Some(monster) => monster,
                None => return,
            };
            let base_speed = monster
                .speed_effect
                .map(|effect| effect.original_speed)
                .unwrap_or(monster.speed);
            let delta = match percent {
                Some(percent) => {
                    let scaled = (i32::from(base_speed) * i32::from(percent)) / 100;
                    scaled.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16
                }
                None => delta,
            };
            if delta < 0 && monster.flags.no_paralyze {
                return;
            }
            let original_speed = base_speed;
            let current_speed = monster.speed;
            let adjusted = i32::from(base_speed) + i32::from(delta);
            let new_speed = adjusted.clamp(1, i32::from(u16::MAX)) as u16;
            monster.speed = new_speed;
            monster.speed_effect = Some(SpeedEffect {
                speed: new_speed,
                original_speed,
                expires_at,
            });
            if current_speed != new_speed {
                new_speed_update = Some(new_speed);
            }
        }
        if duration_ticks > 0 {
            self.note_status_effect_tick(expires_at);
        }
        if let Some(new_speed) = new_speed_update {
            updates.push(CreatureSpeedUpdate {
                id: monster_id.0,
                speed: new_speed,
            });
        }
    }

    fn apply_player_drunken_effect(
        &mut self,
        player_id: PlayerId,
        intensity: u8,
        duration_ticks: u64,
        now: GameTick,
        clock: &GameClock,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        {
            let player = match self.players.get_mut(&player_id) {
                Some(player) => player,
                None => return,
            };
            player.drunken_effect = match player.drunken_effect {
                Some(existing) => Some(DrunkenEffect {
                    intensity: existing.intensity.max(intensity),
                    expires_at: existing.expires_at.max(expires_at),
                }),
                None => Some(DrunkenEffect {
                    intensity,
                    expires_at,
                }),
            };
        }
        self.note_status_effect_tick(expires_at);
        if duration_ticks > 0 {
            let duration_seconds = Self::skill_timer_seconds_from_ticks(duration_ticks, clock);
            if duration_seconds > 0 {
                let player = match self.players.get_mut(&player_id) {
                    Some(player) => player,
                    None => return,
                };
                if player.raw_skills.is_empty() {
                    player.raw_skills = skill_rows_from_player(player);
                }
                let index =
                    Self::ensure_skill_row(&mut player.raw_skills, SKILL_DRUNKEN, player.profession);
                let current = player.raw_skills[index].values[SKILL_FIELD_CYCLE];
                if current <= i32::from(intensity) {
                    Self::set_skill_timer(
                        &mut player.raw_skills[index],
                        i32::from(intensity),
                        duration_seconds,
                        duration_seconds,
                    );
                }
            }
        }
    }

    fn apply_player_strength_effect(
        &mut self,
        player_id: PlayerId,
        delta: i16,
        duration_ticks: u64,
        now: GameTick,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        {
            let player = match self.players.get_mut(&player_id) {
                Some(player) => player,
                None => return,
            };
            player.strength_effect = match player.strength_effect {
                Some(existing) => Some(StrengthEffect {
                    delta,
                    expires_at: existing.expires_at.max(expires_at),
                }),
                None => Some(StrengthEffect { delta, expires_at }),
            };
        }
        self.note_status_effect_tick(expires_at);
    }

    fn apply_monster_strength_effect(
        &mut self,
        monster_id: CreatureId,
        delta: i16,
        duration_ticks: u64,
        now: GameTick,
    ) {
        let expires_at = GameTick(now.0.saturating_add(duration_ticks));
        {
            let monster = match self.monsters.get_mut(&monster_id) {
                Some(monster) => monster,
                None => return,
            };
            monster.strength_effect = match monster.strength_effect {
                Some(existing) => Some(StrengthEffect {
                    delta,
                    expires_at: existing.expires_at.max(expires_at),
                }),
                None => Some(StrengthEffect { delta, expires_at }),
            };
        }
        self.note_status_effect_tick(expires_at);
    }

    fn direction_components(direction: Direction) -> Option<(Direction, Direction)> {
        match direction {
            Direction::Northeast => Some((Direction::North, Direction::East)),
            Direction::Northwest => Some((Direction::North, Direction::West)),
            Direction::Southeast => Some((Direction::South, Direction::East)),
            Direction::Southwest => Some((Direction::South, Direction::West)),
            _ => None,
        }
    }

    pub fn cast_spell(
        &mut self,
        caster_id: PlayerId,
        spell: &Spell,
        target_position: Option<Position>,
        direction: Option<Direction>,
        clock: &GameClock,
    ) -> Result<SpellCastReport, String> {
        self.cast_spell_inner(
            caster_id,
            spell,
            target_position,
            direction,
            clock,
            SpellCostMode::Standard,
            None,
        )
    }

    pub fn cast_spell_words(
        &mut self,
        caster_id: PlayerId,
        words: &str,
        target_position: Option<Position>,
        direction: Option<Direction>,
        clock: &GameClock,
    ) -> Result<SpellCastReport, String> {
        let words = words.trim();
        if words.is_empty() {
            return Err("spell cast failed: empty words".to_string());
        }
        let spell = self
            .spellbook
            .get_by_input(words)
            .cloned()
            .ok_or_else(|| "spell cast failed: unknown words".to_string())?;
        {
            let caster = self
                .players
                .get(&caster_id)
                .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
            if !caster.knows_spell(spell.id) {
                return Err("spell cast failed: spell not known".to_string());
            }
        }
        if spell.kind == SpellKind::Rune && spell.rune_type_id.is_some() {
            return Err("spell cast failed: rune requires item".to_string());
        }
        let spell_args = Self::spell_args_from_input(&spell, words)?;
        self.cast_spell_inner(
            caster_id,
            &spell,
            target_position,
            direction,
            clock,
            SpellCostMode::Standard,
            Some(spell_args),
        )
    }

    fn spell_args_from_input(spell: &Spell, words: &str) -> Result<Vec<String>, String> {
        let def_tokens = spell_word_tokens(&spell.words);
        let input_tokens = spell_word_tokens(words);
        let placeholder_positions: Vec<usize> = def_tokens
            .iter()
            .enumerate()
            .filter(|(_, token)| token.quoted)
            .map(|(index, _)| index)
            .collect();
        if placeholder_positions.is_empty() {
            return Ok(Vec::new());
        }
        if input_tokens.len() < def_tokens.len() {
            return Err("spell cast failed: missing parameter".to_string());
        }

        let mut args = Vec::new();
        for (index, token) in def_tokens.iter().enumerate() {
            if !token.quoted {
                continue;
            }
            let value = input_tokens
                .get(index)
                .ok_or_else(|| "spell cast failed: missing parameter".to_string())?;
            args.push(value.text.clone());
        }

        if input_tokens.len() > def_tokens.len() {
            let last_placeholder = *placeholder_positions.last().unwrap_or(&0);
            if last_placeholder != def_tokens.len().saturating_sub(1) {
                return Err("spell cast failed: invalid parameters".to_string());
            }
            if let Some(last) = args.pop() {
                let mut combined = vec![last];
                for token in input_tokens.iter().skip(def_tokens.len()) {
                    combined.push(token.text.clone());
                }
                args.push(combined.join(" "));
            }
        }

        Ok(args)
    }

    pub fn cast_rune(
        &mut self,
        caster_id: PlayerId,
        spell: &Spell,
        rune_slot: InventorySlot,
        target_position: Option<Position>,
        direction: Option<Direction>,
        clock: &GameClock,
    ) -> Result<SpellCastReport, String> {
        if spell.kind != SpellKind::Rune {
            return Err("rune cast failed: spell is not a rune".to_string());
        }

        {
            let player = self
                .players
                .get(&caster_id)
                .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
            let stack = player
                .inventory
                .slot(rune_slot)
                .ok_or_else(|| "rune cast failed: inventory slot empty".to_string())?;
            let kind = self
                .item_types
                .as_ref()
                .and_then(|item_types| item_types.get(stack.type_id))
                .map(|item| item.kind)
                .ok_or_else(|| "rune cast failed: item type missing".to_string())?;
            if kind != ItemKind::Rune {
                return Err("rune cast failed: item is not a rune".to_string());
            }
            let expected_rune = spell
                .rune_type_id
                .ok_or_else(|| "rune cast failed: spell missing rune item id".to_string())?;
            if stack.type_id != expected_rune {
                return Err("rune cast failed: wrong rune".to_string());
            }
        }

        let report = self.cast_spell_inner(
            caster_id,
            spell,
            target_position,
            direction,
            clock,
            SpellCostMode::Rune,
            None,
        )?;

        let player = self
            .players
            .get_mut(&caster_id)
            .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
        player.inventory.remove_item(rune_slot, 1)?;

        Ok(report)
    }

    fn cast_spell_inner(
        &mut self,
        caster_id: PlayerId,
        spell: &Spell,
        target_position: Option<Position>,
        direction: Option<Direction>,
        clock: &GameClock,
        cost_mode: SpellCostMode,
        spell_args: Option<Vec<String>>,
    ) -> Result<SpellCastReport, String> {
        let caster_position = self
            .players
            .get(&caster_id)
            .ok_or_else(|| format!("unknown player {:?}", caster_id))?
            .position;

        let spell_args = spell_args.as_deref();
        let conjure = spell.conjure;
        let antidote = spell.antidote;
        let magic_rope = spell.magic_rope;
        let find_person = spell.find_person;
        let enchant_staff = spell.enchant_staff;

        if let Some(summon) = spell.summon {
            return self.cast_summon_spell(
                caster_id,
                spell,
                summon,
                caster_position,
                target_position,
                clock,
                cost_mode,
                spell_args,
            );
        }

        let mut positions = Vec::new();
        let mut target_positions = Vec::new();
        let mut target_position = target_position;
        let mut player_targets = std::collections::HashSet::new();
        let mut monster_targets = std::collections::HashSet::new();
        let mut offensive = false;
        let effect = spell.effect;
        let dispel = spell.dispel;
        let field = spell.field;
        if spell.target == SpellTarget::Creature && target_position.is_none() {
            if let Some(name) = spell_args.and_then(|args| args.first()) {
                target_position = self.resolve_creature_position_by_name(name);
                if target_position.is_none() {
                    return Err("spell cast failed: target creature missing".to_string());
                }
            }
        }
        let find_person_target = if find_person.is_some() {
            let name = spell_args
                .and_then(|args| args.first())
                .ok_or_else(|| "spell cast failed: missing parameter".to_string())?;
            let target = self
                .players
                .values()
                .find(|player| player.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| "spell cast failed: target creature missing".to_string())?;
            Some((target.name.clone(), target.position))
        } else {
            None
        };
        let magic_rope_target = if magic_rope.is_some() {
            let target = target_position.unwrap_or(caster_position);
            let tile = self
                .map
                .tile(target)
                .ok_or_else(|| "spell cast failed: not possible".to_string())?;
            let destination = match self.tile_floor_change(tile) {
                Some(FloorChange::Down) => self
                    .apply_floor_change(target, FloorChange::Up)
                    .ok_or_else(|| "spell cast failed: not possible".to_string())?,
                _ => return Err("spell cast failed: not possible".to_string()),
            };
            Some(destination)
        } else {
            None
        };
        let enchant_slot = if let Some(enchant) = enchant_staff {
            let player = self
                .players
                .get(&caster_id)
                .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
            let right = player
                .inventory
                .slot(InventorySlot::RightHand)
                .filter(|stack| stack.type_id == enchant.source_type_id)
                .map(|_| InventorySlot::RightHand);
            let left = player
                .inventory
                .slot(InventorySlot::LeftHand)
                .filter(|stack| stack.type_id == enchant.source_type_id)
                .map(|_| InventorySlot::LeftHand);
            Some(
                right
                    .or(left)
                    .ok_or_else(|| "spell cast failed: missing staff".to_string())?,
            )
        } else {
            None
        };
        let target_shape = effect
            .map(|effect| effect.shape)
            .or_else(|| spell.haste.map(|haste| haste.shape))
            .or_else(|| dispel.map(|effect| effect.shape))
            .or_else(|| field.map(|effect| effect.shape))
            .or_else(|| antidote.map(|effect| effect.shape));
        if let Some(shape) = target_shape {
            let center = match spell.target {
                SpellTarget::SelfOnly => caster_position,
                _ => target_position.unwrap_or(caster_position),
            };

            target_positions = match shape {
                SpellShape::Area { radius } => {
                    circle_positions(self.circles.as_ref(), center, radius)
                }
                SpellShape::Line { length } => {
                    let direction = direction.ok_or_else(|| {
                        "spell cast failed: direction required for line spell".to_string()
                    })?;
                    line_positions(center, direction, length)
                }
                SpellShape::Cone {
                    range,
                    angle_degrees,
                } => {
                    let direction = direction.ok_or_else(|| {
                        "spell cast failed: direction required for cone spell".to_string()
                    })?;
                    cone_positions(center, direction, range, angle_degrees)
                }
            };
            if effect.is_some() || field.is_some() {
                positions = target_positions.clone();
            }

            offensive = effect
                .map(|effect| {
                    effect.kind == SpellEffectKind::Damage
                        && (effect.min_damage > 0 || effect.max_damage > 0)
                })
                .unwrap_or(false)
                || field.is_some()
                || spell
                    .haste
                    .map(|haste| {
                        haste.speed_delta < 0
                            || haste.speed_percent.map(|value| value < 0).unwrap_or(false)
                    })
                    .unwrap_or(false);
            if offensive {
                let area_has_pz = target_positions
                    .iter()
                    .any(|position| self.is_protection_zone(*position));
                if self.is_protection_zone(caster_position) || area_has_pz {
                    return Err("spell cast failed: protection zone".to_string());
                }
            }

            for position in &target_positions {
                if !self.map.tiles.is_empty() && !self.map.has_tile(*position) {
                    continue;
                }
                for (id, player) in self.players.iter() {
                    if player.position == *position {
                        player_targets.insert(*id);
                    }
                }
                for (id, monster) in self.monsters.iter() {
                    if monster.position == *position {
                        monster_targets.insert(*id);
                    }
                }
            }
            let include_caster = effect
                .map(|effect| effect.include_caster)
                .or_else(|| spell.haste.map(|haste| haste.include_caster))
                .or_else(|| antidote.map(|effect| effect.include_caster))
                .unwrap_or(true);
            if !include_caster {
                player_targets.remove(&caster_id);
            }

            if offensive && !self.combat_rules.pvp_enabled {
                if player_targets
                    .iter()
                    .any(|target_id| *target_id != caster_id)
                {
                    return Err("spell cast failed: pvp disabled".to_string());
                }
            }
        }

        let levitate_target = if spell.levitate.is_some() {
            Some(self.resolve_levitate_target(
                caster_position,
                direction,
                spell_args,
            )?)
        } else {
            None
        };

        let resolved_outfit = if let Some(outfit) = spell.outfit {
            match outfit {
                SpellOutfitEffect::CreatureName { duration_ms } => {
                    let arg = spell_args
                        .and_then(|args| args.first())
                        .ok_or_else(|| "spell cast failed: missing parameter".to_string())?;
                    let outfit = self.resolve_creature_illusion_outfit(arg)?;
                    Some((outfit, duration_ms))
                }
                SpellOutfitEffect::Chameleon { duration_ms } => {
                    let position = target_position.unwrap_or(caster_position);
                    let outfit = self
                        .resolve_chameleon_outfit(position)
                        .ok_or_else(|| "spell cast failed: not possible".to_string())?;
                    Some((outfit, duration_ms))
                }
                _ => None,
            }
        } else {
            None
        };

        let raise_dead_race = if let Some(effect) = spell.raise_dead.as_ref() {
            self.resolve_monster_race_by_name(&effect.creature_name)
        } else {
            None
        };
        if spell.raise_dead.is_some() && raise_dead_race.is_none() {
            return Err("spell cast failed: unknown creature".to_string());
        }

        if effect.is_none()
            && spell.haste.is_none()
            && spell.light.is_none()
            && spell.dispel.is_none()
            && spell.field.is_none()
            && spell.magic_shield.is_none()
            && spell.outfit.is_none()
            && spell.challenge.is_none()
            && spell.levitate.is_none()
            && spell.raise_dead.is_none()
            && conjure.is_none()
            && antidote.is_none()
            && magic_rope.is_none()
            && find_person.is_none()
            && enchant_staff.is_none()
        {
            self.apply_spell_costs(caster_id, spell, clock, cost_mode)?;
            return Ok(SpellCastReport {
                positions: Vec::new(),
                hits: Vec::new(),
                text_effects: Vec::new(),
                messages: Vec::new(),
                speed_updates: Vec::new(),
                light_updates: Vec::new(),
                outfit_updates: Vec::new(),
                refresh_map: false,
            });
        }

        self.apply_spell_costs(caster_id, spell, clock, cost_mode)?;

        let mut messages = Vec::new();
        if let Some((name, position)) = find_person_target {
            messages.push(SpellCastMessage {
                message_type: 0x14,
                message: self.find_person_message(caster_position, position, &name),
            });
            return Ok(SpellCastReport {
                positions: Vec::new(),
                hits: Vec::new(),
                text_effects: Vec::new(),
                messages,
                speed_updates: Vec::new(),
                light_updates: Vec::new(),
                outfit_updates: Vec::new(),
                refresh_map: false,
            });
        }

        if let Some(conjure) = conjure {
            self.add_item_to_player(caster_id, conjure.item_type_id, conjure.count)?;
            return Ok(SpellCastReport {
                positions: Vec::new(),
                hits: Vec::new(),
                text_effects: Vec::new(),
                messages,
                speed_updates: Vec::new(),
                light_updates: Vec::new(),
                outfit_updates: Vec::new(),
                refresh_map: false,
            });
        }

        if let Some(enchant) = enchant_staff {
            let slot = enchant_slot.ok_or_else(|| "spell cast failed: missing staff".to_string())?;
            let count = self
                .players
                .get(&caster_id)
                .and_then(|player| player.inventory.slot(slot).map(|stack| stack.count))
                .unwrap_or(1);
            let enchanted = ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: enchant.enchanted_type_id,
                count,
                attributes: Vec::new(),
                contents: Vec::new(),
            };
            self.schedule_cron_for_item_tree(&enchanted);
            let player = self
                .players
                .get_mut(&caster_id)
                .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
            player.inventory.set_slot(
                slot,
                Some(enchanted),
            );
            return Ok(SpellCastReport {
                positions: Vec::new(),
                hits: Vec::new(),
                text_effects: Vec::new(),
                messages,
                speed_updates: Vec::new(),
                light_updates: Vec::new(),
                outfit_updates: Vec::new(),
                refresh_map: false,
            });
        }

        if magic_rope.is_some() {
            let destination = magic_rope_target
                .ok_or_else(|| "spell cast failed: not possible".to_string())?;
            self.teleport_player(caster_id, destination)?;
            return Ok(SpellCastReport {
                positions: Vec::new(),
                hits: Vec::new(),
                text_effects: Vec::new(),
                messages,
                speed_updates: Vec::new(),
                light_updates: Vec::new(),
                outfit_updates: Vec::new(),
                refresh_map: true,
            });
        }

        const HEAL_TEXT_COLOR: u8 = 4;
        let mut hits = Vec::new();
        let mut text_effects = Vec::new();
        if let Some(effect) = effect {
            match effect.kind {
                SpellEffectKind::Damage => {
                    for target_id in &player_targets {
                        let attempted_damage = self.roll_spell_amount(caster_id, spell, effect);
                        let reduced_damage = self.apply_player_protection_reduction(
                            *target_id,
                            effect.damage_type,
                            attempted_damage,
                        );
                        let applied_damage = {
                            let target = self
                                .players
                                .get_mut(target_id)
                                .ok_or_else(|| format!("unknown player {:?}", target_id))?;
                            let (applied, _) = target.apply_damage_with_magic_shield(
                                effect.damage_type,
                                reduced_damage,
                            );
                            applied
                        };
                        hits.push(SpellHit {
                            target: SpellTargetId::Player(*target_id),
                            attempted_damage,
                            applied_damage,
                        });
                    }
                    for monster_id in &monster_targets {
                        let Some(monster) = self.monsters.get(monster_id) else {
                            continue;
                        };
                        let before = monster.stats.health;
                        let attempted_damage = self.roll_spell_amount(caster_id, spell, effect);
                        let _ = self.apply_damage_to_monster(
                            *monster_id,
                            effect.damage_type,
                            attempted_damage,
                            Some(caster_id),
                        );
                        let after = self
                            .monsters
                            .get(monster_id)
                            .map(|monster| monster.stats.health)
                            .unwrap_or(0);
                        hits.push(SpellHit {
                            target: SpellTargetId::Monster(*monster_id),
                            attempted_damage,
                            applied_damage: before.saturating_sub(after),
                        });
                    }
                }
                SpellEffectKind::Healing => {
                    for target_id in &player_targets {
                        let attempted_heal = self.roll_spell_amount(caster_id, spell, effect);
                        let (applied_heal, position) = {
                            let target = self
                                .players
                                .get_mut(target_id)
                                .ok_or_else(|| format!("unknown player {:?}", target_id))?;
                            let position = target.position;
                            let applied_heal = target.stats.apply_heal(attempted_heal);
                            (applied_heal, position)
                        };
                        if applied_heal > 0 {
                            text_effects.push(SpellTextEffect {
                                position,
                                color: HEAL_TEXT_COLOR,
                                message: format!("+{applied_heal}"),
                            });
                        }
                        hits.push(SpellHit {
                            target: SpellTargetId::Player(*target_id),
                            attempted_damage: attempted_heal,
                            applied_damage: applied_heal,
                        });
                    }
                    for monster_id in &monster_targets {
                        let attempted_heal = self.roll_spell_amount(caster_id, spell, effect);
                        let Some(monster) = self.monsters.get_mut(monster_id) else {
                            continue;
                        };
                        let position = monster.position;
                        let applied_heal = monster.stats.apply_heal(attempted_heal);
                        if applied_heal > 0 {
                            text_effects.push(SpellTextEffect {
                                position,
                                color: HEAL_TEXT_COLOR,
                                message: format!("+{applied_heal}"),
                            });
                        }
                        hits.push(SpellHit {
                            target: SpellTargetId::Monster(*monster_id),
                            attempted_damage: attempted_heal,
                            applied_damage: applied_heal,
                        });
                    }
                }
            }
        }
        if let Some(antidote) = antidote {
            for target_id in &player_targets {
                if let Some(target) = self.players.get_mut(target_id) {
                    target.clear_condition(antidote.kind);
                    if let Some(skill_id) = Self::condition_skill_id(antidote.kind) {
                        if target.raw_skills.is_empty() {
                            target.raw_skills = skill_rows_from_player(target);
                        }
                        if let Some(index) =
                            target.raw_skills.iter().position(|row| row.skill_id == skill_id)
                        {
                            Self::clear_skill_timer(&mut target.raw_skills[index]);
                        }
                    }
                }
            }
        }

        let mut refresh_map = false;
        if let Some(dispel) = dispel {
            if dispel.remove_magic_fields {
                for position in &target_positions {
                    if self.remove_magic_fields_at_position(*position) {
                        refresh_map = true;
                    }
                }
            }
            if dispel.remove_items {
                for position in &target_positions {
                    if self.remove_dispersable_item_at_position(*position) {
                        refresh_map = true;
                    }
                }
            }
        }

        if let Some(field) = field {
            if let Some(type_id) = Self::field_item_type_id(u16::from(field.field_kind)) {
                let stackable = self
                    .item_types
                    .as_ref()
                    .and_then(|item_types| item_types.get(type_id))
                    .map(|item| item.stackable)
                    .unwrap_or(false);
                let mut stack = ItemStack { id: crate::entities::item::ItemId::next(),
                    type_id,
                    count: 1,
                    attributes: Vec::new(),
                    contents: Vec::new(),
};
                let _ = set_itemstack_attribute_u32(
                    &mut stack,
                    "Responsible",
                    caster_id.0.min(i32::MAX as u32) as i32,
                    ItemAttribute::Responsible,
                );
                self.schedule_cron_for_item_tree(&stack);
                let movable = self.item_is_movable(&stack);
                let mut placed_any = false;
                for position in &target_positions {
                    if self.is_protection_zone(*position) {
                        continue;
                    }
                    if !self.map.tiles.is_empty() && !self.map.has_tile(*position) {
                        continue;
                    }
                    let Some(tile) = self.map.tile_mut(*position) else {
                        continue;
                    };
                    if place_on_tile_with_dustbin(tile, stack.clone(), stackable, movable)
                        .is_ok()
                    {
                        placed_any = true;
                    }
                }
                if placed_any {
                    refresh_map = true;
                }
            }
        }

        if let Some(effect) = spell.raise_dead.as_ref() {
            let race_number = raise_dead_race.ok_or_else(|| "spell cast failed: unknown creature".to_string())?;
            let center = match spell.target {
                SpellTarget::SelfOnly => caster_position,
                _ => target_position.unwrap_or(caster_position),
            };
            if self.raise_dead_from_corpses(
                caster_id,
                center,
                effect.radius,
                race_number,
            ) {
                refresh_map = true;
            }
        }

        let mut speed_updates = Vec::new();
        if let Some(haste) = spell.haste {
            let duration = Duration::from_millis(u64::from(haste.duration_ms));
            let duration_ticks = clock.ticks_from_duration_round_up(duration);
            if duration_ticks > 0 {
                let mut updates = Vec::new();
                for target_id in &player_targets {
                    self.apply_player_speed_effect(
                        *target_id,
                        haste.speed_delta,
                        haste.speed_percent,
                        duration_ticks,
                        &mut updates,
                        clock.now(),
                    );
                }
                for target_id in &monster_targets {
                    self.apply_monster_speed_effect(
                        *target_id,
                        haste.speed_delta,
                        haste.speed_percent,
                        duration_ticks,
                        &mut updates,
                        clock.now(),
                    );
                }
                speed_updates.extend(
                    updates
                        .into_iter()
                        .map(|update| SpellSpeedUpdate {
                            id: update.id,
                            speed: update.speed,
                        }),
                );
            }
        }

        let mut light_updates = Vec::new();
        if let Some(light) = spell.light {
            let duration = Duration::from_millis(u64::from(light.duration_ms));
            let duration_ticks = clock.ticks_from_duration_round_up(duration);
            let mut updates = Vec::new();
            self.apply_player_light_effect(
                caster_id,
                light.level,
                light.color,
                duration_ticks,
                &mut updates,
                clock.now(),
                clock,
            );
            light_updates.extend(
                updates
                    .into_iter()
                    .map(|update| SpellLightUpdate {
                        id: update.id,
                        level: update.level,
                        color: update.color,
                    }),
            );
        }

        if let Some(shield) = spell.magic_shield {
            let duration = Duration::from_millis(u64::from(shield.duration_ms));
            let duration_ticks = clock.ticks_from_duration_round_up(duration);
            self.apply_player_magic_shield_effect(caster_id, duration_ticks, clock.now(), clock);
        }

        let mut outfit_updates = Vec::new();
        if let Some(outfit) = spell.outfit {
            match outfit {
                SpellOutfitEffect::Apply { outfit, duration_ms } => {
                    let duration = Duration::from_millis(u64::from(duration_ms));
                    let duration_ticks = clock.ticks_from_duration_round_up(duration);
                    if duration_ticks > 0 {
                        self.apply_player_outfit_effect(
                            caster_id,
                            outfit,
                            duration_ticks,
                            &mut outfit_updates,
                            clock.now(),
                            clock,
                        );
                    }
                }
                SpellOutfitEffect::Cancel => {
                    self.clear_player_outfit_effect(caster_id, &mut outfit_updates);
                }
                SpellOutfitEffect::CreatureName { duration_ms }
                | SpellOutfitEffect::Chameleon { duration_ms } => {
                    let (outfit, _) = resolved_outfit
                        .ok_or_else(|| "spell cast failed: not possible".to_string())?;
                    let duration = Duration::from_millis(u64::from(duration_ms));
                    let duration_ticks = clock.ticks_from_duration_round_up(duration);
                    if duration_ticks > 0 {
                        self.apply_player_outfit_effect(
                            caster_id,
                            outfit,
                            duration_ticks,
                            &mut outfit_updates,
                            clock.now(),
                            clock,
                        );
                    }
                }
            }
        }

        if let Some(challenge) = spell.challenge {
            self.apply_challenge(caster_id, caster_position, challenge.radius);
        }

        if let Some(target) = levitate_target {
            self.teleport_player(caster_id, target)?;
            refresh_map = true;
        }

        if offensive && !hits.is_empty() {
            let fight_timer = self.combat_rules.fight_timer;
            let skull_timer = self.combat_rules.white_skull_timer;
            if let Some(caster) = self.players.get_mut(&caster_id) {
                caster.mark_in_combat(clock, fight_timer);
                if hits.iter().any(|hit| {
                    matches!(hit.target, SpellTargetId::Player(target_id) if target_id != caster_id)
                }) {
                    caster.mark_white_skull(clock, skull_timer);
                }
            }
            for hit in &hits {
                if let SpellTargetId::Player(target_id) = hit.target {
                    if target_id == caster_id {
                        continue;
                    }
                    if let Some(target) = self.players.get_mut(&target_id) {
                        target.mark_in_combat(clock, fight_timer);
                    }
                }
            }
        }

        Ok(SpellCastReport {
            positions,
            hits,
            text_effects,
            messages,
            speed_updates,
            light_updates,
            outfit_updates: outfit_updates
                .into_iter()
                .map(|update| SpellOutfitUpdate {
                    id: update.id,
                    outfit: update.outfit,
                })
                .collect(),
            refresh_map,
        })
    }

    fn apply_spell_costs(
        &mut self,
        caster_id: PlayerId,
        spell: &Spell,
        clock: &GameClock,
        cost_mode: SpellCostMode,
    ) -> Result<(), String> {
        let caster = self
            .players
            .get_mut(&caster_id)
            .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
        match cost_mode {
            SpellCostMode::Standard => {
                caster.check_spell_requirements(spell, clock)?;
                caster.spend_spell_costs(spell)?;
                caster.trigger_spell_cooldowns(spell, clock);
            }
            SpellCostMode::Rune => {
                caster.check_spell_requirements_no_costs(spell, clock)?;
                caster.trigger_spell_cooldowns(spell, clock);
            }
        }
        Ok(())
    }

    fn cast_summon_spell(
        &mut self,
        caster_id: PlayerId,
        spell: &Spell,
        summon: SummonSpellEffect,
        caster_position: Position,
        target_position: Option<Position>,
        clock: &GameClock,
        cost_mode: SpellCostMode,
        spell_args: Option<&[String]>,
    ) -> Result<SpellCastReport, String> {
        let count = if summon.convince { 1 } else { summon.count };
        if count == 0 {
            return Ok(SpellCastReport {
                positions: Vec::new(),
                hits: Vec::new(),
                text_effects: Vec::new(),
                messages: Vec::new(),
                speed_updates: Vec::new(),
                light_updates: Vec::new(),
                outfit_updates: Vec::new(),
                refresh_map: false,
            });
        }

        let current_summons = self.player_summon_count(caster_id);
        if current_summons >= PLAYER_SUMMON_LIMIT {
            return Err("spell cast failed: summon limit".to_string());
        }
        if usize::from(count) + current_summons > PLAYER_SUMMON_LIMIT {
            return Err("spell cast failed: summon limit".to_string());
        }

        let mut target_monster_id = None;
        let race_number = if summon.convince {
            let target_position = target_position
                .ok_or_else(|| "spell cast failed: target required".to_string())?;
            let monster_id = self
                .monster_id_at_position(target_position)
                .ok_or_else(|| "spell cast failed: target creature missing".to_string())?;
            let monster = self
                .monsters
                .get(&monster_id)
                .ok_or_else(|| "spell cast failed: target creature missing".to_string())?;
            target_monster_id = Some(monster_id);
            monster.race_number
        } else {
            match summon.race_number {
                Some(race_number) => race_number,
                None => {
                    let name = spell_args
                        .and_then(|args| args.first())
                        .ok_or_else(|| "spell cast failed: missing parameter".to_string())?;
                    self.resolve_monster_race_by_name(name)
                        .ok_or_else(|| "spell cast failed: unknown creature".to_string())?
                }
            }
        };

        if summon.convince {
            if self.race_blocks_convince(race_number) {
                return Err("spell cast failed: target cannot be convinced".to_string());
            }
        } else if self.race_blocks_summon(race_number) {
            return Err("spell cast failed: target cannot be summoned".to_string());
        }

        let extra_cost = self
            .race_summon_cost(race_number)
            .saturating_mul(u32::from(count));

        {
            let caster = self
                .players
                .get_mut(&caster_id)
                .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
            match cost_mode {
                SpellCostMode::Standard => {
                    caster.check_spell_requirements(spell, clock)?;
                    let total_mana = u32::from(spell.mana_cost).saturating_add(extra_cost);
                    if caster.stats.mana < total_mana {
                        return Err("spell cast failed: insufficient mana".to_string());
                    }
                    caster.spend_spell_costs(spell)?;
                    if extra_cost > 0 {
                        caster.stats.mana = caster.stats.mana.saturating_sub(extra_cost);
                    }
                    caster.trigger_spell_cooldowns(spell, clock);
                }
                SpellCostMode::Rune => {
                    caster.check_spell_requirements_no_costs(spell, clock)?;
                    caster.trigger_spell_cooldowns(spell, clock);
                }
            }
        }

        if summon.convince {
            let Some(monster_id) = target_monster_id else {
                return Err("spell cast failed: target creature missing".to_string());
            };
            if let Some(monster) = self.monsters.get_mut(&monster_id) {
                monster.summoner = Some(caster_id);
                monster.target = None;
                monster.damage_by.clear();
                monster.talk_lines.clear();
            }
        } else {
            let spawned = self.player_spawn_summons(caster_id, caster_position, race_number, count);
            if spawned > 0 {
                return Ok(SpellCastReport {
                    positions: Vec::new(),
                    hits: Vec::new(),
                    text_effects: Vec::new(),
                    messages: Vec::new(),
                    speed_updates: Vec::new(),
                    light_updates: Vec::new(),
                    outfit_updates: Vec::new(),
                    refresh_map: true,
                });
            }
        }

        Ok(SpellCastReport {
            positions: Vec::new(),
            hits: Vec::new(),
            text_effects: Vec::new(),
            messages: Vec::new(),
            speed_updates: Vec::new(),
            light_updates: Vec::new(),
            outfit_updates: Vec::new(),
            refresh_map: false,
        })
    }

    fn remove_magic_fields_at_position(&mut self, position: Position) -> bool {
        let Some(tile) = self.map.tile_mut(position) else {
            return false;
        };
        let mut removed = false;
        let mut idx = 0;
        while idx < tile.items.len() {
            let type_id = tile.items[idx].type_id;
            let is_magic_field = self
                .object_types
                .as_ref()
                .and_then(|index| index.get(type_id))
                .map(|object| object.has_flag("MagicField"))
                .unwrap_or(false);
            if is_magic_field {
                remove_item_at_index(tile, idx);
                removed = true;
            } else {
                idx += 1;
            }
        }
        removed
    }

    fn remove_dispersable_item_at_position(&mut self, position: Position) -> bool {
        let Some(tile) = self.map.tile_mut(position) else {
            return false;
        };
        let mut idx = 0;
        while idx < tile.items.len() {
            let type_id = tile.items[idx].type_id;
            let Some(object) = self
                .object_types
                .as_ref()
                .and_then(|index| index.get(type_id))
            else {
                idx += 1;
                continue;
            };
            if object.has_flag("Bottom") {
                idx += 1;
                continue;
            }
            if !object.has_flag("Take") {
                idx += 1;
                continue;
            }
            remove_item_at_index(tile, idx);
            return true;
        }
        false
    }

    fn is_protection_zone(&self, position: Position) -> bool {
        self.map
            .tile(position)
            .map(|tile| tile.protection_zone)
            .unwrap_or(false)
    }

    fn roll_spell_amount(&mut self, caster_id: PlayerId, spell: &Spell, effect: SpellEffect) -> u32 {
        let Some(base_damage) = effect.base_damage else {
            return self.roll_spell_amount_fallback(effect);
        };
        let Some(variance) = effect.variance else {
            return self.roll_spell_amount_fallback(effect);
        };
        let Some(caster) = self.players.get(&caster_id) else {
            return self.roll_spell_amount_fallback(effect);
        };
        let variance = variance.abs();
        let random_offset = if variance == 0 {
            0
        } else {
            let span = (variance as u32).saturating_mul(2);
            self.moveuse_rng.roll_range(0, span) as i32 - variance
        };
        // Match ComputeDamage scaling from game.orig (magic level + level with clamp flags).
        let mut scaled = compute_damage(
            base_damage,
            variance,
            i32::from(caster.skills.magic.level),
            i32::from(caster.level),
            spell.damage_scale_flags,
            random_offset,
        );
        if spell.words == "exori" {
            let level = i64::from(caster.level);
            let scaled_value = (i64::from(scaled) * level) / 25;
            scaled = scaled_value
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
        }
        scaled.max(0) as u32
    }

    fn roll_spell_amount_fallback(&mut self, effect: SpellEffect) -> u32 {
        let min_damage = effect.min_damage.min(effect.max_damage);
        let max_damage = effect.max_damage.max(effect.min_damage);
        if min_damage == max_damage {
            return min_damage;
        }
        self.moveuse_rng.roll_range(min_damage, max_damage)
    }

    pub fn pickup_from_tile(
        &mut self,
        id: PlayerId,
        position: Position,
        type_id: ItemTypeId,
        count: u16,
    ) -> Result<InventorySlot, String> {
        self.ensure_player_in_range(id, position)?;
        self.ensure_house_item_access(id, position)?;
        let stackable = self
            .item_types
            .as_ref()
            .and_then(|item_types| item_types.get(type_id))
            .map(|item| item.stackable)
            .unwrap_or(false);

        let removed = {
            let tile = self
                .map
                .tile_mut(position)
                .ok_or_else(|| "tile missing".to_string())?;
            take_from_tile(tile, type_id, count, stackable)?
        };

        let slot = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            player.inventory.add_item(removed, stackable)?
        };

        Ok(slot)
    }

    pub fn pickup_to_inventory_slot(
        &mut self,
        id: PlayerId,
        position: Position,
        type_id: ItemTypeId,
        count: u16,
        slot: InventorySlot,
    ) -> Result<Vec<ContainerUpdate>, String> {
        self.ensure_player_in_range(id, position)?;
        self.ensure_house_item_access(id, position)?;
        let stackable = self
            .item_types
            .as_ref()
            .and_then(|item_types| item_types.get(type_id))
            .map(|item| item.stackable)
            .unwrap_or(false);
        let use_container_slot = self.slot_container_capacity(id, slot).is_some();
        if !use_container_slot {
            self.ensure_slot_allows_item(slot, type_id)?;
            self.ensure_two_handed_slot_free(id, slot, None, type_id)?;
        }

        let removed = {
            let tile = self
                .map
                .tile_mut(position)
                .ok_or_else(|| "tile missing".to_string())?;
            take_from_tile(tile, type_id, count, stackable)?
        };
        let movable = self.item_is_movable(&removed);
        let should_close = self.is_container_item(&removed);
        let close_snapshot = if should_close {
            Some(removed.clone())
        } else {
            None
        };

        let object_types = self
            .object_types
            .as_ref()
            .ok_or_else(|| "Item weight data missing.".to_string())?;
        let added_weight = item_stack_total_weight(object_types, &removed);
        let player = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        if let Err(err) = ensure_capacity_for_weight(player, object_types, added_weight) {
            if let Some(tile) = self.map.tile_mut(position) {
                let _ = place_on_tile_with_dustbin(tile, removed, stackable, movable);
            }
            return Err(err);
        }

        let mut updates = Vec::new();
        if use_container_slot {
            match self.insert_into_inventory_slot_container(id, slot, removed.clone(), stackable) {
                Ok(Some(update)) => updates.push(update),
                Ok(None) => {}
                Err(err) => {
                    if let Some(tile) = self.map.tile_mut(position) {
                        let _ = place_on_tile_with_dustbin(tile, removed, stackable, movable);
                    }
                    return Err(err);
                }
            }
        } else {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            if let Err(err) = player
                .inventory
                .add_item_to_slot(slot, removed.clone(), stackable)
            {
                if let Some(tile) = self.map.tile_mut(position) {
                    let _ = place_on_tile_with_dustbin(tile, removed, stackable, movable);
                }
                return Err(err);
            }
        }

        if let Some(snapshot) = close_snapshot {
            self.close_open_containers_for_item(id, &snapshot);
        }
        Ok(updates)
    }

    fn take_inventory_item_with_contents(
        &mut self,
        player_id: PlayerId,
        slot: InventorySlot,
        count: u16,
    ) -> Result<ItemStack, String> {
        let mut removed = {
            let player = self
                .players
                .get_mut(&player_id)
                .ok_or_else(|| format!("unknown player {:?}", player_id))?;
            player.inventory.remove_item(slot, count)?
        };
        if count != removed.count {
            return Ok(removed);
        }
        let has_slot_contents = self
            .players
            .get(&player_id)
            .map(|player| {
                player.inventory_containers.contains_key(&slot)
                    || player
                        .open_containers
                        .values()
                        .any(|container| container.source_slot == Some(slot))
            })
            .unwrap_or(false);
        let is_container = self
            .item_types
            .as_ref()
            .and_then(|types| types.get(removed.type_id))
            .map(|entry| entry.kind == ItemKind::Container)
            .unwrap_or(false)
            || has_slot_contents
            || !removed.contents.is_empty();
        if !is_container {
            return Ok(removed);
        }
        let fallback_contents = removed.contents.clone();
        let contents = {
            let player = self
                .players
                .get_mut(&player_id)
                .ok_or_else(|| format!("unknown player {:?}", player_id))?;
            let open_container_id = player.open_containers.iter().find_map(|(id, container)| {
                if container.source_slot == Some(slot) {
                    Some(*id)
                } else {
                    None
                }
            });
            let contents = if let Some(container_id) = open_container_id {
                player
                    .open_containers
                    .get(&container_id)
                    .map(|container| container.items.clone())
                    .unwrap_or_default()
            } else {
                player
                    .inventory_containers
                    .remove(&slot)
                    .unwrap_or(fallback_contents)
            };
            if open_container_id.is_some() {
                player.inventory_containers.remove(&slot);
            }
            contents
        };
        removed.contents = contents;
        Ok(removed)
    }

    pub fn drop_to_tile(
        &mut self,
        id: PlayerId,
        position: Position,
        slot: InventorySlot,
        count: u16,
    ) -> Result<(), String> {
        let origin = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?
            .position;
        self.ensure_player_can_throw_to(id, origin, position)?;
        self.ensure_house_item_access(id, position)?;
        let open_container_id = self.players.get(&id).and_then(|player| {
            player
                .open_containers
                .iter()
                .find_map(|(id, container)| {
                    if container.source_slot == Some(slot) {
                        Some(*id)
                    } else {
                        None
                    }
                })
        });
        let removed = {
            self.take_inventory_item_with_contents(id, slot, count)?
        };

        let stackable = self
            .item_types
            .as_ref()
            .and_then(|item_types| item_types.get(removed.type_id))
            .map(|item| item.stackable)
            .unwrap_or(false);
        let movable = self.item_is_movable(&removed);
        let should_close = self.is_container_item(&removed);
        let close_snapshot = if should_close {
            Some(removed.clone())
        } else {
            None
        };

        let removed_type_id = removed.type_id;
        let mut placed_stack_pos = None;
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        if Self::should_delete_on_dustbin(tile, movable) {
            if let Some(container_id) = open_container_id {
                let _ = self.close_container_for_player(id, container_id);
            }
            return Ok(());
        }
        if stackable {
            place_on_tile_with_dustbin(tile, removed, stackable, movable)?;
        } else {
            ensure_item_details_len(tile);
            tile.items.push(removed);
            let index = tile.items.len().saturating_sub(1);
            if let Some(added) = tile.items.get(index) {
                tile.item_details.push(map_item_for_stack(added));
            }
            if index <= u8::MAX as usize {
                placed_stack_pos = Some(index as u8);
            }
        }
        if let Some(container_id) = open_container_id {
            if let Some(stack_pos) = placed_stack_pos {
                if let Some(player) = self.players.get_mut(&id) {
                    if let Some(container) = player.open_containers.get_mut(&container_id) {
                        if container.item_type == removed_type_id {
                            container.source_slot = None;
                            container.source_position = Some(position);
                            container.source_stack_pos = Some(stack_pos);
                        }
                    }
                }
            }
        }
        if let Some(snapshot) = close_snapshot {
            self.close_open_containers_for_item(id, &snapshot);
        }
        let _ = self.send_mail_from_tile(id, position);
        Ok(())
    }

    pub fn move_inventory_item(
        &mut self,
        id: PlayerId,
        from: InventorySlot,
        to: InventorySlot,
        count: u16,
    ) -> Result<Vec<ContainerUpdate>, String> {
        let (item, dest_item) = {
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let Some(item) = player.inventory.slot(from) else {
                return Err("inventory slot empty".to_string());
            };
            (item.clone(), player.inventory.slot(to).cloned())
        };
        let use_container_slot = self.slot_container_capacity(id, to).is_some() && from != to;
        if !use_container_slot {
            self.ensure_slot_allows_item(to, item.type_id)?;
            self.ensure_two_handed_slot_free(id, to, Some(from), item.type_id)?;
        }
        let stackable = self
            .item_types
            .as_ref()
            .and_then(|item_types| item_types.get(item.type_id))
            .map(|item| item.stackable)
            .unwrap_or(false);
        if use_container_slot {
            let removed = self.take_inventory_item_with_contents(id, from, count)?;
            let should_close = self.is_container_item(&removed);
            let mut updates = Vec::new();
            match self.insert_into_inventory_slot_container(id, to, removed.clone(), stackable) {
                Ok(Some(update)) => updates.push(update),
                Ok(None) => {}
                Err(err) => {
                    if let Some(player) = self.players.get_mut(&id) {
                        let _ = player.inventory.add_item_to_slot(from, removed, stackable);
                    }
                    return Err(err);
                }
            }
            if should_close {
                self.close_open_containers_for_item(id, &removed);
            }
            return Ok(updates);
        }
        if let Some(dest) = dest_item.as_ref() {
            if stackable {
                if dest.type_id != item.type_id && count == item.count {
                    self.ensure_slot_allows_item(from, dest.type_id)?;
                }
            } else if count == item.count {
                self.ensure_slot_allows_item(from, dest.type_id)?;
            }
        }
        let player = self
            .players
            .get_mut(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        player.inventory.move_item(from, to, count, stackable)?;
        if count == item.count {
            let is_container = self
                .item_types
                .as_ref()
                .and_then(|types| types.get(item.type_id))
                .map(|entry| entry.kind == ItemKind::Container)
                .unwrap_or(false);
            if is_container {
                if let Some(player) = self.players.get_mut(&id) {
                    if let Some(contents) = player.inventory_containers.remove(&from) {
                        player.inventory_containers.insert(to, contents);
                    }
                    for container in player.open_containers.values_mut() {
                        if container.source_slot == Some(from) {
                            container.source_slot = Some(to);
                        }
                    }
                }
                self.close_open_containers_for_item(id, &item);
            }
        }
        Ok(Vec::new())
    }

    pub fn move_item_between_tiles(
        &mut self,
        id: PlayerId,
        from: Position,
        to: Position,
        type_id: ItemTypeId,
        count: u16,
    ) -> Result<(), String> {
        self.ensure_player_in_range(id, from)?;
        self.ensure_player_can_throw_to(id, from, to)?;
        self.ensure_house_item_access(id, from)?;
        self.ensure_house_item_access(id, to)?;
        let stackable = self
            .item_types
            .as_ref()
            .and_then(|item_types| item_types.get(type_id))
            .map(|item| item.stackable)
            .unwrap_or(false);

        let removed = {
            let tile = self
                .map
                .tile_mut(from)
                .ok_or_else(|| "source tile missing".to_string())?;
            take_from_tile(tile, type_id, count, stackable)?
        };
        let should_close = self.is_container_item(&removed);
        let close_snapshot = if should_close {
            Some(removed.clone())
        } else {
            None
        };
        let movable = self.item_is_movable(&removed);

        {
            let tile = self
                .map
                .tile_mut(to)
                .ok_or_else(|| "destination tile missing".to_string())?;
            place_on_tile_with_dustbin(tile, removed, stackable, movable)?;
        }
        if let Some(snapshot) = close_snapshot {
            self.close_open_containers_for_item(id, &snapshot);
        }
        let _ = self.send_mail_from_tile(id, to);
        Ok(())
    }

    pub fn move_container_item_to_inventory_slot(
        &mut self,
        id: PlayerId,
        container_id: u8,
        from_slot: u8,
        count: u16,
        item_type: ItemTypeId,
        to_slot: InventorySlot,
    ) -> Result<Vec<ContainerUpdate>, String> {
        let stackable = self.stackable_for(item_type);
        let use_container_slot = self.slot_container_capacity(id, to_slot).is_some();
        if !use_container_slot {
            self.ensure_slot_allows_item(to_slot, item_type)?;
            self.ensure_two_handed_slot_free(id, to_slot, None, item_type)?;
        }
        let (removed, update) = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&container_id)
                .ok_or_else(|| "container not open".to_string())?;
            take_from_container(container, container_id, from_slot, count, stackable, item_type)?
        };
        let should_close = self.is_container_item(&removed);

        let is_inventory_container = self
            .players
            .get(&id)
            .map(|player| container_belongs_to_inventory(player, container_id))
            .unwrap_or(false);
        if !is_inventory_container {
            let object_types = self
                .object_types
                .as_ref()
                .ok_or_else(|| "Item weight data missing.".to_string())?;
            let added_weight = item_stack_total_weight(object_types, &removed);
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            if let Err(err) = ensure_capacity_for_weight(player, object_types, added_weight) {
                if let Some(player) = self.players.get_mut(&id) {
                    if let Some(container) = player.open_containers.get_mut(&container_id) {
                        let _ = restore_container_item(container, from_slot, removed, stackable);
                    }
                }
                return Err(err);
            }
        }

        let mut updates = vec![update];
        if use_container_slot {
            match self.insert_into_inventory_slot_container(id, to_slot, removed.clone(), stackable) {
                Ok(Some(update)) => updates.push(update),
                Ok(None) => {}
                Err(err) => {
                    if let Some(player) = self.players.get_mut(&id) {
                        if let Some(container) = player.open_containers.get_mut(&container_id) {
                            let _ = restore_container_item(container, from_slot, removed, stackable);
                        }
                    }
                    return Err(err);
                }
            }
        } else {
            let result = {
                let player = self
                    .players
                    .get_mut(&id)
                    .ok_or_else(|| format!("unknown player {:?}", id))?;
                player
                    .inventory
                    .add_item_to_slot(to_slot, removed.clone(), stackable)
            };
            if let Err(err) = result {
                if let Some(player) = self.players.get_mut(&id) {
                    if let Some(container) = player.open_containers.get_mut(&container_id) {
                        let _ = restore_container_item(container, from_slot, removed, stackable);
                    }
                }
                return Err(err);
            }
        }
        self.sync_container_contents(id, container_id);
        if should_close {
            self.close_open_containers_for_item(id, &removed);
        }
        Ok(updates)
    }

    pub fn move_inventory_item_to_container(
        &mut self,
        id: PlayerId,
        from_slot: InventorySlot,
        count: u16,
        item_type: ItemTypeId,
        container_id: u8,
        to_slot: u8,
    ) -> Result<Vec<ContainerUpdate>, String> {
        if let Some(player) = self.players.get(&id) {
            if let Some(source_id) = find_container_by_slot(player, from_slot) {
                if container_is_descendant(player, source_id, container_id) {
                    return Err("cannot move container into itself".to_string());
                }
            }
        }
        let stackable = self.stackable_for(item_type);
        let removed = self.take_inventory_item_with_contents(id, from_slot, count)?;
        let should_close = self.is_container_item(&removed);
        if let Some(player) = self.players.get(&id) {
            if let Some(dest) = player.open_containers.get(&container_id) {
                if item_contains_open_container(&removed, dest) {
                    if let Some(player) = self.players.get_mut(&id) {
                        let _ = player
                            .inventory
                            .add_item_to_slot(from_slot, removed, stackable);
                    }
                    return Err("cannot move container into itself".to_string());
                }
            }
        }
        if let Err(err) = self.ensure_depot_capacity_for_insert(id, container_id, None, &removed) {
            if let Some(player) = self.players.get_mut(&id) {
                let _ = player
                    .inventory
                    .add_item_to_slot(from_slot, removed, stackable);
            }
            return Err(err);
        }

        let inserted = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&container_id)
                .ok_or_else(|| "container not open".to_string())?;
            insert_into_container(container, container_id, to_slot, removed.clone(), stackable)
        };
        match inserted {
            Ok(update) => {
                self.sync_container_contents(id, container_id);
                if should_close {
                    self.close_open_containers_for_item(id, &removed);
                }
                Ok(vec![update])
            }
            Err(err) => {
                if let Some(player) = self.players.get_mut(&id) {
                    let _ = player
                        .inventory
                        .add_item_to_slot(from_slot, removed, stackable);
                }
                Err(err)
            }
        }
    }

    pub fn move_container_item_to_tile(
        &mut self,
        id: PlayerId,
        container_id: u8,
        from_slot: u8,
        count: u16,
        item_type: ItemTypeId,
        position: Position,
    ) -> Result<Vec<ContainerUpdate>, String> {
        let origin = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?
            .position;
        self.ensure_player_can_throw_to(id, origin, position)?;
        self.ensure_house_item_access(id, position)?;
        let stackable = self.stackable_for(item_type);
        let (removed, update) = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&container_id)
                .ok_or_else(|| "container not open".to_string())?;
            take_from_container(container, container_id, from_slot, count, stackable, item_type)?
        };
        let should_close = self.is_container_item(&removed);
        let movable = self.item_is_movable(&removed);

        let placed = {
            let tile = self
                .map
                .tile_mut(position)
                .ok_or_else(|| "tile missing".to_string())?;
            place_on_tile_with_dustbin(tile, removed.clone(), stackable, movable)
        };
        if let Err(err) = placed {
            if let Some(player) = self.players.get_mut(&id) {
                if let Some(container) = player.open_containers.get_mut(&container_id) {
                    let _ = restore_container_item(container, from_slot, removed, stackable);
                }
            }
            return Err(err);
        }
        self.sync_container_contents(id, container_id);
        if should_close {
            self.close_open_containers_for_item(id, &removed);
        }
        let _ = self.send_mail_from_tile(id, position);
        Ok(vec![update])
    }

    pub fn move_tile_item_to_container(
        &mut self,
        id: PlayerId,
        position: Position,
        item_type: ItemTypeId,
        count: u16,
        from_stack: u8,
        container_id: u8,
        to_slot: u8,
    ) -> Result<Vec<ContainerUpdate>, String> {
        self.ensure_player_in_range(id, position)?;
        self.ensure_house_item_access(id, position)?;
        let mut map_item_snapshot = None;
        if let Some(tile) = self.map.tile(position) {
            let index = usize::from(from_stack);
            if let Some(item) = tile.items.get(index) {
                if item.type_id == item_type {
                    map_item_snapshot = Some(item.clone());
                }
            }
        }
        if let (Some(player), Some(snapshot)) = (self.players.get(&id), map_item_snapshot.as_ref())
        {
            if let Some(source_id) =
                find_container_by_map_item(player, position, from_stack, snapshot)
            {
                if container_is_descendant(player, source_id, container_id) {
                    return Err("cannot move container into itself".to_string());
                }
            } else if let Some(dest) = player.open_containers.get(&container_id) {
                if item_contains_open_container(snapshot, dest) {
                    return Err("cannot move container into itself".to_string());
                }
            }
        }
        let stackable = self.stackable_for(item_type);
        let removed = {
            let tile = self
                .map
                .tile_mut(position)
                .ok_or_else(|| "tile missing".to_string())?;
            let index = usize::from(from_stack);
            if index >= tile.items.len() {
                return Err("tile item index out of range".to_string());
            }
            if tile.items[index].type_id != item_type {
                return Err("tile item type mismatch".to_string());
            }
            take_from_tile_at(tile, index, count, stackable)?
        };
        let should_close = self.is_container_item(&removed);
        let movable = self.item_is_movable(&removed);

        let is_inventory_container = self
            .players
            .get(&id)
            .map(|player| container_belongs_to_inventory(player, container_id))
            .unwrap_or(false);
        if is_inventory_container {
            let object_types = self
                .object_types
                .as_ref()
                .ok_or_else(|| "Item weight data missing.".to_string())?;
            let added_weight = item_stack_total_weight(object_types, &removed);
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            if let Err(err) = ensure_capacity_for_weight(player, object_types, added_weight) {
                if let Some(tile) = self.map.tile_mut(position) {
                    let _ = place_on_tile_with_dustbin(tile, removed, stackable, movable);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.ensure_depot_capacity_for_insert(id, container_id, None, &removed) {
            if let Some(tile) = self.map.tile_mut(position) {
                let _ = place_on_tile_with_dustbin(tile, removed, stackable, movable);
            }
            return Err(err);
        }

        let inserted = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&container_id)
                .ok_or_else(|| "container not open".to_string())?;
            insert_into_container(container, container_id, to_slot, removed.clone(), stackable)
        };
        match inserted {
            Ok(update) => {
                self.sync_container_contents(id, container_id);
                if should_close {
                    self.close_open_containers_for_item(id, &removed);
                }
                Ok(vec![update])
            }
            Err(err) => {
                if let Some(tile) = self.map.tile_mut(position) {
                    let _ = place_on_tile_with_dustbin(tile, removed, stackable, movable);
                }
                Err(err)
            }
        }
    }

    pub fn move_container_item_between_containers(
        &mut self,
        id: PlayerId,
        from_container: u8,
        from_slot: u8,
        count: u16,
        item_type: ItemTypeId,
        to_container: u8,
        to_slot: u8,
    ) -> Result<Vec<ContainerUpdate>, String> {
        if from_container == to_container {
            return self.move_container_item_within_container(
                id,
                from_container,
                from_slot,
                count,
                item_type,
                to_slot,
            );
        }
        if let Some(player) = self.players.get(&id) {
            if let Some(source_id) =
                find_container_by_parent_slot(player, from_container, from_slot)
            {
                if container_is_descendant(player, source_id, to_container) {
                    return Err("cannot move container into itself".to_string());
                }
            }
        }
        let stackable = self.stackable_for(item_type);
        let (removed, source_update) = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&from_container)
                .ok_or_else(|| "container not open".to_string())?;
            take_from_container(container, from_container, from_slot, count, stackable, item_type)?
        };
        let should_close = self.is_container_item(&removed);
        if let Some(player) = self.players.get(&id) {
            if let Some(dest) = player.open_containers.get(&to_container) {
                if item_contains_open_container(&removed, dest) {
                    if let Some(player) = self.players.get_mut(&id) {
                        if let Some(container) = player.open_containers.get_mut(&from_container) {
                            let _ = restore_container_item(container, from_slot, removed, stackable);
                        }
                    }
                    return Err("cannot move container into itself".to_string());
                }
            }
        }

        let (source_inventory, dest_inventory) = self
            .players
            .get(&id)
            .map(|player| {
                (
                    container_belongs_to_inventory(player, from_container),
                    container_belongs_to_inventory(player, to_container),
                )
            })
            .unwrap_or((false, false));
        if dest_inventory && !source_inventory {
            let object_types = self
                .object_types
                .as_ref()
                .ok_or_else(|| "Item weight data missing.".to_string())?;
            let added_weight = item_stack_total_weight(object_types, &removed);
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            if let Err(err) = ensure_capacity_for_weight(player, object_types, added_weight) {
                if let Some(player) = self.players.get_mut(&id) {
                    if let Some(container) = player.open_containers.get_mut(&from_container) {
                        let _ = restore_container_item(container, from_slot, removed, stackable);
                    }
                }
                return Err(err);
            }
        }
        if let Err(err) = self.ensure_depot_capacity_for_insert(
            id,
            to_container,
            Some(from_container),
            &removed,
        ) {
            if let Some(player) = self.players.get_mut(&id) {
                if let Some(container) = player.open_containers.get_mut(&from_container) {
                    let _ = restore_container_item(container, from_slot, removed, stackable);
                }
            }
            return Err(err);
        }

        let inserted = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&to_container)
                .ok_or_else(|| "container not open".to_string())?;
            insert_into_container(container, to_container, to_slot, removed.clone(), stackable)
        };
        match inserted {
            Ok(dest_update) => {
                self.sync_container_contents(id, from_container);
                self.sync_container_contents(id, to_container);
                if should_close {
                    self.close_open_containers_for_item(id, &removed);
                }
                Ok(vec![source_update, dest_update])
            }
            Err(err) => {
                if let Some(player) = self.players.get_mut(&id) {
                    if let Some(container) = player.open_containers.get_mut(&from_container) {
                        let _ = restore_container_item(container, from_slot, removed, stackable);
                    }
                }
                Err(err)
            }
        }
    }

    pub fn move_container_item_within_container(
        &mut self,
        id: PlayerId,
        container_id: u8,
        from_slot: u8,
        count: u16,
        item_type: ItemTypeId,
        to_slot: u8,
    ) -> Result<Vec<ContainerUpdate>, String> {
        let stackable = self.stackable_for(item_type);
        let moved_item = {
            let player = self
                .players
                .get(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get(&container_id)
                .ok_or_else(|| "container not open".to_string())?;
            container.items.get(from_slot as usize).cloned()
        };
        let updates = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let container = player
                .open_containers
                .get_mut(&container_id)
                .ok_or_else(|| "container not open".to_string())?;
            let before = container.items.clone();
            move_item_within_container(
                container,
                from_slot,
                to_slot,
                count,
                stackable,
                item_type,
            )?;
            diff_container_updates(container_id, &before, &container.items)
        };
        if !updates.is_empty() {
            self.sync_container_contents(id, container_id);
        }
        if let Some(item) = moved_item {
            if self.is_container_item(&item) {
                self.close_open_containers_for_item(id, &item);
            }
        }
        Ok(updates)
    }

    fn add_item_stack_to_player(
        &mut self,
        player_id: PlayerId,
        item: ItemStack,
    ) -> Result<(), String> {
        if item.count == 0 {
            return Ok(());
        }
        self.schedule_cron_for_item_tree(&item);
        let object_types = self
            .object_types
            .as_ref()
            .ok_or_else(|| "Item weight data missing.".to_string())?;
        let stackable = self.stackable_for(item.type_id);
        let movable = self.item_is_movable(&item);
        let added_weight = item_stack_total_weight(object_types, &item);
        let player = self
            .players
            .get(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        if let Err(_) = ensure_capacity_for_weight(player, object_types, added_weight) {
            let tile = self
                .map
                .tile_mut(player.position)
                .ok_or_else(|| "player tile missing".to_string())?;
            return place_on_tile_with_dustbin(tile, item, stackable, movable);
        }
        let item_types = self.item_types.as_ref();
        let mut sync_container_id = None;
        let position = {
            let player = self
                .players
                .get_mut(&player_id)
                .ok_or_else(|| format!("unknown player {:?}", player_id))?;

            let mut container_ids: Vec<u8> = player
                .open_containers
                .iter()
                .filter(|(_, container)| container.source_slot.is_some())
                .map(|(id, _)| *id)
                .collect();
            container_ids.sort_unstable();
            if let Some(backpack_id) = player
                .open_containers
                .iter()
                .find_map(|(id, container)| {
                    if container.source_slot == Some(InventorySlot::Backpack) {
                        Some(*id)
                    } else {
                        None
                    }
                })
            {
                container_ids.retain(|id| *id != backpack_id);
                container_ids.insert(0, backpack_id);
            }

            for container_id in container_ids {
                let Some(container) = player.open_containers.get_mut(&container_id) else {
                    continue;
                };
                if insert_into_container(container, container_id, 0xff, item.clone(), stackable)
                    .is_ok()
                {
                    sync_container_id = Some(container_id);
                    break;
                }
            }
            if sync_container_id.is_some() {
                return Ok(());
            }

            if let Some(backpack_item) = player.inventory.slot(InventorySlot::Backpack).cloned() {
                let is_container = item_types
                    .and_then(|types| types.get(backpack_item.type_id))
                    .map(|entry| entry.kind == ItemKind::Container)
                    .unwrap_or(true);
                if is_container {
                    let capacity = item_types
                        .and_then(|types| types.get(backpack_item.type_id))
                        .and_then(|entry| entry.container_capacity)
                        .unwrap_or(0)
                        .min(u16::from(u8::MAX)) as u8;
                    let items = player
                        .inventory_containers
                        .entry(InventorySlot::Backpack)
                        .or_insert_with(Vec::new);
                    if insert_into_inventory_container_items(
                        items,
                        capacity,
                        item.clone(),
                        stackable,
                    )
                    .is_ok()
                    {
                        return Ok(());
                    }
                }
            }

            player.position
        };
        if let Some(container_id) = sync_container_id {
            self.sync_container_contents(player_id, container_id);
            return Ok(());
        }

        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "player tile missing".to_string())?;
        place_on_tile_with_dustbin(tile, item, stackable, movable)
    }

    fn add_item_to_player(
        &mut self,
        player_id: PlayerId,
        type_id: ItemTypeId,
        count: u16,
    ) -> Result<(), String> {
        if count == 0 {
            return Ok(());
        }
        let item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id,
            count,
            attributes: Vec::new(),
            contents: Vec::new(),
};
        self.schedule_cron_for_item_tree(&item);
        self.add_item_stack_to_player(player_id, item)
    }

    fn remove_item_from_player(
        &mut self,
        player_id: PlayerId,
        type_id: ItemTypeId,
        count: u16,
    ) -> Result<(), String> {
        let removed = {
            let player = self
                .players
                .get_mut(&player_id)
                .ok_or_else(|| format!("unknown player {:?}", player_id))?;
            remove_item_from_player_count(player, type_id, u32::from(count))
        };
        if removed < u32::from(count) {
            return Err("player item count insufficient".to_string());
        }
        self.sync_open_containers_for_player(player_id);
        Ok(())
    }

    fn add_money_to_player(&mut self, player_id: PlayerId, amount: u32) -> Result<(), String> {
        if amount == 0 {
            return Ok(());
        }
        let money_types = self.money_types();
        if money_types.is_empty() {
            return Err("money types missing".to_string());
        }
        let mut remaining = amount;
        for money in &money_types {
            if remaining == 0 {
                break;
            }
            let count = remaining / money.value;
            if count == 0 {
                continue;
            }
            let mut left = count;
            while left > 0 {
                let chunk = left.min(u32::from(u16::MAX)) as u16;
                self.add_item_to_player(player_id, money.type_id, chunk)?;
                left = left.saturating_sub(u32::from(chunk));
            }
            remaining = remaining.saturating_sub(count.saturating_mul(money.value));
        }
        Ok(())
    }

    fn remove_money_from_player(
        &mut self,
        player_id: PlayerId,
        amount: u32,
    ) -> Result<(), String> {
        if amount == 0 {
            return Ok(());
        }
        let money_types = self.money_types();
        let gold_type = money_types.iter().find(|money| money.value == 1);
        let platinum_type = money_types.iter().find(|money| money.value == 100);
        let crystal_type = money_types.iter().find(|money| money.value == 10_000);
        let (gold_type, platinum_type, crystal_type) = match (gold_type, platinum_type, crystal_type)
        {
            (Some(gold_type), Some(platinum_type), Some(crystal_type)) => {
                (gold_type, platinum_type, crystal_type)
            }
            _ => return Err("money types missing".to_string()),
        };
        let total = self
            .players
            .get(&player_id)
            .map(|player| npc_count_money(player, self.object_types.as_ref()))
            .unwrap_or(0);
        if total < amount {
            return Err("insufficient money".to_string());
        }
        let gold = self
            .players
            .get(&player_id)
            .map(|player| npc_count_item(player, gold_type.type_id) as i32)
            .unwrap_or(0);
        let platinum = self
            .players
            .get(&player_id)
            .map(|player| npc_count_item(player, platinum_type.type_id) as i32)
            .unwrap_or(0);
        let crystal = self
            .players
            .get(&player_id)
            .map(|player| npc_count_item(player, crystal_type.type_id) as i32)
            .unwrap_or(0);
        let (gold_delta, platinum_delta, crystal_delta) =
            calculate_money_change(amount as i32, gold, platinum, crystal)
                .ok_or_else(|| "money change unavailable".to_string())?;

        if gold_delta > 0 {
            if let Some(player) = self.players.get_mut(&player_id) {
                let _ = remove_item_from_player_count(
                    player,
                    gold_type.type_id,
                    gold_delta as u32,
                );
            }
        } else if gold_delta < 0 {
            let _ = self.add_item_to_player(player_id, gold_type.type_id, (-gold_delta) as u16);
        }
        if platinum_delta > 0 {
            if let Some(player) = self.players.get_mut(&player_id) {
                let _ = remove_item_from_player_count(
                    player,
                    platinum_type.type_id,
                    platinum_delta as u32,
                );
            }
        } else if platinum_delta < 0 {
            let _ =
                self.add_item_to_player(player_id, platinum_type.type_id, (-platinum_delta) as u16);
        }
        if crystal_delta < 0 {
            return Err("money change unavailable".to_string());
        }
        if crystal_delta > 0 {
            if let Some(player) = self.players.get_mut(&player_id) {
                let _ = remove_item_from_player_count(
                    player,
                    crystal_type.type_id,
                    crystal_delta as u32,
                );
            }
        }
        self.sync_open_containers_for_player(player_id);
        Ok(())
    }

    fn sync_open_containers_for_player(&mut self, player_id: PlayerId) {
        let container_ids: Vec<u8> = match self.players.get(&player_id) {
            Some(player) => player.open_containers.keys().copied().collect(),
            None => return,
        };
        for container_id in container_ids {
            self.sync_container_contents(player_id, container_id);
        }
        self.queue_container_refresh(player_id);
    }

    fn money_types(&self) -> Vec<MoneyType> {
        let Some(objects) = self.object_types.as_ref() else {
            return Vec::new();
        };
        let mut money_types = Vec::new();
        for (id, object) in objects.iter() {
            if let Some(value) = money_value_from_object(object) {
                money_types.push(MoneyType {
                    type_id: *id,
                    value,
                });
            }
        }
        money_types.sort_by(|a, b| b.value.cmp(&a.value));
        money_types
    }

    pub fn use_object(
        &mut self,
        id: PlayerId,
        position: Position,
        type_id: ItemTypeId,
    ) -> Result<MoveUseOutcome, String> {
        self.use_object_with_clock(id, position, type_id, None)
    }

    pub fn use_object_with_clock(
        &mut self,
        id: PlayerId,
        position: Position,
        type_id: ItemTypeId,
        clock: Option<&GameClock>,
    ) -> Result<MoveUseOutcome, String> {
        let player = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;

        let tile = self
            .map
            .tile(position)
            .ok_or_else(|| "tile missing".to_string())?;

        let dx = i32::from(player.position.x) - i32::from(position.x);
        let dy = i32::from(player.position.y) - i32::from(position.y);
        let in_range = player.position.z == position.z
            && dx.unsigned_abs() <= 1
            && dy.unsigned_abs() <= 1;
        if !in_range && !player.is_test_god {
            return Err("object is out of reach".to_string());
        }

        if !tile.items.iter().any(|item| item.type_id == type_id) {
            return Err("object not found on tile".to_string());
        }

        let ctx = MoveUseContext {
            event: MoveUseEvent::Use,
            user_id: id,
            user_position: player.position,
            object_position: position,
            object_type_id: type_id,
            object_source: UseObjectSource::Map(position),
            object2_position: None,
            object2_type_id: None,
        };
        let mut outcome = self.run_moveuse_event(ctx, clock)?;
        if outcome.matched_rule.is_none() {
            if let Some(edit) = self.try_open_list_edit(id, position, type_id) {
                outcome.edit_lists.push(edit);
            } else if let Some(edit) = self.try_open_text_edit(id, position, type_id) {
                outcome.edit_texts.push(edit);
            } else if let Some(object_types) = self.object_types.as_ref() {
                if let Some(object_type) = object_types.get(type_id) {
                    if object_type.has_flag("ChangeUse") {
                        if let Some(change_target) = object_type.attribute_u16("ChangeTarget") {
                            let to_type = ItemTypeId(change_target);
                            self.change_item_on_tile(position, type_id, to_type, 0)?;
                            record_moveuse_refresh(&mut outcome, position);
                        }
                    }
                }
            }
        }
        Ok(outcome)
    }

    pub fn try_consume_food(
        &mut self,
        id: PlayerId,
        source: UseObjectSource,
        item_type: ItemTypeId,
        clock: &GameClock,
    ) -> Result<Option<MoveUseOutcome>, String> {
        let Some(object_types) = self.object_types.as_ref() else {
            return Ok(None);
        };
        let Some(object_type) = object_types.get(item_type) else {
            return Ok(None);
        };
        if !object_type.has_flag("Food") && !object_type.has_flag("Drink") {
            return Ok(None);
        }
        let nutrition = object_type.attribute_u16("Nutrition").unwrap_or(0);
        if nutrition == 0 {
            return Ok(None);
        }

        match source {
            UseObjectSource::Map(position) => {
                self.ensure_player_in_range(id, position)?;
                let tile = self
                    .map
                    .tile(position)
                    .ok_or_else(|| "source tile missing".to_string())?;
                if !tile.items.iter().any(|item| item.type_id == item_type) {
                    return Err("source object not found on tile".to_string());
                }
            }
            UseObjectSource::Inventory(slot) => {
                let player = self
                    .players
                    .get(&id)
                    .ok_or_else(|| format!("unknown player {:?}", id))?;
                let Some(item) = player.inventory.slot(slot) else {
                    return Err("source object missing from inventory".to_string());
                };
                if item.type_id != item_type {
                    return Err("source object type mismatch".to_string());
                }
            }
            UseObjectSource::Container { container_id, slot } => {
                let player = self
                    .players
                    .get(&id)
                    .ok_or_else(|| format!("unknown player {:?}", id))?;
                let Some(container) = player.open_containers.get(&container_id) else {
                    return Err("source container not open".to_string());
                };
                let index = slot as usize;
                if index >= container.items.len() {
                    return Err("source container slot out of range".to_string());
                }
                let item = &container.items[index];
                if item.type_id != item_type {
                    return Err("source object type mismatch".to_string());
                }
            }
        }

        let added_seconds = u64::from(nutrition).saturating_mul(FOOD_SECONDS_PER_NUTRITION);
        let added = Duration::from_secs(added_seconds);
        let added_ticks = clock.ticks_from_duration_round_up(added);
        if added_ticks == 0 {
            return Ok(None);
        }
        let max_ticks = clock.ticks_from_duration_round_up(Duration::from_secs(FOOD_MAX_SECONDS));
        let now = clock.now();
        let was_active = {
            let player = self
                .players
                .get_mut(&id)
                .ok_or_else(|| format!("unknown player {:?}", id))?;
            let remaining = player
                .food_expires_at
                .filter(|expires_at| *expires_at > now)
                .map(|expires_at| expires_at.0.saturating_sub(now.0))
                .unwrap_or(0);
            if remaining.saturating_add(added_ticks) > max_ticks {
                let mut outcome = MoveUseOutcome {
                    matched_rule: None,
                    ignored_actions: Vec::new(),
                    effects: Vec::new(),
                    texts: Vec::new(),
                    edit_texts: Vec::new(),
                    edit_lists: Vec::new(),
                    messages: Vec::new(),
                    damages: Vec::new(),
                    quest_updates: Vec::new(),
                    logout_users: Vec::new(),
                    refresh_positions: Vec::new(),
                    inventory_updates: Vec::new(),
                    container_updates: Vec::new(),
                };
                outcome.messages.push(MoveUseMessage {
                    player_id: id,
                    message_type: 0x14,
                    message: "You are full.".to_string(),
                });
                return Ok(Some(outcome));
            }
            let new_total = remaining.saturating_add(added_ticks);
            player.food_expires_at = Some(GameTick(now.0.saturating_add(new_total)));
            let remaining_seconds = Self::skill_timer_seconds_from_ticks(remaining, clock);
            let mut new_cycle = remaining_seconds.saturating_add(added_seconds.min(i32::MAX as u64) as i32);
            new_cycle = new_cycle.min(FOOD_MAX_SECONDS.min(i32::MAX as u64) as i32);
            if player.raw_skills.is_empty() {
                player.raw_skills = skill_rows_from_player(player);
            }
            let index = Self::ensure_skill_row(&mut player.raw_skills, SKILL_FED, player.profession);
            Self::set_skill_timer(&mut player.raw_skills[index], new_cycle, 0, 0);
            remaining > 0
        };

        if !was_active {
            if let Some(player) = self.players.get_mut(&id) {
                let (hp_interval, mana_interval) = food_regen_intervals(player.profession);
                player
                    .food_hp_cooldown
                    .reset_from_now_duration(clock, hp_interval);
                player
                    .food_mana_cooldown
                    .reset_from_now_duration(clock, mana_interval);
            }
        }

        let stackable = self.stackable_for(item_type);
        let mut outcome = MoveUseOutcome {
            matched_rule: None,
            ignored_actions: Vec::new(),
            effects: Vec::new(),
            texts: Vec::new(),
            edit_texts: Vec::new(),
            edit_lists: Vec::new(),
            messages: Vec::new(),
            damages: Vec::new(),
            quest_updates: Vec::new(),
            logout_users: Vec::new(),
            refresh_positions: Vec::new(),
            inventory_updates: Vec::new(),
            container_updates: Vec::new(),
        };

        match source {
            UseObjectSource::Map(position) => {
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "source tile missing".to_string())?;
                let _ = take_from_tile(tile, item_type, 1, stackable)?;
                record_moveuse_refresh(&mut outcome, position);
            }
            UseObjectSource::Inventory(slot) => {
                let player = self
                    .players
                    .get_mut(&id)
                    .ok_or_else(|| format!("unknown player {:?}", id))?;
                let item = player
                    .inventory
                    .slot(slot)
                    .cloned()
                    .ok_or_else(|| "inventory slot empty".to_string())?;
                if item.type_id != item_type {
                    return Err("source object type mismatch".to_string());
                }
                player.inventory.remove_item(slot, 1)?;
            }
            UseObjectSource::Container { container_id, slot } => {
                let update = {
                    let player = self
                        .players
                        .get_mut(&id)
                        .ok_or_else(|| format!("unknown player {:?}", id))?;
                    let container = player
                        .open_containers
                        .get_mut(&container_id)
                        .ok_or_else(|| "container not open".to_string())?;
                    let (.., update) = take_from_container(
                        container,
                        container_id,
                        slot,
                        1,
                        stackable,
                        item_type,
                    )?;
                    update
                };
                outcome.container_updates.push(update);
                self.sync_container_contents(id, container_id);
            }
        }

        Ok(Some(outcome))
    }

    fn ensure_player_in_range(
        &self,
        id: PlayerId,
        position: Position,
    ) -> Result<(), String> {
        if self
            .players
            .get(&id)
            .map(|player| player.is_test_god)
            .unwrap_or(false)
        {
            return Ok(());
        }
        let player = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        let dx = i32::from(player.position.x) - i32::from(position.x);
        let dy = i32::from(player.position.y) - i32::from(position.y);
        let in_range = player.position.z == position.z
            && dx.unsigned_abs() <= 1
            && dy.unsigned_abs() <= 1;
        if !in_range {
            return Err("object is out of reach".to_string());
        }
        Ok(())
    }

    fn ensure_moveuse_audit_tile(&mut self, position: Position) {
        if !self.position_in_bounds(position) {
            return;
        }
        if self.map.tile(position).is_some() {
            return;
        }
        self.map.tiles.insert(
            position,
            Tile {
                position,
                items: Vec::new(),
                item_details: Vec::new(),
                refresh: false,
                protection_zone: false,
                no_logout: false,
                annotations: Vec::new(),
                tags: Vec::new(),
            },
        );
    }

    fn place_moveuse_audit_item(&mut self, position: Position, type_id: ItemTypeId, count: u16) {
        if !self.position_in_bounds(position) {
            return;
        }
        self.ensure_moveuse_audit_tile(position);
        let Some(tile) = self.map.tile_mut(position) else {
            return;
        };
        let item = ItemStack {
            id: ItemId::next(),
            type_id,
            count: normalize_stack_count(count),
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        tile.items.push(item.clone());
        ensure_item_details_len(tile);
        tile.item_details.push(map_item_for_stack(&item));
    }

    fn prepare_moveuse_audit_rule(
        &mut self,
        rule: &MoveUseRule,
        object_position: Position,
        object2_position: Position,
    ) {
        for action in &rule.actions {
            let name = action.name.trim_start_matches('!');
            match name {
                "ChangeOnMap" if action.args.len() >= 3 => {
                    if let Ok(position) = parse_position_arg(&action.args[0]) {
                        if let Ok(from_type) = parse_item_type_id(&action.args[1]) {
                            self.ensure_moveuse_audit_tile(position);
                            self.place_moveuse_audit_item(position, from_type, 1);
                        }
                    }
                }
                "DeleteOnMap" | "DeleteTopOnMap" if action.args.len() >= 2 => {
                    if let Ok(position) = parse_position_arg(&action.args[0]) {
                        if let Ok(type_id) = parse_item_type_id(&action.args[1]) {
                            self.ensure_moveuse_audit_tile(position);
                            self.place_moveuse_audit_item(position, type_id, 1);
                        }
                    }
                }
                "MoveTopOnMap" if action.args.len() >= 3 => {
                    if let Ok(from) = parse_position_arg(&action.args[0]) {
                        if let Ok(type_id) = parse_item_type_id(&action.args[1]) {
                            self.ensure_moveuse_audit_tile(from);
                            self.place_moveuse_audit_item(from, type_id, 1);
                        }
                    }
                    if let Ok(to) = parse_position_arg(&action.args[2]) {
                        self.ensure_moveuse_audit_tile(to);
                    }
                }
                "CreateOnMap" | "EffectOnMap" if !action.args.is_empty() => {
                    if let Ok(position) = parse_position_arg(&action.args[0]) {
                        self.ensure_moveuse_audit_tile(position);
                    }
                }
                "LoadDepot" | "SaveDepot" if action.args.len() >= 2 => {
                    if let Ok(position) = parse_position_arg(&action.args[0]) {
                        if let Ok(type_id) = parse_item_type_id(&action.args[1]) {
                            self.ensure_moveuse_audit_tile(position);
                            self.place_moveuse_audit_item(position, type_id, 1);
                        }
                    }
                }
                "Move" | "MoveTop" if action.args.len() >= 2 => {
                    if let Ok(position) = parse_position_arg(&action.args[1]) {
                        self.ensure_moveuse_audit_tile(position);
                    }
                }
                "MoveTopRel" if action.args.len() >= 2 => {
                    if let Ok(delta) = parse_delta_arg(&action.args[1]) {
                        if let Some(target) = object_position.offset(delta) {
                            self.ensure_moveuse_audit_tile(target);
                        }
                    }
                }
                "MonsterOnMap" if action.args.len() >= 2 => {
                    if let Ok(position) = parse_position_arg(&action.args[0]) {
                        self.prepare_moveuse_audit_spawn_tile(position);
                    }
                }
                "Monster" if action.args.len() >= 2 => {
                    match action.args[0].trim() {
                        "Obj1" => self.prepare_moveuse_audit_spawn_tile(object_position),
                        "Obj2" => self.prepare_moveuse_audit_spawn_tile(object2_position),
                        _ => {}
                    }
                }
                "Retrieve" if action.args.len() >= 3 => {
                    let base = match action.args[0].trim() {
                        "Obj1" => Some(object_position),
                        "Obj2" => Some(object2_position),
                        "User" => Some(object_position),
                        other => parse_position_arg(other).ok(),
                    };
                    if let Some(base) = base {
                        if let Ok(from_delta) = parse_delta_arg(&action.args[1]) {
                            if let Some(from) = base.offset(from_delta) {
                                self.ensure_moveuse_audit_tile(from);
                                self.place_moveuse_audit_item(from, ItemTypeId(100), 1);
                            }
                        }
                        if let Ok(to_delta) = parse_delta_arg(&action.args[2]) {
                            if let Some(to) = base.offset(to_delta) {
                                self.ensure_moveuse_audit_tile(to);
                            }
                        }
                    }
                }
                "ChangeRel" if action.args.len() >= 4 => {
                    if let Ok(delta) = parse_delta_arg(&action.args[1]) {
                        if let Some(target) = object_position.offset(delta) {
                            self.ensure_moveuse_audit_tile(target);
                            if let Ok(from_type) = parse_item_type_id(&action.args[2]) {
                                self.place_moveuse_audit_item(target, from_type, 1);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        self.ensure_moveuse_audit_tile(object_position);
        self.ensure_moveuse_audit_tile(object2_position);
    }

    fn prepare_moveuse_audit_spawn_tile(&mut self, position: Position) {
        if !self.position_in_bounds(position) {
            return;
        }
        self.ensure_moveuse_audit_tile(position);
        let Some(tile) = self.map.tile_mut(position) else {
            return;
        };
        tile.items.clear();
        tile.item_details.clear();
        tile.protection_zone = false;
    }

    fn write_moveuse_audit_report(
        &self,
        player_id: PlayerId,
        failures: &[String],
    ) -> Result<String, String> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| "world root unavailable".to_string())?;
        let save_dir = root.join("save");
        std::fs::create_dir_all(&save_dir)
            .map_err(|err| format!("create save dir failed: {}", err))?;
        let path = save_dir.join(format!("moveuse_audit_{}.txt", player_id.0));

        let mut body = String::new();
        body.push_str("# moveuse audit failures\n");
        for failure in failures {
            body.push_str(failure);
            body.push('\n');
        }
        std::fs::write(&path, body)
            .map_err(|err| format!("write report failed: {}", err))?;
        Ok(path.display().to_string())
    }

    fn ensure_player_can_throw_to(
        &self,
        id: PlayerId,
        origin: Position,
        position: Position,
    ) -> Result<(), String> {
        let player = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        if player.is_test_god {
            return Ok(());
        }
        if !crate::net::game::position_in_viewport(player.position, position) {
            return Err("object is out of reach".to_string());
        }
        if !self.throw_possible(origin, position, 1) {
            return Err("cannot throw there".to_string());
        }
        Ok(())
    }

    pub fn use_object_on_position_with_clock(
        &mut self,
        id: PlayerId,
        object_source: UseObjectSource,
        item_type: ItemTypeId,
        target_position: Position,
        target_type: ItemTypeId,
        clock: Option<&GameClock>,
    ) -> Result<MoveUseOutcome, String> {
        let player = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        let player_position = player.position;

        let target_tile = self
            .map
            .tile(target_position)
            .ok_or_else(|| "target tile missing".to_string())?;
        if !target_tile.items.iter().any(|item| item.type_id == target_type) {
            return Err("target object not found on tile".to_string());
        }

        match object_source {
            UseObjectSource::Map(position) => {
                self.ensure_player_in_range(id, position)?;
                let tile = self
                    .map
                    .tile(position)
                    .ok_or_else(|| "source tile missing".to_string())?;
                if !tile.items.iter().any(|item| item.type_id == item_type) {
                    return Err("source object not found on tile".to_string());
                }
            }
            UseObjectSource::Inventory(slot) => {
                if !player.is_test_god {
                    let Some(item) = player.inventory.slot(slot) else {
                        return Err("source object missing from inventory".to_string());
                    };
                    if item.type_id != item_type {
                        return Err("source object type mismatch".to_string());
                    }
                }
            }
            UseObjectSource::Container { container_id, slot } => {
                if !player.is_test_god {
                    let Some(container) = player.open_containers.get(&container_id) else {
                        return Err("source container not open".to_string());
                    };
                    let index = slot as usize;
                    if index >= container.items.len() {
                        return Err("source container slot out of range".to_string());
                    }
                    let item = &container.items[index];
                    if item.type_id != item_type {
                        return Err("source object type mismatch".to_string());
                    }
                }
            }
        }

        self.ensure_player_in_range(id, target_position)?;
        let object_position = match object_source {
            UseObjectSource::Map(position) => position,
            _ => player_position,
        };

        let ctx = MoveUseContext {
            event: MoveUseEvent::MultiUse,
            user_id: id,
            user_position: player_position,
            object_position,
            object_type_id: item_type,
            object_source,
            object2_position: Some(target_position),
            object2_type_id: Some(target_type),
        };
        self.run_moveuse_event(ctx, clock)
    }

    pub fn rotate_item(
        &mut self,
        id: PlayerId,
        position: Position,
        stack_pos: u8,
        type_id: ItemTypeId,
    ) -> Result<bool, String> {
        let player = self
            .players
            .get(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;

        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;

        let dx = i32::from(player.position.x) - i32::from(position.x);
        let dy = i32::from(player.position.y) - i32::from(position.y);
        let in_range = player.position.z == position.z
            && dx.unsigned_abs() <= 1
            && dy.unsigned_abs() <= 1;
        if !in_range && !player.is_test_god {
            return Err("object is out of reach".to_string());
        }

        let index = usize::from(stack_pos);
        if index >= tile.items.len() {
            return Err("rotate item missing stack".to_string());
        }
        if tile.items[index].type_id != type_id {
            return Err("rotate item type mismatch".to_string());
        }

        let Some(object_types) = self.object_types.as_ref() else {
            return Ok(false);
        };
        let Some(object_type) = object_types.get(type_id) else {
            return Ok(false);
        };
        if !object_type.has_flag("Rotate") {
            return Ok(false);
        }
        let Some(target_id) = object_type.attribute_u16("RotateTarget") else {
            return Ok(false);
        };
        let new_type = ItemTypeId(target_id);

        let mut new_count = tile.items[index].count;
        if let Some(item_types) = self.item_types.as_ref() {
            if let Some(item_type) = item_types.get(new_type) {
                if !item_type.stackable {
                    new_count = 1;
                }
            }
        }

        tile.items[index].type_id = new_type;
        tile.items[index].count = new_count;
        ensure_item_details_len(tile);
        if let Some(detail) = tile.item_details.get_mut(index) {
            detail.type_id = new_type;
            detail.count = new_count;
        }
        Ok(true)
    }

    pub fn apply_edit_text(
        &mut self,
        player_id: PlayerId,
        text_id: u32,
        text: &str,
    ) -> Result<(), String> {
        let session = match self.text_edit_sessions.remove(&text_id) {
            Some(session) => session,
            None => return Err("edit text id not found".to_string()),
        };
        if session.player_id != player_id {
            return Err("edit text player mismatch".to_string());
        }
        if !session.can_write {
            return Ok(());
        }
        let updated = truncate_text_to_len(text, session.max_len as usize);
        match session.target {
            TextEditTarget::Position {
                position,
                stack_index,
            } => {
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "edit text target tile missing".to_string())?;
                ensure_item_details_len(tile);
                let detail = tile
                    .item_details
                    .get_mut(stack_index)
                    .ok_or_else(|| "edit text target item missing".to_string())?;
                if detail.type_id != session.item_type {
                    return Err("edit text target item changed".to_string());
                }
                set_map_item_attribute(detail, "String", &updated)?;
            }
            TextEditTarget::Inventory { slot } => {
                let player = self
                    .players
                    .get_mut(&player_id)
                    .ok_or_else(|| format!("unknown player {:?}", player_id))?;
                let item = player
                    .inventory
                    .slot_mut(slot)
                    .ok_or_else(|| "edit text inventory item missing".to_string())?;
                if item.type_id != session.item_type {
                    return Err("edit text target item changed".to_string());
                }
                set_item_stack_attribute(item, "String", &updated)?;
            }
            TextEditTarget::Container { container_id, slot } => {
                {
                    let player = self
                        .players
                        .get_mut(&player_id)
                        .ok_or_else(|| format!("unknown player {:?}", player_id))?;
                    let container = player
                        .open_containers
                        .get_mut(&container_id)
                        .ok_or_else(|| "edit text target container missing".to_string())?;
                    let item = container
                        .items
                        .get_mut(slot as usize)
                        .ok_or_else(|| "edit text target item missing".to_string())?;
                    if item.type_id != session.item_type {
                        return Err("edit text target item changed".to_string());
                    }
                    set_item_stack_attribute(item, "String", &updated)?;
                }
                self.sync_container_contents(player_id, container_id);
            }
        }
        Ok(())
    }

    pub fn apply_edit_list(
        &mut self,
        player_id: PlayerId,
        list_type: u8,
        list_id: u32,
        text: &str,
    ) -> Result<(), String> {
        let session = match self.list_edit_sessions.remove(&list_id) {
            Some(session) => session,
            None => return Err("edit list id not found".to_string()),
        };
        if session.player_id != player_id {
            return Err("edit list player mismatch".to_string());
        }
        if session.list_type != list_type {
            return Err("edit list type mismatch".to_string());
        }
        let updated = truncate_text_to_len(text, session.max_len);
        match session.target {
            ListEditTarget::Door {
                position,
                stack_index,
                item_type,
            } => {
                if list_type != EDIT_LIST_TYPE_NAME_DOOR {
                    return Err("edit list target mismatch".to_string());
                }
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "edit list target tile missing".to_string())?;
                ensure_item_details_len(tile);
                let detail = tile
                    .item_details
                    .get_mut(stack_index)
                    .ok_or_else(|| "edit list target item missing".to_string())?;
                if detail.type_id != item_type {
                    return Err("edit list target item changed".to_string());
                }
                set_map_item_attribute(detail, "String", &updated)?;
            }
            ListEditTarget::House { house_id, kind } => {
                let expected = match kind {
                    HouseListKind::Guests => EDIT_LIST_TYPE_HOUSE_GUEST,
                    HouseListKind::Subowners => EDIT_LIST_TYPE_HOUSE_SUBOWNER,
                };
                if list_type != expected {
                    return Err("edit list target mismatch".to_string());
                }
                let owner = self
                    .house_owner_for_house_mut(house_id)
                    .ok_or_else(|| "house owner not found".to_string())?;
                let parsed = parse_access_list(&updated);
                match kind {
                    HouseListKind::Guests => owner.guests = parsed,
                    HouseListKind::Subowners => owner.subowners = parsed,
                }
                owner.last_transition = unix_time_now();
            }
        }
        Ok(())
    }

    fn try_open_list_edit(
        &mut self,
        player_id: PlayerId,
        position: Position,
        type_id: ItemTypeId,
    ) -> Option<MoveUseEditList> {
        let object_types = self.object_types.as_ref()?;
        let object_type = object_types.get(type_id)?;
        if !object_type.has_flag("NameDoor") {
            return None;
        }
        let tile = self.map.tile_mut(position)?;
        let stack_index = tile.items.iter().position(|item| item.type_id == type_id)?;
        ensure_item_details_len(tile);
        let detail = tile.item_details.get(stack_index)?;
        let text = Self::map_item_string(detail).unwrap_or_default();
        self.open_list_edit_session(
            player_id,
            EDIT_LIST_TYPE_NAME_DOOR,
            text,
            ListEditTarget::Door {
                position,
                stack_index,
                item_type: type_id,
            },
        )
    }

    fn try_open_text_edit(
        &mut self,
        player_id: PlayerId,
        position: Position,
        type_id: ItemTypeId,
    ) -> Option<MoveUseEditText> {
        let tile = self.map.tile_mut(position)?;
        let stack_index = tile.items.iter().position(|item| item.type_id == type_id)?;
        ensure_item_details_len(tile);
        let detail = tile.item_details.get(stack_index)?;
        let text = Self::map_item_string(detail).unwrap_or_default();
        self.open_text_edit_session(
            player_id,
            type_id,
            text,
            TextEditTarget::Position {
                position,
                stack_index,
            },
        )
    }

    fn open_list_edit_session(
        &mut self,
        player_id: PlayerId,
        list_type: u8,
        mut text: String,
        target: ListEditTarget,
    ) -> Option<MoveUseEditList> {
        if text.len() > MAX_EDIT_LIST_LEN {
            text = truncate_text_to_len(&text, MAX_EDIT_LIST_LEN);
        }
        let edit_id = self.allocate_list_edit_id();
        self.list_edit_sessions.insert(
            edit_id,
            ListEditSession {
                player_id,
                list_type,
                target,
                max_len: MAX_EDIT_LIST_LEN,
            },
        );
        Some(MoveUseEditList {
            id: edit_id,
            list_type,
            text,
        })
    }

    pub(crate) fn open_house_list(
        &mut self,
        player_id: PlayerId,
        kind: HouseListKind,
    ) -> Result<MoveUseEditList, String> {
        let player = self
            .players
            .get(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        let house = self
            .house_for_position(player.position)
            .ok_or_else(|| "not inside a house".to_string())?;
        let access = self.house_access_level(player, house);
        let allowed = match kind {
            HouseListKind::Guests => matches!(access, HouseAccessLevel::Subowner | HouseAccessLevel::Owner),
            HouseListKind::Subowners => access == HouseAccessLevel::Owner,
        };
        if !allowed {
            return Err("house access list blocked".to_string());
        }
        let owner = self
            .house_owner_for_house(house.id)
            .ok_or_else(|| "house owner missing".to_string())?;
        let text = match kind {
            HouseListKind::Guests => format_access_list(&owner.guests),
            HouseListKind::Subowners => format_access_list(&owner.subowners),
        };
        let list_type = match kind {
            HouseListKind::Guests => EDIT_LIST_TYPE_HOUSE_GUEST,
            HouseListKind::Subowners => EDIT_LIST_TYPE_HOUSE_SUBOWNER,
        };
        self.open_list_edit_session(
            player_id,
            list_type,
            text,
            ListEditTarget::House {
                house_id: house.id,
                kind,
            },
        )
        .ok_or_else(|| "edit list session unavailable".to_string())
    }

    pub fn open_text_edit_for_inventory(
        &mut self,
        player_id: PlayerId,
        slot: InventorySlot,
        type_id: ItemTypeId,
    ) -> Option<MoveUseEditText> {
        let player = self.players.get(&player_id)?;
        let item = player.inventory.slot(slot)?;
        if item.type_id != type_id {
            return None;
        }
        let text = Self::item_stack_string(item).unwrap_or_default();
        self.open_text_edit_session(
            player_id,
            type_id,
            text,
            TextEditTarget::Inventory { slot },
        )
    }

    pub fn open_text_edit_for_container(
        &mut self,
        player_id: PlayerId,
        container_id: u8,
        slot: u8,
        type_id: ItemTypeId,
    ) -> Option<MoveUseEditText> {
        let player = self.players.get(&player_id)?;
        let container = player.open_containers.get(&container_id)?;
        let item = container.items.get(slot as usize)?;
        if item.type_id != type_id {
            return None;
        }
        let text = Self::item_stack_string(item).unwrap_or_default();
        self.open_text_edit_session(
            player_id,
            type_id,
            text,
            TextEditTarget::Container { container_id, slot },
        )
    }

    fn open_text_edit_session(
        &mut self,
        player_id: PlayerId,
        type_id: ItemTypeId,
        mut text: String,
        target: TextEditTarget,
    ) -> Option<MoveUseEditText> {
        let object_types = self.object_types.as_ref()?;
        let object_type = object_types.get(type_id)?;
        let has_text = object_type.has_flag("Text")
            || object_type.has_flag("Write")
            || object_type.has_flag("WriteOnce");
        if !has_text {
            return None;
        }
        let write_once = object_type.has_flag("WriteOnce");
        let can_write = object_type.has_flag("Write") || (write_once && text.is_empty());
        let max_len = if write_once {
            object_type
                .attribute_u16("MaxLengthOnce")
                .or_else(|| object_type.attribute_u16("MaxLength"))
        } else {
            object_type.attribute_u16("MaxLength")
        }
        .unwrap_or(DEFAULT_TEXT_MAX_LEN);
        if text.len() > max_len as usize {
            text = truncate_text_to_len(&text, max_len as usize);
        }
        let send_len = if can_write {
            max_len
        } else {
            text.len().min(u16::MAX as usize) as u16
        };
        let edit_id = self.allocate_text_edit_id();
        self.text_edit_sessions.insert(
            edit_id,
            TextEditSession {
                player_id,
                item_type: type_id,
                target,
                max_len,
                can_write,
            },
        );
        Some(MoveUseEditText {
            id: edit_id,
            item_type: type_id,
            max_len: send_len,
            text,
            author: String::new(),
            date: String::new(),
        })
    }

    fn allocate_text_edit_id(&mut self) -> u32 {
        let mut next = self.next_text_edit_id.max(1);
        while self.text_edit_sessions.contains_key(&next) {
            next = next.wrapping_add(1).max(1);
        }
        self.next_text_edit_id = next.wrapping_add(1).max(1);
        next
    }

    fn allocate_list_edit_id(&mut self) -> u32 {
        let mut next = self.next_list_edit_id.max(1);
        while self.list_edit_sessions.contains_key(&next) {
            next = next.wrapping_add(1).max(1);
        }
        self.next_list_edit_id = next.wrapping_add(1).max(1);
        next
    }

    fn run_moveuse_event(
        &mut self,
        ctx: MoveUseContext,
        clock: Option<&GameClock>,
    ) -> Result<MoveUseOutcome, String> {
        let moveuse = match self.moveuse.as_ref() {
            Some(moveuse) => moveuse,
            None => {
                return Ok(MoveUseOutcome {
                    matched_rule: None,
                    ignored_actions: Vec::new(),
                    effects: Vec::new(),
                    texts: Vec::new(),
                    edit_texts: Vec::new(),
                    edit_lists: Vec::new(),
                    messages: Vec::new(),
                    damages: Vec::new(),
                    quest_updates: Vec::new(),
                    logout_users: Vec::new(),
                    refresh_positions: Vec::new(),
                    inventory_updates: Vec::new(),
                    container_updates: Vec::new(),
                });
            }
        };

        let (rule, next_rng_state) = {
            let mut rng_state = self.moveuse_rng.state;
            let rule = find_moveuse_rule(moveuse, &ctx, self, &mut rng_state)?;
            (rule.cloned(), rng_state)
        };
        self.moveuse_rng.state = next_rng_state;

        let Some(rule) = rule else {
            return Ok(MoveUseOutcome {
                matched_rule: None,
                ignored_actions: Vec::new(),
                effects: Vec::new(),
                texts: Vec::new(),
                edit_texts: Vec::new(),
                edit_lists: Vec::new(),
                messages: Vec::new(),
                damages: Vec::new(),
                quest_updates: Vec::new(),
                logout_users: Vec::new(),
                refresh_positions: Vec::new(),
                inventory_updates: Vec::new(),
                container_updates: Vec::new(),
            });
        };

        let mut outcome = MoveUseOutcome {
            matched_rule: Some(rule.line_no),
            ignored_actions: Vec::new(),
            effects: Vec::new(),
            texts: Vec::new(),
            edit_texts: Vec::new(),
            edit_lists: Vec::new(),
            messages: Vec::new(),
            damages: Vec::new(),
            quest_updates: Vec::new(),
            logout_users: Vec::new(),
            refresh_positions: Vec::new(),
            inventory_updates: Vec::new(),
            container_updates: Vec::new(),
        };

        self.apply_moveuse_actions(&rule, &ctx, &mut outcome, clock)?;
        self.apply_moveuse_outcome_damage(&mut outcome)?;

        Ok(outcome)
    }

    fn trigger_moveuse_tile_event(
        &mut self,
        event: MoveUseEvent,
        user_id: PlayerId,
        user_position: Position,
        tile_position: Position,
    ) -> Vec<MoveUseOutcome> {
        let Some(tile) = self.map.tile(tile_position) else {
            return Vec::new();
        };
        let items = tile.items.clone();
        let mut outcomes = Vec::new();
        for item in items {
            let ctx = MoveUseContext {
                event,
                user_id,
                user_position,
                object_position: tile_position,
                object_type_id: item.type_id,
                object_source: UseObjectSource::Map(tile_position),
                object2_position: None,
                object2_type_id: None,
            };
            match self.run_moveuse_event(ctx, None) {
                Ok(outcome) => {
                    if moveuse_outcome_has_payload(&outcome) {
                        outcomes.push(outcome);
                    }
                }
                Err(err) => {
                    eprintln!("tibia: moveuse {:?} failed: {}", event, err);
                }
            }
        }
        outcomes
    }

    fn resolve_movement_destination(
        &self,
        origin: Position,
        direction: Direction,
    ) -> Result<Position, String> {
        let next = origin
            .step(direction)
            .ok_or_else(|| "movement overflow".to_string())?;
        if !self.position_in_bounds(next) {
            return Err("movement blocked: out of bounds".to_string());
        }
        if !self.map.tiles.is_empty() && !self.map.has_tile(next) {
            return Err("movement blocked: missing tile".to_string());
        }
        if self.position_occupied(next) {
            return Err("movement blocked: creature".to_string());
        }
        let mut destination = next;
        if let Some(tile) = self.map.tile(next) {
            let moveuse_floor_change = self.tile_moveuse_floor_change(tile);
            let floor_change = if moveuse_floor_change {
                None
            } else {
                self.tile_floor_change(tile)
            };
            if self.tile_blocks_movement(tile) && floor_change.is_none() && !moveuse_floor_change {
                return Err("movement blocked: tile blocked".to_string());
            }
            if let Some(change) = floor_change {
                if let Some(vertical) = self.apply_floor_change(next, change) {
                    if !self.position_in_bounds(vertical) {
                        return Err("movement blocked: floor change out of bounds".to_string());
                    }
                    if self.map.tiles.is_empty() || self.map.has_tile(vertical) {
                        if let Some(target_tile) = self.map.tile(vertical) {
                            if self.tile_blocks_movement(target_tile) {
                                return Err("movement blocked: target tile blocked".to_string());
                            }
                        }
                        if self.position_occupied(vertical) {
                            return Err("movement blocked: creature".to_string());
                        }
                        destination = vertical;
                    }
                }
            }
        }
        Ok(destination)
    }

    fn resolve_monster_movement_destination(
        &mut self,
        origin: Position,
        direction: Direction,
        flags: MonsterFlags,
    ) -> Result<Position, String> {
        let next = origin
            .step(direction)
            .ok_or_else(|| "movement overflow".to_string())?;
        if !self.position_in_bounds(next) {
            return Err("movement blocked: out of bounds".to_string());
        }
        if !self.map.tiles.is_empty() && !self.map.has_tile(next) {
            return Err("movement blocked: missing tile".to_string());
        }
        if self.is_protection_zone(next) {
            return Err("movement blocked: protection zone".to_string());
        }
        if self.position_occupied(next) {
            let kicked = if flags.kick_creatures {
                self.try_kick_creature(next, direction)?
            } else {
                false
            };
            if !kicked {
                return Err("movement blocked: creature".to_string());
            }
        }
        let mut destination = next;
        if self.map.tile(next).is_some() {
            let moveuse_floor_change = self
                .map
                .tile(next)
                .map(|tile| self.tile_moveuse_floor_change(tile))
                .unwrap_or(false);
            let floor_change = if moveuse_floor_change {
                None
            } else {
                self.map.tile(next).and_then(|tile| self.tile_floor_change(tile))
            };
            let tile_blocks = self
                .map
                .tile(next)
                .map(|tile| self.tile_blocks_movement(tile))
                .unwrap_or(false);
            if let Some(tile) = self.map.tile(next) {
                if self.tile_has_avoid_unmove(tile) {
                    return Err("movement blocked: avoid".to_string());
                }
            }
            if tile_blocks && floor_change.is_none() && !moveuse_floor_change {
                let kicked = if flags.kick_boxes {
                    self.try_kick_boxes(next, direction)?
                } else {
                    false
                };
                if !kicked {
                    return Err("movement blocked: tile blocked".to_string());
                }
                if let Some(tile) = self.map.tile(next) {
                    if self.tile_blocks_movement(tile) {
                        return Err("movement blocked: tile blocked".to_string());
                    }
                }
            }
            if let Some(change) = floor_change {
                if let Some(vertical) = self.apply_floor_change(next, change) {
                    if !self.position_in_bounds(vertical) {
                        return Err("movement blocked: floor change out of bounds".to_string());
                    }
                    if self.map.tiles.is_empty() || self.map.has_tile(vertical) {
                        if let Some(target_tile) = self.map.tile(vertical) {
                            if self.tile_blocks_movement(target_tile) {
                                return Err("movement blocked: target tile blocked".to_string());
                            }
                        }
                        if self.position_occupied(vertical) {
                            return Err("movement blocked: creature".to_string());
                        }
                        destination = vertical;
                    }
                }
            }
        }
        if self.is_protection_zone(destination) {
            return Err("movement blocked: protection zone".to_string());
        }
        Ok(destination)
    }

    fn try_kick_boxes(
        &mut self,
        position: Position,
        direction: Direction,
    ) -> Result<bool, String> {
        let Some(object_types) = self.object_types.as_ref() else {
            return Ok(false);
        };
        let blocking_index = {
            let Some(source_tile) = self.map.tile(position) else {
                return Ok(false);
            };
            let mut blocking_indices = Vec::new();
            for (index, item) in source_tile.items.iter().enumerate().rev() {
                let Some(object_type) = object_types.get(item.type_id) else {
                    continue;
                };
                if !object_type.blocks_movement() {
                    continue;
                }
                if !object_type.is_movable() {
                    return Ok(false);
                }
                blocking_indices.push(index);
            }
            if blocking_indices.len() != 1 {
                return Ok(false);
            }
            blocking_indices[0]
        };
        let target = match position.step(direction) {
            Some(target) => target,
            None => return Ok(false),
        };
        if !self.position_in_bounds(target) {
            return Ok(false);
        }
        if !self.map.tiles.is_empty() && !self.map.has_tile(target) {
            return Ok(false);
        }
        if self.position_occupied(target) {
            return Ok(false);
        }
        if let Some(tile) = self.map.tile(target) {
            if self.tile_blocks_movement(tile) {
                return Ok(false);
            }
        }
        let item = {
            let Some(source_tile) = self.map.tile_mut(position) else {
                return Ok(false);
            };
            let Some(item) = remove_item_at_index(source_tile, blocking_index) else {
                return Ok(false);
            };
            item
        };
        let stackable = self.stackable_for(item.type_id);
        let movable = self.item_is_movable(&item);
        let placed = {
            let Some(dest_tile) = self.map.tile_mut(target) else {
                let Some(source_tile) = self.map.tile_mut(position) else {
                    return Ok(false);
                };
                insert_item_at_index(source_tile, blocking_index, item);
                return Ok(false);
            };
            place_on_tile_with_dustbin(dest_tile, item.clone(), stackable, movable).is_ok()
        };
        if !placed {
            let Some(source_tile) = self.map.tile_mut(position) else {
                return Ok(false);
            };
            insert_item_at_index(source_tile, blocking_index, item);
            return Ok(false);
        }
        Ok(true)
    }

    fn try_kick_creature(
        &mut self,
        position: Position,
        direction: Direction,
    ) -> Result<bool, String> {
        let target = match position.step(direction) {
            Some(target) => target,
            None => return Ok(false),
        };
        if !self.position_in_bounds(target) {
            return Ok(false);
        }
        if !self.map.tiles.is_empty() && !self.map.has_tile(target) {
            return Ok(false);
        }
        if self.position_occupied(target) {
            return Ok(false);
        }

        if let Some(player_id) = self
            .players
            .iter()
            .find_map(|(id, player)| (player.position == position).then_some(*id))
        {
            let destination = match self.resolve_movement_destination(position, direction) {
                Ok(destination) => destination,
                Err(_) => return Ok(false),
            };
            if let Some(player) = self.players.get_mut(&player_id) {
                player.move_to(destination, direction);
            }
            let separation = self.trigger_moveuse_tile_event(
                MoveUseEvent::Separation,
                player_id,
                position,
                position,
            );
            let collision = self.trigger_moveuse_tile_event(
                MoveUseEvent::Collision,
                player_id,
                destination,
                destination,
            );
            self.queue_moveuse_outcomes(player_id, separation);
            self.queue_moveuse_outcomes(player_id, collision);
            return Ok(true);
        }

        if let Some(monster_id) = self
            .monsters
            .iter()
            .find_map(|(id, monster)| (monster.position == position).then_some(*id))
        {
            let flags = match self.monsters.get(&monster_id) {
                Some(monster) => monster.flags,
                None => return Ok(false),
            };
            if flags.unpushable {
                return Ok(false);
            }
            let mut push_flags = flags;
            push_flags.kick_boxes = false;
            let destination =
                match self.resolve_monster_movement_destination(position, direction, push_flags) {
                    Ok(destination) => destination,
                    Err(_) => return Ok(false),
                };
            if let Some(monster) = self.monsters.get_mut(&monster_id) {
                let origin = monster.position;
                monster.position = destination;
                self.update_monster_sector_index(monster_id, origin, destination);
            }
            return Ok(true);
        }

        Ok(false)
    }

    fn position_in_bounds(&self, position: Position) -> bool {
        let bounds = self
            .map_dat
            .as_ref()
            .and_then(|map_dat| map_dat.sector_bounds)
            .or(self.map.sector_bounds);

        let sector = self.map.sector_for_position(position);

        if let Some(bounds) = bounds {
            if sector.x < bounds.min.x
                || sector.y < bounds.min.y
                || sector.z < bounds.min.z
                || sector.x > bounds.max.x
                || sector.y > bounds.max.y
                || sector.z > bounds.max.z
            {
                return false;
            }
        }

        if !self.map.sectors.is_empty() && !self.map.has_sector(sector) {
            return false;
        }

        true
    }

    fn tile_blocks_movement(&self, tile: &Tile) -> bool {
        let Some(object_types) = self.object_types.as_ref() else {
            return false;
        };
        tile.items.iter().any(|item| {
            object_types
                .get(item.type_id)
                .map_or(false, |object_type| object_type.blocks_movement())
        })
    }

    fn tile_has_avoid_unmove(&self, tile: &Tile) -> bool {
        let Some(object_types) = self.object_types.as_ref() else {
            return false;
        };
        tile.items.iter().any(|item| {
            object_types.get(item.type_id).map_or(false, |object_type| {
                object_type.has_flag("Avoid") && object_type.has_flag("Unmove")
            })
        })
    }

    fn position_occupied(&self, position: Position) -> bool {
        if self
            .players
            .values()
            .any(|player| player.position == position)
        {
            return true;
        }
        if self
            .monsters
            .values()
            .any(|monster| monster.position == position)
        {
            return true;
        }
        self.npcs.values().any(|npc| npc.position == position)
    }

    fn position_occupied_except(
        &self,
        position: Position,
        ignore: Option<CreatureId>,
        reserved: &HashSet<Position>,
    ) -> bool {
        if reserved.contains(&position) {
            return true;
        }
        if self
            .players
            .values()
            .any(|player| player.position == position)
        {
            return true;
        }
        if self
            .monsters
            .iter()
            .any(|(id, monster)| Some(*id) != ignore && monster.position == position)
        {
            return true;
        }
        self.npcs
            .iter()
            .any(|(id, npc)| Some(*id) != ignore && npc.position == position)
    }

    fn find_free_refresh_position(
        &self,
        origin: Position,
        ignore: Option<CreatureId>,
        reserved: &HashSet<Position>,
    ) -> Option<Position> {
        let positions = circle_positions(self.circles.as_ref(), origin, 1);
        for pos in positions {
            if pos == origin {
                continue;
            }
            if !self.position_in_bounds(pos) {
                continue;
            }
            let Some(tile) = self.map.tile(pos) else {
                continue;
            };
            if self.tile_blocks_movement(tile) {
                continue;
            }
            if self.position_occupied_except(pos, ignore, reserved) {
                continue;
            }
            return Some(pos);
        }
        None
    }

    fn find_login_position(
        &self,
        origin: Position,
        distance: u8,
        allow_house: bool,
    ) -> Option<Position> {
        let mut positions = circle_positions(self.circles.as_ref(), origin, distance);
        if positions.is_empty() {
            positions.push(origin);
        } else if !positions.contains(&origin) {
            positions.insert(0, origin);
        }
        for pos in positions {
            if !self.position_in_bounds(pos) {
                continue;
            }
            if !self.map.tiles.is_empty() && !self.map.has_tile(pos) {
                continue;
            }
            if !allow_house && self.house_for_position(pos).is_some() {
                continue;
            }
            let Some(tile) = self.map.tile(pos) else {
                continue;
            };
            if self.tile_blocks_movement(tile) {
                continue;
            }
            if self.position_occupied(pos) {
                continue;
            }
            return Some(pos);
        }
        None
    }

    fn tile_floor_change(&self, tile: &Tile) -> Option<FloorChange> {
        let Some(object_types) = self.object_types.as_ref() else {
            return None;
        };
        let mut up = false;
        for item in &tile.items {
            if let Some(object_type) = object_types.get(item.type_id) {
                match object_type.floor_change_hint() {
                    Some(FloorChange::Down) => return Some(FloorChange::Down),
                    Some(FloorChange::Up) => up = true,
                    None => {}
                }
            }
        }
        if up {
            Some(FloorChange::Up)
        } else {
            None
        }
    }

    fn tile_moveuse_floor_change(&self, tile: &Tile) -> bool {
        let Some(moveuse) = self.moveuse.as_ref() else {
            return false;
        };
        tile.items
            .iter()
            .any(|item| moveuse_has_collision_move_top_rel(moveuse, item.type_id))
    }

    fn apply_floor_change(&self, position: Position, change: FloorChange) -> Option<Position> {
        match change {
            FloorChange::Up => position.offset(crate::world::position::PositionDelta {
                dx: 1,
                dy: 1,
                dz: -1,
            }),
            FloorChange::Down => position.offset(crate::world::position::PositionDelta {
                dx: -1,
                dy: -1,
                dz: 1,
            }),
        }
    }

    fn apply_moveuse_actions(
        &mut self,
        rule: &MoveUseRule,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
        clock: Option<&GameClock>,
    ) -> Result<(), String> {
        for action in &rule.actions {
            if let Some(action_name) = action.name.strip_prefix('!') {
                outcome
                    .ignored_actions
                    .push(format!("{}{}", "!", action_name));
                continue;
            }

            match action.name.as_str() {
                "Change" => self.moveuse_change(action, ctx, outcome)?,
                "ChangeAttribute" => self.moveuse_change_attribute(action, ctx, outcome)?,
                "ChangeRel" => self.moveuse_change_rel(action, ctx, outcome)?,
                "ChangeOnMap" => self.moveuse_change_on_map(action, outcome)?,
                "Create" => self.moveuse_create(action, ctx, outcome)?,
                "CreateOnMap" => self.moveuse_create_on_map(action, outcome)?,
                "Damage" => self.moveuse_damage(action, ctx, outcome)?,
                "DeleteInInventory" => self.moveuse_delete_in_inventory(action, ctx)?,
                "Delete" => self.moveuse_delete(action, ctx, outcome)?,
                "DeleteOnMap" => self.moveuse_delete_on_map(action, outcome)?,
                "DeleteTopOnMap" => self.moveuse_delete_top_on_map(action, outcome)?,
                "Description" => self.moveuse_description(action, ctx, outcome)?,
                "Effect" => self.moveuse_effect(action, ctx, outcome)?,
                "EffectOnMap" => self.moveuse_effect_on_map(action, outcome)?,
                "LoadDepot" => self.moveuse_load_depot(action, ctx, outcome)?,
                "Logout" => self.moveuse_logout(action, ctx, outcome, clock)?,
                "Monster" => self.moveuse_monster(action, ctx)?,
                "MonsterOnMap" => self.moveuse_monster_on_map(action)?,
                "Move" => self.moveuse_move(action, ctx)?,
                "MoveRel" => self.moveuse_move_rel(action, ctx)?,
                "MoveTop" => self.moveuse_move_top(action, ctx)?,
                "MoveTopOnMap" => self.moveuse_move_top_on_map(action, outcome)?,
                "MoveTopRel" => self.moveuse_move_top_rel(action, ctx)?,
                "NOP" => {}
                "Retrieve" => self.moveuse_retrieve(action, ctx, outcome)?,
                "SaveDepot" => self.moveuse_save_depot(action, ctx)?,
                "SendMail" => self.moveuse_send_mail(action, ctx, outcome)?,
                "SetAttribute" => self.moveuse_set_attribute(action, ctx, outcome)?,
                "SetStart" => self.moveuse_set_start(action, ctx)?,
                "SetQuestValue" => self.moveuse_set_quest_value(action, ctx, outcome)?,
                "Text" => self.moveuse_text(action, ctx, outcome)?,
                "WriteName" => self.moveuse_write_name(action, ctx, outcome)?,
                other => outcome.ignored_actions.push(other.to_string()),
            }
        }

        Ok(())
    }

    fn apply_moveuse_outcome_damage(&mut self, outcome: &mut MoveUseOutcome) -> Result<(), String> {
        for damage in &mut outcome.damages {
            if let MoveUseActor::User(player_id) = damage.target {
                let reduced_damage = self.apply_player_protection_reduction(
                    player_id,
                    damage.damage_type,
                    damage.amount,
                );
                let player = self
                    .players
                    .get_mut(&player_id)
                    .ok_or_else(|| format!("unknown player {:?}", player_id))?;
                let (applied, _) =
                    player.apply_damage_with_magic_shield(damage.damage_type, reduced_damage);
                damage.amount = applied;
            }
        }
        Ok(())
    }

    fn moveuse_change(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("Change expects 3 args".to_string());
        }
        let target = action.args[0].trim();
        let to_type = parse_item_type_id(&action.args[1])?;
        let count = parse_optional_count(&action.args[2])?;
        match target {
            "Obj1" => match ctx.object_source {
                UseObjectSource::Map(position) => {
                    self.change_item_on_tile(position, ctx.object_type_id, to_type, count)?;
                    record_moveuse_refresh(outcome, position);
                }
                UseObjectSource::Inventory(slot) => {
                    let item = {
                        let player = self
                            .players
                            .get(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let item = player
                            .inventory
                            .slot(slot)
                            .cloned()
                            .ok_or_else(|| "inventory slot empty".to_string())?;
                        if item.type_id != ctx.object_type_id {
                            return Err("Change target missing".to_string());
                        }
                        item
                    };
                    let mut updated = item.clone();
                    self.change_itemstack_type(&mut updated, to_type, count)?;
                    let player = self
                        .players
                        .get_mut(&ctx.user_id)
                        .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                    player.inventory.set_slot(slot, Some(updated));
                }
                UseObjectSource::Container { container_id, slot } => {
                    let item = {
                        let player = self
                            .players
                            .get(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let container = player
                            .open_containers
                            .get(&container_id)
                            .ok_or_else(|| "container not open".to_string())?;
                        let index = slot as usize;
                        if index >= container.items.len() {
                            return Err("container slot out of range".to_string());
                        }
                        let item = container.items[index].clone();
                        if item.type_id != ctx.object_type_id {
                            return Err("Change target missing".to_string());
                        }
                        item
                    };
                    let mut updated = item.clone();
                    self.change_itemstack_type(&mut updated, to_type, count)?;
                    let update = {
                        let player = self
                            .players
                            .get_mut(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let container = player
                            .open_containers
                            .get_mut(&container_id)
                            .ok_or_else(|| "container not open".to_string())?;
                        let index = slot as usize;
                        if index >= container.items.len() {
                            return Err("container slot out of range".to_string());
                        }
                        container.items[index] = updated.clone();
                        ContainerUpdate::Update {
                            container_id,
                            slot,
                            item: updated,
                        }
                    };
                    outcome.container_updates.push(update);
                    self.sync_container_contents(ctx.user_id, container_id);
                }
            },
            "Obj2" => {
                let position = ctx
                    .object2_position
                    .ok_or_else(|| "Change missing Obj2 position".to_string())?;
                let from_type = ctx
                    .object2_type_id
                    .ok_or_else(|| "Change missing Obj2 type".to_string())?;
                self.change_item_on_tile(position, from_type, to_type, count)?;
                record_moveuse_refresh(outcome, position);
            }
            _ => return Err("Change expects Obj1 or Obj2 as first arg".to_string()),
        }
        Ok(())
    }

    fn moveuse_change_rel(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 5 {
            return Err("ChangeRel expects 5 args".to_string());
        }
        if action.args[0].trim() != "Obj1" {
            return Err("ChangeRel expects Obj1 as first arg".to_string());
        }
        let delta = parse_delta_arg(&action.args[1])?;
        let position = ctx
            .object_position
            .offset(delta)
            .ok_or_else(|| "ChangeRel target out of bounds".to_string())?;
        let from_type = parse_item_type_id(&action.args[2])?;
        let to_type = parse_item_type_id(&action.args[3])?;
        let count = parse_optional_count(&action.args[4])?;
        self.change_item_on_tile(position, from_type, to_type, count)?;
        record_moveuse_refresh(outcome, position);
        Ok(())
    }

    fn moveuse_change_on_map(
        &mut self,
        action: &MoveUseExpr,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 4 {
            return Err("ChangeOnMap expects 4 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let from_type = parse_item_type_id(&action.args[1])?;
        let to_type = parse_item_type_id(&action.args[2])?;
        let count = parse_optional_count(&action.args[3])?;
        self.change_item_on_tile(position, from_type, to_type, count)?;
        record_moveuse_refresh(outcome, position);
        Ok(())
    }

    fn change_item_on_tile(
        &mut self,
        position: Position,
        from_type: ItemTypeId,
        to_type: ItemTypeId,
        count: u16,
    ) -> Result<(), String> {
        let (mut item, index) = {
            let tile = self
                .map
                .tile_mut(position)
                .ok_or_else(|| "tile missing".to_string())?;
            let Some(index) = tile.items.iter().position(|item| item.type_id == from_type) else {
                return Err("item to change not found".to_string());
            };
            ensure_item_details_len(tile);
            let item = tile.items.remove(index);
            if index < tile.item_details.len() {
                tile.item_details.remove(index);
            }
            (item, index)
        };
        self.change_itemstack_type(&mut item, to_type, count)?;
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        let insert_index = index.min(tile.items.len());
        tile.items.insert(insert_index, item);
        ensure_item_details_len(tile);
        if let Some(added) = tile.items.get(insert_index) {
            tile.item_details.insert(insert_index, map_item_for_stack(added));
        }
        Ok(())
    }

    fn moveuse_create(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("Create expects 3 args".to_string());
        }
        let target = action.args[0].trim();
        let type_id = parse_item_type_id(&action.args[1])?;
        let count = parse_optional_count(&action.args[2])?;
        let stackable = self.stackable_for(type_id);
        let item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id,
            count: normalize_stack_count(count),
            attributes: Vec::new(),
            contents: Vec::new(),
};
        self.schedule_cron_for_item_tree(&item);
        let movable = self.item_is_movable(&item);
        match target {
            "Obj1" => match ctx.object_source {
                UseObjectSource::Map(position) => {
                    let tile = self
                        .map
                        .tile_mut(position)
                        .ok_or_else(|| "tile missing".to_string())?;
                    place_on_tile_with_dustbin(tile, item, stackable, movable)?;
                    record_moveuse_refresh(outcome, position);
                }
                UseObjectSource::Inventory(_) => {
                    let added = {
                        let player = self
                            .players
                            .get_mut(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        player.inventory.add_item(item.clone(), stackable)
                    };
                    if added.is_err() {
                        let tile = self
                            .map
                            .tile_mut(ctx.user_position)
                            .ok_or_else(|| "tile missing".to_string())?;
                        place_on_tile_with_dustbin(tile, item, stackable, movable)?;
                        record_moveuse_refresh(outcome, ctx.user_position);
                    }
                }
                UseObjectSource::Container { container_id, .. } => {
                    let update = {
                        let player = self
                            .players
                            .get_mut(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let container = player
                            .open_containers
                            .get_mut(&container_id)
                            .ok_or_else(|| "container not open".to_string())?;
                        insert_into_container(container, container_id, 0xff, item.clone(), stackable)
                    };
                    match update {
                        Ok(update) => {
                            outcome.container_updates.push(update);
                            self.sync_container_contents(ctx.user_id, container_id);
                        }
                        Err(_) => {
                            let tile = self
                                .map
                                .tile_mut(ctx.user_position)
                                .ok_or_else(|| "tile missing".to_string())?;
                            place_on_tile_with_dustbin(tile, item, stackable, movable)?;
                            record_moveuse_refresh(outcome, ctx.user_position);
                        }
                    }
                }
            },
            "Obj2" => {
                let position = ctx
                    .object2_position
                    .ok_or_else(|| "Create missing Obj2 position".to_string())?;
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "tile missing".to_string())?;
                place_on_tile_with_dustbin(tile, item, stackable, movable)?;
                record_moveuse_refresh(outcome, position);
            }
            _ => return Err("Create expects Obj1 or Obj2 as first arg".to_string()),
        }
        Ok(())
    }

    fn moveuse_create_on_map(
        &mut self,
        action: &MoveUseExpr,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("CreateOnMap expects 3 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let type_id = parse_item_type_id(&action.args[1])?;
        let count = parse_optional_count(&action.args[2])?;
        let stackable = self.stackable_for(type_id);
        let item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id,
            count: normalize_stack_count(count),
            attributes: Vec::new(),
            contents: Vec::new(),
};
        self.schedule_cron_for_item_tree(&item);
        let movable = self.item_is_movable(&item);
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        place_on_tile_with_dustbin(tile, item, stackable, movable)?;
        record_moveuse_refresh(outcome, position);
        Ok(())
    }

    fn moveuse_delete(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.is_empty() {
            return Err("Delete expects 1 arg".to_string());
        }
        let target = action.args[0].trim();
        match target {
            "Obj1" => match ctx.object_source {
                UseObjectSource::Map(position) => {
                    let tile = self
                        .map
                        .tile_mut(position)
                        .ok_or_else(|| "tile missing".to_string())?;
                    remove_item_from_tile(tile, ctx.object_type_id, false)
                        .ok_or_else(|| "Delete target missing".to_string())?;
                    record_moveuse_refresh(outcome, position);
                }
                UseObjectSource::Inventory(slot) => {
                    let player = self
                        .players
                        .get_mut(&ctx.user_id)
                        .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                    let item = player
                        .inventory
                        .slot(slot)
                        .cloned()
                        .ok_or_else(|| "inventory slot empty".to_string())?;
                    if item.type_id != ctx.object_type_id {
                        return Err("Delete target missing".to_string());
                    }
                    player.inventory.remove_item(slot, item.count)?;
                }
                UseObjectSource::Container { container_id, slot } => {
                    let stackable = self.stackable_for(ctx.object_type_id);
                    let update = {
                        let player = self
                            .players
                            .get_mut(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let container = player
                            .open_containers
                            .get_mut(&container_id)
                            .ok_or_else(|| "container not open".to_string())?;
                        let index = slot as usize;
                        if index >= container.items.len() {
                            return Err("container slot out of range".to_string());
                        }
                        let item = container.items[index].clone();
                        if item.type_id != ctx.object_type_id {
                            return Err("Delete target missing".to_string());
                        }
                        let (.., update) = take_from_container(
                            container,
                            container_id,
                            slot,
                            item.count,
                            stackable,
                            item.type_id,
                        )?;
                        update
                    };
                    outcome.container_updates.push(update);
                    self.sync_container_contents(ctx.user_id, container_id);
                }
            },
            "Obj2" => {
                let position = ctx
                    .object2_position
                    .ok_or_else(|| "Delete missing Obj2 position".to_string())?;
                let type_id = ctx
                    .object2_type_id
                    .ok_or_else(|| "Delete missing Obj2 type".to_string())?;
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "tile missing".to_string())?;
                remove_item_from_tile(tile, type_id, false)
                    .ok_or_else(|| "Delete target missing".to_string())?;
                record_moveuse_refresh(outcome, position);
            }
            _ => return Err("Delete expects Obj1 or Obj2 as first arg".to_string()),
        }
        Ok(())
    }

    fn moveuse_delete_in_inventory(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("DeleteInInventory expects 3 args".to_string());
        }
        match action.args[0].trim() {
            "User" | "Obj2" => {}
            _ => return Err("DeleteInInventory expects User or Obj2".to_string()),
        }
        let type_id = parse_item_type_id(&action.args[1])?;
        let count = normalize_stack_count(parse_optional_count(&action.args[2])?);
        let player = self
            .players
            .get_mut(&ctx.user_id)
            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
        remove_inventory_item(player, type_id, count)?;
        Ok(())
    }

    fn moveuse_delete_on_map(
        &mut self,
        action: &MoveUseExpr,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("DeleteOnMap expects 2 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let type_id = parse_item_type_id(&action.args[1])?;
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        remove_item_from_tile(tile, type_id, false)
            .ok_or_else(|| "DeleteOnMap target missing".to_string())?;
        record_moveuse_refresh(outcome, position);
        Ok(())
    }

    fn moveuse_delete_top_on_map(
        &mut self,
        action: &MoveUseExpr,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("DeleteTopOnMap expects 2 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let type_id = parse_item_type_id(&action.args[1])?;
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        remove_item_from_tile(tile, type_id, true)
            .ok_or_else(|| "DeleteTopOnMap target missing".to_string())?;
        record_moveuse_refresh(outcome, position);
        Ok(())
    }

    fn moveuse_move(&mut self, action: &MoveUseExpr, ctx: &MoveUseContext) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("Move expects 2 args".to_string());
        }
        match action.args[0].trim() {
            "User" | "Obj2" => {}
            _ => return Err("Move expects User or Obj2".to_string()),
        }
        let position = parse_position_arg(&action.args[1])?;
        self.teleport_player(ctx.user_id, position)?;
        Ok(())
    }

    fn moveuse_move_rel(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("MoveRel expects 3 args".to_string());
        }
        if action.args[0].trim() != "User" {
            return Err("MoveRel expects User as first arg".to_string());
        }
        let base = match action.args[1].trim() {
            "Obj1" => ctx.object_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "MoveRel missing Obj2 position".to_string())?,
            _ => return Err("MoveRel expects Obj1 or Obj2".to_string()),
        };
        let delta = parse_delta_arg(&action.args[2])?;
        let Some(target) = base.offset(delta) else {
            return Err("MoveRel target out of bounds".to_string());
        };
        self.teleport_player(ctx.user_id, target)?;
        Ok(())
    }

    fn moveuse_move_top(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("MoveTop expects 2 args".to_string());
        }
        if action.args[0].trim() != "Obj1" {
            return Err("MoveTop expects Obj1 as first arg".to_string());
        }
        let target = parse_position_arg(&action.args[1])?;
        self.teleport_player(ctx.user_id, target)?;
        Ok(())
    }

    fn moveuse_move_top_on_map(
        &mut self,
        action: &MoveUseExpr,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("MoveTopOnMap expects 3 args".to_string());
        }
        let from = parse_position_arg(&action.args[0])?;
        let type_id = parse_item_type_id(&action.args[1])?;
        let to = parse_position_arg(&action.args[2])?;
        let source_tile = self
            .map
            .tile_mut(from)
            .ok_or_else(|| "source tile missing".to_string())?;
        let item = remove_item_from_tile(source_tile, type_id, true)
            .ok_or_else(|| "MoveTopOnMap source missing".to_string())?;
        let stackable = self.stackable_for(type_id);
        let movable = self.item_is_movable(&item);
        let dest_tile = self
            .map
            .tile_mut(to)
            .ok_or_else(|| "destination tile missing".to_string())?;
        place_on_tile_with_dustbin(dest_tile, item, stackable, movable)?;
        record_moveuse_refresh(outcome, from);
        record_moveuse_refresh(outcome, to);
        Ok(())
    }

    fn move_top_item_between_tiles(
        &mut self,
        from: Position,
        to: Position,
    ) -> Result<bool, String> {
        let top_item_movable = {
            let source_tile = self
                .map
                .tile(from)
                .ok_or_else(|| "source tile missing".to_string())?;
            let Some(top_item) = source_tile.items.last() else {
                return Ok(false);
            };
            self.item_is_movable(top_item)
        };
        if !top_item_movable {
            return Ok(false);
        }
        let source_tile = self
            .map
            .tile_mut(from)
            .ok_or_else(|| "source tile missing".to_string())?;
        let Some(index) = source_tile.items.len().checked_sub(1) else {
            return Ok(false);
        };
        let Some(item) = remove_item_at_index(source_tile, index) else {
            return Ok(false);
        };
        let stackable = self.stackable_for(item.type_id);
        let movable = self.item_is_movable(&item);
        let dest_tile = self
            .map
            .tile_mut(to)
            .ok_or_else(|| "destination tile missing".to_string())?;
        place_on_tile_with_dustbin(dest_tile, item, stackable, movable)?;
        Ok(true)
    }

    fn moveuse_move_top_rel(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("MoveTopRel expects 2 args".to_string());
        }
        let base = match action.args[0].trim() {
            "Obj1" => ctx.object_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "MoveTopRel missing Obj2 position".to_string())?,
            _ => return Err("MoveTopRel expects Obj1 or Obj2".to_string()),
        };
        let delta = parse_delta_arg(&action.args[1])?;
        let target = base
            .offset(delta)
            .ok_or_else(|| "MoveTopRel target out of bounds".to_string())?;
        self.teleport_player(ctx.user_id, target)?;
        Ok(())
    }

    fn moveuse_load_depot(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 4 {
            return Err("LoadDepot expects 4 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let container_type = parse_item_type_id(&action.args[1])?;
        match action.args[2].trim() {
            "User" | "Obj2" => {}
            _ => return Err("LoadDepot expects User or Obj2".to_string()),
        }
        let depot_id = action.args[3]
            .trim()
            .parse::<u16>()
            .map_err(|_| "LoadDepot depot id parse failed".to_string())?;

        let capacity = self.depot_capacity(depot_id);
        let player = self
            .players
            .get_mut(&ctx.user_id)
            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
        let stored = player.depots.get(&depot_id).cloned().unwrap_or_default();
        let locker_contents = normalize_depot_contents(stored);
        let depot_item_count = depot_item_count(&locker_contents);
        let depot_space = capacity.map(|cap| cap.saturating_sub(depot_item_count));

        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        let locker_index = tile
            .items
            .iter()
            .position(|item| item.type_id == container_type)
            .ok_or_else(|| "depot locker missing".to_string())?;
        tile.items[locker_index].contents = locker_contents;
        ensure_item_details_len(tile);
        if let Some(detail) = tile.item_details.get_mut(locker_index) {
            *detail = map_item_for_stack(&tile.items[locker_index]);
        }

        player.active_depot = Some(ActiveDepot {
            depot_id,
            locker_position: position,
            locker_type: container_type,
            capacity,
        });

        outcome.messages.push(MoveUseMessage {
            player_id: ctx.user_id,
            message_type: 0x14,
            message: format!(
                "Your depot contains {} item{}.",
                depot_item_count,
                if depot_item_count == 1 { "" } else { "s" }
            ),
        });
        if let Some(space) = depot_space {
            if space == 0 {
                outcome.messages.push(MoveUseMessage {
                    player_id: ctx.user_id,
                    message_type: 0x14,
                    message: "Your depot is full. Remove surplus items before storing new ones."
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    fn moveuse_save_depot(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 4 {
            return Err("SaveDepot expects 4 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let container_type = parse_item_type_id(&action.args[1])?;
        match action.args[2].trim() {
            "User" | "Obj2" => {}
            _ => return Err("SaveDepot expects User or Obj2".to_string()),
        }
        let depot_id = action.args[3]
            .trim()
            .parse::<u16>()
            .map_err(|_| "SaveDepot depot id parse failed".to_string())?;

        self.close_depot_containers(ctx.user_id, position);

        let player = self
            .players
            .get_mut(&ctx.user_id)
            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
        let tile = self
            .map
            .tile_mut(position)
            .ok_or_else(|| "tile missing".to_string())?;
        let locker_index = tile
            .items
            .iter()
            .position(|item| item.type_id == container_type)
            .ok_or_else(|| "depot locker missing".to_string())?;
        let stored_items = normalize_depot_contents(tile.items[locker_index].contents.clone());
        if stored_items.is_empty() {
            player.depots.remove(&depot_id);
        } else {
            player.depots.insert(depot_id, stored_items);
        }
        tile.items[locker_index].contents.clear();
        ensure_item_details_len(tile);
        if let Some(detail) = tile.item_details.get_mut(locker_index) {
            *detail = map_item_for_stack(&tile.items[locker_index]);
        }
        if let Some(active) = player.active_depot {
            if active.depot_id == depot_id && active.locker_position == position {
                player.active_depot = None;
            }
        }
        Ok(())
    }

    fn moveuse_set_start(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("SetStart expects 2 args".to_string());
        }
        match action.args[0].trim() {
            "User" | "Obj2" => {}
            _ => return Err("SetStart expects User or Obj2".to_string()),
        }
        let position = parse_position_arg(&action.args[1])?;
        let player = self
            .players
            .get_mut(&ctx.user_id)
            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
        player.start_position = position;
        Ok(())
    }

    fn moveuse_effect(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("Effect expects 2 args".to_string());
        }
        let position = match action.args[0].trim() {
            "Obj1" => ctx.object_position,
            "User" => ctx.user_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "Effect missing Obj2 position".to_string())?,
            other => parse_position_arg(other)?,
        };
        let effect_id = parse_effect_id(&action.args[1])?;
        outcome.effects.push(MoveUseEffect {
            position,
            effect_id,
        });
        Ok(())
    }

    fn moveuse_effect_on_map(
        &mut self,
        action: &MoveUseExpr,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("EffectOnMap expects 2 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let effect_id = parse_effect_id(&action.args[1])?;
        outcome.effects.push(MoveUseEffect {
            position,
            effect_id,
        });
        Ok(())
    }

    fn moveuse_text(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("Text expects 3 args".to_string());
        }
        let position = match action.args[0].trim() {
            "Obj1" => ctx.object_position,
            "User" => ctx.user_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "Text missing Obj2 position".to_string())?,
            other => parse_position_arg(other)?,
        };
        let message = parse_string_arg(&action.args[1])?;
        let mode = action.args[2]
            .trim()
            .parse::<u8>()
            .map_err(|_| "Text mode parse failed".to_string())?;
        outcome.texts.push(MoveUseText {
            position,
            message,
            mode,
        });
        Ok(())
    }

    fn moveuse_description(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("Description expects 2 args".to_string());
        }
        if action.args[1].trim() != "User" {
            return Err("Description expects User as second arg".to_string());
        }
        let (position, type_id) = match action.args[0].trim() {
            "Obj1" => (ctx.object_position, Some(ctx.object_type_id)),
            "Obj2" => (
                ctx.object2_position
                    .ok_or_else(|| "Description missing Obj2 position".to_string())?,
                ctx.object2_type_id,
            ),
            other => (parse_position_arg(other)?, None),
        };
        let Some(tile) = self.map.tile(position) else {
            return Ok(());
        };
        let item = match type_id {
            Some(type_id) => tile.item_details.iter().find(|item| item.type_id == type_id),
            None => tile.item_details.first(),
        };
        let Some(item) = item else {
            return Ok(());
        };
        let description = item.attributes.iter().find_map(|attribute| {
            if let ItemAttribute::String(value) = attribute {
                Some(value.clone())
            } else {
                None
            }
        });
        if let Some(description) = description {
            outcome.messages.push(MoveUseMessage {
                player_id: ctx.user_id,
                message_type: 0x14,
                message: description,
            });
        }
        Ok(())
    }

    fn moveuse_write_name(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("WriteName expects 3 args".to_string());
        }
        if action.args[0].trim() != "User" {
            return Err("WriteName expects User as first arg".to_string());
        }
        if action.args[2].trim() != "Obj1" {
            return Err("WriteName expects Obj1 as third arg".to_string());
        }
        let template = parse_string_arg(&action.args[1])?;
        let player_name = self
            .players
            .get(&ctx.user_id)
            .map(|player| player.name.clone())
            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
        let text = template.replace("%N", &player_name);
        match ctx.object_source {
            UseObjectSource::Map(position) => {
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "tile missing".to_string())?;
                let item = map_item_for_type_mut(tile, ctx.object_type_id)
                    .ok_or_else(|| "WriteName target missing".to_string())?;
                set_map_item_attribute(item, "String", &text)?;
                record_moveuse_refresh(outcome, position);
            }
            UseObjectSource::Inventory(slot) => {
                let player = self
                    .players
                    .get_mut(&ctx.user_id)
                    .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                let item = player
                    .inventory
                    .slot_mut(slot)
                    .ok_or_else(|| "inventory slot empty".to_string())?;
                if item.type_id != ctx.object_type_id {
                    return Err("WriteName target missing".to_string());
                }
                set_item_stack_attribute(item, "String", &text)?;
            }
            UseObjectSource::Container { container_id, slot } => {
                let update = {
                    let player = self
                        .players
                        .get_mut(&ctx.user_id)
                        .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                    let container = player
                        .open_containers
                        .get_mut(&container_id)
                        .ok_or_else(|| "container not open".to_string())?;
                    let index = slot as usize;
                    if index >= container.items.len() {
                        return Err("container slot out of range".to_string());
                    }
                    let item = &mut container.items[index];
                    if item.type_id != ctx.object_type_id {
                        return Err("WriteName target missing".to_string());
                    }
                    set_item_stack_attribute(item, "String", &text)?;
                    ContainerUpdate::Update {
                        container_id,
                        slot,
                        item: item.clone(),
                    }
                };
                outcome.container_updates.push(update);
                self.sync_container_contents(ctx.user_id, container_id);
            }
        }
        Ok(())
    }

    fn moveuse_set_attribute(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("SetAttribute expects 3 args".to_string());
        }
        let target = action.args[0].trim();
        let key = action.args[1].trim();
        let value = action.args[2]
            .trim()
            .parse::<i32>()
            .map_err(|_| "SetAttribute value parse failed".to_string())?;
        match target {
            "Obj1" => match ctx.object_source {
                UseObjectSource::Map(position) => {
                    let tile = self
                        .map
                        .tile_mut(position)
                        .ok_or_else(|| "tile missing".to_string())?;
                    let item = map_item_for_type_mut(tile, ctx.object_type_id)
                        .ok_or_else(|| "SetAttribute target missing".to_string())?;
                    set_map_item_attribute_numeric(item, key, value)?;
                    record_moveuse_refresh(outcome, position);
                }
                UseObjectSource::Inventory(slot) => {
                    let player = self
                        .players
                        .get_mut(&ctx.user_id)
                        .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                    let item = player
                        .inventory
                        .slot_mut(slot)
                        .ok_or_else(|| "inventory slot empty".to_string())?;
                    if item.type_id != ctx.object_type_id {
                        return Err("SetAttribute target missing".to_string());
                    }
                    set_itemstack_attribute_numeric(item, key, value)?;
                }
                UseObjectSource::Container { container_id, slot } => {
                    let update = {
                        let player = self
                            .players
                            .get_mut(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let container = player
                            .open_containers
                            .get_mut(&container_id)
                            .ok_or_else(|| "container not open".to_string())?;
                        let index = slot as usize;
                        if index >= container.items.len() {
                            return Err("container slot out of range".to_string());
                        }
                        let item = &mut container.items[index];
                        if item.type_id != ctx.object_type_id {
                            return Err("SetAttribute target missing".to_string());
                        }
                        set_itemstack_attribute_numeric(item, key, value)?;
                        ContainerUpdate::Update {
                            container_id,
                            slot,
                            item: item.clone(),
                        }
                    };
                    outcome.container_updates.push(update);
                    self.sync_container_contents(ctx.user_id, container_id);
                }
            },
            "Obj2" => {
                let position = ctx
                    .object2_position
                    .ok_or_else(|| "SetAttribute missing Obj2 position".to_string())?;
                let type_id = ctx
                    .object2_type_id
                    .ok_or_else(|| "SetAttribute missing Obj2 type".to_string())?;
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "tile missing".to_string())?;
                let item = map_item_for_type_mut(tile, type_id)
                    .ok_or_else(|| "SetAttribute target missing".to_string())?;
                set_map_item_attribute_numeric(item, key, value)?;
                record_moveuse_refresh(outcome, position);
            }
            _ => return Err("SetAttribute expects Obj1 or Obj2 as first arg".to_string()),
        }
        Ok(())
    }

    fn moveuse_change_attribute(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("ChangeAttribute expects 3 args".to_string());
        }
        let target = action.args[0].trim();
        let key = action.args[1].trim();
        let delta = action.args[2]
            .trim()
            .parse::<i32>()
            .map_err(|_| "ChangeAttribute value parse failed".to_string())?;
        match target {
            "Obj1" => match ctx.object_source {
                UseObjectSource::Map(position) => {
                    let tile = self
                        .map
                        .tile_mut(position)
                        .ok_or_else(|| "tile missing".to_string())?;
                    let item = map_item_for_type_mut(tile, ctx.object_type_id)
                        .ok_or_else(|| "ChangeAttribute target missing".to_string())?;
                    change_map_item_attribute_numeric(item, key, delta)?;
                    record_moveuse_refresh(outcome, position);
                }
                UseObjectSource::Inventory(slot) => {
                    let player = self
                        .players
                        .get_mut(&ctx.user_id)
                        .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                    let item = player
                        .inventory
                        .slot_mut(slot)
                        .ok_or_else(|| "inventory slot empty".to_string())?;
                    if item.type_id != ctx.object_type_id {
                        return Err("ChangeAttribute target missing".to_string());
                    }
                    change_itemstack_attribute_numeric(item, key, delta)?;
                }
                UseObjectSource::Container { container_id, slot } => {
                    let update = {
                        let player = self
                            .players
                            .get_mut(&ctx.user_id)
                            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
                        let container = player
                            .open_containers
                            .get_mut(&container_id)
                            .ok_or_else(|| "container not open".to_string())?;
                        let index = slot as usize;
                        if index >= container.items.len() {
                            return Err("container slot out of range".to_string());
                        }
                        let item = &mut container.items[index];
                        if item.type_id != ctx.object_type_id {
                            return Err("ChangeAttribute target missing".to_string());
                        }
                        change_itemstack_attribute_numeric(item, key, delta)?;
                        ContainerUpdate::Update {
                            container_id,
                            slot,
                            item: item.clone(),
                        }
                    };
                    outcome.container_updates.push(update);
                    self.sync_container_contents(ctx.user_id, container_id);
                }
            },
            "Obj2" => {
                let position = ctx
                    .object2_position
                    .ok_or_else(|| "ChangeAttribute missing Obj2 position".to_string())?;
                let type_id = ctx
                    .object2_type_id
                    .ok_or_else(|| "ChangeAttribute missing Obj2 type".to_string())?;
                let tile = self
                    .map
                    .tile_mut(position)
                    .ok_or_else(|| "tile missing".to_string())?;
                let item = map_item_for_type_mut(tile, type_id)
                    .ok_or_else(|| "ChangeAttribute target missing".to_string())?;
                change_map_item_attribute_numeric(item, key, delta)?;
                record_moveuse_refresh(outcome, position);
            }
            _ => {
                return Err("ChangeAttribute expects Obj1 or Obj2 as first arg".to_string());
            }
        }
        Ok(())
    }

    fn moveuse_monster(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
    ) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("Monster expects 2 args".to_string());
        }
        let target = action.args[0].trim();
        let race_number = action.args[1]
            .trim()
            .parse::<i64>()
            .map_err(|_| "Monster race parse failed".to_string())?;
        let position = match target {
            "Obj1" => ctx.object_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "Monster missing Obj2 position".to_string())?,
            _ => return Err("Monster expects Obj1 or Obj2 as first arg".to_string()),
        };
        self.spawn_monster_by_race(race_number, position)?;
        Ok(())
    }

    fn moveuse_monster_on_map(&mut self, action: &MoveUseExpr) -> Result<(), String> {
        if action.args.len() < 2 {
            return Err("MonsterOnMap expects 2 args".to_string());
        }
        let position = parse_position_arg(&action.args[0])?;
        let race_number = action.args[1]
            .trim()
            .parse::<i64>()
            .map_err(|_| "MonsterOnMap race parse failed".to_string())?;
        self.spawn_monster_by_race(race_number, position)?;
        Ok(())
    }

    fn moveuse_retrieve(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("Retrieve expects 3 args".to_string());
        }
        let base = match action.args[0].trim() {
            "Obj1" => ctx.object_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "Retrieve missing Obj2 position".to_string())?,
            "User" => ctx.user_position,
            other => parse_position_arg(other)?,
        };
        let from_delta = parse_delta_arg(&action.args[1])?;
        let to_delta = parse_delta_arg(&action.args[2])?;
        let from = base
            .offset(from_delta)
            .ok_or_else(|| "Retrieve source out of bounds".to_string())?;
        let to = base
            .offset(to_delta)
            .ok_or_else(|| "Retrieve destination out of bounds".to_string())?;
        if self.move_top_item_between_tiles(from, to)? {
            record_moveuse_refresh(outcome, from);
            record_moveuse_refresh(outcome, to);
        }
        Ok(())
    }

    fn is_mailbox_type(type_id: ItemTypeId) -> bool {
        matches!(type_id.0, 3501 | 3508)
    }

    fn is_mail_item_type(&self, type_id: ItemTypeId) -> bool {
        if let Some(object_types) = self.object_types.as_ref() {
            if let Some(object_type) = object_types.get(type_id) {
                if let Some(meaning) = object_type.attribute_u16("Meaning") {
                    return (22..=26).contains(&meaning);
                }
            }
        }
        matches!(type_id.0, 3503 | 3504 | 3505 | 3506 | 3507)
    }

    fn is_mail_label_type(type_id: ItemTypeId) -> bool {
        matches!(type_id.0, 3507)
    }

    fn is_mail_letter_type(type_id: ItemTypeId) -> bool {
        matches!(type_id.0, 3505 | 3506)
    }

    fn is_mail_parcel_type(type_id: ItemTypeId) -> bool {
        matches!(type_id.0, 3503 | 3504)
    }

    fn is_mail_letter_new(type_id: ItemTypeId) -> bool {
        matches!(type_id.0, 3505)
    }

    fn is_mail_parcel_new(type_id: ItemTypeId) -> bool {
        matches!(type_id.0, 3503)
    }

    fn stamped_mail_type(type_id: ItemTypeId) -> ItemTypeId {
        match type_id.0 {
            3505 => ItemTypeId(3506),
            3503 => ItemTypeId(3504),
            _ => type_id,
        }
    }

    fn map_item_string(item: &MapItem) -> Option<String> {
        item.attributes.iter().find_map(|attribute| {
            if let ItemAttribute::String(text) = attribute {
                Some(text.clone())
            } else {
                None
            }
        })
    }

    fn item_stack_string(item: &ItemStack) -> Option<String> {
        item.attributes.iter().find_map(|attribute| {
            if let ItemAttribute::String(text) = attribute {
                Some(text.clone())
            } else {
                None
            }
        })
    }

    fn parse_mail_label(text: &str) -> Option<(String, String)> {
        let mut lines = text
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty());
        let name = lines.next()?.to_string();
        let town = lines.next()?.to_string();
        Some((name, town))
    }

    fn depot_id_for_town(&self, town: &str) -> Option<u16> {
        let trimmed = town.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Ok(parsed) = trimmed.parse::<u16>() {
            return Some(parsed);
        }
        self.map_dat
            .as_ref()
            .and_then(|map_dat| {
                map_dat
                    .depots
                    .iter()
                    .find(|depot| depot.name.eq_ignore_ascii_case(trimmed))
            })
            .map(|depot| depot.id)
    }

    fn mail_address_for_item(&self, item: &MapItem) -> Option<MailAddress> {
        let label_text = if Self::is_mail_label_type(item.type_id)
            || Self::is_mail_letter_type(item.type_id)
        {
            Self::map_item_string(item)
        } else if Self::is_mail_parcel_type(item.type_id) {
            item.contents.iter().find_map(|child| {
                if Self::is_mail_label_type(child.type_id) {
                    Self::map_item_string(child)
                } else {
                    None
                }
            })
        } else {
            None
        }?;
        let (name, town) = Self::parse_mail_label(&label_text)?;
        let depot_id = self.depot_id_for_town(&town)?;
        Some(MailAddress { name, depot_id })
    }

    fn append_mail_to_depot(
        depots: &mut HashMap<u16, Vec<ItemStack>>,
        depot_id: u16,
        item: ItemStack,
        capacity: Option<u32>,
    ) -> Result<(), String> {
        let depot_items = depots.entry(depot_id).or_insert_with(Vec::new);
        let normalized = normalize_depot_contents(std::mem::take(depot_items));
        *depot_items = normalized;
        let existing_count = depot_item_count(depot_items);
        let added_count = count_item_with_contents(&item);
        if let Some(capacity) = capacity {
            if existing_count.saturating_add(added_count) > capacity {
                return Err("depot capacity exceeded".to_string());
            }
        }
        if let Some(chest) = depot_items
            .iter_mut()
            .find(|stored| stored.type_id == DEPOT_CHEST_TYPE_ID)
        {
            chest.contents.push(item);
        } else {
            depot_items.push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: DEPOT_CHEST_TYPE_ID,
                count: 1,
                attributes: Vec::new(),
                contents: vec![item],
            });
        }
        Ok(())
    }

    fn deliver_mail_item(&mut self, address: &MailAddress, item: ItemStack) -> MailDelivery {
        let online_id = self
            .players
            .iter()
            .find_map(|(id, player)| {
                player
                    .name
                    .eq_ignore_ascii_case(address.name.trim())
                    .then_some(*id)
            });
        let capacity = self.depot_capacity(address.depot_id);
        if let Some(player_id) = online_id {
            if let Some(player) = self.players.get_mut(&player_id) {
                if Self::append_mail_to_depot(
                    &mut player.depots,
                    address.depot_id,
                    item,
                    capacity,
                )
                    .is_ok()
                {
                    return MailDelivery::DeliveredOnline(player_id);
                }
            }
            return MailDelivery::Failed;
        }
        let root = match self.root.as_ref() {
            Some(root) => root,
            None => return MailDelivery::Failed,
        };
        let player_id = match self.find_player_id_by_name_in_saves(address.name.as_str()) {
            Ok(Some(id)) => id,
            Ok(None) => return MailDelivery::Failed,
            Err(err) => {
                logging::log_error(&format!(
                    "mail delivery lookup failed for '{}': {}",
                    address.name, err
                ));
                return MailDelivery::Failed;
            }
        };
        let store = SaveStore::from_root(root);
        let mut player = match store.load_player(player_id) {
            Ok(Some(player)) => player,
            Ok(None) => return MailDelivery::Failed,
            Err(err) => {
                logging::log_error(&format!(
                    "mail delivery load failed for '{}': {}",
                    address.name, err
                ));
                return MailDelivery::Failed;
            }
        };
        if let Err(err) = Self::append_mail_to_depot(
            &mut player.depots,
            address.depot_id,
            item,
            capacity,
        ) {
            logging::log_error(&format!(
                "mail delivery depot failed for '{}': {}",
                address.name, err
            ));
            return MailDelivery::Failed;
        }
        if let Err(err) = store.save_player(&player) {
            logging::log_error(&format!(
                "mail delivery save failed for '{}': {}",
                address.name, err
            ));
            return MailDelivery::Failed;
        }
        MailDelivery::DeliveredOffline
    }

    fn find_player_id_by_name_in_saves(&self, name: &str) -> Result<Option<PlayerId>, String> {
        let root = match self.root.as_ref() {
            Some(root) => root,
            None => return Ok(None),
        };
        let dir = root.join("save").join("players");
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(format!("read save players dir failed: {}", err)),
        };
        let target = name.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let store = SaveStore::from_root(root);
        for entry in entries {
            let entry = entry.map_err(|err| format!("read save players entry failed: {}", err))?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sav") {
                continue;
            }
            let stem = match path.file_stem().and_then(|stem| stem.to_str()) {
                Some(stem) => stem,
                None => continue,
            };
            let id: u32 = match stem.parse() {
                Ok(id) => id,
                Err(_) => continue,
            };
            match store.load_player(PlayerId(id)) {
                Ok(Some(player)) => {
                    if player.name.eq_ignore_ascii_case(target) {
                        return Ok(Some(PlayerId(id)));
                    }
                }
                Ok(None) => {}
                Err(_) => {
                    if let Ok(contents) = std::fs::read_to_string(&path) {
                        if let Some(found) = Self::parse_saved_player_name(&contents) {
                            if found.eq_ignore_ascii_case(target) {
                                return Ok(Some(PlayerId(id)));
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn parse_saved_player_name(contents: &str) -> Option<String> {
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let line = if let Some(stripped) = trimmed.strip_prefix('#') {
                stripped.trim_start()
            } else {
                trimmed
            };
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            if key.trim().eq_ignore_ascii_case("name") {
                return Some(value.trim().trim_matches('"').to_string());
            }
        }
        None
    }

    fn private_channel_name(&self, owner: PlayerId) -> String {
        self.player_name_by_id(owner)
            .map(|name| format!("{name}'s Channel"))
            .unwrap_or_else(|| "Private Channel".to_string())
    }

    fn allocate_private_channel_id(&mut self) -> Option<u16> {
        let start = self.next_private_channel_id.max(PRIVATE_CHANNEL_ID_START);
        let mut next = start;
        loop {
            if !self.private_channels.contains_key(&next) {
                self.next_private_channel_id = next
                    .wrapping_add(1)
                    .max(PRIVATE_CHANNEL_ID_START);
                return Some(next);
            }
            next = next.wrapping_add(1);
            if next < PRIVATE_CHANNEL_ID_START {
                next = PRIVATE_CHANNEL_ID_START;
            }
            if next == start {
                return None;
            }
        }
    }

    fn find_player_id_by_name(&self, name: &str) -> Result<Option<PlayerId>, String> {
        let target = name.trim();
        if target.is_empty() {
            return Ok(None);
        }
        if let Some((id, _)) = self
            .players
            .iter()
            .find(|(_, player)| player.name.eq_ignore_ascii_case(target))
        {
            return Ok(Some(*id));
        }
        self.find_player_id_by_name_in_saves(target)
    }

    fn player_name_by_id(&self, id: PlayerId) -> Option<String> {
        if let Some(player) = self.players.get(&id) {
            return Some(player.name.clone());
        }
        let root = self.root.as_ref()?;
        let store = SaveStore::from_root(root);
        match store.load_player(id) {
            Ok(Some(player)) => return Some(player.name),
            Ok(None) | Err(_) => {}
        }
        let path = root
            .join("save")
            .join("players")
            .join(format!("{}.sav", id.0));
        if let Ok(contents) = std::fs::read_to_string(&path) {
            if let Some(found) = Self::parse_saved_player_name(&contents) {
                return Some(found);
            }
        }
        None
    }

    fn house_for_position(&self, position: Position) -> Option<&House> {
        if let (Some(index), Some(houses)) =
            (self.house_position_index.as_ref(), self.houses.as_ref())
        {
            return index.get(&position).and_then(|house_index| houses.get(*house_index));
        }
        self.houses
            .as_ref()
            .and_then(|houses| houses.iter().find(|house| house.fields.contains(&position)))
    }

    fn house_owner_for_house(&self, house_id: u32) -> Option<&HouseOwner> {
        self.house_owners
            .as_ref()
            .and_then(|owners| owners.iter().find(|owner| owner.id == house_id))
    }

    fn house_owner_for_house_mut(&mut self, house_id: u32) -> Option<&mut HouseOwner> {
        self.house_owners
            .as_mut()
            .and_then(|owners| owners.iter_mut().find(|owner| owner.id == house_id))
    }

    fn house_access_level(&self, player: &PlayerState, house: &House) -> HouseAccessLevel {
        if player.is_gm {
            return HouseAccessLevel::Owner;
        }
        let Some(owner) = self.house_owner_for_house(house.id) else {
            return HouseAccessLevel::Guest;
        };
        if owner.owner == 0 {
            return HouseAccessLevel::Guest;
        }
        if house.guild_house {
            if player.guild_id == Some(owner.owner) {
                return HouseAccessLevel::Owner;
            }
        }
        if owner.owner == player.id.0 {
            return HouseAccessLevel::Owner;
        }
        if let Some(owner_name) = self.player_name_by_id(PlayerId(owner.owner)) {
            if owner_name.eq_ignore_ascii_case(player.name.trim()) {
                return HouseAccessLevel::Owner;
            }
        }
        let name = player.name.trim();
        if name.is_empty() {
            return HouseAccessLevel::None;
        }
        if owner
            .subowners
            .iter()
            .any(|entry| entry.eq_ignore_ascii_case(name))
        {
            return HouseAccessLevel::Subowner;
        }
        if owner
            .guests
            .iter()
            .any(|entry| entry.eq_ignore_ascii_case(name))
        {
            return HouseAccessLevel::Guest;
        }
        HouseAccessLevel::None
    }

    fn player_can_enter_house(&self, player: &PlayerState, house: &House) -> bool {
        self.house_access_level(player, house) != HouseAccessLevel::None
    }

    fn player_can_manage_house_items(&self, player: &PlayerState, house: &House) -> bool {
        matches!(
            self.house_access_level(player, house),
            HouseAccessLevel::Owner | HouseAccessLevel::Subowner
        )
    }

    fn door_allows_player(&self, player: &PlayerState, position: Position) -> bool {
        if let Some(house) = self.house_for_position(position) {
            if matches!(
                self.house_access_level(player, house),
                HouseAccessLevel::Owner | HouseAccessLevel::Subowner
            ) {
                return true;
            }
        }
        let Some(tile) = self.map.tile(position) else {
            return true;
        };
        let Some(object_types) = self.object_types.as_ref() else {
            return true;
        };
        for (index, item) in tile.items.iter().enumerate() {
            let Some(object_type) = object_types.get(item.type_id) else {
                continue;
            };
            if !object_type.has_flag("NameDoor") {
                continue;
            }
            let allowed = match tile.item_details.get(index) {
                Some(detail) => {
                    let text = Self::map_item_string(detail).unwrap_or_default();
                    let entries = parse_access_list(&text);
                    if entries.is_empty() {
                        true
                    } else {
                        let name = player.name.trim();
                        if name.is_empty() {
                            false
                        } else {
                            entries
                                .iter()
                                .any(|entry| entry.eq_ignore_ascii_case(name))
                        }
                    }
                }
                None => true,
            };
            if !allowed {
                return false;
            }
        }
        true
    }

    fn ensure_house_item_access(&self, player_id: PlayerId, position: Position) -> Result<(), String> {
        let Some(house) = self.house_for_position(position) else {
            return Ok(());
        };
        let player = self
            .players
            .get(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        if self.player_can_manage_house_items(player, house) {
            Ok(())
        } else {
            Err("movement blocked: house item access".to_string())
        }
    }

    fn tile_has_bed(&self, position: Position) -> bool {
        let Some(tile) = self.map.tile(position) else {
            return false;
        };
        let Some(object_types) = self.object_types.as_ref() else {
            return false;
        };
        tile.items.iter().any(|item| {
            object_types
                .get(item.type_id)
                .map_or(false, |object_type| object_type.has_flag("Bed"))
        })
    }

    fn buddy_entry_for_id(&self, buddy_id: PlayerId) -> BuddyEntry {
        let name = self
            .player_name_by_id(buddy_id)
            .unwrap_or_else(|| format!("Buddy {}", buddy_id.0));
        BuddyEntry {
            id: buddy_id,
            name,
            online: self.players.contains_key(&buddy_id),
        }
    }

    fn send_mail_from_tile(
        &mut self,
        _user_id: PlayerId,
        mailbox_position: Position,
    ) -> Result<usize, String> {
        let tile = self
            .map
            .tile(mailbox_position)
            .ok_or_else(|| "tile missing".to_string())?;
        let has_mailbox = tile
            .items
            .iter()
            .any(|item| Self::is_mailbox_type(item.type_id));
        if !has_mailbox {
            return Ok(0);
        }

        let mut candidates = Vec::new();
        for (idx, item) in tile.items.iter().enumerate() {
            if !self.is_mail_item_type(item.type_id) {
                continue;
            }
            if !Self::is_mail_letter_new(item.type_id) && !Self::is_mail_parcel_new(item.type_id) {
                continue;
            }
            let address = tile
                .item_details
                .get(idx)
                .and_then(|detail| self.mail_address_for_item(detail));
            candidates.push((idx, item.clone(), address));
        }
        if candidates.is_empty() {
            return Ok(0);
        }

        let mut delivered_indices = Vec::new();
        let mut delivered_count = 0usize;
        let mut notified = HashSet::new();
        for (idx, item, address) in candidates {
            let Some(address) = address else {
                continue;
            };
            let mut stamped = item;
            stamped.type_id = Self::stamped_mail_type(stamped.type_id);
            match self.deliver_mail_item(&address, stamped) {
                MailDelivery::DeliveredOnline(recipient_id) => {
                    delivered_indices.push(idx);
                    delivered_count = delivered_count.saturating_add(1);
                    if notified.insert(recipient_id) {
                        self.queue_message(MoveUseMessage {
                            player_id: recipient_id,
                            message_type: 0x14,
                            message: "New mail has arrived.".to_string(),
                        });
                    }
                }
                MailDelivery::DeliveredOffline => {
                    delivered_indices.push(idx);
                    delivered_count = delivered_count.saturating_add(1);
                }
                MailDelivery::Failed => {}
            }
        }
        if delivered_indices.is_empty() {
            return Ok(0);
        }
        let tile = self
            .map
            .tile_mut(mailbox_position)
            .ok_or_else(|| "tile missing".to_string())?;
        delivered_indices.sort_unstable();
        for idx in delivered_indices.iter().rev() {
            let _ = remove_item_at_index(tile, *idx);
        }
        Ok(delivered_count)
    }

    fn moveuse_send_mail(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.is_empty() {
            return Err("SendMail expects 1 arg".to_string());
        }
        let position = match action.args[0].trim() {
            "User" => ctx.user_position,
            "Obj2" => ctx
                .object2_position
                .ok_or_else(|| "SendMail missing Obj2 position".to_string())?,
            _ => return Err("SendMail expects User or Obj2".to_string()),
        };
        let moved = self.send_mail_from_tile(ctx.user_id, position)?;
        if moved > 0 {
            outcome.messages.push(MoveUseMessage {
                player_id: ctx.user_id,
                message_type: 0x14,
                message: "Your mail has been sent.".to_string(),
            });
            record_moveuse_refresh(outcome, position);
        }
        Ok(())
    }

    fn moveuse_damage(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 4 {
            return Err("Damage expects 4 args".to_string());
        }
        let source = parse_damage_actor(&action.args[0], ctx)?;
        let target = parse_damage_actor(&action.args[1], ctx)?;
        let damage_type = DamageType::from_mask(parse_effect_id(&action.args[2])?);
        let amount = action.args[3]
            .trim()
            .parse::<u32>()
            .map_err(|_| "Damage amount parse failed".to_string())?;
        outcome.damages.push(MoveUseDamage {
            source,
            target,
            damage_type,
            amount,
        });
        Ok(())
    }

    fn moveuse_set_quest_value(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
    ) -> Result<(), String> {
        if action.args.len() < 3 {
            return Err("SetQuestValue expects 3 args".to_string());
        }
        match action.args[0].trim() {
            "User" | "Obj2" => {}
            _ => return Err("SetQuestValue expects User or Obj2 as first arg".to_string()),
        }
        let quest_id = action.args[1]
            .trim()
            .parse::<u16>()
            .map_err(|_| "SetQuestValue quest id parse failed".to_string())?;
        let value = action.args[2]
            .trim()
            .parse::<i32>()
            .map_err(|_| "SetQuestValue value parse failed".to_string())?;
        let player = self
            .players
            .get_mut(&ctx.user_id)
            .ok_or_else(|| format!("unknown player {:?}", ctx.user_id))?;
        player.quest_values.insert(quest_id, value);
        outcome.quest_updates.push(MoveUseQuestUpdate {
            player_id: ctx.user_id,
            quest_id,
            value,
        });
        Ok(())
    }

    fn moveuse_logout(
        &mut self,
        action: &MoveUseExpr,
        ctx: &MoveUseContext,
        outcome: &mut MoveUseOutcome,
        clock: Option<&GameClock>,
    ) -> Result<(), String> {
        if action.args.is_empty() {
            return Err("Logout expects 1 arg".to_string());
        }
        if action.args[0].trim() != "User" {
            return Err("Logout expects User as first arg".to_string());
        }

        if let Err(reason) = self.request_logout(ctx.user_id, clock) {
            return Err(reason.as_error().to_string());
        }
        let bed_position = self.players.get(&ctx.user_id).map(|player| player.position);
        let has_bed = bed_position.map(|pos| self.tile_has_bed(pos)).unwrap_or(false);
        if let Some(player) = self.players.get_mut(&ctx.user_id) {
            if has_bed {
                if let Some(position) = bed_position {
                    player.start_position = position;
                }
            }
            player.last_logout = unix_time_now();
        }
        outcome.logout_users.push(ctx.user_id);
        Ok(())
    }

    fn stackable_for(&self, type_id: ItemTypeId) -> bool {
        self.item_types
            .as_ref()
            .and_then(|item_types| item_types.get(type_id))
            .map(|item| item.stackable)
            .unwrap_or(false)
    }

    fn item_is_movable(&self, item: &ItemStack) -> bool {
        self.object_types
            .as_ref()
            .and_then(|types| types.get(item.type_id))
            .map(|object| object.has_flag("Take"))
            .unwrap_or(false)
    }

    fn tile_has_object_flag(&self, position: Position, flag: &str) -> bool {
        let Some(tile) = self.map.tile(position) else {
            return false;
        };
        let Some(object_types) = self.object_types.as_ref() else {
            return false;
        };
        tile.items.iter().any(|item| {
            object_types
                .get(item.type_id)
                .map_or(false, |object_type| object_type.has_flag(flag))
        })
    }

    fn throw_possible(&self, origin: Position, destination: Position, power: i32) -> bool {
        let orig_x = i32::from(origin.x);
        let orig_y = i32::from(origin.y);
        let orig_z = i32::from(origin.z);
        let dest_x = i32::from(destination.x);
        let dest_y = i32::from(destination.y);
        let dest_z = i32::from(destination.z);

        let mut min_z = (orig_z - power).max(0);
        if orig_z > 0 && min_z <= orig_z - 1 {
            for cur_z in (min_z..=orig_z - 1).rev() {
                let pos = Position {
                    x: origin.x,
                    y: origin.y,
                    z: cur_z as u8,
                };
                if self.tile_has_object_flag(pos, "Bank") {
                    min_z = cur_z + 1;
                    break;
                }
            }
        }

        let max_t = (dest_x - orig_x).abs().max((dest_y - orig_y).abs());
        let mut start_t = 1;
        if (dest_x < orig_x && self.tile_has_object_flag(origin, "HookEast"))
            || (dest_y < orig_y && self.tile_has_object_flag(origin, "HookSouth"))
        {
            start_t = 0;
        }

        while min_z <= dest_z {
            let mut last_x = orig_x;
            let mut last_y = orig_y;
            if max_t > 0 {
                for t in start_t..=max_t {
                    let cur_x = (orig_x * (max_t - t) + dest_x * t) / max_t;
                    let cur_y = (orig_y * (max_t - t) + dest_y * t) / max_t;
                    let pos = Position {
                        x: cur_x as u16,
                        y: cur_y as u16,
                        z: min_z as u8,
                    };
                    if self.tile_has_object_flag(pos, "Unthrow") {
                        break;
                    }
                    last_x = cur_x;
                    last_y = cur_y;
                }
            }
            if last_x == dest_x && last_y == dest_y {
                let mut last_z = min_z;
                while last_z < dest_z {
                    let pos = Position {
                        x: dest_x as u16,
                        y: dest_y as u16,
                        z: last_z as u8,
                    };
                    if self.tile_has_object_flag(pos, "Bank") {
                        break;
                    }
                    last_z += 1;
                }
                if last_z == dest_z {
                    return true;
                }
            }
            min_z += 1;
        }

        false
    }

    fn tile_has_dustbin(tile: &Tile) -> bool {
        tile.items
            .iter()
            .any(|item| item.type_id == DUSTBIN_TYPE_ID)
    }

    fn should_delete_on_dustbin(tile: &Tile, movable: bool) -> bool {
        Self::tile_has_dustbin(tile) && movable
    }

    fn slot_allows_item(&self, slot: InventorySlot, type_id: ItemTypeId) -> bool {
        let Some(object_types) = self.object_types.as_ref() else {
            return true;
        };
        let Some(object_type) = object_types.get(type_id) else {
            return true;
        };
        if slot == InventorySlot::Ammo {
            return true;
        }
        if matches!(slot, InventorySlot::RightHand | InventorySlot::LeftHand) {
            return true;
        }
        let Some(body) = object_type.body_position() else {
            return false;
        };
        match body {
            0 => matches!(slot, InventorySlot::RightHand | InventorySlot::LeftHand),
            1 => slot == InventorySlot::Head,
            2 => slot == InventorySlot::Necklace,
            3 => slot == InventorySlot::Backpack,
            4 => slot == InventorySlot::Armor,
            7 => slot == InventorySlot::Legs,
            8 => slot == InventorySlot::Feet,
            9 => slot == InventorySlot::Ring,
            _ => false,
        }
    }

    fn ensure_slot_allows_item(
        &self,
        slot: InventorySlot,
        type_id: ItemTypeId,
    ) -> Result<(), String> {
        if self.slot_allows_item(slot, type_id) {
            Ok(())
        } else {
            Err("item cannot be equipped in slot".to_string())
        }
    }

    fn is_two_handed_item(&self, type_id: ItemTypeId) -> bool {
        let Some(object_types) = self.object_types.as_ref() else {
            return false;
        };
        let Some(object_type) = object_types.get(type_id) else {
            return false;
        };
        let is_weapon = object_type.has_flag("Weapon")
            || object_type.attribute("WeaponType").is_some()
            || object_type.attribute("WeaponAttackValue").is_some();
        object_type.body_position() == Some(0) && is_weapon
    }

    fn ensure_two_handed_slot_free(
        &self,
        player_id: PlayerId,
        to_slot: InventorySlot,
        from_slot: Option<InventorySlot>,
        type_id: ItemTypeId,
    ) -> Result<(), String> {
        if !matches!(to_slot, InventorySlot::RightHand | InventorySlot::LeftHand) {
            return Ok(());
        }
        if !self.is_two_handed_item(type_id) {
            return Ok(());
        }
        let other_slot = match to_slot {
            InventorySlot::RightHand => InventorySlot::LeftHand,
            InventorySlot::LeftHand => InventorySlot::RightHand,
            _ => return Ok(()),
        };
        if from_slot == Some(other_slot) {
            return Ok(());
        }
        let player = self
            .players
            .get(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        if player.inventory.slot(other_slot).is_some() {
            Err("Both hands must be free.".to_string())
        } else {
            Ok(())
        }
    }

    fn slot_container_capacity(&self, player_id: PlayerId, slot: InventorySlot) -> Option<u8> {
        let player = self.players.get(&player_id)?;
        let item = player.inventory.slot(slot)?;
        let item_types = self.item_types.as_ref()?;
        let capacity = item_types.get(item.type_id).and_then(|entry| entry.container_capacity)?;
        Some(capacity.min(u8::MAX as u16) as u8)
    }

    fn insert_into_inventory_slot_container(
        &mut self,
        player_id: PlayerId,
        slot: InventorySlot,
        item: ItemStack,
        stackable: bool,
    ) -> Result<Option<ContainerUpdate>, String> {
        let capacity = self
            .slot_container_capacity(player_id, slot)
            .ok_or_else(|| "target slot is not a container".to_string())?;
        let open_container_id = {
            let player = self
                .players
                .get(&player_id)
                .ok_or_else(|| format!("unknown player {:?}", player_id))?;
            player.open_containers.iter().find_map(|(id, container)| {
                if container.source_slot == Some(slot) {
                    Some(*id)
                } else {
                    None
                }
            })
        };
        if let Some(container_id) = open_container_id {
            let update = {
                let player = self
                    .players
                    .get_mut(&player_id)
                    .ok_or_else(|| format!("unknown player {:?}", player_id))?;
                let container = player
                    .open_containers
                    .get_mut(&container_id)
                    .ok_or_else(|| "container not open".to_string())?;
                insert_into_container(container, container_id, 0xff, item, stackable)?
            };
            self.sync_container_contents(player_id, container_id);
            return Ok(Some(update));
        }
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or_else(|| format!("unknown player {:?}", player_id))?;
        let items = player.inventory_containers.entry(slot).or_insert_with(Vec::new);
        insert_into_inventory_container_items(items, capacity, item, stackable)?;
        Ok(None)
    }

    fn depot_capacity(&self, depot_id: u16) -> Option<u32> {
        self.map_dat
            .as_ref()
            .and_then(|map_dat| map_dat.depots.iter().find(|depot| depot.id == depot_id))
            .map(|depot| depot.capacity)
    }

    fn teleport_player(&mut self, id: PlayerId, position: Position) -> Result<(), String> {
        if !self.position_in_bounds(position) {
            return Err("target out of bounds".to_string());
        }
        let is_test_god = self
            .players
            .get(&id)
            .map(|player| player.is_test_god)
            .unwrap_or(false);
        if self.map.tile(position).is_none() && is_test_god {
            self.map.tiles.insert(
                position,
                Tile {
                    position,
                    items: Vec::new(),
                    item_details: Vec::new(),
                    refresh: false,
                    protection_zone: false,
                    no_logout: false,
                    annotations: Vec::new(),
                    tags: Vec::new(),
                },
            );
        }
        let tile = self
            .map
            .tile(position)
            .ok_or_else(|| "target tile missing".to_string())?;
        if self.tile_blocks_movement(tile) && !is_test_god {
            return Err("target tile blocked".to_string());
        }
        let player = self
            .players
            .get_mut(&id)
            .ok_or_else(|| format!("unknown player {:?}", id))?;
        player.move_to(position, player.direction);
        Ok(())
    }

    fn next_monster_id(&mut self) -> CreatureId {
        let id = self.next_monster_id.max(1);
        self.next_monster_id = self.next_monster_id.saturating_add(1);
        CreatureId(id)
    }

    fn next_npc_id(&mut self) -> CreatureId {
        let id = self.next_npc_id.max(NPC_ID_BASE);
        self.next_npc_id = self.next_npc_id.saturating_add(1);
        CreatureId(id)
    }

    fn ensure_raid_schedules(&mut self, now: GameTick, clock: &GameClock) {
        if !self.raid_schedules.is_empty() {
            return;
        }
        let Some(index) = self.monster_index.as_ref() else {
            return;
        };
        for (name, raid) in &index.raids {
            let Some(interval) = raid.interval else {
                continue;
            };
            if interval <= 0 {
                continue;
            }
            let ticks = clock.ticks_from_duration_round_up(Duration::from_secs(interval as u64));
            if ticks == 0 {
                continue;
            }
            let next_at = GameTick(now.0.saturating_add(ticks));
            self.raid_schedules.insert(
                name.clone(),
                RaidSchedule {
                    interval_ticks: ticks,
                    next_at,
                },
            );
        }
    }
}

fn index_item_tree(
    index: &mut HashMap<ItemId, ItemPath>,
    root: ItemRoot,
    item: &ItemStack,
    path: &mut Vec<usize>,
) {
    index.insert(
        item.id,
        ItemPath {
            root: root.clone(),
            path: path.clone(),
        },
    );

    for (child_index, child) in item.contents.iter().enumerate() {
        path.push(child_index);
        index_item_tree(index, root.clone(), child, path);
        path.pop();
    }
}

fn raid_seed(name: &str, now: GameTick) -> u64 {
    let mut hash = 0u64;
    for byte in name.as_bytes() {
        hash = hash.wrapping_mul(16777619).wrapping_add(u64::from(*byte));
    }
    hash ^ now.0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseOutcome {
    pub matched_rule: Option<usize>,
    pub ignored_actions: Vec<String>,
    pub effects: Vec<MoveUseEffect>,
    pub texts: Vec<MoveUseText>,
    pub edit_texts: Vec<MoveUseEditText>,
    pub edit_lists: Vec<MoveUseEditList>,
    pub messages: Vec<MoveUseMessage>,
    pub damages: Vec<MoveUseDamage>,
    pub quest_updates: Vec<MoveUseQuestUpdate>,
    pub logout_users: Vec<PlayerId>,
    pub refresh_positions: Vec<Position>,
    pub inventory_updates: Vec<InventorySlot>,
    pub container_updates: Vec<ContainerUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseEditText {
    pub id: u32,
    pub item_type: ItemTypeId,
    pub max_len: u16,
    pub text: String,
    pub author: String,
    pub date: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseEditList {
    pub id: u32,
    pub list_type: u8,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseEffect {
    pub position: Position,
    pub effect_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseText {
    pub position: Position,
    pub message: String,
    pub mode: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseMessage {
    pub player_id: PlayerId,
    pub message_type: u8,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseDamage {
    pub source: MoveUseActor,
    pub target: MoveUseActor,
    pub damage_type: DamageType,
    pub amount: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveUseActor {
    None,
    User(PlayerId),
    Object(Position),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveUseQuestUpdate {
    pub player_id: PlayerId,
    pub quest_id: u16,
    pub value: i32,
}

#[derive(Debug, Clone, Copy)]
struct MoveUseRng {
    state: u64,
}

impl MoveUseRng {
    fn from_time() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0x9e3779b97f4a7c15);
        Self { state: seed }
    }

    fn from_seed(seed: u64) -> Self {
        let seed = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state: seed }
    }

    fn roll_percent(&mut self, chance: u32) -> bool {
        if chance >= 100 {
            return true;
        }
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let bucket = (self.state >> 32) as u32 % 100;
        bucket < chance
    }

    fn roll_range(&mut self, min: u32, max: u32) -> u32 {
        let (min, max) = if min >= max { (min, min) } else { (min, max) };
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let span = u64::from(max - min) + 1;
        let value = ((self.state >> 32) as u64) % span;
        min + value as u32
    }
}

impl Default for MoveUseRng {
    fn default() -> Self {
        Self { state: 0x9e3779b97f4a7c15 }
    }
}

#[derive(Debug, Clone, Copy)]
struct MonsterRng {
    state: u64,
}

impl MonsterRng {
    fn from_time() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0x9e3779b97f4a7c15);
        Self { state: seed }
    }

    fn from_seed(seed: u64) -> Self {
        let seed = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state: seed }
    }

    fn roll_range(&mut self, min: u32, max: u32) -> u32 {
        let (min, max) = if min >= max { (min, min) } else { (min, max) };
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let span = u64::from(max - min) + 1;
        let value = ((self.state >> 32) as u64) % span;
        min + value as u32
    }

    fn roll_percent(&mut self, chance: u32) -> bool {
        let chance = chance.min(100);
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let bucket = (self.state >> 32) as u32 % 100;
        bucket < chance
    }

    fn roll_direction(&mut self) -> Direction {
        const DIRECTIONS: [Direction; 8] = [
            Direction::North,
            Direction::East,
            Direction::South,
            Direction::West,
            Direction::Northeast,
            Direction::Northwest,
            Direction::Southeast,
            Direction::Southwest,
        ];
        let index = if DIRECTIONS.len() <= 1 {
            0
        } else {
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
            (self.state >> 32) as usize % DIRECTIONS.len()
        };
        DIRECTIONS[index]
    }
}

impl Default for MonsterRng {
    fn default() -> Self {
        Self { state: 0x9e3779b97f4a7c15 }
    }
}

#[derive(Debug, Clone, Copy)]
struct NpcRng {
    state: u64,
}

impl NpcRng {
    fn from_time() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0x9e3779b97f4a7c15);
        Self { state: seed }
    }

    fn from_seed(seed: u64) -> Self {
        let seed = if seed == 0 { 0x9e3779b97f4a7c15 } else { seed };
        Self { state: seed }
    }

    fn roll_direction(&mut self) -> Direction {
        const DIRECTIONS: [Direction; 8] = [
            Direction::North,
            Direction::East,
            Direction::South,
            Direction::West,
            Direction::Northeast,
            Direction::Northwest,
            Direction::Southeast,
            Direction::Southwest,
        ];
        let index = if DIRECTIONS.len() <= 1 {
            0
        } else {
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
            (self.state >> 32) as usize % DIRECTIONS.len()
        };
        DIRECTIONS[index]
    }
}

impl Default for NpcRng {
    fn default() -> Self {
        Self { state: 0x9e3779b97f4a7c15 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UseObjectSource {
    Map(Position),
    Inventory(InventorySlot),
    Container { container_id: u8, slot: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveUseEvent {
    Use,
    MultiUse,
    Collision,
    Separation,
}

impl MoveUseEvent {
    fn as_str(self) -> &'static str {
        match self {
            MoveUseEvent::Use => "Use",
            MoveUseEvent::MultiUse => "MultiUse",
            MoveUseEvent::Collision => "Collision",
            MoveUseEvent::Separation => "Separation",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MoveUseContext {
    event: MoveUseEvent,
    user_id: PlayerId,
    user_position: Position,
    object_position: Position,
    object_type_id: ItemTypeId,
    object_source: UseObjectSource,
    object2_position: Option<Position>,
    object2_type_id: Option<ItemTypeId>,
}

fn find_moveuse_rule<'a>(
    moveuse: &'a MoveUseDatabase,
    ctx: &MoveUseContext,
    state: &WorldState,
    rng_state: &mut u64,
) -> Result<Option<&'a MoveUseRule>, String> {
    for section in &moveuse.sections {
        if let Some(rule) = find_moveuse_rule_in_section(section, ctx, state, rng_state)? {
            return Ok(Some(rule));
        }
    }
    Ok(None)
}

fn find_moveuse_rule_in_section<'a>(
    section: &'a MoveUseSection,
    ctx: &MoveUseContext,
    state: &WorldState,
    rng_state: &mut u64,
) -> Result<Option<&'a MoveUseRule>, String> {
    for rule in &section.rules {
        if matches_moveuse_rule(rule, ctx, state, rng_state)? {
            return Ok(Some(rule));
        }
    }
    for child in &section.children {
        if let Some(rule) = find_moveuse_rule_in_section(child, ctx, state, rng_state)? {
            return Ok(Some(rule));
        }
    }
    Ok(None)
}

fn collect_moveuse_rules(moveuse: &MoveUseDatabase) -> Vec<MoveUseRule> {
    let mut rules = Vec::new();
    for section in &moveuse.sections {
        collect_moveuse_rules_in_section(section, &mut rules);
    }
    rules
}

fn collect_moveuse_rules_in_section(section: &MoveUseSection, out: &mut Vec<MoveUseRule>) {
    out.extend(section.rules.iter().cloned());
    for child in &section.children {
        collect_moveuse_rules_in_section(child, out);
    }
}

fn parse_moveuse_event_name(name: &str) -> Option<MoveUseEvent> {
    match name.trim() {
        "Use" => Some(MoveUseEvent::Use),
        "MultiUse" => Some(MoveUseEvent::MultiUse),
        "Movement" => Some(MoveUseEvent::Collision),
        "Collision" => Some(MoveUseEvent::Collision),
        "Separation" => Some(MoveUseEvent::Separation),
        _ => None,
    }
}

fn moveuse_rule_obj_type(rule: &MoveUseRule, target: &str) -> Option<ItemTypeId> {
    for condition in &rule.conditions {
        if condition.name.trim() != "IsType" || condition.args.len() < 2 {
            continue;
        }
        if condition.args[0].trim() != target {
            continue;
        }
        if let Ok(type_id) = parse_item_type_id(&condition.args[1]) {
            return Some(type_id);
        }
    }
    None
}

fn matches_moveuse_rule(
    rule: &MoveUseRule,
    ctx: &MoveUseContext,
    state: &WorldState,
    rng_state: &mut u64,
) -> Result<bool, String> {
    let event_name = rule.event.name.trim();
    let event_matches = event_name == ctx.event.as_str()
        || (event_name == "Movement" && ctx.event == MoveUseEvent::Collision);
    if !event_matches {
        return Ok(false);
    }

    for condition in &rule.conditions {
        if !evaluate_moveuse_condition(condition, ctx, state, rng_state)? {
            return Ok(false);
        }
    }

    Ok(true)
}

fn evaluate_moveuse_condition(
    condition: &MoveUseExpr,
    ctx: &MoveUseContext,
    state: &WorldState,
    rng_state: &mut u64,
) -> Result<bool, String> {
    let is_test_god = state
        .players
        .get(&ctx.user_id)
        .map(|player| player.is_test_god)
        .unwrap_or(false);
    let (name, negated) = if let Some(stripped) = condition.name.strip_prefix('!') {
        (stripped, true)
    } else {
        (condition.name.as_str(), false)
    };

    let result = match name {
        "IsType" => {
            if condition.args.len() < 2 {
                return Err("IsType expects 2 args".to_string());
            }
            let type_id = parse_item_type_id(&condition.args[1])?;
            match condition.args[0].trim() {
                "Obj1" => ctx.object_type_id == type_id,
                "Obj2" => ctx.object2_type_id == Some(type_id),
                _ => return Err("IsType expects Obj1 or Obj2".to_string()),
            }
        }
        "IsPosition" => {
            if condition.args.len() < 2 {
                return Err("IsPosition expects 2 args".to_string());
            }
            let target = parse_position_arg(&condition.args[1])?;
            match condition.args[0].trim() {
                "Obj1" => ctx.object_position == target,
                "User" => ctx.user_position == target,
                "Obj2" => ctx.object2_position == Some(target),
                _ => return Err("IsPosition expects Obj1, Obj2, or User".to_string()),
            }
        }
        "IsObjectThere" => {
            if condition.args.len() < 2 {
                return Err("IsObjectThere expects 2 args".to_string());
            }
            let position = parse_position_arg(&condition.args[0])?;
            let type_id = parse_item_type_id(&condition.args[1])?;
            state
                .map
                .tile(position)
                .map(|tile| tile.items.iter().any(|item| item.type_id == type_id))
                .unwrap_or(false)
        }
        "IsProtectionZone" => {
            if condition.args.is_empty() {
                return Err("IsProtectionZone expects 1 arg".to_string());
            }
            let position = match condition.args[0].trim() {
                "Obj1" => Some(ctx.object_position),
                "User" => Some(ctx.user_position),
                "Obj2" => ctx.object2_position,
                other => Some(parse_position_arg(other)?),
            };
            let Some(position) = position else {
                return Ok(false);
            };
            state
                .map
                .tile(position)
                .map(|tile| tile.protection_zone)
                .unwrap_or(false)
        }
        "Random" => {
            if condition.args.is_empty() {
                return Err("Random expects 1 arg".to_string());
            }
            let chance = condition.args[0]
                .trim()
                .parse::<u32>()
                .map_err(|_| "Random chance parse failed".to_string())?;
            let mut rng = MoveUseRng { state: *rng_state };
            let result = rng.roll_percent(chance);
            *rng_state = rng.state;
            result
        }
        "IsHouse" => {
            if condition.args.is_empty() {
                return Err("IsHouse expects 1 arg".to_string());
            }
            let position = match condition.args[0].trim() {
                "Obj1" => Some(ctx.object_position),
                "User" => Some(ctx.user_position),
                "Obj2" => ctx.object2_position,
                other => Some(parse_position_arg(other)?),
            };
            let Some(position) = position else {
                return Ok(false);
            };
            state
                .houses
                .as_ref()
                .map(|houses| houses.iter().any(|house| house.fields.contains(&position)))
                .unwrap_or(false)
        }
        "IsHouseOwner" => {
            if condition.args.len() < 2 {
                return Err("IsHouseOwner expects 2 args".to_string());
            }
            if condition.args[1].trim() != "User" {
                return Err("IsHouseOwner expects Obj1 or Obj2, User".to_string());
            }
            let position = match condition.args[0].trim() {
                "Obj1" => Some(ctx.object_position),
                "Obj2" => ctx.object2_position,
                _ => return Err("IsHouseOwner expects Obj1 or Obj2, User".to_string()),
            };
            let Some(position) = position else {
                return Ok(false);
            };
            let Some(houses) = state.houses.as_ref() else {
                return Ok(false);
            };
            let Some(owners) = state.house_owners.as_ref() else {
                return Ok(false);
            };
            let Some(house) = houses.iter().find(|house| house.fields.contains(&position)) else {
                return Ok(false);
            };
            owners
                .iter()
                .find(|owner| owner.id == house.id)
                .map(|owner| owner.owner == ctx.user_id.0)
                .unwrap_or(false)
        }
        "IsPlayer" => {
            if condition.args.is_empty() {
                return Err("IsPlayer expects 1 arg".to_string());
            }
            matches!(condition.args[0].trim(), "User" | "Obj2")
        }
        "IsCreature" => {
            if condition.args.is_empty() {
                return Err("IsCreature expects 1 arg".to_string());
            }
            matches!(condition.args[0].trim(), "User" | "Obj2")
        }
        "IsPlayerThere" => {
            if condition.args.is_empty() {
                return Err("IsPlayerThere expects 1 arg".to_string());
            }
            let position = parse_position_arg(&condition.args[0])?;
            state
                .players
                .values()
                .any(|player| player.position == position)
        }
        "IsObjectInInventory" => {
            if condition.args.len() < 3 {
                return Err("IsObjectInInventory expects 3 args".to_string());
            }
            let target = condition.args[0].trim();
            if target != "User" && target != "Obj2" {
                return Err("IsObjectInInventory expects User or Obj2".to_string());
            }
            let type_id = parse_item_type_id(&condition.args[1])?;
            let required = condition.args[2]
                .trim()
                .parse::<u16>()
                .map_err(|_| "IsObjectInInventory count parse failed".to_string())?;
            let required = if required == 0 { 1 } else { required };
            let Some(player) = state.players.get(&ctx.user_id) else {
                return Ok(false);
            };
            if is_test_god {
                return Ok(true);
            }
            player.inventory.count_type(type_id) >= required
        }
        "CountObjects" => {
            if condition.args.len() < 3 {
                return Err("CountObjects expects 3 args".to_string());
            }
            if condition.args[0].trim() != "Obj1" {
                return Err("CountObjects expects Obj1".to_string());
            }
            let comparator = normalize_comparator(&condition.args[1]);
            let expected = condition.args[2]
                .trim()
                .parse::<i32>()
                .map_err(|_| "CountObjects count parse failed".to_string())?;
            let count = state
                .map
                .tile(ctx.object_position)
                .map(|tile| {
                    tile.items
                        .iter()
                        .filter(|item| item.type_id == ctx.object_type_id)
                        .count() as i32
                })
                .unwrap_or(0);
            compare_value(count, &comparator, expected)?
        }
        "CountObjectsOnMap" => {
            if condition.args.len() < 3 {
                return Err("CountObjectsOnMap expects 3 args".to_string());
            }
            let position = parse_position_arg(&condition.args[0])?;
            let comparator = normalize_comparator(&condition.args[1]);
            let expected = condition.args[2]
                .trim()
                .parse::<i32>()
                .map_err(|_| "CountObjectsOnMap count parse failed".to_string())?;
            let count = state
                .map
                .tile(position)
                .map(|tile| tile.items.len() as i32)
                .unwrap_or(0);
            compare_value(count, &comparator, expected)?
        }
        "HasInstanceAttribute" => {
            if condition.args.len() < 4 {
                return Err("HasInstanceAttribute expects 4 args".to_string());
            }
            let target = condition.args[0].trim();
            let key = condition.args[1].trim();
            let comparator = normalize_comparator(&condition.args[2]);
            let expected = condition.args[3]
                .trim()
                .parse::<i32>()
                .map_err(|_| "HasInstanceAttribute value parse failed".to_string())?;
            let (position, type_id) = match target {
                "Obj1" => (Some(ctx.object_position), Some(ctx.object_type_id)),
                "Obj2" => (ctx.object2_position, ctx.object2_type_id),
                _ => return Ok(false),
            };
            let Some(position) = position else {
                return Ok(false);
            };
            let Some(tile) = state.map.tile(position) else {
                return Ok(false);
            };
            let item = match type_id {
                Some(type_id) => tile.item_details.iter().find(|item| item.type_id == type_id),
                None => tile.item_details.first(),
            };
            let Some(item) = item else {
                return Ok(false);
            };
            let Some(value) = map_item_attribute_value(item, key) else {
                return Ok(false);
            };
            compare_value(value, &comparator, expected)?
        }
        "HasFlag" => {
            if condition.args.len() < 2 {
                return Err("HasFlag expects 2 args".to_string());
            }
            if condition.args[0].trim() != "Obj1" {
                return Err("HasFlag expects Obj1".to_string());
            }
            let flag = condition.args[1].trim();
            state
                .object_types
                .as_ref()
                .and_then(|object_types| object_types.get(ctx.object_type_id))
                .map(|object_type| object_type.has_flag(flag))
                .unwrap_or(false)
        }
        "IsDressed" => {
            if condition.args.is_empty() {
                return Err("IsDressed expects 1 arg".to_string());
            }
            if condition.args[0].trim() != "Obj1" {
                return Err("IsDressed expects Obj1".to_string());
            }
            state
                .object_types
                .as_ref()
                .and_then(|object_types| object_types.get(ctx.object_type_id))
                .map(|object_type| {
                    object_type.has_flag("Clothes")
                        || object_type.has_flag("Armor")
                        || object_type.has_flag("Weapon")
                })
                .unwrap_or(false)
        }
        "IsPeaceful" => {
            if condition.args.is_empty() {
                return Err("IsPeaceful expects 1 arg".to_string());
            }
            let position = match condition.args[0].trim() {
                "User" => Some(ctx.user_position),
                "Obj2" => ctx.object2_position,
                _ => return Err("IsPeaceful expects User or Obj2".to_string()),
            };
            let Some(position) = position else {
                return Ok(false);
            };
            state
                .map
                .tile(position)
                .map(|tile| tile.protection_zone)
                .unwrap_or(false)
        }
        "HasQuestValue" => {
            if condition.args.len() < 4 {
                return Err("HasQuestValue expects 4 args".to_string());
            }
            let target = condition.args[0].trim();
            if target != "User" && target != "Obj2" {
                return Err("HasQuestValue expects User or Obj2".to_string());
            }
            let quest_id = condition.args[1]
                .trim()
                .parse::<u16>()
                .map_err(|_| "HasQuestValue quest id parse failed".to_string())?;
            let comparator = normalize_comparator(&condition.args[2]);
            let expected = condition.args[3]
                .trim()
                .parse::<i32>()
                .map_err(|_| "HasQuestValue value parse failed".to_string())?;
            if is_test_god {
                return Ok(true);
            }
            let value = state
                .players
                .get(&ctx.user_id)
                .and_then(|player| player.quest_values.get(&quest_id).copied())
                .unwrap_or(0);
            compare_value(value, &comparator, expected)?
        }
        "HasLevel" => {
            if condition.args.len() < 3 {
                return Err("HasLevel expects 3 args".to_string());
            }
            let target = condition.args[0].trim();
            if target != "User" && target != "Obj2" {
                return Err("HasLevel expects User or Obj2".to_string());
            }
            let comparator = normalize_comparator(&condition.args[1]);
            let expected = condition.args[2]
                .trim()
                .parse::<i32>()
                .map_err(|_| "HasLevel value parse failed".to_string())?;
            if is_test_god {
                return Ok(true);
            }
            let level = state
                .players
                .get(&ctx.user_id)
                .map(|player| player.level as i32)
                .unwrap_or(0);
            compare_value(level, &comparator, expected)?
        }
        "HasProfession" => {
            if condition.args.len() < 2 {
                return Err("HasProfession expects 2 args".to_string());
            }
            let target = condition.args[0].trim();
            if target != "User" && target != "Obj2" {
                return Err("HasProfession expects User or Obj2".to_string());
            }
            let expected = condition.args[1]
                .trim()
                .parse::<u8>()
                .map_err(|_| "HasProfession value parse failed".to_string())?;
            if is_test_god {
                return Ok(true);
            }
            state
                .players
                .get(&ctx.user_id)
                .map(|player| player.profession == expected)
                .unwrap_or(false)
        }
        "TestSkill" => {
            if condition.args.len() < 4 {
                return Err("TestSkill expects 4 args".to_string());
            }
            let target = condition.args[0].trim();
            if target != "User" && target != "Obj2" {
                return Err("TestSkill expects User or Obj2".to_string());
            }
            let skill = parse_skill_type(&condition.args[1])?;
            let required = condition.args[2]
                .trim()
                .parse::<u16>()
                .map_err(|_| "TestSkill required level parse failed".to_string())?;
            let chance = condition.args[3]
                .trim()
                .parse::<u32>()
                .map_err(|_| "TestSkill chance parse failed".to_string())?;
            if is_test_god {
                return Ok(true);
            }
            let Some(player) = state.players.get(&ctx.user_id) else {
                return Ok(false);
            };
            if player.skills.get(skill).level < required {
                return Ok(false);
            }
            let mut rng = MoveUseRng { state: *rng_state };
            let result = rng.roll_percent(chance);
            *rng_state = rng.state;
            result
        }
        "HasRight" => {
            if condition.args.len() < 2 {
                return Err("HasRight expects 2 args".to_string());
            }
            let target = condition.args[0].trim();
            if target != "User" && target != "Obj2" {
                return Err("HasRight expects User or Obj2".to_string());
            }
            let right = condition.args[1].trim();
            if right.eq_ignore_ascii_case("PREMIUM_ACCOUNT") {
                if is_test_god {
                    return Ok(true);
                }
                return Ok(state
                    .players
                    .get(&ctx.user_id)
                    .map(|player| player.premium)
                    .unwrap_or(false));
            }
            true
        }
        "MayLogout" => {
            let Some(player) = state.players.get(&ctx.user_id) else {
                return Ok(false);
            };
            if let Some(tile) = state.map.tile(player.position) {
                if tile.protection_zone || tile.no_logout {
                    return Ok(false);
                }
            }
            player.pvp.fight_expires_at.is_none()
        }
        other => {
            eprintln!("tibia: moveuse condition unsupported: {}", other);
            false
        }
    };

    Ok(if negated { !result } else { result })
}

fn parse_item_type_id(raw: &str) -> Result<ItemTypeId, String> {
    raw.trim()
        .parse::<u16>()
        .map(ItemTypeId)
        .map_err(|_| format!("invalid item type id '{}'", raw))
}

fn parse_position_arg(raw: &str) -> Result<Position, String> {
    let raw = raw.trim();
    if !raw.starts_with('[') || !raw.ends_with(']') {
        return Err(format!("invalid position '{}'", raw));
    }
    let inner = &raw[1..raw.len() - 1];
    let parts: Vec<&str> = inner.split(',').map(|part| part.trim()).collect();
    if parts.len() != 3 {
        return Err(format!("invalid position '{}'", raw));
    }
    let x = parts[0]
        .parse::<u16>()
        .map_err(|_| format!("invalid position '{}'", raw))?;
    let y = parts[1]
        .parse::<u16>()
        .map_err(|_| format!("invalid position '{}'", raw))?;
    let z = parts[2]
        .parse::<u8>()
        .map_err(|_| format!("invalid position '{}'", raw))?;
    Ok(Position { x, y, z })
}

fn normalize_comparator(raw: &str) -> String {
    raw.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn food_regen_intervals(profession: u8) -> (Duration, Duration) {
    let (hp_secs, mana_secs) = match profession {
        0 | 4 => (6, 6),
        3 => (8, 4),
        1 | 2 => (12, 3),
        14 => (4, 6),
        13 => (6, 3),
        11 | 12 => (12, 2),
        _ => (12, 6),
    };
    (
        Duration::from_secs(hp_secs.max(1)),
        Duration::from_secs(mana_secs.max(1)),
    )
}

fn unix_time_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn parse_access_list(text: &str) -> Vec<String> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for raw_line in text.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        for part in trimmed.split(',') {
            let value = part.trim();
            if value.is_empty() {
                continue;
            }
            let key = value.to_ascii_lowercase();
            if seen.insert(key) {
                entries.push(value.to_string());
            }
        }
    }
    entries
}

fn format_access_list(entries: &[String]) -> String {
    entries
        .iter()
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn compare_value(value: i32, comparator: &str, expected: i32) -> Result<bool, String> {
    match comparator {
        "=" | "==" => Ok(value == expected),
        "<" => Ok(value < expected),
        ">" => Ok(value > expected),
        "<=" => Ok(value <= expected),
        ">=" => Ok(value >= expected),
        "<>" | "!=" => Ok(value != expected),
        _ => Err(format!("invalid comparator '{}'", comparator)),
    }
}

fn parse_skill_type(raw: &str) -> Result<SkillType, String> {
    match raw.trim() {
        "FistFighting" | "Fist" => Ok(SkillType::Fist),
        "ClubFighting" | "Club" => Ok(SkillType::Club),
        "SwordFighting" | "Sword" => Ok(SkillType::Sword),
        "AxeFighting" | "Axe" => Ok(SkillType::Axe),
        "Distance" | "DistanceFighting" => Ok(SkillType::Distance),
        "Shielding" => Ok(SkillType::Shielding),
        "Fishing" => Ok(SkillType::Fishing),
        "Magic" | "MagicLevel" => Ok(SkillType::Magic),
        other => Err(format!("unknown skill '{}'", other)),
    }
}

fn parse_delta_arg(raw: &str) -> Result<crate::world::position::PositionDelta, String> {
    let raw = raw.trim();
    if !raw.starts_with('[') || !raw.ends_with(']') {
        return Err(format!("invalid delta '{}'", raw));
    }
    let inner = &raw[1..raw.len() - 1];
    let parts: Vec<&str> = inner.split(',').map(|part| part.trim()).collect();
    if parts.len() != 3 {
        return Err(format!("invalid delta '{}'", raw));
    }
    let dx = parts[0]
        .parse::<i16>()
        .map_err(|_| format!("invalid delta '{}'", raw))?;
    let dy = parts[1]
        .parse::<i16>()
        .map_err(|_| format!("invalid delta '{}'", raw))?;
    let dz = parts[2]
        .parse::<i8>()
        .map_err(|_| format!("invalid delta '{}'", raw))?;
    Ok(crate::world::position::PositionDelta { dx, dy, dz })
}

fn parse_optional_count(raw: &str) -> Result<u16, String> {
    let value = raw
        .trim()
        .parse::<i32>()
        .map_err(|_| format!("invalid count '{}'", raw))?;
    if value < 0 || value > i32::from(u16::MAX) {
        return Err(format!("count out of range '{}'", raw));
    }
    Ok(value as u16)
}

fn parse_string_arg(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        return Ok(raw[1..raw.len() - 1].to_string());
    }
    Err(format!("invalid string argument '{}'", raw))
}

fn parse_effect_id(raw: &str) -> Result<u16, String> {
    raw.trim()
        .parse::<u16>()
        .map_err(|_| format!("invalid effect id '{}'", raw))
}

fn parse_damage_actor(raw: &str, ctx: &MoveUseContext) -> Result<MoveUseActor, String> {
    match raw.trim() {
        "Null" => Ok(MoveUseActor::None),
        "User" => Ok(MoveUseActor::User(ctx.user_id)),
        "Obj2" => Ok(ctx
            .object2_position
            .map(MoveUseActor::Object)
            .unwrap_or(MoveUseActor::User(ctx.user_id))),
        "Obj1" => Ok(MoveUseActor::Object(ctx.object_position)),
        other => Err(format!("unsupported damage actor '{}'", other)),
    }
}

fn moveuse_has_collision_move_top_rel(
    moveuse: &MoveUseDatabase,
    type_id: ItemTypeId,
) -> bool {
    moveuse
        .sections
        .iter()
        .any(|section| moveuse_section_has_collision_move_top_rel(section, type_id))
}

fn moveuse_section_has_collision_move_top_rel(
    section: &MoveUseSection,
    type_id: ItemTypeId,
) -> bool {
    for rule in &section.rules {
        if rule.event.name != "Collision" {
            continue;
        }
        let matches_type = rule.conditions.iter().any(|condition| {
            if condition.name != "IsType" || condition.args.len() < 2 {
                return false;
            }
            if condition.args[0].trim() != "Obj1" {
                return false;
            }
            parse_item_type_id(&condition.args[1])
                .map(|id| id == type_id)
                .unwrap_or(false)
        });
        if !matches_type {
            continue;
        }
        let has_floor_move = rule.actions.iter().any(|action| {
            if action.name != "MoveTopRel" || action.args.len() < 2 {
                return false;
            }
            let target = action.args[0].trim();
            if target != "Obj1" && target != "Obj2" {
                return false;
            }
            parse_delta_arg(&action.args[1])
                .map(|delta| delta.dz != 0)
                .unwrap_or(false)
        });
        if has_floor_move {
            return true;
        }
    }
    section
        .children
        .iter()
        .any(|child| moveuse_section_has_collision_move_top_rel(child, type_id))
}

fn normalize_stack_count(count: u16) -> u16 {
    if count == 0 { 1 } else { count }
}

fn normalize_depot_contents(items: Vec<ItemStack>) -> Vec<ItemStack> {
    let mut chest: Option<ItemStack> = None;
    let mut extra: Vec<ItemStack> = Vec::new();
    for item in items {
        if item.type_id == DEPOT_CHEST_TYPE_ID {
            if let Some(existing) = chest.as_mut() {
                existing.contents.extend(item.contents);
            } else {
                chest = Some(item);
            }
        } else {
            extra.push(item);
        }
    }

    let mut chest = chest.unwrap_or_else(|| ItemStack { id: crate::entities::item::ItemId::next(),
        type_id: DEPOT_CHEST_TYPE_ID,
        count: 1,
        attributes: Vec::new(),
        contents: Vec::new(),
    });
    if !extra.is_empty() {
        chest.contents.extend(extra);
    }
    vec![chest]
}

fn depot_item_count(items: &[ItemStack]) -> u32 {
    if let Some(chest) = items.iter().find(|item| item.type_id == DEPOT_CHEST_TYPE_ID) {
        count_item_tree(&chest.contents)
    } else {
        count_item_tree(items)
    }
}

fn count_item_tree(items: &[ItemStack]) -> u32 {
    let mut total = 0u32;
    for item in items {
        total = total.saturating_add(1);
        total = total.saturating_add(count_item_tree(&item.contents));
    }
    total
}

fn count_item_with_contents(item: &ItemStack) -> u32 {
    1u32.saturating_add(count_item_tree(&item.contents))
}

fn item_mut_from_items<'a>(
    items: &'a mut Vec<ItemStack>,
    index: usize,
    path: &[usize],
) -> Option<&'a mut ItemStack> {
    if path.is_empty() {
        return items.get_mut(index);
    }
    let (first, rest) = path.split_first()?;
    let item = items.get_mut(index)?;
    item_mut_from_items(&mut item.contents, *first, rest)
}

fn item_from_items<'a>(
    items: &'a [ItemStack],
    index: usize,
    path: &[usize],
) -> Option<&'a ItemStack> {
    if path.is_empty() {
        return items.get(index);
    }
    let (first, rest) = path.split_first()?;
    let item = items.get(index)?;
    item_from_items(&item.contents, *first, rest)
}

fn parent_contents_mut<'a>(
    item: &'a mut ItemStack,
    path: &[usize],
) -> Option<(&'a mut Vec<ItemStack>, usize)> {
    if path.is_empty() {
        return None;
    }
    if path.len() == 1 {
        return Some((&mut item.contents, path[0]));
    }
    let index = path[0];
    let child = item.contents.get_mut(index)?;
    parent_contents_mut(child, &path[1..])
}

fn parent_in_items_mut<'a>(
    items: &'a mut Vec<ItemStack>,
    index: usize,
    path: &[usize],
) -> Option<(&'a mut Vec<ItemStack>, usize)> {
    if path.is_empty() {
        return Some((items, index));
    }
    let item = items.get_mut(index)?;
    parent_contents_mut(item, path)
}

#[allow(dead_code)]
fn path_is_descendant(descendant: &ItemPath, ancestor: &ItemPath) -> bool {
    if descendant.root != ancestor.root {
        return false;
    }
    if descendant.path.len() < ancestor.path.len() {
        return false;
    }
    descendant.path[..ancestor.path.len()] == ancestor.path
}

#[allow(dead_code)]
fn item_type_is_container(item_types: &ItemTypeIndex, type_id: ItemTypeId) -> bool {
    item_types
        .get(type_id)
        .and_then(|entry| entry.container_capacity)
        .is_some()
}

fn item_remaining_expire_secs(item: &ItemStack) -> Option<u16> {
    for attribute in &item.attributes {
        if let ItemAttribute::RemainingExpireTime(value) = attribute {
            return Some(*value);
        }
    }
    None
}

fn item_saved_expire_secs(item: &ItemStack) -> Option<u16> {
    for attribute in &item.attributes {
        if let ItemAttribute::SavedExpireTime(value) = attribute {
            return Some(*value);
        }
    }
    None
}

fn set_remaining_expire_secs(item: &mut ItemStack, value: u32) {
    let value = value.min(u16::MAX as u32) as u16;
    let mut found = false;
    for attribute in item.attributes.iter_mut() {
        if let ItemAttribute::RemainingExpireTime(existing) = attribute {
            *existing = value;
            found = true;
            break;
        }
    }
    if !found {
        item.attributes.push(ItemAttribute::RemainingExpireTime(value));
    }
    item.attributes
        .retain(|attr| !matches!(attr, ItemAttribute::SavedExpireTime(_)));
}

fn set_saved_expire_secs(item: &mut ItemStack, value: u32) {
    let value = value.min(u16::MAX as u32) as u16;
    let mut found = false;
    for attribute in item.attributes.iter_mut() {
        if let ItemAttribute::SavedExpireTime(existing) = attribute {
            *existing = value;
            found = true;
            break;
        }
    }
    if !found {
        item.attributes.push(ItemAttribute::SavedExpireTime(value));
    }
    item.attributes
        .retain(|attr| !matches!(attr, ItemAttribute::RemainingExpireTime(_)));
}


fn remove_inventory_item(
    player: &mut PlayerState,
    type_id: ItemTypeId,
    count: u16,
) -> Result<(), String> {
    if player.is_test_god {
        return Ok(());
    }
    let mut remaining = count;
    for slot in crate::entities::inventory::INVENTORY_SLOTS {
        if remaining == 0 {
            break;
        }
        let Some(item) = player.inventory.slot(slot).cloned() else {
            continue;
        };
        if item.type_id != type_id {
            continue;
        }
        let remove_count = remaining.min(item.count);
        player.inventory.remove_item(slot, remove_count)?;
        remaining = remaining.saturating_sub(remove_count);
    }
    if remaining == 0 {
        Ok(())
    } else {
        Err("DeleteInInventory target missing".to_string())
    }
}

fn map_item_for_stack(item: &ItemStack) -> MapItem {
    MapItem {
        type_id: item.type_id,
        count: item.count,
        attributes: normalize_item_attributes(&item.attributes),
        contents: item.contents.iter().map(map_item_for_stack).collect(),
    }
}

fn stack_from_map_item(item: &MapItem) -> ItemStack {
    ItemStack { id: crate::entities::item::ItemId::next(),
        type_id: item.type_id,
        count: item.count,
        attributes: normalize_item_attributes(&item.attributes),
        contents: item.contents.iter().map(stack_from_map_item).collect(),
    }
}

fn normalize_item_attributes(attributes: &[ItemAttribute]) -> Vec<ItemAttribute> {
    attributes
        .iter()
        .filter(|attribute| {
            !matches!(attribute, ItemAttribute::Amount(_) | ItemAttribute::Charges(_))
        })
        .cloned()
        .collect()
}

fn map_item_for_type_mut(tile: &mut Tile, type_id: ItemTypeId) -> Option<&mut MapItem> {
    let index = tile.items.iter().position(|item| item.type_id == type_id)?;
    ensure_item_details_len(tile);
    tile.item_details.get_mut(index)
}

fn ensure_item_details_len(tile: &mut Tile) {
    while tile.item_details.len() < tile.items.len() {
        let index = tile.item_details.len();
        let item = &tile.items[index];
        tile.item_details.push(map_item_for_stack(item));
    }
    if tile.item_details.len() > tile.items.len() {
        tile.item_details.truncate(tile.items.len());
    }
}

fn record_moveuse_refresh(outcome: &mut MoveUseOutcome, position: Position) {
    if !outcome.refresh_positions.contains(&position) {
        outcome.refresh_positions.push(position);
    }
}

fn moveuse_outcome_has_payload(outcome: &MoveUseOutcome) -> bool {
    outcome.matched_rule.is_some()
        || !outcome.effects.is_empty()
        || !outcome.texts.is_empty()
        || !outcome.edit_texts.is_empty()
        || !outcome.edit_lists.is_empty()
        || !outcome.messages.is_empty()
        || !outcome.damages.is_empty()
        || !outcome.quest_updates.is_empty()
        || !outcome.logout_users.is_empty()
        || !outcome.refresh_positions.is_empty()
        || !outcome.inventory_updates.is_empty()
        || !outcome.container_updates.is_empty()
}

fn truncate_text_to_len(text: &str, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut end = max_len;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn set_item_stack_attribute(
    item: &mut ItemStack,
    key: &str,
    value: &str,
) -> Result<(), String> {
    if key.eq_ignore_ascii_case("String") {
        if let Some(existing) = item.attributes.iter_mut().find_map(|attribute| {
            if let ItemAttribute::String(text) = attribute {
                Some(text)
            } else {
                None
            }
        }) {
            *existing = value.to_string();
        } else {
            item.attributes
                .push(ItemAttribute::String(value.to_string()));
        }
        return Ok(());
    }
    Err(format!("unsupported string attribute '{}'", key))
}

fn set_itemstack_attribute_numeric(item: &mut ItemStack, key: &str, value: i32) -> Result<(), String> {
    match key {
        "ContainerLiquidType" => set_itemstack_attribute_u8(
            item,
            key,
            value,
            ItemAttribute::ContainerLiquidType,
        ),
        "PoolLiquidType" => {
            set_itemstack_attribute_u8(item, key, value, ItemAttribute::PoolLiquidType)
        }
        "ChestQuestNumber" => {
            set_itemstack_attribute_u16(item, key, value, ItemAttribute::ChestQuestNumber)
        }
        "Amount" => set_itemstack_attribute_u16(item, key, value, ItemAttribute::Amount),
        "RemainingExpireTime" => set_itemstack_attribute_u16(
            item,
            key,
            value,
            ItemAttribute::RemainingExpireTime,
        ),
        "KeyholeNumber" => {
            set_itemstack_attribute_u16(item, key, value, ItemAttribute::KeyholeNumber)
        }
        "DoorQuestNumber" => {
            set_itemstack_attribute_u16(item, key, value, ItemAttribute::DoorQuestNumber)
        }
        "DoorQuestValue" => {
            set_itemstack_attribute_u16(item, key, value, ItemAttribute::DoorQuestValue)
        }
        "Level" => set_itemstack_attribute_u16(item, key, value, ItemAttribute::Level),
        "RemainingUses" => {
            set_itemstack_attribute_u16(item, key, value, ItemAttribute::RemainingUses)
        }
        "KeyNumber" => set_itemstack_attribute_u16(item, key, value, ItemAttribute::KeyNumber),
        "SavedExpireTime" => {
            set_itemstack_attribute_u16(item, key, value, ItemAttribute::SavedExpireTime)
        }
        "Charges" => set_itemstack_attribute_u16(item, key, value, ItemAttribute::Charges),
        "AbsTeleportDestination" => {
            set_or_replace_stack_attribute(
                item,
                ItemAttribute::AbsTeleportDestination(value),
                |attribute| matches!(attribute, ItemAttribute::AbsTeleportDestination(_)),
            );
            Ok(())
        }
        "Responsible" => {
            set_itemstack_attribute_u32(item, key, value, ItemAttribute::Responsible)
        }
        other => {
            item.attributes.push(ItemAttribute::Unknown {
                key: other.to_string(),
                value: value.to_string(),
            });
            Ok(())
        }
    }
}

fn change_itemstack_attribute_numeric(item: &mut ItemStack, key: &str, delta: i32) -> Result<(), String> {
    match key {
        "ContainerLiquidType" => {
            change_itemstack_attribute_u8(item, key, delta, ItemAttribute::ContainerLiquidType)
        }
        "PoolLiquidType" => {
            change_itemstack_attribute_u8(item, key, delta, ItemAttribute::PoolLiquidType)
        }
        "ChestQuestNumber" => {
            change_itemstack_attribute_u16(item, key, delta, ItemAttribute::ChestQuestNumber)
        }
        "Amount" => change_itemstack_attribute_u16(item, key, delta, ItemAttribute::Amount),
        "RemainingExpireTime" => change_itemstack_attribute_u16(
            item,
            key,
            delta,
            ItemAttribute::RemainingExpireTime,
        ),
        "KeyholeNumber" => {
            change_itemstack_attribute_u16(item, key, delta, ItemAttribute::KeyholeNumber)
        }
        "DoorQuestNumber" => {
            change_itemstack_attribute_u16(item, key, delta, ItemAttribute::DoorQuestNumber)
        }
        "DoorQuestValue" => {
            change_itemstack_attribute_u16(item, key, delta, ItemAttribute::DoorQuestValue)
        }
        "Level" => change_itemstack_attribute_u16(item, key, delta, ItemAttribute::Level),
        "RemainingUses" => {
            change_itemstack_attribute_u16(item, key, delta, ItemAttribute::RemainingUses)
        }
        "KeyNumber" => change_itemstack_attribute_u16(item, key, delta, ItemAttribute::KeyNumber),
        "SavedExpireTime" => {
            change_itemstack_attribute_u16(item, key, delta, ItemAttribute::SavedExpireTime)
        }
        "Charges" => change_itemstack_attribute_u16(item, key, delta, ItemAttribute::Charges),
        "AbsTeleportDestination" => {
            let current = item.attributes.iter().find_map(|attribute| {
                if let ItemAttribute::AbsTeleportDestination(value) = attribute {
                    Some(*value)
                } else {
                    None
                }
            });
            let current = current.unwrap_or(0);
            let next = current.saturating_add(delta);
            set_or_replace_stack_attribute(
                item,
                ItemAttribute::AbsTeleportDestination(next),
                |attribute| matches!(attribute, ItemAttribute::AbsTeleportDestination(_)),
            );
            Ok(())
        }
        "Responsible" => {
            change_itemstack_attribute_u32(item, key, delta, ItemAttribute::Responsible)
        }
        other => Err(format!("unsupported attribute '{}'", other)),
    }
}

fn set_itemstack_attribute_u8(
    item: &mut ItemStack,
    key: &str,
    value: i32,
    builder: fn(u8) -> ItemAttribute,
) -> Result<(), String> {
    let value = value.clamp(0, i32::from(u8::MAX)) as u8;
    if let Some(existing) = item.attributes.iter_mut().find(|attribute| {
        matches!(
            (key, attribute),
            ("ContainerLiquidType", ItemAttribute::ContainerLiquidType(_))
                | ("PoolLiquidType", ItemAttribute::PoolLiquidType(_))
        )
    }) {
        *existing = builder(value);
        return Ok(());
    }
    item.attributes.push(builder(value));
    Ok(())
}

fn set_itemstack_attribute_u16(
    item: &mut ItemStack,
    key: &str,
    value: i32,
    builder: fn(u16) -> ItemAttribute,
) -> Result<(), String> {
    let value = value.clamp(0, i32::from(u16::MAX)) as u16;
    if let Some(existing) = item.attributes.iter_mut().find(|attribute| {
        matches!(
            (key, attribute),
            ("ChestQuestNumber", ItemAttribute::ChestQuestNumber(_))
                | ("Amount", ItemAttribute::Amount(_))
                | ("RemainingExpireTime", ItemAttribute::RemainingExpireTime(_))
                | ("KeyholeNumber", ItemAttribute::KeyholeNumber(_))
                | ("DoorQuestNumber", ItemAttribute::DoorQuestNumber(_))
                | ("DoorQuestValue", ItemAttribute::DoorQuestValue(_))
                | ("Level", ItemAttribute::Level(_))
                | ("RemainingUses", ItemAttribute::RemainingUses(_))
                | ("KeyNumber", ItemAttribute::KeyNumber(_))
                | ("SavedExpireTime", ItemAttribute::SavedExpireTime(_))
                | ("Charges", ItemAttribute::Charges(_))
        )
    }) {
        *existing = builder(value);
        return Ok(());
    }
    item.attributes.push(builder(value));
    Ok(())
}

fn set_itemstack_attribute_u32(
    item: &mut ItemStack,
    key: &str,
    value: i32,
    builder: fn(u32) -> ItemAttribute,
) -> Result<(), String> {
    let value = value.max(0) as u32;
    if let Some(existing) = item.attributes.iter_mut().find(|attribute| {
        matches!((key, attribute), ("Responsible", ItemAttribute::Responsible(_)))
    }) {
        *existing = builder(value);
        return Ok(());
    }
    item.attributes.push(builder(value));
    Ok(())
}

fn change_itemstack_attribute_u8(
    item: &mut ItemStack,
    key: &str,
    delta: i32,
    builder: fn(u8) -> ItemAttribute,
) -> Result<(), String> {
    let current = item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("ContainerLiquidType", ItemAttribute::ContainerLiquidType(value)) => Some(*value),
        ("PoolLiquidType", ItemAttribute::PoolLiquidType(value)) => Some(*value),
        _ => None,
    });
    let current = i32::from(current.unwrap_or(0));
    let next = current.saturating_add(delta).clamp(0, i32::from(u8::MAX)) as u8;
    set_itemstack_attribute_u8(item, key, next as i32, builder)
}

fn change_itemstack_attribute_u16(
    item: &mut ItemStack,
    key: &str,
    delta: i32,
    builder: fn(u16) -> ItemAttribute,
) -> Result<(), String> {
    let current = item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("ChestQuestNumber", ItemAttribute::ChestQuestNumber(value)) => Some(*value),
        ("Amount", ItemAttribute::Amount(value)) => Some(*value),
        ("RemainingExpireTime", ItemAttribute::RemainingExpireTime(value)) => Some(*value),
        ("KeyholeNumber", ItemAttribute::KeyholeNumber(value)) => Some(*value),
        ("DoorQuestNumber", ItemAttribute::DoorQuestNumber(value)) => Some(*value),
        ("DoorQuestValue", ItemAttribute::DoorQuestValue(value)) => Some(*value),
        ("Level", ItemAttribute::Level(value)) => Some(*value),
        ("RemainingUses", ItemAttribute::RemainingUses(value)) => Some(*value),
        ("KeyNumber", ItemAttribute::KeyNumber(value)) => Some(*value),
        ("SavedExpireTime", ItemAttribute::SavedExpireTime(value)) => Some(*value),
        ("Charges", ItemAttribute::Charges(value)) => Some(*value),
        _ => None,
    });
    let current = i32::from(current.unwrap_or(0));
    let next = current.saturating_add(delta).clamp(0, i32::from(u16::MAX)) as u16;
    set_itemstack_attribute_u16(item, key, next as i32, builder)
}

fn change_itemstack_attribute_u32(
    item: &mut ItemStack,
    key: &str,
    delta: i32,
    builder: fn(u32) -> ItemAttribute,
) -> Result<(), String> {
    let current = item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("Responsible", ItemAttribute::Responsible(value)) => Some(*value),
        _ => None,
    });
    let current = current.unwrap_or(0);
    let next = (i64::from(current) + i64::from(delta)).max(0) as u32;
    set_itemstack_attribute_u32(item, key, next as i32, builder)
}
fn set_or_replace_stack_attribute(
    item: &mut ItemStack,
    attribute: ItemAttribute,
    matches: impl Fn(&ItemAttribute) -> bool,
) {
    if let Some(existing) = item.attributes.iter_mut().find(|entry| matches(entry)) {
        *existing = attribute;
    } else {
        item.attributes.push(attribute);
    }
}

fn set_map_item_attribute(item: &mut MapItem, key: &str, value: &str) -> Result<(), String> {
    if key.eq_ignore_ascii_case("String") {
        if let Some(existing) = item.attributes.iter_mut().find_map(|attribute| {
            if let ItemAttribute::String(text) = attribute {
                Some(text)
            } else {
                None
            }
        }) {
            *existing = value.to_string();
        } else {
            item.attributes
                .push(ItemAttribute::String(value.to_string()));
        }
        return Ok(());
    }
    Err(format!("unsupported string attribute '{}'", key))
}

fn map_item_attribute_value(item: &MapItem, key: &str) -> Option<i32> {
    item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("ContainerLiquidType", ItemAttribute::ContainerLiquidType(value)) => Some(i32::from(*value)),
        ("PoolLiquidType", ItemAttribute::PoolLiquidType(value)) => Some(i32::from(*value)),
        ("ChestQuestNumber", ItemAttribute::ChestQuestNumber(value)) => Some(i32::from(*value)),
        ("Amount", ItemAttribute::Amount(value)) => Some(i32::from(*value)),
        ("RemainingExpireTime", ItemAttribute::RemainingExpireTime(value)) => Some(i32::from(*value)),
        ("KeyholeNumber", ItemAttribute::KeyholeNumber(value)) => Some(i32::from(*value)),
        ("DoorQuestNumber", ItemAttribute::DoorQuestNumber(value)) => Some(i32::from(*value)),
        ("DoorQuestValue", ItemAttribute::DoorQuestValue(value)) => Some(i32::from(*value)),
        ("Level", ItemAttribute::Level(value)) => Some(i32::from(*value)),
        ("RemainingUses", ItemAttribute::RemainingUses(value)) => Some(i32::from(*value)),
        ("KeyNumber", ItemAttribute::KeyNumber(value)) => Some(i32::from(*value)),
        ("SavedExpireTime", ItemAttribute::SavedExpireTime(value)) => Some(i32::from(*value)),
        ("Charges", ItemAttribute::Charges(value)) => Some(i32::from(*value)),
        ("AbsTeleportDestination", ItemAttribute::AbsTeleportDestination(value)) => Some(*value),
        ("Responsible", ItemAttribute::Responsible(value)) => Some((*value).min(i32::MAX as u32) as i32),
        _ => None,
    })
}

fn set_map_item_attribute_numeric(item: &mut MapItem, key: &str, value: i32) -> Result<(), String> {
    match key {
        "ContainerLiquidType" => set_map_item_attribute_u8(
            item,
            key,
            value,
            ItemAttribute::ContainerLiquidType,
        ),
        "PoolLiquidType" => {
            set_map_item_attribute_u8(item, key, value, ItemAttribute::PoolLiquidType)
        }
        "ChestQuestNumber" => {
            set_map_item_attribute_u16(item, key, value, ItemAttribute::ChestQuestNumber)
        }
        "Amount" => set_map_item_attribute_u16(item, key, value, ItemAttribute::Amount),
        "RemainingExpireTime" => set_map_item_attribute_u16(
            item,
            key,
            value,
            ItemAttribute::RemainingExpireTime,
        ),
        "KeyholeNumber" => {
            set_map_item_attribute_u16(item, key, value, ItemAttribute::KeyholeNumber)
        }
        "DoorQuestNumber" => {
            set_map_item_attribute_u16(item, key, value, ItemAttribute::DoorQuestNumber)
        }
        "DoorQuestValue" => {
            set_map_item_attribute_u16(item, key, value, ItemAttribute::DoorQuestValue)
        }
        "Level" => set_map_item_attribute_u16(item, key, value, ItemAttribute::Level),
        "RemainingUses" => {
            set_map_item_attribute_u16(item, key, value, ItemAttribute::RemainingUses)
        }
        "KeyNumber" => set_map_item_attribute_u16(item, key, value, ItemAttribute::KeyNumber),
        "SavedExpireTime" => {
            set_map_item_attribute_u16(item, key, value, ItemAttribute::SavedExpireTime)
        }
        "Charges" => set_map_item_attribute_u16(item, key, value, ItemAttribute::Charges),
        "AbsTeleportDestination" => {
            set_or_replace_attribute(
                item,
                ItemAttribute::AbsTeleportDestination(value),
                |attribute| matches!(attribute, ItemAttribute::AbsTeleportDestination(_)),
            );
            Ok(())
        }
        "Responsible" => set_map_item_attribute_u32(item, key, value, ItemAttribute::Responsible),
        other => {
            item.attributes.push(ItemAttribute::Unknown {
                key: other.to_string(),
                value: value.to_string(),
            });
            Ok(())
        }
    }
}

fn change_map_item_attribute_numeric(
    item: &mut MapItem,
    key: &str,
    delta: i32,
) -> Result<(), String> {
    match key {
        "ContainerLiquidType" => {
            change_map_item_attribute_u8(item, key, delta, ItemAttribute::ContainerLiquidType)
        }
        "PoolLiquidType" => {
            change_map_item_attribute_u8(item, key, delta, ItemAttribute::PoolLiquidType)
        }
        "ChestQuestNumber" => {
            change_map_item_attribute_u16(item, key, delta, ItemAttribute::ChestQuestNumber)
        }
        "Amount" => change_map_item_attribute_u16(item, key, delta, ItemAttribute::Amount),
        "RemainingExpireTime" => change_map_item_attribute_u16(
            item,
            key,
            delta,
            ItemAttribute::RemainingExpireTime,
        ),
        "KeyholeNumber" => {
            change_map_item_attribute_u16(item, key, delta, ItemAttribute::KeyholeNumber)
        }
        "DoorQuestNumber" => {
            change_map_item_attribute_u16(item, key, delta, ItemAttribute::DoorQuestNumber)
        }
        "DoorQuestValue" => {
            change_map_item_attribute_u16(item, key, delta, ItemAttribute::DoorQuestValue)
        }
        "Level" => change_map_item_attribute_u16(item, key, delta, ItemAttribute::Level),
        "RemainingUses" => {
            change_map_item_attribute_u16(item, key, delta, ItemAttribute::RemainingUses)
        }
        "KeyNumber" => change_map_item_attribute_u16(item, key, delta, ItemAttribute::KeyNumber),
        "SavedExpireTime" => {
            change_map_item_attribute_u16(item, key, delta, ItemAttribute::SavedExpireTime)
        }
        "Charges" => change_map_item_attribute_u16(item, key, delta, ItemAttribute::Charges),
        "AbsTeleportDestination" => {
            let current = item.attributes.iter().find_map(|attribute| {
                if let ItemAttribute::AbsTeleportDestination(value) = attribute {
                    Some(*value)
                } else {
                    None
                }
            });
            let next = current.unwrap_or(0).saturating_add(delta);
            set_or_replace_attribute(
                item,
                ItemAttribute::AbsTeleportDestination(next),
                |attribute| matches!(attribute, ItemAttribute::AbsTeleportDestination(_)),
            );
            Ok(())
        }
        "Responsible" => {
            change_map_item_attribute_u32(item, key, delta, ItemAttribute::Responsible)
        }
        other => {
            item.attributes.push(ItemAttribute::Unknown {
                key: other.to_string(),
                value: delta.to_string(),
            });
            Ok(())
        }
    }
}

fn set_map_item_attribute_u8(
    item: &mut MapItem,
    key: &str,
    value: i32,
    builder: fn(u8) -> ItemAttribute,
) -> Result<(), String> {
    let value = value.clamp(0, i32::from(u8::MAX)) as u8;
    set_or_replace_attribute(item, builder(value), |attribute| match (key, attribute) {
        ("ContainerLiquidType", ItemAttribute::ContainerLiquidType(_)) => true,
        ("PoolLiquidType", ItemAttribute::PoolLiquidType(_)) => true,
        _ => false,
    });
    Ok(())
}

fn set_map_item_attribute_u16(
    item: &mut MapItem,
    key: &str,
    value: i32,
    builder: fn(u16) -> ItemAttribute,
) -> Result<(), String> {
    let value = value.clamp(0, i32::from(u16::MAX)) as u16;
    set_or_replace_attribute(item, builder(value), |attribute| match (key, attribute) {
        ("ChestQuestNumber", ItemAttribute::ChestQuestNumber(_)) => true,
        ("Amount", ItemAttribute::Amount(_)) => true,
        ("RemainingExpireTime", ItemAttribute::RemainingExpireTime(_)) => true,
        ("KeyholeNumber", ItemAttribute::KeyholeNumber(_)) => true,
        ("DoorQuestNumber", ItemAttribute::DoorQuestNumber(_)) => true,
        ("DoorQuestValue", ItemAttribute::DoorQuestValue(_)) => true,
        ("Level", ItemAttribute::Level(_)) => true,
        ("RemainingUses", ItemAttribute::RemainingUses(_)) => true,
        ("KeyNumber", ItemAttribute::KeyNumber(_)) => true,
        ("SavedExpireTime", ItemAttribute::SavedExpireTime(_)) => true,
        ("Charges", ItemAttribute::Charges(_)) => true,
        _ => false,
    });
    Ok(())
}

fn set_map_item_attribute_u32(
    item: &mut MapItem,
    key: &str,
    value: i32,
    builder: fn(u32) -> ItemAttribute,
) -> Result<(), String> {
    let value = value.max(0) as u32;
    set_or_replace_attribute(item, builder(value), |attribute| match (key, attribute) {
        ("Responsible", ItemAttribute::Responsible(_)) => true,
        _ => false,
    });
    Ok(())
}

fn change_map_item_attribute_u8(
    item: &mut MapItem,
    key: &str,
    delta: i32,
    builder: fn(u8) -> ItemAttribute,
) -> Result<(), String> {
    let current = item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("ContainerLiquidType", ItemAttribute::ContainerLiquidType(value)) => Some(*value),
        ("PoolLiquidType", ItemAttribute::PoolLiquidType(value)) => Some(*value),
        _ => None,
    });
    let next = i32::from(current.unwrap_or(0)).saturating_add(delta);
    set_map_item_attribute_u8(item, key, next, builder)
}

fn change_map_item_attribute_u16(
    item: &mut MapItem,
    key: &str,
    delta: i32,
    builder: fn(u16) -> ItemAttribute,
) -> Result<(), String> {
    let current = item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("ChestQuestNumber", ItemAttribute::ChestQuestNumber(value)) => Some(*value),
        ("Amount", ItemAttribute::Amount(value)) => Some(*value),
        ("RemainingExpireTime", ItemAttribute::RemainingExpireTime(value)) => Some(*value),
        ("KeyholeNumber", ItemAttribute::KeyholeNumber(value)) => Some(*value),
        ("DoorQuestNumber", ItemAttribute::DoorQuestNumber(value)) => Some(*value),
        ("DoorQuestValue", ItemAttribute::DoorQuestValue(value)) => Some(*value),
        ("Level", ItemAttribute::Level(value)) => Some(*value),
        ("RemainingUses", ItemAttribute::RemainingUses(value)) => Some(*value),
        ("KeyNumber", ItemAttribute::KeyNumber(value)) => Some(*value),
        ("SavedExpireTime", ItemAttribute::SavedExpireTime(value)) => Some(*value),
        ("Charges", ItemAttribute::Charges(value)) => Some(*value),
        _ => None,
    });
    let next = i32::from(current.unwrap_or(0)).saturating_add(delta);
    set_map_item_attribute_u16(item, key, next, builder)
}

fn change_map_item_attribute_u32(
    item: &mut MapItem,
    key: &str,
    delta: i32,
    builder: fn(u32) -> ItemAttribute,
) -> Result<(), String> {
    let current = item.attributes.iter().find_map(|attribute| match (key, attribute) {
        ("Responsible", ItemAttribute::Responsible(value)) => Some(*value),
        _ => None,
    });
    let current = current.unwrap_or(0);
    let next = (i64::from(current) + i64::from(delta)).max(0) as u32;
    set_map_item_attribute_u32(item, key, next as i32, builder)
}
fn set_or_replace_attribute(
    item: &mut MapItem,
    attribute: ItemAttribute,
    matches: impl Fn(&ItemAttribute) -> bool,
) {
    if let Some(existing) = item.attributes.iter_mut().find(|entry| matches(entry)) {
        *existing = attribute;
    } else {
        item.attributes.push(attribute);
    }
}

fn remove_item_from_tile(
    tile: &mut Tile,
    type_id: ItemTypeId,
    prefer_top: bool,
) -> Option<ItemStack> {
    ensure_item_details_len(tile);
    let index = if prefer_top {
        tile.items
            .iter()
            .rposition(|item| item.type_id == type_id)
    } else {
        tile.items
            .iter()
            .position(|item| item.type_id == type_id)
    };
    index.and_then(|idx| take_item_from_tile_at(tile, idx))
}

fn remove_item_at_index(tile: &mut Tile, index: usize) -> Option<ItemStack> {
    take_item_from_tile_at(tile, index)
}

fn insert_item_at_index(tile: &mut Tile, index: usize, item: ItemStack) {
    ensure_item_details_len(tile);
    let index = index.min(tile.items.len());
    tile.items.insert(index, item);
    if let Some(added) = tile.items.get(index) {
        tile.item_details.insert(index, map_item_for_stack(added));
    }
}

fn take_item_from_tile_at(tile: &mut Tile, index: usize) -> Option<ItemStack> {
    if index >= tile.items.len() {
        return None;
    }
    ensure_item_details_len(tile);
    let mut item = tile.items.remove(index);
    if index < tile.item_details.len() {
        let detail = tile.item_details.remove(index);
        item.attributes = normalize_item_attributes(&detail.attributes);
        item.contents = detail.contents.iter().map(stack_from_map_item).collect();
    }
    Some(item)
}

fn take_from_tile_at(
    tile: &mut Tile,
    index: usize,
    count: u16,
    stackable: bool,
) -> Result<ItemStack, String> {
    if count == 0 {
        return Err("cannot take zero-count item".to_string());
    }
    ensure_item_details_len(tile);
    if index >= tile.items.len() {
        return Err("tile item index out of range".to_string());
    }
    let item = &mut tile.items[index];
    if stackable {
        if count > item.count {
            return Err("tile has insufficient count".to_string());
        }
        if count == item.count {
            return take_item_from_tile_at(tile, index)
                .ok_or_else(|| "item not found on tile".to_string());
        }
        let attributes = tile
            .item_details
            .get(index)
            .map(|detail| normalize_item_attributes(&detail.attributes))
            .unwrap_or_default();
        item.count -= count;
        if let Some(detail) = tile.item_details.get_mut(index) {
            detail.count = item.count;
        }
        return Ok(ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: item.type_id,
            count,
            attributes,
            contents: Vec::new(),
        });
    }
    if count != item.count {
        return Err("cannot split non-stackable item".to_string());
    }
    take_item_from_tile_at(tile, index).ok_or_else(|| "item not found on tile".to_string())
}

fn take_from_tile(
    tile: &mut Tile,
    type_id: ItemTypeId,
    count: u16,
    stackable: bool,
) -> Result<ItemStack, String> {
    if count == 0 {
        return Err("cannot take zero-count item".to_string());
    }
    ensure_item_details_len(tile);
    let Some(index) = tile
        .items
        .iter()
        .position(|item| item.type_id == type_id)
    else {
        return Err("item not found on tile".to_string());
    };
    let item = &mut tile.items[index];
    if stackable {
        if count > item.count {
            return Err("tile has insufficient count".to_string());
        }
        if count == item.count {
            return take_item_from_tile_at(tile, index)
                .ok_or_else(|| "item not found on tile".to_string());
        }
        let attributes = tile
            .item_details
            .get(index)
            .map(|detail| normalize_item_attributes(&detail.attributes))
            .unwrap_or_default();
        item.count -= count;
        if let Some(detail) = tile.item_details.get_mut(index) {
            detail.count = item.count;
        }
        return Ok(ItemStack { id: crate::entities::item::ItemId::next(),
            type_id,
            count,
            attributes,
            contents: Vec::new(),
});
    }
    if count != item.count {
        return Err("cannot split non-stackable item".to_string());
    }
    take_item_from_tile_at(tile, index).ok_or_else(|| "item not found on tile".to_string())
}

fn take_from_container(
    container: &mut OpenContainer,
    container_id: u8,
    slot: u8,
    count: u16,
    stackable: bool,
    expected_type: ItemTypeId,
) -> Result<(ItemStack, ContainerUpdate), String> {
    if count == 0 {
        return Err("cannot take zero-count item".to_string());
    }
    let index = slot as usize;
    if index >= container.items.len() {
        return Err("container slot out of range".to_string());
    }
    let item = &mut container.items[index];
    if item.type_id != expected_type {
        return Err("container item type mismatch".to_string());
    }
    if stackable {
        if count > item.count {
            return Err("container has insufficient count".to_string());
        }
        if count == item.count {
            let removed = container.items.remove(index);
            return Ok((
                removed.clone(),
                ContainerUpdate::Remove {
                    container_id,
                    slot,
                },
            ));
        }
        item.count -= count;
        let removed = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: item.type_id,
            count,
            attributes: item.attributes.clone(),
            contents: Vec::new(),
        };
        return Ok((
            removed,
            ContainerUpdate::Update {
                container_id,
                slot,
                item: item.clone(),
            },
        ));
    }
    if count != item.count {
        return Err("cannot split non-stackable item".to_string());
    }
    let removed = container.items.remove(index);
    Ok((
        removed.clone(),
        ContainerUpdate::Remove {
            container_id,
            slot,
        },
    ))
}

fn insert_into_container(
    container: &mut OpenContainer,
    container_id: u8,
    slot: u8,
    item: ItemStack,
    stackable: bool,
) -> Result<ContainerUpdate, String> {
    const CONTAINER_APPEND_SLOT: u8 = 0xff;
    if item.count == 0 {
        return Err("cannot insert zero-count item".to_string());
    }
    if slot == CONTAINER_APPEND_SLOT {
        if stackable {
            if let Some((index, existing)) = container.items.iter_mut().enumerate().find(
                |(_, existing)| {
                    existing.type_id == item.type_id && existing.attributes == item.attributes
                },
            ) {
                let total = existing.count as u32 + item.count as u32;
                if total > u16::MAX as u32 {
                    return Err("container stack overflow".to_string());
                }
                existing.count = total as u16;
                let slot = u8::try_from(index).unwrap_or(u8::MAX);
                return Ok(ContainerUpdate::Update {
                    container_id,
                    slot,
                    item: existing.clone(),
                });
            }
        }
    }
    let index = if slot == CONTAINER_APPEND_SLOT {
        container.items.len()
    } else {
        slot as usize
    };
    if index < container.items.len() {
        let existing = &mut container.items[index];
        if stackable && existing.type_id == item.type_id && existing.attributes == item.attributes {
            let total = existing.count as u32 + item.count as u32;
            if total > u16::MAX as u32 {
                return Err("container stack overflow".to_string());
            }
            existing.count = total as u16;
            return Ok(ContainerUpdate::Update {
                container_id,
                slot,
                item: existing.clone(),
            });
        }
        return Err("container slot occupied".to_string());
    }
    if index > container.items.len() {
        return Err("container slot out of range".to_string());
    }
    if container.capacity > 0 && container.items.len() >= container.capacity as usize {
        return Err("container full".to_string());
    }
    container.items.push(item.clone());
    Ok(ContainerUpdate::Add { container_id, item })
}

fn move_item_within_container(
    container: &mut OpenContainer,
    from_slot: u8,
    to_slot: u8,
    count: u16,
    stackable: bool,
    expected_type: ItemTypeId,
) -> Result<(), String> {
    const CONTAINER_APPEND_SLOT: u8 = 0xff;
    if count == 0 {
        return Err("cannot take zero-count item".to_string());
    }
    let len = container.items.len();
    let from_index = from_slot as usize;
    if from_index >= len {
        return Err("container slot out of range".to_string());
    }
    let from_item = container.items[from_index].clone();
    if from_item.type_id != expected_type {
        return Err("container item type mismatch".to_string());
    }
    if stackable {
        if count > from_item.count {
            return Err("container has insufficient count".to_string());
        }
    } else if count != from_item.count {
        return Err("cannot split non-stackable item".to_string());
    }

    let mut dest_index = if to_slot == CONTAINER_APPEND_SLOT {
        len
    } else {
        to_slot as usize
    };
    if dest_index > len {
        dest_index = len;
    }
    if dest_index == from_index && to_slot != CONTAINER_APPEND_SLOT {
        return Ok(());
    }

    if dest_index < len {
        if dest_index == from_index {
            return Ok(());
        }
        if stackable {
            let dest_item = container.items[dest_index].clone();
            if dest_item.type_id == from_item.type_id
                && dest_item.attributes == from_item.attributes
            {
                let total = dest_item.count as u32 + count as u32;
                if total > u16::MAX as u32 {
                    return Err("container stack overflow".to_string());
                }
                if count == from_item.count {
                    if from_index < dest_index {
                        container.items.remove(from_index);
                        let dest = &mut container.items[dest_index - 1];
                        dest.count = total as u16;
                    } else {
                        container.items.remove(from_index);
                        let dest = &mut container.items[dest_index];
                        dest.count = total as u16;
                    }
                } else {
                    container.items[from_index].count = from_item.count - count;
                    container.items[dest_index].count = total as u16;
                }
                return Ok(());
            }
            if count != from_item.count {
                return Err("cannot split stack onto occupied slot".to_string());
            }
        }
        container.items.swap(from_index, dest_index);
        return Ok(());
    }

    if stackable {
        if count == from_item.count {
            if from_index + 1 == len {
                return Ok(());
            }
            let item = container.items.remove(from_index);
            container.items.push(item);
        } else {
            if container.capacity > 0 && container.items.len() >= container.capacity as usize {
                return Err("container full".to_string());
            }
            container.items[from_index].count = from_item.count - count;
            container.items.push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: from_item.type_id,
                count,
                attributes: from_item.attributes.clone(),
                contents: Vec::new(),
            });
        }
        return Ok(());
    }

    if from_index + 1 == len {
        return Ok(());
    }
    let item = container.items.remove(from_index);
    container.items.push(item);
    Ok(())
}

fn diff_container_updates(
    container_id: u8,
    before: &[ItemStack],
    after: &[ItemStack],
) -> Vec<ContainerUpdate> {
    let mut updates = Vec::new();
    let min_len = before.len().min(after.len());
    for index in 0..min_len {
        if before[index] != after[index] {
            let slot = u8::try_from(index).unwrap_or(u8::MAX);
            updates.push(ContainerUpdate::Update {
                container_id,
                slot,
                item: after[index].clone(),
            });
        }
    }
    if after.len() > before.len() {
        for item in after.iter().skip(before.len()) {
            updates.push(ContainerUpdate::Add {
                container_id,
                item: item.clone(),
            });
        }
    }
    if after.len() < before.len() {
        for index in (after.len()..before.len()).rev() {
            let slot = u8::try_from(index).unwrap_or(u8::MAX);
            updates.push(ContainerUpdate::Remove { container_id, slot });
        }
    }
    updates
}

fn insert_into_inventory_container_items(
    items: &mut Vec<ItemStack>,
    capacity: u8,
    item: ItemStack,
    stackable: bool,
) -> Result<(), String> {
    if item.count == 0 {
        return Err("cannot insert zero-count item".to_string());
    }
    if stackable {
        if let Some(existing) = items
            .iter_mut()
            .find(|entry| entry.type_id == item.type_id && entry.attributes == item.attributes)
        {
            let total = existing.count as u32 + item.count as u32;
            if total > u16::MAX as u32 {
                return Err("container stack overflow".to_string());
            }
            existing.count = total as u16;
            return Ok(());
        }
    }
    if capacity > 0 && items.len() >= capacity as usize {
        return Err("container full".to_string());
    }
    items.push(item);
    Ok(())
}

fn restore_container_item(
    container: &mut OpenContainer,
    slot: u8,
    item: ItemStack,
    stackable: bool,
) -> Result<(), String> {
    if item.count == 0 {
        return Err("cannot restore zero-count item".to_string());
    }
    let index = slot as usize;
    if index < container.items.len() {
        let existing = &mut container.items[index];
        if stackable && existing.type_id == item.type_id && existing.attributes == item.attributes {
            let total = existing.count as u32 + item.count as u32;
            if total > u16::MAX as u32 {
                return Err("container stack overflow".to_string());
            }
            existing.count = total as u16;
            return Ok(());
        }
    }
    if container.capacity > 0 && container.items.len() >= container.capacity as usize {
        return Err("container full".to_string());
    }
    if index > container.items.len() {
        container.items.push(item);
    } else {
        container.items.insert(index, item);
    }
    Ok(())
}

fn place_on_tile(
    tile: &mut Tile,
    item: ItemStack,
    stackable: bool,
) -> Result<(), String> {
    if item.count == 0 {
        return Err("cannot place zero-count item".to_string());
    }
    ensure_item_details_len(tile);
    if stackable {
        if let Some((index, existing)) = tile.items.iter_mut().enumerate().find(
            |(_, existing)| {
                existing.type_id == item.type_id && existing.attributes == item.attributes
            },
        ) {
            let total = existing.count as u32 + item.count as u32;
            if total > u16::MAX as u32 {
                return Err("tile stack overflow".to_string());
            }
            existing.count = total as u16;
            if let Some(detail) = tile.item_details.get_mut(index) {
                detail.count = existing.count;
            }
            return Ok(());
        }
    }
    tile.items.push(item);
    if let Some(added) = tile.items.last() {
        tile.item_details.push(map_item_for_stack(added));
    }
    Ok(())
}

fn place_on_tile_with_dustbin(
    tile: &mut Tile,
    item: ItemStack,
    stackable: bool,
    movable: bool,
) -> Result<(), String> {
    if WorldState::should_delete_on_dustbin(tile, movable) {
        return Ok(());
    }
    place_on_tile(tile, item, stackable)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NpcPlannedAction {
    SetTopic(i64),
    SetNpcVar { key: String, value: i64 },
    SetQuestValue(u16, i32),
    TeachSpell(SpellId),
    SetProfession(u8),
    SetHealth(u32),
    CreateItem { type_id: ItemTypeId, count: u16 },
    DeleteItem { type_id: ItemTypeId, count: u16 },
    CreateMoney(u32),
    DeleteMoney(u32),
    Teleport(Position),
    EffectOpp(u16),
    EffectMe(u16),
    QueuePlayer,
    FocusPlayer { expires_at: Option<GameTick> },
    ClearFocus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MoneyType {
    type_id: ItemTypeId,
    value: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NpcActionPlan {
    npc_id: CreatureId,
    actions: Vec<NpcPlannedAction>,
}

impl NpcActionPlan {
    fn new(npc_id: CreatureId) -> Self {
        Self {
            npc_id,
            actions: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

struct NpcEvalContext<'a> {
    player: &'a PlayerState,
    npc_id: CreatureId,
    tokens: &'a [String],
    message: &'a str,
    rng: &'a mut MoveUseRng,
    object_types: Option<&'a ObjectTypeIndex>,
    spellbook: &'a SpellBook,
    clock: Option<&'a GameClock>,
    focus_owner: Option<PlayerId>,
    is_busy: bool,
    is_queued: bool,
    required_ident: Option<&'static str>,
}

fn npc_reply_for_message(
    script: &NpcScript,
    ctx: &mut NpcEvalContext<'_>,
) -> Option<(Option<String>, NpcActionPlan)> {
    npc_reply_pass(script, ctx, true).or_else(|| npc_reply_pass(script, ctx, false))
}

fn npc_reply_pass(
    script: &NpcScript,
    ctx: &mut NpcEvalContext<'_>,
    require_string: bool,
) -> Option<(Option<String>, NpcActionPlan)> {
    let mut last_reply_template: Option<String> = None;
    for rule in &script.behaviour {
        if let Some(template) = npc_rule_first_reply(rule) {
            last_reply_template = Some(template);
        }
        if require_string && !npc_rule_has_string(rule) {
            continue;
        }
        if let Some(required) = ctx.required_ident {
            if !npc_rule_has_ident(rule, required) {
                continue;
            }
        }
        let vars = npc_rule_assignments(rule, ctx);
        if !npc_rule_matches(rule, ctx, &vars) {
            continue;
        }
        let reply = npc_rule_reply(rule, ctx, &vars)
            .or_else(|| npc_rule_star_reply(rule, ctx, &vars, last_reply_template.as_deref()));
        let plan = npc_rule_action_plan(rule, ctx, &vars);
        return Some((reply, plan));
    }
    None
}

fn npc_rule_matches(
    rule: &NpcBehaviourRule,
    ctx: &mut NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> bool {
    if !npc_string_conditions_match(rule, ctx.message, ctx.tokens) {
        return false;
    }
    let mut negate_next = false;
    for condition in &rule.parsed_conditions {
        match condition {
            NpcCondition::String(_) => continue,
            NpcCondition::Negation => {
                negate_next = !negate_next;
                continue;
            }
            other => {
                let mut matched = npc_condition_matches(other, ctx, vars);
                if negate_next {
                    matched = !matched;
                    negate_next = false;
                }
                if !matched {
                    return false;
                }
            }
        }
    }
    true
}

fn npc_trade_entry_allowed(
    entry: &NpcTradeEntry,
    ctx: &mut NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> bool {
    let mut negate_next = false;
    for condition in &entry.parsed_conditions {
        match condition {
            NpcCondition::String(_) => continue,
            NpcCondition::Negation => {
                negate_next = !negate_next;
                continue;
            }
            other => {
                let mut matched = npc_condition_matches(other, ctx, vars);
                if negate_next {
                    matched = !matched;
                    negate_next = false;
                }
                if !matched {
                    return false;
                }
            }
        }
    }
    true
}

fn npc_rule_has_string(rule: &NpcBehaviourRule) -> bool {
    rule.parsed_conditions
        .iter()
        .any(|condition| matches!(condition, NpcCondition::String(_)))
}

fn npc_rule_has_ident(rule: &NpcBehaviourRule, ident: &str) -> bool {
    rule.parsed_conditions.iter().any(|condition| {
        let NpcCondition::Ident(value) = condition else {
            return false;
        };
        value.eq_ignore_ascii_case(ident)
    })
}

fn npc_rule_first_reply(rule: &NpcBehaviourRule) -> Option<String> {
    for action in &rule.parsed_actions {
        let NpcAction::Say(text) = action else {
            continue;
        };
        return Some(text.clone());
    }
    None
}

fn npc_rule_star_reply(
    rule: &NpcBehaviourRule,
    ctx: &NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
    fallback: Option<&str>,
) -> Option<String> {
    let mut has_star = false;
    for action in &rule.parsed_actions {
        let NpcAction::Ident(value) = action else {
            continue;
        };
        if value.trim() == "*" {
            has_star = true;
            break;
        }
    }
    if !has_star {
        return None;
    }
    fallback.map(|template| npc_format_reply(template, ctx, vars))
}

fn npc_condition_matches(
    condition: &NpcCondition,
    ctx: &mut NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> bool {
    match condition {
        NpcCondition::Comparison { left, op, right } => {
            let left = npc_eval_expr(left, ctx, vars);
            let right = npc_eval_expr(right, ctx, vars);
            match (left, right) {
                (Some(left), Some(right)) => npc_compare(op, left, right),
                _ => false,
            }
        }
        NpcCondition::Call { name, args } => {
            npc_eval_call(name, args, ctx, vars)
                .map(|value| value != 0)
                .unwrap_or(false)
        }
        NpcCondition::Ident(value) => npc_ident_matches(value, ctx, vars),
        NpcCondition::Number(value) => *value != 0,
        NpcCondition::Raw(_) => true,
        NpcCondition::Negation | NpcCondition::String(_) => true,
    }
}

fn npc_rule_assignments(
    rule: &NpcBehaviourRule,
    ctx: &mut NpcEvalContext<'_>,
) -> HashMap<String, i64> {
    let mut vars = npc_base_vars(ctx);
    for action in &rule.parsed_actions {
        let NpcAction::Assignment { key, value } = action else {
            continue;
        };
        if let Some(val) = npc_eval_expr(value, ctx, &vars) {
            vars.insert(key.trim().to_ascii_lowercase(), val);
        }
    }
    vars
}

fn npc_base_vars(ctx: &NpcEvalContext<'_>) -> HashMap<String, i64> {
    let mut vars = HashMap::new();
    if let Some(cached) = ctx.player.npc_vars.get(&ctx.npc_id) {
        for (key, value) in cached {
            vars.insert(key.clone(), *value);
        }
    }
    for (key, value) in npc_message_vars(ctx.tokens) {
        vars.insert(key, value);
    }
    vars
}

fn npc_message_vars(tokens: &[String]) -> HashMap<String, i64> {
    let mut vars = HashMap::new();
    let mut index = 1;
    for token in tokens {
        let value = match token.parse::<i64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let key = format!("%{index}");
        vars.insert(key, value);
        index += 1;
    }
    vars
}

fn npc_rule_action_plan(
    rule: &NpcBehaviourRule,
    ctx: &mut NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> NpcActionPlan {
    let mut plan = NpcActionPlan::new(ctx.npc_id);
    for action in &rule.parsed_actions {
        match action {
            NpcAction::Assignment { key, value } if key.eq_ignore_ascii_case("topic") => {
                if let Some(topic) = npc_eval_expr(value, ctx, vars) {
                    plan.actions.push(NpcPlannedAction::SetTopic(topic));
                }
            }
            NpcAction::Assignment { key, value } if key.eq_ignore_ascii_case("hp") => {
                if let Some(amount) = npc_eval_expr(value, ctx, vars)
                    .and_then(|value| u32::try_from(value).ok())
                {
                    plan.actions.push(NpcPlannedAction::SetHealth(amount));
                }
            }
            NpcAction::Assignment { key, value } => {
                if let Some(value) = npc_eval_expr(value, ctx, vars) {
                    plan.actions.push(NpcPlannedAction::SetNpcVar {
                        key: key.trim().to_ascii_lowercase(),
                        value,
                    });
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("SetQuestValue") => {
                if args.len() < 2 {
                    continue;
                }
                let quest_id = args[0].trim().parse::<u16>().ok();
                let value = npc_eval_expr(&args[1], ctx, vars)
                    .and_then(|value| i32::try_from(value).ok());
                if let (Some(quest_id), Some(value)) = (quest_id, value) {
                    plan.actions
                        .push(NpcPlannedAction::SetQuestValue(quest_id, value));
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("TeachSpell") => {
                if args.is_empty() {
                    continue;
                }
                let spell_id = npc_eval_expr(&args[0], ctx, vars)
                    .and_then(|value| u16::try_from(value).ok())
                    .map(SpellId);
                if let Some(spell_id) = spell_id {
                    plan.actions.push(NpcPlannedAction::TeachSpell(spell_id));
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("Profession") => {
                if args.is_empty() {
                    continue;
                }
                let profession = npc_eval_expr(&args[0], ctx, vars)
                    .and_then(|value| u8::try_from(value).ok());
                if let Some(profession) = profession {
                    plan.actions.push(NpcPlannedAction::SetProfession(profession));
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("Create") => {
                if let Some(type_id) = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars)) {
                    let count = vars
                        .get("amount")
                        .copied()
                        .unwrap_or(1)
                        .max(1);
                    if let (Ok(type_id), Ok(count)) =
                        (u16::try_from(type_id), u16::try_from(count))
                    {
                        plan.actions.push(NpcPlannedAction::CreateItem {
                            type_id: ItemTypeId(type_id),
                            count,
                        });
                    }
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("Delete") => {
                if let Some(type_id) = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars)) {
                    let count = vars
                        .get("amount")
                        .copied()
                        .unwrap_or(1)
                        .max(1);
                    if let (Ok(type_id), Ok(count)) =
                        (u16::try_from(type_id), u16::try_from(count))
                    {
                        plan.actions.push(NpcPlannedAction::DeleteItem {
                            type_id: ItemTypeId(type_id),
                            count,
                        });
                    }
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("Teleport") => {
                if args.len() < 3 {
                    continue;
                }
                let x = npc_eval_expr(&args[0], ctx, vars)
                    .and_then(|value| u16::try_from(value).ok());
                let y = npc_eval_expr(&args[1], ctx, vars)
                    .and_then(|value| u16::try_from(value).ok());
                let z = npc_eval_expr(&args[2], ctx, vars)
                    .and_then(|value| u8::try_from(value).ok());
                if let (Some(x), Some(y), Some(z)) = (x, y, z) {
                    plan.actions.push(NpcPlannedAction::Teleport(Position { x, y, z }));
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("EffectOpp") => {
                if let Some(effect) = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars)) {
                    if let Ok(effect) = u16::try_from(effect) {
                        plan.actions.push(NpcPlannedAction::EffectOpp(effect));
                    }
                }
            }
            NpcAction::Call { name, args } if name.eq_ignore_ascii_case("EffectMe") => {
                if let Some(effect) = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars)) {
                    if let Ok(effect) = u16::try_from(effect) {
                        plan.actions.push(NpcPlannedAction::EffectMe(effect));
                    }
                }
            }
            NpcAction::Ident(value) if value.eq_ignore_ascii_case("DeleteMoney") => {
                let amount = vars.get("price").copied().or_else(|| vars.get("amount").copied());
                if let Some(amount) = amount.and_then(|value| u32::try_from(value).ok()) {
                    plan.actions.push(NpcPlannedAction::DeleteMoney(amount));
                }
            }
            NpcAction::Ident(value) if value.eq_ignore_ascii_case("CreateMoney") => {
                let amount = vars.get("price").copied().or_else(|| vars.get("amount").copied());
                if let Some(amount) = amount.and_then(|value| u32::try_from(value).ok()) {
                    plan.actions.push(NpcPlannedAction::CreateMoney(amount));
                }
            }
            NpcAction::Ident(value) if value.eq_ignore_ascii_case("Queue") => {
                plan.actions.push(NpcPlannedAction::QueuePlayer);
            }
            NpcAction::Ident(value) if value.eq_ignore_ascii_case("Idle") => {
                plan.actions.push(NpcPlannedAction::SetTopic(0));
                plan.actions.push(NpcPlannedAction::ClearFocus);
            }
            _ => {}
        }
    }
    plan
}

fn npc_rule_reply(
    rule: &NpcBehaviourRule,
    ctx: &NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> Option<String> {
    for action in &rule.parsed_actions {
        if let NpcAction::Say(text) = action {
            return Some(npc_format_reply(text, ctx, vars));
        }
    }
    None
}

fn npc_format_reply(text: &str, ctx: &NpcEvalContext<'_>, vars: &HashMap<String, i64>) -> String {
    let mut reply = text.replace("%N", &ctx.player.name);
    if reply.contains("%A") {
        let amount = vars
            .get("amount")
            .copied()
            .or_else(|| vars.get("%1").copied())
            .unwrap_or(0);
        reply = reply.replace("%A", &amount.to_string());
    }
    if reply.contains("%P") {
        let price = vars.get("price").copied().unwrap_or(0);
        reply = reply.replace("%P", &price.to_string());
    }
    if reply.contains("%T") {
        reply = reply.replace("%T", &npc_time_string());
    }
    reply
}

fn npc_time_string() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|delta| delta.as_secs())
        .unwrap_or(0);
    let seconds_of_day = (secs % 86_400) as u32;
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    format!("{hour:02}:{minute:02}")
}

fn npc_string_conditions_match(
    rule: &NpcBehaviourRule,
    _message: &str,
    tokens: &[String],
) -> bool {
    for condition in &rule.parsed_conditions {
        let NpcCondition::String(value) = condition else {
            continue;
        };
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        if let Some(stripped) = value.strip_suffix('$') {
            if !tokens.iter().any(|token| token == stripped) {
                return false;
            }
        } else if !tokens.iter().any(|token| token.starts_with(&value)) {
            return false;
        }
    }
    true
}

fn npc_tokenize_message(message: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in message.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn npc_ident_matches(
    ident: &str,
    ctx: &NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> bool {
    let key = ident.trim().to_ascii_lowercase();
    match key.as_str() {
        "premium" => ctx.player.premium,
        "sorcerer" | "druid" | "paladin" | "knight" => {
            npc_profession_matches(ctx.player.profession, &key)
        }
        "male" => ctx.player.race == 0,
        "female" => ctx.player.race == 1,
        "pzblock" => ctx.player.pvp.fight_expires_at.is_some(),
        "address" => ctx.focus_owner.is_none(),
        "busy" => ctx.is_busy,
        "queue" => ctx.is_queued,
        "idle" => ctx.focus_owner.is_none(),
        "vanish" => false,
        _ => vars.get(&key).copied().unwrap_or(1) != 0,
    }
}

fn npc_profession_matches(profession: u8, key: &str) -> bool {
    match key {
        "sorcerer" => matches!(profession, 3 | 13),
        "druid" => matches!(profession, 4 | 14),
        "paladin" => matches!(profession, 2 | 12),
        "knight" => matches!(profession, 1 | 11),
        _ => false,
    }
}

fn npc_compare(op: &NpcCompareOp, left: i64, right: i64) -> bool {
    match op {
        NpcCompareOp::Eq => left == right,
        NpcCompareOp::Ne => left != right,
        NpcCompareOp::Lt => left < right,
        NpcCompareOp::Gt => left > right,
        NpcCompareOp::Le => left <= right,
        NpcCompareOp::Ge => left >= right,
    }
}

fn npc_eval_expr(
    expr: &str,
    ctx: &mut NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> Option<i64> {
    let expr = expr.trim();
    if expr.is_empty() {
        return None;
    }
    if let Some((left, op, right)) = npc_split_top_level(expr, &['+', '-']) {
        let left = npc_eval_expr(&left, ctx, vars)?;
        let right = npc_eval_expr(&right, ctx, vars)?;
        return Some(if op == '+' { left + right } else { left - right });
    }
    if let Some((left, _, right)) = npc_split_top_level(expr, &['*']) {
        let left = npc_eval_expr(&left, ctx, vars)?;
        let right = npc_eval_expr(&right, ctx, vars)?;
        return Some(left * right);
    }
    if let Some((name, args)) = npc_parse_call(expr) {
        return npc_eval_call(&name, &args, ctx, vars);
    }
    if let Ok(value) = expr.parse::<i64>() {
        return Some(value);
    }
    let key = expr.to_ascii_lowercase();
    if let Some(value) = vars.get(&key) {
        return Some(*value);
    }
    match key.as_str() {
        "topic" => Some(ctx.player.npc_topic(ctx.npc_id)),
        "level" => Some(i64::from(ctx.player.level)),
        "premium" => Some(if ctx.player.premium { 1 } else { 0 }),
        "profession" => Some(i64::from(ctx.player.profession)),
        "hp" => Some(i64::from(ctx.player.stats.health)),
        "countmoney" => Some(i64::from(npc_count_money(ctx.player, ctx.object_types))),
        "poison" => Some(i64::from(npc_condition_total(
            ctx.player,
            ConditionKind::Poison,
            ctx.clock,
        ))),
        "burning" => Some(i64::from(npc_condition_total(
            ctx.player,
            ConditionKind::Fire,
            ctx.clock,
        ))),
        _ => None,
    }
}

fn npc_condition_total(
    player: &PlayerState,
    kind: ConditionKind,
    clock: Option<&GameClock>,
) -> u32 {
    if player.conditions.is_empty() {
        return npc_skill_timer_total(player, kind);
    }
    let Some(now) = clock.map(|clock| clock.now()) else {
        return if player.conditions.iter().any(|cond| cond.kind == kind) {
            1
        } else {
            0
        };
    };
    let mut total = 0u32;
    for condition in &player.conditions {
        if condition.kind != kind {
            continue;
        }
        if condition.is_expired(now) {
            continue;
        }
        let start = condition.next_tick.max(now);
        if start >= condition.expires_at {
            continue;
        }
        let remaining = condition.expires_at.0.saturating_sub(start.0);
        let ticks = remaining
            .saturating_div(condition.interval_ticks.max(1))
            .saturating_add(1);
        let damage = condition
            .tick_damage
            .saturating_mul(ticks.min(u64::from(u32::MAX)) as u32);
        total = total.saturating_add(damage);
    }
    if total == 0 {
        return npc_skill_timer_total(player, kind);
    }
    total
}

fn npc_skill_timer_total(player: &PlayerState, kind: ConditionKind) -> u32 {
    let skill_id = match kind {
        ConditionKind::Poison => SKILL_POISON,
        ConditionKind::Fire => SKILL_BURNING,
        ConditionKind::Energy => SKILL_ENERGY,
        _ => return 0,
    };
    let Some(row) = player
        .raw_skills
        .iter()
        .find(|row| row.skill_id == skill_id)
    else {
        return 0;
    };
    if row.values[SKILL_FIELD_MIN] == i32::MIN || row.values[SKILL_FIELD_CYCLE] <= 0 {
        return 0;
    }
    let cycle = row.values[SKILL_FIELD_CYCLE].max(0) as u32;
    match kind {
        ConditionKind::Poison => cycle,
        ConditionKind::Fire => cycle.saturating_mul(10),
        ConditionKind::Energy => cycle.saturating_mul(25),
        _ => 0,
    }
}

fn npc_eval_call(
    name: &str,
    args: &[String],
    ctx: &mut NpcEvalContext<'_>,
    vars: &HashMap<String, i64>,
) -> Option<i64> {
    let key = name.trim().to_ascii_lowercase();
    match key.as_str() {
        "count" => {
            let id = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars))?;
            let type_id = u16::try_from(id).ok().map(ItemTypeId)?;
            Some(i64::from(npc_count_item(ctx.player, type_id)))
        }
        "countmoney" => Some(i64::from(npc_count_money(ctx.player, ctx.object_types))),
        "questvalue" => {
            let id = args.get(0)?.trim().parse::<u16>().ok()?;
            Some(i64::from(
                ctx.player.quest_values.get(&id).copied().unwrap_or(0),
            ))
        }
        "spellknown" => {
            let id = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars))?;
            let spell_id = u16::try_from(id).ok().map(SpellId)?;
            Some(if ctx.player.known_spells.contains(&spell_id) {
                1
            } else {
                0
            })
        }
        "spelllevel" => {
            let id = args.get(0).and_then(|value| npc_eval_expr(value, ctx, vars))?;
            let spell_id = u16::try_from(id).ok().map(SpellId)?;
            if let Some(spell) = ctx.spellbook.get(spell_id) {
                return Some(i64::from(spell.level_required));
            }
            crate::entities::spell_definitions::spell_level_by_id(spell_id)
                .map(i64::from)
        }
        "random" => {
            if args.len() < 2 {
                return None;
            }
            let min = npc_eval_expr(&args[0], ctx, vars)?;
            let max = npc_eval_expr(&args[1], ctx, vars)?;
            let min_value = min.min(max);
            let max_value = min.max(max);
            let min_u32 = u32::try_from(min_value).ok()?;
            let max_u32 = u32::try_from(max_value).ok()?;
            Some(i64::from(ctx.rng.roll_range(min_u32, max_u32)))
        }
        _ => None,
    }
}

fn npc_parse_call(token: &str) -> Option<(String, Vec<String>)> {
    let open = token.find('(')?;
    if !token.ends_with(')') {
        return None;
    }
    let name = token[..open].trim();
    if name.is_empty() {
        return None;
    }
    let inner = &token[open + 1..token.len() - 1];
    if inner.trim().is_empty() {
        return Some((name.to_string(), Vec::new()));
    }
    let args = split_top_level(inner, ',').ok()?;
    Some((name.to_string(), args))
}

fn npc_split_top_level(input: &str, ops: &[char]) -> Option<(String, char, String)> {
    let mut depth = 0i32;
    let mut in_quotes = false;
    for (idx, ch) in input.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth -= 1,
            _ => {}
        }
        if depth == 0 && !in_quotes && ops.contains(&ch) {
            if idx == 0 {
                continue;
            }
            let left = input[..idx].trim();
            let right = input[idx + 1..].trim();
            if left.is_empty() || right.is_empty() {
                continue;
            }
            return Some((left.to_string(), ch, right.to_string()));
        }
    }
    None
}

fn npc_count_item(player: &PlayerState, type_id: ItemTypeId) -> u32 {
    let mut count = u32::from(player.inventory.count_type(type_id));
    let mut covered_slots = HashSet::new();
    for container in player.open_containers.values() {
        if let Some(slot) = container.source_slot {
            covered_slots.insert(slot);
        }
        for item in &container.items {
            if item.type_id == type_id {
                count = count.saturating_add(u32::from(item.count));
            }
        }
    }
    for (slot, items) in &player.inventory_containers {
        if covered_slots.contains(slot) {
            continue;
        }
        for item in items {
            if item.type_id == type_id {
                count = count.saturating_add(u32::from(item.count));
            }
        }
    }
    count
}

fn npc_count_money(player: &PlayerState, object_types: Option<&ObjectTypeIndex>) -> u32 {
    let Some(object_types) = object_types else {
        return 0;
    };
    let mut total = 0u32;
    for slot in INVENTORY_SLOTS {
        if let Some(item) = player.inventory.slot(slot) {
            if let Some(value) = money_value_for_type(object_types, item.type_id) {
                total = total.saturating_add(value.saturating_mul(u32::from(item.count)));
            }
        }
    }
    let mut covered_slots = HashSet::new();
    for container in player.open_containers.values() {
        if let Some(slot) = container.source_slot {
            covered_slots.insert(slot);
        }
        for item in &container.items {
            if let Some(value) = money_value_for_type(object_types, item.type_id) {
                total = total.saturating_add(value.saturating_mul(u32::from(item.count)));
            }
        }
    }
    for (slot, items) in &player.inventory_containers {
        if covered_slots.contains(slot) {
            continue;
        }
        for item in items {
            if let Some(value) = money_value_for_type(object_types, item.type_id) {
                total = total.saturating_add(value.saturating_mul(u32::from(item.count)));
            }
        }
    }
    total
}

fn item_stack_weight(object_types: &ObjectTypeIndex, item: &ItemStack) -> u32 {
    let weight = object_types
        .get(item.type_id)
        .and_then(|object| object.attribute_u16("Weight"))
        .map(u32::from)
        .unwrap_or(0);
    weight.saturating_mul(u32::from(item.count))
}

fn item_stack_total_weight(object_types: &ObjectTypeIndex, item: &ItemStack) -> u32 {
    let mut total = item_stack_weight(object_types, item);
    if item.count > 1 || item.contents.is_empty() {
        return total;
    }
    for content in &item.contents {
        total = total.saturating_add(item_stack_total_weight(object_types, content));
    }
    total
}

fn container_items_weight(object_types: &ObjectTypeIndex, items: &[ItemStack]) -> u32 {
    let mut total = 0u32;
    for item in items {
        total = total.saturating_add(item_stack_total_weight(object_types, item));
    }
    total
}

fn item_type_weight(object_types: &ObjectTypeIndex, type_id: ItemTypeId, count: u32) -> u32 {
    let weight = object_types
        .get(type_id)
        .and_then(|object| object.attribute_u16("Weight"))
        .map(u32::from)
        .unwrap_or(0);
    weight.saturating_mul(count)
}

fn player_total_weight(player: &PlayerState, object_types: &ObjectTypeIndex) -> u32 {
    let mut total = 0u32;
    for slot in INVENTORY_SLOTS {
        if let Some(item) = player.inventory.slot(slot) {
            total = total.saturating_add(item_stack_weight(object_types, item));
            let contents = player
                .inventory_containers
                .get(&slot)
                .map(|items| items.as_slice())
                .or_else(|| {
                    if item.contents.is_empty() {
                        None
                    } else {
                        Some(item.contents.as_slice())
                    }
                });
            if let Some(contents) = contents {
                total = total.saturating_add(container_items_weight(object_types, contents));
            }
        }
    }
    total
}

fn monster_total_weight(monster: &MonsterInstance, object_types: &ObjectTypeIndex) -> u32 {
    let mut total = 0u32;
    for slot in INVENTORY_SLOTS {
        if let Some(item) = monster.inventory.slot(slot) {
            total = total.saturating_add(item_stack_weight(object_types, item));
            if let Some(contents) = monster.inventory_containers.get(&slot) {
                total = total.saturating_add(container_items_weight(object_types, contents));
            }
        }
    }
    total
}

fn monster_inventory_items(monster: &MonsterInstance) -> Vec<ItemStack> {
    let mut items = Vec::new();
    for slot in INVENTORY_SLOTS {
        let Some(item) = monster.inventory.slot(slot) else {
            continue;
        };
        let mut entry = item.clone();
        if let Some(contents) = monster.inventory_containers.get(&slot) {
            entry.contents = contents.clone();
        }
        items.push(entry);
    }
    items
}

fn container_belongs_to_inventory(player: &PlayerState, container_id: u8) -> bool {
    let mut current_id = container_id;
    let mut guard = 0;
    loop {
        let Some(container) = player.open_containers.get(&current_id) else {
            return false;
        };
        if container.source_slot.is_some() {
            return true;
        }
        let Some(parent_id) = container.parent_container_id else {
            return false;
        };
        current_id = parent_id;
        guard += 1;
        if guard > 16 {
            return false;
        }
    }
}

fn container_root_position(player: &PlayerState, container_id: u8) -> Option<Position> {
    let mut current_id = container_id;
    let mut guard = 0;
    loop {
        let container = player.open_containers.get(&current_id)?;
        if let Some(position) = container.source_position {
            return Some(position);
        }
        let parent_id = container.parent_container_id?;
        current_id = parent_id;
        guard += 1;
        if guard > 32 {
            return None;
        }
    }
}

fn container_active_depot(player: &PlayerState, container_id: u8) -> Option<ActiveDepot> {
    let active = player.active_depot?;
    let root = container_root_position(player, container_id)?;
    if root == active.locker_position {
        Some(active)
    } else {
        None
    }
}

fn find_container_by_map_source(
    player: &PlayerState,
    position: Position,
    stack_pos: u8,
) -> Option<u8> {
    player.open_containers.iter().find_map(|(id, container)| {
        if container.source_position == Some(position) && container.source_stack_pos == Some(stack_pos)
        {
            Some(*id)
        } else {
            None
        }
    })
}

fn find_container_by_map_item(
    player: &PlayerState,
    position: Position,
    stack_pos: u8,
    item: &ItemStack,
) -> Option<u8> {
    if let Some(id) = find_container_by_map_source(player, position, stack_pos) {
        return Some(id);
    }
    player.open_containers.iter().find_map(|(id, container)| {
        if container.source_position == Some(position)
            && container.item_type == item.type_id
            && container.items == item.contents
        {
            Some(*id)
        } else {
            None
        }
    })
}

fn find_container_by_slot(player: &PlayerState, slot: InventorySlot) -> Option<u8> {
    player.open_containers.iter().find_map(|(id, container)| {
        if container.source_slot == Some(slot) {
            Some(*id)
        } else {
            None
        }
    })
}

fn find_container_by_parent_slot(
    player: &PlayerState,
    parent_id: u8,
    parent_slot: u8,
) -> Option<u8> {
    player.open_containers.iter().find_map(|(id, container)| {
        if container.parent_container_id == Some(parent_id) && container.parent_slot == Some(parent_slot)
        {
            Some(*id)
        } else {
            None
        }
    })
}

fn container_is_descendant(player: &PlayerState, ancestor_id: u8, container_id: u8) -> bool {
    let mut current = Some(container_id);
    let mut guard = 0;
    while let Some(id) = current {
        if id == ancestor_id {
            return true;
        }
        let next = player
            .open_containers
            .get(&id)
            .and_then(|container| container.parent_container_id);
        current = next;
        guard += 1;
        if guard > 32 {
            break;
        }
    }
    false
}

fn item_contains_open_container(item: &ItemStack, target: &OpenContainer) -> bool {
    if item.type_id == target.item_type && item.contents == target.items {
        return true;
    }
    for child in &item.contents {
        if item_contains_open_container(child, target) {
            return true;
        }
    }
    false
}

fn ensure_capacity_for_weight(
    player: &PlayerState,
    object_types: &ObjectTypeIndex,
    added_weight: u32,
) -> Result<(), String> {
    let max_weight = player.stats.capacity.saturating_mul(100);
    let current_weight = player_total_weight(player, object_types);
    if current_weight.saturating_add(added_weight) > max_weight {
        return Err("You do not have enough capacity.".to_string());
    }
    Ok(())
}

fn money_value_for_type(
    object_types: &ObjectTypeIndex,
    type_id: ItemTypeId,
) -> Option<u32> {
    let object = object_types.get(type_id)?;
    money_value_from_object(object)
}

fn money_value_from_object(object: &ObjectType) -> Option<u32> {
    let meaning = object.attribute_u16("Meaning")?;
    match meaning {
        1 => Some(1),
        2 => Some(100),
        3 => Some(10_000),
        _ => None,
    }
}

fn calculate_money_change(
    amount: i32,
    gold: i32,
    platinum: i32,
    crystal: i32,
) -> Option<(i32, i32, i32)> {
    let mut go = gold;
    let mut pl = platinum;
    let mut cr = crystal;
    if amount <= 0 {
        return Some((0, 0, 0));
    }
    if cr.saturating_mul(10_000).saturating_add(pl.saturating_mul(100)).saturating_add(go)
        < amount
    {
        return None;
    }
    let amount_cr = amount / 10_000;
    let mut amount_rem = amount % 10_000;
    if pl.saturating_mul(100).saturating_add(go) < amount_rem {
        cr = amount_cr + 1;
        pl = (amount_rem - 10_000) / 100;
        go = (amount_rem - 10_000) % 100;
    } else {
        if cr < amount_cr {
            amount_rem = amount - cr * 10_000;
        } else {
            cr = amount_cr;
        }
        let amount_pl = amount_rem / 100;
        let amount_go = amount_rem % 100;
        if go < amount_go {
            pl = amount_pl + 1;
            go = amount_go - 100;
        } else if pl < amount_pl {
            go = amount_rem - pl * 100;
        } else {
            pl = amount_pl;
            go = amount_go;
        }
    }
    Some((go, pl, cr))
}

fn remove_item_from_player_count(
    player: &mut PlayerState,
    type_id: ItemTypeId,
    mut count: u32,
) -> u32 {
    if count == 0 {
        return 0;
    }
    let mut removed = 0u32;
    for slot in INVENTORY_SLOTS {
        if count == 0 {
            break;
        }
        let existing = player.inventory.slot(slot).cloned();
        let Some(item) = existing else {
            continue;
        };
        if item.type_id != type_id {
            continue;
        }
        let to_remove = count.min(u32::from(item.count)) as u16;
        if to_remove == 0 {
            continue;
        }
        if player.inventory.remove_item(slot, to_remove).is_ok() {
            removed = removed.saturating_add(u32::from(to_remove));
            count = count.saturating_sub(u32::from(to_remove));
        }
    }
    if count == 0 {
        return removed;
    }
    let mut covered_slots = HashSet::new();
    for container in player.open_containers.values_mut() {
        if count == 0 {
            break;
        }
        if let Some(slot) = container.source_slot {
            covered_slots.insert(slot);
        }
        let mut index = 0usize;
        while index < container.items.len() && count > 0 {
            if container.items[index].type_id != type_id {
                index += 1;
                continue;
            }
            let available = container.items[index].count;
            let to_remove = count.min(u32::from(available)) as u16;
            if to_remove == 0 {
                index += 1;
                continue;
            }
            removed = removed.saturating_add(u32::from(to_remove));
            count = count.saturating_sub(u32::from(to_remove));
            if to_remove == available {
                container.items.remove(index);
            } else {
                container.items[index].count = available.saturating_sub(to_remove);
                index += 1;
            }
        }
    }
    if count == 0 {
        return removed;
    }
    for (slot, items) in player.inventory_containers.iter_mut() {
        if covered_slots.contains(slot) {
            continue;
        }
        if count == 0 {
            break;
        }
        let mut index = 0usize;
        while index < items.len() && count > 0 {
            if items[index].type_id != type_id {
                index += 1;
                continue;
            }
            let available = items[index].count;
            let to_remove = count.min(u32::from(available)) as u16;
            if to_remove == 0 {
                index += 1;
                continue;
            }
            removed = removed.saturating_add(u32::from(to_remove));
            count = count.saturating_sub(u32::from(to_remove));
            if to_remove == available {
                items.remove(index);
            } else {
                items[index].count = available.saturating_sub(to_remove);
                index += 1;
            }
        }
    }
    removed
}

fn collect_valid_spells(
    spellbook: &SpellBook,
    plans: &[NpcActionPlan],
) -> HashSet<SpellId> {
    let mut valid = HashSet::new();
    let mut missing = HashSet::new();
    for plan in plans {
        for action in &plan.actions {
            if let NpcPlannedAction::TeachSpell(spell_id) = action {
                if spellbook.get(*spell_id).is_some() {
                    valid.insert(*spell_id);
                } else if missing.insert(*spell_id) {
                    logging::log_game(&format!(
                        "npc spell id {:?} missing from spellbook",
                        spell_id
                    ));
                }
            }
        }
    }
    valid
}

fn apply_npc_plans(
    world: &mut WorldState,
    player_id: PlayerId,
    plans: &[NpcActionPlan],
    valid_spells: &HashSet<SpellId>,
) -> (Vec<MoveUseEffect>, bool) {
    let mut effects = Vec::new();
    let mut containers_dirty = false;
    let npc_positions: HashMap<CreatureId, Position> = world
        .npcs
        .iter()
        .map(|(id, npc)| (*id, npc.position))
        .collect();
    for plan in plans {
        for action in &plan.actions {
            match *action {
                NpcPlannedAction::SetTopic(topic) => {
                    if let Some(player) = world.players.get_mut(&player_id) {
                        player.set_npc_topic(plan.npc_id, topic);
                        if topic == 0 {
                            player.clear_npc_vars(plan.npc_id);
                        }
                    }
                }
                NpcPlannedAction::SetNpcVar { ref key, value } => {
                    if let Some(player) = world.players.get_mut(&player_id) {
                        player.set_npc_var(plan.npc_id, key, value);
                    }
                }
                NpcPlannedAction::SetProfession(profession) => {
                    if let Some(player) = world.players.get_mut(&player_id) {
                        if profession == 10 {
                            if (1..=4).contains(&player.profession) {
                                player.profession = player.profession.saturating_add(10);
                            }
                        } else {
                            player.profession = profession;
                        }
                    }
                }
                NpcPlannedAction::SetHealth(health) => {
                    if let Some(player) = world.players.get_mut(&player_id) {
                        let max = player.stats.max_health.max(1);
                        player.stats.health = health.clamp(1, max);
                    }
                }
                NpcPlannedAction::SetQuestValue(quest_id, value) => {
                    if let Some(player) = world.players.get_mut(&player_id) {
                        player.quest_values.insert(quest_id, value);
                    }
                }
                NpcPlannedAction::TeachSpell(spell_id) => {
                    if valid_spells.contains(&spell_id) {
                        if let Some(player) = world.players.get_mut(&player_id) {
                            player.learn_spell(spell_id);
                        }
                    }
                }
                NpcPlannedAction::CreateItem { type_id, count } => {
                    containers_dirty = true;
                    let _ = world.add_item_to_player(player_id, type_id, count);
                }
                NpcPlannedAction::DeleteItem { type_id, count } => {
                    containers_dirty = true;
                    let _ = world.remove_item_from_player(player_id, type_id, count);
                }
                NpcPlannedAction::CreateMoney(amount) => {
                    containers_dirty = true;
                    let _ = world.add_money_to_player(player_id, amount);
                }
                NpcPlannedAction::DeleteMoney(amount) => {
                    containers_dirty = true;
                    let _ = world.remove_money_from_player(player_id, amount);
                }
                NpcPlannedAction::Teleport(position) => {
                    let _ = world.teleport_player(player_id, position);
                }
                NpcPlannedAction::EffectOpp(effect_id) => {
                    if let Some(player) = world.players.get(&player_id) {
                        effects.push(MoveUseEffect {
                            position: player.position,
                            effect_id,
                        });
                    }
                }
                NpcPlannedAction::EffectMe(effect_id) => {
                    if let Some(position) = npc_positions.get(&plan.npc_id).copied() {
                        effects.push(MoveUseEffect {
                            position,
                            effect_id,
                        });
                    }
                }
                NpcPlannedAction::QueuePlayer => {
                    if let Some(npc) = world.npcs.get_mut(&plan.npc_id) {
                        if !npc.queue.iter().any(|id| *id == player_id)
                            && npc.focused != Some(player_id)
                        {
                            npc.queue.push_back(player_id);
                        }
                    }
                }
                NpcPlannedAction::FocusPlayer { expires_at } => {
                    if let Some(npc) = world.npcs.get_mut(&plan.npc_id) {
                        npc.focused = Some(player_id);
                        npc.focus_expires_at = expires_at;
                        npc.queue.retain(|id| *id != player_id);
                    }
                }
                NpcPlannedAction::ClearFocus => {
                    if let Some(npc) = world.npcs.get_mut(&plan.npc_id) {
                        if npc.focused == Some(player_id) {
                            npc.focused = None;
                            npc.focus_expires_at = None;
                        }
                    }
                }
            }
        }
    }
    (effects, containers_dirty)
}

fn npc_in_range(npc_position: Position, player_position: Position, radius: u16) -> bool {
    if npc_position.z != player_position.z {
        return false;
    }
    let dx = i32::from(npc_position.x) - i32::from(player_position.x);
    let dy = i32::from(npc_position.y) - i32::from(player_position.y);
    let max_axis = dx.unsigned_abs().max(dy.unsigned_abs());
    max_axis <= u32::from(radius)
}

fn player_in_active_range(
    creature_position: Position,
    player_position: Position,
    radius: u16,
) -> bool {
    let (min_z, max_z) = if player_position.z > 7 {
        let min_z = player_position
            .z
            .saturating_sub(ACTIVE_Z_RANGE_UNDERGROUND);
        let max_z = player_position
            .z
            .saturating_add(ACTIVE_Z_RANGE_UNDERGROUND)
            .min(MAX_FLOOR);
        (min_z, max_z)
    } else {
        (0, 7)
    };
    if creature_position.z < min_z || creature_position.z > max_z {
        return false;
    }
    let dx = i32::from(creature_position.x) - i32::from(player_position.x);
    let dy = i32::from(creature_position.y) - i32::from(player_position.y);
    let max_axis = dx.unsigned_abs().max(dy.unsigned_abs());
    max_axis <= u32::from(radius)
}

fn npc_within_wander_radius(home: Position, radius: u16, position: Position) -> bool {
    if radius == 0 {
        return position == home;
    }
    if position.z != home.z {
        return false;
    }
    let dx = i32::from(position.x) - i32::from(home.x);
    let dy = i32::from(position.y) - i32::from(home.y);
    let max_axis = dx.unsigned_abs().max(dy.unsigned_abs());
    max_axis <= u32::from(radius)
}

impl WorldState {
    fn queue_container_refresh(&mut self, player_id: PlayerId) {
        self.pending_container_refresh.insert(player_id);
    }

    pub fn take_container_refresh(&mut self, player_id: PlayerId) -> bool {
        self.pending_container_refresh.remove(&player_id)
    }

    fn queue_container_close(&mut self, player_id: PlayerId, container_id: u8) {
        let pending = self
            .pending_container_closes
            .entry(player_id)
            .or_insert_with(Vec::new);
        if !pending.contains(&container_id) {
            pending.push(container_id);
        }
    }

    pub fn take_container_closes(&mut self, player_id: PlayerId) -> Vec<u8> {
        self.pending_container_closes
            .remove(&player_id)
            .unwrap_or_default()
    }

    fn sync_container_contents(&mut self, player_id: PlayerId, mut container_id: u8) {
        let mut guard = 0;
        loop {
            let snapshot = {
                let Some(player) = self.players.get(&player_id) else {
                    return;
                };
                player.open_containers.get(&container_id).cloned()
            };
            let Some(container) = snapshot else {
                return;
            };
            if let Some(slot) = container.source_slot {
                if let Some(player) = self.players.get_mut(&player_id) {
                    player.inventory_containers.insert(slot, container.items);
                }
                self.queue_container_refresh(player_id);
                return;
            }
            if let (Some(position), Some(stack_pos)) =
                (container.source_position, container.source_stack_pos)
            {
                if let Some(tile) = self.map.tile_mut(position) {
                    let index = stack_pos as usize;
                    if let Some(item) = tile.items.get_mut(index) {
                        item.contents = container.items;
                    }
                    ensure_item_details_len(tile);
                    let map_item = tile.items.get(index).map(map_item_for_stack);
                    if let (Some(detail), Some(map_item)) =
                        (tile.item_details.get_mut(index), map_item)
                    {
                        *detail = map_item;
                    }
                }
                self.queue_container_refresh(player_id);
                return;
            }
            let (parent_id, parent_slot) = match (container.parent_container_id, container.parent_slot)
            {
                (Some(parent_id), Some(parent_slot)) => (parent_id, parent_slot),
                _ => return,
            };
            let Some(player) = self.players.get_mut(&player_id) else {
                return;
            };
            let Some(parent) = player.open_containers.get_mut(&parent_id) else {
                return;
            };
            if let Some(item) = parent.items.get_mut(parent_slot as usize) {
                item.contents = container.items;
            } else {
                return;
            }
            container_id = parent_id;
            guard += 1;
            if guard > 16 {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combat::damage::DamageType;
    use crate::entities::item::ItemKind;
    use crate::scripting::monster::{parse_monster_script, MonsterScript};
    use crate::scripting::raid::{RaidPosition, RaidScript, RaidSpawn};
    use crate::world::item_types::{ItemType, ItemTypeIndex};
    use crate::world::map::{SectorBounds, SectorCoord};
    use crate::world::object_types::{ObjectAttribute, ObjectType, ObjectTypeIndex};
    use crate::world::position::Direction;
    use std::time::Duration;

    fn test_world() -> WorldState {
        WorldState {
            root: None,
            map: Map {
                name: "test".to_string(),
                sector_bounds: None,
                sectors: Vec::new(),
                tiles: HashMap::new(),
            },
            map_original: None,
            map_dat: None,
            mem_dat: None,
            circles: None,
            npc_index: None,
            monster_index: None,
            monster_homes: Vec::new(),
            npcs: HashMap::new(),
            monsters: HashMap::new(),
            raid_events: Vec::new(),
            raid_schedules: HashMap::new(),
            house_areas: None,
            houses: None,
            house_owners: None,
            house_position_index: None,
            next_house_rent_check: None,
            moveuse: None,
            object_types: None,
            item_types: None,
            cron: crate::world::cron::CronSystem::new(),
            players: HashMap::new(),
            offline_players: HashMap::new(),
            spellbook: SpellBook::default(),
            combat_rules: CombatRules::default(),
            pending_messages: Vec::new(),
            pending_skill_updates: Vec::new(),
            pending_data_updates: Vec::new(),
            pending_turn_updates: Vec::new(),
            pending_outfit_updates: Vec::new(),
            pending_map_refreshes: Vec::new(),
            pending_buddy_updates: Vec::new(),
            pending_party_updates: Vec::new(),
            pending_trade_updates: Vec::new(),
            pending_container_closes: HashMap::new(),
            pending_container_refresh: HashSet::new(),
            shop_sessions: HashMap::new(),
            request_queue: Vec::new(),
            request_queue_players: HashSet::new(),
            private_channels: HashMap::new(),
            private_channel_owners: HashMap::new(),
            next_private_channel_id: PRIVATE_CHANNEL_ID_START,
            parties: HashMap::new(),
            next_party_id: 1,
            pending_moveuse_outcomes: HashMap::new(),
            trade_sessions: HashMap::new(),
            trade_by_player: HashMap::new(),
            next_trade_id: 1,
            next_text_edit_id: 1,
            text_edit_sessions: HashMap::new(),
            next_list_edit_id: 1,
            list_edit_sessions: HashMap::new(),
            moveuse_rng: MoveUseRng::default(),
            loot_rng: LootRng::default(),
            monster_rng: MonsterRng::default(),
            npc_rng: NpcRng::default(),
            next_npc_id: NPC_ID_BASE,
            next_monster_id: 1,
            refresh_state: None,
            skill_tick_last: None,
            monster_home_tick_last: None,
            cron_tick_last: None,
            cron_tick_accum: 0,
            cron_round: 0,
            item_index: HashMap::new(),
            item_index_dirty: true,
        }
    }

fn make_tile(position: Position, protection_zone: bool) -> Tile {
        Tile {
            position,
            items: Vec::new(),
            item_details: Vec::new(),
            refresh: false,
            protection_zone,
            no_logout: false,
            annotations: Vec::new(),
            tags: Vec::new(),
        }
    }

    fn make_spell(target: SpellTarget, effect: SpellEffect) -> Spell {
        Spell {
            id: crate::entities::spells::SpellId(1),
            name: "Test Spell".to_string(),
            words: "test".to_string(),
            kind: SpellKind::Instant,
            rune_type_id: None,
            target,
            group: None,
            mana_cost: 0,
            soul_cost: 0,
            level_required: 1,
            magic_level_required: 0,
            cooldown_ms: 0,
            group_cooldown_ms: 0,
            damage_scale_flags: crate::combat::damage::DamageScaleFlags::NONE,
            effect: Some(effect),
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
    fn cast_spell_words_requires_known_spell() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 200, y: 200, z: 7 };

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 0,
            max_damage: 0,
            include_caster: true,
            base_damage: None,
            variance: None,
            base_damage: None,
            variance: None,
        };
        let spell = make_spell(SpellTarget::SelfOnly, effect);
        world.add_spell(spell).expect("spell insert");

        let clock = GameClock::new(Duration::from_millis(100));
        let err = world
            .cast_spell_words(caster_id, "test", None, None, &clock)
            .expect_err("cast should require known spell");
        assert_eq!(err, "spell cast failed: spell not known");
    }

    #[test]
    fn cast_spell_words_casts_for_known_spell() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 210, y: 210, z: 7 };

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 0,
            max_damage: 0,
            include_caster: true,
            base_damage: None,
            variance: None,
            base_damage: None,
            variance: None,
        };
        let spell = make_spell(SpellTarget::SelfOnly, effect);
        let spell_id = spell.id;
        world.add_spell(spell).expect("spell insert");
        world
            .teach_spell(caster_id, spell_id)
            .expect("teach spell");

        let clock = GameClock::new(Duration::from_millis(100));
        let report = world
            .cast_spell_words(caster_id, "test", None, None, &clock)
            .expect("cast should succeed");
        assert_eq!(report.hits.len(), 1);
        assert_eq!(report.hits[0].target, caster_id);
    }

    #[test]
    fn cast_spell_words_resolves_named_target() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let target_id = PlayerId(2);
        let caster_pos = Position { x: 210, y: 210, z: 7 };
        let target_pos = Position { x: 211, y: 210, z: 7 };

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));
        world
            .players
            .insert(target_id, PlayerState::new(target_id, "Target".to_string(), target_pos));
        if let Some(target) = world.players.get_mut(&target_id) {
            target.stats.health = 50;
        }

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Healing,
            damage_type: DamageType::Holy,
            min_damage: 10,
            max_damage: 10,
            include_caster: true,
            base_damage: None,
            variance: None,
        };
        let spell = Spell {
            id: crate::entities::spells::SpellId(2),
            name: "Heal Friend".to_string(),
            words: "exura sio \"name\"".to_string(),
            kind: SpellKind::Instant,
            rune_type_id: None,
            target: SpellTarget::Creature,
            group: None,
            mana_cost: 0,
            soul_cost: 0,
            level_required: 1,
            magic_level_required: 0,
            cooldown_ms: 0,
            group_cooldown_ms: 0,
            damage_scale_flags: crate::combat::damage::DamageScaleFlags::NONE,
            effect: Some(effect),
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
        world.add_spell(spell).expect("spell insert");
        world
            .teach_spell(caster_id, crate::entities::spells::SpellId(2))
            .expect("teach spell");

        let clock = GameClock::new(Duration::from_millis(100));
        let report = world
            .cast_spell_words(caster_id, "exura sio Target", None, None, &clock)
            .expect("cast should succeed");

        assert_eq!(report.hits.len(), 1);
        let target = world.players.get(&target_id).unwrap();
        assert_eq!(target.stats.health, 60);
    }

    #[test]
    fn cast_spell_applies_damage_to_target() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let target_id = PlayerId(2);
        let caster_pos = Position { x: 100, y: 100, z: 7 };
        let target_pos = Position { x: 101, y: 100, z: 7 };

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));
        world
            .players
            .insert(target_id, PlayerState::new(target_id, "Target".to_string(), target_pos));

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 12,
            max_damage: 12,
            include_caster: false,
            base_damage: None,
            variance: None,
        };
        let spell = make_spell(SpellTarget::Position, effect);
        let mut clock = GameClock::new(Duration::from_millis(100));
        let report = world
            .cast_spell(caster_id, &spell, Some(target_pos), None, &clock)
            .expect("cast should succeed");

        assert_eq!(report.hits.len(), 1);
        let target = world.players.get(&target_id).unwrap();
        assert_eq!(target.stats.health, 88);
        let caster = world.players.get(&caster_id).unwrap();
        assert_eq!(caster.stats.health, 100);
        clock.advance(1);
    }

    #[test]
    fn offensive_spell_blocked_by_protection_zone() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let target_id = PlayerId(2);
        let caster_pos = Position { x: 10, y: 10, z: 7 };
        let target_pos = Position { x: 11, y: 10, z: 7 };

        world.map.tiles.insert(caster_pos, make_tile(caster_pos, false));
        world.map.tiles.insert(target_pos, make_tile(target_pos, true));

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));
        world
            .players
            .insert(target_id, PlayerState::new(target_id, "Target".to_string(), target_pos));

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 5,
            max_damage: 5,
            include_caster: false,
            base_damage: None,
            variance: None,
        };
        let spell = make_spell(SpellTarget::Position, effect);
        let clock = GameClock::new(Duration::from_millis(100));
        let err = world
            .cast_spell(caster_id, &spell, Some(target_pos), None, &clock)
            .expect_err("cast should be blocked");
        assert_eq!(err, "spell cast failed: protection zone");
    }

    #[test]
    fn line_spell_requires_direction() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 50, y: 50, z: 7 };

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));

        let effect = SpellEffect {
            shape: SpellShape::Line { length: 3 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Energy,
            min_damage: 3,
            max_damage: 3,
            include_caster: true,
            base_damage: None,
            variance: None,
        };
        let spell = make_spell(SpellTarget::SelfOnly, effect);
        let clock = GameClock::new(Duration::from_millis(100));
        let err = world
            .cast_spell(caster_id, &spell, None, None, &clock)
            .expect_err("line spell should require a direction");
        assert_eq!(
            err,
            "spell cast failed: direction required for line spell"
        );
    }

    #[test]
    fn spell_cast_consumes_costs_and_respects_cooldown() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 300, y: 300, z: 7 };
        let mut player = PlayerState::new(caster_id, "Caster".to_string(), caster_pos);
        player.stats.mana = 50;
        player.stats.soul = 5;
        world.players.insert(caster_id, player);

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 1,
            max_damage: 1,
            include_caster: true,
            base_damage: None,
            variance: None,
        };
        let mut spell = make_spell(SpellTarget::SelfOnly, effect);
        spell.mana_cost = 10;
        spell.soul_cost = 2;
        spell.cooldown_ms = 1_000;

        let mut clock = GameClock::new(Duration::from_millis(100));
        world
            .cast_spell(caster_id, &spell, None, None, &clock)
            .expect("first cast");

        let player = world.players.get(&caster_id).expect("player");
        assert_eq!(player.stats.mana, 40);
        assert_eq!(player.stats.soul, 3);

        let err = world
            .cast_spell(caster_id, &spell, None, None, &clock)
            .expect_err("cooldown should block");
        assert_eq!(err, "spell cast failed: spell cooldown");

        clock.advance(10);
        world
            .cast_spell(caster_id, &spell, None, None, &clock)
            .expect("cooldown expired");
        let player = world.players.get(&caster_id).expect("player");
        assert_eq!(player.stats.mana, 30);
        assert_eq!(player.stats.soul, 1);
    }

    #[test]
    fn spell_cast_blocks_insufficient_mana_and_soul() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 320, y: 320, z: 7 };
        let mut player = PlayerState::new(caster_id, "Caster".to_string(), caster_pos);
        player.stats.mana = 5;
        player.stats.soul = 1;
        world.players.insert(caster_id, player);

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 1,
            max_damage: 1,
            include_caster: true,
            base_damage: None,
            variance: None,
        };
        let mut spell = make_spell(SpellTarget::SelfOnly, effect);
        spell.mana_cost = 10;
        spell.soul_cost = 2;

        let clock = GameClock::new(Duration::from_millis(100));
        let err = world
            .cast_spell(caster_id, &spell, None, None, &clock)
            .expect_err("insufficient mana");
        assert_eq!(err, "spell cast failed: insufficient mana");

        let player = world.players.get_mut(&caster_id).expect("player");
        player.stats.mana = 50;
        let err = world
            .cast_spell(caster_id, &spell, None, None, &clock)
            .expect_err("insufficient soul");
        assert_eq!(err, "spell cast failed: insufficient soul");
    }

    #[test]
    fn spell_group_cooldown_blocks_other_spells() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let caster_pos = Position { x: 340, y: 340, z: 7 };
        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 0 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 1,
            max_damage: 1,
            include_caster: true,
            base_damage: None,
            variance: None,
        };
        let group = Some(SpellGroupId(1));
        let mut spell_a = make_spell(SpellTarget::SelfOnly, effect);
        spell_a.id = SpellId(10);
        spell_a.group = group;
        spell_a.group_cooldown_ms = 1_000;
        let mut spell_b = make_spell(SpellTarget::SelfOnly, effect);
        spell_b.id = SpellId(11);
        spell_b.group = group;
        spell_b.group_cooldown_ms = 1_000;

        let clock = GameClock::new(Duration::from_millis(100));
        world
            .cast_spell(caster_id, &spell_a, None, None, &clock)
            .expect("cast spell a");
        let err = world
            .cast_spell(caster_id, &spell_b, None, None, &clock)
            .expect_err("group cooldown should block");
        assert_eq!(err, "spell cast failed: group cooldown");
    }

    #[test]
    fn area_spell_hits_targets_in_radius() {
        let mut world = test_world();
        let caster_id = PlayerId(1);
        let target_a = PlayerId(2);
        let target_b = PlayerId(3);
        let caster_pos = Position { x: 400, y: 400, z: 7 };
        let target_a_pos = Position { x: 401, y: 400, z: 7 };
        let target_b_pos = Position { x: 400, y: 401, z: 7 };

        world
            .players
            .insert(caster_id, PlayerState::new(caster_id, "Caster".to_string(), caster_pos));
        world.players.insert(
            target_a,
            PlayerState::new(target_a, "TargetA".to_string(), target_a_pos),
        );
        world.players.insert(
            target_b,
            PlayerState::new(target_b, "TargetB".to_string(), target_b_pos),
        );

        let effect = SpellEffect {
            shape: SpellShape::Area { radius: 1 },
            kind: crate::entities::spells::SpellEffectKind::Damage,
            damage_type: DamageType::Physical,
            min_damage: 1,
            max_damage: 1,
            include_caster: false,
            base_damage: None,
            variance: None,
        };
        let spell = make_spell(SpellTarget::Area, effect);
        let clock = GameClock::new(Duration::from_millis(100));
        let report = world
            .cast_spell(caster_id, &spell, Some(caster_pos), None, &clock)
            .expect("cast");
        let hit_targets: HashSet<PlayerId> = report
            .hits
            .iter()
            .filter_map(|hit| match hit.target {
                SpellTargetId::Player(target_id) => Some(target_id),
                SpellTargetId::Monster(_) => None,
            })
            .collect();
        assert!(hit_targets.contains(&target_a));
        assert!(hit_targets.contains(&target_b));
        assert!(!hit_targets.contains(&caster_id));
    }

    #[test]
    fn move_player_blocks_missing_tile() {
        let mut world = test_world();
        let player_id = PlayerId(1);
        let origin = Position { x: 10, y: 10, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Mover".to_string(), origin));
        let clock = GameClock::new(Duration::from_millis(100));

        let err = world
            .move_player(player_id, Direction::East, &clock)
            .expect_err("movement should fail");
        assert_eq!(err, "movement blocked: missing tile");
    }

    #[test]
    fn move_player_blocks_occupied_tile() {
        let mut world = test_world();
        let mover_id = PlayerId(10);
        let blocker_id = PlayerId(11);
        let origin = Position { x: 20, y: 20, z: 7 };
        let destination = Position { x: 21, y: 20, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        world
            .map
            .tiles
            .insert(destination, make_tile(destination, false));
        world
            .players
            .insert(mover_id, PlayerState::new(mover_id, "Mover".to_string(), origin));
        world.players.insert(
            blocker_id,
            PlayerState::new(blocker_id, "Blocker".to_string(), destination),
        );
        let clock = GameClock::new(Duration::from_millis(100));

        let err = world
            .move_player(mover_id, Direction::East, &clock)
            .expect_err("movement should fail");
        assert_eq!(err, "movement blocked: creature");
    }

    #[test]
    fn move_player_blocks_out_of_bounds_sector() {
        let mut world = test_world();
        world.map.sector_bounds = Some(SectorBounds {
            min: SectorCoord { x: 0, y: 0, z: 7 },
            max: SectorCoord { x: 0, y: 0, z: 7 },
        });
        let player_id = PlayerId(2);
        let origin = Position { x: 31, y: 0, z: 7 };
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Mover".to_string(), origin));
        let clock = GameClock::new(Duration::from_millis(100));

        let err = world
            .move_player(player_id, Direction::East, &clock)
            .expect_err("movement should fail");
        assert_eq!(err, "movement blocked: out of bounds");
    }

    #[test]
    fn move_player_succeeds_when_tiles_unbounded() {
        let mut world = test_world();
        let player_id = PlayerId(3);
        let origin = Position { x: 100, y: 100, z: 7 };
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Mover".to_string(), origin));
        let clock = GameClock::new(Duration::from_millis(100));

        let destination = world
            .move_player(player_id, Direction::North, &clock)
            .expect("movement should succeed");
        assert_eq!(destination, Position { x: 100, y: 99, z: 7 });
        let player = world.players.get(&player_id).expect("player exists");
        assert_eq!(player.position, destination);
    }

    #[test]
    fn move_player_respects_cooldown() {
        let mut world = test_world();
        let player_id = PlayerId(4);
        let origin = Position { x: 200, y: 200, z: 7 };
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Mover".to_string(), origin));
        let mut clock = GameClock::new(Duration::from_millis(100));

        world
            .move_player(player_id, Direction::North, &clock)
            .expect("first move should succeed");
        let err = world
            .move_player(player_id, Direction::North, &clock)
            .expect_err("movement should respect cooldown");
        assert_eq!(err, "movement blocked: cooldown");
        clock.advance(20);
        world
            .move_player(player_id, Direction::North, &clock)
            .expect("move should succeed after cooldown");
    }

    #[test]
    fn tick_monsters_moves_when_ready() {
        let mut world = test_world();
        let origin = Position { x: 50, y: 50, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        for direction in [
            Direction::North,
            Direction::East,
            Direction::South,
            Direction::West,
            Direction::Northeast,
            Direction::Northwest,
            Direction::Southeast,
            Direction::Southwest,
        ] {
            let neighbor = origin.step(direction).expect("step");
            world.map.tiles.insert(neighbor, make_tile(neighbor, false));
        }
        let player_id = PlayerId(1);
        let player_pos = Position { x: 51, y: 50, z: 7 };
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Nearby".to_string(), player_pos));
        let monster_id = CreatureId(1);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "TestMonster".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags::default(),
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        let moved = world.tick_monsters(&clock);
        assert_eq!(moved.moved, 1);
        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_ne!(monster.position, origin);
    }

    #[test]
    fn tick_monsters_moves_towards_target() {
        let mut world = test_world();
        let origin = Position { x: 50, y: 50, z: 7 };
        let step = Position { x: 51, y: 50, z: 7 };
        let target_pos = Position { x: 52, y: 50, z: 7 };
        for pos in [origin, step, target_pos] {
            world.map.tiles.insert(pos, make_tile(pos, false));
        }
        let player_id = PlayerId(1);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Target".to_string(), target_pos));
        let monster_id = CreatureId(2);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Hunter".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags::default(),
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 8,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        world.tick_monsters(&clock);

        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(monster.position, step);
    }

    #[test]
    fn tick_monsters_moves_away_when_fleeing() {
        let mut world = test_world();
        let origin = Position { x: 51, y: 50, z: 7 };
        let step = Position { x: 50, y: 50, z: 7 };
        let target_pos = Position { x: 52, y: 50, z: 7 };
        for pos in [origin, step, target_pos] {
            world.map.tiles.insert(pos, make_tile(pos, false));
        }
        let player_id = PlayerId(1);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Target".to_string(), target_pos));
        let monster_id = CreatureId(2);
        let mut stats = Stats::default();
        stats.health = 5;
        stats.max_health = 5;
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Runner".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats,
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags::default(),
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 10,
                lose_target_distance: 8,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        world.tick_monsters(&clock);

        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(monster.position, step);
    }

    #[test]
    fn monster_target_ignores_players_in_protection_zone() {
        let mut world = test_world();
        let monster_pos = Position { x: 70, y: 70, z: 7 };
        let player_pos = Position { x: 71, y: 70, z: 7 };
        world.map.tiles.insert(monster_pos, make_tile(monster_pos, false));
        world.map.tiles.insert(player_pos, make_tile(player_pos, true));
        let player_id = PlayerId(10);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Safe".to_string(), player_pos));

        let target = world.select_monster_target(
            monster_pos,
            None,
            8,
            [100, 0, 0, 0],
            &HashMap::new(),
            MonsterFlags::default(),
        );
        assert!(target.is_none());
    }

    #[test]
    fn monster_movement_blocks_entering_protection_zone() {
        let mut world = test_world();
        let origin = Position { x: 80, y: 80, z: 7 };
        let destination = Position { x: 81, y: 80, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        world.map.tiles.insert(destination, make_tile(destination, true));
        let monster_id = CreatureId(9);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "PZTester".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags::default(),
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let err = world
            .move_monster(monster_id, Direction::East)
            .expect_err("movement should be blocked");
        assert_eq!(err, "movement blocked: protection zone");
    }

    #[test]
    fn monster_kicks_blocking_box() {
        let mut world = test_world();
        let origin = Position { x: 40, y: 40, z: 7 };
        let destination = Position { x: 41, y: 40, z: 7 };
        let pushed = Position { x: 42, y: 40, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        world.map.tiles.insert(destination, make_tile(destination, false));
        world.map.tiles.insert(pushed, make_tile(pushed, false));
        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(100),
                name: "Box".to_string(),
                flags: vec!["Unpass".to_string(), "Take".to_string()],
                attributes: Vec::new(),
            })
            .expect("insert box type");
        world.object_types = Some(object_types);
        let dest_tile = world.map.tile_mut(destination).expect("dest tile");
        place_on_tile(
            dest_tile,
            ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(100),
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
            false,
        )
        .expect("place box");
        let monster_id = CreatureId(2);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Kicker".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags {
                    kick_boxes: true,
                    ..MonsterFlags::default()
                },
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let moved = world
            .move_monster(monster_id, Direction::East)
            .expect("movement should succeed");
        assert_eq!(moved, destination);
        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(monster.position, destination);
        let dest_tile = world.map.tile(destination).expect("dest tile");
        assert!(dest_tile.items.is_empty());
        let pushed_tile = world.map.tile(pushed).expect("pushed tile");
        assert_eq!(pushed_tile.items.len(), 1);
        assert_eq!(pushed_tile.items[0].type_id, ItemTypeId(100));
    }

    #[test]
    fn monster_kicks_blocking_creature() {
        let mut world = test_world();
        let origin = Position { x: 45, y: 40, z: 7 };
        let destination = Position { x: 46, y: 40, z: 7 };
        let pushed = Position { x: 47, y: 40, z: 7 };
        for pos in [origin, destination, pushed] {
            world.map.tiles.insert(pos, make_tile(pos, false));
        }
        let player_id = PlayerId(5);
        world.players.insert(
            player_id,
            PlayerState::new(player_id, "Blocker".to_string(), destination),
        );
        let monster_id = CreatureId(6);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Kicker".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags {
                    kick_creatures: true,
                    ..MonsterFlags::default()
                },
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let moved = world
            .move_monster(monster_id, Direction::East)
            .expect("movement should succeed");
        assert_eq!(moved, destination);
        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(monster.position, destination);
        let player = world.players.get(&player_id).expect("player exists");
        assert_eq!(player.position, pushed);
    }

    #[test]
    fn monster_kick_creature_respects_unpushable() {
        let mut world = test_world();
        let origin = Position { x: 50, y: 40, z: 7 };
        let destination = Position { x: 51, y: 40, z: 7 };
        let pushed = Position { x: 52, y: 40, z: 7 };
        for pos in [origin, destination, pushed] {
            world.map.tiles.insert(pos, make_tile(pos, false));
        }
        let blocker_id = CreatureId(7);
        world.monsters.insert(
            blocker_id,
            MonsterInstance {
                id: blocker_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Blocker".to_string(),
                position: destination,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags {
                    unpushable: true,
                    ..MonsterFlags::default()
                },
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );
        let monster_id = CreatureId(8);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Kicker".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags {
                    kick_creatures: true,
                    ..MonsterFlags::default()
                },
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let err = world
            .move_monster(monster_id, Direction::East)
            .expect_err("movement should be blocked");
        assert_eq!(err, "movement blocked: creature");
        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(monster.position, origin);
        let blocker = world.monsters.get(&blocker_id).expect("blocker exists");
        assert_eq!(blocker.position, destination);
    }

    #[test]
    fn tick_monsters_respects_cooldown() {
        let mut world = test_world();
        let origin = Position { x: 60, y: 60, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        let player_id = PlayerId(1);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Nearby".to_string(), origin));
        let monster_id = CreatureId(3);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Sleeper".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags::default(),
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(5)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        let moved = world.tick_monsters(&clock);
        assert_eq!(moved.moved, 0);
        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(monster.position, origin);
    }

    #[test]
    fn tick_monsters_resets_cooldown_after_tick() {
        let mut world = test_world();
        let origin = Position { x: 70, y: 70, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        let neighbor = origin.step(Direction::North).expect("step");
        world.map.tiles.insert(neighbor, make_tile(neighbor, false));
        let player_id = PlayerId(1);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Nearby".to_string(), origin));
        let monster_id = CreatureId(4);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Runner".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: MonsterFlags::default(),
                skills: MonsterSkills::default(),
                attack: 0,
                defend: 0,
                armor: 0,
                poison: 0,
                spells: Vec::new(),
                strategy: [100, 0, 0, 0],
                flee_threshold: 0,
                lose_target_distance: 0,
                target: None,
                damage_by: HashMap::new(),
                speed: 220,
                outfit_effect: None,
                speed_effect: None,
                strength_effect: None,
                move_cooldown: Cooldown::new(GameTick(0)),
                combat_cooldown: Cooldown::new(GameTick(0)),
                talk_lines: Vec::new(),
                talk_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        world.tick_monsters(&clock);
        let monster = world.monsters.get(&monster_id).expect("monster exists");
        assert_eq!(
            monster.move_cooldown.ready_at(),
            GameTick(MONSTER_MOVE_INTERVAL_TICKS)
        );
    }

    #[test]
    fn tick_npcs_respects_cooldown() {
        let mut world = test_world();
        let origin = Position { x: 80, y: 80, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        let player_id = PlayerId(1);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Nearby".to_string(), origin));
        let npc_id = CreatureId(0x4000_0001);
        world.npcs.insert(
            npc_id,
            NpcInstance {
                id: npc_id,
                script_key: "test".to_string(),
                name: "Npc".to_string(),
                position: origin,
                direction: Direction::South,
                home: origin,
                outfit: DEFAULT_OUTFIT,
                radius: 2,
                focused: None,
                focus_expires_at: None,
                queue: VecDeque::new(),
                move_cooldown: Cooldown::new(GameTick(10)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        let moved = world.tick_npcs(&clock);
        assert_eq!(moved.len(), 0);
        let npc = world.npcs.get(&npc_id).expect("npc exists");
        assert_eq!(npc.position, origin);
    }

    #[test]
    fn tick_npcs_resets_cooldown_after_tick() {
        let mut world = test_world();
        let origin = Position { x: 90, y: 90, z: 7 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        let neighbor = origin.step(Direction::North).expect("step");
        world.map.tiles.insert(neighbor, make_tile(neighbor, false));
        let player_id = PlayerId(1);
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Nearby".to_string(), origin));
        let npc_id = CreatureId(0x4000_0002);
        world.npcs.insert(
            npc_id,
            NpcInstance {
                id: npc_id,
                script_key: "test".to_string(),
                name: "Npc".to_string(),
                position: origin,
                direction: Direction::South,
                home: origin,
                outfit: DEFAULT_OUTFIT,
                radius: 2,
                focused: None,
                focus_expires_at: None,
                queue: VecDeque::new(),
                move_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let clock = GameClock::new(Duration::from_millis(100));
        let _ = world.tick_npcs(&clock);
        let npc = world.npcs.get(&npc_id).expect("npc exists");
        assert_eq!(
            npc.move_cooldown.ready_at(),
            GameTick(NPC_MOVE_INTERVAL_TICKS)
        );
    }

    #[test]
    fn raid_schedule_spawns_monsters() {
        let mut world = test_world();
        let mut index = MonsterIndex::default();
        let mut script = MonsterScript::default();
        script.name = Some("Badger".to_string());
        index.scripts.insert("Badger".to_string(), script);
        index.race_index.insert(1, "Badger".to_string());
        index.raids.insert(
            "TestRaid".to_string(),
            RaidScript {
                raid_type: Some("SmallRaid".to_string()),
                interval: None,
                spawns: vec![RaidSpawn {
                    delay: Some(5),
                    position: Some(RaidPosition { x: 10, y: 10, z: 7 }),
                    spread: None,
                    race: Some(1),
                    count: None,
                    message: None,
                    fields: Vec::new(),
                }],
                fields: Vec::new(),
            },
        );
        world.monster_index = Some(index);

        world
            .schedule_raid("TestRaid", 1, GameTick(10))
            .expect("schedule");
        assert_eq!(world.raid_events.len(), 1);
        assert_eq!(world.raid_events[0].at, GameTick(15));

        let none = world.spawn_due_raids(GameTick(14));
        assert!(none.is_empty());
        let spawned = world.spawn_due_raids(GameTick(15));
        assert_eq!(spawned.len(), 1);
        let id = spawned[0];
        let monster = world.monsters.get(&id).expect("monster spawned");
        assert_eq!(monster.name, "Badger");
        assert_eq!(monster.position, Position { x: 10, y: 10, z: 7 });
    }

    #[test]
    fn defeating_monster_awards_experience_and_drops() {
        let mut world = test_world();
        let killer_id = PlayerId(1);
        let killer_pos = Position { x: 20, y: 20, z: 7 };
        world
            .players
            .insert(killer_id, PlayerState::new(killer_id, "Hunter".to_string(), killer_pos));

        let monster_pos = Position { x: 21, y: 20, z: 7 };
        world.map.tiles.insert(monster_pos, make_tile(monster_pos, false));

        let script_input = r#"
Name = "Rat"
RaceNumber = 1
Experience = 25
Skills = {(HitPoints, 10, 0, 0, 0, 0, 0)}
Inventory = {(3031, 2, 1000)}
Corpse = 4240
"#;
        let script = parse_monster_script(script_input).expect("parse script");
        let mut index = MonsterIndex::default();
        index.race_index.insert(1, "Rat".to_string());
        index.scripts.insert("Rat".to_string(), script);
        world.monster_index = Some(index);

        let monster_id = world
            .spawn_monster_by_race(1, monster_pos)
            .expect("spawn monster");
        let reward = world
            .apply_damage_to_monster(monster_id, DamageType::Physical, 10, Some(killer_id))
            .expect("apply damage")
            .expect("monster reward");

        assert_eq!(reward.experience, 25);
        assert_eq!(reward.drops.len(), 1);
        assert_eq!(reward.drops[0].type_id.0, 3031);
        assert_eq!(reward.drops[0].count, 2);

        let killer = world.players.get(&killer_id).expect("killer exists");
        assert_eq!(killer.experience, 25);

        let tile = world.map.tile(monster_pos).expect("tile exists");
        assert_eq!(tile.items.len(), 1);
        assert_eq!(tile.items[0].type_id.0, 4240);
        assert_eq!(tile.items[0].count, 1);
        assert_eq!(tile.items[0].contents.len(), 1);
        assert_eq!(tile.items[0].contents[0].type_id.0, 3031);
        assert_eq!(tile.items[0].contents[0].count, 2);
    }

    #[test]
    fn move_inventory_item_rejects_invalid_slot_for_body_position() {
        let mut world = test_world();
        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(100),
                name: "Helmet".to_string(),
                flags: Vec::new(),
                attributes: vec![ObjectAttribute {
                    key: "BodyPosition".to_string(),
                    value: "1".to_string(),
                }],
            })
            .expect("insert object type");
        world.object_types = Some(object_types);

        let mut item_types = ItemTypeIndex::default();
        item_types
            .insert(ItemType {
                id: ItemTypeId(100),
                name: "Helmet".to_string(),
                kind: ItemKind::Armor,
                stackable: false,
                has_count: false,
                container_capacity: None,
                takeable: true,
                is_expiring: false,
                expire_stop: false,
                expire_time_secs: None,
                expire_target: None,
            })
            .expect("insert item type");
        world.item_types = Some(item_types);

        let player_id = PlayerId(1);
        let player_pos = Position { x: 10, y: 10, z: 7 };
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Player".to_string(), player_pos));
        let player = world.players.get_mut(&player_id).expect("player exists");
        player.inventory.set_slot(
            InventorySlot::Head,
            Some(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(100),
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            }),
        );

        let err = world
            .move_inventory_item(player_id, InventorySlot::Head, InventorySlot::Armor, 1)
            .expect_err("invalid slot should fail");
        assert_eq!(err, "item cannot be equipped in slot");
    }

    #[test]
    fn place_on_tile_merges_stackable_items() {
        let position = Position { x: 1, y: 1, z: 7 };
        let mut tile = make_tile(position, false);
        tile.items.push(ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(200),
            count: 4,
            attributes: Vec::new(),
            contents: Vec::new(),
        });

        place_on_tile(
            &mut tile,
            ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(200),
                count: 6,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
            true,
        )
        .expect("place on tile");

        assert_eq!(tile.items.len(), 1);
        assert_eq!(tile.items[0].count, 10);
    }

    #[test]
    fn cron_container_decay_moves_contents_to_parent_tile() {
        let mut world = test_world();
        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(100),
                name: "Bag".to_string(),
                flags: vec!["Container".to_string(), "Expire".to_string()],
                attributes: vec![
                    ObjectAttribute {
                        key: "Capacity".to_string(),
                        value: "10".to_string(),
                    },
                    ObjectAttribute {
                        key: "ExpireTarget".to_string(),
                        value: "101".to_string(),
                    },
                    ObjectAttribute {
                        key: "TotalExpireTime".to_string(),
                        value: "1".to_string(),
                    },
                ],
            })
            .expect("insert container type");
        object_types
            .insert(ObjectType {
                id: ItemTypeId(101),
                name: "Bag (decayed)".to_string(),
                flags: Vec::new(),
                attributes: Vec::new(),
            })
            .expect("insert target type");
        object_types
            .insert(ObjectType {
                id: ItemTypeId(200),
                name: "Gem".to_string(),
                flags: Vec::new(),
                attributes: Vec::new(),
            })
            .expect("insert child type");
        world.item_types = Some(crate::world::item_types::build_item_types(&object_types));
        world.object_types = Some(object_types);

        let child = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(200),
            count: 1,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        let container = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(100),
            count: 1,
            attributes: Vec::new(),
            contents: vec![child],
        };

        let position = Position { x: 5, y: 5, z: 7 };
        let mut tile = make_tile(position, false);
        tile.items.push(container.clone());
        world.map.tiles.insert(position, tile);
        world.schedule_cron_for_item_tree(&container);

        world.cron_round = 1;
        let processed = world.process_cron_system();
        assert_eq!(processed, 1);

        let tile = world.map.tile(position).expect("tile exists");
        assert_eq!(tile.items.len(), 2);
        assert_eq!(tile.items[0].type_id, ItemTypeId(101));
        assert!(tile.items[0].contents.is_empty());
        assert_eq!(tile.items[1].type_id, ItemTypeId(200));
    }

    #[test]
    fn cron_corpse_decay_deletes_contents() {
        let mut world = test_world();
        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(300),
                name: "Corpse".to_string(),
                flags: vec![
                    "Container".to_string(),
                    "Corpse".to_string(),
                    "Expire".to_string(),
                ],
                attributes: vec![
                    ObjectAttribute {
                        key: "ExpireTarget".to_string(),
                        value: "301".to_string(),
                    },
                    ObjectAttribute {
                        key: "TotalExpireTime".to_string(),
                        value: "1".to_string(),
                    },
                ],
            })
            .expect("insert corpse type");
        object_types
            .insert(ObjectType {
                id: ItemTypeId(301),
                name: "Bone".to_string(),
                flags: Vec::new(),
                attributes: Vec::new(),
            })
            .expect("insert corpse target type");
        object_types
            .insert(ObjectType {
                id: ItemTypeId(302),
                name: "Loot".to_string(),
                flags: Vec::new(),
                attributes: Vec::new(),
            })
            .expect("insert loot type");
        world.item_types = Some(crate::world::item_types::build_item_types(&object_types));
        world.object_types = Some(object_types);

        let child = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(302),
            count: 1,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        let corpse = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(300),
            count: 1,
            attributes: Vec::new(),
            contents: vec![child],
        };

        let position = Position { x: 6, y: 6, z: 7 };
        let mut tile = make_tile(position, false);
        tile.items.push(corpse.clone());
        world.map.tiles.insert(position, tile);
        world.schedule_cron_for_item_tree(&corpse);

        world.cron_round = 1;
        let processed = world.process_cron_system();
        assert_eq!(processed, 1);

        let tile = world.map.tile(position).expect("tile exists");
        assert_eq!(tile.items.len(), 1);
        assert_eq!(tile.items[0].type_id, ItemTypeId(301));
        assert!(tile.items[0].contents.is_empty());
    }

    #[test]
    fn schedule_cron_respects_remaining_expire_time() {
        let mut world = test_world();
        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(400),
                name: "Timed".to_string(),
                flags: vec!["Expire".to_string()],
                attributes: vec![ObjectAttribute {
                    key: "TotalExpireTime".to_string(),
                    value: "10".to_string(),
                }],
            })
            .expect("insert expiring type");
        world.item_types = Some(crate::world::item_types::build_item_types(&object_types));
        world.object_types = Some(object_types);

        let mut item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(400),
            count: 1,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        set_remaining_expire_secs(&mut item, 2);
        world.schedule_cron_for_item_tree(&item);

        assert_eq!(world.cron.get_remaining(item.id, world.cron_round), Some(2));
    }

    #[test]
    fn expirestop_preserves_and_restores_remaining_time() {
        let mut world = test_world();
        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(500),
                name: "Timed".to_string(),
                flags: vec!["Expire".to_string()],
                attributes: vec![ObjectAttribute {
                    key: "TotalExpireTime".to_string(),
                    value: "10".to_string(),
                }],
            })
            .expect("insert expiring type");
        object_types
            .insert(ObjectType {
                id: ItemTypeId(501),
                name: "Stopped".to_string(),
                flags: vec!["ExpireStop".to_string()],
                attributes: Vec::new(),
            })
            .expect("insert stop type");
        world.item_types = Some(crate::world::item_types::build_item_types(&object_types));
        world.object_types = Some(object_types);

        let mut item = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(500),
            count: 1,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        world.schedule_cron_for_item_tree(&item);

        world.cron_round = 3;
        assert_eq!(world.cron.get_remaining(item.id, world.cron_round), Some(7));

        world
            .change_itemstack_type(&mut item, ItemTypeId(501), 0)
            .expect("change to stop");
        assert!(item_saved_expire_secs(&item).unwrap_or(0) > 0);
        assert_eq!(world.cron.get_remaining(item.id, world.cron_round), None);

        world
            .change_itemstack_type(&mut item, ItemTypeId(500), 0)
            .expect("change to expiring");
        assert_eq!(world.cron.get_remaining(item.id, world.cron_round), Some(7));
    }

    #[test]
    fn insert_into_container_append_merges_stackable() {
        let mut container = OpenContainer {
            container_id: 1,
            item_type: ItemTypeId(400),
            name: "Bag".to_string(),
            capacity: 10,
            has_parent: false,
            parent_container_id: None,
            parent_slot: None,
            source_slot: None,
            source_position: None,
            source_stack_pos: None,
            items: vec![ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(300),
                count: 5,
                attributes: Vec::new(),
                contents: Vec::new(),
            }],
        };

        let update = insert_into_container(
            &mut container,
            1,
            0xff,
            ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(300),
                count: 7,
                attributes: Vec::new(),
                contents: Vec::new(),
            },
            true,
        )
        .expect("insert into container");

        let expected = ItemStack { id: crate::entities::item::ItemId::next(),
            type_id: ItemTypeId(300),
            count: 12,
            attributes: Vec::new(),
            contents: Vec::new(),
        };
        assert_eq!(container.items.len(), 1);
        assert_eq!(container.items[0], expected);
        assert_eq!(
            update,
            ContainerUpdate::Update {
                container_id: 1,
                slot: 0,
                item: expected
            }
        );
    }

    #[test]
    fn moveuse_use_emits_effect_text_and_damage() {
        let mut world = test_world();
        let player_id = PlayerId(7);
        let player_pos = Position { x: 10, y: 10, z: 7 };
        world.players.insert(
            player_id,
            PlayerState::new(player_id, "User".to_string(), player_pos),
        );

        let item_type = ItemTypeId(100);
        let tile_pos = Position { x: 11, y: 10, z: 7 };
        world.map.tiles.insert(tile_pos, make_tile(tile_pos, false));
        world
            .map
            .tile_mut(tile_pos)
            .expect("tile exists")
            .items
            .push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: item_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            });

        let rule = MoveUseRule {
            event: MoveUseExpr {
                name: "Use".to_string(),
                args: Vec::new(),
            },
            conditions: vec![MoveUseExpr {
                name: "IsType".to_string(),
                args: vec!["Obj1".to_string(), "100".to_string()],
            }],
            actions: vec![
                MoveUseExpr {
                    name: "Effect".to_string(),
                    args: vec!["User".to_string(), "13".to_string()],
                },
                MoveUseExpr {
                    name: "Text".to_string(),
                    args: vec![
                        "User".to_string(),
                        "\"Hello\"".to_string(),
                        "20".to_string(),
                    ],
                },
                MoveUseExpr {
                    name: "Damage".to_string(),
                    args: vec![
                        "User".to_string(),
                        "User".to_string(),
                        "1".to_string(),
                        "5".to_string(),
                    ],
                },
            ],
            line_no: 1,
        };

        world.moveuse = Some(MoveUseDatabase {
            sections: vec![MoveUseSection {
                name: "Root".to_string(),
                rules: vec![rule],
                children: Vec::new(),
            }],
        });

        let outcome = world
            .use_object(player_id, tile_pos, item_type)
            .expect("use object");
        assert_eq!(outcome.matched_rule, Some(1));
        assert_eq!(outcome.effects.len(), 1);
        assert_eq!(outcome.effects[0].position, player_pos);
        assert_eq!(outcome.effects[0].effect_id, 13);
        assert_eq!(outcome.texts.len(), 1);
        assert_eq!(outcome.texts[0].position, player_pos);
        assert_eq!(outcome.texts[0].message, "Hello");
        assert_eq!(outcome.texts[0].mode, 20);
        assert_eq!(outcome.damages.len(), 1);
        assert_eq!(outcome.damages[0].amount, 5);

        let player = world.players.get(&player_id).expect("player exists");
        assert_eq!(player.stats.health, 95);
    }

    #[test]
    fn moveuse_delete_in_inventory_removes_item() {
        let mut world = test_world();
        let player_id = PlayerId(8);
        let player_pos = Position { x: 20, y: 20, z: 7 };
        world.players.insert(
            player_id,
            PlayerState::new(player_id, "User".to_string(), player_pos),
        );
        let player = world.players.get_mut(&player_id).expect("player exists");
        player.inventory.set_slot(
            InventorySlot::Backpack,
            Some(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(200),
                count: 5,
                attributes: Vec::new(),
                contents: Vec::new(),
            }),
        );

        let trigger_type = ItemTypeId(100);
        let tile_pos = Position { x: 21, y: 20, z: 7 };
        world.map.tiles.insert(tile_pos, make_tile(tile_pos, false));
        world
            .map
            .tile_mut(tile_pos)
            .expect("tile exists")
            .items
            .push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: trigger_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            });

        let rule = MoveUseRule {
            event: MoveUseExpr {
                name: "Use".to_string(),
                args: Vec::new(),
            },
            conditions: vec![MoveUseExpr {
                name: "IsType".to_string(),
                args: vec!["Obj1".to_string(), "100".to_string()],
            }],
            actions: vec![MoveUseExpr {
                name: "DeleteInInventory".to_string(),
                args: vec![
                    "User".to_string(),
                    "200".to_string(),
                    "3".to_string(),
                ],
            }],
            line_no: 1,
        };

        world.moveuse = Some(MoveUseDatabase {
            sections: vec![MoveUseSection {
                name: "Root".to_string(),
                rules: vec![rule],
                children: Vec::new(),
            }],
        });

        world
            .use_object(player_id, tile_pos, trigger_type)
            .expect("use object");
        let player = world.players.get(&player_id).expect("player exists");
        let item = player
            .inventory
            .slot(InventorySlot::Backpack)
            .expect("item exists");
        assert_eq!(item.count, 2);
    }

    #[test]
    fn moveuse_move_top_rel_teleports_player() {
        let mut world = test_world();
        let player_id = PlayerId(9);
        let origin = Position { x: 30, y: 30, z: 7 };
        let collision = Position { x: 31, y: 30, z: 7 };
        let target = Position { x: 31, y: 30, z: 8 };
        world.map.tiles.insert(origin, make_tile(origin, false));
        world.map.tiles.insert(collision, make_tile(collision, false));
        world.map.tiles.insert(target, make_tile(target, false));

        world.players.insert(
            player_id,
            PlayerState::new(player_id, "Mover".to_string(), origin),
        );
        world
            .map
            .tile_mut(collision)
            .expect("tile exists")
            .items
            .push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(300),
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            });

        let rule = MoveUseRule {
            event: MoveUseExpr {
                name: "Collision".to_string(),
                args: Vec::new(),
            },
            conditions: vec![MoveUseExpr {
                name: "IsType".to_string(),
                args: vec!["Obj1".to_string(), "300".to_string()],
            }],
            actions: vec![MoveUseExpr {
                name: "MoveTopRel".to_string(),
                args: vec!["Obj1".to_string(), "[0,0,1]".to_string()],
            }],
            line_no: 1,
        };

        world.moveuse = Some(MoveUseDatabase {
            sections: vec![MoveUseSection {
                name: "Root".to_string(),
                rules: vec![rule],
                children: Vec::new(),
            }],
        });

        let clock = GameClock::new(Duration::from_millis(100));
        world
            .move_player(player_id, Direction::East, &clock)
            .expect("move player");
        let player = world.players.get(&player_id).expect("player exists");
        assert_eq!(player.position, target);
    }

    #[test]
    fn moveuse_retrieve_ignores_immovable_top_item() {
        let mut world = test_world();
        let player_id = PlayerId(91);
        let player_pos = Position { x: 70, y: 70, z: 7 };
        let target_pos = Position { x: 71, y: 70, z: 7 };
        let destination_pos = Position { x: 72, y: 70, z: 7 };
        world.players.insert(
            player_id,
            PlayerState::new(player_id, "Roper".to_string(), player_pos),
        );
        let player = world.players.get_mut(&player_id).expect("player exists");
        player.inventory.set_slot(
            InventorySlot::Backpack,
            Some(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: ItemTypeId(3003),
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            }),
        );

        world.map.tiles.insert(target_pos, make_tile(target_pos, false));
        world
            .map
            .tile_mut(target_pos)
            .expect("target tile exists")
            .items
            .extend([
                ItemStack {
                    id: crate::entities::item::ItemId::next(),
                    type_id: ItemTypeId(501),
                    count: 1,
                    attributes: Vec::new(),
                    contents: Vec::new(),
                },
                ItemStack {
                    id: crate::entities::item::ItemId::next(),
                    type_id: ItemTypeId(500),
                    count: 1,
                    attributes: Vec::new(),
                    contents: Vec::new(),
                },
            ]);
        world
            .map
            .tiles
            .insert(destination_pos, make_tile(destination_pos, false));

        let mut object_types = ObjectTypeIndex::default();
        object_types
            .insert(ObjectType {
                id: ItemTypeId(501),
                name: "movable".to_string(),
                flags: vec!["Take".to_string()],
                attributes: Vec::new(),
            })
            .expect("insert movable object type");
        object_types
            .insert(ObjectType {
                id: ItemTypeId(500),
                name: "immovable".to_string(),
                flags: vec!["Unmove".to_string()],
                attributes: Vec::new(),
            })
            .expect("insert immovable object type");
        world.object_types = Some(object_types);

        let rule = MoveUseRule {
            event: MoveUseExpr {
                name: "MultiUse".to_string(),
                args: Vec::new(),
            },
            conditions: vec![
                MoveUseExpr {
                    name: "IsType".to_string(),
                    args: vec!["Obj1".to_string(), "3003".to_string()],
                },
                MoveUseExpr {
                    name: "IsType".to_string(),
                    args: vec!["Obj2".to_string(), "500".to_string()],
                },
            ],
            actions: vec![MoveUseExpr {
                name: "Retrieve".to_string(),
                args: vec![
                    "Obj2".to_string(),
                    "[0,0,0]".to_string(),
                    "[1,0,0]".to_string(),
                ],
            }],
            line_no: 1,
        };
        world.moveuse = Some(MoveUseDatabase {
            sections: vec![MoveUseSection {
                name: "Root".to_string(),
                rules: vec![rule],
                children: Vec::new(),
            }],
        });

        world
            .use_object_on_position_with_clock(
                player_id,
                UseObjectSource::Inventory(InventorySlot::Backpack),
                ItemTypeId(3003),
                target_pos,
                ItemTypeId(500),
                None,
            )
            .expect("multi-use should succeed");

        let target_tile = world.map.tile(target_pos).expect("target tile exists");
        assert_eq!(target_tile.items.len(), 2);
        assert_eq!(target_tile.items[0].type_id, ItemTypeId(501));
        assert_eq!(target_tile.items[1].type_id, ItemTypeId(500));
        let destination_tile = world
            .map
            .tile(destination_pos)
            .expect("destination tile exists");
        assert!(destination_tile.items.is_empty());
    }

    #[test]
    fn moveuse_move_sets_player_position() {
        let mut world = test_world();
        let player_id = PlayerId(10);
        let player_pos = Position { x: 40, y: 40, z: 7 };
        let target = Position { x: 41, y: 40, z: 7 };
        world.players.insert(
            player_id,
            PlayerState::new(player_id, "User".to_string(), player_pos),
        );

        let trigger_type = ItemTypeId(110);
        world.map.tiles.insert(player_pos, make_tile(player_pos, false));
        world.map.tiles.insert(target, make_tile(target, false));
        world
            .map
            .tile_mut(player_pos)
            .expect("tile exists")
            .items
            .push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: trigger_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            });

        let rule = MoveUseRule {
            event: MoveUseExpr {
                name: "Use".to_string(),
                args: Vec::new(),
            },
            conditions: vec![MoveUseExpr {
                name: "IsType".to_string(),
                args: vec!["Obj1".to_string(), "110".to_string()],
            }],
            actions: vec![MoveUseExpr {
                name: "Move".to_string(),
                args: vec!["User".to_string(), "[41,40,7]".to_string()],
            }],
            line_no: 1,
        };

        world.moveuse = Some(MoveUseDatabase {
            sections: vec![MoveUseSection {
                name: "Root".to_string(),
                rules: vec![rule],
                children: Vec::new(),
            }],
        });

        world
            .use_object(player_id, player_pos, trigger_type)
            .expect("use object");
        let player = world.players.get(&player_id).expect("player exists");
        assert_eq!(player.position, target);
    }

    #[test]
    fn moveuse_set_start_updates_start_position() {
        let mut world = test_world();
        let player_id = PlayerId(11);
        let player_pos = Position { x: 50, y: 50, z: 7 };
        let start = Position { x: 51, y: 50, z: 7 };
        world.players.insert(
            player_id,
            PlayerState::new(player_id, "User".to_string(), player_pos),
        );

        let trigger_type = ItemTypeId(120);
        world.map.tiles.insert(player_pos, make_tile(player_pos, false));
        world
            .map
            .tile_mut(player_pos)
            .expect("tile exists")
            .items
            .push(ItemStack { id: crate::entities::item::ItemId::next(),
                type_id: trigger_type,
                count: 1,
                attributes: Vec::new(),
                contents: Vec::new(),
            });

        let rule = MoveUseRule {
            event: MoveUseExpr {
                name: "Use".to_string(),
                args: Vec::new(),
            },
            conditions: vec![MoveUseExpr {
                name: "IsType".to_string(),
                args: vec!["Obj1".to_string(), "120".to_string()],
            }],
            actions: vec![MoveUseExpr {
                name: "SetStart".to_string(),
                args: vec!["User".to_string(), "[51,50,7]".to_string()],
            }],
            line_no: 1,
        };

        world.moveuse = Some(MoveUseDatabase {
            sections: vec![MoveUseSection {
                name: "Root".to_string(),
                rules: vec![rule],
                children: Vec::new(),
            }],
        });

        world
            .use_object(player_id, player_pos, trigger_type)
            .expect("use object");
        let player = world.players.get(&player_id).expect("player exists");
        assert_eq!(player.start_position, start);
    }
}

fn tile_has_added_items(base: &Tile, current: &Tile) -> bool {
    if current.items.len() > base.items.len() {
        return true;
    }
    let mut base_counts: HashMap<ItemTypeId, u32> = HashMap::new();
    for item in &base.items {
        *base_counts.entry(item.type_id).or_insert(0) += item.count as u32;
    }
    for item in &current.items {
        let current_count = item.count as u32;
        let base_count = base_counts.get(&item.type_id).copied().unwrap_or(0);
        if current_count > base_count {
            return true;
        }
    }
    false
}

fn advance_refresh_cursor_coords(x: &mut u16, y: &mut u16, bounds: SectorBounds) {
    if *x < bounds.max.x {
        *x += 1;
        return;
    }
    *x = bounds.min.x;
    if *y < bounds.max.y {
        *y += 1;
    } else {
        *y = bounds.min.y;
    }
}
