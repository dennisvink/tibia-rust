use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as FmtWrite;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::combat::conditions::{ConditionKind, ConditionTick};
use crate::entities::creature::{CreatureId, DEFAULT_OUTFIT};
use crate::entities::inventory::{InventorySlot, INVENTORY_SLOTS};
use crate::entities::item::{ItemAttribute, ItemStack, ItemTypeId};
use crate::entities::player::{PlayerId, PlayerState};
use crate::entities::skills::{
    SKILL_BURNING,
    SKILL_DRUNKEN,
    SKILL_ENERGY,
    SKILL_FIELD_CYCLE,
    SKILL_FIELD_MIN,
    SKILL_MANASHIELD,
    SKILL_POISON,
};
use crate::net::game;
use crate::net::game_client::{
    handle_client_packet, AdminOutcome, ClientPacketOutcome, CTalkMessage, LogoutRequestOutcome,
    LookRequest, LookTarget, PartyRequest, ShopRequest, TradeRequest,
};
use crate::net::game_login::{parse_game_login, GameLogin};
use crate::net::packet::{PacketReader, PacketWriter};
use crate::net::ws;
use crate::net::login::{build_login_success_v1, LoginPayloadV1, LoginSuccessV1};
use crate::net::login_flow::{handle_login_packet_v1, waitlist_response, LoginDecision, LoginErrorKind, LoginFlowConfig, WaitlistConfig};
use crate::persistence::accounts::{AccountRegistry, BanList};
use crate::persistence::autosave::autosave_world;
use crate::persistence::store::SaveStore;
use crate::telemetry::logging;
use crate::world::position::Position;
use crate::world::state::{
    BuddyAddResult, ChannelExcludeResult, ChannelInviteResult, ContainerUpdate, CreatureStep,
    CreatureTurnUpdate, LogoutBlockReason, MonsterTickOutcome, MoveUseActor, MoveUseOutcome,
    PlayerCombatOutcome, TradeUpdate, WorldState,
};
use crate::world::time::GameClock;
use crate::world::item_types::ItemTypeIndex;
use crate::world::object_types::ObjectTypeIndex;

static TRACE_COUNTER: AtomicUsize = AtomicUsize::new(1);
const TRACE_ENV: &str = "TIBIA_PACKET_TRACE";
const TRACE_MAX_BYTES: usize = 4096;
const MESSAGE_LOOK: u8 = 0x16;
const GAME_PING_INTERVAL: Duration = Duration::from_secs(15);
const WS_RATE_LIMIT_PACKETS: usize = 200;
const WS_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
const DEFAULT_WORLD_LIGHT_LEVEL: u8 = 0xff;
const DEFAULT_WORLD_LIGHT_COLOR: u8 = 0xff;
const HELP_CHANNEL_ID: u16 = 0x09;
const REQUEST_WAIT_MESSAGE: &str = "Please wait until your request is answered.";
const REQUEST_ALREADY_SUBMITTED: &str =
    "You have already submitted a request. Please wait until it is answered.";
const GLOBAL_REPLAY_HISTORY_TICKS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerExit {
    Shutdown,
    Restart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerSignal {
    Running = 0,
    Shutdown = 1,
    Restart = 2,
}

#[derive(Debug, Clone, Copy)]
struct CreatureMove {
    from: Position,
    to: Position,
    stack_pos: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlayerDataSnapshot {
    health: u32,
    max_health: u32,
    mana: u32,
    max_mana: u32,
    capacity: u32,
    experience: u64,
    level: u16,
    magic_level: u16,
    magic_progress: u8,
    soul: u32,
}

fn snapshot_player_data(player: &PlayerState, capacity: u32) -> PlayerDataSnapshot {
    PlayerDataSnapshot {
        health: player.stats.health,
        max_health: player.stats.max_health,
        mana: player.stats.mana,
        max_mana: player.stats.max_mana,
        capacity,
        experience: player.experience,
        level: player.level,
        magic_level: player.skills.magic.level,
        magic_progress: player.skills.magic.progress,
        soul: player.stats.soul,
    }
}

#[derive(Debug)]
pub struct ServerControl {
    signal: AtomicU8,
}

impl ServerControl {
    pub fn new() -> Self {
        Self {
            signal: AtomicU8::new(ServerSignal::Running as u8),
        }
    }

    pub fn request_shutdown(&self) {
        self.signal.store(ServerSignal::Shutdown as u8, Ordering::SeqCst);
    }

    pub fn request_restart(&self) {
        self.signal.store(ServerSignal::Restart as u8, Ordering::SeqCst);
    }

    fn is_running(&self) -> bool {
        matches!(self.current_signal(), ServerSignal::Running)
    }

    fn exit_reason(&self) -> ServerExit {
        match self.current_signal() {
            ServerSignal::Restart => ServerExit::Restart,
            _ => ServerExit::Shutdown,
        }
    }

    fn current_signal(&self) -> ServerSignal {
        match self.signal.load(Ordering::SeqCst) {
            2 => ServerSignal::Restart,
            1 => ServerSignal::Shutdown,
            _ => ServerSignal::Running,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoginCharacterSelection {
    pub player_id: PlayerId,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct LoginSelection {
    pub account: String,
    pub premium: bool,
    pub is_gm: bool,
    pub is_test_god: bool,
    pub characters: Vec<LoginCharacterSelection>,
}

impl LoginSelection {
    fn pick_from_login(&self, login: &GameLogin) -> Option<(PlayerId, String)> {
        let desired = login.character.trim();
        if !desired.is_empty() {
            if let Some(entry) = self
                .characters
                .iter()
                .find(|entry| entry.name.eq_ignore_ascii_case(desired))
            {
                return Some((entry.player_id, entry.name.clone()));
            }
        }
        if self.characters.len() == 1 {
            let entry = &self.characters[0];
            return Some((entry.player_id, entry.name.clone()));
        }
        self.characters
            .first()
            .map(|entry| (entry.player_id, entry.name.clone()))
    }

    fn pick_default(&self) -> Option<(PlayerId, String)> {
        self.characters
            .first()
            .map(|entry| (entry.player_id, entry.name.clone()))
    }
}

#[derive(Debug, Default)]
pub struct LoginRegistry {
    entries: Mutex<HashMap<IpAddr, LoginSelection>>,
}

impl LoginRegistry {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&self, addr: IpAddr, selection: LoginSelection) {
        if let Ok(mut map) = self.entries.lock() {
            map.insert(addr, selection);
        }
    }

    pub fn take(&self, addr: IpAddr) -> Option<LoginSelection> {
        self.entries.lock().ok().and_then(|mut map| map.remove(&addr))
    }
}

#[derive(Debug, Clone)]
pub struct LoginServerConfig {
    pub bind_addr: String,
    pub ws_bind_addr: Option<String>,
    pub ws_allowed_origins: Option<Vec<String>>,
    pub max_packet: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub flow: LoginFlowConfig,
    pub waitlist: Option<WaitlistConfig>,
    pub root: Option<PathBuf>,
    pub login_registry: Option<Arc<LoginRegistry>>,
    pub world_name: String,
    pub world_addr: Option<String>,
    pub premium_days: u16,
}

impl Default for LoginServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7171".to_string(),
            ws_bind_addr: None,
            ws_allowed_origins: None,
            max_packet: 0x7fe,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            flow: LoginFlowConfig::default(),
            waitlist: Some(WaitlistConfig {
                max_active_logins: 100,
            }),
            root: None,
            login_registry: None,
            world_name: "World".to_string(),
            world_addr: None,
            premium_days: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GameServerConfig {
    pub bind_addr: String,
    pub max_packet: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_warning_after: Option<Duration>,
    pub autosave_interval_seconds: u64,
    pub ws_bind_addr: Option<String>,
    pub ws_allowed_origins: Option<Vec<String>>,
    pub root: Option<PathBuf>,
    pub login_registry: Option<Arc<LoginRegistry>>,
}

impl Default for GameServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7172".to_string(),
            max_packet: 0x7fe,
            read_timeout: Duration::from_secs(15 * 60),
            write_timeout: Duration::from_secs(5),
            idle_warning_after: Some(Duration::from_secs(14 * 60)),
            autosave_interval_seconds: 0,
            ws_bind_addr: None,
            ws_allowed_origins: None,
            root: None,
            login_registry: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StatusServerConfig {
    pub bind_addr: String,
    pub max_packet: usize,
    pub max_request_bytes: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub server_name: String,
    pub login_addr: String,
    pub location: String,
    pub url: String,
    pub owner_name: String,
    pub owner_email: String,
    pub motd: String,
    pub max_players: u32,
    pub software_name: String,
    pub software_version: String,
    pub client_version: String,
}

impl Default for StatusServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7173".to_string(),
            max_packet: 0x7fe,
            max_request_bytes: 8192,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            server_name: "World".to_string(),
            login_addr: "0.0.0.0:7171".to_string(),
            location: String::new(),
            url: String::new(),
            owner_name: String::new(),
            owner_email: String::new(),
            motd: String::new(),
            max_players: 0,
            software_name: "tibia".to_string(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            client_version: "7.72".to_string(),
        }
    }
}

struct StatusServerState {
    start: Instant,
    peak_players: AtomicUsize,
}

impl StatusServerState {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            peak_players: AtomicUsize::new(0),
        }
    }

    fn uptime_secs(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    fn record_peak(&self, online: usize) -> u32 {
        let prev = self.peak_players.fetch_max(online, Ordering::SeqCst);
        prev.max(online) as u32
    }
}

pub(crate) fn build_login_state(
    config: &LoginServerConfig,
) -> Result<Arc<LoginServerState>, String> {
    let accounts = match config.root.as_ref() {
        Some(root) => AccountRegistry::load(root)?,
        None => None,
    };
    let bans = match config.root.as_ref() {
        Some(root) => BanList::load(root)?,
        None => None,
    };
    Ok(Arc::new(LoginServerState {
        active_logins: AtomicUsize::new(0),
        accounts: accounts.map(Arc::new),
        bans: bans.map(Arc::new),
    }))
}

pub fn run_login_server(
    config: LoginServerConfig,
    control: Arc<ServerControl>,
) -> Result<ServerExit, String> {
    let state = build_login_state(&config)?;
    run_login_server_with_state(config, control, state)
}

pub(crate) fn run_login_server_with_state(
    config: LoginServerConfig,
    control: Arc<ServerControl>,
    state: Arc<LoginServerState>,
) -> Result<ServerExit, String> {
    let listener = TcpListener::bind(&config.bind_addr)
        .map_err(|err| format!("bind {} failed: {}", config.bind_addr, err))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| format!("login listener nonblocking failed: {}", err))?;

    logging::log_game(&format!(
        "login server listening on {}",
        config.bind_addr
    ));
    println!("tibia: login server listening on {}", config.bind_addr);

    while control.is_running() {
        match listener.accept() {
            Ok((stream, addr)) => {
                println!("tibia: login connection from {}", addr);
                let config = config.clone();
                let state = Arc::clone(&state);
                thread::spawn(move || {
                    if let Err(err) = handle_login_connection(stream, &config, &state) {
                        logging::log_error(&format!("login connection error: {}", err));
                        eprintln!("login connection error: {}", err);
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => {
                logging::log_error(&format!("login accept error: {}", err));
                eprintln!("login accept error: {}", err);
            }
        }
    }

    Ok(control.exit_reason())
}

pub(crate) fn run_login_ws_server(
    config: LoginServerConfig,
    control: Arc<ServerControl>,
    state: Arc<LoginServerState>,
) -> Result<(), String> {
    let Some(bind_addr) = config.ws_bind_addr.clone() else {
        return Ok(());
    };
    let listener = TcpListener::bind(&bind_addr)
        .map_err(|err| format!("bind {} failed: {}", bind_addr, err))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| format!("login ws listener nonblocking failed: {}", err))?;

    logging::log_game(&format!(
        "login ws server listening on {}",
        bind_addr
    ));
    println!("tibia: login ws server listening on {}", bind_addr);

    let ws_config = ws::WsHandshakeConfig {
        allowed_origins: config.ws_allowed_origins.clone(),
        ..ws::WsHandshakeConfig::default()
    };

    while control.is_running() {
        match listener.accept() {
            Ok((stream, addr)) => {
                println!("tibia: login ws connection from {}", addr);
                let config = config.clone();
                let state = Arc::clone(&state);
                let ws_config = ws_config.clone();
                thread::spawn(move || {
                    if let Err(err) =
                        handle_login_ws_connection(stream, &config, &ws_config, &state)
                    {
                        logging::log_error(&format!("login ws connection error: {}", err));
                        eprintln!("login ws connection error: {}", err);
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => {
                logging::log_error(&format!("login ws accept error: {}", err));
                eprintln!("login ws accept error: {}", err);
            }
        }
    }

    Ok(())
}

pub fn run_game_server(
    config: GameServerConfig,
    world: Arc<Mutex<WorldState>>,
    control: Arc<ServerControl>,
) -> Result<(), String> {
    let state = Arc::new(GameServerState::new());
    run_game_server_with_state(config, world, control, state)
}

pub(crate) fn run_game_server_with_state(
    config: GameServerConfig,
    world: Arc<Mutex<WorldState>>,
    control: Arc<ServerControl>,
    state: Arc<GameServerState>,
) -> Result<(), String> {
    let listener = TcpListener::bind(&config.bind_addr)
        .map_err(|err| format!("bind {} failed: {}", config.bind_addr, err))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| format!("game listener nonblocking failed: {}", err))?;

    logging::log_game(&format!(
        "game server listening on {}",
        config.bind_addr
    ));
    println!("tibia: game server listening on {}", config.bind_addr);

    let _autosave_guard = spawn_autosave_loop(&config, Arc::clone(&world), Arc::clone(&control));
    let _world_tick_guard =
        spawn_world_tick_loop(Arc::clone(&state), Arc::clone(&world), Arc::clone(&control));

    while control.is_running() {
        match listener.accept() {
            Ok((stream, addr)) => {
                println!("tibia: game connection from {}", addr);
                let config = config.clone();
                let state = Arc::clone(&state);
                let world = Arc::clone(&world);
                let control = Arc::clone(&control);
                thread::spawn(move || {
                    if let Err(err) =
                        handle_game_connection(stream, &config, &state, &world, &control)
                    {
                        logging::log_error(&format!("game connection error: {}", err));
                        eprintln!("game connection error: {}", err);
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => {
                logging::log_error(&format!("game accept error: {}", err));
                eprintln!("game accept error: {}", err);
            }
        }
    }

    Ok(())
}

pub(crate) fn run_game_ws_server(
    config: GameServerConfig,
    world: Arc<Mutex<WorldState>>,
    control: Arc<ServerControl>,
    state: Arc<GameServerState>,
) -> Result<(), String> {
    let Some(bind_addr) = config.ws_bind_addr.clone() else {
        return Ok(());
    };
    let listener = TcpListener::bind(&bind_addr)
        .map_err(|err| format!("bind {} failed: {}", bind_addr, err))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| format!("game ws listener nonblocking failed: {}", err))?;

    logging::log_game(&format!(
        "game ws server listening on {}",
        bind_addr
    ));
    println!("tibia: game ws server listening on {}", bind_addr);

    let ws_config = ws::WsHandshakeConfig {
        allowed_origins: config.ws_allowed_origins.clone(),
        ..ws::WsHandshakeConfig::default()
    };

    while control.is_running() {
        match listener.accept() {
            Ok((stream, addr)) => {
                println!("tibia: game ws connection from {}", addr);
                let config = config.clone();
                let state = Arc::clone(&state);
                let world = Arc::clone(&world);
                let control = Arc::clone(&control);
                let ws_config = ws_config.clone();
                thread::spawn(move || {
                    if let Err(err) = handle_game_ws_connection(
                        stream,
                        &config,
                        &ws_config,
                        &state,
                        &world,
                        &control,
                    ) {
                        logging::log_error(&format!("game ws connection error: {}", err));
                        eprintln!("game ws connection error: {}", err);
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => {
                logging::log_error(&format!("game ws accept error: {}", err));
                eprintln!("game ws accept error: {}", err);
            }
        }
    }

    Ok(())
}

const STATUS_OPCODE_INFO: u8 = 0x01;
const STATUS_OPCODE_XML: u8 = 0xff;
const STATUS_INFO_BASIC: u16 = 0x01;
const STATUS_INFO_OWNER: u16 = 0x02;
const STATUS_INFO_MISC: u16 = 0x04;
const STATUS_INFO_PLAYERS: u16 = 0x08;
const STATUS_INFO_MAP: u16 = 0x10;
const STATUS_INFO_PLAYERS_EXT: u16 = 0x20;
const STATUS_INFO_PLAYER_STATUS: u16 = 0x40;
const STATUS_INFO_SOFTWARE: u16 = 0x80;
const STATUS_SECTOR_TILE_SIZE: u32 = 32;

pub fn run_status_server(
    config: StatusServerConfig,
    world: Arc<Mutex<WorldState>>,
    control: Arc<ServerControl>,
) -> Result<(), String> {
    let listener = TcpListener::bind(&config.bind_addr)
        .map_err(|err| format!("bind {} failed: {}", config.bind_addr, err))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| format!("status listener nonblocking failed: {}", err))?;

    logging::log_game(&format!(
        "status server listening on {}",
        config.bind_addr
    ));
    println!("tibia: status server listening on {}", config.bind_addr);

    let state = Arc::new(StatusServerState::new());

    while control.is_running() {
        match listener.accept() {
            Ok((stream, addr)) => {
                println!("tibia: status connection from {}", addr);
                let config = config.clone();
                let state = Arc::clone(&state);
                let world = Arc::clone(&world);
                thread::spawn(move || {
                    if let Err(err) = handle_status_connection(stream, &config, &state, &world) {
                        logging::log_error(&format!("status connection error: {}", err));
                        eprintln!("status connection error: {}", err);
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => {
                logging::log_error(&format!("status accept error: {}", err));
                eprintln!("status accept error: {}", err);
            }
        }
    }

    Ok(())
}

fn handle_status_connection(
    mut stream: TcpStream,
    config: &StatusServerConfig,
    state: &StatusServerState,
    world: &Arc<Mutex<WorldState>>,
) -> Result<(), String> {
    stream
        .set_read_timeout(Some(config.read_timeout))
        .map_err(|err| format!("status read timeout failed: {}", err))?;
    stream
        .set_write_timeout(Some(config.write_timeout))
        .map_err(|err| format!("status write timeout failed: {}", err))?;

    let mut header = [0u8; 2];
    stream
        .read_exact(&mut header)
        .map_err(|err| format!("status header read failed: {}", err))?;
    let len = u16::from_le_bytes(header) as usize;

    if len > config.max_packet {
        let request = read_http_request_with_prefix(&mut stream, &header, config.max_request_bytes)?;
        return handle_status_http_request(&mut stream, config, state, world, &request);
    }
    if len == 0 {
        return Err("status packet length is zero".to_string());
    }

    let mut body = vec![0u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|err| format!("status payload read failed: {}", err))?;

    handle_status_packet(&mut stream, config, state, world, &body)
}

fn handle_status_packet(
    stream: &mut TcpStream,
    config: &StatusServerConfig,
    state: &StatusServerState,
    world: &Arc<Mutex<WorldState>>,
    body: &[u8],
) -> Result<(), String> {
    let mut reader = PacketReader::new(body);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| "status packet missing opcode".to_string())?;
    match opcode {
        STATUS_OPCODE_XML => {
            let tail = reader.read_bytes(reader.remaining()).unwrap_or(&[]);
            if tail != b"info" {
                return Ok(());
            }
            let snapshot = build_status_snapshot(config, state, world, stream.peer_addr().ok())?;
            let xml = build_status_xml(&snapshot);
            stream
                .write_all(xml.as_bytes())
                .map_err(|err| format!("status xml write failed: {}", err))?;
        }
        STATUS_OPCODE_INFO => {
            let requested = reader
                .read_u16_le()
                .ok_or_else(|| "status request missing info mask".to_string())?;
            let player_name = if requested & STATUS_INFO_PLAYER_STATUS != 0 {
                reader.read_string_lossy(64)
            } else {
                None
            };
            let snapshot = build_status_snapshot(config, state, world, stream.peer_addr().ok())?;
            let mut writer = PacketWriter::new();
            write_status_info(&mut writer, &snapshot, requested, player_name.as_deref());
            write_packet(stream, writer.as_slice(), None)
                .map_err(|err| format!("status info write failed: {}", err))?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_status_http_request(
    stream: &mut TcpStream,
    config: &StatusServerConfig,
    state: &StatusServerState,
    world: &Arc<Mutex<WorldState>>,
    _request: &str,
) -> Result<(), String> {
    let snapshot = build_status_snapshot(config, state, world, stream.peer_addr().ok())?;
    let xml = build_status_xml(&snapshot);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        xml.len()
    );
    stream
        .write_all(response.as_bytes())
        .and_then(|_| stream.write_all(xml.as_bytes()))
        .map_err(|err| format!("status http write failed: {}", err))?;
    Ok(())
}

fn read_http_request_with_prefix(
    stream: &mut TcpStream,
    prefix: &[u8],
    max_bytes: usize,
) -> Result<String, String> {
    let mut data = Vec::with_capacity(prefix.len() + 512);
    data.extend_from_slice(prefix);
    let mut buf = [0u8; 512];
    loop {
        let read = stream
            .read(&mut buf)
            .map_err(|err| format!("status http read failed: {}", err))?;
        if read == 0 {
            return Err("status http request closed".to_string());
        }
        data.extend_from_slice(&buf[..read]);
        if data.len() > max_bytes {
            return Err("status http request exceeded max bytes".to_string());
        }
        if data.windows(4).any(|chunk| chunk == b"\r\n\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&data).to_string())
}

struct StatusSnapshot {
    uptime_secs: u64,
    server_name: String,
    ip: String,
    port: String,
    location: String,
    url: String,
    owner_name: String,
    owner_email: String,
    motd: String,
    players_online: u32,
    players_max: u32,
    players_peak: u32,
    map_name: String,
    map_author: String,
    map_width: u16,
    map_height: u16,
    software_name: String,
    software_version: String,
    client_version: String,
    players: Vec<(String, u16)>,
}

fn build_status_snapshot(
    config: &StatusServerConfig,
    state: &StatusServerState,
    world: &Arc<Mutex<WorldState>>,
    peer: Option<SocketAddr>,
) -> Result<StatusSnapshot, String> {
    let (host, port) = split_host_port(&config.login_addr, 7171);
    let ip = resolve_world_ipv4(host, peer);
    let (players, map_name, map_width, map_height) = match world.lock() {
        Ok(world) => {
            let mut players: Vec<(String, u16)> = world
                .players
                .values()
                .map(|player| (player.name.clone(), player.level))
                .collect();
            players.sort_by(|a, b| a.0.cmp(&b.0));
            let (width, height) = map_dimensions(&world);
            (players, world.map.name.clone(), width, height)
        }
        Err(_) => (Vec::new(), "map".to_string(), 0, 0),
    };
    let players_online = players.len() as u32;
    let players_peak = state.record_peak(players_online as usize);
    let players_max = if config.max_players == 0 {
        players_online
    } else {
        config.max_players.max(players_online)
    };
    Ok(StatusSnapshot {
        uptime_secs: state.uptime_secs(),
        server_name: config.server_name.clone(),
        ip: ip.to_string(),
        port: port.to_string(),
        location: config.location.clone(),
        url: config.url.clone(),
        owner_name: config.owner_name.clone(),
        owner_email: config.owner_email.clone(),
        motd: config.motd.clone(),
        players_online,
        players_max,
        players_peak,
        map_name,
        map_author: String::new(),
        map_width,
        map_height,
        software_name: config.software_name.clone(),
        software_version: config.software_version.clone(),
        client_version: config.client_version.clone(),
        players,
    })
}

fn map_dimensions(world: &WorldState) -> (u16, u16) {
    let bounds = world
        .map_dat
        .as_ref()
        .and_then(|dat| dat.sector_bounds)
        .or(world.map.sector_bounds);
    let Some(bounds) = bounds else {
        return (0, 0);
    };
    let width_sectors = bounds
        .max
        .x
        .saturating_sub(bounds.min.x)
        .saturating_add(1) as u32;
    let height_sectors = bounds
        .max
        .y
        .saturating_sub(bounds.min.y)
        .saturating_add(1) as u32;
    let width = width_sectors.saturating_mul(STATUS_SECTOR_TILE_SIZE);
    let height = height_sectors.saturating_mul(STATUS_SECTOR_TILE_SIZE);
    (
        width.min(u16::MAX as u32) as u16,
        height.min(u16::MAX as u32) as u16,
    )
}

fn write_status_info(
    writer: &mut PacketWriter,
    snapshot: &StatusSnapshot,
    requested: u16,
    player_name: Option<&str>,
) {
    if requested & STATUS_INFO_BASIC != 0 {
        writer.write_u8(0x10);
        writer.write_string_str(&snapshot.server_name);
        writer.write_string_str(&snapshot.ip);
        writer.write_string_str(&snapshot.port);
    }
    if requested & STATUS_INFO_OWNER != 0 {
        writer.write_u8(0x11);
        writer.write_string_str(&snapshot.owner_name);
        writer.write_string_str(&snapshot.owner_email);
    }
    if requested & STATUS_INFO_MISC != 0 {
        writer.write_u8(0x12);
        writer.write_string_str(&snapshot.motd);
        writer.write_string_str(&snapshot.location);
        writer.write_string_str(&snapshot.url);
        let uptime = snapshot.uptime_secs;
        writer.write_u32_le((uptime >> 32) as u32);
        writer.write_u32_le(uptime as u32);
    }
    if requested & STATUS_INFO_PLAYERS != 0 {
        writer.write_u8(0x20);
        writer.write_u32_le(snapshot.players_online);
        writer.write_u32_le(snapshot.players_max);
        writer.write_u32_le(snapshot.players_peak);
    }
    if requested & STATUS_INFO_MAP != 0 {
        writer.write_u8(0x30);
        writer.write_string_str(&snapshot.map_name);
        writer.write_string_str(&snapshot.map_author);
        writer.write_u16_le(snapshot.map_width);
        writer.write_u16_le(snapshot.map_height);
    }
    if requested & STATUS_INFO_PLAYERS_EXT != 0 {
        writer.write_u8(0x21);
        writer.write_u32_le(snapshot.players_online);
        for (name, level) in &snapshot.players {
            writer.write_string_str(name);
            writer.write_u32_le((*level).into());
        }
    }
    if requested & STATUS_INFO_PLAYER_STATUS != 0 {
        writer.write_u8(0x22);
        let online = player_name.map_or(false, |name| {
            snapshot
                .players
                .iter()
                .any(|(player, _)| player.eq_ignore_ascii_case(name))
        });
        writer.write_u8(u8::from(online));
    }
    if requested & STATUS_INFO_SOFTWARE != 0 {
        writer.write_u8(0x23);
        writer.write_string_str(&snapshot.software_name);
        writer.write_string_str(&snapshot.software_version);
        writer.write_string_str(&snapshot.client_version);
    }
}

fn build_status_xml(snapshot: &StatusSnapshot) -> String {
    let mut xml = String::new();
    let _ = write!(xml, "<?xml version=\"1.0\"?>\n");
    let _ = write!(xml, "<tsqp version=\"1.0\">");
    let _ = write!(
        xml,
        "<serverinfo uptime=\"{}\" ip=\"{}\" servername=\"{}\" port=\"{}\" location=\"{}\" url=\"{}\" server=\"{}\" version=\"{}\" client=\"{}\"/>",
        snapshot.uptime_secs,
        escape_xml(&snapshot.ip),
        escape_xml(&snapshot.server_name),
        escape_xml(&snapshot.port),
        escape_xml(&snapshot.location),
        escape_xml(&snapshot.url),
        escape_xml(&snapshot.software_name),
        escape_xml(&snapshot.software_version),
        escape_xml(&snapshot.client_version)
    );
    let _ = write!(
        xml,
        "<owner name=\"{}\" email=\"{}\"/>",
        escape_xml(&snapshot.owner_name),
        escape_xml(&snapshot.owner_email)
    );
    let _ = write!(
        xml,
        "<players online=\"{}\" max=\"{}\" peak=\"{}\"/>",
        snapshot.players_online, snapshot.players_max, snapshot.players_peak
    );
    let _ = write!(
        xml,
        "<map name=\"{}\" author=\"{}\" width=\"{}\" height=\"{}\"/>",
        escape_xml(&snapshot.map_name),
        escape_xml(&snapshot.map_author),
        snapshot.map_width,
        snapshot.map_height
    );
    let _ = write!(xml, "<motd>{}</motd>", escape_xml(&snapshot.motd));
    let _ = write!(xml, "</tsqp>");
    xml
}

fn escape_xml(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn spawn_autosave_loop(
    config: &GameServerConfig,
    world: Arc<Mutex<WorldState>>,
    control: Arc<ServerControl>,
) -> Option<thread::JoinHandle<()>> {
    let root = config.root.clone()?;
    let interval = config.autosave_interval_seconds.max(1);
    if config.autosave_interval_seconds == 0 {
        return None;
    }
    let store = SaveStore::from_root(&root);
    logging::log_game(&format!(
        "autosave enabled: interval={}s",
        interval
    ));
    println!("tibia: autosave enabled (interval={}s)", interval);
    Some(thread::spawn(move || {
        let mut state = crate::persistence::autosave::AutosaveState::new(
            crate::persistence::autosave::AutosaveConfig {
                interval_seconds: interval,
            },
            Instant::now(),
        );
        while control.is_running() {
            let now = Instant::now();
            if state.due(now) {
                let report = match world.lock() {
                    Ok(world) => autosave_world(&world, &store, &root),
                    Err(_) => {
                        logging::log_error("autosave failed (world lock poisoned)");
                        eprintln!("tibia: autosave failed (world lock poisoned)");
                        state.mark_saved(now);
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                };
                for err in report.player_errors {
                    logging::log_error(&format!("autosave player error: {}", err));
                    eprintln!("tibia: autosave player error: {}", err);
                }
                if let Some(err) = report.house_owner_error {
                    logging::log_houses(&format!("autosave house owners error: {}", err));
                    eprintln!("tibia: autosave house owners error: {}", err);
                }
                logging::log_game(&format!(
                    "autosave completed (players: {})",
                    report.saved_players
                ));
                println!(
                    "tibia: autosave completed (players: {})",
                    report.saved_players
                );
                state.mark_saved(now);
            }
            thread::sleep(Duration::from_millis(250));
        }
    }))
}

fn spawn_world_tick_loop(
    state: Arc<GameServerState>,
    world: Arc<Mutex<WorldState>>,
    control: Arc<ServerControl>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let tick_length = state.clock_tick_length();
        while control.is_running() {
            let clock = state.tick_clock();
            let tick = clock.now().0;
            if state.claim_global_world_tick(tick) {
                if let Ok(mut world_guard) = world.lock() {
                    let condition_ticks = world_guard.tick_conditions(clock.now());
                    let mut status_updates = world_guard.tick_status_effects(clock.now());
                    let skill_outcome = world_guard.tick_skill_timers(&clock);
                    status_updates
                        .outfit_updates
                        .extend(skill_outcome.status_updates.outfit_updates.clone());
                    status_updates
                        .speed_updates
                        .extend(skill_outcome.status_updates.speed_updates.clone());
                    status_updates
                        .light_updates
                        .extend(skill_outcome.status_updates.light_updates.clone());
                    let _ = world_guard.tick_raids(clock.now(), &clock);
                    let _ = world_guard.tick_monster_homes(&clock);
                    world_guard.tick_map_refresh(&clock);
                    world_guard.tick_houses();
                    let mut creature_stacks = snapshot_creature_stacks(&world_guard);
                    let npc_steps = world_guard.tick_npcs(&clock);
                    let monster_outcome = world_guard.tick_monsters(&clock);
                    let npc_moves = apply_creature_steps(&world_guard, &npc_steps, &mut creature_stacks);
                    let monster_moves = apply_creature_steps(
                        &world_guard,
                        &monster_outcome.moves,
                        &mut creature_stacks,
                    );
                    world_guard.tick_cron_system(&clock);
                    state.store_global_tick_replay(
                        tick,
                        &condition_ticks,
                        &status_updates,
                        &skill_outcome,
                        &npc_moves,
                        &monster_moves,
                        &monster_outcome,
                    );
                }
            }
            thread::sleep(tick_length / 2);
        }
    })
}

#[derive(Debug)]
pub(crate) struct LoginServerState {
    active_logins: AtomicUsize,
    accounts: Option<Arc<AccountRegistry>>,
    bans: Option<Arc<BanList>>,
}

#[derive(Debug)]
struct GlobalClockState {
    clock: GameClock,
    last_tick: Instant,
}

#[derive(Debug, Default)]
struct CreatureStackCache {
    tick: u64,
    stacks: Arc<HashMap<Position, Vec<u32>>>,
}

#[derive(Debug)]
enum CreatureStacksSnapshot {
    Shared(Arc<HashMap<Position, Vec<u32>>>),
    Owned(HashMap<Position, Vec<u32>>),
}

impl CreatureStacksSnapshot {
    fn as_mut_map(&mut self) -> &mut HashMap<Position, Vec<u32>> {
        if let Self::Shared(shared) = self {
            *self = Self::Owned((**shared).clone());
        }
        match self {
            Self::Owned(owned) => owned,
            Self::Shared(_) => unreachable!("shared snapshot converted to owned"),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct GlobalTickReplayCache {
    tick: u64,
    condition_ticks: Vec<(PlayerId, Vec<ConditionTick>)>,
    status_updates: crate::world::state::CreatureStatusUpdates,
    skill_outcome: crate::world::state::SkillTimerOutcome,
    npc_moves: Vec<CreatureMove>,
    monster_moves: Vec<CreatureMove>,
    monster_outcome: MonsterTickOutcome,
}

#[derive(Debug, Default)]
struct GlobalTickReplayHistory {
    entries: VecDeque<Arc<GlobalTickReplayCache>>,
}

impl GlobalTickReplayHistory {
    fn push(&mut self, replay: GlobalTickReplayCache) {
        self.entries.push_back(Arc::new(replay));
        while self.entries.len() > GLOBAL_REPLAY_HISTORY_TICKS {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Default)]
struct ReplayBatch {
    replays: Vec<Arc<GlobalTickReplayCache>>,
    gap: bool,
}

#[derive(Debug)]
pub(crate) struct GameServerState {
    next_player_id: AtomicUsize,
    clock: Mutex<GlobalClockState>,
    last_global_world_tick: AtomicU64,
    creature_stack_cache: Mutex<CreatureStackCache>,
    global_tick_replay: RwLock<GlobalTickReplayHistory>,
}

impl GameServerState {
    pub(crate) fn new() -> Self {
        Self {
            next_player_id: AtomicUsize::new(1),
            clock: Mutex::new(GlobalClockState {
                clock: GameClock::new(Duration::from_millis(100)),
                last_tick: Instant::now(),
            }),
            last_global_world_tick: AtomicU64::new(u64::MAX),
            creature_stack_cache: Mutex::new(CreatureStackCache {
                tick: u64::MAX,
                stacks: Arc::new(HashMap::new()),
            }),
            global_tick_replay: RwLock::new(GlobalTickReplayHistory::default()),
        }
    }

    fn tick_clock(&self) -> GameClock {
        let mut state = self.clock.lock().expect("clock lock");
        let tick_nanos = state.clock.tick_length().as_nanos().max(1);
        let elapsed = state.last_tick.elapsed();
        let ticks = (elapsed.as_nanos() / tick_nanos) as u64;
        if ticks > 0 {
            state.clock.advance(ticks);
            state.last_tick = state.last_tick + state.clock.duration_for_ticks(ticks);
        }
        state.clock.clone()
    }

    fn clock_tick_length(&self) -> Duration {
        self.clock
            .lock()
            .map(|state| state.clock.tick_length())
            .unwrap_or_else(|_| Duration::from_millis(100))
    }

    fn current_clock(&self) -> GameClock {
        self.clock
            .lock()
            .map(|state| state.clock.clone())
            .unwrap_or_else(|_| GameClock::new(Duration::from_millis(100)))
    }

    fn claim_global_world_tick(&self, tick: u64) -> bool {
        self.last_global_world_tick.swap(tick, Ordering::AcqRel) != tick
    }

    fn creature_stacks_for_tick(
        &self,
        tick: u64,
        world: &WorldState,
    ) -> Arc<HashMap<Position, Vec<u32>>> {
        let mut cache = self
            .creature_stack_cache
            .lock()
            .expect("creature stack cache lock");
        if cache.tick != tick {
            cache.tick = tick;
            cache.stacks = Arc::new(snapshot_creature_stacks(world));
        }
        Arc::clone(&cache.stacks)
    }

    fn store_global_tick_replay(
        &self,
        tick: u64,
        condition_ticks: &[(PlayerId, Vec<ConditionTick>)],
        status_updates: &crate::world::state::CreatureStatusUpdates,
        skill_outcome: &crate::world::state::SkillTimerOutcome,
        npc_moves: &[CreatureMove],
        monster_moves: &[CreatureMove],
        monster_outcome: &MonsterTickOutcome,
    ) {
        let replay = GlobalTickReplayCache {
            tick,
            condition_ticks: condition_ticks.to_vec(),
            status_updates: status_updates.clone(),
            skill_outcome: skill_outcome.clone(),
            npc_moves: npc_moves.to_vec(),
            monster_moves: monster_moves.to_vec(),
            monster_outcome: monster_outcome.clone(),
        };
        let mut guard = self
            .global_tick_replay
            .write()
            .expect("global tick replay lock");
        guard.push(replay);
    }

    fn global_tick_replays_after(&self, last_applied: Option<u64>, upto_tick: u64) -> ReplayBatch {
        let history = self
            .global_tick_replay
            .read()
            .expect("global tick replay lock");
        let mut batch = ReplayBatch::default();
        if history.entries.is_empty() {
            return batch;
        }
        let first_tick = history.entries.front().map(|entry| entry.tick).unwrap_or(u64::MAX);
        let start_tick = match last_applied {
            Some(last) => {
                let next = last.saturating_add(1);
                if next < first_tick {
                    batch.gap = true;
                    first_tick
                } else {
                    next
                }
            }
            None => first_tick,
        };
        batch.replays = history
            .entries
            .iter()
            .filter(|entry| entry.tick >= start_tick && entry.tick <= upto_tick)
            .cloned()
            .collect();
        batch
    }
}

trait PacketTransport {
    fn peer_addr(&self) -> Option<SocketAddr>;
    fn set_nonblocking(&mut self, nonblocking: bool) -> Result<(), String>;
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String>;
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String>;
    fn read_packet(
        &mut self,
        max_len: usize,
        trace: Option<&mut PacketTrace>,
    ) -> Result<ReadPacketOutcome, String>;
    fn write_packet(&mut self, body: &[u8], trace: Option<&mut PacketTrace>) -> Result<(), String>;
}

struct TcpPacketTransport {
    stream: TcpStream,
}

impl TcpPacketTransport {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl PacketTransport for TcpPacketTransport {
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> Result<(), String> {
        self.stream
            .set_nonblocking(nonblocking)
            .map_err(|err| format!("stream nonblocking set failed: {err}"))
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(|err| format!("read timeout set failed: {err}"))
    }

    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.stream
            .set_write_timeout(timeout)
            .map_err(|err| format!("write timeout set failed: {err}"))
    }

    fn read_packet(
        &mut self,
        max_len: usize,
        trace: Option<&mut PacketTrace>,
    ) -> Result<ReadPacketOutcome, String> {
        read_packet(&mut self.stream, max_len, trace)
    }

    fn write_packet(&mut self, body: &[u8], trace: Option<&mut PacketTrace>) -> Result<(), String> {
        write_packet(&mut self.stream, body, trace)
            .map_err(|err| format!("write packet failed: {err}"))
    }
}

struct WsPacketTransport {
    stream: TcpStream,
    recv_buffer: Vec<u8>,
    rate_limiter: WsRateLimiter,
}

impl WsPacketTransport {
    fn accept(mut stream: TcpStream, config: &ws::WsHandshakeConfig) -> Result<Self, String> {
        ws::accept_handshake(&mut stream, config)?;
        Ok(Self {
            stream,
            recv_buffer: Vec::new(),
            rate_limiter: WsRateLimiter::new(WS_RATE_LIMIT_PACKETS, WS_RATE_LIMIT_WINDOW),
        })
    }

    fn try_take_packet(
        &mut self,
        max_len: usize,
        trace: Option<&mut PacketTrace>,
    ) -> Result<Option<Vec<u8>>, String> {
        if self.recv_buffer.len() < 2 {
            return Ok(None);
        }
        let len = u16::from_le_bytes([self.recv_buffer[0], self.recv_buffer[1]]) as usize;
        if len == 0 {
            return Err("packet length is zero".to_string());
        }
        if len > max_len {
            return Err(format!("packet length {} exceeds max {}", len, max_len));
        }
        let total = 2 + len;
        if self.recv_buffer.len() < total {
            return Ok(None);
        }
        self.rate_limiter.check()?;
        let payload = self.recv_buffer[2..total].to_vec();
        self.recv_buffer.drain(..total);
        if let Some(trace) = trace {
            trace.record("in", &payload);
        }
        Ok(Some(payload))
    }
}

struct WsRateLimiter {
    window_start: Instant,
    window: Duration,
    max_packets: usize,
    packets: usize,
}

impl WsRateLimiter {
    fn new(max_packets: usize, window: Duration) -> Self {
        Self {
            window_start: Instant::now(),
            window,
            max_packets,
            packets: 0,
        }
    }

    fn check(&mut self) -> Result<(), String> {
        let now = Instant::now();
        if now.duration_since(self.window_start) >= self.window {
            self.window_start = now;
            self.packets = 0;
        }
        if self.packets >= self.max_packets {
            return Err(format!(
                "websocket rate limit exceeded ({} packets per {:?})",
                self.max_packets, self.window
            ));
        }
        self.packets += 1;
        Ok(())
    }
}

impl PacketTransport for WsPacketTransport {
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> Result<(), String> {
        self.stream
            .set_nonblocking(nonblocking)
            .map_err(|err| format!("stream nonblocking set failed: {err}"))
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(|err| format!("read timeout set failed: {err}"))
    }

    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.stream
            .set_write_timeout(timeout)
            .map_err(|err| format!("write timeout set failed: {err}"))
    }

    fn read_packet(
        &mut self,
        max_len: usize,
        trace: Option<&mut PacketTrace>,
    ) -> Result<ReadPacketOutcome, String> {
        let mut trace = trace;
        if let Some(packet) = self.try_take_packet(max_len, trace.as_deref_mut())? {
            return Ok(ReadPacketOutcome::Packet(packet));
        }
        let max_payload = max_len.saturating_add(2);
        loop {
            match ws::read_frame(&mut self.stream, max_payload) {
                Ok(frame) => match frame.opcode {
                    0x2 | 0x1 => {
                        if !frame.payload.is_empty() {
                            self.recv_buffer.extend_from_slice(&frame.payload);
                        }
                        if let Some(packet) = self.try_take_packet(max_len, trace.as_deref_mut())? {
                            return Ok(ReadPacketOutcome::Packet(packet));
                        }
                    }
                    0x8 => return Err("websocket closed".to_string()),
                    0x9 => {
                        ws::write_frame(&mut self.stream, 0xA, &frame.payload)?;
                    }
                    0xA => {}
                    _ => {}
                },
                Err(ws::WsFrameError::Timeout) => return Ok(ReadPacketOutcome::Timeout),
                Err(ws::WsFrameError::Closed) => return Err("websocket closed".to_string()),
                Err(ws::WsFrameError::Io(err)) => {
                    return Err(format!("websocket read failed: {err}"));
                }
                Err(ws::WsFrameError::Protocol(err)) => {
                    return Err(format!("websocket protocol error: {err}"));
                }
            }
        }
    }

    fn write_packet(&mut self, body: &[u8], trace: Option<&mut PacketTrace>) -> Result<(), String> {
        let len_u16 = u16::try_from(body.len()).map_err(|_| "packet too large".to_string())?;
        let mut framed = Vec::with_capacity(2 + body.len());
        framed.push((len_u16 & 0xff) as u8);
        framed.push((len_u16 >> 8) as u8);
        framed.extend_from_slice(body);
        ws::write_frame(&mut self.stream, 0x2, &framed)?;
        if let Some(trace) = trace {
            trace.record("out", body);
        }
        Ok(())
    }
}

struct GamePlayerGuard {
    world: Arc<Mutex<WorldState>>,
    player_id: PlayerId,
}

impl GamePlayerGuard {
    fn new(world: Arc<Mutex<WorldState>>, player_id: PlayerId) -> Self {
        Self { world, player_id }
    }
}

impl Drop for GamePlayerGuard {
    fn drop(&mut self) {
        if let Ok(mut world) = self.world.lock() {
            world.handle_disconnect(self.player_id);
        }
    }
}

struct ActiveLoginGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> ActiveLoginGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for ActiveLoginGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy)]
enum LoginResponseMode {
    LegacySuccess,
    CharacterList,
}

fn handle_login_session<T: PacketTransport>(
    transport: &mut T,
    config: &LoginServerConfig,
    state: &LoginServerState,
    response_mode: LoginResponseMode,
) -> Result<(), String> {
    let peer = transport.peer_addr();
    let mut trace = PacketTrace::new(config.root.as_ref(), "login", peer);
    let _guard = ActiveLoginGuard::new(&state.active_logins);

    transport
        .set_nonblocking(false)
        .map_err(|err| format!("login stream nonblocking reset failed: {}", err))?;
    transport
        .set_read_timeout(Some(config.read_timeout))
        .map_err(|err| format!("read timeout set failed: {}", err))?;
    transport
        .set_write_timeout(Some(config.write_timeout))
        .map_err(|err| format!("write timeout set failed: {}", err))?;

    let payload = match transport.read_packet(config.max_packet, trace.as_mut()) {
        Ok(ReadPacketOutcome::Packet(payload)) => payload,
        Ok(ReadPacketOutcome::Timeout) => {
            return Err("read packet failed: timeout".to_string());
        }
        Err(err) => {
            return Err(format!("read packet failed: {}", err));
        }
    };

    if let Some(waitlist) = config.waitlist.as_ref() {
        let active = state.active_logins.load(Ordering::SeqCst);
        if let Some(response) = waitlist_response(waitlist, active) {
            send_response(transport, &response, trace.as_mut())?;
            return Ok(());
        }
    }

    let decision = match handle_login_packet_v1(&payload, &config.flow) {
        Ok(decision) => decision,
        Err(err) => {
            logging::log_error(&err);
            eprintln!("{}", err);
            let response = LoginErrorKind::CorruptData.to_response();
            send_response(transport, &response, trace.as_mut())?;
            if let Some(peer) = peer {
                println!("tibia: login rejected (corrupt data) from {}", peer);
                logging::log_error(&format!(
                    "login rejected (corrupt data) from {}",
                    peer
                ));
            }
            return Ok(());
        }
    };

    let world = resolve_world_endpoint(config, peer);

    match decision {
        LoginDecision::NeedsRegistration(payload) => {
            if let Some(peer) = peer {
                println!(
                    "tibia: login attempt from {} account='{}'",
                    peer, payload.account
                );
            }
            if let Some(bans) = state.bans.as_ref() {
                if bans.is_banned(&payload.account, std::time::SystemTime::now()) {
                    let response = LoginErrorKind::AccountBanned.to_response();
                    send_response(transport, &response, trace.as_mut())?;
                    if let Some(peer) = peer {
                        println!("tibia: login banned for {} account='{}'", peer, payload.account);
                    }
                    return Ok(());
                }
            }

            let (characters, selection_name, premium_days, selection_is_gm, selection_is_test_god) =
                if let Some(accounts) = state.accounts.as_ref() {
                    match accounts.verify(&payload.account, &payload.password) {
                        Some(record) => {
                            let list = build_character_list(
                                config.root.as_ref(),
                                &record.player_ids,
                                &payload.account,
                                &world,
                            );
                            let premium_days = if record.premium { 30 } else { 0 };
                            (
                                list,
                                payload.account.clone(),
                                premium_days,
                                record.gamemaster || record.test_god,
                                record.test_god,
                            )
                        }
                        None => {
                            let response = LoginErrorKind::AccountNotAssigned.to_response();
                            send_response(transport, &response, trace.as_mut())?;
                            if let Some(peer) = peer {
                                println!(
                                    "tibia: login rejected (bad credentials) from {} account='{}'",
                                    peer, payload.account
                                );
                            }
                            return Ok(());
                        }
                    }
                } else {
                    let player_id = resolve_player_id(&payload, config.root.as_ref());
                    let list = build_character_list(
                        config.root.as_ref(),
                        &[player_id],
                        &payload.account,
                        &world,
                    );
                    let is_test_god = payload.account.trim().eq_ignore_ascii_case("test_god")
                        && payload.password == "test_god";
                    (
                        list,
                        payload.account.clone(),
                        config.premium_days,
                        is_test_god,
                        is_test_god,
                    )
                };

            if let (Some(registry), Some(peer_ip)) =
                (config.login_registry.as_ref(), peer.map(|addr| addr.ip()))
            {
                let selection = LoginSelection {
                    account: selection_name,
                    premium: premium_days > 0,
                    is_gm: selection_is_gm,
                    is_test_god: selection_is_test_god,
                    characters: characters
                        .iter()
                        .map(|entry| LoginCharacterSelection {
                            player_id: PlayerId(entry.player_id),
                            name: entry.name.clone(),
                        })
                        .collect(),
                };
                registry.insert(peer_ip, selection);
            }

            match response_mode {
                LoginResponseMode::LegacySuccess => {
                    let player_id = characters
                        .first()
                        .map(|entry| entry.player_id)
                        .unwrap_or(0);
                    let success = LoginSuccessV1 {
                        client_type: payload.client_type,
                        client_version: payload.client_version,
                        player_id: u64::from(player_id),
                    };
                    let body = build_login_success_v1(&success);
                    transport
                        .write_packet(&body, trace.as_mut())
                        .map_err(|err| format!("send login success failed: {}", err))?;
                }
                LoginResponseMode::CharacterList => {
                    let body =
                        crate::net::login::build_login_character_list(&characters, premium_days);
                    transport
                        .write_packet(&body, trace.as_mut())
                        .map_err(|err| format!("send login list failed: {}", err))?;
                }
            }

            if let Some(peer) = peer {
                println!(
                    "tibia: login success for {} account='{}' characters={}",
                    peer,
                    payload.account,
                    characters.len()
                );
            }
        }
        LoginDecision::Error(response) => {
            send_response(transport, &response, trace.as_mut())?;
            if let Some(peer) = peer {
                println!(
                    "tibia: login rejected ({}) from {}",
                    response.message, peer
                );
            }
        }
    }

    Ok(())
}

fn handle_login_connection(
    stream: TcpStream,
    config: &LoginServerConfig,
    state: &LoginServerState,
) -> Result<(), String> {
    let mut transport = TcpPacketTransport::new(stream);
    handle_login_session(
        &mut transport,
        config,
        state,
        LoginResponseMode::LegacySuccess,
    )
}

fn handle_login_ws_connection(
    stream: TcpStream,
    config: &LoginServerConfig,
    ws_config: &ws::WsHandshakeConfig,
    state: &LoginServerState,
) -> Result<(), String> {
    let mut transport = WsPacketTransport::accept(stream, ws_config)?;
    handle_login_session(
        &mut transport,
        config,
        state,
        LoginResponseMode::CharacterList,
    )
}

fn handle_game_connection(
    stream: TcpStream,
    config: &GameServerConfig,
    state: &GameServerState,
    world: &Arc<Mutex<WorldState>>,
    control: &Arc<ServerControl>,
) -> Result<(), String> {
    let mut transport = TcpPacketTransport::new(stream);
    handle_game_session(
        &mut transport,
        config,
        state,
        world,
        control,
        "game",
    )
}

fn handle_game_ws_connection(
    stream: TcpStream,
    config: &GameServerConfig,
    ws_config: &ws::WsHandshakeConfig,
    state: &GameServerState,
    world: &Arc<Mutex<WorldState>>,
    control: &Arc<ServerControl>,
) -> Result<(), String> {
    let mut transport = WsPacketTransport::accept(stream, ws_config)?;
    handle_game_session(
        &mut transport,
        config,
        state,
        world,
        control,
        "game_ws",
    )
}

fn handle_game_session<T: PacketTransport>(
    transport: &mut T,
    config: &GameServerConfig,
    state: &GameServerState,
    world: &Arc<Mutex<WorldState>>,
    control: &Arc<ServerControl>,
    trace_kind: &str,
) -> Result<(), String> {
    let mut trace = PacketTrace::new(config.root.as_ref(), trace_kind, transport.peer_addr());
    transport
        .set_nonblocking(false)
        .map_err(|err| format!("game stream nonblocking reset failed: {}", err))?;
    transport
        .set_read_timeout(Some(config.read_timeout))
        .map_err(|err| format!("read timeout set failed: {}", err))?;
    transport
        .set_write_timeout(Some(config.write_timeout))
        .map_err(|err| format!("write timeout set failed: {}", err))?;

    let peer_ip = transport.peer_addr().map(|addr| addr.ip());

    let mut queued_payload: Option<Vec<u8>> = None;
    let mut login_info: Option<GameLogin> = None;
    let payload = match transport
        .read_packet(config.max_packet, trace.as_mut())
        .map_err(|err| format!("read initial packet failed: {}", err))?
    {
        ReadPacketOutcome::Packet(payload) => payload,
        ReadPacketOutcome::Timeout => return Err("read initial packet timed out".to_string()),
    };
    match parse_game_login(&payload) {
        Ok(login) => {
            login_info = Some(login);
        }
        Err(_) => {
            queued_payload = Some(payload);
        }
    }

    let (mut player_id, player_name, player_premium, player_is_gm, player_is_test_god) = match login_info.as_ref() {
        Some(login) => select_player_from_login(config, state, peer_ip, login)?,
        None => select_connection_player(config, state, peer_ip),
    };

    {
        let mut world = world
            .lock()
            .map_err(|_| "world lock poisoned".to_string())?;
        player_id = spawn_connection_player(
            &mut world,
            player_id,
            player_name,
            player_premium,
            player_is_gm,
            player_is_test_god,
            config.root.as_ref(),
        )?;
        world.queue_buddy_status_update(player_id, true);
    }
    let _guard = GamePlayerGuard::new(Arc::clone(world), player_id);

    let tick_length = state.clock_tick_length();
    transport
        .set_read_timeout(Some(tick_length))
        .map_err(|err| format!("read timeout set failed: {}", err))?;
    let mut last_activity = Instant::now();
    let mut idle_warning_sent = false;
    let mut sent_init_packets = false;
    let mut request_queue_open = false;
    let mut last_ping = Instant::now();
    let mut last_player_state: Option<u8> = None;
    let mut last_attack_target: Option<CreatureId> = None;
    let mut last_player_data: Option<PlayerDataSnapshot> = None;
    let mut last_applied_global_tick: Option<u64> = None;

    {
        let world_guard = world
            .lock()
            .map_err(|_| "world lock poisoned".to_string())?;
        if let Some(player) = world_guard.players.get(&player_id) {
            let has_rights = player.is_gm;
            let mut writer = PacketWriter::new();
            game::write_init_game(&mut writer, player_id.0, has_rights);
            transport
                .write_packet(&writer.into_vec(), trace.as_mut())
                .map_err(|err| format!("send init game failed: {}", err))?;
            let mut writer = PacketWriter::new();
            game::write_rights(&mut writer, if has_rights { 1 } else { 0 });
            transport
                .write_packet(&writer.into_vec(), trace.as_mut())
                .map_err(|err| format!("send rights failed: {}", err))?;
            let mut writer = PacketWriter::new();
            game::write_world_light(
                &mut writer,
                DEFAULT_WORLD_LIGHT_LEVEL,
                DEFAULT_WORLD_LIGHT_COLOR,
            );
            transport
                .write_packet(&writer.into_vec(), trace.as_mut())
                .map_err(|err| format!("send world light failed: {}", err))?;
            let mut writer = PacketWriter::new();
            game::write_map_description(&mut writer, &world_guard, player.position, player_id);
            transport
                .write_packet(&writer.into_vec(), trace.as_mut())
                .map_err(|err| format!("send map description failed: {}", err))?;
            let mut writer = PacketWriter::new();
            let capacity = world_guard.player_capacity_remaining(player);
            game::write_player_data(&mut writer, player, capacity);
            transport
                .write_packet(&writer.into_vec(), trace.as_mut())
                .map_err(|err| format!("send player data failed: {}", err))?;
            last_player_data = Some(snapshot_player_data(player, capacity));
            let mut writer = PacketWriter::new();
            game::write_player_skills(&mut writer, player);
            transport
                .write_packet(&writer.into_vec(), trace.as_mut())
                .map_err(|err| format!("send player skills failed: {}", err))?;
            for packet in build_inventory_snapshot_packets(player, world_guard.item_types.as_ref()) {
                transport
                    .write_packet(&packet, trace.as_mut())
                    .map_err(|err| format!("send inventory failed: {}", err))?;
            }
            for packet in build_buddy_list_packets(&world_guard, player_id) {
                transport
                    .write_packet(&packet, trace.as_mut())
                    .map_err(|err| format!("send buddy list failed: {}", err))?;
            }
            println!(
                "tibia: monsters active for init: {}",
                world_guard.monsters.len()
            );
            println!(
                "tibia: sent init packets for player {} (init/rights/map)",
                player_id.0
            );
            sent_init_packets = true;
        }
    }

    loop {
        if !control.is_running() {
            return Ok(());
        }
        let idle_elapsed = last_activity.elapsed();
        if let Some(warn_after) = config.idle_warning_after {
            if !idle_warning_sent
                && warn_after < config.read_timeout
                && idle_elapsed >= warn_after
            {
                let minutes = (idle_elapsed.as_secs() / 60).max(1);
                let message = format!(
                    "You have been idle for {minutes} minutes. You will be disconnected in one minute if you are still idle then."
                );
                let mut writer = PacketWriter::new();
                game::write_message(&mut writer, 0x14, &message);
                transport
                    .write_packet(&writer.into_vec(), trace.as_mut())
                    .map_err(|err| format!("send idle warning failed: {}", err))?;
                idle_warning_sent = true;
            }
        }
        if idle_elapsed >= config.read_timeout {
            return Err("read packet failed: idle timeout".to_string());
        }

        let payload = match queued_payload.take() {
            Some(payload) => {
                last_activity = Instant::now();
                idle_warning_sent = false;
                Some(payload)
            }
            None => match transport.read_packet(config.max_packet, trace.as_mut()) {
                Ok(ReadPacketOutcome::Packet(payload)) => {
                    last_activity = Instant::now();
                    idle_warning_sent = false;
                    Some(payload)
                }
                Ok(ReadPacketOutcome::Timeout) => None,
                Err(err) => return Err(format!("read packet failed: {}", err)),
            },
        };

        let clock = state.current_clock();
        let current_tick = clock.now().0;
        let replay_batch = state.global_tick_replays_after(last_applied_global_tick, current_tick);

        if let Some(payload) = payload.as_ref() {
            if let Some(opcode) = payload.first().copied() {
                println!(
                    "tibia: game packet received ({} bytes, opcode 0x{opcode:02x})",
                    payload.len()
                );
            } else {
                println!("tibia: game packet received (empty)");
            }
        }

        let mut admin_action: Option<AdminOutcome> = None;
        let mut disconnect_after_send = false;
        let mut logout_requested = false;
        let mut responses = {
            let mut world_guard = world
                .lock()
                .map_err(|_| "world lock poisoned".to_string())?;
            let old_position = world_guard.players.get(&player_id).map(|player| player.position);
            let mut condition_ticks: Vec<(PlayerId, Vec<ConditionTick>)> = Vec::new();
            let mut status_updates = crate::world::state::CreatureStatusUpdates::default();
            let mut skill_outcome = crate::world::state::SkillTimerOutcome::default();
            let mut npc_moves: Vec<CreatureMove> = Vec::new();
            let mut monster_moves: Vec<CreatureMove> = Vec::new();
            let mut monster_combat_packets: Vec<Vec<u8>> = Vec::new();
            let mut monster_refresh_map = false;
            // Stack positions in move packets must be based on the client's current view.
            // Snapshot before AI movement ticks and apply all steps against this snapshot.
            let mut creature_stacks: Option<CreatureStacksSnapshot> = Some(
                CreatureStacksSnapshot::Shared(state.creature_stacks_for_tick(
                    current_tick,
                    &world_guard,
                )),
            );
            for replay in &replay_batch.replays {
                condition_ticks.extend(replay.condition_ticks.clone());
                status_updates
                    .outfit_updates
                    .extend(replay.status_updates.outfit_updates.clone());
                status_updates
                    .speed_updates
                    .extend(replay.status_updates.speed_updates.clone());
                status_updates
                    .light_updates
                    .extend(replay.status_updates.light_updates.clone());
                if replay.skill_outcome.data_updates.contains(&player_id)
                    && !skill_outcome.data_updates.contains(&player_id)
                {
                    skill_outcome.data_updates.push(player_id);
                }
                if replay.skill_outcome.health_updates.contains(&player_id)
                    && !skill_outcome.health_updates.contains(&player_id)
                {
                    skill_outcome.health_updates.push(player_id);
                }
                monster_combat_packets.extend(build_monster_combat_packets(
                    &replay.monster_outcome,
                    &world_guard,
                    player_id,
                ));
                monster_refresh_map |= replay.monster_outcome.refresh_map;
                npc_moves.extend(replay.npc_moves.clone());
                monster_moves.extend(replay.monster_moves.clone());
                last_applied_global_tick = Some(replay.tick);
            }
            let mut packets = build_condition_tick_packets(&world_guard, &condition_ticks);
            if replay_batch.gap {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let mut writer = PacketWriter::new();
                    game::write_map_description(&mut writer, &world_guard, player.position, player_id);
                    packets.push(writer.into_vec());
                }
                if replay_batch.replays.is_empty() {
                    last_applied_global_tick = Some(current_tick);
                }
            }
            packets.extend(build_status_update_packets(&status_updates));
            packets.extend(monster_combat_packets);
            if skill_outcome.data_updates.contains(&player_id) {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let mut writer = PacketWriter::new();
                    let capacity = world_guard.player_capacity_remaining(player);
                    game::write_player_data(&mut writer, player, capacity);
                    packets.push(writer.into_vec());
                    last_player_data = Some(snapshot_player_data(player, capacity));
                }
            }
            if world_guard.take_pending_data_update(player_id) {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let mut writer = PacketWriter::new();
                    let capacity = world_guard.player_capacity_remaining(player);
                    game::write_player_data(&mut writer, player, capacity);
                    packets.push(writer.into_vec());
                    last_player_data = Some(snapshot_player_data(player, capacity));
                }
            }
            if skill_outcome.health_updates.contains(&player_id) {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let percent = health_percent(player.stats.health, player.stats.max_health);
                    packets.push(creature_health_packet(player_id.0, percent));
                }
            }
            if world_guard.take_pending_skill_update(player_id) {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let mut writer = PacketWriter::new();
                    game::write_player_skills(&mut writer, player);
                    packets.push(writer.into_vec());
                }
            }
            if monster_refresh_map {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let mut writer = PacketWriter::new();
                    game::write_map_description(&mut writer, &world_guard, player.position, player_id);
                    packets.push(writer.into_vec());
                }
            }
            let decay_positions = world_guard.take_pending_map_refreshes(player_id);
            if !decay_positions.is_empty() {
                if let Some(player) = world_guard.players.get(&player_id) {
                    for position in decay_positions {
                        if !game::position_in_viewport(player.position, position) {
                            continue;
                        }
                        let mut writer = PacketWriter::new();
                        game::write_field_data(&mut writer, &world_guard, position, player_id);
                        packets.push(writer.into_vec());
                    }
                }
            }
            if !sent_init_packets {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let has_rights = player.is_gm;
                    let mut writer = PacketWriter::new();
                    game::write_init_game(&mut writer, player_id.0, has_rights);
                    packets.push(writer.into_vec());
                    let mut writer = PacketWriter::new();
                    game::write_rights(&mut writer, if has_rights { 1 } else { 0 });
                    packets.push(writer.into_vec());
                    let mut writer = PacketWriter::new();
                    game::write_map_description(&mut writer, &world_guard, player.position, player_id);
                    packets.push(writer.into_vec());
                    let mut writer = PacketWriter::new();
                    let capacity = world_guard.player_capacity_remaining(player);
                    game::write_player_data(&mut writer, player, capacity);
                    packets.push(writer.into_vec());
                    last_player_data = Some(snapshot_player_data(player, capacity));
                    let mut writer = PacketWriter::new();
                    game::write_player_skills(&mut writer, player);
                    packets.push(writer.into_vec());
                    packets.extend(build_inventory_snapshot_packets(
                        player,
                        world_guard.item_types.as_ref(),
                    ));
                    packets.extend(build_saved_container_packets(&mut world_guard, player_id));
                    packets.extend(build_buddy_list_packets(&world_guard, player_id));
                }
                sent_init_packets = true;
            }
            if let Some(payload) = payload.as_ref() {
                if !payload.is_empty() {
                    let inventory_before =
                        world_guard.players.get(&player_id).map(snapshot_inventory);
                    match handle_client_packet(&mut world_guard, player_id, payload, &clock) {
                        Ok(ClientPacketOutcome::SpellCast(report)) => {
                            packets.extend(build_spell_cast_responses(&report, &world_guard));
                            if report.refresh_map {
                                if let Some(player) = world_guard.players.get(&player_id) {
                                    let mut writer = PacketWriter::new();
                                    game::write_map_description(
                                        &mut writer,
                                        &world_guard,
                                        player.position,
                                        player_id,
                                    );
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::MoveUse(outcome)) => {
                            packets.extend(build_moveuse_packets(
                                &outcome,
                                &world_guard,
                                player_id,
                            ));
                            if outcome.logout_users.contains(&player_id) {
                                logout_requested = true;
                            }
                        }
                        Ok(ClientPacketOutcome::EditText { text_id, text }) => {
                            if let Err(err) =
                                world_guard.apply_edit_text(player_id, text_id, &text)
                            {
                                logging::log_error(&format!("edit text failed: {}", err));
                                eprintln!("tibia: edit text failed: {}", err);
                            }
                        }
                        Ok(ClientPacketOutcome::EditList {
                            list_type,
                            list_id,
                            text,
                        }) => {
                            if let Err(err) =
                                world_guard.apply_edit_list(player_id, list_type, list_id, &text)
                            {
                                logging::log_error(&format!("edit list failed: {}", err));
                                eprintln!("tibia: edit list failed: {}", err);
                            }
                        }
                        Ok(ClientPacketOutcome::OutfitRequest) => {
                            if let Some(player) = world_guard.players.get(&player_id) {
                                let mut outfit = player.current_outfit;
                                if outfit.look_type == 0 {
                                    outfit = DEFAULT_OUTFIT;
                                }
                                let (legacy_opcode, legacy_base) =
                                    legacy_outfit_dialog_base(player, player.is_gm);
                                let mut legacy_writer = PacketWriter::new();
                                game::write_outfit_dialog_legacy(
                                    &mut legacy_writer,
                                    legacy_opcode,
                                    legacy_base,
                                );
                                packets.push(legacy_writer.into_vec());
                                let mut writer = PacketWriter::new();
                                game::write_outfit_dialog(&mut writer, outfit);
                                packets.push(writer.into_vec());
                            }
                        }
                        Ok(ClientPacketOutcome::OutfitSet { outfit }) => {
                            let mut outfit = outfit;
                            if outfit.look_type == 0 {
                                outfit = DEFAULT_OUTFIT;
                            }
                            world_guard.set_player_outfit(player_id, outfit);
                        }
                        Ok(ClientPacketOutcome::Look(request)) => {
                            let text = build_look_message(&world_guard, player_id, &request)
                                .unwrap_or_else(|| "You see nothing special.".to_string());
                            let mut writer = PacketWriter::new();
                            game::write_message(&mut writer, MESSAGE_LOOK, &text);
                            packets.push(writer.into_vec());
                        }
                        Ok(ClientPacketOutcome::MoveItem {
                            refresh_map,
                            refresh_positions,
                            container_updates,
                        }) => {
                            if refresh_map {
                                if let Some(player) = world_guard.players.get(&player_id) {
                                    let mut sent_field = false;
                                    for position in refresh_positions {
                                        if !game::position_in_viewport(player.position, position) {
                                            continue;
                                        }
                                        let mut writer = PacketWriter::new();
                                        game::write_field_data(
                                            &mut writer,
                                            &world_guard,
                                            position,
                                            player_id,
                                        );
                                        packets.push(writer.into_vec());
                                        sent_field = true;
                                    }
                                    if !sent_field {
                                        let mut writer = PacketWriter::new();
                                        game::write_map_description(
                                            &mut writer,
                                            &world_guard,
                                            player.position,
                                            player_id,
                                        );
                                        packets.push(writer.into_vec());
                                    }
                                }
                            }
                            if !container_updates.is_empty() {
                                packets.extend(build_container_update_packets(
                                    &container_updates,
                                    world_guard.item_types.as_ref(),
                                ));
                            }
                        }
                        Ok(ClientPacketOutcome::RefreshField(position)) => {
                            if let Some(player) = world_guard.players.get(&player_id) {
                                if game::position_in_viewport(player.position, position)
                                    && world_guard.map.tile(position).is_some()
                                {
                                    let mut writer = PacketWriter::new();
                                    game::write_field_data(
                                        &mut writer,
                                        &world_guard,
                                        position,
                                        player_id,
                                    );
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::RefreshContainer(container_id)) => {
                            if let Some(player) = world_guard.players.get(&player_id) {
                                if let Some(container) = player.open_containers.get(&container_id) {
                                    let mut writer = PacketWriter::new();
                                    game::write_open_container(
                                        &mut writer,
                                        container,
                                        world_guard.item_types.as_ref(),
                                    );
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::OpenContainer(open)) => {
                            let mut writer = PacketWriter::new();
                            game::write_open_container(
                                &mut writer,
                                &open,
                                world_guard.item_types.as_ref(),
                            );
                            packets.push(writer.into_vec());
                        }
                        Ok(ClientPacketOutcome::CloseContainer(container_id)) => {
                            let mut writer = PacketWriter::new();
                            game::write_close_container(&mut writer, container_id);
                            packets.push(writer.into_vec());
                        }
                        Ok(ClientPacketOutcome::Logout(outcome)) => match outcome {
                            LogoutRequestOutcome::Allowed => {
                                logout_requested = true;
                            }
                            LogoutRequestOutcome::Blocked(reason) => {
                                let mut writer = PacketWriter::new();
                                game::write_message(
                                    &mut writer,
                                    0x14,
                                    logout_block_message(reason),
                                );
                                packets.push(writer.into_vec());
                            }
                        },
                        Ok(ClientPacketOutcome::Talk(talk)) => {
                            let handled = handle_house_list_command(
                                &talk,
                                player_id,
                                &mut world_guard,
                                &mut packets,
                            )
                            .map_err(|err| format!("house list error: {}", err));
                            let handled = match handled {
                                Ok(true) => Ok(true),
                                Ok(false) => handle_help_channel_request(
                                    &talk,
                                    player_id,
                                    &mut world_guard,
                                    &mut packets,
                                )
                                .map_err(|err| format!("help request error: {}", err)),
                                Err(err) => Err(err),
                            };
                            match handled {
                                Ok(true) => {}
                                Ok(false) => {
                                    let talk_packet =
                                        talk_to_packet(&talk, player_id, &mut world_guard)
                                            .map_err(|err| format!("talk packet error: {}", err));
                                    match talk_packet {
                                        Ok(Some(packet)) => packets.push(packet),
                                        Ok(None) => {}
                                        Err(err) => {
                                            logging::log_error(&err);
                                            eprintln!("{}", err);
                                        }
                                    }
                                    match build_npc_talk_packets(
                                        &talk,
                                        player_id,
                                        &mut world_guard,
                                        &clock,
                                    ) {
                                        Ok(mut npc_packets) => packets.append(&mut npc_packets),
                                        Err(err) => {
                                            logging::log_error(&err);
                                            eprintln!("{}", err);
                                        }
                                    }
                                }
                                Err(err) => {
                                    logging::log_error(&err);
                                    eprintln!("{}", err);
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::ChannelListRequest) => {
                            let channels = world_guard
                                .channel_list_for(player_id)
                                .into_iter()
                                .map(|(id, name)| game::ChannelEntry { id, name })
                                .collect::<Vec<_>>();
                            let mut writer = PacketWriter::new();
                            game::write_channel_list(&mut writer, &channels);
                            packets.push(writer.into_vec());
                        }
                        Ok(ClientPacketOutcome::OpenChannel { channel_id }) => {
                            if let Some(name) = world_guard.channel_name_for(player_id, channel_id) {
                                let mut writer = PacketWriter::new();
                                if world_guard.private_channel_owner(channel_id) == Some(player_id) {
                                    game::write_open_own_channel(&mut writer, channel_id, &name);
                                } else {
                                    game::write_open_channel(&mut writer, channel_id, &name);
                                }
                                packets.push(writer.into_vec());
                            } else {
                                let mut writer = PacketWriter::new();
                                game::write_message(&mut writer, 0x14, "Channel not available.");
                                packets.push(writer.into_vec());
                            }
                        }
                        Ok(ClientPacketOutcome::CloseChannel { channel_id }) => {
                            let mut writer = PacketWriter::new();
                            game::write_close_channel(&mut writer, channel_id);
                            packets.push(writer.into_vec());
                        }
                        Ok(ClientPacketOutcome::OpenPrivateChannel { name }) => {
                            let name = name.trim();
                            if name.is_empty() {
                                let mut writer = PacketWriter::new();
                                game::write_message(
                                    &mut writer,
                                    0x14,
                                    "Private channel requires a player name.",
                                );
                                packets.push(writer.into_vec());
                            } else if world_guard
                                .players
                                .get(&player_id)
                                .map(|player| player.name.eq_ignore_ascii_case(name))
                                .unwrap_or(false)
                            {
                                let mut writer = PacketWriter::new();
                                game::write_message(
                                    &mut writer,
                                    0x14,
                                    "You cannot open a private channel with yourself.",
                                );
                                packets.push(writer.into_vec());
                            } else {
                                let mut writer = PacketWriter::new();
                                game::write_private_channel(&mut writer, name);
                                packets.push(writer.into_vec());
                            }
                        }
                        Ok(ClientPacketOutcome::CreatePrivateChannel) => {
                            match world_guard.ensure_private_channel(player_id) {
                                Ok((channel_id, name)) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_open_own_channel(&mut writer, channel_id, &name);
                                    packets.push(writer.into_vec());
                                }
                                Err(err) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(&mut writer, 0x14, &err);
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::InviteToChannel { name }) => {
                            let outcome = world_guard
                                .invite_to_private_channel(player_id, &name)
                                .unwrap_or(ChannelInviteResult::NotFound);
                            match outcome {
                                ChannelInviteResult::Invited {
                                    channel_name,
                                    invitee_id,
                                    invitee_name,
                                    ..
                                } => {
                                    let owner_name = world_guard
                                        .players
                                        .get(&player_id)
                                        .map(|player| player.name.clone())
                                        .unwrap_or_else(|| "Someone".to_string());
                                    world_guard.queue_player_message(
                                        invitee_id,
                                        0x14,
                                        format!(
                                            "{owner_name} invites you to {channel_name} private chat channel."
                                        ),
                                    );
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        &format!("{invitee_name} has been invited."),
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelInviteResult::AlreadyInvited { invitee_name } => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        &format!("{invitee_name} is already invited."),
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelInviteResult::SelfInvite => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        "You cannot invite yourself.",
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelInviteResult::NoChannel => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        "You need to create a private channel first.",
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelInviteResult::NotFound => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(&mut writer, 0x14, "Player not found.");
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::ExcludeFromChannel { name }) => {
                            let outcome = world_guard
                                .exclude_from_private_channel(player_id, &name)
                                .unwrap_or(ChannelExcludeResult::NotFound);
                            match outcome {
                                ChannelExcludeResult::Excluded {
                                    invitee_id,
                                    invitee_name,
                                    ..
                                } => {
                                    world_guard.queue_player_message(
                                        invitee_id,
                                        0x14,
                                        "You have been excluded from a private channel."
                                            .to_string(),
                                    );
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        &format!("{invitee_name} has been excluded."),
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelExcludeResult::NotInvited { invitee_name } => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        &format!("{invitee_name} is not invited."),
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelExcludeResult::SelfExclude => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        "You cannot exclude yourself.",
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelExcludeResult::NoChannel => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        "You need to create a private channel first.",
                                    );
                                    packets.push(writer.into_vec());
                                }
                                ChannelExcludeResult::NotFound => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(&mut writer, 0x14, "Player not found.");
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::RequestProcess { name }) => {
                            let is_gm = world_guard
                                .players
                                .get(&player_id)
                                .map(|player| player.is_gm)
                                .unwrap_or(false);
                            if !is_gm {
                                let mut writer = PacketWriter::new();
                                game::write_message(&mut writer, 0x14, REQUEST_WAIT_MESSAGE);
                                packets.push(writer.into_vec());
                            } else if let Some(entry) = world_guard.take_request_by_name(&name) {
                                if !request_queue_open {
                                    let mut writer = PacketWriter::new();
                                    game::write_open_request_queue(&mut writer);
                                    packets.push(writer.into_vec());
                                    request_queue_open = true;
                                }
                                let mut writer = PacketWriter::new();
                                game::write_finish_request(&mut writer, &entry.name);
                                packets.push(writer.into_vec());
                            }
                        }
                        Ok(ClientPacketOutcome::RequestRemove { name }) => {
                            let is_gm = world_guard
                                .players
                                .get(&player_id)
                                .map(|player| player.is_gm)
                                .unwrap_or(false);
                            if !is_gm {
                                let mut writer = PacketWriter::new();
                                game::write_message(&mut writer, 0x14, REQUEST_WAIT_MESSAGE);
                                packets.push(writer.into_vec());
                            } else if let Some(entry) = world_guard.take_request_by_name(&name) {
                                if !request_queue_open {
                                    let mut writer = PacketWriter::new();
                                    game::write_open_request_queue(&mut writer);
                                    packets.push(writer.into_vec());
                                    request_queue_open = true;
                                }
                                let mut writer = PacketWriter::new();
                                game::write_delete_request(&mut writer, &entry.name);
                                packets.push(writer.into_vec());
                            }
                        }
                        Ok(ClientPacketOutcome::RequestCancel) => {
                            let is_gm = world_guard
                                .players
                                .get(&player_id)
                                .map(|player| player.is_gm)
                                .unwrap_or(false);
                            if !is_gm {
                                let mut writer = PacketWriter::new();
                                game::write_message(&mut writer, 0x14, REQUEST_WAIT_MESSAGE);
                                packets.push(writer.into_vec());
                            } else if request_queue_open {
                                let mut writer = PacketWriter::new();
                                game::write_close_request(&mut writer);
                                packets.push(writer.into_vec());
                                request_queue_open = false;
                            }
                        }
                        Ok(ClientPacketOutcome::Shop(request)) => match request {
                            ShopRequest::Look { item_type, count } => {
                                match world_guard.shop_look(player_id, item_type, count) {
                                    Ok(text) => {
                                        let mut writer = PacketWriter::new();
                                        game::write_message(&mut writer, MESSAGE_LOOK, &text);
                                        packets.push(writer.into_vec());
                                    }
                                    Err(err) => {
                                        let mut writer = PacketWriter::new();
                                        game::write_message(&mut writer, 0x14, &err);
                                        packets.push(writer.into_vec());
                                    }
                                }
                            }
                            ShopRequest::Buy {
                                item_type,
                                count,
                                amount,
                                ignore_capacity,
                                buy_with_backpack,
                            } => match world_guard.shop_buy(
                                player_id,
                                item_type,
                                count,
                                amount,
                                ignore_capacity,
                                buy_with_backpack,
                            ) {
                                Ok(sell_list) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_shop_sell_list(
                                        &mut writer,
                                        sell_list.money,
                                        &sell_list.entries,
                                    );
                                    packets.push(writer.into_vec());
                                    if let Some(player) = world_guard.players.get(&player_id) {
                                        for container in player.open_containers.values() {
                                            let mut writer = PacketWriter::new();
                                            game::write_open_container(
                                                &mut writer,
                                                container,
                                                world_guard.item_types.as_ref(),
                                            );
                                            packets.push(writer.into_vec());
                                        }
                                    }
                                }
                                Err(err) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(&mut writer, 0x14, &err);
                                    packets.push(writer.into_vec());
                                }
                            },
                            ShopRequest::Sell {
                                item_type,
                                count,
                                amount,
                            } => match world_guard.shop_sell(player_id, item_type, count, amount) {
                                Ok(sell_list) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_shop_sell_list(
                                        &mut writer,
                                        sell_list.money,
                                        &sell_list.entries,
                                    );
                                    packets.push(writer.into_vec());
                                    if let Some(player) = world_guard.players.get(&player_id) {
                                        for container in player.open_containers.values() {
                                            let mut writer = PacketWriter::new();
                                            game::write_open_container(
                                                &mut writer,
                                                container,
                                                world_guard.item_types.as_ref(),
                                            );
                                            packets.push(writer.into_vec());
                                        }
                                    }
                                }
                                Err(err) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(&mut writer, 0x14, &err);
                                    packets.push(writer.into_vec());
                                }
                            },
                            ShopRequest::Close => {
                                if world_guard.shop_close(player_id) {
                                    let mut writer = PacketWriter::new();
                                    game::write_shop_close(&mut writer);
                                    packets.push(writer.into_vec());
                                }
                            }
                        },
                        Ok(ClientPacketOutcome::Trade(request)) => match request {
                            TradeRequest::Request {
                                position,
                                item_type,
                                stack_pos,
                                partner_id,
                            } => {
                                let partner_id = PlayerId(partner_id.0);
                                if world_guard.players.contains_key(&partner_id) {
                                    if let Err(err) = world_guard.trade_request(
                                        player_id,
                                        partner_id,
                                        position,
                                        item_type,
                                        stack_pos,
                                    ) {
                                        world_guard.queue_player_message(player_id, 0x14, err);
                                    }
                                } else {
                                    world_guard.queue_player_message(
                                        player_id,
                                        0x14,
                                        "Player not found.".to_string(),
                                    );
                                }
                            }
                            TradeRequest::Look {
                                counter_offer,
                                index,
                            } => {
                                if let Some(item) = world_guard.trade_item_for_look(
                                    player_id,
                                    counter_offer,
                                    index,
                                ) {
                                    let message = describe_item(
                                        &item,
                                        world_guard.item_types.as_ref(),
                                        world_guard.object_types.as_ref(),
                                    );
                                    world_guard.queue_player_message(player_id, 0x14, message);
                                } else {
                                    world_guard.queue_player_message(
                                        player_id,
                                        0x14,
                                        "Item not found.".to_string(),
                                    );
                                }
                            }
                            TradeRequest::Accept => {
                                if let Err(err) = world_guard.trade_accept(player_id) {
                                    world_guard.queue_player_message(player_id, 0x14, err);
                                }
                            }
                            TradeRequest::Close => {
                                if let Err(err) = world_guard.trade_close(player_id) {
                                    world_guard.queue_player_message(player_id, 0x14, err);
                                }
                            }
                        },
                        Ok(ClientPacketOutcome::Party(request)) => match request {
                            PartyRequest::Invite { creature_id } => {
                                let target_id = PlayerId(creature_id.0);
                                if world_guard.players.contains_key(&target_id) {
                                    world_guard.party_invite(player_id, target_id)?;
                                } else {
                                    world_guard.queue_player_message(
                                        player_id,
                                        0x14,
                                        "Player not found.".to_string(),
                                    );
                                }
                            }
                            PartyRequest::Join { creature_id } => {
                                let leader_id = PlayerId(creature_id.0);
                                if world_guard.players.contains_key(&leader_id) {
                                    world_guard.party_join(player_id, leader_id)?;
                                } else {
                                    world_guard.queue_player_message(
                                        player_id,
                                        0x14,
                                        "Party leader not found.".to_string(),
                                    );
                                }
                            }
                            PartyRequest::Revoke { creature_id } => {
                                let target_id = PlayerId(creature_id.0);
                                if world_guard.players.contains_key(&target_id) {
                                    world_guard.party_revoke(player_id, target_id)?;
                                } else {
                                    world_guard.queue_player_message(
                                        player_id,
                                        0x14,
                                        "Player not found.".to_string(),
                                    );
                                }
                            }
                            PartyRequest::PassLeadership { creature_id } => {
                                let target_id = PlayerId(creature_id.0);
                                if world_guard.players.contains_key(&target_id) {
                                    world_guard.party_pass_leadership(player_id, target_id)?;
                                } else {
                                    world_guard.queue_player_message(
                                        player_id,
                                        0x14,
                                        "Player not found.".to_string(),
                                    );
                                }
                            }
                            PartyRequest::Leave => {
                                world_guard.party_leave(player_id, true)?;
                            }
                            PartyRequest::ShareExp { enabled, .. } => {
                                world_guard.party_set_shared_exp(player_id, enabled)?;
                            }
                        },
                        Ok(ClientPacketOutcome::BuddyAdd { name }) => {
                            let result = world_guard.add_buddy_by_name(player_id, &name)?;
                            match result {
                                BuddyAddResult::Added(entry)
                                | BuddyAddResult::AlreadyPresent(entry) => {
                                    let mut writer = PacketWriter::new();
                                    game::write_buddy_data(
                                        &mut writer,
                                        entry.id.0,
                                        &entry.name,
                                        entry.online,
                                    );
                                    packets.push(writer.into_vec());
                                }
                                BuddyAddResult::SelfBuddy => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(
                                        &mut writer,
                                        0x14,
                                        "You cannot add yourself.",
                                    );
                                    packets.push(writer.into_vec());
                                }
                                BuddyAddResult::NotFound => {
                                    let mut writer = PacketWriter::new();
                                    game::write_message(&mut writer, 0x14, "Buddy not found.");
                                    packets.push(writer.into_vec());
                                }
                            }
                        }
                        Ok(ClientPacketOutcome::BuddyRemove { buddy_id }) => {
                            let removed = world_guard.remove_buddy(player_id, buddy_id)?;
                            if !removed {
                                let mut writer = PacketWriter::new();
                                game::write_message(
                                    &mut writer,
                                    0x14,
                                    "Buddy is not on your list.",
                                );
                                packets.push(writer.into_vec());
                            }
                        }
                        Ok(ClientPacketOutcome::Admin(admin)) => {
                            admin_action = Some(admin);
                        }
                        Err(err) => {
                            logging::log_error(&format!("game packet error: {}", err));
                            eprintln!("game packet error: {}", err);
                        }
                        _ => {}
                    }

                    if let Some(inventory_before) = inventory_before {
                        if let Some(player) = world_guard.players.get(&player_id) {
                            let inventory_after = snapshot_inventory(player);
                            packets.extend(build_inventory_packets(
                                &inventory_before,
                                &inventory_after,
                                world_guard.item_types.as_ref(),
                            ));
                        }
                    }
                }
            }
            world_guard.tick_player_autowalk(player_id, &clock);
            let pending_outcomes = world_guard.take_pending_moveuse_outcomes(player_id);
            for outcome in pending_outcomes {
                packets.extend(build_moveuse_packets(&outcome, &world_guard, player_id));
                if outcome.logout_users.contains(&player_id) {
                    logout_requested = true;
                }
            }
            let closed_containers = world_guard.close_out_of_range_map_containers(player_id);
            for container_id in closed_containers {
                let mut writer = PacketWriter::new();
                game::write_close_container(&mut writer, container_id);
                packets.push(writer.into_vec());
            }
            let pending_closes = world_guard.take_container_closes(player_id);
            for container_id in pending_closes {
                let mut writer = PacketWriter::new();
                game::write_close_container(&mut writer, container_id);
                packets.push(writer.into_vec());
            }
            if world_guard.take_container_refresh(player_id) {
                if let Some(player) = world_guard.players.get(&player_id) {
                    for container in player.open_containers.values() {
                        let mut writer = PacketWriter::new();
                        game::write_open_container(
                            &mut writer,
                            container,
                            world_guard.item_types.as_ref(),
                        );
                        packets.push(writer.into_vec());
                    }
                }
            }
            let player_combat = world_guard.tick_player_attack(player_id, &clock);
            packets.extend(build_player_combat_packets(&player_combat, &world_guard));
            if player_combat.refresh_map {
                if let Some(player) = world_guard.players.get(&player_id) {
                    let mut writer = PacketWriter::new();
                    game::write_map_description(
                        &mut writer,
                        &world_guard,
                        player.position,
                        player_id,
                    );
                    packets.push(writer.into_vec());
                }
            }
            let decay_positions = world_guard.take_pending_map_refreshes(player_id);
            if !decay_positions.is_empty() {
                if let Some(player) = world_guard.players.get(&player_id) {
                    for position in decay_positions {
                        if !game::position_in_viewport(player.position, position) {
                            continue;
                        }
                        let mut writer = PacketWriter::new();
                        game::write_field_data(&mut writer, &world_guard, position, player_id);
                        packets.push(writer.into_vec());
                    }
                }
            }
            let mut condition_packets = Vec::new();
            let mut move_updates = Vec::new();
            if let Some(old_position) = old_position {
                let new_position = world_guard
                    .players
                    .get(&player_id)
                    .map(|player| player.position);
                if let Some(new_position) = new_position {
                    if new_position != old_position {
                        let mut writer = PacketWriter::new();
                        let stacks = creature_stacks
                            .get_or_insert_with(|| {
                                CreatureStacksSnapshot::Shared(state.creature_stacks_for_tick(
                                    clock.now().0,
                                    &world_guard,
                                ))
                            });
                        let stacks = stacks.as_mut_map();
                        let stack_pos = creature_stack_pos_from_snapshot(
                            &world_guard,
                            stacks,
                            old_position,
                            player_id.0,
                        );
                        move_creature_in_snapshot(stacks, old_position, new_position, player_id.0);
                        let dx = i32::from(new_position.x) - i32::from(old_position.x);
                        let dy = i32::from(new_position.y) - i32::from(old_position.y);
                        let dz = i32::from(new_position.z) - i32::from(old_position.z);
                        let z_changed = new_position.z != old_position.z;
                        let teleport = if z_changed {
                            dz.abs() > 1 || dx.abs() > 2 || dy.abs() > 2
                        } else {
                            dx.abs() > 1 || dy.abs() > 1 || dz.abs() > 1
                        };
                        if !teleport {
                            game::write_move_creature(
                                &mut writer,
                                old_position,
                                stack_pos,
                                new_position,
                            );
                            condition_packets.push(writer.into_vec());
                        }
                        if teleport {
                            // Teleports need a full map refresh to avoid missing tiles.
                            world_guard.clear_player_autowalk(player_id);
                            let mut writer = PacketWriter::new();
                            game::write_snapback(&mut writer, 0);
                            condition_packets.push(writer.into_vec());
                            let mut writer = PacketWriter::new();
                            game::write_map_description(
                                &mut writer,
                                &world_guard,
                                new_position,
                                player_id,
                            );
                            condition_packets.push(writer.into_vec());
                        } else {
                            let mut current = old_position;
                            if z_changed {
                                let steps = i32::from(new_position.z) - i32::from(current.z);
                                let step_count = steps.abs();
                                let moving_up = steps < 0;
                                for _ in 0..step_count {
                                    if moving_up {
                                        current.x = current.x.saturating_add(1);
                                        current.y = current.y.saturating_add(1);
                                        current.z = current.z.saturating_sub(1);
                                    } else {
                                        current.x = current.x.saturating_sub(1);
                                        current.y = current.y.saturating_sub(1);
                                        current.z = current.z.saturating_add(1);
                                    }
                                    let mut writer = PacketWriter::new();
                                    game::write_floor_change(
                                        &mut writer,
                                        &world_guard,
                                        current,
                                        moving_up,
                                        player_id,
                                    );
                                    condition_packets.push(writer.into_vec());
                                }
                            }

                            while current.x < new_position.x {
                                current.x = current.x.saturating_add(1);
                                let mut writer = PacketWriter::new();
                                if let Err(err) = game::write_map_row(
                                    &mut writer,
                                    &world_guard,
                                    game::OPCODE_MAP_ROW_EAST,
                                    current,
                                    player_id,
                                ) {
                                    logging::log_error(&format!("map row write failed: {}", err));
                                    eprintln!("tibia: map row write failed: {}", err);
                                } else {
                                    condition_packets.push(writer.into_vec());
                                }
                            }
                            while current.x > new_position.x {
                                current.x = current.x.saturating_sub(1);
                                let mut writer = PacketWriter::new();
                                if let Err(err) = game::write_map_row(
                                    &mut writer,
                                    &world_guard,
                                    game::OPCODE_MAP_ROW_WEST,
                                    current,
                                    player_id,
                                ) {
                                    logging::log_error(&format!("map row write failed: {}", err));
                                    eprintln!("tibia: map row write failed: {}", err);
                                } else {
                                    condition_packets.push(writer.into_vec());
                                }
                            }
                            while current.y < new_position.y {
                                current.y = current.y.saturating_add(1);
                                let mut writer = PacketWriter::new();
                                if let Err(err) = game::write_map_row(
                                    &mut writer,
                                    &world_guard,
                                    game::OPCODE_MAP_ROW_SOUTH,
                                    current,
                                    player_id,
                                ) {
                                    logging::log_error(&format!("map row write failed: {}", err));
                                    eprintln!("tibia: map row write failed: {}", err);
                                } else {
                                    condition_packets.push(writer.into_vec());
                                }
                            }
                            while current.y > new_position.y {
                                current.y = current.y.saturating_sub(1);
                                let mut writer = PacketWriter::new();
                                if let Err(err) = game::write_map_row(
                                    &mut writer,
                                    &world_guard,
                                    game::OPCODE_MAP_ROW_NORTH,
                                    current,
                                    player_id,
                                ) {
                                    logging::log_error(&format!("map row write failed: {}", err));
                                    eprintln!("tibia: map row write failed: {}", err);
                                } else {
                                    condition_packets.push(writer.into_vec());
                                }
                            }
                        }
                    }
                }
            }
            for movement in npc_moves.iter().chain(monster_moves.iter()) {
                if let Some(packet) = npc_move_packet(&world_guard, movement) {
                    move_updates.push(packet);
                }
            }
            if !condition_packets.is_empty() {
                packets.extend(condition_packets);
            }
            if !move_updates.is_empty() {
                packets.extend(move_updates);
            }
            for update in world_guard.take_pending_turn_updates(player_id) {
                if let Some(packet) = turn_update_packet(&world_guard, &update) {
                    packets.push(packet);
                }
            }
            for update in world_guard.take_pending_outfit_updates(player_id) {
                let mut writer = PacketWriter::new();
                game::write_creature_outfit(&mut writer, update.id, update.outfit);
                packets.push(writer.into_vec());
            }
            for message in world_guard.take_pending_messages(player_id) {
                let mut writer = PacketWriter::new();
                game::write_message(&mut writer, message.message_type, &message.message);
                packets.push(writer.into_vec());
            }
            for update in world_guard.take_pending_buddy_updates(player_id) {
                let mut writer = PacketWriter::new();
                game::write_buddy_status(&mut writer, update.buddy_id.0, update.online);
                packets.push(writer.into_vec());
            }
            for update in world_guard.take_pending_party_updates(player_id) {
                let mut writer = PacketWriter::new();
                game::write_creature_party(&mut writer, update.target_id.0, update.mark);
                packets.push(writer.into_vec());
            }
            for update in world_guard.take_pending_trade_updates(player_id) {
                let mut writer = PacketWriter::new();
                match update {
                    TradeUpdate::Offer {
                        counter,
                        name,
                        items,
                    } => {
                        game::write_trade_offer(
                            &mut writer,
                            counter,
                            &name,
                            &items,
                            world_guard.item_types.as_ref(),
                        );
                    }
                    TradeUpdate::Close => {
                        game::write_trade_close(&mut writer);
                    }
                }
                packets.push(writer.into_vec());
            }
            let attack_target = world_guard
                .players
                .get(&player_id)
                .and_then(|player| player.attack_target);
            let attack_target_valid = attack_target
                .map(|target| world_guard.creature_exists(target))
                .unwrap_or(true);
            let mut clear_target = false;
            if let Some(player) = world_guard.players.get_mut(&player_id) {
                if attack_target.is_some() && !attack_target_valid {
                    player.attack_target = None;
                }
                let current_target = player.attack_target;
                if last_attack_target.is_some() && current_target.is_none() {
                    clear_target = true;
                }
                last_attack_target = current_target;
                let state = player_state_flags(player, &clock);
                if last_player_state != Some(state) {
                    let mut writer = PacketWriter::new();
                    game::write_player_state(&mut writer, state);
                    packets.push(writer.into_vec());
                    last_player_state = Some(state);
                }
            }
            if clear_target {
                let mut writer = PacketWriter::new();
                game::write_clear_target(&mut writer);
                packets.push(writer.into_vec());
            }
            if let Some(player) = world_guard.players.get(&player_id) {
                let capacity = world_guard.player_capacity_remaining(player);
                let snapshot = snapshot_player_data(player, capacity);
                if last_player_data != Some(snapshot) {
                    let mut writer = PacketWriter::new();
                    game::write_player_data(&mut writer, player, capacity);
                    packets.push(writer.into_vec());
                    last_player_data = Some(snapshot);
                }
            }
            packets
        };

        if let Some(admin_action) = admin_action.take() {
            let world_guard = world
                .lock()
                .map_err(|_| "world lock poisoned".to_string())?;
            let mut response: Option<String> = None;
            match admin_action {
                AdminOutcome::DisconnectSelf => {
                    disconnect_after_send = true;
                }
                AdminOutcome::OnlineList(names) => {
                    if names.is_empty() {
                        response = Some("Online: (none)".to_string());
                    } else {
                        response = Some(format!("Online: {}", names.join(", ")));
                    }
                }
                AdminOutcome::Log(message) => {
                    response = Some(message);
                }
                AdminOutcome::Shutdown => {
                    control.request_shutdown();
                    response = Some("Server shutting down.".to_string());
                    disconnect_after_send = true;
                }
                AdminOutcome::Restart => {
                    control.request_restart();
                    response = Some("Server restarting.".to_string());
                    disconnect_after_send = true;
                }
            }
            if let Some(response) = response {
                let mut writer = PacketWriter::new();
                game::write_message(&mut writer, 0x14, &response);
                responses.push(writer.into_vec());
            }
            if let Some(player) = world_guard.players.get(&player_id) {
                responses.extend(build_inventory_snapshot_packets(
                    player,
                    world_guard.item_types.as_ref(),
                ));
            }
        }

        if logout_requested {
            disconnect_after_send = true;
            if let Ok(mut world_guard) = world.lock() {
                world_guard.handle_disconnect(player_id);
            }
        }
        if last_ping.elapsed() >= GAME_PING_INTERVAL {
            let mut writer = PacketWriter::new();
            game::write_ping(&mut writer);
            responses.push(writer.into_vec());
            last_ping = Instant::now();
        }

        for packet in responses {
            transport
                .write_packet(&packet, trace.as_mut())
                .map_err(|err| format!("send packet failed: {}", err))?;
        }

        if disconnect_after_send {
            return Ok(());
        }
    }
}

fn logout_block_message(reason: LogoutBlockReason) -> &'static str {
    match reason {
        LogoutBlockReason::ProtectionZone => "You must leave the protection zone to logout.",
        LogoutBlockReason::NoLogoutZone => "You cannot logout here.",
        LogoutBlockReason::InFight => "You cannot logout while in a fight.",
    }
}

fn legacy_outfit_dialog_base(player: &PlayerState, has_rights: bool) -> (u8, u16) {
    let is_male = player.race == 0;
    let opcode = if is_male {
        game::OPCODE_OUTFIT_DIALOG_LEGACY_MALE
    } else {
        game::OPCODE_OUTFIT_DIALOG_LEGACY_FEMALE
    };
    let mut base: u16 = if is_male { 0x83 } else { 0x8b };
    if has_rights {
        base = base.saturating_add(3);
    }
    (opcode, base)
}

fn build_look_message(
    world: &WorldState,
    player_id: PlayerId,
    request: &LookRequest,
) -> Option<String> {
    match &request.target {
        LookTarget::Creature(id) => describe_creature(world, player_id, *id),
        LookTarget::Position {
            position,
            type_id: _,
            stack_pos,
        } => describe_position(world, player_id, *position, *stack_pos),
    }
}

fn describe_position(
    world: &WorldState,
    player_id: PlayerId,
    position: Position,
    stack_pos: u8,
) -> Option<String> {
    let player = world.players.get(&player_id)?;
    if !game::position_in_viewport(player.position, position) {
        return Some("You cannot see that.".to_string());
    }
    let tile = world.map.tile(position)?;
    let index = stack_pos as usize;
    if index < tile.items.len() {
        if let Some(detail) = tile.item_details.get(index) {
            return Some(map_item_description(
                detail,
                world.item_types.as_ref(),
                world.object_types.as_ref(),
                player,
                position,
            ));
        }
        let item = tile.items.get(index)?;
        return Some(describe_item(
            item,
            world.item_types.as_ref(),
            world.object_types.as_ref(),
        ));
    }
    let creature_index = index.saturating_sub(tile.items.len());
    let mut creature_ids = collect_creatures_at(world, position);
    creature_ids.sort_unstable();
    let creature_id = creature_ids.get(creature_index).copied()?;
    describe_creature(world, player_id, creature_id)
}

fn weight_line(
    object_types: Option<&ObjectTypeIndex>,
    type_id: ItemTypeId,
    count: u16,
) -> Option<String> {
    let object_type = object_types?.get(type_id)?;
    if !object_type.has_flag("Take") {
        return None;
    }
    let weight = u32::from(object_type.attribute_u16("Weight")?);
    if weight == 0 {
        return None;
    }
    let total = weight.saturating_mul(u32::from(count.max(1)));
    let oz = total / 100;
    let hundredths = total % 100;
    Some(format!("It weighs {}.{:02} oz.", oz, hundredths))
}

fn describe_item(
    item: &ItemStack,
    item_types: Option<&ItemTypeIndex>,
    object_types: Option<&ObjectTypeIndex>,
) -> String {
    let mut lines = Vec::new();
    if let Some(types) = item_types {
        if let Some(entry) = types.get(item.type_id) {
            lines.push(format!("You see {}.", entry.name));
        }
    }
    if lines.is_empty() {
        lines.push(format!("You see an object ({}).", item.type_id.0));
    }
    if let Some(weight) = weight_line(object_types, item.type_id, item.count) {
        lines.push(weight);
    }
    lines.join("\n")
}

fn map_item_description(
    item: &crate::world::map::MapItem,
    item_types: Option<&ItemTypeIndex>,
    object_types: Option<&ObjectTypeIndex>,
    player: &PlayerState,
    position: Position,
) -> String {
    let name = item_types
        .and_then(|types| types.get(item.type_id))
        .map(|entry| entry.name.clone())
        .unwrap_or_else(|| format!("an object ({})", item.type_id.0));
    let mut lines = vec![format!("You see {}.", name)];
    let text = item.attributes.iter().find_map(|attribute| {
        if let ItemAttribute::String(value) = attribute {
            Some(value.clone())
        } else {
            None
        }
    });

    if let Some(object_type) = object_types.and_then(|types| types.get(item.type_id)) {
        if let Some(weight) = weight_line(object_types, item.type_id, item.count) {
            lines.push(weight);
        }
        if object_type.has_flag("ShowDetail") {
            if let Some(detail) = object_type.attribute("Description") {
                if !detail.trim().is_empty() {
                    lines.push(format!("{}.", detail.trim()));
                }
            }
        }
        if object_type.has_flag("Text") {
            let font_size = object_type.attribute_u16("FontSize").unwrap_or(0);
            if font_size == 0 {
                if let Some(text) = text {
                    lines.push(format!("{}.", text.trim()));
                }
            } else if font_size >= 2 {
                let in_range = player.position.z == position.z
                    && (i32::from(player.position.x) - i32::from(position.x)).abs()
                        <= i32::from(font_size)
                    && (i32::from(player.position.y) - i32::from(position.y)).abs()
                        <= i32::from(font_size);
                if let Some(text) = text {
                    if in_range {
                        lines.push(format!("You read: {}", text.trim()));
                    } else {
                        lines.push("You are too far away to read it.".to_string());
                    }
                } else {
                    lines.push("Nothing is written on it.".to_string());
                }
            }
        }
    } else if let Some(text) = text {
        lines.push(text);
    }

    lines.join("\n")
}

fn describe_creature(
    world: &WorldState,
    player_id: PlayerId,
    creature_id: u32,
) -> Option<String> {
    if creature_id == player_id.0 {
        let player = world.players.get(&player_id)?;
        let vocation = player_vocation_line("You", player.profession, true);
        return Some(format!("You see yourself. {vocation}"));
    }
    if let Some(player) = world.players.get(&PlayerId(creature_id)) {
        let name = format!("{} (Level {})", player.name, player.level);
        let pronoun = player_pronoun(player.race);
        let vocation = player_vocation_line(pronoun, player.profession, false);
        return Some(format!("You see {name}. {vocation}"));
    }
    if let Some(npc) = world.npcs.get(&CreatureId(creature_id)) {
        return Some(format!("You see {}.", npc.name));
    }
    if let Some(monster) = world.monsters.get(&CreatureId(creature_id)) {
        let article = world
            .monster_index
            .as_ref()
            .and_then(|index| index.script_by_race(monster.race_number))
            .and_then(|script| script.article())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(article) = article {
            return Some(format!("You see {} {}.", article, monster.name));
        }
        return Some(format!("You see {}.", monster.name));
    }
    None
}

fn player_pronoun(race: u8) -> &'static str {
    if race == 1 { "She" } else { "He" }
}

fn profession_name(profession: u8, with_article: bool, lowercase: bool) -> String {
    let base = match profession {
        1 => "Knight",
        2 => "Paladin",
        3 => "Sorcerer",
        4 => "Druid",
        11 => "Elite Knight",
        12 => "Royal Paladin",
        13 => "Master Sorcerer",
        14 => "Elder Druid",
        _ => "None",
    };
    let mut text = String::new();
    if with_article {
        let article = matches!(profession, 11 | 14);
        text.push_str(if article { "an " } else { "a " });
    }
    text.push_str(base);
    if lowercase {
        text.make_ascii_lowercase();
    }
    text
}

fn player_vocation_line(subject: &str, profession: u8, self_view: bool) -> String {
    if profession == 0 {
        if self_view {
            return "You have no vocation.".to_string();
        }
        return format!("{subject} has no vocation.");
    }
    let name = profession_name(profession, true, true);
    if self_view {
        format!("You are {name}.")
    } else {
        format!("{subject} is {name}.")
    }
}

fn collect_creatures_at(world: &WorldState, position: Position) -> Vec<u32> {
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
    creature_ids
}

fn handle_help_channel_request(
    talk: &CTalkMessage,
    caster_id: PlayerId,
    world: &mut WorldState,
    packets: &mut Vec<Vec<u8>>,
) -> Result<bool, String> {
    let Some(channel_id) = talk.channel_id else {
        return Ok(false);
    };
    if channel_id != HELP_CHANNEL_ID {
        return Ok(false);
    }
    let player = world
        .players
        .get(&caster_id)
        .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
    if player.is_gm {
        return Ok(false);
    }
    let message = talk.message.trim();
    if message.is_empty() {
        return Ok(true);
    }
    let added = world.submit_request(caster_id)?;
    let response = if added {
        REQUEST_WAIT_MESSAGE
    } else {
        REQUEST_ALREADY_SUBMITTED
    };
    let mut writer = PacketWriter::new();
    game::write_message(&mut writer, 0x14, response);
    packets.push(writer.into_vec());
    Ok(true)
}

fn handle_house_list_command(
    talk: &CTalkMessage,
    caster_id: PlayerId,
    world: &mut WorldState,
    packets: &mut Vec<Vec<u8>>,
) -> Result<bool, String> {
    let message = talk.message.trim();
    if !message.to_ascii_lowercase().starts_with("!house") {
        return Ok(false);
    }
    let mut parts = message.split_whitespace();
    let _ = parts.next();
    let target = parts.next().unwrap_or("guests");
    let kind = match target.to_ascii_lowercase().as_str() {
        "guest" | "guests" => crate::world::state::HouseListKind::Guests,
        "subowner" | "subowners" | "sub" => crate::world::state::HouseListKind::Subowners,
        _ => {
            let mut writer = PacketWriter::new();
            game::write_message(
                &mut writer,
                0x14,
                "Usage: !house guests | !house subowners",
            );
            packets.push(writer.into_vec());
            return Ok(true);
        }
    };
    match world.open_house_list(caster_id, kind) {
        Ok(edit) => {
            let mut writer = PacketWriter::new();
            game::write_edit_list(&mut writer, edit.list_type, edit.id, &edit.text);
            packets.push(writer.into_vec());
        }
        Err(err) => {
            let mut writer = PacketWriter::new();
            game::write_message(&mut writer, 0x14, &err);
            packets.push(writer.into_vec());
        }
    }
    Ok(true)
}

fn talk_to_packet(
    talk: &CTalkMessage,
    caster_id: PlayerId,
    world: &mut WorldState,
) -> Result<Option<Vec<u8>>, String> {
    const TALK_TYPES_POSITION: [u8; 5] = [0x01, 0x02, 0x03, 0x10, 0x11];
    const TALK_TYPES_CHANNEL: [u8; 4] = [0x05, 0x0a, 0x0c, 0x0e];
    const TALK_TYPES_TEXT: [u8; 6] = [0x04, 0x06, 0x07, 0x08, 0x09, 0x0b];

    let message = talk.message.trim();
    if message.is_empty() {
        return Ok(None);
    }
    let player = world
        .players
        .get(&caster_id)
        .ok_or_else(|| format!("unknown player {:?}", caster_id))?;
    let payload = if TALK_TYPES_POSITION.contains(&talk.talk_type) {
        game::TalkPayload::Position {
            position: player.position,
            text: message,
        }
    } else if TALK_TYPES_CHANNEL.contains(&talk.talk_type) {
        let channel_id = talk.channel_id.unwrap_or(0);
        if world.channel_name_for(caster_id, channel_id).is_none() {
            return Ok(None);
        }
        game::TalkPayload::Channel {
            channel_id,
            text: message,
        }
    } else if TALK_TYPES_TEXT.contains(&talk.talk_type) {
        game::TalkPayload::Text {
            text: message,
            arg: if talk.talk_type == 0x06 { Some(0) } else { None },
        }
    } else {
        return Err(format!("unsupported talk type 0x{:02x}", talk.talk_type));
    };
    let mut writer = PacketWriter::new();
    game::write_talk(&mut writer, caster_id.0, &player.name, talk.talk_type, payload)?;
    Ok(Some(writer.into_vec()))
}

fn build_npc_talk_packets(
    talk: &CTalkMessage,
    caster_id: PlayerId,
    world: &mut WorldState,
    clock: &GameClock,
) -> Result<Vec<Vec<u8>>, String> {
    const TALK_TYPES_POSITION: [u8; 5] = [0x01, 0x02, 0x03, 0x10, 0x11];
    const NPC_TALK_TYPE: u8 = 0x01;

    let message = talk.message.trim();
    if message.is_empty() || !TALK_TYPES_POSITION.contains(&talk.talk_type) {
        return Ok(Vec::new());
    }

    let outcome = world.npc_talk_responses(caster_id, message, Some(clock));
    let mut packets = Vec::new();
    for response in outcome.responses {
        let mut writer = PacketWriter::new();
        game::write_talk(
            &mut writer,
            response.npc_id.0,
            &response.name,
            NPC_TALK_TYPE,
            game::TalkPayload::Position {
                position: response.position,
                text: &response.message,
            },
        )?;
        packets.push(writer.into_vec());
    }
    for effect in outcome.effects {
        let mut writer = PacketWriter::new();
        game::write_graphical_effect(&mut writer, effect.position, effect.effect_id);
        packets.push(writer.into_vec());
    }
    if let Some(shop) = outcome.shop {
        let mut writer = PacketWriter::new();
        game::write_shop_open(&mut writer, &shop.items);
        packets.push(writer.into_vec());
        let mut writer = PacketWriter::new();
        game::write_shop_sell_list(&mut writer, shop.sell_list.money, &shop.sell_list.entries);
        packets.push(writer.into_vec());
    }
    if outcome.containers_dirty {
        if let Some(player) = world.players.get(&caster_id) {
            for container in player.open_containers.values() {
                let mut writer = PacketWriter::new();
                game::write_open_container(&mut writer, container, world.item_types.as_ref());
                packets.push(writer.into_vec());
            }
        }
    }
    Ok(packets)
}

fn npc_move_packet(_world: &WorldState, movement: &CreatureMove) -> Option<Vec<u8>> {
    let mut writer = PacketWriter::new();
    game::write_move_creature(&mut writer, movement.from, movement.stack_pos, movement.to);
    Some(writer.into_vec())
}

fn turn_update_packet(world: &WorldState, update: &CreatureTurnUpdate) -> Option<Vec<u8>> {
    let stack_pos = game::creature_stack_pos(world, update.position, update.id);
    if stack_pos >= 10 {
        return None;
    }
    let mut writer = PacketWriter::new();
    game::write_creature_turn(
        &mut writer,
        update.position,
        stack_pos,
        update.id,
        update.direction,
    );
    Some(writer.into_vec())
}

fn select_player_from_login(
    config: &GameServerConfig,
    state: &GameServerState,
    peer_ip: Option<IpAddr>,
    login: &GameLogin,
) -> Result<(PlayerId, String, bool, bool, bool), String> {
    if let Some(selection) = take_login_selection(config, peer_ip) {
        if login.character.trim().is_empty() {
            return Err("login failed: missing character selection".to_string());
        }
        if let Some((player_id, name)) = selection.pick_from_login(login) {
            return Ok((
                player_id,
                name,
                selection.premium,
                selection.is_gm,
                selection.is_test_god,
            ));
        }
        return Err("login failed: character not on account".to_string());
    }

    let player_id = resolve_player_id_from_game_login(login, config.root.as_ref(), state)
        .ok_or_else(|| "login failed: character not found".to_string())?;
    let mut name = login.character.trim().to_string();
    if name.is_empty() {
        name = login.account.trim().to_string();
    }
    if name.is_empty() {
        name = format!("Player{}", player_id.0);
    }
    let (premium, is_gm, is_test_god) = resolve_game_login_privileges(config.root.as_ref(), login);
    Ok((player_id, name, premium, is_gm, is_test_god))
}

fn select_connection_player(
    config: &GameServerConfig,
    state: &GameServerState,
    peer_ip: Option<IpAddr>,
) -> (PlayerId, String, bool, bool, bool) {
    if let Some(selection) = take_login_selection(config, peer_ip) {
        if let Some((player_id, name)) = selection.pick_default() {
            return (
                player_id,
                name,
                selection.premium,
                selection.is_gm,
                selection.is_test_god,
            );
        }
    }
    let player_id = next_player_id(state);
    (player_id, format!("Player{}", player_id.0), true, false, false)
}

fn resolve_game_login_privileges(root: Option<&PathBuf>, login: &GameLogin) -> (bool, bool, bool) {
    if let Some(root) = root {
        if let Ok(Some(accounts)) = AccountRegistry::load(root) {
            if let Some(record) = accounts.verify(&login.account, &login.password) {
                return (
                    record.premium,
                    record.gamemaster || record.test_god,
                    record.test_god,
                );
            }
        }
    }

    let is_test_god = login.account.trim().eq_ignore_ascii_case("test_god")
        && login.password == "test_god";
    (true, is_test_god, is_test_god)
}

fn take_login_selection(
    config: &GameServerConfig,
    peer_ip: Option<IpAddr>,
) -> Option<LoginSelection> {
    let registry = config.login_registry.as_ref()?;
    let peer_ip = peer_ip?;
    registry.take(peer_ip)
}

fn build_character_list(
    root: Option<&PathBuf>,
    player_ids: &[PlayerId],
    account: &str,
    world: &WorldEndpoint,
) -> Vec<crate::net::login::LoginCharacter> {
    let mut entries = Vec::new();
    for player_id in player_ids {
        let name = load_player_name(root, *player_id)
            .unwrap_or_else(|| format!("Player{}", player_id.0));
        entries.push(crate::net::login::LoginCharacter {
            player_id: player_id.0,
            name,
            world: world.name.clone(),
            ip: world.ip,
            port: world.port,
        });
    }
    if entries.is_empty() {
        entries.push(crate::net::login::LoginCharacter {
            player_id: 0,
            name: account.to_string(),
            world: world.name.clone(),
            ip: world.ip,
            port: world.port,
        });
    }
    entries
}

fn load_player_name(root: Option<&PathBuf>, player_id: PlayerId) -> Option<String> {
    let root = root?;
    let path = root
        .join("save")
        .join("players")
        .join(format!("{}.sav", player_id.0));
    let contents = std::fs::read_to_string(&path).ok()?;
    parse_saved_player_name(&contents)
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

fn resolve_player_id(payload: &LoginPayloadV1, root: Option<&PathBuf>) -> PlayerId {
    if payload.account_id > 0 && payload.account_id <= u64::from(u32::MAX) {
        return PlayerId(payload.account_id as u32);
    }
    if let Some(root) = root {
        if let Ok(Some(id)) = find_player_id_by_name(root, &payload.account) {
            return id;
        }
        if let Ok(id) = next_available_player_id(root) {
            return id;
        }
    }
    hash_player_id(&payload.account)
}

fn next_player_id(state: &GameServerState) -> PlayerId {
    let id = state.next_player_id.fetch_add(1, Ordering::SeqCst) as u32;
    PlayerId(id.max(1))
}

#[derive(Debug, Clone)]
struct WorldEndpoint {
    name: String,
    ip: u32,
    port: u16,
}

fn resolve_world_endpoint(config: &LoginServerConfig, peer: Option<SocketAddr>) -> WorldEndpoint {
    let world_addr = config
        .world_addr
        .as_deref()
        .unwrap_or(config.bind_addr.as_str());
    let (host, port) = split_host_port(world_addr, 7172);
    let ip = resolve_world_ipv4(host, peer);
    WorldEndpoint {
        name: config.world_name.clone(),
        ip: u32::from_le_bytes(ip.octets()),
        port,
    }
}

fn split_host_port(addr: &str, fallback_port: u16) -> (&str, u16) {
    match addr.rsplit_once(':') {
        Some((host, port_str)) => (
            host,
            port_str.parse::<u16>().unwrap_or(fallback_port),
        ),
        None => (addr, fallback_port),
    }
}

fn resolve_world_ipv4(host: &str, peer: Option<SocketAddr>) -> Ipv4Addr {
    let peer_ipv4 = peer.and_then(|addr| match addr.ip() {
        IpAddr::V4(ip) => Some(ip),
        IpAddr::V6(_) => None,
    });
    let fallback = peer_ipv4.unwrap_or_else(|| Ipv4Addr::new(127, 0, 0, 1));
    if host.is_empty() || host == "0.0.0.0" || host == "::" {
        return fallback;
    }
    if host.eq_ignore_ascii_case("localhost") {
        return Ipv4Addr::new(127, 0, 0, 1);
    }
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            if ip.is_unspecified() {
                fallback
            } else {
                ip
            }
        }
        Ok(IpAddr::V6(_)) => fallback,
        Err(_) => fallback,
    }
}

fn resolve_player_id_from_game_login(
    login: &GameLogin,
    root: Option<&PathBuf>,
    state: &GameServerState,
) -> Option<PlayerId> {
    println!(
        "tibia: resolve player from login account='{}' character='{}'",
        login.account.trim(),
        login.character.trim()
    );
    if let Ok(id) = login.account.trim().parse::<u32>() {
        if id > 0 {
            println!("tibia: resolve player using numeric account id {id}");
            return Some(PlayerId(id));
        }
    }
    if let Some(root) = root {
        if !login.character.trim().is_empty() {
            if let Ok(Some(id)) = find_player_id_by_name(root, &login.character) {
                println!(
                    "tibia: resolved character '{}' to saved player {}",
                    login.character.trim(),
                    id.0
                );
                return Some(id);
            }
        }
        if !login.account.trim().is_empty() {
            if let Ok(Some(id)) = find_player_id_by_name(root, &login.account) {
                println!(
                    "tibia: resolved account '{}' to saved player {}",
                    login.account.trim(),
                    id.0
                );
                return Some(id);
            }
        }
        return None;
    }
    let key = if !login.account.trim().is_empty() {
        login.account.as_str()
    } else if !login.character.trim().is_empty() {
        login.character.as_str()
    } else {
        return Some(next_player_id(state));
    };
    Some(hash_player_id(key))
}

fn find_player_id_by_name(root: &PathBuf, name: &str) -> Result<Option<PlayerId>, String> {
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
                    if let Some(found) = parse_name_from_save(&contents) {
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

fn parse_name_from_save(contents: &str) -> Option<String> {
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

fn next_available_player_id(root: &PathBuf) -> Result<PlayerId, String> {
    let dir = root.join("save").join("players");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(PlayerId(1)),
        Err(err) => return Err(format!("read save players dir failed: {}", err)),
    };
    let mut max_id = 0u32;
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
        max_id = max_id.max(id);
    }
    Ok(PlayerId(max_id.saturating_add(1).max(1)))
}

fn hash_player_id(account: &str) -> PlayerId {
    let mut hash: u64 = 1469598103934665603;
    for &byte in account.as_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    let id = (hash & u64::from(u32::MAX)) as u32;
    PlayerId(id.max(1))
}

fn spawn_connection_player(
    world: &mut WorldState,
    player_id: PlayerId,
    name: String,
    premium: bool,
    is_gm: bool,
    is_test_god: bool,
    root: Option<&PathBuf>,
) -> Result<PlayerId, String> {
    if let Some(player) = world.players.get_mut(&player_id) {
        if !name.trim().is_empty() {
            player.name = name;
        }
        player.premium = premium;
        player.is_gm = is_gm;
        player.is_test_god = is_test_god;
        return Ok(player_id);
    }
    if let Some(mut player) = world.offline_players.remove(&player_id) {
        if !name.trim().is_empty() {
            player.name = name;
        }
        player.premium = premium;
        player.is_gm = is_gm;
        player.is_test_god = is_test_god;
        world.players.insert(player_id, player);
        world.schedule_cron_for_player_items(player_id);
        world.index_player_items(player_id);
        return Ok(player_id);
    }
    if let Some(root) = root {
        let save_path = root
            .join("save")
            .join("players")
            .join(format!("{}.sav", player_id.0));
        println!(
            "tibia: loading player {} for '{}' from {}",
            player_id.0,
            name.trim(),
            save_path.display()
        );
        let store = SaveStore::from_root(root);
        match store.load_player(player_id) {
            Ok(Some(mut player)) => {
                if !name.trim().is_empty() {
                    player.name = name;
                }
                player.premium = premium;
                player.is_gm = is_gm;
                player.is_test_god = is_test_god;
                player.clamp_outfits();
                world.players.insert(player_id, player);
                world.schedule_cron_for_player_items(player_id);
                world.index_player_items(player_id);
                println!(
                    "tibia: loaded player {} successfully",
                    player_id.0
                );
                return Ok(player_id);
            }
            Ok(None) => {
                println!(
                    "tibia: no save found for player {}",
                    player_id.0
                );
            }
            Err(err) => {
                eprintln!("tibia: load player {} failed: {}", player_id.0, err);
            }
        }

        let desired = name.trim();
        if !desired.is_empty() {
            if let Ok(Some(found_id)) = find_player_id_by_name(root, desired) {
                if found_id != player_id {
                    println!(
                        "tibia: resolved '{}' to saved player {}",
                        desired,
                        found_id.0
                    );
                    match store.load_player(found_id) {
                        Ok(Some(mut player)) => {
                            player.premium = premium;
                            player.is_gm = is_gm;
                            player.is_test_god = is_test_god;
                            world.players.insert(found_id, player);
                            world.schedule_cron_for_player_items(found_id);
                            world.index_player_items(found_id);
                            println!(
                                "tibia: loaded player {} successfully",
                                found_id.0
                            );
                            return Ok(found_id);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            eprintln!(
                                "tibia: load player {} for '{}' failed: {}",
                                found_id.0, desired, err
                            );
                        }
                    }
                }
            }
        }
    }

    if world.spawn_player(player_id, name.clone(), false).is_ok() {
        if let Some(player) = world.players.get_mut(&player_id) {
            player.premium = premium;
            player.is_gm = is_gm;
            player.is_test_god = is_test_god;
        }
        world.schedule_cron_for_player_items(player_id);
        world.index_player_items(player_id);
        return Ok(player_id);
    }

    let position = world
        .spawn_position(false)
        .unwrap_or(Position { x: 100, y: 100, z: 7 });
    let mut player = PlayerState::new(player_id, name, position);
    player.premium = premium;
    player.is_gm = is_gm;
    player.is_test_god = is_test_god;
    world.players.insert(player_id, player);
    world.schedule_cron_for_player_items(player_id);
    world.index_player_items(player_id);
    Ok(player_id)
}

fn build_spell_cast_responses(
    report: &crate::combat::spells::SpellCastReport,
    world: &WorldState,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for position in &report.positions {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_graphical_effect(&mut writer, *position, 1);
        packets.push(writer.into_vec());
    }
    for hit in &report.hits {
        match hit.target {
            crate::combat::spells::SpellTargetId::Player(target_id) => {
                let Some(player) = world.players.get(&target_id) else {
                    continue;
                };
                let percent = health_percent(player.stats.health, player.stats.max_health);
                packets.push(creature_health_packet(target_id.0, percent));
            }
            crate::combat::spells::SpellTargetId::Monster(target_id) => {
                let Some(monster) = world.monsters.get(&target_id) else {
                    continue;
                };
                let percent = health_percent(monster.stats.health, monster.stats.max_health);
                packets.push(creature_health_packet(target_id.0, percent));
            }
        }
    }
    for update in &report.speed_updates {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_creature_speed(&mut writer, update.id, update.speed);
        packets.push(writer.into_vec());
    }
    for update in &report.light_updates {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_creature_light(&mut writer, update.id, update.level, update.color);
        packets.push(writer.into_vec());
    }
    for text in &report.text_effects {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_textual_effect(
            &mut writer,
            text.position,
            text.color,
            &text.message,
        );
        packets.push(writer.into_vec());
    }
    for message in &report.messages {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_message(&mut writer, message.message_type, &message.message);
        packets.push(writer.into_vec());
    }
    for update in &report.outfit_updates {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_creature_outfit(&mut writer, update.id, update.outfit);
        packets.push(writer.into_vec());
    }
    packets
}

fn build_monster_combat_packets(
    outcome: &crate::world::state::MonsterTickOutcome,
    world: &WorldState,
    player_id: PlayerId,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for effect in &outcome.effects {
        let mut writer = PacketWriter::new();
        game::write_graphical_effect(&mut writer, effect.position, effect.effect_id);
        packets.push(writer.into_vec());
    }
    for missile in &outcome.missiles {
        let mut writer = PacketWriter::new();
        game::write_missile_effect(&mut writer, missile.from, missile.to, missile.missile_id);
        packets.push(writer.into_vec());
    }
    for talk in &outcome.talks {
        let mut writer = PacketWriter::new();
        if game::write_talk(
            &mut writer,
            talk.monster_id.0,
            &talk.name,
            talk.talk_type,
            game::TalkPayload::Position {
                position: talk.position,
                text: &talk.message,
            },
        )
        .is_ok()
        {
            packets.push(writer.into_vec());
        }
    }
    for update in &outcome.outfit_updates {
        let mut writer = PacketWriter::new();
        game::write_creature_outfit(&mut writer, update.id, update.outfit);
        packets.push(writer.into_vec());
    }
    for update in &outcome.speed_updates {
        let mut writer = PacketWriter::new();
        game::write_creature_speed(&mut writer, update.id, update.speed);
        packets.push(writer.into_vec());
    }
    for player_id in &outcome.player_hits {
        let Some(player) = world.players.get(player_id) else {
            continue;
        };
        let percent = health_percent(player.stats.health, player.stats.max_health);
        packets.push(creature_health_packet(player_id.0, percent));
    }
    for marker in &outcome.player_hit_marks {
        if marker.player_id != player_id {
            continue;
        }
        let mut writer = PacketWriter::new();
        game::write_creature_mark(&mut writer, marker.attacker_id.0, 0);
        packets.push(writer.into_vec());
    }
    for monster_id in &outcome.monster_updates {
        let Some(monster) = world.monsters.get(monster_id) else {
            continue;
        };
        let percent = health_percent(monster.stats.health, monster.stats.max_health);
        packets.push(creature_health_packet(monster_id.0, percent));
    }
    packets
}

fn build_player_combat_packets(
    outcome: &PlayerCombatOutcome,
    world: &WorldState,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for effect in &outcome.effects {
        let mut writer = PacketWriter::new();
        game::write_graphical_effect(&mut writer, effect.position, effect.effect_id);
        packets.push(writer.into_vec());
    }
    for monster_id in &outcome.monster_updates {
        let Some(monster) = world.monsters.get(monster_id) else {
            continue;
        };
        let percent = health_percent(monster.stats.health, monster.stats.max_health);
        packets.push(creature_health_packet(monster_id.0, percent));
    }
    for player_id in &outcome.player_updates {
        let Some(player) = world.players.get(player_id) else {
            continue;
        };
        let percent = health_percent(player.stats.health, player.stats.max_health);
        packets.push(creature_health_packet(player_id.0, percent));
    }
    packets
}

fn build_moveuse_packets(
    outcome: &MoveUseOutcome,
    world: &WorldState,
    player_id: PlayerId,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for effect in &outcome.effects {
        let mut writer = PacketWriter::new();
        game::write_graphical_effect(&mut writer, effect.position, effect.effect_id);
        packets.push(writer.into_vec());
    }
    for text in &outcome.texts {
        let mut writer = PacketWriter::new();
        game::write_textual_effect(&mut writer, text.position, text.mode, &text.message);
        packets.push(writer.into_vec());
    }
    for edit in &outcome.edit_texts {
        let mut writer = PacketWriter::new();
        game::write_edit_text(
            &mut writer,
            edit.id,
            edit.item_type,
            edit.max_len,
            &edit.text,
            &edit.author,
            &edit.date,
        );
        packets.push(writer.into_vec());
    }
    for edit in &outcome.edit_lists {
        let mut writer = PacketWriter::new();
        game::write_edit_list(&mut writer, edit.list_type, edit.id, &edit.text);
        packets.push(writer.into_vec());
    }
    for message in &outcome.messages {
        if message.player_id != player_id {
            continue;
        }
        let mut writer = PacketWriter::new();
        game::write_message(&mut writer, message.message_type, &message.message);
        packets.push(writer.into_vec());
    }
    for damage in &outcome.damages {
        let MoveUseActor::User(target) = damage.target else {
            continue;
        };
        let Some(player) = world.players.get(&target) else {
            continue;
        };
        let percent = health_percent(player.stats.health, player.stats.max_health);
        packets.push(creature_health_packet(target.0, percent));
    }
    if !outcome.refresh_positions.is_empty() {
        if let Some(player) = world.players.get(&player_id) {
            let mut sent_field = false;
            let mut seen = HashSet::new();
            for position in &outcome.refresh_positions {
                if !seen.insert(*position) {
                    continue;
                }
                if !game::position_in_viewport(player.position, *position) {
                    continue;
                }
                let mut writer = PacketWriter::new();
                game::write_field_data(&mut writer, world, *position, player_id);
                packets.push(writer.into_vec());
                sent_field = true;
            }
            if !sent_field {
                let mut writer = PacketWriter::new();
                game::write_map_description(&mut writer, world, player.position, player_id);
                packets.push(writer.into_vec());
            }
        }
    }
    if !outcome.container_updates.is_empty() {
        packets.extend(build_container_update_packets(
            &outcome.container_updates,
            world.item_types.as_ref(),
        ));
    }
    packets
}

fn build_inventory_snapshot_packets(
    player: &PlayerState,
    item_types: Option<&ItemTypeIndex>,
) -> Vec<Vec<u8>> {
    let snapshot = snapshot_inventory(player);
    let mut packets = Vec::new();
    for (index, slot) in INVENTORY_SLOTS.iter().enumerate() {
        let entry = snapshot.get(index).and_then(|entry| entry.as_ref());
        match entry {
            Some(item) => {
                let mut writer = PacketWriter::new();
                game::write_inventory_set(&mut writer, *slot, item, item_types);
                packets.push(writer.into_vec());
            }
            None => {
                let mut writer = PacketWriter::new();
                game::write_inventory_reset(&mut writer, *slot);
                packets.push(writer.into_vec());
            }
        }
    }
    packets
}

fn build_buddy_list_packets(world: &WorldState, player_id: PlayerId) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for entry in world.buddy_list_entries(player_id) {
        let mut writer = PacketWriter::new();
        game::write_buddy_data(&mut writer, entry.id.0, &entry.name, entry.online);
        packets.push(writer.into_vec());
    }
    packets
}

fn build_saved_container_packets(
    world: &mut WorldState,
    player_id: PlayerId,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    let (item_type, slot) = {
        let Some(player) = world.players.get(&player_id) else {
            return packets;
        };
        if !player.open_containers.is_empty() {
            return packets;
        }
        let slot = InventorySlot::Backpack;
        let Some(item) = player.inventory.slot(slot) else {
            return packets;
        };
        let has_contents = player.inventory_containers.contains_key(&slot)
            || !item.contents.is_empty();
        if !has_contents {
            return packets;
        }
        (item.type_id, slot)
    };
    if let Ok(open) = world.open_container_for_player(
        player_id,
        item_type,
        crate::world::state::ContainerSource::InventorySlot(slot),
        None,
    ) {
        let mut writer = PacketWriter::new();
        game::write_open_container(&mut writer, &open, world.item_types.as_ref());
        packets.push(writer.into_vec());
    }
    packets
}

fn build_inventory_packets(
    before: &[Option<ItemStack>],
    after: &[Option<ItemStack>],
    item_types: Option<&ItemTypeIndex>,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for (index, slot) in INVENTORY_SLOTS.iter().enumerate() {
        let before_entry = before.get(index).and_then(|entry| entry.as_ref());
        let after_entry = after.get(index).and_then(|entry| entry.as_ref());
        if before_entry == after_entry {
            continue;
        }
        match after_entry {
            Some(item) => {
                let mut writer = PacketWriter::new();
                game::write_inventory_set(&mut writer, *slot, item, item_types);
                packets.push(writer.into_vec());
            }
            None => {
                let mut writer = PacketWriter::new();
                game::write_inventory_reset(&mut writer, *slot);
                packets.push(writer.into_vec());
            }
        }
    }
    packets
}

fn build_container_update_packets(
    updates: &[ContainerUpdate],
    item_types: Option<&ItemTypeIndex>,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for update in updates {
        let mut writer = PacketWriter::new();
        match update {
            ContainerUpdate::Add { container_id, item } => {
                game::write_container_add(&mut writer, *container_id, item, item_types);
            }
            ContainerUpdate::Update {
                container_id,
                slot,
                item,
            } => {
                game::write_container_update(
                    &mut writer,
                    *container_id,
                    *slot,
                    item,
                    item_types,
                );
            }
            ContainerUpdate::Remove { container_id, slot } => {
                game::write_container_remove(&mut writer, *container_id, *slot);
            }
        }
        packets.push(writer.into_vec());
    }
    packets
}

#[cfg(test)]
fn snapshot_monster_positions(world: &WorldState) -> HashMap<CreatureId, Position> {
    world
        .monsters
        .iter()
        .map(|(id, monster)| (*id, monster.position))
        .collect()
}

#[cfg(test)]
fn snapshot_npc_positions(world: &WorldState) -> HashMap<CreatureId, Position> {
    world
        .npcs
        .iter()
        .map(|(id, npc)| (*id, npc.position))
        .collect()
}

fn snapshot_creature_stacks(world: &WorldState) -> HashMap<Position, Vec<u32>> {
    let mut stacks: HashMap<Position, Vec<u32>> = HashMap::new();
    for player in world.players.values() {
        stacks.entry(player.position).or_default().push(player.id.0);
    }
    for npc in world.npcs.values() {
        stacks.entry(npc.position).or_default().push(npc.id.0);
    }
    for monster in world.monsters.values() {
        stacks
            .entry(monster.position)
            .or_default()
            .push(monster.id.0);
    }
    for stack in stacks.values_mut() {
        stack.sort_unstable();
    }
    stacks
}

fn creature_stack_pos_from_snapshot(
    world: &WorldState,
    stacks: &mut HashMap<Position, Vec<u32>>,
    position: Position,
    creature_id: u32,
) -> u8 {
    let items_len = world
        .map
        .tile(position)
        .map(|tile| tile.items.len())
        .unwrap_or(0);
    let entry = stacks.entry(position).or_default();
    match entry.binary_search(&creature_id) {
        Ok(_) => {}
        Err(index) => entry.insert(index, creature_id),
    }
    let index = entry
        .iter()
        .position(|id| *id == creature_id)
        .unwrap_or(0);
    let stack = items_len.saturating_add(index);
    stack.min(u8::MAX as usize) as u8
}

fn move_creature_in_snapshot(
    stacks: &mut HashMap<Position, Vec<u32>>,
    from: Position,
    to: Position,
    creature_id: u32,
) {
    if let Some(entry) = stacks.get_mut(&from) {
        if let Some(index) = entry.iter().position(|id| *id == creature_id) {
            entry.remove(index);
        }
        if entry.is_empty() {
            stacks.remove(&from);
        }
    }
    let entry = stacks.entry(to).or_default();
    if let Err(insert_at) = entry.binary_search(&creature_id) {
        entry.insert(insert_at, creature_id);
    }
}

fn apply_creature_steps(
    world: &WorldState,
    steps: &[CreatureStep],
    creature_stacks: &mut HashMap<Position, Vec<u32>>,
) -> Vec<CreatureMove> {
    let mut moves = Vec::with_capacity(steps.len());
    for step in steps {
        if step.from == step.to {
            continue;
        }
        let stack_pos =
            creature_stack_pos_from_snapshot(world, creature_stacks, step.from, step.id.0);
        move_creature_in_snapshot(creature_stacks, step.from, step.to, step.id.0);
        moves.push(CreatureMove {
            from: step.from,
            to: step.to,
            stack_pos,
        });
    }
    moves
}

#[cfg(test)]
fn collect_monster_moves(
    world: &WorldState,
    before_monsters: &HashMap<CreatureId, Position>,
    creature_stacks: &mut HashMap<Position, Vec<u32>>,
) -> Vec<CreatureMove> {
    let mut moves = Vec::new();
    for (id, from) in before_monsters {
        let Some(monster) = world.monsters.get(id) else {
            continue;
        };
        if monster.position == *from {
            continue;
        }
        let stack_pos =
            creature_stack_pos_from_snapshot(world, creature_stacks, *from, id.0);
        move_creature_in_snapshot(creature_stacks, *from, monster.position, id.0);
        moves.push(CreatureMove {
            from: *from,
            to: monster.position,
            stack_pos,
        });
    }
    moves
}

#[cfg(test)]
fn collect_npc_moves(
    world: &WorldState,
    before_npcs: &HashMap<CreatureId, Position>,
    creature_stacks: &mut HashMap<Position, Vec<u32>>,
) -> Vec<CreatureMove> {
    let mut moves = Vec::new();
    for (id, from) in before_npcs {
        let Some(npc) = world.npcs.get(id) else {
            continue;
        };
        if npc.position == *from {
            continue;
        }
        let stack_pos =
            creature_stack_pos_from_snapshot(world, creature_stacks, *from, id.0);
        move_creature_in_snapshot(creature_stacks, *from, npc.position, id.0);
        moves.push(CreatureMove {
            from: *from,
            to: npc.position,
            stack_pos,
        });
    }
    moves
}

fn snapshot_inventory(player: &PlayerState) -> Vec<Option<ItemStack>> {
    INVENTORY_SLOTS
        .iter()
        .map(|slot| player.inventory.slot(*slot).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::creature::Outfit;
    use crate::entities::inventory::Inventory;
    use crate::entities::stats::Stats;
    use crate::world::position::Direction;
    use crate::world::monsters::MonsterLootTable;
    use crate::world::time::{Cooldown, GameTick};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn send_ws_handshake(stream: &mut TcpStream) -> Result<(), String> {
        let request = concat!(
            "GET /game HTTP/1.1\r\n",
            "Host: localhost\r\n",
            "Upgrade: websocket\r\n",
            "Connection: Upgrade\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "Origin: http://localhost\r\n",
            "\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|err| format!("handshake write failed: {err}"))?;
        let mut response = Vec::new();
        let mut buf = [0u8; 256];
        loop {
            let read = stream
                .read(&mut buf)
                .map_err(|err| format!("handshake read failed: {err}"))?;
            if read == 0 {
                break;
            }
            response.extend_from_slice(&buf[..read]);
            if response.windows(4).any(|chunk| chunk == b"\r\n\r\n") {
                break;
            }
        }
        let response_text = String::from_utf8_lossy(&response);
        if !response_text.starts_with("HTTP/1.1 101") {
            return Err(format!("unexpected handshake response: {response_text}"));
        }
        Ok(())
    }

    fn write_masked_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> Result<(), String> {
        let len = payload.len();
        if len >= 126 {
            return Err("test helper only supports payload < 126 bytes".to_string());
        }
        let mask = [0x12, 0x34, 0x56, 0x78];
        let mut header = vec![0x80 | (opcode & 0x0f), 0x80 | (len as u8)];
        header.extend_from_slice(&mask);
        let mut masked = Vec::with_capacity(len);
        for (idx, byte) in payload.iter().enumerate() {
            masked.push(byte ^ mask[idx % 4]);
        }
        stream
            .write_all(&header)
            .and_then(|_| stream.write_all(&masked))
            .map_err(|err| format!("write masked frame failed: {err}"))?;
        Ok(())
    }

    fn write_masked_packet(stream: &mut TcpStream, body: &[u8]) -> Result<(), String> {
        let len_u16 = u16::try_from(body.len()).map_err(|_| "packet too large".to_string())?;
        let mut framed = Vec::with_capacity(2 + body.len());
        framed.push((len_u16 & 0xff) as u8);
        framed.push((len_u16 >> 8) as u8);
        framed.extend_from_slice(body);
        write_masked_frame(stream, 0x2, &framed)
    }

    fn read_ws_packet(stream: &mut TcpStream, max_payload: usize) -> Result<Vec<u8>, String> {
        let frame = ws::read_frame(stream, max_payload)
            .map_err(|err| format!("read ws frame failed: {err:?}"))?;
        if frame.opcode != 0x2 {
            return Err(format!("unexpected ws opcode {:02x}", frame.opcode));
        }
        if frame.payload.len() < 2 {
            return Err("ws packet payload too short".to_string());
        }
        let len = u16::from_le_bytes([frame.payload[0], frame.payload[1]]) as usize;
        if len != frame.payload.len() - 2 {
            return Err("ws packet length mismatch".to_string());
        }
        Ok(frame.payload[2..].to_vec())
    }

    fn read_ws_packets(stream: &mut TcpStream, count: usize) -> Result<(), String> {
        for _ in 0..count {
            read_ws_packet(stream, 8192)?;
        }
        Ok(())
    }

    fn read_tcp_packets(stream: &mut TcpStream, count: usize) -> Result<(), String> {
        for _ in 0..count {
            match read_packet(stream, 8192, None)? {
                ReadPacketOutcome::Packet(_) => {}
                ReadPacketOutcome::Timeout => {
                    return Err("tcp packet read timed out".to_string());
                }
            }
        }
        Ok(())
    }

    fn build_game_login_packet(account: &str, character: &str, password: &str) -> Vec<u8> {
        let mut writer = PacketWriter::new();
        writer.write_u8(crate::net::game_login::OPCODE_GAME_LOGIN);
        writer.write_u16_le(0x0001);
        writer.write_u16_le(0x0304);
        writer.write_u8(0);
        writer.write_u32_le(0);
        writer.write_u32_le(0);
        writer.write_u32_le(0);
        writer.write_u32_le(0);
        writer.write_u8(0);
        writer.write_string_str(account);
        writer.write_string_str(character);
        writer.write_string_str(password);
        writer.into_vec()
    }

    #[test]
    fn collect_monster_moves_detects_position_change() {
        let mut world = WorldState::default();
        let player_id = PlayerId(1);
        let origin = Position { x: 100, y: 100, z: 7 };
        world
            .players
            .insert(player_id, PlayerState::new(player_id, "Tester".to_string(), origin));

        let monster_id = CreatureId(2);
        world.monsters.insert(
            monster_id,
            MonsterInstance {
                id: monster_id,
                race_number: 1,
                summoner: None,
                summoned: false,
                home_id: None,
                name: "Rat".to_string(),
                position: origin,
                direction: Direction::South,
                outfit: crate::entities::creature::DEFAULT_OUTFIT,
                stats: Stats::default(),
                experience: 0,
                loot: MonsterLootTable::default(),
                inventory: Inventory::default(),
                inventory_containers: HashMap::new(),
                corpse_ids: Vec::new(),
                flags: crate::world::monsters::MonsterFlags::default(),
                skills: crate::world::monsters::MonsterSkills::default(),
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

        let before_monsters = snapshot_monster_positions(&world);
        let mut creature_stacks = snapshot_creature_stacks(&world);
        let new_pos = Position { x: 101, y: 100, z: 7 };
        world.monsters.get_mut(&monster_id).unwrap().position = new_pos;

        let moves = collect_monster_moves(&world, &before_monsters, &mut creature_stacks);
        assert_eq!(moves.len(), 1);
        let movement = &moves[0];
        assert_eq!(movement.id, monster_id);
        assert_eq!(movement.from, origin);
        assert_eq!(movement.to, new_pos);
        assert_eq!(movement.stack_pos, 1);
    }

    #[test]
    fn collect_npc_moves_detects_teleport() {
        let mut world = WorldState::default();
        let npc_id = CreatureId(5);
        let origin = Position { x: 50, y: 50, z: 7 };
        let destination = Position { x: 150, y: 150, z: 7 };
        world.npcs.insert(
            npc_id,
            NpcInstance {
                id: npc_id,
                script_key: "npc".to_string(),
                name: "Guide".to_string(),
                position: origin,
                home: origin,
                outfit: Outfit::default(),
                radius: 3,
                focused: None,
                focus_expires_at: None,
                queue: std::collections::VecDeque::new(),
                move_cooldown: Cooldown::new(GameTick(0)),
            },
        );

        let before_npcs = snapshot_npc_positions(&world);
        let mut creature_stacks = snapshot_creature_stacks(&world);
        world.npcs.get_mut(&npc_id).unwrap().position = destination;

        let moves = collect_npc_moves(&world, &before_npcs, &mut creature_stacks);
        assert_eq!(moves.len(), 1);
        let movement = &moves[0];
        assert_eq!(movement.id, npc_id);
        assert_eq!(movement.from, origin);
        assert_eq!(movement.to, destination);
    }

    #[test]
    fn ws_transport_handles_ping_and_packet_exchange() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ws test listener");
        let addr = listener.local_addr().expect("listener addr");
        let (tx, rx) = mpsc::channel();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept ws connection");
            let ws_config = ws::WsHandshakeConfig::default();
            let mut transport = WsPacketTransport::accept(stream, &ws_config)
                .expect("ws transport accept");
            let outcome = transport
                .read_packet(1024, None)
                .expect("read packet");
            let packet = match outcome {
                ReadPacketOutcome::Packet(payload) => payload,
                _ => panic!("unexpected read outcome"),
            };
            transport
                .write_packet(&[0xAA, 0xBB], None)
                .expect("write packet");
            tx.send(packet).expect("send packet");
        });

        let mut client = TcpStream::connect(addr).expect("connect ws test");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        send_ws_handshake(&mut client).expect("handshake");

        write_masked_frame(&mut client, 0x9, b"ping").expect("send ping frame");
        write_masked_packet(&mut client, &[0x01, 0x02, 0x03]).expect("send packet");

        let frame = ws::read_frame(&mut client, 1024).expect("read pong");
        assert_eq!(frame.opcode, 0xA);
        assert_eq!(frame.payload, b"ping");

        let response = ws::read_frame(&mut client, 1024).expect("read response packet");
        assert_eq!(response.opcode, 0x2);
        assert!(response.payload.len() >= 2);
        let len = u16::from_le_bytes([response.payload[0], response.payload[1]]) as usize;
        assert_eq!(len, response.payload.len() - 2);
        assert_eq!(&response.payload[2..], &[0xAA, 0xBB]);

        let packet = rx.recv_timeout(Duration::from_secs(2)).expect("packet recv");
        assert_eq!(packet, vec![0x01, 0x02, 0x03]);

        server.join().expect("server join");
    }

    #[test]
    fn ws_login_flow_sends_init_packets() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ws login listener");
        let addr = listener.local_addr().expect("listener addr");
        let (tx, rx) = mpsc::channel();

        let mut config = GameServerConfig::default();
        config.read_timeout = Duration::from_millis(200);
        config.write_timeout = Duration::from_secs(2);
        config.idle_warning_after = None;

        let world = Arc::new(Mutex::new(WorldState::default()));
        let control = Arc::new(ServerControl::new());
        let state = GameServerState::new();
        let server_world = Arc::clone(&world);
        let server_control = Arc::clone(&control);

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept ws login connection");
            let ws_config = ws::WsHandshakeConfig::default();
            let result = handle_game_ws_connection(
                stream,
                &config,
                &ws_config,
                &state,
                &server_world,
                &server_control,
            );
            tx.send(result).expect("send ws login result");
        });

        let mut client = TcpStream::connect(addr).expect("connect ws login test");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        send_ws_handshake(&mut client).expect("handshake");

        let login_packet = build_game_login_packet("1", "Tester", "pw");
        write_masked_packet(&mut client, &login_packet).expect("send login packet");

        let init_packet = read_ws_packet(&mut client, 4096).expect("read init packet");
        assert_eq!(init_packet.first().copied(), Some(game::OPCODE_INIT_GAME));

        let rights_packet = read_ws_packet(&mut client, 4096).expect("read rights packet");
        assert_eq!(rights_packet.first().copied(), Some(game::OPCODE_RIGHTS));

        let map_packet = read_ws_packet(&mut client, 8192).expect("read map packet");
        assert_eq!(
            map_packet.first().copied(),
            Some(game::OPCODE_MAP_DESCRIPTION)
        );

        control.request_shutdown();
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("receive ws login result");
        assert!(result.is_ok(), "ws login flow failed: {result:?}");

        server.join().expect("server join");
    }

    #[test]
    fn tcp_disconnect_allows_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp reconnect listener");
        let addr = listener.local_addr().expect("listener addr");
        let (tx, rx) = mpsc::channel();

        let mut config = GameServerConfig::default();
        config.read_timeout = Duration::from_millis(200);
        config.write_timeout = Duration::from_secs(2);
        config.idle_warning_after = None;

        let world = Arc::new(Mutex::new(WorldState::default()));
        let control = Arc::new(ServerControl::new());
        let server_world = Arc::clone(&world);
        let server_control = Arc::clone(&control);

        let server = thread::spawn(move || {
            let state = GameServerState::new();
            let (stream, _) = listener.accept().expect("accept tcp connection 1");
            let result1 =
                handle_game_connection(stream, &config, &state, &server_world, &server_control);
            let count1 = server_world
                .lock()
                .map(|world| world.players.len())
                .unwrap_or(usize::MAX);

            let (stream, _) = listener.accept().expect("accept tcp connection 2");
            let result2 =
                handle_game_connection(stream, &config, &state, &server_world, &server_control);
            let count2 = server_world
                .lock()
                .map(|world| world.players.len())
                .unwrap_or(usize::MAX);

            tx.send((result1, count1, result2, count2))
                .expect("send tcp results");
        });

        for _ in 0..2 {
            let mut client = TcpStream::connect(addr).expect("connect tcp test");
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let login_packet = build_game_login_packet("1", "Tester", "pw");
            write_packet(&mut client, &login_packet, None).expect("send login packet");
            read_tcp_packets(&mut client, 3).expect("read init packets");
            let _ = client.shutdown(std::net::Shutdown::Both);
        }

        let (result1, count1, result2, count2) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("receive tcp results");
        assert!(result1.is_err(), "expected disconnect error: {result1:?}");
        assert_eq!(count1, 0, "expected cleanup after tcp disconnect");
        assert!(result2.is_err(), "expected disconnect error: {result2:?}");
        assert_eq!(count2, 0, "expected cleanup after tcp reconnect");

        server.join().expect("server join");
    }

    #[test]
    fn ws_disconnect_allows_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ws reconnect listener");
        let addr = listener.local_addr().expect("listener addr");
        let (tx, rx) = mpsc::channel();

        let mut config = GameServerConfig::default();
        config.read_timeout = Duration::from_millis(200);
        config.write_timeout = Duration::from_secs(2);
        config.idle_warning_after = None;

        let world = Arc::new(Mutex::new(WorldState::default()));
        let control = Arc::new(ServerControl::new());
        let server_world = Arc::clone(&world);
        let server_control = Arc::clone(&control);

        let server = thread::spawn(move || {
            let state = GameServerState::new();
            let ws_config = ws::WsHandshakeConfig::default();
            let (stream, _) = listener.accept().expect("accept ws connection 1");
            let result1 = handle_game_ws_connection(
                stream,
                &config,
                &ws_config,
                &state,
                &server_world,
                &server_control,
            );
            let count1 = server_world
                .lock()
                .map(|world| world.players.len())
                .unwrap_or(usize::MAX);

            let (stream, _) = listener.accept().expect("accept ws connection 2");
            let result2 = handle_game_ws_connection(
                stream,
                &config,
                &ws_config,
                &state,
                &server_world,
                &server_control,
            );
            let count2 = server_world
                .lock()
                .map(|world| world.players.len())
                .unwrap_or(usize::MAX);

            tx.send((result1, count1, result2, count2))
                .expect("send ws results");
        });

        for _ in 0..2 {
            let mut client = TcpStream::connect(addr).expect("connect ws test");
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            send_ws_handshake(&mut client).expect("handshake");
            let login_packet = build_game_login_packet("1", "Tester", "pw");
            write_masked_packet(&mut client, &login_packet).expect("send login packet");
            read_ws_packets(&mut client, 3).expect("read init packets");
            write_masked_frame(&mut client, 0x8, &[]).expect("send close frame");
            let _ = client.shutdown(std::net::Shutdown::Both);
        }

        let (result1, count1, result2, count2) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("receive ws results");
        assert!(result1.is_err(), "expected disconnect error: {result1:?}");
        assert_eq!(count1, 0, "expected cleanup after ws disconnect");
        assert!(result2.is_err(), "expected disconnect error: {result2:?}");
        assert_eq!(count2, 0, "expected cleanup after ws reconnect");

        server.join().expect("server join");
    }
}

fn build_condition_tick_packets(
    world: &WorldState,
    ticks: &[(PlayerId, Vec<ConditionTick>)],
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for (player_id, player_ticks) in ticks {
        if player_ticks.is_empty() {
            continue;
        }
        let Some(player) = world.players.get(player_id) else {
            continue;
        };
        let percent = health_percent(player.stats.health, player.stats.max_health);
        packets.push(creature_health_packet(player_id.0, percent));
    }
    packets
}

fn build_status_update_packets(
    updates: &crate::world::state::CreatureStatusUpdates,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for update in &updates.outfit_updates {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_creature_outfit(&mut writer, update.id, update.outfit);
        packets.push(writer.into_vec());
    }
    for update in &updates.speed_updates {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_creature_speed(&mut writer, update.id, update.speed);
        packets.push(writer.into_vec());
    }
    for update in &updates.light_updates {
        let mut writer = crate::net::packet::PacketWriter::new();
        game::write_creature_light(&mut writer, update.id, update.level, update.color);
        packets.push(writer.into_vec());
    }
    packets
}

fn player_state_flags(player: &mut PlayerState, clock: &GameClock) -> u8 {
    let mut poisoned = false;
    let mut burning = false;
    let mut electrified = false;
    for condition in &player.conditions {
        match condition.kind {
            ConditionKind::Poison => poisoned = true,
            ConditionKind::Fire => burning = true,
            ConditionKind::Energy => electrified = true,
            _ => {}
        }
    }
    if !poisoned && skill_timer_active(player, SKILL_POISON) {
        poisoned = true;
    }
    if !burning && skill_timer_active(player, SKILL_BURNING) {
        burning = true;
    }
    if !electrified && skill_timer_active(player, SKILL_ENERGY) {
        electrified = true;
    }
    let mut flags = 0u8;
    if poisoned {
        flags |= 1 << 0;
    }
    if burning {
        flags |= 1 << 1;
    }
    if electrified {
        flags |= 1 << 2;
    }
    if player.drunken_effect.is_some() || skill_timer_active(player, SKILL_DRUNKEN) {
        flags |= 1 << 3;
    }
    if player.magic_shield_effect.is_some() || skill_timer_active(player, SKILL_MANASHIELD) {
        flags |= 1 << 4;
    }
    if let Some(effect) = player.speed_effect {
        if effect.speed < effect.original_speed {
            flags |= 1 << 5;
        } else if effect.speed > effect.original_speed {
            flags |= 1 << 6;
        }
    }
    if player.in_combat(clock) {
        flags |= 1 << 7;
    }
    flags
}

fn skill_timer_active(player: &PlayerState, skill_id: u32) -> bool {
    player
        .raw_skills
        .iter()
        .find(|row| row.skill_id == skill_id)
        .map(|row| row.values[SKILL_FIELD_MIN] != i32::MIN && row.values[SKILL_FIELD_CYCLE] != 0)
        .unwrap_or(false)
}

fn creature_health_packet(creature_id: u32, percent: u8) -> Vec<u8> {
    let mut writer = crate::net::packet::PacketWriter::new();
    game::write_creature_health(&mut writer, creature_id, percent);
    writer.into_vec()
}

fn health_percent(health: u32, max_health: u32) -> u8 {
    let max = max_health.max(1);
    (health.saturating_mul(100) / max).min(100) as u8
}

fn send_response<T: PacketTransport>(
    transport: &mut T,
    response: &crate::net::login_flow::LoginResponse,
    trace: Option<&mut PacketTrace>,
) -> Result<(), String> {
    let body = response.to_bytes()?;
    transport
        .write_packet(&body, trace)
        .map_err(|err| format!("send login response failed: {}", err))
}

enum ReadPacketOutcome {
    Packet(Vec<u8>),
    Timeout,
}

fn read_packet(
    stream: &mut TcpStream,
    max_len: usize,
    mut trace: Option<&mut PacketTrace>,
) -> Result<ReadPacketOutcome, String> {
    let mut header = [0u8; 2];
    if let Err(err) = stream.read_exact(&mut header) {
        if matches!(err.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) {
            return Ok(ReadPacketOutcome::Timeout);
        }
        return Err(format!("header read failed: {}", err));
    }
    let len = u16::from_le_bytes(header) as usize;
    if len == 0 {
        return Err("packet length is zero".to_string());
    }
    if len > max_len {
        return Err(format!("packet length {} exceeds max {}", len, max_len));
    }

    let mut body = vec![0u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|err| format!("payload read failed: {}", err))?;
    if let Some(trace) = trace.as_mut() {
        trace.record("in", &body);
    }
    Ok(ReadPacketOutcome::Packet(body))
}

fn write_packet(
    stream: &mut TcpStream,
    body: &[u8],
    mut trace: Option<&mut PacketTrace>,
) -> std::io::Result<()> {
    let len = body.len();
    let len_u16 = u16::try_from(len).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "packet too large")
    })?;
    stream.write_all(&len_u16.to_le_bytes())?;
    stream.write_all(body)?;
    if let Some(trace) = trace.as_mut() {
        trace.record("out", body);
    }
    Ok(())
}

struct PacketTrace {
    file: std::fs::File,
}

impl PacketTrace {
    fn new(
        root: Option<&PathBuf>,
        kind: &str,
        peer: Option<std::net::SocketAddr>,
    ) -> Option<Self> {
        if !trace_enabled() {
            return None;
        }
        let root = root?;
        let id = TRACE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let file_name = format!("packet_trace_{kind}_{id}.log");
        let path = root.join("log").join(file_name);
        let mut file = OpenOptions::new().create(true).append(true).open(path).ok()?;
        let timestamp = unix_millis();
        let peer = peer
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let header = format!("# packet trace {kind} {id} peer={peer} ts={timestamp}\n");
        let _ = file.write_all(header.as_bytes());
        Some(Self { file })
    }

    fn record(&mut self, direction: &str, body: &[u8]) {
        let ts = unix_millis();
        let len = body.len();
        let max = TRACE_MAX_BYTES.min(len);
        let mut line = String::with_capacity(64 + max * 3);
        line.push_str(&format!("{ts} {direction} len={len}"));
        if len > TRACE_MAX_BYTES {
            line.push_str(&format!(" trunc={}", len - TRACE_MAX_BYTES));
        }
        line.push_str(" data=");
        for (idx, byte) in body[..max].iter().enumerate() {
            if idx > 0 {
                line.push(' ');
            }
            let _ = write!(line, "{:02x}", byte);
        }
        line.push('\n');
        let _ = self.file.write_all(line.as_bytes());
        let _ = self.file.flush();
    }
}

fn trace_enabled() -> bool {
    match std::env::var(TRACE_ENV) {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            !value.is_empty() && value != "0" && value != "false" && value != "off"
        }
        Err(_) => false,
    }
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
