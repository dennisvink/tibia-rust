mod assets;
pub mod admin;
pub mod combat;
mod config;
pub mod entities;
mod net;
pub mod persistence;
pub mod scripting;
pub mod telemetry;
pub mod world;

pub use net::packet::{PacketReader, PacketWriter};
pub use net::login::{
    build_login_message, parse_login, parse_login_packet_v1, parse_login_payload_v1,
    build_login_success_v1, LoginBuildError, LoginParseError, LoginPayloadV1, LoginRequest,
    LoginSuccessV1,
};
pub use net::login_flow::{
    build_login_success, handle_login_packet_v1, LoginDecision, LoginErrorKind, LoginFlowConfig,
    LoginResponse,
};
pub use net::server::{
    run_game_server, run_login_server, run_status_server, GameServerConfig, LoginServerConfig,
    ServerControl, ServerExit, StatusServerConfig,
};

pub fn run(args: &[String]) -> Result<(), String> {
    loop {
        let config = config::AppConfig::from_args(args)?;
        telemetry::logging::init(&config.root)?;
        let summary = assets::scan(&config.root)?;
        let world = std::sync::Arc::new(std::sync::Mutex::new(world::state::WorldState::load(
            &config.root,
        )?));
        let login_registry = std::sync::Arc::new(net::server::LoginRegistry::new());

        {
            let world = world
                .lock()
                .map_err(|_| "world lock poisoned".to_string())?;
            let save_report =
                persistence::store::SaveStore::from_root(&config.root).validate_player_saves();
            let npc_report = world::npc::validate_npcs(&config.root.join("npc"));
            let monster_report = world::monsters::validate_monsters(&config.root.join("mon"));
            telemetry::logging::log_game(&format!(
                "asset scan: dat={}, map={}, npc={}, mon={}, save={}",
                summary.dat_files,
                summary.map_files,
                summary.npc_files,
                summary.mon_files,
                summary.save_files
            ));
            println!("tibia: asset scan");
            println!("- root: {}", config.root.display());
            println!("- dat files: {}", summary.dat_files);
            println!("- map files: {}", summary.map_files);
            println!("- npc files: {}", summary.npc_files);
            println!("- mon files: {}", summary.mon_files);
            println!("- save files: {}", summary.save_files);
            if save_report.missing_dir {
                println!("- save players: missing save/players directory");
            } else {
                println!(
                    "- save players: files={}, parsed={}, errors={}, skipped={}",
                    save_report.player_files,
                    save_report.parsed,
                    save_report.errors.len(),
                    save_report.skipped
                );
            }
            if !save_report.errors.is_empty() {
                for err in &save_report.errors {
                    eprintln!("tibia: save validate {}", err);
                }
            }
            println!(
                "- npc scripts: files={}, parsed={}, errors={}",
                npc_report.files,
                npc_report.parsed,
                npc_report.errors.len()
            );
            if !npc_report.errors.is_empty() {
                for err in &npc_report.errors {
                    eprintln!("tibia: npc validate {}", err);
                }
            }
            println!(
                "- monster scripts: mon_files={}, parsed_monsters={}, raid_files={}, parsed_raids={}, errors={}",
                monster_report.monster_files,
                monster_report.parsed_monsters,
                monster_report.raid_files,
                monster_report.parsed_raids,
                monster_report.errors.len()
            );
            if !monster_report.errors.is_empty() {
                for err in &monster_report.errors {
                    eprintln!("tibia: monster validate {}", err);
                }
            }
            println!("- map sectors: {}", world.map.sector_count());
            if let Some(bounds) = world.map.sector_bounds {
                println!(
                    "- map sector bounds: ({},{},{}) -> ({},{},{})",
                    bounds.min.x,
                    bounds.min.y,
                    bounds.min.z,
                    bounds.max.x,
                    bounds.max.y,
                    bounds.max.z
                );
            }
            if let Some(map_dat) = world.map_dat.as_ref() {
                if let Some(bounds) = map_dat.sector_bounds {
                    println!(
                        "- map.dat sector bounds: ({},{},{}) -> ({},{},{})",
                        bounds.min.x,
                        bounds.min.y,
                        bounds.min.z,
                        bounds.max.x,
                        bounds.max.y,
                        bounds.max.z
                    );
                }
                if let Some(position) = map_dat.newbie_start {
                    println!(
                        "- map.dat newbie start: ({},{},{})",
                        position.x, position.y, position.z
                    );
                }
                if let Some(position) = map_dat.veteran_start {
                    println!(
                        "- map.dat veteran start: ({},{},{})",
                        position.x, position.y, position.z
                    );
                }
            }
            if let Some(mem_dat) = world.mem_dat.as_ref() {
                if let Some(objects) = mem_dat.objects {
                    println!("- mem.dat objects: {}", objects);
                }
                if let Some(cache_size) = mem_dat.cache_size {
                    println!("- mem.dat cache size: {}", cache_size);
                }
            }
            if let Some(circles) = world.circles.as_ref() {
                println!(
                    "- circles.dat: {}x{} max_radius {}",
                    circles.width, circles.height, circles.max_radius
                );
            }
            if let Some(item_types) = world.item_types.as_ref() {
                println!("- item types: {}", item_types.len());
            }
        }

        let autosave_interval_seconds = match std::env::var("TIBIA_AUTOSAVE_SECS") {
            Ok(value) => match value.trim().parse::<u64>() {
                Ok(parsed) => parsed,
                Err(_) => {
                    eprintln!("tibia: invalid TIBIA_AUTOSAVE_SECS '{}', autosave disabled", value);
                    0
                }
            },
            Err(_) => 0,
        };
        let world_name = std::env::var("TIBIA_WORLD_NAME").unwrap_or_else(|_| "World".to_string());
        let world_addr = config
            .ws_game_bind_addr
            .clone()
            .or_else(|| Some(config.game_bind_addr.clone()));
        let server_config = LoginServerConfig {
            bind_addr: config.login_bind_addr.clone(),
            ws_bind_addr: config.ws_login_bind_addr.clone(),
            ws_allowed_origins: config.ws_allowed_origins.clone(),
            root: Some(config.root.clone()),
            login_registry: Some(std::sync::Arc::clone(&login_registry)),
            world_name: world_name.clone(),
            world_addr,
            ..LoginServerConfig::default()
        };
        let game_config = GameServerConfig {
            bind_addr: config.game_bind_addr.clone(),
            autosave_interval_seconds,
            ws_bind_addr: config.ws_game_bind_addr.clone(),
            ws_allowed_origins: config.ws_allowed_origins.clone(),
            root: Some(config.root.clone()),
            login_registry: Some(std::sync::Arc::clone(&login_registry)),
            ..GameServerConfig::default()
        };
        let control = std::sync::Arc::new(ServerControl::new());
        let game_state = std::sync::Arc::new(net::server::GameServerState::new());
        let game_world = std::sync::Arc::clone(&world);
        let game_control = std::sync::Arc::clone(&control);
        let game_state_for_game = std::sync::Arc::clone(&game_state);
        let login_state = net::server::build_login_state(&server_config)?;
        let game_handle = std::thread::spawn(move || {
            net::server::run_game_server_with_state(
                game_config,
                game_world,
                game_control,
                std::sync::Arc::clone(&game_state_for_game),
            )
        });
        let ws_handle = if config.ws_game_bind_addr.is_some() {
            let ws_config = GameServerConfig {
                bind_addr: config.game_bind_addr.clone(),
                autosave_interval_seconds,
                ws_bind_addr: config.ws_game_bind_addr.clone(),
                ws_allowed_origins: config.ws_allowed_origins.clone(),
                root: Some(config.root.clone()),
                login_registry: Some(std::sync::Arc::clone(&login_registry)),
                ..GameServerConfig::default()
            };
            let ws_world = std::sync::Arc::clone(&world);
            let ws_control = std::sync::Arc::clone(&control);
            let ws_state = std::sync::Arc::clone(&game_state);
            Some(std::thread::spawn(move || {
                net::server::run_game_ws_server(ws_config, ws_world, ws_control, ws_state)
            }))
        } else {
            None
        };
        let login_ws_handle = if config.ws_login_bind_addr.is_some() {
            let ws_config = LoginServerConfig {
                bind_addr: config.login_bind_addr.clone(),
                ws_bind_addr: config.ws_login_bind_addr.clone(),
                ws_allowed_origins: config.ws_allowed_origins.clone(),
                root: Some(config.root.clone()),
                login_registry: Some(std::sync::Arc::clone(&login_registry)),
                ..LoginServerConfig::default()
            };
            let ws_control = std::sync::Arc::clone(&control);
            let ws_state = std::sync::Arc::clone(&login_state);
            Some(std::thread::spawn(move || {
                net::server::run_login_ws_server(ws_config, ws_control, ws_state)
            }))
        } else {
            None
        };
        let status_handle = if let Some(status_bind_addr) = config.status_bind_addr.clone() {
            let max_players = std::env::var("TIBIA_MAX_PLAYERS")
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok())
                .unwrap_or(0);
            let status_config = StatusServerConfig {
                bind_addr: status_bind_addr,
                server_name: world_name.clone(),
                login_addr: config.login_bind_addr.clone(),
                max_players,
                ..StatusServerConfig::default()
            };
            let status_world = std::sync::Arc::clone(&world);
            let status_control = std::sync::Arc::clone(&control);
            Some(std::thread::spawn(move || {
                net::server::run_status_server(status_config, status_world, status_control)
            }))
        } else {
            None
        };
        let exit = net::server::run_login_server_with_state(
            server_config,
            std::sync::Arc::clone(&control),
            std::sync::Arc::clone(&login_state),
        )?;

        match game_handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => eprintln!("game server error: {}", err),
            Err(_) => eprintln!("game server thread panicked"),
        }
        if let Some(ws_handle) = ws_handle {
            match ws_handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("game ws server error: {}", err),
                Err(_) => eprintln!("game ws server thread panicked"),
            }
        }
        if let Some(login_ws_handle) = login_ws_handle {
            match login_ws_handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("login ws server error: {}", err),
                Err(_) => eprintln!("login ws server thread panicked"),
            }
        }
        if let Some(status_handle) = status_handle {
            match status_handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("status server error: {}", err),
                Err(_) => eprintln!("status server thread panicked"),
            }
        }

        match exit {
            ServerExit::Shutdown => return Ok(()),
            ServerExit::Restart => {
                println!("tibia: restart requested, relaunching");
            }
        }
    }
}
