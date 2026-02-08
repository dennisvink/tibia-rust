use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct AppConfig {
    pub root: PathBuf,
    pub login_bind_addr: String,
    pub game_bind_addr: String,
    pub ws_game_bind_addr: Option<String>,
    pub ws_login_bind_addr: Option<String>,
    pub status_bind_addr: Option<String>,
    pub ws_allowed_origins: Option<Vec<String>>,
}

impl AppConfig {
    pub fn from_args(args: &[String]) -> Result<Self, String> {
        if args.len() < 2 {
            return Err(
                "usage: tibia <asset-root> [login_bind_addr] [game_bind_addr] [ws_game_bind_addr] [ws_login_bind_addr] [status_bind_addr]"
                    .to_string(),
            );
        }

        let root = Path::new(&args[1]).to_path_buf();
        let login_bind_addr = if args.len() > 2 {
            args[2].clone()
        } else {
            "0.0.0.0:7171".to_string()
        };
        let game_bind_addr = if args.len() > 3 {
            args[3].clone()
        } else {
            "0.0.0.0:7172".to_string()
        };
        let ws_game_bind_addr = if args.len() > 4 {
            Some(args[4].clone())
        } else {
            std::env::var("TIBIA_WS_GAME_ADDR")
                .ok()
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .or_else(|| derive_ws_bind_addr(&game_bind_addr))
        };
        let ws_login_bind_addr = if args.len() > 5 {
            Some(args[5].clone())
        } else {
            std::env::var("TIBIA_WS_LOGIN_ADDR")
                .ok()
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .or_else(|| derive_ws_login_bind_addr(ws_game_bind_addr.as_deref()))
        };
        let status_bind_addr = if args.len() > 6 {
            Some(args[6].clone())
        } else {
            std::env::var("TIBIA_STATUS_ADDR")
                .ok()
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
        };
        let ws_allowed_origins = std::env::var("TIBIA_WS_ORIGINS")
            .ok()
            .and_then(|value| {
                let entries: Vec<String> = value
                    .split(',')
                    .map(|entry| entry.trim())
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| entry.to_string())
                    .collect();
                if entries.is_empty() {
                    None
                } else {
                    Some(entries)
                }
            });
        Ok(Self {
            root,
            login_bind_addr,
            game_bind_addr,
            ws_game_bind_addr,
            ws_login_bind_addr,
            status_bind_addr,
            ws_allowed_origins,
        })
    }
}

fn derive_ws_bind_addr(game_bind_addr: &str) -> Option<String> {
    let (host, port_str) = game_bind_addr.rsplit_once(':')?;
    let port: u16 = port_str.parse().ok()?;
    let ws_port = port.saturating_add(1);
    Some(format!("{host}:{ws_port}"))
}

fn derive_ws_login_bind_addr(ws_game_bind_addr: Option<&str>) -> Option<String> {
    let ws_game_bind_addr = ws_game_bind_addr?;
    let (host, port_str) = ws_game_bind_addr.rsplit_once(':')?;
    let port: u16 = port_str.parse().ok()?;
    let ws_port = port.saturating_add(1);
    Some(format!("{host}:{ws_port}"))
}
