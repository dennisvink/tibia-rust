use crate::world::position::Position;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminCommand {
    Kick { target: Option<String> },
    Online,
    MoveUseAudit,
    Restart,
    Shutdown,
    Teleport { position: Position },
    Where,
    Unknown(String),
}

pub fn parse_admin_command(message: &str) -> Result<Option<AdminCommand>, String> {
    let trimmed = message.trim();
    if !trimmed.starts_with('!') {
        return Ok(None);
    }

    let mut parts = trimmed[1..].split_whitespace();
    let command = parts
        .next()
        .ok_or_else(|| "admin command missing name".to_string())?;
    let command = command.to_ascii_lowercase();
    let parsed = match command.as_str() {
        "kick" => AdminCommand::Kick {
            target: parts.next().map(str::to_string),
        },
        "online" => AdminCommand::Online,
        "moveuseaudit" | "muaudit" => AdminCommand::MoveUseAudit,
        "restart" => AdminCommand::Restart,
        "shutdown" => AdminCommand::Shutdown,
        "teleport" | "tp" => {
            let x = parse_u16(parts.next())?;
            let y = parse_u16(parts.next())?;
            let z = parse_u8(parts.next())?;
            AdminCommand::Teleport {
                position: Position { x, y, z },
            }
        }
        "where" | "pos" => AdminCommand::Where,
        _ => AdminCommand::Unknown(command),
    };
    Ok(Some(parsed))
}

fn parse_u16(value: Option<&str>) -> Result<u16, String> {
    let value = value.ok_or_else(|| "admin command missing position value".to_string())?;
    value
        .parse::<u16>()
        .map_err(|_| format!("admin command expected u16, got '{value}'"))
}

fn parse_u8(value: Option<&str>) -> Result<u8, String> {
    let value = value.ok_or_else(|| "admin command missing position value".to_string())?;
    value
        .parse::<u8>()
        .map_err(|_| format!("admin command expected u8, got '{value}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_admin_command_ignores_non_command() {
        assert_eq!(parse_admin_command("hello").unwrap(), None);
    }

    #[test]
    fn parse_admin_command_parses_online() {
        assert_eq!(
            parse_admin_command("!online").unwrap(),
            Some(AdminCommand::Online)
        );
    }

    #[test]
    fn parse_admin_command_parses_kick_target() {
        assert_eq!(
            parse_admin_command("!kick Bob").unwrap(),
            Some(AdminCommand::Kick {
                target: Some("Bob".to_string())
            })
        );
    }

    #[test]
    fn parse_admin_command_handles_unknown() {
        assert_eq!(
            parse_admin_command("!whoami").unwrap(),
            Some(AdminCommand::Unknown("whoami".to_string()))
        );
    }

    #[test]
    fn parse_admin_command_parses_teleport() {
        assert_eq!(
            parse_admin_command("!teleport 100 200 7").unwrap(),
            Some(AdminCommand::Teleport {
                position: Position { x: 100, y: 200, z: 7 }
            })
        );
    }

    #[test]
    fn parse_admin_command_parses_restart() {
        assert_eq!(
            parse_admin_command("!restart").unwrap(),
            Some(AdminCommand::Restart)
        );
    }

    #[test]
    fn parse_admin_command_parses_moveuse_audit() {
        assert_eq!(
            parse_admin_command("!moveuseaudit").unwrap(),
            Some(AdminCommand::MoveUseAudit)
        );
    }

    #[test]
    fn parse_admin_command_parses_where() {
        assert_eq!(
            parse_admin_command("!where").unwrap(),
            Some(AdminCommand::Where)
        );
    }
}
