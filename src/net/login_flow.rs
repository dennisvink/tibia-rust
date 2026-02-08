use crate::net::login::{build_login_message, build_login_success_v1, parse_login_packet_v1, LoginPayloadV1, LoginSuccessV1};

#[derive(Debug, Clone)]
pub struct LoginFlowConfig {
    pub min_versions: [u32; 3],
}

impl Default for LoginFlowConfig {
    fn default() -> Self {
        Self {
            min_versions: [0x0000_0302, 0x0000_0302, 0x0000_0302],
        }
    }
}

#[derive(Debug, Clone)]
pub struct WaitlistConfig {
    pub max_active_logins: usize,
}

pub fn waitlist_response(config: &WaitlistConfig, active_logins: usize) -> Option<LoginResponse> {
    if active_logins <= config.max_active_logins {
        return None;
    }

    let position = active_logins - config.max_active_logins;
    let hint = u8::try_from(position.min(u8::MAX as usize)).unwrap_or(u8::MAX).max(1);
    Some(
        LoginErrorKind::WaitlistNotYourTurn {
            wait_hint: hint,
        }
        .to_response(),
    )
}

#[derive(Debug)]
pub enum LoginDecision {
    NeedsRegistration(LoginPayloadV1),
    Error(LoginResponse),
}

#[derive(Debug, Clone)]
pub struct LoginResponse {
    pub opcode: u8,
    pub message: String,
    pub extra: Option<u8>,
}

impl LoginResponse {
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        build_login_message(self.opcode, &self.message, self.extra)
            .map_err(|err| err.message)
    }
}

#[derive(Debug, Clone)]
pub enum LoginErrorKind {
    ServerOffline,
    GameStarting,
    GameEnding,
    ClientTooOld,
    CorruptData,
    InternalError,
    CharacterNameRequired,
    AccountNotAssigned,
    AccountBanned,
    WaitlistNotYourTurn { wait_hint: u8 },
}

impl LoginErrorKind {
    pub fn to_response(&self) -> LoginResponse {
        match self {
            LoginErrorKind::ServerOffline => LoginResponse {
                opcode: 0x14,
                message: "The server is not online.\nPlease try again later.".to_string(),
                extra: None,
            },
            LoginErrorKind::GameStarting => LoginResponse {
                opcode: 0x14,
                message: "The game is just starting.\nPlease try again later.".to_string(),
                extra: None,
            },
            LoginErrorKind::GameEnding => LoginResponse {
                opcode: 0x14,
                message: "The game is just going down.\nPlease try again later.".to_string(),
                extra: None,
            },
            LoginErrorKind::ClientTooOld => LoginResponse {
                opcode: 0x14,
                message: "Your terminal version is too old.\nPlease get a new version at\nhttp://www.tibia.com.".to_string(),
                extra: None,
            },
            LoginErrorKind::CorruptData => LoginResponse {
                opcode: 0x14,
                message: "Login failed due to corrupt data.".to_string(),
                extra: None,
            },
            LoginErrorKind::InternalError => LoginResponse {
                opcode: 0x14,
                message: "Internal error, closing connection.".to_string(),
                extra: None,
            },
            LoginErrorKind::CharacterNameRequired => LoginResponse {
                opcode: 0x14,
                message: "You must enter a character name.".to_string(),
                extra: None,
            },
            LoginErrorKind::AccountNotAssigned => LoginResponse {
                opcode: 0x15,
                message: "Character is not assigned to an account.".to_string(),
                extra: None,
            },
            LoginErrorKind::AccountBanned => LoginResponse {
                opcode: 0x15,
                message: "Your account is banished.".to_string(),
                extra: None,
            },
            LoginErrorKind::WaitlistNotYourTurn { wait_hint } => LoginResponse {
                opcode: 0x16,
                message: "It's not your turn yet.".to_string(),
                extra: Some(*wait_hint),
            },
        }
    }
}

pub fn handle_login_packet_v1(
    data: &[u8],
    config: &LoginFlowConfig,
) -> Result<LoginDecision, String> {
    let payload = parse_login_packet_v1(data).map_err(|err| err.message)?;

    let client_type = payload.client_type as usize;
    if client_type >= config.min_versions.len() {
        return Ok(LoginDecision::Error(LoginErrorKind::ClientTooOld.to_response()));
    }

    let min_version = config.min_versions[client_type];
    if (payload.client_version as u32) < min_version {
        return Ok(LoginDecision::Error(LoginErrorKind::ClientTooOld.to_response()));
    }

    if payload.account.trim().is_empty() {
        return Ok(LoginDecision::Error(
            LoginErrorKind::CharacterNameRequired.to_response(),
        ));
    }

    Ok(LoginDecision::NeedsRegistration(payload))
}

pub fn build_login_success(payload: &LoginPayloadV1, player_id: u64) -> Vec<u8> {
    let success = LoginSuccessV1 {
        client_type: payload.client_type,
        client_version: payload.client_version,
        player_id,
    };
    build_login_success_v1(&success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::packet::PacketWriter;

    fn build_login_packet(
        client_type: u16,
        client_version: u16,
        account: &str,
        password: &str,
    ) -> Vec<u8> {
        let mut writer = PacketWriter::new();
        writer.write_u8(0x0a);
        writer.write_u8(0x42);
        writer.write_bytes(&[0x11; 0x10]);
        writer.write_u16_le(client_type);
        writer.write_u16_le(client_version);
        writer.write_u8(0);
        writer.write_u64_le(0);
        writer.write_string_str(account);
        writer.write_string_str(password);
        writer.into_vec()
    }

    #[test]
    fn waitlist_response_skips_when_under_limit() {
        let config = WaitlistConfig {
            max_active_logins: 5,
        };
        assert!(waitlist_response(&config, 5).is_none());
    }

    #[test]
    fn waitlist_response_caps_hint_and_requires_extra() {
        let config = WaitlistConfig {
            max_active_logins: 2,
        };
        let response = waitlist_response(&config, 260).expect("waitlist");
        assert_eq!(response.opcode, 0x16);
        assert_eq!(response.extra, Some(0xff));
    }

    #[test]
    fn login_flow_rejects_old_client() {
        let config = LoginFlowConfig::default();
        let packet = build_login_packet(0, 0x0100, "account", "pw");
        let decision = handle_login_packet_v1(&packet, &config).expect("decision");
        match decision {
            LoginDecision::Error(response) => {
                assert_eq!(response.opcode, 0x14);
                assert_eq!(
                    response.message,
                    "Your terminal version is too old.\nPlease get a new version at\nhttp://www.tibia.com."
                );
            }
            LoginDecision::NeedsRegistration(_) => panic!("expected error"),
        }
    }

    #[test]
    fn login_flow_requires_account_name() {
        let config = LoginFlowConfig::default();
        let packet = build_login_packet(0, 0x0302, "", "pw");
        let decision = handle_login_packet_v1(&packet, &config).expect("decision");
        match decision {
            LoginDecision::Error(response) => {
                assert_eq!(response.opcode, 0x14);
                assert_eq!(response.message, "You must enter a character name.");
            }
            LoginDecision::NeedsRegistration(_) => panic!("expected error"),
        }
    }

    #[test]
    fn login_flow_accepts_772_client() {
        let config = LoginFlowConfig::default();
        let packet = build_login_packet(0, 0x0304, "account", "pw");
        let decision = handle_login_packet_v1(&packet, &config).expect("decision");
        match decision {
            LoginDecision::NeedsRegistration(payload) => {
                assert_eq!(payload.client_version, 0x0304);
            }
            LoginDecision::Error(_) => panic!("expected registration"),
        }
    }
}
