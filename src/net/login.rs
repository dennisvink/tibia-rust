use crate::net::packet::{PacketReader, PacketWriter};
use crate::net::xtea::{XteaKey, XTEA_KEY_BYTES};

#[derive(Debug)]
pub struct LoginRequest {
    pub opcode: u8,
    pub account: String,
    pub password: String,
    pub trailing: Vec<u8>,
}

#[derive(Debug)]
pub struct LoginPayloadV1 {
    pub marker: u8,
    pub client_type: u16,
    pub client_version: u16,
    pub flag: u8,
    pub account_id: u64,
    pub account: String,
    pub password: String,
    pub trailing: Vec<u8>,
    pub xtea_key: Option<XteaKey>,
}

#[derive(Debug)]
pub struct LoginSuccessV1 {
    pub client_type: u16,
    pub client_version: u16,
    pub player_id: u64,
}

#[derive(Debug, Clone)]
pub struct LoginCharacter {
    pub player_id: u32,
    pub name: String,
    pub world: String,
    pub ip: u32,
    pub port: u16,
}

pub const LOGIN_OPCODE_CHARACTER_LIST: u8 = 0x64;

#[derive(Debug)]
pub struct LoginParseError {
    pub message: String,
}

#[derive(Debug)]
pub struct LoginBuildError {
    pub message: String,
}

const RECEIVE_DATA_NO_DATA: &str = "ReceiveData: Keine Daten vorhanden.";

fn receive_data_bad_login(opcode: u8) -> String {
    format!("ReceiveData: Falsches Login-Kommando {}.", opcode)
}

pub fn parse_login(data: &[u8]) -> Result<LoginRequest, LoginParseError> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| LoginParseError {
            message: RECEIVE_DATA_NO_DATA.to_string(),
        })?;

    if opcode != 0x0a {
        return Err(LoginParseError {
            message: receive_data_bad_login(opcode),
        });
    }

    // Encryptionless mode: account/password follow the opcode directly.
    let account = reader
        .read_string_lossy(0x1e)
        .ok_or_else(|| LoginParseError {
            message: "login packet missing account string".to_string(),
        })?;

    let password = reader
        .read_string_lossy(0x1e)
        .ok_or_else(|| LoginParseError {
            message: "login packet missing password string".to_string(),
        })?;

    let trailing = reader
        .read_bytes(reader.remaining())
        .unwrap_or(&[])
        .to_vec();

    Ok(LoginRequest {
        opcode,
        account,
        password,
        trailing,
    })
}

pub fn parse_login_packet_v1(data: &[u8]) -> Result<LoginPayloadV1, LoginParseError> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| LoginParseError {
            message: RECEIVE_DATA_NO_DATA.to_string(),
        })?;

    if opcode != 0x0a {
        return Err(LoginParseError {
            message: receive_data_bad_login(opcode),
        });
    }

    let payload = reader
        .read_bytes(reader.remaining())
        .unwrap_or(&[]);
    parse_login_payload_v1(payload)
}

pub fn parse_login_payload_v1(data: &[u8]) -> Result<LoginPayloadV1, LoginParseError> {
    let mut reader = PacketReader::new(data);
    let marker = reader.read_u8().ok_or_else(|| LoginParseError {
        message: "login payload missing marker byte".to_string(),
    })?;

    let key_bytes = reader
        .read_bytes(XTEA_KEY_BYTES)
        .ok_or_else(|| LoginParseError {
            message: "login payload missing XTEA seed block".to_string(),
        })?;
    let mut key_arr = [0u8; XTEA_KEY_BYTES];
    key_arr.copy_from_slice(key_bytes);
    let xtea_key = {
        let key = XteaKey::from_bytes(key_arr);
        if key.is_zero() { None } else { Some(key) }
    };

    let payload = reader
        .read_bytes(reader.remaining())
        .unwrap_or(&[])
        .to_vec();

    let plain_result = parse_login_payload_fields(&payload);
    if let Ok(fields) = plain_result {
        return Ok(LoginPayloadV1 {
            marker,
            xtea_key,
            ..fields
        });
    }

    if let Some(key) = xtea_key {
        if payload.len() % crate::net::xtea::XTEA_BLOCK_BYTES == 0 {
            if let Ok(decrypted) = key.decrypt_to_vec(&payload) {
                if let Ok(fields) = parse_login_payload_fields(&decrypted) {
                    return Ok(LoginPayloadV1 {
                        marker,
                        xtea_key: Some(key),
                        ..fields
                    });
                }
            }
        }
    }

    Err(plain_result.unwrap_err())
}

fn parse_login_payload_fields(data: &[u8]) -> Result<LoginPayloadV1, LoginParseError> {
    let mut reader = PacketReader::new(data);
    let client_type = reader.read_u16_le().ok_or_else(|| LoginParseError {
        message: "login payload missing client type".to_string(),
    })?;
    let client_version = reader.read_u16_le().ok_or_else(|| LoginParseError {
        message: "login payload missing client version".to_string(),
    })?;
    let flag = reader.read_u8().ok_or_else(|| LoginParseError {
        message: "login payload missing flag".to_string(),
    })?;
    let account_id = reader.read_u64_le().ok_or_else(|| LoginParseError {
        message: "login payload missing account id".to_string(),
    })?;
    let account = reader
        .read_string_lossy(0x1e)
        .ok_or_else(|| LoginParseError {
            message: "login payload missing account string".to_string(),
        })?;
    let password = reader
        .read_string_lossy(0x1e)
        .ok_or_else(|| LoginParseError {
            message: "login payload missing password string".to_string(),
        })?;

    let trailing = reader
        .read_bytes(reader.remaining())
        .unwrap_or(&[])
        .to_vec();

    Ok(LoginPayloadV1 {
        marker: 0,
        client_type,
        client_version,
        flag,
        account_id,
        account,
        password,
        trailing,
        xtea_key: None,
    })
}

pub fn build_login_message(
    opcode: u8,
    message: &str,
    extra: Option<u8>,
) -> Result<Vec<u8>, LoginBuildError> {
    if message.len() > 0x122 {
        return Err(LoginBuildError {
            message: format!("login message too long: {}", message.len()),
        });
    }

    if opcode == 0x16 {
        let extra = extra.ok_or_else(|| LoginBuildError {
            message: "opcode 0x16 requires an extra byte".to_string(),
        })?;

        if extra == 0 {
            return Err(LoginBuildError {
                message: "extra byte must be in range 1..=255".to_string(),
            });
        }
    } else if extra.is_some() {
        return Err(LoginBuildError {
            message: "extra byte only allowed for opcode 0x16".to_string(),
        });
    }

    let mut writer = PacketWriter::with_capacity(0x12c);
    writer.write_u8(opcode);
    writer.write_string_str(message);
    if opcode == 0x16 {
        writer.write_u8(extra.unwrap());
    }

    Ok(writer.into_vec())
}

pub fn build_login_success_v1(payload: &LoginSuccessV1) -> Vec<u8> {
    let mut writer = PacketWriter::with_capacity(0x20);
    writer.write_u8(0x0b);
    writer.write_u16_le(payload.client_type);
    writer.write_u16_le(payload.client_version);
    writer.write_u64_le(payload.player_id);
    writer.into_vec()
}

pub fn build_login_character_list(characters: &[LoginCharacter], premium_days: u16) -> Vec<u8> {
    let mut writer = PacketWriter::new();
    let count = u8::try_from(characters.len().min(u8::MAX as usize)).unwrap_or(u8::MAX);
    writer.write_u8(LOGIN_OPCODE_CHARACTER_LIST);
    writer.write_u8(count);
    for entry in characters.iter().take(count as usize) {
        writer.write_string_str(&entry.name);
        writer.write_string_str(&entry.world);
        writer.write_u32_le(entry.ip);
        writer.write_u16_le(entry.port);
    }
    writer.write_u16_le(premium_days);
    writer.into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::packet::PacketWriter;
    use crate::net::xtea::XteaKey;

    #[test]
    fn parse_login_reads_basic_fields() {
        let mut writer = PacketWriter::new();
        writer.write_u8(0x0a);
        writer.write_string_str("account");
        writer.write_string_str("secret");
        let data = writer.into_vec();

        let request = parse_login(&data).expect("parse login");
        assert_eq!(request.opcode, 0x0a);
        assert_eq!(request.account, "account");
        assert_eq!(request.password, "secret");
        assert!(request.trailing.is_empty());
    }

    #[test]
    fn parse_login_payload_v1_reads_layout() {
        let mut writer = PacketWriter::new();
        writer.write_u8(0x42);
        writer.write_bytes(&[0x11; 0x10]);
        writer.write_u16_le(0x1234);
        writer.write_u16_le(0x5678);
        writer.write_u8(0x9a);
        writer.write_u64_le(0x0102030405060708);
        writer.write_string_str("acc");
        writer.write_string_str("pw");
        writer.write_bytes(&[0xaa, 0xbb]);
        let data = writer.into_vec();

        let payload = parse_login_payload_v1(&data).expect("parse payload");
        assert_eq!(payload.marker, 0x42);
        assert_eq!(payload.client_type, 0x1234);
        assert_eq!(payload.client_version, 0x5678);
        assert_eq!(payload.flag, 0x9a);
        assert_eq!(payload.account_id, 0x0102030405060708);
        assert_eq!(payload.account, "acc");
        assert_eq!(payload.password, "pw");
        assert_eq!(payload.trailing, vec![0xaa, 0xbb]);
    }

    #[test]
    fn build_login_message_writes_opcode_and_string() {
        let bytes = build_login_message(0x14, "hi", None).expect("build");
        assert_eq!(bytes, vec![0x14, 0x02, 0x00, b'h', b'i']);
    }

    #[test]
    fn build_login_message_requires_extra_for_0x16() {
        let err = build_login_message(0x16, "wait", None).unwrap_err();
        assert_eq!(err.message, "opcode 0x16 requires an extra byte");
    }

    #[test]
    fn build_login_message_writes_extra_for_0x16() {
        let bytes = build_login_message(0x16, "wait", Some(3)).expect("build");
        assert_eq!(bytes, vec![0x16, 0x04, 0x00, b'w', b'a', b'i', b't', 0x03]);
    }

    #[test]
    fn build_login_success_v1_writes_fixed_layout() {
        let payload = LoginSuccessV1 {
            client_type: 0x0102,
            client_version: 0x0304,
            player_id: 0x05060708090a0b0c,
        };
        let bytes = build_login_success_v1(&payload);
        assert_eq!(
            bytes,
            vec![
                0x0b, 0x02, 0x01, 0x04, 0x03, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06,
                0x05
            ]
        );
    }

    #[test]
    fn parse_login_payload_v1_decrypts_xtea_payload() {
        let key_bytes = [0x44; 16];
        let key = XteaKey::from_bytes(key_bytes);

        let mut inner = PacketWriter::new();
        inner.write_u16_le(0x1234);
        inner.write_u16_le(0x5678);
        inner.write_u8(0x9a);
        inner.write_u64_le(0x0102030405060708);
        inner.write_string_str("acc");
        inner.write_string_str("pw");
        inner.write_bytes(&[0xde, 0xad]);

        let encrypted = key.encrypt_padded(inner.as_slice());
        let mut writer = PacketWriter::new();
        writer.write_u8(0x42);
        writer.write_bytes(&key_bytes);
        writer.write_bytes(&encrypted);

        let payload = parse_login_payload_v1(writer.as_slice()).expect("parse payload");
        assert_eq!(payload.marker, 0x42);
        assert_eq!(payload.client_type, 0x1234);
        assert_eq!(payload.client_version, 0x5678);
        assert_eq!(payload.flag, 0x9a);
        assert_eq!(payload.account_id, 0x0102030405060708);
        assert_eq!(payload.account, "acc");
        assert_eq!(payload.password, "pw");
        assert_eq!(payload.xtea_key, Some(key));
    }
}
