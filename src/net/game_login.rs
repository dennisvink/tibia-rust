use crate::net::packet::PacketReader;

pub const OPCODE_GAME_LOGIN: u8 = 0x0a;

const MAX_ACCOUNT_LEN: usize = 64;
const MAX_NAME_LEN: usize = 64;
const MAX_PASSWORD_LEN: usize = 64;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GameLogin {
    pub client_os: u16,
    pub client_version: u16,
    pub is_gm: bool,
    pub account: String,
    pub character: String,
    pub password: String,
    pub xtea_key: Option<[u32; 4]>,
}

pub fn parse_game_login(data: &[u8]) -> Result<GameLogin, String> {
    parse_game_login_with_layout(data, Layout::Plain, AccountMode::String)
        .or_else(|_| parse_game_login_with_layout(data, Layout::Plain, AccountMode::U32))
        .or_else(|_| parse_game_login_with_layout(data, Layout::WithXtea, AccountMode::String))
        .or_else(|_| parse_game_login_with_layout(data, Layout::WithXtea, AccountMode::U32))
}

#[derive(Debug, Clone, Copy)]
enum Layout {
    Plain,
    WithXtea,
}

#[derive(Debug, Clone, Copy)]
enum AccountMode {
    String,
    U32,
}

fn parse_game_login_with_layout(
    data: &[u8],
    layout: Layout,
    account_mode: AccountMode,
) -> Result<GameLogin, String> {
    let mut reader = PacketReader::new(data);
    let opcode = reader
        .read_u8()
        .ok_or_else(|| "game login missing opcode".to_string())?;
    if opcode != OPCODE_GAME_LOGIN {
        return Err(format!("unexpected game login opcode: 0x{opcode:02x}"));
    }
    let client_os = reader
        .read_u16_le()
        .ok_or_else(|| "game login missing client os".to_string())?;
    let client_version = reader
        .read_u16_le()
        .ok_or_else(|| "game login missing client version".to_string())?;

    let xtea_key = match layout {
        Layout::Plain => None,
        Layout::WithXtea => {
            let marker = reader
                .read_u8()
                .ok_or_else(|| "game login missing xtea marker".to_string())?;
            if marker != 0 {
                return Err("game login xtea marker mismatch".to_string());
            }
            let k0 = reader
                .read_u32_le()
                .ok_or_else(|| "game login missing xtea key[0]".to_string())?;
            let k1 = reader
                .read_u32_le()
                .ok_or_else(|| "game login missing xtea key[1]".to_string())?;
            let k2 = reader
                .read_u32_le()
                .ok_or_else(|| "game login missing xtea key[2]".to_string())?;
            let k3 = reader
                .read_u32_le()
                .ok_or_else(|| "game login missing xtea key[3]".to_string())?;
            Some([k0, k1, k2, k3])
        }
    };

    let is_gm = reader
        .read_u8()
        .ok_or_else(|| "game login missing gm flag".to_string())?
        != 0;
    let account = match account_mode {
        AccountMode::String => read_text(&mut reader, MAX_ACCOUNT_LEN, "account")?,
        AccountMode::U32 => reader
            .read_u32_le()
            .ok_or_else(|| "game login missing account id".to_string())?
            .to_string(),
    };
    let character = read_text(&mut reader, MAX_NAME_LEN, "character name")?;
    let password = read_text(&mut reader, MAX_PASSWORD_LEN, "password")?;

    Ok(GameLogin {
        client_os,
        client_version,
        is_gm,
        account,
        character,
        password,
        xtea_key,
    })
}

fn read_text(reader: &mut PacketReader<'_>, max_len: usize, label: &str) -> Result<String, String> {
    let value = reader
        .read_string_lossy(max_len)
        .ok_or_else(|| format!("game login missing {label}"))?;
    if !is_valid_text(&value) {
        return Err(format!("game login {label} has invalid characters"));
    }
    Ok(value)
}

fn is_valid_text(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_graphic() || ch == ' ')
}
