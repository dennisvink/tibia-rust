use crate::entities::player::PlayerId;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct AccountRecord {
    pub name: String,
    pub password: String,
    pub player_ids: Vec<PlayerId>,
    pub premium: bool,
    pub gamemaster: bool,
    pub test_god: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AccountRegistry {
    accounts: HashMap<String, AccountRecord>,
}

impl AccountRegistry {
    pub fn load(root: &Path) -> Result<Option<Self>, String> {
        let path = root.join("save").join("accounts.txt");
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(format!(
                    "account registry read failed for {}: {}",
                    path.display(),
                    err
                ))
            }
        };
        let mut accounts = parse_accounts(&data)?;
        ensure_builtin_test_god(&mut accounts);
        Ok(Some(AccountRegistry { accounts }))
    }

    pub fn verify(&self, account: &str, password: &str) -> Option<&AccountRecord> {
        let key = normalize_account_name(account);
        let record = self.accounts.get(&key)?;
        if record.password == password {
            Some(record)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct BanRecord {
    pub account: String,
    pub expires_at: Option<SystemTime>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BanList {
    accounts: HashMap<String, BanRecord>,
}

impl BanList {
    pub fn load(root: &Path) -> Result<Option<Self>, String> {
        let path = root.join("save").join("banlist.txt");
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(format!(
                    "banlist read failed for {}: {}",
                    path.display(),
                    err
                ))
            }
        };
        let accounts = parse_bans(&data)?;
        Ok(Some(BanList { accounts }))
    }

    pub fn is_banned(&self, account: &str, now: SystemTime) -> bool {
        let key = normalize_account_name(account);
        let Some(record) = self.accounts.get(&key) else {
            return false;
        };
        match record.expires_at {
            None => true,
            Some(expires_at) => now < expires_at,
        }
    }
}

#[derive(Debug, Default)]
struct AccountEntry {
    name: Option<String>,
    password: Option<String>,
    player_id: Option<PlayerId>,
    premium: Option<bool>,
    gamemaster: Option<bool>,
    test_god: Option<bool>,
}

impl AccountEntry {
    fn has_data(&self) -> bool {
        self.name.is_some()
            || self.password.is_some()
            || self.player_id.is_some()
            || self.premium.is_some()
            || self.gamemaster.is_some()
            || self.test_god.is_some()
    }

    fn into_record(self, line_no: usize) -> Result<AccountRecord, String> {
        let name = self
            .name
            .ok_or_else(|| format!("accounts.txt missing account name at line {}", line_no))?;
        let password = self.password.ok_or_else(|| {
            format!("accounts.txt missing password for account {} at line {}", name, line_no)
        })?;
        let player_id = self.player_id.ok_or_else(|| {
            format!(
                "accounts.txt missing player_id for account {} at line {}",
                name, line_no
            )
        })?;
        Ok(AccountRecord {
            name,
            password,
            player_ids: vec![player_id],
            premium: self.premium.unwrap_or(false),
            gamemaster: self.gamemaster.unwrap_or(false),
            test_god: self.test_god.unwrap_or(false),
        })
    }
}

#[derive(Debug, Default)]
struct BanEntry {
    account: Option<String>,
    expires_at: Option<SystemTime>,
    reason: Option<String>,
}

impl BanEntry {
    fn has_data(&self) -> bool {
        self.account.is_some() || self.expires_at.is_some() || self.reason.is_some()
    }

    fn into_record(self, line_no: usize) -> Result<BanRecord, String> {
        let account = self
            .account
            .ok_or_else(|| format!("banlist.txt missing account name at line {}", line_no))?;
        Ok(BanRecord {
            account,
            expires_at: self.expires_at,
            reason: self.reason,
        })
    }
}

fn parse_accounts(data: &str) -> Result<HashMap<String, AccountRecord>, String> {
    let mut accounts = HashMap::new();
    let mut entry = AccountEntry::default();
    let mut last_line = 1usize;

    for (idx, raw_line) in data.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();
        if line.is_empty() {
            if entry.has_data() {
                let record = entry.into_record(last_line)?;
                insert_account_record(&mut accounts, record, last_line)?;
                entry = AccountEntry::default();
            }
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let (key, value) = split_kv(line, "accounts.txt", line_no)?;
        if key.eq_ignore_ascii_case("account") {
            if entry.has_data() {
                let record = entry.into_record(last_line)?;
                insert_account_record(&mut accounts, record, last_line)?;
                entry = AccountEntry::default();
            }
            entry.name = Some(parse_string(value, "account", line_no)?);
            last_line = line_no;
            continue;
        }

        match key {
            "password" => {
                entry.password = Some(parse_string(value, "password", line_no)?);
            }
            "player_id" => {
                entry.player_id = Some(PlayerId(parse_u32(value, "player_id", line_no)?));
            }
            "premium" => {
                entry.premium = Some(parse_bool(value, "premium", line_no)?);
            }
            "gm" | "gamemaster" => {
                entry.gamemaster = Some(parse_bool(value, "gamemaster", line_no)?);
            }
            "test_god" | "testgod" => {
                entry.test_god = Some(parse_bool(value, "test_god", line_no)?);
            }
            other => {
                return Err(format!(
                    "accounts.txt unknown field '{}' at line {}",
                    other, line_no
                ));
            }
        }
        last_line = line_no;
    }

    if entry.has_data() {
        let record = entry.into_record(last_line)?;
        insert_account_record(&mut accounts, record, last_line)?;
    }

    Ok(accounts)
}

fn insert_account_record(
    accounts: &mut HashMap<String, AccountRecord>,
    record: AccountRecord,
    line_no: usize,
) -> Result<(), String> {
    let key = normalize_account_name(&record.name);
    if let Some(existing) = accounts.get_mut(&key) {
        if existing.password != record.password {
            return Err(format!(
                "accounts.txt conflicting password for account '{}' at line {}",
                record.name, line_no
            ));
        }
        if let Some(player_id) = record.player_ids.first() {
            if !existing.player_ids.contains(player_id) {
                existing.player_ids.push(*player_id);
            }
        }
        existing.premium = existing.premium || record.premium;
        existing.gamemaster = existing.gamemaster || record.gamemaster;
        existing.test_god = existing.test_god || record.test_god;
        return Ok(());
    }
    accounts.insert(key, record);
    Ok(())
}

const TEST_GOD_ACCOUNT: &str = "test_god";
const TEST_GOD_PASSWORD: &str = "test_god";
const TEST_GOD_PLAYER_ID: u32 = 999_900;

fn ensure_builtin_test_god(accounts: &mut HashMap<String, AccountRecord>) {
    let key = normalize_account_name(TEST_GOD_ACCOUNT);
    let record = AccountRecord {
        name: TEST_GOD_ACCOUNT.to_string(),
        password: TEST_GOD_PASSWORD.to_string(),
        player_ids: vec![PlayerId(TEST_GOD_PLAYER_ID)],
        premium: true,
        gamemaster: true,
        test_god: true,
    };
    if let Some(existing) = accounts.get_mut(&key) {
        if existing.password == TEST_GOD_PASSWORD {
            if !existing.player_ids.contains(&PlayerId(TEST_GOD_PLAYER_ID)) {
                existing.player_ids.push(PlayerId(TEST_GOD_PLAYER_ID));
            }
            existing.premium = true;
            existing.gamemaster = true;
            existing.test_god = true;
        }
        return;
    }
    accounts.insert(key, record);
}

fn parse_bans(data: &str) -> Result<HashMap<String, BanRecord>, String> {
    let mut bans = HashMap::new();
    let mut entry = BanEntry::default();
    let mut last_line = 1usize;

    for (idx, raw_line) in data.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();
        if line.is_empty() {
            if entry.has_data() {
                let record = entry.into_record(last_line)?;
                let key = normalize_account_name(&record.account);
                if bans.contains_key(&key) {
                    return Err(format!(
                        "banlist.txt duplicate account '{}' at line {}",
                        record.account, last_line
                    ));
                }
                bans.insert(key, record);
                entry = BanEntry::default();
            }
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let (key, value) = split_kv(line, "banlist.txt", line_no)?;
        if key.eq_ignore_ascii_case("account") {
            if entry.has_data() {
                let record = entry.into_record(last_line)?;
                let key = normalize_account_name(&record.account);
                if bans.contains_key(&key) {
                    return Err(format!(
                        "banlist.txt duplicate account '{}' at line {}",
                        record.account, last_line
                    ));
                }
                bans.insert(key, record);
                entry = BanEntry::default();
            }
            entry.account = Some(parse_string(value, "account", line_no)?);
            last_line = line_no;
            continue;
        }

        match key {
            "expires_at" => {
                entry.expires_at = parse_epoch(value, line_no)?;
            }
            "reason" => {
                entry.reason = Some(parse_string(value, "reason", line_no)?);
            }
            other => {
                return Err(format!(
                    "banlist.txt unknown field '{}' at line {}",
                    other, line_no
                ));
            }
        }
        last_line = line_no;
    }

    if entry.has_data() {
        let record = entry.into_record(last_line)?;
        let key = normalize_account_name(&record.account);
        if bans.contains_key(&key) {
            return Err(format!(
                "banlist.txt duplicate account '{}' at line {}",
                record.account, last_line
            ));
        }
        bans.insert(key, record);
    }

    Ok(bans)
}

fn normalize_account_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn split_kv<'a>(line: &'a str, label: &str, line_no: usize) -> Result<(&'a str, &'a str), String> {
    let (key, value) = line.split_once('=').ok_or_else(|| {
        format!(
            "{} expected key=value at line {}, got '{}'",
            label, line_no, line
        )
    })?;
    Ok((key.trim(), value.trim()))
}

fn parse_bool(value: &str, label: &str, line_no: usize) -> Result<bool, String> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        other => Err(format!(
            "{} expects 0 or 1 at line {}, got '{}'",
            label, line_no, other
        )),
    }
}

fn parse_u32(value: &str, label: &str, line_no: usize) -> Result<u32, String> {
    value.parse::<u32>().map_err(|_| {
        format!(
            "{} expects unsigned int at line {}, got '{}'",
            label, line_no, value
        )
    })
}

fn parse_epoch(value: &str, line_no: usize) -> Result<Option<SystemTime>, String> {
    if value.is_empty() || value == "0" {
        return Ok(None);
    }
    let seconds = value.parse::<u64>().map_err(|_| {
        format!(
            "banlist expires_at expects unix epoch at line {}, got '{}'",
            line_no, value
        )
    })?;
    Ok(Some(UNIX_EPOCH + Duration::from_secs(seconds)))
}

fn parse_string(value: &str, label: &str, line_no: usize) -> Result<String, String> {
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        unescape_string(&value[1..value.len() - 1]).map_err(|err| {
            format!("{} string parse failed at line {}: {}", label, line_no, err)
        })
    } else {
        Ok(value.to_string())
    }
}

fn unescape_string(input: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            return Err("invalid escape: trailing backslash".to_string());
        };
        match next {
            '\\' => out.push('\\'),
            '"' => out.push('"'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            other => {
                return Err(format!("invalid escape '\\{}'", other));
            }
        }
    }
    Ok(out)
}
