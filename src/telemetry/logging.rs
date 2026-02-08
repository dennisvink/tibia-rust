use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};



#[derive(Debug, Default)]
pub struct LogConfig {
    pub level: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum LogFile {
    Banish,
    Error,
    Game,
    Houses,
    Lag,
    Load,
    Netload,
}

struct Logger {
    files: Mutex<BTreeMap<LogFile, File>>,
}

static LOGGER: OnceLock<Logger> = OnceLock::new();

const HEADER_LINE: &str = "-------------------------------------------------------------------------------";
const HEADER_TITLE: &str = "Tibia - Graphical Multi-User-Dungeon";

const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub fn init(root: &Path) -> Result<(), String> {
    if LOGGER.get().is_some() {
        return Ok(());
    }
    let log_dir = root.join("log");
    std::fs::create_dir_all(&log_dir)
        .map_err(|err| format!("log directory create failed: {}", err))?;

    let mut files = BTreeMap::new();
    for (log_file, name, header) in [
        (LogFile::Banish, "banish.log", true),
        (LogFile::Error, "error.log", false),
        (LogFile::Game, "game.log", true),
        (LogFile::Houses, "houses.log", true),
        (LogFile::Lag, "lag.log", false),
        (LogFile::Load, "load.log", false),
        (LogFile::Netload, "netload.log", true),
    ] {
        let path = log_dir.join(name);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|err| format!("open log {} failed: {}", name, err))?;
        if header && file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
            write_header(&mut file, name)?;
        }
        files.insert(log_file, file);
    }

    LOGGER
        .set(Logger {
            files: Mutex::new(files),
        })
        .map_err(|_| "log system already initialized".to_string())?;
    Ok(())
}

pub fn log_game(message: &str) {
    log_timestamped(LogFile::Game, message);
}

pub fn log_error(message: &str) {
    log_timestamped(LogFile::Error, message);
}

pub fn log_lag(message: &str) {
    log_timestamped(LogFile::Lag, message);
}

pub fn log_houses(message: &str) {
    log_timestamped(LogFile::Houses, message);
}

pub fn log_banish(message: &str) {
    log_timestamped(LogFile::Banish, message);
}

pub fn log_netload(message: &str) {
    log_timestamped(LogFile::Netload, message);
}

pub fn log_load(count: u64) {
    if let Some(logger) = LOGGER.get() {
        let epoch = unix_timestamp();
        let line = format!("{epoch} {count}\n");
        let _ = write_line(logger, LogFile::Load, &line);
    }
}

fn log_timestamped(log_file: LogFile, message: &str) {
    if let Some(logger) = LOGGER.get() {
        let timestamp = format_timestamp();
        let line = format!("{timestamp} (0): {message}\n");
        let _ = write_line(logger, log_file, &line);
    }
}

fn write_line(logger: &Logger, log_file: LogFile, line: &str) -> std::io::Result<()> {
    let mut files = logger
        .files
        .lock()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "log lock poisoned"))?;
    if let Some(file) = files.get_mut(&log_file) {
        file.write_all(line.as_bytes())?;
        file.flush()?;
    }
    Ok(())
}

fn write_header(file: &mut File, name: &str) -> Result<(), String> {
    let timestamp = format_header_timestamp();
    writeln!(file, "{HEADER_LINE}")
        .map_err(|err| format!("header write failed: {}", err))?;
    writeln!(file, "{HEADER_TITLE}")
        .map_err(|err| format!("header write failed: {}", err))?;
    writeln!(file, "{name} - gestartet {timestamp}")
        .map_err(|err| format!("header write failed: {}", err))?;
    Ok(())
}

fn format_header_timestamp() -> String {
    let ts = unix_timestamp();
    let datetime = breakdown_timestamp(ts);
    let weekday = WEEKDAYS[(datetime.weekday as usize).min(6)];
    let month = MONTHS[(datetime.month as usize).saturating_sub(1).min(11)];
    format!(
        "{weekday} {month} {:>2} {:02}:{:02}:{:02} {}",
        datetime.day, datetime.hour, datetime.minute, datetime.second, datetime.year
    )
}

fn format_timestamp() -> String {
    let ts = unix_timestamp();
    let datetime = breakdown_timestamp(ts);
    format!(
        "{:02}.{:02}.{} {:02}:{:02}:{:02}",
        datetime.day, datetime.month, datetime.year, datetime.hour, datetime.minute, datetime.second
    )
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

struct DateTimeParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    weekday: u32,
}

fn breakdown_timestamp(ts: i64) -> DateTimeParts {
    let secs = ts.max(0);
    let days = secs / 86_400;
    let seconds_of_day = (secs % 86_400) as u32;
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let (year, month, day) = civil_from_days(days);
    let weekday = ((days + 4).rem_euclid(7)) as u32;
    DateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        weekday,
    }
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = (y + if m <= 2 { 1 } else { 0 }) as i32;
    let month = (m as i32) as u32;
    let day = d as u32;
    (year, month, day)
}
