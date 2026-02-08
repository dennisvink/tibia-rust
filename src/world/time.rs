use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GameTick(pub u64);

#[derive(Debug, Clone)]
pub struct GameClock {
    tick_length: Duration,
    tick: GameTick,
}

/// Game time (24x faster than real time)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GameTime {
    pub hour: u8,     // 0-23
    pub minute: u8,   // 0-59
    pub second: u8,   // 0-59
}

/// Game date (1 real week = 1 game year)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GameDate {
    pub year: u16,    // Each real week = 1 game year
    pub month: u8,    // 0-11
    pub day: u8,      // 1-30
}

/// Ambient light for day/night cycle
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ambiente {
    pub brightness: u8,
    pub color: u8,
}

/// Maps real-world time to game time (24x faster)
#[derive(Debug, Clone)]
pub struct GameTimeClock {
    server_start_time: Instant,
    current_round: u32,  // Seconds since startup
    current_milliseconds: u32,
}

impl GameClock {
    pub fn new(tick_length: Duration) -> Self {
        let tick_length = if tick_length.is_zero() {
            Duration::from_millis(1)
        } else {
            tick_length
        };
        Self {
            tick_length,
            tick: GameTick(0),
        }
    }

    pub fn tick_length(&self) -> Duration {
        self.tick_length
    }

    pub fn now(&self) -> GameTick {
        self.tick
    }

    pub fn advance(&mut self, ticks: u64) -> GameTick {
        self.tick.0 = self.tick.0.saturating_add(ticks);
        self.tick
    }

    pub fn advance_duration(&mut self, duration: Duration) -> GameTick {
        let ticks = self.ticks_from_duration_round_up(duration);
        self.advance(ticks)
    }

    pub fn ticks_from_duration_round_up(&self, duration: Duration) -> u64 {
        if duration.is_zero() {
            return 0;
        }
        let tick_nanos = self.tick_length.as_nanos().max(1);
        let duration_nanos = duration.as_nanos();
        let ticks = (duration_nanos + tick_nanos - 1) / tick_nanos;
        ticks.min(u64::MAX as u128) as u64
    }

    pub fn duration_for_ticks(&self, ticks: u64) -> Duration {
        let nanos = self
            .tick_length
            .as_nanos()
            .saturating_mul(ticks as u128)
            .min(u64::MAX as u128) as u64;
        Duration::from_nanos(nanos)
    }
}

impl GameTimeClock {
    pub fn new() -> Self {
        GameTimeClock {
            server_start_time: Instant::now(),
            current_round: 0,
            current_milliseconds: 0,
        }
    }
    
    pub fn update(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.server_start_time);
        self.current_round = elapsed.as_secs() as u32;
        self.current_milliseconds = elapsed.subsec_millis();
    }
    
    /// Get game time (24x faster than real time)
    /// 150 seconds = 1 game hour
    pub fn get_game_time(&self) -> GameTime {
        let time = (self.current_round % 86400) as u32;
        
        // Convert to game time: 150 seconds = 1 game hour
        let game_time_hours = time / 150;
        let game_time_minutes = (time % 150) * 2 / 5;
        
        GameTime {
            hour: (game_time_hours % 24) as u8,
            minute: game_time_minutes as u8,
            second: 0,
        }
    }
    
    /// Get game date (1 real week = 1 game year)
    pub fn get_game_date(&self) -> GameDate {
        let elapsed = self.server_start_time.elapsed();
        let real_time = elapsed.as_secs();
        
        // Calculate game year (each real week)
        let year = (((real_time / 86400) + 4) / 7) as u16;
        
        // Calculate day of week (0-6)
        let day_of_week = (real_time / 86400) % 7;
        
        // Convert to month/day (simplified 30-day months)
        let day = (day_of_week % 30 + 1) as u8;
        let month = (day_of_week / 30) as u8;
        
        GameDate { year, month, day }
    }
    
    /// Calculate ambient light based on game time
    pub fn get_ambiente(&self) -> Ambiente {
        let game_time = self.get_game_time();
        let time = game_time.minute as u32 + game_time.hour as u32 * 60;
        
        let (brightness, color) = match time {
            t if t < 60 => (0x33, 0xD7),                    // 00:00 - 01:00
            t if t < 120 => (0x66, 0xD7),                   // 01:00 - 02:00
            t if t < 180 => (0x99, 0xAD),                   // 02:00 - 03:00
            t if t < 240 => (0xCC, 0xAD),                   // 03:00 - 04:00
            t if t <= 1200 => (0xFF, 0xD7),                 // 04:00 - 20:00 (Day)
            t if t <= 1260 => (0xCC, 0xD0),                 // 20:00 - 21:00
            t if t <= 1320 => (0x99, 0xD0),                 // 21:00 - 22:00
            t if t <= 1380 => (0x66, 0xD7),                 // 22:00 - 23:00
            _ => (0x33, 0xD7),                              // 23:00 - 24:00
        };
        
        Ambiente { brightness, color }
    }
    
    /// Calculate round when specific game time occurs
    pub fn get_round_at_game_time(&self, hour: u8, minute: u8) -> u32 {
        let now = self.server_start_time.elapsed().as_secs();
        let target_time = hour as u64 * 150 + minute as u64 * 150 / 60;
        let current_time = now % 86400;
        
        let seconds_diff = if target_time >= current_time {
            target_time - current_time
        } else {
            86400 + target_time - current_time
        };
        
        self.current_round + seconds_diff as u32
    }
    
    /// Calculate round for next game minute
    pub fn get_round_for_next_minute(&self) -> u32 {
        let now = self.server_start_time.elapsed().as_secs();
        let seconds_to_next_minute = 60 - (now % 60);
        self.current_round + seconds_to_next_minute as u32 + 30
    }
}

impl std::fmt::Display for GameTime {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:02}:{:02}", self.hour, self.minute)
    }
}

impl std::fmt::Display for GameDate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn game_time_24x_multiplier() {
        // After 150 real seconds, we should have 1 game hour
        let mut clock = GameTimeClock::new();
        clock.server_start_time = Instant::now() - Duration::from_secs(150);
        clock.update(Instant::now());
        
        let game_time = clock.get_game_time();
        assert_eq!(game_time.hour, 1);
        assert_eq!(game_time.minute, 0);
    }

    #[test]
    fn game_day_night_cycle() {
        // Test ambient light at different times
        let mut clock = GameTimeClock::new();
        
        // Test at 00:00 (midnight) - should be darkest
        clock.current_round = 0;
        let ambiente = clock.get_ambiente();
        assert_eq!(ambiente.brightness, 0x33);
        
        // Test at 12:00 (noon) - should be brightest
        clock.current_round = (12 * 60 * 150) as u32; // 12 hours * 60 min * 150 sec/hour
        let ambiente = clock.get_ambiente();
        assert_eq!(ambiente.brightness, 0xFF);
    }

    #[test]
    fn game_time_display_formatting() {
        let game_time = GameTime {
            hour: 12,
            minute: 30,
            second: 45,
        };
        assert_eq!(format!("{}", game_time), "12:30");
    }

    #[test]
    fn game_date_display_formatting() {
        let game_date = GameDate {
            year: 2024,
            month: 6,
            day: 15,
        };
        assert_eq!(format!("{}", game_date), "2024-06-15");
    }

    #[test]
    fn ambiente_values_match_reference() {
        let mut clock = GameTimeClock::new();
        
        // Test all day/night periods match reference values
        let test_cases = vec![
            (0, 0, 0x33, 0xD7),    // 00:00
            (1, 30, 0x66, 0xD7),   // 01:30
            (3, 0, 0xCC, 0xAD),    // 03:00
            (12, 0, 0xFF, 0xD7),   // 12:00 (noon)
            (20, 0, 0xCC, 0xD0),   // 20:00 (dusk)
            (22, 0, 0x99, 0xD0),   // 22:00
            (23, 30, 0x33, 0xD7),  // 23:30
        ];
        
        for (hour, minute, expected_brightness, expected_color) in test_cases {
            clock.current_round = (hour as u32 * 60 * 150 + minute as u32 * 150 / 60) as u32;
            let ambiente = clock.get_ambiente();
            assert_eq!(
                ambiente.brightness, expected_brightness,
                "Brightness mismatch at {:02}:{:02}",
                hour, minute
            );
            assert_eq!(
                ambiente.color, expected_color,
                "Color mismatch at {:02}:{:02}",
                hour, minute
            );
        }
    }

    #[test]
    fn calculate_round_at_game_time() {
        let mut clock = GameTimeClock::new();
        clock.current_round = 1000;
        
        // Calculate round for a future time
        let round = clock.get_round_at_game_time(12, 0);
        assert!(round > clock.current_round);
    }

    #[test]
    fn game_date_year_calculation() {
        let mut clock = GameTimeClock::new();
        
        // After 7 real days (1 week), should be 1 game year
        let week_later = Instant::now() - Duration::from_secs(7 * 86400);
        clock.server_start_time = week_later;
        clock.update(Instant::now());
        
        let game_date = clock.get_game_date();
        assert!(game_date.year >= 1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cooldown {
    ready_at: GameTick,
}

impl Cooldown {
    pub fn new(ready_at: GameTick) -> Self {
        Self { ready_at }
    }

    pub fn from_ticks_from_now(clock: &GameClock, ticks: u64) -> Self {
        let ready_at = GameTick(clock.now().0.saturating_add(ticks));
        Self { ready_at }
    }

    pub fn from_duration_from_now(clock: &GameClock, duration: Duration) -> Self {
        let ticks = clock.ticks_from_duration_round_up(duration);
        Self::from_ticks_from_now(clock, ticks)
    }

    pub fn ready_at(&self) -> GameTick {
        self.ready_at
    }

    pub fn is_ready(&self, clock: &GameClock) -> bool {
        clock.now() >= self.ready_at
    }

    pub fn remaining_ticks(&self, clock: &GameClock) -> u64 {
        self.ready_at
            .0
            .saturating_sub(clock.now().0)
    }

    pub fn remaining_duration(&self, clock: &GameClock) -> Duration {
        clock.duration_for_ticks(self.remaining_ticks(clock))
    }

    pub fn reset_from_now_ticks(&mut self, clock: &GameClock, ticks: u64) {
        self.ready_at = GameTick(clock.now().0.saturating_add(ticks));
    }

    pub fn reset_from_now_duration(&mut self, clock: &GameClock, duration: Duration) {
        let ticks = clock.ticks_from_duration_round_up(duration);
        self.reset_from_now_ticks(clock, ticks);
    }
}
