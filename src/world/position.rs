#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Position {
    pub x: u16,
    pub y: u16,
    pub z: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    North,
    East,
    South,
    West,
    Northeast,
    Northwest,
    Southeast,
    Southwest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionDelta {
    pub dx: i16,
    pub dy: i16,
    pub dz: i8,
}

impl Position {
    pub fn offset(self, delta: PositionDelta) -> Option<Self> {
        let x = i32::from(self.x) + i32::from(delta.dx);
        let y = i32::from(self.y) + i32::from(delta.dy);
        let z = i16::from(self.z) + i16::from(delta.dz);

        if x < 0 || y < 0 || z < 0 {
            return None;
        }

        if x > i32::from(u16::MAX) || y > i32::from(u16::MAX) || z > i16::from(u8::MAX) {
            return None;
        }

        Some(Self {
            x: x as u16,
            y: y as u16,
            z: z as u8,
        })
    }

    pub fn step(self, direction: Direction) -> Option<Self> {
        self.offset(direction.delta())
    }
}

impl Direction {
    pub fn delta(self) -> PositionDelta {
        match self {
            Direction::North => PositionDelta { dx: 0, dy: -1, dz: 0 },
            Direction::East => PositionDelta { dx: 1, dy: 0, dz: 0 },
            Direction::South => PositionDelta { dx: 0, dy: 1, dz: 0 },
            Direction::West => PositionDelta { dx: -1, dy: 0, dz: 0 },
            Direction::Northeast => PositionDelta { dx: 1, dy: -1, dz: 0 },
            Direction::Northwest => PositionDelta { dx: -1, dy: -1, dz: 0 },
            Direction::Southeast => PositionDelta { dx: 1, dy: 1, dz: 0 },
            Direction::Southwest => PositionDelta { dx: -1, dy: 1, dz: 0 },
        }
    }

    pub fn is_diagonal(self) -> bool {
        matches!(
            self,
            Direction::Northeast
                | Direction::Northwest
                | Direction::Southeast
                | Direction::Southwest
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opposite(direction: Direction) -> Direction {
        match direction {
            Direction::North => Direction::South,
            Direction::East => Direction::West,
            Direction::South => Direction::North,
            Direction::West => Direction::East,
            Direction::Northeast => Direction::Southwest,
            Direction::Northwest => Direction::Southeast,
            Direction::Southeast => Direction::Northwest,
            Direction::Southwest => Direction::Northeast,
        }
    }

    fn negate(delta: PositionDelta) -> PositionDelta {
        PositionDelta {
            dx: -delta.dx,
            dy: -delta.dy,
            dz: -delta.dz,
        }
    }

    fn lcg_next(state: &mut u64) -> u32 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        (*state >> 32) as u32
    }

    #[test]
    fn step_roundtrip_with_opposites() {
        let origin = Position { x: 100, y: 100, z: 7 };
        let directions = [
            Direction::North,
            Direction::East,
            Direction::South,
            Direction::West,
            Direction::Northeast,
            Direction::Northwest,
            Direction::Southeast,
            Direction::Southwest,
        ];
        for direction in directions {
            let next = origin.step(direction).expect("step");
            let back = next.step(opposite(direction)).expect("step back");
            assert_eq!(back, origin);
        }
    }

    #[test]
    fn offset_roundtrip_for_small_deltas() {
        let mut state = 0xfeed_face_cafe_beef;
        for _ in 0..256 {
            let x = 200 + (lcg_next(&mut state) % 100) as u16;
            let y = 200 + (lcg_next(&mut state) % 100) as u16;
            let z = 7;
            let dx = (lcg_next(&mut state) % 7) as i16 - 3;
            let dy = (lcg_next(&mut state) % 7) as i16 - 3;
            let dz = 0i8;
            let origin = Position { x, y, z };
            let delta = PositionDelta { dx, dy, dz };
            let Some(next) = origin.offset(delta) else {
                continue;
            };
            let Some(back) = next.offset(negate(delta)) else {
                continue;
            };
            assert_eq!(back, origin);
        }
    }
}
