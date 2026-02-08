use crate::world::position::Position;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewportSize {
    pub width: u16,
    pub height: u16,
}

impl Default for ViewportSize {
    fn default() -> Self {
        // Classic clients show an 18x14 tile viewport; adjust once protocol is verified.
        Self { width: 18, height: 14 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Viewport {
    pub center: Position,
    pub min: Position,
    pub max: Position,
    pub size: ViewportSize,
}

impl Viewport {
    pub fn from_center(center: Position, size: ViewportSize) -> Self {
        let half_left = size.width / 2;
        let half_right = size.width.saturating_sub(half_left + 1);
        let half_up = size.height / 2;
        let half_down = size.height.saturating_sub(half_up + 1);

        let min = Position {
            x: center.x.saturating_sub(half_left),
            y: center.y.saturating_sub(half_up),
            z: center.z,
        };
        let max = Position {
            x: center.x.saturating_add(half_right),
            y: center.y.saturating_add(half_down),
            z: center.z,
        };

        Self {
            center,
            min,
            max,
            size,
        }
    }

    pub fn contains(&self, position: Position) -> bool {
        position.z == self.center.z
            && position.x >= self.min.x
            && position.x <= self.max.x
            && position.y >= self.min.y
            && position.y <= self.max.y
    }
}
