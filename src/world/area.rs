use crate::world::circles::Circles;
use crate::world::position::{Direction, Position, PositionDelta};

pub fn circle_offsets(circles: Option<&Circles>, radius: u8) -> Vec<(i16, i16)> {
    if radius == 0 {
        return vec![(0, 0)];
    }

    if let Some(circles) = circles {
        let radius = radius.min(circles.max_radius);
        let center_x = (circles.width / 2) as i16;
        let center_y = (circles.height / 2) as i16;
        let mut offsets = Vec::new();
        for y in 0..circles.height {
            for x in 0..circles.width {
                if let Some(value) = circles.cell(x, y) {
                    if value <= radius {
                        let dx = x as i16 - center_x;
                        let dy = y as i16 - center_y;
                        offsets.push((dx, dy));
                    }
                }
            }
        }
        return offsets;
    }

    let radius_i = i16::from(radius);
    let mut offsets = Vec::new();
    for dy in -radius_i..=radius_i {
        for dx in -radius_i..=radius_i {
            if (dx * dx + dy * dy) <= radius_i * radius_i {
                offsets.push((dx, dy));
            }
        }
    }
    offsets
}

pub fn circle_positions(
    circles: Option<&Circles>,
    center: Position,
    radius: u8,
) -> Vec<Position> {
    circle_offsets(circles, radius)
        .into_iter()
        .filter_map(|(dx, dy)| {
            center.offset(PositionDelta {
                dx,
                dy,
                dz: 0,
            })
        })
        .collect()
}

pub fn line_positions(origin: Position, direction: Direction, length: u8) -> Vec<Position> {
    if length == 0 {
        return Vec::new();
    }

    let mut positions = Vec::new();
    let mut current = origin;
    for _ in 0..length {
        let Some(next) = current.step(direction) else {
            break;
        };
        positions.push(next);
        current = next;
    }
    positions
}

pub fn cone_positions(
    origin: Position,
    direction: Direction,
    range: u8,
    angle_degrees: u16,
) -> Vec<Position> {
    if range == 0 {
        return Vec::new();
    }

    let base_delta = direction.delta();
    let base_angle = (base_delta.dy as f32).atan2(base_delta.dx as f32).to_degrees();
    let max_angle = angle_degrees as f32;
    let range_i = i16::from(range);
    let mut positions = Vec::new();

    for dy in -range_i..=range_i {
        for dx in -range_i..=range_i {
            if dx == 0 && dy == 0 {
                continue;
            }
            if dx.abs().max(dy.abs()) > range_i {
                continue;
            }
            let angle = (dy as f32).atan2(dx as f32).to_degrees();
            if angle_delta(base_angle, angle).abs() > max_angle {
                continue;
            }
            if let Some(position) = origin.offset(PositionDelta { dx, dy, dz: 0 }) {
                positions.push(position);
            }
        }
    }

    positions
}

fn angle_delta(a: f32, b: f32) -> f32 {
    let mut delta = b - a;
    while delta > 180.0 {
        delta -= 360.0;
    }
    while delta < -180.0 {
        delta += 360.0;
    }
    delta
}
