#[derive(Debug, Clone)]
pub struct PacketReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PacketReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        if self.remaining() < 1 {
            return None;
        }
        let value = self.data[self.pos];
        self.pos += 1;
        Some(value)
    }

    pub fn read_u16_le(&mut self) -> Option<u16> {
        if self.remaining() < 2 {
            return None;
        }
        let lo = self.data[self.pos] as u16;
        let hi = self.data[self.pos + 1] as u16;
        self.pos += 2;
        Some(lo | (hi << 8))
    }

    pub fn read_u32_le(&mut self) -> Option<u32> {
        if self.remaining() < 4 {
            return None;
        }
        let b0 = self.data[self.pos] as u32;
        let b1 = self.data[self.pos + 1] as u32;
        let b2 = self.data[self.pos + 2] as u32;
        let b3 = self.data[self.pos + 3] as u32;
        self.pos += 4;
        Some(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))
    }

    pub fn read_u64_le(&mut self) -> Option<u64> {
        if self.remaining() < 8 {
            return None;
        }
        let low = self.read_u32_le()? as u64;
        let high = self.read_u32_le()? as u64;
        Some(low | (high << 32))
    }

    pub fn read_len_prefixed(&mut self) -> Option<usize> {
        let len = self.read_u16_le()? as usize;
        if len == 0xffff {
            let long_len = self.read_u32_le()? as usize;
            return Some(long_len);
        }
        Some(len)
    }

    pub fn read_string_limited(&mut self, max_len: usize) -> Option<Vec<u8>> {
        let len = self.read_len_prefixed()?;
        if len == 0 {
            return Some(Vec::new());
        }

        if max_len > 0 && len >= max_len {
            let take = max_len - 1;
            let kept = self.read_bytes(take)?.to_vec();
            let skip = len.saturating_sub(take);
            self.skip(skip)?;
            return Some(kept);
        }

        Some(self.read_bytes(len)?.to_vec())
    }

    pub fn read_string_lossy(&mut self, max_len: usize) -> Option<String> {
        let bytes = self.read_string_limited(max_len)?;
        Some(String::from_utf8_lossy(&bytes).to_string())
    }

    pub fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.remaining() < len {
            return None;
        }
        let start = self.pos;
        self.pos += len;
        Some(&self.data[start..start + len])
    }

    pub fn skip(&mut self, len: usize) -> Option<()> {
        if self.remaining() < len {
            return None;
        }
        self.pos += len;
        Some(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct PacketWriter {
    data: Vec<u8>,
}

impl PacketWriter {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn write_u8(&mut self, value: u8) {
        self.data.push(value);
    }

    pub fn write_u16_le(&mut self, value: u16) {
        self.data.push((value & 0xff) as u8);
        self.data.push((value >> 8) as u8);
    }

    pub fn write_u32_le(&mut self, value: u32) {
        self.data.push((value & 0xff) as u8);
        self.data.push(((value >> 8) & 0xff) as u8);
        self.data.push(((value >> 16) & 0xff) as u8);
        self.data.push(((value >> 24) & 0xff) as u8);
    }

    pub fn write_u64_le(&mut self, value: u64) {
        self.write_u32_le((value & 0xffff_ffff) as u32);
        self.write_u32_le((value >> 32) as u32);
    }

    pub fn write_len_prefixed(&mut self, len: usize) {
        if len > 0xfffe {
            self.write_u16_le(0xffff);
            self.write_u32_le(len as u32);
        } else {
            self.write_u16_le(len as u16);
        }
    }

    pub fn write_string(&mut self, bytes: &[u8]) {
        self.write_len_prefixed(bytes.len());
        self.write_bytes(bytes);
    }

    pub fn write_string_str(&mut self, value: &str) {
        self.write_string(value.as_bytes());
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lcg_next(state: &mut u64) -> u32 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        (*state >> 32) as u32
    }

    #[test]
    fn string_roundtrip_varied_lengths() {
        let mut state = 0x1234_5678_9abc_def0;
        for _ in 0..128 {
            let len = (lcg_next(&mut state) % 1024) as usize;
            let mut bytes = Vec::with_capacity(len);
            for _ in 0..len {
                bytes.push((lcg_next(&mut state) & 0xff) as u8);
            }
            let mut writer = PacketWriter::new();
            writer.write_string(&bytes);
            let mut reader = PacketReader::new(writer.as_slice());
            let max_len = len.saturating_add(1).max(1);
            let decoded = reader.read_string_limited(max_len).expect("string");
            assert_eq!(decoded, bytes);
            assert_eq!(reader.remaining(), 0);
        }
    }

    #[test]
    fn string_truncates_and_skips() {
        let mut writer = PacketWriter::new();
        writer.write_string(b"abcdefghij");
        writer.write_u8(0x42);
        let mut reader = PacketReader::new(writer.as_slice());
        let decoded = reader.read_string_limited(5).expect("string");
        assert_eq!(decoded, b"abcd");
        assert_eq!(reader.read_u8(), Some(0x42));
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn long_len_prefix_roundtrip() {
        let len = 0xffff + 5;
        let bytes = vec![0x7f; len];
        let mut writer = PacketWriter::new();
        writer.write_string(&bytes);
        let mut reader = PacketReader::new(writer.as_slice());
        let decoded = reader.read_string_limited(len + 1).expect("string");
        assert_eq!(decoded.len(), len);
        assert_eq!(decoded, bytes);
        assert_eq!(reader.remaining(), 0);
    }
}
