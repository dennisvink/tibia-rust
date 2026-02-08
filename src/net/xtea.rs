pub const XTEA_KEY_BYTES: usize = 16;
pub const XTEA_BLOCK_BYTES: usize = 8;
const XTEA_DELTA: u32 = 0x9e3779b9;
const XTEA_ROUNDS: u32 = 32;
const XTEA_PAD_BYTE: u8 = 0x33;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XteaKey {
    key: [u32; 4],
}

impl XteaKey {
    pub fn from_bytes(bytes: [u8; XTEA_KEY_BYTES]) -> Self {
        let mut key = [0u32; 4];
        for (idx, chunk) in bytes.chunks_exact(4).enumerate() {
            key[idx] = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        Self { key }
    }

    pub fn is_zero(&self) -> bool {
        self.key.iter().all(|&value| value == 0)
    }

    pub fn decrypt_in_place(&self, data: &mut [u8]) -> Result<(), String> {
        if data.len() % XTEA_BLOCK_BYTES != 0 {
            return Err("xtea decrypt length is not a multiple of 8".to_string());
        }
        for chunk in data.chunks_exact_mut(8) {
            let mut v0 = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            let mut v1 = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
            let mut sum = XTEA_DELTA.wrapping_mul(XTEA_ROUNDS);
            for _ in 0..XTEA_ROUNDS {
                v1 = v1.wrapping_sub(
                    ((v0 << 4) ^ (v0 >> 5))
                        .wrapping_add(v0)
                        .wrapping_add(sum ^ self.key[((sum >> 11) & 3) as usize]),
                );
                sum = sum.wrapping_sub(XTEA_DELTA);
                v0 = v0.wrapping_sub(
                    ((v1 << 4) ^ (v1 >> 5))
                        .wrapping_add(v1)
                        .wrapping_add(sum ^ self.key[(sum & 3) as usize]),
                );
            }
            chunk[..4].copy_from_slice(&v0.to_le_bytes());
            chunk[4..].copy_from_slice(&v1.to_le_bytes());
        }
        Ok(())
    }

    pub fn encrypt_in_place(&self, data: &mut [u8]) -> Result<(), String> {
        if data.len() % XTEA_BLOCK_BYTES != 0 {
            return Err("xtea encrypt length is not a multiple of 8".to_string());
        }
        for chunk in data.chunks_exact_mut(8) {
            let mut v0 = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            let mut v1 = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
            let mut sum = 0u32;
            for _ in 0..XTEA_ROUNDS {
                v0 = v0.wrapping_add(
                    ((v1 << 4) ^ (v1 >> 5))
                        .wrapping_add(v1)
                        .wrapping_add(sum ^ self.key[(sum & 3) as usize]),
                );
                sum = sum.wrapping_add(XTEA_DELTA);
                v1 = v1.wrapping_add(
                    ((v0 << 4) ^ (v0 >> 5))
                        .wrapping_add(v0)
                        .wrapping_add(sum ^ self.key[((sum >> 11) & 3) as usize]),
                );
            }
            chunk[..4].copy_from_slice(&v0.to_le_bytes());
            chunk[4..].copy_from_slice(&v1.to_le_bytes());
        }
        Ok(())
    }

    pub fn decrypt_to_vec(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut buf = data.to_vec();
        self.decrypt_in_place(&mut buf)?;
        Ok(buf)
    }

    pub fn encrypt_padded(&self, data: &[u8]) -> Vec<u8> {
        let mut buf = data.to_vec();
        let padding = (XTEA_BLOCK_BYTES - (buf.len() % XTEA_BLOCK_BYTES)) % XTEA_BLOCK_BYTES;
        if padding > 0 {
            buf.extend(std::iter::repeat(XTEA_PAD_BYTE).take(padding));
        }
        let _ = self.encrypt_in_place(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xtea_roundtrip_preserves_payload_with_padding() {
        let key = XteaKey::from_bytes([0x11; 16]);
        let payload = b"hello-xtea";
        let encrypted = key.encrypt_padded(payload);
        assert_eq!(encrypted.len() % XTEA_BLOCK_BYTES, 0);
        let decrypted = key.decrypt_to_vec(&encrypted).expect("decrypt");
        assert_eq!(&decrypted[..payload.len()], payload);
        for byte in &decrypted[payload.len()..] {
            assert_eq!(*byte, XTEA_PAD_BYTE);
        }
    }

    #[test]
    fn xtea_roundtrip_exact_block() {
        let key = XteaKey::from_bytes([0x22; 16]);
        let payload = b"12345678";
        let encrypted = key.encrypt_padded(payload);
        assert_eq!(encrypted.len(), payload.len());
        let decrypted = key.decrypt_to_vec(&encrypted).expect("decrypt");
        assert_eq!(decrypted, payload);
    }
}
