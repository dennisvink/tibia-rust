use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;

use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine as _;
use sha1::{Digest, Sha1};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug, Clone)]
pub struct WsHandshakeConfig {
    pub allowed_origins: Option<Vec<String>>,
    pub max_request_bytes: usize,
}

impl Default for WsHandshakeConfig {
    fn default() -> Self {
        Self {
            allowed_origins: None,
            max_request_bytes: 8192,
        }
    }
}

#[derive(Debug)]
pub struct WsFrame {
    pub opcode: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum WsFrameError {
    Timeout,
    Closed,
    Io(std::io::Error),
    Protocol(String),
}

pub fn accept_handshake(
    stream: &mut TcpStream,
    config: &WsHandshakeConfig,
) -> Result<(), String> {
    let request = read_http_request(stream, config.max_request_bytes)?;
    let (_path, headers) = parse_headers(&request)?;
    let origin = headers.get("origin").cloned();

    if !matches!(
        headers.get("upgrade").map(|value| value.to_ascii_lowercase()),
        Some(value) if value == "websocket"
    ) {
        reject_handshake(stream, 400, "Missing Upgrade: websocket")?;
        return Err("websocket upgrade missing".to_string());
    }
    let connection = headers
        .get("connection")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if !connection.contains("upgrade") {
        reject_handshake(stream, 400, "Missing Connection: Upgrade")?;
        return Err("websocket connection upgrade missing".to_string());
    }
    let version = headers
        .get("sec-websocket-version")
        .map(|value| value.trim())
        .unwrap_or("");
    if version != "13" {
        reject_handshake(stream, 400, "Unsupported WebSocket version")?;
        return Err(format!("unsupported websocket version '{version}'"));
    }
    let key = headers
        .get("sec-websocket-key")
        .ok_or_else(|| "missing sec-websocket-key".to_string())?;

    if let Some(allowed) = config.allowed_origins.as_ref() {
        let origin_value = origin
            .as_ref()
            .map(|value| value.trim().to_string())
            .unwrap_or_default();
        let allow_all = allowed.iter().any(|value| value == "*");
        let allowed_origin = allow_all || allowed.iter().any(|value| value == &origin_value);
        if !allowed_origin {
            reject_handshake(stream, 403, "Origin not allowed")?;
            return Err("websocket origin rejected".to_string());
        }
    }

    let mut sha1 = Sha1::new();
    sha1.update(key.trim().as_bytes());
    sha1.update(WS_GUID.as_bytes());
    let accept = BASE64_ENGINE.encode(sha1.finalize());

    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: {accept}\r\n\
\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|err| format!("websocket handshake write failed: {err}"))?;

    Ok(())
}

pub fn read_frame(stream: &mut TcpStream, max_payload: usize) -> Result<WsFrame, WsFrameError> {
    let mut header = [0u8; 2];
    if let Err(err) = stream.read_exact(&mut header) {
        return Err(map_ws_read_error(err));
    }

    let fin = (header[0] & 0x80) != 0;
    let opcode = header[0] & 0x0f;
    if !fin {
        return Err(WsFrameError::Protocol(
            "fragmented frames not supported".to_string(),
        ));
    }
    let masked = (header[1] & 0x80) != 0;
    let mut len = (header[1] & 0x7f) as u64;
    if len == 126 {
        let mut ext = [0u8; 2];
        if let Err(err) = stream.read_exact(&mut ext) {
            return Err(map_ws_read_error(err));
        }
        len = u16::from_be_bytes(ext) as u64;
    } else if len == 127 {
        let mut ext = [0u8; 8];
        if let Err(err) = stream.read_exact(&mut ext) {
            return Err(map_ws_read_error(err));
        }
        len = u64::from_be_bytes(ext);
    }

    if opcode >= 0x8 && len > 125 {
        return Err(WsFrameError::Protocol(
            "control frame payload too large".to_string(),
        ));
    }
    if len as usize > max_payload {
        return Err(WsFrameError::Protocol(format!(
            "websocket payload {} exceeds max {}",
            len, max_payload
        )));
    }

    let mut mask = [0u8; 4];
    if masked {
        if let Err(err) = stream.read_exact(&mut mask) {
            return Err(map_ws_read_error(err));
        }
    }

    let mut payload = vec![0u8; len as usize];
    if !payload.is_empty() {
        if let Err(err) = stream.read_exact(&mut payload) {
            return Err(map_ws_read_error(err));
        }
        if masked {
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[idx % 4];
            }
        }
    }

    Ok(WsFrame { opcode, payload })
}

pub fn write_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> Result<(), String> {
    let len = payload.len();
    let mut header = Vec::with_capacity(14);
    header.push(0x80 | (opcode & 0x0f));
    if len < 126 {
        header.push(len as u8);
    } else if len <= u16::MAX as usize {
        header.push(126);
        header.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        header.push(127);
        header.extend_from_slice(&(len as u64).to_be_bytes());
    }
    stream
        .write_all(&header)
        .map_err(|err| format!("websocket header write failed: {err}"))?;
    if !payload.is_empty() {
        stream
            .write_all(payload)
            .map_err(|err| format!("websocket payload write failed: {err}"))?;
    }
    Ok(())
}

fn read_http_request(stream: &mut TcpStream, max_bytes: usize) -> Result<String, String> {
    let mut data = Vec::new();
    let mut buf = [0u8; 512];
    loop {
        let read = stream
            .read(&mut buf)
            .map_err(|err| format!("handshake read failed: {err}"))?;
        if read == 0 {
            return Err("handshake closed".to_string());
        }
        data.extend_from_slice(&buf[..read]);
        if data.len() > max_bytes {
            return Err("handshake exceeded max bytes".to_string());
        }
        if data.windows(4).any(|chunk| chunk == b"\r\n\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&data).to_string())
}

fn parse_headers(request: &str) -> Result<(String, HashMap<String, String>), String> {
    let mut lines = request.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "empty handshake request".to_string())?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");
    if method.to_ascii_uppercase() != "GET" {
        return Err(format!("unexpected method '{method}'"));
    }
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(
                key.trim().to_ascii_lowercase(),
                value.trim().to_string(),
            );
        }
    }
    Ok((path.to_string(), headers))
}

fn reject_handshake(stream: &mut TcpStream, code: u16, message: &str) -> Result<(), String> {
    let response = format!("HTTP/1.1 {code} {message}\r\n\r\n");
    stream
        .write_all(response.as_bytes())
        .map_err(|err| format!("handshake reject write failed: {err}"))?;
    Ok(())
}

fn map_ws_read_error(err: std::io::Error) -> WsFrameError {
    match err.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => WsFrameError::Timeout,
        std::io::ErrorKind::UnexpectedEof => WsFrameError::Closed,
        _ => WsFrameError::Io(err),
    }
}
