# tibia-rust

Rust implementation of the Tibia game server, based on [fusion32/tibia-game](https://github.com/fusion32/tibia-game).

This project is **for educational purposes only**. Most core features work, but it is **not yet 100% on par** with the original server behavior and data compatibility.

## Status

- Single-crate Rust server (`edition = 2021`)
- Login + game TCP servers
- Optional WebSocket login/game endpoints
- Optional status server endpoint
- Save/account loading and persistence
- Spell metadata loaded from bundled CSV resources in `data/spells/`

## Repository Structure

- `src/main.rs`: CLI entrypoint
- `src/lib.rs`: bootstrap, server startup, world loading
- `src/net/`: login/game protocols and server runtime
- `src/world/`: world state, map, NPC/monster loading, movement/use logic
- `src/entities/`: players, items, skills, spells
- `src/combat/`: damage, conditions, combat rules
- `src/persistence/`: save files, accounts, autosave
- `src/scripting/`: parsers/runtime helpers for NPC/monster/raid script data
- `src/admin/`: in-game admin command parsing
- `src/telemetry/`: log file setup and metrics helpers
- `src/bin/`: helper binaries (`spell_validate`, `spell_count`, `spell_effect_audit`)
- `data/spells/`: spell metadata CSV files required at compile time
- `save/`: sample local save data (`accounts.txt`, `players/*.sav`)

## Prerequisites

- Rust toolchain (stable) with Cargo

## Build

```bash
cargo build
```

## Run

The server expects an asset root directory as the first argument:

```bash
cargo run -- <asset-root>
```

Example using the current repository root as asset root:

```bash
cargo run -- .
```

CLI format:

```text
tibia <asset-root> [login_bind_addr] [game_bind_addr] [ws_game_bind_addr] [ws_login_bind_addr] [status_bind_addr]
```

Defaults:

- login bind: `0.0.0.0:7171`
- game bind: `0.0.0.0:7172`
- WS game bind: derived from game bind (`+1` port) unless overridden
- WS login bind: derived from WS game bind (`+1` port) unless overridden
- status bind: disabled unless set

## Runtime Environment Variables

- `TIBIA_WS_GAME_ADDR`: override WS game bind address
- `TIBIA_WS_LOGIN_ADDR`: override WS login bind address
- `TIBIA_STATUS_ADDR`: enable/override status server bind address
- `TIBIA_WS_ORIGINS`: comma-separated allowed WS origins
- `TIBIA_AUTOSAVE_SECS`: autosave interval in seconds (`0` or invalid disables autosave)
- `TIBIA_WORLD_NAME`: world name shown in login/status data
- `TIBIA_MAX_PLAYERS`: max player count for status endpoint
- `TIBIA_PACKET_TRACE`: packet trace toggle for debugging
- `TIBIA_SPELL_DEBUG`: additional spell debugging

## Expected Asset Layout

`<asset-root>` should contain game data directories used by startup/world loading:

- `dat/` (for files like `map.dat`, `mem.dat`, `circles.dat`, `monster.db`, `objects.srv`, etc.)
- `map/`
- `npc/`
- `mon/`
- `save/` (`accounts.txt`, optional `banlist.txt`, `players/*.sav`)

In this repository, `data/spells/*.csv` is also required to compile spell definitions.

## Useful Commands

Build and run checks:

```bash
cargo build
cargo test
```

Spell helper binaries:

```bash
cargo run --bin spell_validate
cargo run --bin spell_count
cargo run --bin spell_effect_audit
```

## Connecting to the game world
You'll need a client speaking the 7.72 protocol but without XTEA encryption. Your best bet is to modify otclient.
This Rust server also exposes a Websocket on port 7173, so you could connect through it from a HTML website.
A client is not part of this repo. You'll have to roll your own.

## Notes

- Startup performs asset scans and validation passes for saves, NPC scripts, and monster scripts.
- Logs are written under `<asset-root>/log/`.
- This codebase is intended for learning and experimentation, not production deployment.
