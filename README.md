# DEW

**Lightweight, self-hosted IoT hub written in pure Rust.**

A smaller, cleaner alternative to cloud platforms like Golioth — with a built-in modern web dashboard, REST API, WebSocket live updates, MQTT bridge, and SQLite storage. Single binary. No cloud required.

## Why DEW?

| Feature | DEW | Typical alternatives |
|---------|-----|----------------------|
| Language | Pure Rust | Mixed / Python / Node |
| Binary size | ~5–8 MB (release) | Often larger stacks |
| Web UI | Built-in, modern, real-time | Sometimes separate |
| MQTT | Optional bridge | Often required |
| Database | SQLite (zero config) | Postgres / cloud |
| Deploy | One binary + optional MQTT | Docker compose piles |
| Extensible | Clean modules | Varies |

## Quick Start

```bash
# Prerequisites: Rust 1.80+
cd dew
cargo run

# Open the dashboard
open http://localhost:8080
```

With MQTT (e.g. Mosquitto on localhost):

```bash
DEW_MQTT_HOST=localhost cargo run
```

## Configuration

| Env / Flag | Default | Description |
|------------|---------|-------------|
| `DEW_LISTEN` | `0.0.0.0:8080` | HTTP bind address |
| `DEW_DB` | `data/dew.db` | SQLite path |
| `DEW_MQTT_HOST` | _(empty)_ | MQTT broker host (empty = disabled) |
| `DEW_MQTT_PORT` | `1883` | MQTT port |
| `DEW_MQTT_CLIENT_ID` | `dew` | MQTT client ID |

## Device Protocol (MQTT)

Register a device in the UI (or API) with a **topic base**, e.g. `home/living/light1`.

**Device → Hub**

| Topic | Payload | Meaning |
|-------|---------|---------|
| `{base}/state` | JSON object | Full or partial state update |
| `{base}/telemetry/{key}` | number (text) | Telemetry point (temp, power, …) |

**Hub → Device**

| Topic | Payload | Meaning |
|-------|---------|---------|
| `{base}/cmd` | `{"command":"on"}` or `{"command":"set","payload":{...}}` | Command |

Common commands: `on`, `off`, `toggle`, `set`.

### Example (mosquitto_pub)

```bash
# Report state
mosquitto_pub -t home/living/light1/state -m '{"on":true,"brightness":80}'

# Report temperature
mosquitto_pub -t home/office/sensor1/telemetry/temperature -m '22.5'
```

## REST API

```
GET    /api/health
GET    /api/stats
GET    /api/devices
POST   /api/devices                  { "name", "device_type?", "topic_base", "meta?" }
GET    /api/devices/:id
PUT    /api/devices/:id
DELETE /api/devices/:id
POST   /api/devices/:id/command      { "command", "payload?" }
GET    /api/devices/:id/telemetry?key=&limit=
WS     /ws                           live events (snapshot, device_update, telemetry)
```

## Architecture

```
┌─────────────┐     MQTT      ┌──────────────────┐
│  IoT Device │ ◄──────────►  │  DEW    │
└─────────────┘               │                  │
                              │  ┌────────────┐  │     HTTP/WS
┌─────────────┐               │  │ Axum HTTP  │◄─┼──────────── Browser
│  MQTT Broker│ ◄─────────────┤  │ + WebSocket│  │
└─────────────┘               │  └─────┬──────┘  │
                              │        │         │
                              │  ┌─────▼──────┐  │
                              │  │  SQLite    │  │
                              │  └────────────┘  │
                              └──────────────────┘
```

## Build Release

```bash
cargo build --release
# binary: target/release/dew
```

## License

MIT
