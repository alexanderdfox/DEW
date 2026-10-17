-- Devices registered with the hub
CREATE TABLE IF NOT EXISTS devices (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    device_type TEXT NOT NULL DEFAULT 'generic',  -- light, switch, sensor, thermostat, etc.
    topic_base  TEXT NOT NULL,                     -- MQTT topic prefix, e.g. "home/livingroom/light1"
    state       TEXT NOT NULL DEFAULT '{}',        -- JSON blob of current state
    online      INTEGER NOT NULL DEFAULT 0,        -- 0 = offline, 1 = online
    last_seen   TEXT,
    meta        TEXT NOT NULL DEFAULT '{}',        -- extra metadata JSON
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Time-series telemetry (simple, pruneable)
CREATE TABLE IF NOT EXISTS telemetry (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,                    -- e.g. "temperature", "power", "humidity"
    value       REAL NOT NULL,
    unit        TEXT,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_telemetry_device_time ON telemetry(device_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_key ON telemetry(key);

-- Simple automation rules (optional, extensible)
CREATE TABLE IF NOT EXISTS rules (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    condition   TEXT NOT NULL,                    -- JSON: { "device_id": "...", "key": "temp", "op": ">", "value": 25 }
    action      TEXT NOT NULL,                    -- JSON: { "device_id": "...", "command": "off" }
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Command history / audit log
CREATE TABLE IF NOT EXISTS commands (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT NOT NULL,
    command     TEXT NOT NULL,
    payload     TEXT,
    source      TEXT NOT NULL DEFAULT 'api',      -- api | mqtt | rule | web
    success     INTEGER,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
