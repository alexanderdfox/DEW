use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub device_type: String,
    pub topic_base: String,
    pub state: String, // JSON
    pub online: i64,
    pub last_seen: Option<String>,
    pub meta: String, // JSON
    pub created_at: String,
    pub updated_at: String,
}

impl Device {
    pub fn state_json(&self) -> serde_json::Value {
        serde_json::from_str(&self.state).unwrap_or(serde_json::json!({}))
    }

    pub fn is_online(&self) -> bool {
        self.online != 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDevice {
    pub name: String,
    pub device_type: Option<String>,
    pub topic_base: String,
    pub meta: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateDevice {
    pub name: Option<String>,
    pub device_type: Option<String>,
    pub topic_base: Option<String>,
    pub meta: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceCommand {
    pub command: String,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryPoint {
    pub device_id: String,
    pub key: String,
    pub value: f64,
    pub unit: Option<String>,
    pub recorded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsEvent {
    pub event: String,
    pub device_id: Option<String>,
    pub data: serde_json::Value,
    pub ts: DateTime<Utc>,
}

impl WsEvent {
    pub fn device_update(device: &Device) -> Self {
        Self {
            event: "device_update".into(),
            device_id: Some(device.id.clone()),
            data: serde_json::to_value(device).unwrap_or_default(),
            ts: Utc::now(),
        }
    }

    pub fn telemetry(point: &TelemetryPoint) -> Self {
        Self {
            event: "telemetry".into(),
            device_id: Some(point.device_id.clone()),
            data: serde_json::to_value(point).unwrap_or_default(),
            ts: Utc::now(),
        }
    }

    pub fn status(msg: impl Into<String>) -> Self {
        Self {
            event: "status".into(),
            device_id: None,
            data: serde_json::json!({ "message": msg.into() }),
            ts: Utc::now(),
        }
    }
}

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}
