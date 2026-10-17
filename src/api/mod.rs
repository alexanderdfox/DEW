use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::models::{CreateDevice, DeviceCommand, UpdateDevice, WsEvent};
use crate::AppState;

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

fn err(code: StatusCode, msg: impl Into<String>) -> (StatusCode, String) {
    (code, msg.into())
}

// ---------- Devices ----------

pub async fn list_devices(State(state): State<Arc<AppState>>) -> ApiResult<Vec<crate::models::Device>> {
    state
        .db
        .list_devices()
        .await
        .map(Json)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn get_device(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<crate::models::Device> {
    match state.db.get_device(&id).await {
        Ok(Some(d)) => Ok(Json(d)),
        Ok(None) => Err(err(StatusCode::NOT_FOUND, "device not found")),
        Err(e) => Err(err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

pub async fn create_device(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateDevice>,
) -> ApiResult<crate::models::Device> {
    let device = state
        .db
        .create_device(body)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let _ = state.tx.send(WsEvent::device_update(&device));
    Ok(Json(device))
}

pub async fn update_device(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateDevice>,
) -> ApiResult<crate::models::Device> {
    match state.db.update_device(&id, body).await {
        Ok(Some(d)) => {
            let _ = state.tx.send(WsEvent::device_update(&d));
            Ok(Json(d))
        }
        Ok(None) => Err(err(StatusCode::NOT_FOUND, "device not found")),
        Err(e) => Err(err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

pub async fn delete_device(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_device(&id).await {
        Ok(true) => {
            let _ = state.tx.send(WsEvent::status(format!("device {id} deleted")));
            StatusCode::NO_CONTENT
        }
        Ok(false) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ---------- Commands ----------

pub async fn send_command(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<DeviceCommand>,
) -> impl IntoResponse {
    let device = match state.db.get_device(&id).await {
        Ok(Some(d)) => d,
        Ok(None) => return (StatusCode::NOT_FOUND, "device not found".into()),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    let payload_str = body.payload.as_ref().map(|v| v.to_string());

    // Publish over MQTT if bridge is available
    if let Some(mqtt) = &state.mqtt {
        if let Err(e) = mqtt
            .send_command(&device.topic_base, &body.command, payload_str.as_deref())
            .await
        {
            let _ = state
                .db
                .log_command(&id, &body.command, payload_str.as_deref(), "api", Some(false))
                .await;
            return (StatusCode::BAD_GATEWAY, format!("MQTT publish failed: {e}"));
        }
    }

    // Optimistically update local state for common commands
    let mut st = device.state_json();
    match body.command.as_str() {
        "on" | "turn_on" => {
            st["power"] = serde_json::json!("on");
            st["on"] = serde_json::json!(true);
        }
        "off" | "turn_off" => {
            st["power"] = serde_json::json!("off");
            st["on"] = serde_json::json!(false);
        }
        "toggle" => {
            let current = st.get("on").and_then(|v| v.as_bool()).unwrap_or(false);
            st["on"] = serde_json::json!(!current);
            st["power"] = serde_json::json!(if !current { "on" } else { "off" });
        }
        "set" => {
            if let Some(p) = &body.payload {
                if let Some(obj) = p.as_object() {
                    for (k, v) in obj {
                        st[k] = v.clone();
                    }
                }
            }
        }
        _ => {
            st["last_command"] = serde_json::json!(body.command);
        }
    }

    if let Ok(Some(updated)) = state.db.update_state(&id, &st).await {
        let _ = state.tx.send(WsEvent::device_update(&updated));
    }

    let _ = state
        .db
        .log_command(&id, &body.command, payload_str.as_deref(), "api", Some(true))
        .await;

    (
        StatusCode::OK,
        serde_json::json!({ "ok": true, "command": body.command }).to_string(),
    )
}

// ---------- Telemetry ----------

#[derive(Deserialize)]
pub struct TelemetryQuery {
    pub key: Option<String>,
    pub limit: Option<i64>,
}

pub async fn get_telemetry(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<TelemetryQuery>,
) -> ApiResult<serde_json::Value> {
    let limit = q.limit.unwrap_or(100).min(1000);
    let rows = state
        .db
        .recent_telemetry(&id, q.key.as_deref(), limit)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let points: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(key, value, unit, recorded_at)| {
            serde_json::json!({
                "key": key,
                "value": value,
                "unit": unit,
                "recorded_at": recorded_at,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "points": points })))
}

// ---------- Stats / Health ----------

pub async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.db.stats().await.unwrap_or(serde_json::json!({}));
    Json(serde_json::json!({
        "status": "ok",
        "service": "dew",
        "stats": stats,
    }))
}

pub async fn stats(State(state): State<Arc<AppState>>) -> ApiResult<serde_json::Value> {
    state
        .db
        .stats()
        .await
        .map(Json)
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
