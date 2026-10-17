use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tracing::debug;

use crate::models::WsEvent;
use crate::AppState;

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    // Send initial snapshot
    if let Ok(devices) = state.db.list_devices().await {
        let ev = WsEvent {
            event: "snapshot".into(),
            device_id: None,
            data: serde_json::json!({ "devices": devices }),
            ts: chrono::Utc::now(),
        };
        if let Ok(txt) = serde_json::to_string(&ev) {
            let _ = sender.send(Message::Text(txt.into())).await;
        }
    }

    // Forward broadcast events to this client
    let mut send_task = tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            if let Ok(txt) = serde_json::to_string(&event) {
                if sender.send(Message::Text(txt.into())).await.is_err() {
                    break;
                }
            }
        }
    });

    // Handle incoming messages (ping / future control)
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(t) => {
                    debug!("WS client message: {}", t);
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }
}
