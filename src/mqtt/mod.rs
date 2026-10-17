use anyhow::Result;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::db::Db;
use crate::models::{TelemetryPoint, WsEvent};
use crate::AppState;

pub struct MqttBridge {
    client: AsyncClient,
}

impl MqttBridge {
    pub async fn start(
        broker: &str,
        port: u16,
        client_id: &str,
        state: Arc<AppState>,
    ) -> Result<Self> {
        let mut opts = MqttOptions::new(client_id, broker, port);
        opts.set_keep_alive(Duration::from_secs(30));
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 50);

        // Subscribe to common device topics
        // Devices publish:  <topic_base>/state   and  <topic_base>/telemetry/<key>
        // Hub publishes:    <topic_base>/cmd
        client.subscribe("+/+/state", QoS::AtLeastOnce).await?;
        client.subscribe("+/+/telemetry/#", QoS::AtLeastOnce).await?;
        client.subscribe("dew/+/state", QoS::AtLeastOnce).await?;
        client.subscribe("dew/+/telemetry/#", QoS::AtLeastOnce).await?;
        // Also a catch-all for discovery-style topics
        client.subscribe("home/#", QoS::AtLeastOnce).await?;

        info!("MQTT connected to {}:{}", broker, port);

        let client_clone = client.clone();
        let state_clone = state.clone();

        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let topic = publish.topic.clone();
                        let payload = String::from_utf8_lossy(&publish.payload).to_string();
                        if let Err(e) =
                            handle_message(&state_clone, &client_clone, &topic, &payload).await
                        {
                            warn!("MQTT handle error on {}: {}", topic, e);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!("MQTT eventloop error: {}", e);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });

        Ok(Self { client })
    }

    pub async fn send_command(&self, topic_base: &str, command: &str, payload: Option<&str>) -> Result<()> {
        let topic = format!("{topic_base}/cmd");
        let body = match payload {
            Some(p) => format!(r#"{{"command":"{command}","payload":{p}}}"#),
            None => format!(r#"{{"command":"{command}"}}"#),
        };
        self.client
            .publish(topic, QoS::AtLeastOnce, false, body)
            .await?;
        Ok(())
    }
}

async fn handle_message(
    state: &AppState,
    _client: &AsyncClient,
    topic: &str,
    payload: &str,
) -> Result<()> {
    let parts: Vec<&str> = topic.split('/').collect();

    // Expect patterns like: home/living/light1/state  or  home/living/light1/telemetry/temp
    if parts.len() < 3 {
        return Ok(());
    }

    let last = parts[parts.len() - 1];
    let topic_base = if last == "state" || parts[parts.len() - 2] == "telemetry" {
        if last == "state" {
            parts[..parts.len() - 1].join("/")
        } else {
            // .../telemetry/<key>
            parts[..parts.len() - 2].join("/")
        }
    } else {
        return Ok(());
    };

    let device = match state.db.get_device_by_topic(&topic_base).await? {
        Some(d) => d,
        None => {
            // Auto-register unknown devices (optional convenience)
            tracing::debug!("Unknown device topic {}, ignoring (register via UI/API)", topic_base);
            return Ok(());
        }
    };

    if last == "state" {
        // Merge or replace state JSON
        let new_state: serde_json::Value = serde_json::from_str(payload)
            .unwrap_or_else(|_| serde_json::json!({ "raw": payload }));

        if let Some(updated) = state.db.update_state(&device.id, &new_state).await? {
            let _ = state.tx.send(WsEvent::device_update(&updated));
        }
    } else if parts[parts.len() - 2] == "telemetry" {
        let key = last.to_string();
        let value: f64 = payload.trim().parse().unwrap_or(0.0);
        let point = TelemetryPoint {
            device_id: device.id.clone(),
            key: key.clone(),
            value,
            unit: None,
            recorded_at: None,
        };
        state.db.insert_telemetry(&point).await?;

        // Also reflect latest value into device state for convenience
        let mut st = device.state_json();
        st[&key] = serde_json::json!(value);
        let _ = state.db.update_state(&device.id, &st).await?;

        let _ = state.tx.send(WsEvent::telemetry(&point));
    }

    Ok(())
}
