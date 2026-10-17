mod api;
mod db;
mod models;
mod mqtt;
mod ws;

use anyhow::Result;
use axum::{
    routing::{delete, get, post, put},
    Router,
};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::{
    cors::CorsLayer,
    services::ServeDir,
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::db::Db;
use crate::models::WsEvent;
use crate::mqtt::MqttBridge;

#[derive(Parser, Debug)]
#[command(name = "dew", about = "Lightweight self-hosted IoT hub")]
struct Args {
    /// HTTP listen address
    #[arg(long, env = "DEW_LISTEN", default_value = "0.0.0.0:8080")]
    listen: String,

    /// SQLite database path
    #[arg(long, env = "DEW_DB", default_value = "data/dew.db")]
    db: String,

    /// MQTT broker host (empty = MQTT disabled)
    #[arg(long, env = "DEW_MQTT_HOST", default_value = "")]
    mqtt_host: String,

    /// MQTT broker port
    #[arg(long, env = "DEW_MQTT_PORT", default_value = "1883")]
    mqtt_port: u16,

    /// MQTT client ID
    #[arg(long, env = "DEW_MQTT_CLIENT_ID", default_value = "dew")]
    mqtt_client_id: String,
}

pub struct AppState {
    pub db: Db,
    pub tx: broadcast::Sender<WsEvent>,
    pub mqtt: Option<MqttBridge>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "dew=info,tower_http=info,sqlx=warn".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    tracing::info!("Starting DEW");
    tracing::info!("Database: {}", args.db);

    let db = Db::connect(&args.db).await?;
    let (tx, _) = broadcast::channel::<WsEvent>(256);

    let mqtt = if !args.mqtt_host.is_empty() {
        tracing::info!("MQTT broker: {}:{}", args.mqtt_host, args.mqtt_port);
        let state_for_mqtt = Arc::new(AppState {
            db: db.clone(),
            tx: tx.clone(),
            mqtt: None,
        });
        match MqttBridge::start(
            &args.mqtt_host,
            args.mqtt_port,
            &args.mqtt_client_id,
            state_for_mqtt,
        )
        .await
        {
            Ok(m) => Some(m),
            Err(e) => {
                tracing::warn!("MQTT failed to start (continuing without it): {}", e);
                None
            }
        }
    } else {
        tracing::info!("MQTT disabled (set DEW_MQTT_HOST to enable)");
        None
    };

    let state = Arc::new(AppState { db, tx, mqtt });

    // Seed a couple of demo devices if empty
    if state.db.list_devices().await?.is_empty() {
        tracing::info!("Seeding demo devices...");
        let _ = state
            .db
            .create_device(models::CreateDevice {
                name: "Living Room Light".into(),
                device_type: Some("light".into()),
                topic_base: "home/living/light1".into(),
                meta: Some(serde_json::json!({ "room": "Living Room" })),
            })
            .await;
        let _ = state
            .db
            .create_device(models::CreateDevice {
                name: "Office Sensor".into(),
                device_type: Some("sensor".into()),
                topic_base: "home/office/sensor1".into(),
                meta: Some(serde_json::json!({ "room": "Office" })),
            })
            .await;
        let _ = state
            .db
            .create_device(models::CreateDevice {
                name: "Garage Switch".into(),
                device_type: Some("switch".into()),
                topic_base: "home/garage/switch1".into(),
                meta: Some(serde_json::json!({ "room": "Garage" })),
            })
            .await;
    }

    let app = Router::new()
        // API
        .route("/api/health", get(api::health))
        .route("/api/stats", get(api::stats))
        .route("/api/devices", get(api::list_devices).post(api::create_device))
        .route(
            "/api/devices/:id",
            get(api::get_device)
                .put(api::update_device)
                .delete(api::delete_device),
        )
        .route("/api/devices/:id/command", post(api::send_command))
        .route("/api/devices/:id/telemetry", get(api::get_telemetry))
        // WebSocket
        .route("/ws", get(ws::ws_handler))
        // Static dashboard
        .fallback_service(ServeDir::new("static").append_index_html_on_directories(true))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = args.listen.parse()?;
    tracing::info!("Dashboard → http://{}/", addr);
    tracing::info!("API       → http://{}/api/health", addr);
    tracing::info!("WebSocket → ws://{}/ws", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
