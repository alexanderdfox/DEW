use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::path::Path;

use crate::models::{CreateDevice, Device, TelemetryPoint, UpdateDevice, new_id};

#[derive(Clone)]
pub struct Db {
    pool: SqlitePool,
}

impl Db {
    pub async fn connect(path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let url = format!("sqlite:{path}?mode=rwc");
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;

        // Run embedded migration
        sqlx::query(include_str!("../../migrations/001_init.sql"))
            .execute(&pool)
            .await?;

        Ok(Self { pool })
    }

    pub async fn list_devices(&self) -> Result<Vec<Device>> {
        let rows = sqlx::query_as::<_, Device>(
            "SELECT id, name, device_type, topic_base, state, online, last_seen, meta, created_at, updated_at
             FROM devices ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_device(&self, id: &str) -> Result<Option<Device>> {
        let row = sqlx::query_as::<_, Device>(
            "SELECT id, name, device_type, topic_base, state, online, last_seen, meta, created_at, updated_at
             FROM devices WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_device_by_topic(&self, topic_base: &str) -> Result<Option<Device>> {
        let row = sqlx::query_as::<_, Device>(
            "SELECT id, name, device_type, topic_base, state, online, last_seen, meta, created_at, updated_at
             FROM devices WHERE topic_base = ?",
        )
        .bind(topic_base)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn create_device(&self, input: CreateDevice) -> Result<Device> {
        let id = new_id();
        let device_type = input.device_type.unwrap_or_else(|| "generic".into());
        let meta = input
            .meta
            .map(|v| v.to_string())
            .unwrap_or_else(|| "{}".into());

        sqlx::query(
            "INSERT INTO devices (id, name, device_type, topic_base, meta)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&input.name)
        .bind(&device_type)
        .bind(&input.topic_base)
        .bind(&meta)
        .execute(&self.pool)
        .await?;

        self.get_device(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("device not found after insert"))
    }

    pub async fn update_device(&self, id: &str, input: UpdateDevice) -> Result<Option<Device>> {
        let existing = match self.get_device(id).await? {
            Some(d) => d,
            None => return Ok(None),
        };

        let name = input.name.unwrap_or(existing.name);
        let device_type = input.device_type.unwrap_or(existing.device_type);
        let topic_base = input.topic_base.unwrap_or(existing.topic_base);
        let meta = input
            .meta
            .map(|v| v.to_string())
            .unwrap_or(existing.meta);

        sqlx::query(
            "UPDATE devices SET name=?, device_type=?, topic_base=?, meta=?, updated_at=datetime('now')
             WHERE id=?",
        )
        .bind(&name)
        .bind(&device_type)
        .bind(&topic_base)
        .bind(&meta)
        .bind(id)
        .execute(&self.pool)
        .await?;

        self.get_device(id).await
    }

    pub async fn delete_device(&self, id: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM devices WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn set_online(&self, id: &str, online: bool) -> Result<Option<Device>> {
        sqlx::query(
            "UPDATE devices SET online=?, last_seen=datetime('now'), updated_at=datetime('now') WHERE id=?",
        )
        .bind(if online { 1 } else { 0 })
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.get_device(id).await
    }

    pub async fn update_state(&self, id: &str, state: &serde_json::Value) -> Result<Option<Device>> {
        let state_str = state.to_string();
        sqlx::query(
            "UPDATE devices SET state=?, last_seen=datetime('now'), online=1, updated_at=datetime('now') WHERE id=?",
        )
        .bind(&state_str)
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.get_device(id).await
    }

    pub async fn insert_telemetry(&self, point: &TelemetryPoint) -> Result<()> {
        sqlx::query(
            "INSERT INTO telemetry (device_id, key, value, unit, recorded_at)
             VALUES (?, ?, ?, ?, COALESCE(?, datetime('now')))",
        )
        .bind(&point.device_id)
        .bind(&point.key)
        .bind(point.value)
        .bind(&point.unit)
        .bind(point.recorded_at.map(|t| t.to_rfc3339()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recent_telemetry(
        &self,
        device_id: &str,
        key: Option<&str>,
        limit: i64,
    ) -> Result<Vec<(String, f64, Option<String>, String)>> {
        // returns (key, value, unit, recorded_at)
        let rows = if let Some(k) = key {
            sqlx::query_as::<_, (String, f64, Option<String>, String)>(
                "SELECT key, value, unit, recorded_at FROM telemetry
                 WHERE device_id = ? AND key = ?
                 ORDER BY recorded_at DESC LIMIT ?",
            )
            .bind(device_id)
            .bind(k)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, (String, f64, Option<String>, String)>(
                "SELECT key, value, unit, recorded_at FROM telemetry
                 WHERE device_id = ?
                 ORDER BY recorded_at DESC LIMIT ?",
            )
            .bind(device_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    pub async fn log_command(
        &self,
        device_id: &str,
        command: &str,
        payload: Option<&str>,
        source: &str,
        success: Option<bool>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO commands (device_id, command, payload, source, success)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(device_id)
        .bind(command)
        .bind(payload)
        .bind(source)
        .bind(success.map(|s| if s { 1 } else { 0 }))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn stats(&self) -> Result<serde_json::Value> {
        let (device_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM devices")
            .fetch_one(&self.pool)
            .await?;
        let (online_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM devices WHERE online = 1")
                .fetch_one(&self.pool)
                .await?;
        let (telemetry_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM telemetry")
            .fetch_one(&self.pool)
            .await?;
        Ok(serde_json::json!({
            "devices": device_count,
            "online": online_count,
            "telemetry_points": telemetry_count,
        }))
    }
}
