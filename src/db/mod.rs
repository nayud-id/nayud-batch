use crate::config::{AppConfig, DbEndpoint, DriverConfig};
use crate::errors::{AppError, AppResult};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;

use std::time::Duration;

#[derive(Debug, Default)]
pub struct DbClients {
    pub active: Option<Session>,
    pub passive: Option<Session>,
}

impl DbClients {
    pub fn is_empty(&self) -> bool {
        self.active.is_none() && self.passive.is_none()
    }
}

const DEFAULT_RETRIES: usize = 3;

pub async fn init_clients(cfg: &AppConfig) -> AppResult<DbClients> {
    let active = connect_with_retries(&cfg.active, &cfg.driver, DEFAULT_RETRIES).await.ok();
    let passive = connect_with_retries(&cfg.passive, &cfg.driver, DEFAULT_RETRIES).await.ok();
    if active.is_none() && passive.is_none() {
        return Err(AppError::db("failed to connect to both Active and Passive clusters"));
    }
    Ok(DbClients { active, passive })
}

async fn connect_with_retries(ep: &DbEndpoint, drv: &DriverConfig, retries: usize) -> AppResult<Session> {
    let mut last_err: Option<AppError> = None;
    for _ in 0..retries {
        match connect_once(ep, drv).await {
            Ok(sess) => return Ok(sess),
            Err(e) => { last_err = Some(e); }
        }
    }
    Err(last_err.unwrap_or_else(|| AppError::db("unknown connection error")))
}

async fn connect_once(ep: &DbEndpoint, drv: &DriverConfig) -> AppResult<Session> {
    let addr = format!("{}:{}", ep.host, ep.port);
    let mut builder = SessionBuilder::new().known_node(addr);

    if !ep.username.is_empty() || !ep.password.is_empty() {
        builder = builder.user(ep.username.clone(), ep.password.clone());
    }

    if let Some(ms) = drv.connection_timeout_ms {
        if ms > 0 {
            builder = builder.connection_timeout(Duration::from_millis(ms));
        }
    }

    if let Some(comp) = drv.compression.as_ref() {
        match comp.to_ascii_lowercase().as_str() {
            "snappy" => { builder = builder.compression(Some(Compression::Snappy)); }
            "lz4" => { builder = builder.compression(Some(Compression::Lz4)); }
            "none" | "" => { builder = builder.compression(None); }
            _ => {}
        }
    }

    if let Some(ms) = drv.request_timeout_ms {
        let eph = if ms == 0 {
            ExecutionProfile::builder().request_timeout(None).build().into_handle()
        } else {
            ExecutionProfile::builder()
                .request_timeout(Some(Duration::from_millis(ms)))
                .build()
                .into_handle()
        };
        builder = builder.default_execution_profile_handle(eph);
    }

    let session = builder
        .build()
        .await
        .map_err(|e| AppError::db(format!("connect error: {}", e)))?;
    Ok(session)
}