use crate::config::{AppConfig, DbEndpoint};
use crate::errors::{AppError, AppResult};

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

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
    let active = connect_with_retries(&cfg.active, DEFAULT_RETRIES).await.ok();
    let passive = connect_with_retries(&cfg.passive, DEFAULT_RETRIES).await.ok();
    if active.is_none() && passive.is_none() {
        return Err(AppError::db("failed to connect to both Active and Passive clusters"));
    }
    Ok(DbClients { active, passive })
}

async fn connect_with_retries(ep: &DbEndpoint, retries: usize) -> AppResult<Session> {
    let mut last_err: Option<AppError> = None;
    for _ in 0..retries {
        match connect_once(ep).await {
            Ok(sess) => return Ok(sess),
            Err(e) => { last_err = Some(e); }
        }
    }
    Err(last_err.unwrap_or_else(|| AppError::db("unknown connection error")))
}

async fn connect_once(ep: &DbEndpoint) -> AppResult<Session> {
    let addr = format!("{}:{}", ep.host, ep.port);
    let builder = SessionBuilder::new()
        .known_node(addr);
    let builder = if !ep.username.is_empty() || !ep.password.is_empty() {
        builder.user(ep.username.clone(), ep.password.clone())
    } else {
        builder
    };
    let session = builder
        .build()
        .await
        .map_err(|e| AppError::db(format!("connect error: {}", e)))?;
    Ok(session)
}