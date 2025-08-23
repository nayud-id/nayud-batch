use crate::config::{AppConfig, DbEndpoint, DriverConfig};
use crate::errors::{AppError, AppResult};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::statement::prepared::PreparedStatement;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

type PreparedCache = Arc<Mutex<HashMap<String, Arc<PreparedStatement>>>>;

#[derive(Debug)]
pub struct DbClients {
    pub active: Option<Session>,
    pub passive: Option<Session>,
    active_cache: PreparedCache,
    passive_cache: PreparedCache,
}

impl Default for DbClients {
    fn default() -> Self {
        Self {
            active: None,
            passive: None,
            active_cache: Arc::new(Mutex::new(HashMap::new())),
            passive_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl DbClients {
    pub fn is_empty(&self) -> bool { self.active.is_none() && self.passive.is_none() }

    async fn get_or_prepare(&self, which_active: bool, cql: &str) -> Option<Arc<PreparedStatement>> {
        let (sess_opt, cache) = if which_active { (&self.active, &self.active_cache) } else { (&self.passive, &self.passive_cache) };
        let sess = match sess_opt.as_ref() { Some(s) => s, None => return None };

        if let Some(ps) = cache.lock().await.get(cql).cloned() { return Some(ps); }

        match sess.prepare(cql).await {
            Ok(ps) => {
                let arc_ps = Arc::new(ps);
                cache.lock().await.insert(cql.to_string(), arc_ps.clone());
                Some(arc_ps)
            }
            Err(_) => None,
        }
    }

    pub async fn ping_release_version_active(&self) -> bool { self.ping_release_version(true).await }
    pub async fn ping_release_version_passive(&self) -> bool { self.ping_release_version(false).await }

    async fn ping_release_version(&self, which_active: bool) -> bool {
        let cql = "SELECT release_version FROM system.local";
        if let Some(ps) = self.get_or_prepare(which_active, cql).await {
            let sess_opt = if which_active { self.active.as_ref() } else { self.passive.as_ref() };
            if let Some(sess) = sess_opt {
                return sess.execute_unpaged(&ps, &[]).await.is_ok();
            }
        }
        false
    }

    pub async fn upsert_watermark_active(&self, keyspace: &str, last_id: u64, now_ms: u64) -> bool {
        self.upsert_watermark(true, keyspace, last_id, now_ms).await
    }

    pub async fn upsert_watermark_passive(&self, keyspace: &str, last_id: u64, now_ms: u64) -> bool {
        self.upsert_watermark(false, keyspace, last_id, now_ms).await
    }

    async fn upsert_watermark(&self, which_active: bool, keyspace: &str, last_id: u64, now_ms: u64) -> bool {
        let cql = format!(
            "INSERT INTO {}.repl_watermark (id, last_applied_log_id, heartbeat_ms) VALUES (0, ?, ?)",
            keyspace
        );
        if let Some(ps) = self.get_or_prepare(which_active, &cql).await {
            let sess_opt = if which_active { self.active.as_ref() } else { self.passive.as_ref() };
            if let Some(sess) = sess_opt {
                let last_id_i64 = last_id as i64;
                let now_i64 = now_ms as i64;
                return sess.execute_unpaged(&ps, (last_id_i64, now_i64)).await.is_ok();
            }
        }
        false
    }
}

const DEFAULT_RETRIES: usize = 3;

pub async fn init_clients(cfg: &AppConfig) -> AppResult<DbClients> {
    let active = connect_with_retries(&cfg.active, &cfg.driver, DEFAULT_RETRIES).await.ok();
    let passive = connect_with_retries(&cfg.passive, &cfg.driver, DEFAULT_RETRIES).await.ok();
    if active.is_none() && passive.is_none() {
        return Err(AppError::db("failed to connect to both Active and Passive clusters"));
    }
    Ok(DbClients { active, passive, ..DbClients::default() })
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