use openssl::ssl::{SslContextBuilder, SslMethod, SslVerifyMode};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement as UnpreparedStatement;
use scylla::statement::Consistency;
use scylla::value::Row;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::config::{AppConfig, DbEndpoint, DriverConfig};
use crate::errors::{AppError, AppResult};

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

    pub async fn ping_release_version_active(&self) -> bool {
        self.ping_release_version(true).await
    }

    pub async fn ping_release_version_passive(&self) -> bool {
        self.ping_release_version(false).await
    }

    async fn ping_release_version(&self, which_active: bool) -> bool {
        let sess_opt = if which_active { self.active.as_ref() } else { self.passive.as_ref() };
        let Some(sess) = sess_opt else { return false };
        let mut st = UnpreparedStatement::new("SELECT release_version FROM system.local");
        st.set_consistency(if which_active { Consistency::LocalQuorum } else { Consistency::One });
        st.set_is_idempotent(true);
        match sess.query_unpaged(st, &[]).await {
            Ok(qr) => {
                if let Ok(rows_res) = qr.into_rows_result() {
                    if let Ok(mut iter) = rows_res.rows::<Row>() {
                        if let Some(item) = iter.next() {
                            return item.is_ok();
                        }
                    }
                }
                false
            }
            Err(_) => false,
        }
    }

    pub async fn upsert_watermark_active(&self, keyspace: &str, last_id: u64, now_ms: u64) -> bool {
        self.upsert_watermark(true, keyspace, last_id, now_ms).await
    }

    pub async fn upsert_watermark_passive(&self, keyspace: &str, last_id: u64, now_ms: u64) -> bool {
        self.upsert_watermark(false, keyspace, last_id, now_ms).await
    }

    async fn upsert_watermark(&self, which_active: bool, keyspace: &str, last_id: u64, now_ms: u64) -> bool {
        let sess_opt = if which_active { self.active.as_ref() } else { self.passive.as_ref() };
        let Some(sess) = sess_opt else { return false };
        let qks = quote_ident(keyspace);
        let cql = format!(
            "INSERT INTO {}.repl_watermark (id, last_applied_log_id, heartbeat_ms) VALUES (1, {}, {})",
            qks, last_id, now_ms
        );
        let mut st = UnpreparedStatement::new(&cql);
        st.set_consistency(if which_active { Consistency::LocalQuorum } else { Consistency::One });
        st.set_is_idempotent(true);
        sess.query_unpaged(st, &[]).await.is_ok()
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

pub async fn ensure_keyspaces(cfg: &AppConfig, clients: &DbClients) -> AppResult<()> {
    const ENSURE_KS_CQL_TMPL: &str = include_str!("../../cql/keyspace.cql");

    let mut errors: Vec<String> = Vec::new();

    let (res_active, res_passive) = tokio::join!(
        ensure_keyspace_for_cluster(true, &cfg.active, clients, ENSURE_KS_CQL_TMPL),
        ensure_keyspace_for_cluster(false, &cfg.passive, clients, ENSURE_KS_CQL_TMPL),
    );

    if let Err(e) = res_active { errors.push(format!("Active: {}", e.to_message())); }
    if let Err(e) = res_passive { errors.push(format!("Passive: {}", e.to_message())); }

    if errors.is_empty() { Ok(()) } else { Err(AppError::db(format!("ensure_keyspaces failed: {}", errors.join(" | ")))) }
}

async fn ensure_keyspace_for_cluster(
    which_active: bool,
    ep: &DbEndpoint,
    clients: &DbClients,
    tmpl: &str,
) -> AppResult<()> {
    let (sess_opt, label) = if which_active { (&clients.active, "Active") } else { (&clients.passive, "Passive") };
    let sess = match sess_opt.as_ref() {
        Some(s) => s,
        None => {
            return Err(AppError::db(format!(
                "{} database is unavailable while ensuring keyspace '{}'.",
                label, ep.keyspace
            )));
        }
    };

    let cql_check = "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?";
    if let Some(ps) = clients.get_or_prepare(which_active, cql_check).await {
        match sess.execute_unpaged(&ps, (&ep.keyspace,)).await {
            Ok(qr) => {
                if let Ok(rows_res) = qr.into_rows_result() {
                    if let Ok(mut iter) = rows_res.rows::<Row>() {
                        if let Some(item) = iter.next() {
                            if item.is_ok() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(AppError::db(format!(
                    "{}: failed to check keyspace existence for '{}': {}",
                    label, ep.keyspace, e
                )));
            }
        }
    } else {
        return Err(AppError::db(format!(
            "{}: failed to prepare keyspace existence check statement.",
            label
        )));
    }

    let rf = ep.replication_factor.unwrap_or(1);
    let durable = ep.durable_writes.unwrap_or(true);
    let cql = tmpl
        .replace("{{KEYSPACE}}", &quote_ident(&ep.keyspace))
        .replace("{{DATACENTER}}", &ep.datacenter)
        .replace("{{RF}}", &rf.to_string())
        .replace("{{DURABLE_WRITES}}", if durable { "true" } else { "false" });

    let mut st = UnpreparedStatement::new(&cql);
    st.set_consistency(Consistency::Quorum);
    st.set_is_idempotent(true);

    match sess.query_unpaged(st, &[]).await {
        Ok(_) => Ok(()),
        Err(e) => Err(AppError::db(format!(
            "{}: failed to create keyspace '{}': {}",
            label, ep.keyspace, e
        ))),
    }
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

    if ep.use_tls {
        match SslContextBuilder::new(SslMethod::tls()) {
            Ok(mut ctx_builder) => {
                if ep.tls_insecure_skip_verify {
                    ctx_builder.set_verify(SslVerifyMode::NONE);
                } else {
                    ctx_builder.set_verify(SslVerifyMode::PEER);
                }
                if let Some(ca_path) = ep.tls_ca_file.as_ref() {
                    let _ = ctx_builder.set_ca_file(ca_path);
                }
                let ctx = ctx_builder.build();
                builder = builder.tls_context(Some(ctx));
            }
            Err(e) => {
                return Err(AppError::db(format!("failed to initialize TLS context: {}", e)));
            }
        }
    }

    let session = builder
        .build()
        .await
        .map_err(|e| AppError::db(format!("connect error: {}", e)))?;
    Ok(session)
}


pub(crate) fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}