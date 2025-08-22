use core::future::Future;

use scylla::client::session::Session;
use scylla::statement::Consistency;
use scylla::statement::unprepared::Statement as UnpreparedStatement;
use scylla::value::Row;

use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::config::AppConfig;
use crate::db::DbClients;
use crate::errors::{AppError, AppResult};
use crate::health::{db_health, DbHealth};
use crate::types::ApiResponse;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Cluster {
    Active,
    Passive,
}

#[allow(async_fn_in_trait)]
pub trait SyncCheck: Send + Sync {
    async fn ready_to_switch(
        &self,
        clients: &DbClients,
        from: Cluster,
        to: Cluster,
    ) -> bool;
}

#[derive(Debug, Copy, Clone)]
pub enum OutboxTarget { Active, Passive, Both }

#[derive(Debug, Clone)]
pub struct OutboxRecord {
    pub idempotency_key: String,
    pub statement: String,
    pub params: Vec<Vec<u8>>,
    pub target: OutboxTarget,
    pub created_ms: u64,
}

impl OutboxRecord {
    pub fn new_simple(key: impl Into<String>, cql: impl Into<String>, target: OutboxTarget) -> Self {
        Self { idempotency_key: key.into(), statement: cql.into(), params: Vec::new(), target, created_ms: 0 }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(&self.created_ms.to_le_bytes());
        let t = match self.target { OutboxTarget::Active => 1u8, OutboxTarget::Passive => 2u8, OutboxTarget::Both => 3u8 };
        buf.push(t);
        let key_bytes = self.idempotency_key.as_bytes();
        let key_len = key_bytes.len() as u16;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key_bytes);
        let stmt_bytes = self.statement.as_bytes();
        let stmt_len = stmt_bytes.len() as u32;
        buf.extend_from_slice(&stmt_len.to_le_bytes());
        buf.extend_from_slice(stmt_bytes);
        let pcount = self.params.len() as u32;
        buf.extend_from_slice(&pcount.to_le_bytes());
        for p in &self.params {
            let len = p.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(p);
        }
        buf
    }

    fn decode(mut payload: &[u8]) -> Option<Self> {
        if payload.len() < 8 { return None; }
        let mut ms_arr = [0u8; 8]; ms_arr.copy_from_slice(&payload[..8]);
        let created_ms = u64::from_le_bytes(ms_arr);
        payload = &payload[8..];
        if payload.is_empty() { return None; }
        let t = payload[0]; payload = &payload[1..];
        let target = match t { 1 => OutboxTarget::Active, 2 => OutboxTarget::Passive, 3 => OutboxTarget::Both, _ => return None };
        if payload.len() < 2 { return None; }
        let mut klen_arr = [0u8; 2]; klen_arr.copy_from_slice(&payload[..2]);
        let klen = u16::from_le_bytes(klen_arr) as usize; payload = &payload[2..];
        if payload.len() < klen { return None; }
        let key = String::from_utf8(payload[..klen].to_vec()).ok()?; payload = &payload[klen..];
        if payload.len() < 4 { return None; }
        let mut slen_arr = [0u8; 4]; slen_arr.copy_from_slice(&payload[..4]);
        let slen = u32::from_le_bytes(slen_arr) as usize; payload = &payload[4..];
        if payload.len() < slen { return None; }
        let stmt = String::from_utf8(payload[..slen].to_vec()).ok()?; payload = &payload[slen..];
        if payload.len() < 4 { return None; }
        let mut pc_arr = [0u8; 4]; pc_arr.copy_from_slice(&payload[..4]);
        let pcount = u32::from_le_bytes(pc_arr) as usize; payload = &payload[4..];
        let mut params = Vec::with_capacity(pcount);
        for _ in 0..pcount {
            if payload.len() < 4 { return None; }
            let mut len_arr = [0u8; 4]; len_arr.copy_from_slice(&payload[..4]);
            let len = u32::from_le_bytes(len_arr) as usize; payload = &payload[4..];
            if payload.len() < len { return None; }
            params.push(payload[..len].to_vec());
            payload = &payload[len..];
        }
        Some(OutboxRecord { idempotency_key: key, statement: stmt, params, target, created_ms })
    }
}

const OB_MAGIC: u32 = 0x4E415944;
const OB_VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 4;

#[derive(Debug)]
pub struct Outbox {
    dir: PathBuf,
    log_path: PathBuf,
    cursor_path: PathBuf,
    file: File,
    fsync: bool,
}

impl Outbox {
    pub fn open<P: AsRef<Path>>(dir: P) -> AppResult<Self> {
        let dir_path = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir_path).map_err(|e| AppError::other(format!("outbox create dir: {}", e)))?;
        let log_path = dir_path.join("outbox.log");
        let cursor_path = dir_path.join("outbox.cursor");
        let file = OpenOptions::new().create(true).append(true).open(&log_path)
            .map_err(|e| AppError::other(format!("outbox open log: {}", e)))?;
        if !cursor_path.exists() {
            std::fs::write(&cursor_path, 0u64.to_le_bytes())
                .map_err(|e| AppError::other(format!("outbox init cursor: {}", e)))?;
        }
        Ok(Outbox { dir: dir_path, log_path, cursor_path, file, fsync: true })
    }

    pub fn with_fsync(mut self, fsync: bool) -> Self { self.fsync = fsync; self }

    pub fn append(&mut self, mut rec: OutboxRecord) -> AppResult<u64> {
        if rec.created_ms == 0 {
            rec.created_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        }
        let payload = rec.encode();
        let payload_len = payload.len() as u32;
        let header_len = HEADER_LEN as u64;
        let mut end_offset = std::fs::metadata(&self.log_path).map(|m| m.len()).unwrap_or(0);
        self.file.write_all(&OB_MAGIC.to_le_bytes())
            .and_then(|_| self.file.write_all(&OB_VERSION.to_le_bytes()))
            .and_then(|_| self.file.write_all(&payload_len.to_le_bytes()))
            .and_then(|_| self.file.write_all(&payload))
            .map_err(|e| AppError::other(format!("outbox append: {}", e)))?;
        if self.fsync {
            self.file.sync_data().ok();
        }
        end_offset += header_len + payload_len as u64;
        Ok(end_offset)
    }

    pub fn load_cursor(&self) -> AppResult<u64> {
        let mut buf = [0u8; 8];
        let mut f = File::open(&self.cursor_path).map_err(|e| AppError::other(format!("cursor open: {}", e)))?;
        f.read_exact(&mut buf).map_err(|e| AppError::other(format!("cursor read: {}", e)))?;
        Ok(u64::from_le_bytes(buf))
    }

    pub fn end_offset(&self) -> AppResult<u64> {
        std::fs::metadata(&self.log_path)
            .map(|m| m.len())
            .map_err(|e| AppError::other(format!("outbox metadata: {}", e)))
    }

    pub fn current_cursor(&self) -> AppResult<u64> { self.load_cursor() }

    pub fn store_cursor(&self, offset: u64) -> AppResult<()> {
        std::fs::write(&self.cursor_path, offset.to_le_bytes()).map_err(|e| AppError::other(format!("cursor write: {}", e)))
    }

    pub fn read_from(&self, mut offset: u64, max: usize) -> AppResult<Vec<(u64, u64, OutboxRecord)>> {
        let mut f = OpenOptions::new().read(true).open(&self.log_path)
            .map_err(|e| AppError::other(format!("outbox read open: {}", e)))?;
        f.seek(SeekFrom::Start(offset)).ok();
        let mut out = Vec::new();
        for _ in 0..max {
            let mut hbuf = [0u8; HEADER_LEN];
            match f.read_exact(&mut hbuf) {
                Ok(()) => {}
                Err(e) => {
                    let _ = e; break;
                }
            }
            let magic = u32::from_le_bytes([hbuf[0], hbuf[1], hbuf[2], hbuf[3]]);
            let version = u16::from_le_bytes([hbuf[4], hbuf[5]]);
            let plen = u32::from_le_bytes([hbuf[6], hbuf[7], hbuf[8], hbuf[9]]) as usize;
            if magic != OB_MAGIC || version != OB_VERSION { break; }
            let mut payload = vec![0u8; plen];
            if let Err(_) = f.read_exact(&mut payload) { break; }
            let rec = match OutboxRecord::decode(&payload) { Some(r) => r, None => break };
            let start = offset;
            offset = offset + HEADER_LEN as u64 + plen as u64;
            out.push((start, offset, rec));
        }
        Ok(out)
    }

    pub fn pending_count(&self) -> AppResult<usize> {
        let mut f = OpenOptions::new().read(true).open(&self.log_path)
            .map_err(|e| AppError::other(format!("outbox read open: {}", e)))?;
        let mut offset = self.load_cursor()?;
        f.seek(SeekFrom::Start(offset)).ok();
        let mut count = 0usize;
        loop {
            let mut hbuf = [0u8; HEADER_LEN];
            if f.read_exact(&mut hbuf).is_err() { break; }
            let magic = u32::from_le_bytes([hbuf[0], hbuf[1], hbuf[2], hbuf[3]]);
            let version = u16::from_le_bytes([hbuf[4], hbuf[5]]);
            let plen = u32::from_le_bytes([hbuf[6], hbuf[7], hbuf[8], hbuf[9]]) as usize;
            if magic != OB_MAGIC || version != OB_VERSION { break; }
            if f.seek(SeekFrom::Current(plen as i64)).is_err() { break; }
            offset += HEADER_LEN as u64 + plen as u64;
            count += 1;
        }
        Ok(count)
    }
}

#[derive(Debug, Default)]
pub struct DefaultSyncCheck {
    pub active_keyspace: Option<String>,
    pub passive_keyspace: Option<String>,
}

impl DefaultSyncCheck {
    pub fn with_keyspaces(active: impl Into<String>, passive: impl Into<String>) -> Self {
        Self { active_keyspace: Some(active.into()), passive_keyspace: Some(passive.into()) }
    }

    async fn ping(session: &Session) -> bool {
        session
            .query_unpaged("SELECT release_version FROM system.local", &[])
            .await
            .is_ok()
    }
}

impl SyncCheck for DefaultSyncCheck {
    async fn ready_to_switch(&self, clients: &DbClients, from: Cluster, to: Cluster) -> bool {
        match (from, to) {
            (Cluster::Active, Cluster::Active) | (Cluster::Passive, Cluster::Passive) => true,
            (Cluster::Active, Cluster::Passive) => {
                if let Some(sess) = clients.passive.as_ref() {
                    Self::ping(sess).await
                } else {
                    false
                }
            }
            (Cluster::Passive, Cluster::Active) => {
                if let Some(sess) = clients.active.as_ref() {
                    Self::ping(sess).await
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct FailoverState {
    primary: Cluster,
    last_active_ok: bool,
    last_passive_ok: bool,
    consecutive_active_fail: u32,
    consecutive_active_success: u32,
    consecutive_passive_success: u32,
    pending: Option<Cluster>,
    last_switch: Option<Instant>,
}

impl Default for FailoverState {
    fn default() -> Self {
        Self {
            primary: Cluster::Active,
            last_active_ok: false,
            last_passive_ok: false,
            consecutive_active_fail: 0,
            consecutive_active_success: 0,
            consecutive_passive_success: 0,
            pending: None,
            last_switch: None,
        }
    }
}

impl FailoverState {
    const FAIL_THRESHOLD: u32 = 3;
    const RECOVER_THRESHOLD: u32 = 5;

    fn update_with(&mut self, active_ok: bool, passive_ok: bool) {
        self.last_active_ok = active_ok;
        self.last_passive_ok = passive_ok;

        if active_ok {
            self.consecutive_active_success = self.consecutive_active_success.saturating_add(1);
            self.consecutive_active_fail = 0;
        } else {
            self.consecutive_active_fail = self.consecutive_active_fail.saturating_add(1);
            self.consecutive_active_success = 0;
        }

        if passive_ok {
            self.consecutive_passive_success = self.consecutive_passive_success.saturating_add(1);
        } else {
            self.consecutive_passive_success = 0;
        }

        self.pending = match self.primary {
            Cluster::Active => {
                if !active_ok && self.consecutive_active_fail >= Self::FAIL_THRESHOLD && passive_ok {
                    Some(Cluster::Passive)
                } else {
                    None
                }
            }
            Cluster::Passive => {
                if active_ok && self.consecutive_active_success >= Self::RECOVER_THRESHOLD {
                    Some(Cluster::Active)
                } else {
                    None
                }
            }
        };
    }

    fn commit_switch(&mut self, to: Cluster) {
        self.primary = to;
        self.last_switch = Some(Instant::now());
        self.consecutive_active_fail = 0;
        self.consecutive_active_success = 0;
        self.consecutive_passive_success = 0;
        self.pending = None;
    }

    fn pending(&self) -> Option<Cluster> { self.pending }
}

#[derive(Debug)]
pub struct FailoverManager {
    state: FailoverState,
    sync: DefaultSyncCheck,
    force_ready: bool,
}

impl Default for FailoverManager {
    fn default() -> Self {
        Self { state: FailoverState::default(), sync: DefaultSyncCheck::default(), force_ready: false }
    }
}

impl FailoverManager {
    pub fn new() -> Self { Self::default() }

    pub fn new_with_config(cfg: &AppConfig) -> Self {
        let mut checker = DefaultSyncCheck::default();
        checker.active_keyspace = Some(cfg.active.keyspace.clone());
        checker.passive_keyspace = Some(cfg.passive.keyspace.clone());
        Self { state: FailoverState::default(), sync: checker, force_ready: false }
    }

    pub fn with_force_ready(mut self, v: bool) -> Self { self.force_ready = v; self }

    pub fn current_primary(&self) -> Cluster { self.state.primary }

    pub fn last_switch(&self) -> Option<Instant> { self.state.last_switch }

    pub fn last_status(&self) -> (bool, bool) { (self.state.last_active_ok, self.state.last_passive_ok) }

    async fn maybe_switch(&mut self, clients: &DbClients) {
        if let Some(to) = self.state.pending() {
            let from = self.state.primary;
            if self.force_ready || self.sync.ready_to_switch(clients, from, to).await {
                self.state.commit_switch(to);
            }
        }
    }

    pub async fn tick(&mut self, clients: &DbClients) -> ApiResponse<DbHealth> {
        let resp = db_health(clients).await;
        let (a_ok, p_ok) = match &resp.data {
            Some(d) => (d.active_ok, d.passive_ok),
            None => (false, false),
        };
        self.state.update_with(a_ok, p_ok);

        self.maybe_switch(clients).await;

        resp
    }

    pub async fn tick_with_status(&mut self, clients: &DbClients, a_ok: bool, p_ok: bool) -> ApiResponse<DbHealth> {
        let resp = ApiResponse::success_with("databases healthy", DbHealth { active_ok: a_ok, passive_ok: p_ok });
        self.state.update_with(a_ok, p_ok);
        self.maybe_switch(clients).await;
        resp
    }
}

#[derive(Debug, Default)]
pub struct ReplicationManager {
    outbox: Option<Outbox>,
    active_keyspace: Option<String>,
    passive_keyspace: Option<String>,
}

impl ReplicationManager {
    pub fn new() -> Self { Self { outbox: None, active_keyspace: None, passive_keyspace: None } }

    pub fn with_outbox_dir<P: AsRef<Path>>(dir: P) -> AppResult<Self> {
        let ob = Outbox::open(dir)?;
        Ok(Self { outbox: Some(ob), active_keyspace: None, passive_keyspace: None })
    }

    pub fn with_keyspaces(mut self, active: impl Into<String>, passive: impl Into<String>) -> Self {
        self.active_keyspace = Some(active.into());
        self.passive_keyspace = Some(passive.into());
        self
    }

    pub fn has_outbox(&self) -> bool { self.outbox.is_some() }

    pub fn queue_len(&self) -> usize {
        match &self.outbox {
            Some(ob) => ob.pending_count().unwrap_or(0),
            None => 0,
        }
    }

    pub fn enqueue(&mut self, rec: OutboxRecord) -> AppResult<u64> {
        match &mut self.outbox {
            Some(ob) => ob.append(rec),
            None => Err(AppError::other("outbox not configured")),
        }
    }

    pub fn current_cursor(&self) -> AppResult<Option<u64>> {
        match &self.outbox {
            Some(ob) => Ok(Some(ob.current_cursor()?)),
            None => Ok(None),
        }
    }

    async fn write_watermark_for(&self, sess_opt: Option<&Session>, keyspace_opt: &Option<String>, last_id: u64) -> bool {
        if let (Some(sess), Some(ks)) = (sess_opt, keyspace_opt.as_ref()) {
            let create = format!(
                "CREATE TABLE IF NOT EXISTS {}.repl_watermark (id tinyint PRIMARY KEY, last_applied_log_id bigint, heartbeat_ms bigint)",
                ks
            );
            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            let upsert = format!(
                "INSERT INTO {}.repl_watermark (id, last_applied_log_id, heartbeat_ms) VALUES (0, {}, {})",
                ks, last_id, now_ms
            );
            let c1 = Self::exec_unpaged_session(Some(sess), &create, Consistency::One).await;
            let c2 = Self::exec_unpaged_session(Some(sess), &upsert, Consistency::One).await;
            c1 && c2
        } else {
            false
        }
    }

    pub async fn write_watermark_cluster(&self, cluster: Cluster, last_id: u64, clients: &DbClients) -> bool {
        match cluster {
            Cluster::Active => self.write_watermark_for(clients.active.as_ref(), &self.active_keyspace, last_id).await,
            Cluster::Passive => self.write_watermark_for(clients.passive.as_ref(), &self.passive_keyspace, last_id).await,
        }
    }

    pub fn drift_status(&self, rec_threshold: usize, bytes_threshold: u64) -> AppResult<Option<DriftStatus>> {
        match &self.outbox {
            Some(ob) => {
                let cursor = ob.current_cursor()?;
                let end = ob.end_offset()?;
                let pending_records = ob.pending_count()?;
                let pending_bytes = end.saturating_sub(cursor);
                let healthy = pending_records <= rec_threshold && pending_bytes <= bytes_threshold;
                Ok(Some(DriftStatus { pending_records, pending_bytes, cursor, end, healthy }))
            }
            None => Ok(None),
        }
    }

    pub async fn replay_with<F, Fut>(&mut self, max: usize, mut apply: F) -> AppResult<usize>
    where
        F: FnMut(OutboxRecord) -> Fut,
        Fut: Future<Output = bool>,
    {
        let Some(ob) = self.outbox.as_mut() else { return Ok(0) };
        let cursor = ob.load_cursor()?;
        let batch = ob.read_from(cursor, max)?;
        let mut processed = 0usize;
        for (start, end, rec) in batch {
            let _ = start;
            if apply(rec).await {
                ob.store_cursor(end)?;
                processed += 1;
            } else {
                break;
            }
        }
        Ok(processed)
    }

    pub async fn replay_and_mark(&mut self, max: usize, clients: &DbClients) -> AppResult<usize> {
        let mut processed = 0usize;
        let mut marks: Vec<(Cluster, u64)> = Vec::new();
        {
            let Some(ob) = self.outbox.as_mut() else { return Ok(0) };
            let cursor = ob.load_cursor()?;
            let batch = ob.read_from(cursor, max)?;
            for (_start, end, rec) in batch {
                let applied_ok = match rec.target {
                    OutboxTarget::Active => {
                        Self::exec_unpaged_session(clients.active.as_ref(), rec.statement.as_str(), Consistency::LocalQuorum).await
                    }
                    OutboxTarget::Passive => {
                        Self::exec_unpaged_session(clients.passive.as_ref(), rec.statement.as_str(), Consistency::One).await
                    }
                    OutboxTarget::Both => {
                        let a = Self::exec_unpaged_session(clients.active.as_ref(), rec.statement.as_str(), Consistency::LocalQuorum).await;
                        let b = Self::exec_unpaged_session(clients.passive.as_ref(), rec.statement.as_str(), Consistency::One).await;
                        a && b
                    }
                };

                if applied_ok {
                    ob.store_cursor(end)?;
                    match rec.target {
                        OutboxTarget::Active => marks.push((Cluster::Active, end)),
                        OutboxTarget::Passive => marks.push((Cluster::Passive, end)),
                        OutboxTarget::Both => {
                            marks.push((Cluster::Active, end));
                            marks.push((Cluster::Passive, end));
                        }
                    }
                    processed += 1;
                } else {
                    break;
                }
            }
        }

        for (cl, id) in marks {
            let _ = self.write_watermark_cluster(cl, id, clients).await;
        }

        Ok(processed)
    }


    pub fn tick(&mut self) {}

    fn build_statement(cql: &str, consistency: Consistency) -> UnpreparedStatement {
        let mut st = UnpreparedStatement::new(cql);
        st.set_consistency(consistency);
        st.set_is_idempotent(true);
        st
    }

    async fn exec_unpaged_session(sess_opt: Option<&Session>, cql: &str, consistency: Consistency) -> bool {
        if let Some(sess) = sess_opt {
            let st = Self::build_statement(cql, consistency);
            sess.query_unpaged(st, &[]).await.is_ok()
        } else {
            false
        }
    }

    async fn try_read_rows(sess_opt: Option<&Session>, cql: &str, consistency: Consistency) -> Option<Vec<Row>> {
        if let Some(sess) = sess_opt {
            let st = Self::build_statement(cql, consistency);
            if let Ok(qr) = sess.query_unpaged(st, &[]).await {
                if let Ok(rows_res) = qr.into_rows_result() {
                    if let Ok(mut iter) = rows_res.rows::<Row>() {
                        let mut rows_out = Vec::new();
                        while let Some(item) = iter.next() {
                            match item {
                                Ok(row) => rows_out.push(row),
                                Err(_) => { break; }
                            }
                        }
                        return Some(rows_out);
                    }
                }
            }
        }
        None
    }

    pub async fn write_simple(
        &mut self,
        idempotency_key: impl Into<String>,
        cql: impl Into<String>,
        target: OutboxTarget,
        consistency: Option<Consistency>,
        clients: &DbClients,
    ) -> AppResult<bool> {
        let key = idempotency_key.into();
        let cql = cql.into();
        let mut any_ok = false;

        match target {
            OutboxTarget::Active => {
                let cl = consistency.unwrap_or(Consistency::LocalQuorum);
                let ok = Self::exec_unpaged_session(clients.active.as_ref(), &cql, cl).await;
                if !ok {
                    let _ = self.enqueue(OutboxRecord::new_simple(key.clone(), cql.clone(), OutboxTarget::Active));
                }
                any_ok |= ok;
            }
            OutboxTarget::Passive => {
                let cl = consistency.unwrap_or(Consistency::One);
                let ok = Self::exec_unpaged_session(clients.passive.as_ref(), &cql, cl).await;
                if !ok {
                    let _ = self.enqueue(OutboxRecord::new_simple(key.clone(), cql.clone(), OutboxTarget::Passive));
                }
                any_ok |= ok;
            }
            OutboxTarget::Both => {
                let cl_a = consistency.unwrap_or(Consistency::LocalQuorum);
                let ok_a = Self::exec_unpaged_session(clients.active.as_ref(), &cql, cl_a).await;
                if !ok_a {
                    let _ = self.enqueue(OutboxRecord::new_simple(key.clone(), cql.clone(), OutboxTarget::Active));
                }
                any_ok |= ok_a;

                let cl_p = consistency.unwrap_or(Consistency::One);
                let ok_p = Self::exec_unpaged_session(clients.passive.as_ref(), &cql, cl_p).await;
                if !ok_p {
                    let _ = self.enqueue(OutboxRecord::new_simple(key.clone(), cql.clone(), OutboxTarget::Passive));
                }
                any_ok |= ok_p;
            }
        }

        Ok(any_ok)
    }

    pub async fn read_simple(
        &self,
        cql: impl Into<String>,
        consistency: Option<Consistency>,
        clients: &DbClients,
    ) -> AppResult<Option<(Cluster, Vec<Row>)>> {
        let cql = cql.into();

        let cl_a = consistency.unwrap_or(Consistency::LocalQuorum);
        if let Some(rows_out) = Self::try_read_rows(clients.active.as_ref(), &cql, cl_a).await {
            return Ok(Some((Cluster::Active, rows_out)));
        }

        let cl_p = consistency.unwrap_or(Consistency::One);
        if let Some(rows_out) = Self::try_read_rows(clients.passive.as_ref(), &cql, cl_p).await {
            return Ok(Some((Cluster::Passive, rows_out)));
        }

        Ok(None)
    }

    pub async fn replay_simple(&mut self, max: usize, clients: &DbClients) -> AppResult<usize> {
        self.replay_and_mark(max, clients).await
    }
}

#[derive(Debug)]
pub struct DriftStatus {
    pub pending_records: usize,
    pub pending_bytes: u64,
    pub cursor: u64,
    pub end: u64,
    pub healthy: bool,
}

#[derive(Debug)]
pub struct SyncWorker {
    repl: ReplicationManager,
    failover: FailoverManager,
    interval_ms: u64,
    max_replay_per_tick: usize,
    drift_rec_threshold: usize,
    drift_bytes_threshold: u64,
    last_drift: Option<DriftStatus>,
}

impl SyncWorker {
    pub fn new() -> Self {
        Self {
            repl: ReplicationManager::new(),
            failover: FailoverManager::new(),
            interval_ms: 1000,
            max_replay_per_tick: 128,
            drift_rec_threshold: 100,
            drift_bytes_threshold: 1_000_000,
            last_drift: None,
        }
    }

    pub fn with_interval_ms(mut self, ms: u64) -> Self { self.interval_ms = ms; self }

    pub fn with_max_replay_per_tick(mut self, max: usize) -> Self { self.max_replay_per_tick = max; self }

    pub fn with_outbox_dir<P: AsRef<Path>>(mut self, dir: P) -> AppResult<Self> {
        self.repl = ReplicationManager::with_outbox_dir(dir)?;
        Ok(self)
    }

    pub fn with_drift_thresholds(mut self, rec_threshold: usize, bytes_threshold: u64) -> Self {
        self.drift_rec_threshold = rec_threshold;
        self.drift_bytes_threshold = bytes_threshold;
        self
    }

    pub fn with_keyspaces(mut self, active: impl Into<String>, passive: impl Into<String>) -> Self {
        self.repl = std::mem::take(&mut self.repl).with_keyspaces(active, passive);
        self
    }

    pub fn queue_len(&self) -> usize { self.repl.queue_len() }

    pub fn has_outbox(&self) -> bool { self.repl.has_outbox() }

    pub async fn run_once(&mut self, clients: &DbClients) -> AppResult<(ApiResponse<DbHealth>, usize)> {
        let health = self.failover.tick(clients).await;
        let mut processed = 0usize;
        if self.repl.has_outbox() && self.max_replay_per_tick > 0 {
            let to_drain = self.max_replay_per_tick.min(self.repl.queue_len());
            if to_drain > 0 {
                processed = self.repl.replay_and_mark(to_drain, clients).await?;
            }
        }

        if self.repl.has_outbox() {
            if let Ok(Some(cur)) = self.repl.current_cursor() {
                let _ = self.repl.write_watermark_cluster(Cluster::Passive, cur, clients).await;
            }
        }

        if let Some(ds) = self.repl.drift_status(self.drift_rec_threshold, self.drift_bytes_threshold)? {
            let unhealthy = !ds.healthy;
            if unhealthy {
                println!(
                    "drift warning: pending_records={} pending_bytes={} cursor={} end={}",
                    ds.pending_records, ds.pending_bytes, ds.cursor, ds.end
                );
            }
            self.last_drift = Some(ds);
        }

        Ok((health, processed))
    }

    pub async fn run_loop(&mut self, clients: &DbClients) {
        loop {
            match self.run_once(clients).await {
                Ok((_health, _processed)) => {}
                Err(e) => {
                    println!("sync worker error: {}", e.to_message());
                }
            }
            ntex::time::sleep(Duration::from_millis(self.interval_ms)).await;
        }
    }
}