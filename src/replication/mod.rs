use std::time::Instant;

use crate::config::AppConfig;
use crate::db::DbClients;
use crate::health::{db_health, DbHealth};
use crate::types::ApiResponse;
use scylla::client::session::Session;

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
}

impl Default for FailoverManager {
    fn default() -> Self {
        Self { state: FailoverState::default(), sync: DefaultSyncCheck::default() }
    }
}

impl FailoverManager {
    pub fn new() -> Self { Self::default() }

    pub fn new_with_config(cfg: &AppConfig) -> Self {
        let mut checker = DefaultSyncCheck::default();
        checker.active_keyspace = Some(cfg.active.keyspace.clone());
        checker.passive_keyspace = Some(cfg.passive.keyspace.clone());
        Self { state: FailoverState::default(), sync: checker }
    }

    pub fn current_primary(&self) -> Cluster { self.state.primary }

    pub fn last_switch(&self) -> Option<Instant> { self.state.last_switch }

    pub fn last_status(&self) -> (bool, bool) { (self.state.last_active_ok, self.state.last_passive_ok) }

    pub async fn tick(&mut self, clients: &DbClients) -> ApiResponse<DbHealth> {
        let resp = db_health(clients).await;
        let (a_ok, p_ok) = match &resp.data {
            Some(d) => (d.active_ok, d.passive_ok),
            None => (false, false),
        };
        self.state.update_with(a_ok, p_ok);

        if let Some(to) = self.state.pending() {
            let from = self.state.primary;
            if self.sync.ready_to_switch(clients, from, to).await {
                self.state.commit_switch(to);
            }
        }

        resp
    }
}

#[derive(Debug, Default)]
pub struct ReplicationManager;

impl ReplicationManager {
    pub fn new() -> Self {
        Self
    }

    pub fn is_active(&self) -> bool {
        false
    }

    pub fn queue_len(&self) -> usize {
        0
    }

    pub fn tick(&mut self) {}
}