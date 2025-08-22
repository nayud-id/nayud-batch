use std::time::Instant;

use crate::db::DbClients;
use crate::health::{db_health, DbHealth};
use crate::types::ApiResponse;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Cluster {
    Active,
    Passive,
}

#[derive(Debug, Clone)]
struct FailoverState {
    primary: Cluster,
    last_active_ok: bool,
    last_passive_ok: bool,
    consecutive_active_fail: u32,
    consecutive_active_success: u32,
    consecutive_passive_success: u32,
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

        match self.primary {
            Cluster::Active => {
                if !active_ok
                    && self.consecutive_active_fail >= Self::FAIL_THRESHOLD
                    && passive_ok
                {
                    self.primary = Cluster::Passive;
                    self.last_switch = Some(Instant::now());
                    self.consecutive_active_fail = 0;
                    self.consecutive_active_success = 0;
                    self.consecutive_passive_success = 0;
                }
            }
            Cluster::Passive => {
                if active_ok && self.consecutive_active_success >= Self::RECOVER_THRESHOLD {
                    self.primary = Cluster::Active;
                    self.last_switch = Some(Instant::now());
                    self.consecutive_active_fail = 0;
                    self.consecutive_active_success = 0;
                    self.consecutive_passive_success = 0;
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct FailoverManager {
    state: FailoverState,
}

impl FailoverManager {
    pub fn new() -> Self { Self { state: FailoverState::default() } }

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