use crate::config::AppConfig;
use crate::errors::AppResult;

use scylla::client::session::Session;

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

pub async fn init_clients(_cfg: &AppConfig) -> AppResult<DbClients> {
    Ok(DbClients::default())
}