use crate::types::{ApiResponse};
use crate::db::DbClients;
use scylla::client::session::Session;

#[derive(Debug)]
pub struct ServiceHealth {
    pub ok: bool,
}

#[derive(Debug)]
pub struct DbHealth {
    pub active_ok: bool,
    pub passive_ok: bool,
}

pub fn service_health() -> ApiResponse<ServiceHealth> {
    ApiResponse::success_with("service healthy", ServiceHealth { ok: true })
}

async fn ping_session_opt(session: Option<&Session>) -> bool {
    match session {
        Some(sess) => sess.query_unpaged("SELECT release_version FROM system.local", &[]).await.is_ok(),
        None => false,
    }
}

pub async fn db_health(clients: &DbClients) -> ApiResponse<DbHealth> {
    let active_ok = ping_session_opt(clients.active.as_ref()).await;
    let passive_ok = ping_session_opt(clients.passive.as_ref()).await;

    let data = DbHealth { active_ok, passive_ok };
    if active_ok || passive_ok {
        ApiResponse::success_with("databases healthy", data)
    } else {
        ApiResponse::failure("databases unavailable")
    }
}