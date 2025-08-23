use serde::Serialize;

use crate::types::ApiResponse;
use crate::db::DbClients;

#[derive(Debug, Serialize)]
pub struct ServiceHealth { pub ok: bool }

#[derive(Debug, Serialize)]
pub struct DbHealth { pub active_ok: bool, pub passive_ok: bool }

pub fn service_health() -> ApiResponse<ServiceHealth> {
    ApiResponse::success_with("service healthy", ServiceHealth { ok: true })
}

pub async fn db_health(clients: &DbClients) -> ApiResponse<DbHealth> {
    let (active_ok, passive_ok) = tokio::join!(
        clients.ping_release_version_active(),
        clients.ping_release_version_passive()
    );

    let data = DbHealth { active_ok, passive_ok };
    if active_ok || passive_ok {
        ApiResponse::success_with("databases healthy", data)
    } else {
        ApiResponse::failure("databases unavailable")
    }
}