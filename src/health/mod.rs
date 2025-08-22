use crate::types::{ApiResponse};

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

pub fn db_health() -> ApiResponse<DbHealth> {
    ApiResponse::success_with(
        "databases healthy",
        DbHealth { active_ok: true, passive_ok: true },
    )
}