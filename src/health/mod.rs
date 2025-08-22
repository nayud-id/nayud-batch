#[derive(Debug)]
pub struct ServiceHealth {
    pub ok: bool,
}

#[derive(Debug)]
pub struct DbHealth {
    pub active_ok: bool,
    pub passive_ok: bool,
}

pub fn service_health() -> ServiceHealth {
    ServiceHealth { ok: true }
}

pub fn db_health() -> DbHealth {
    DbHealth {
        active_ok: false,
        passive_ok: false,
    }
}