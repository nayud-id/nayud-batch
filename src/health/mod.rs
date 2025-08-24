use serde::Serialize;

use crate::types::ApiResponse;
use crate::types::response::{ApiMessage, CODE_FAILURE};
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

    match (active_ok, passive_ok) {
        (true, true) => ApiResponse::success_with("databases healthy", data),
        (false, true) => {
            let what = "Active database is unavailable or unhealthy".to_string();
            let why = "The application could not complete a basic health query against the Active cluster (SELECT release_version FROM system.local) or the request failed.".to_string();
            let how = "Ensure the Active database is running and reachable from this service. Verify host/port connectivity (firewall, security groups), credentials, and TLS settings if enabled. Check database logs for errors, then retry.".to_string();
            ApiResponse { code: CODE_FAILURE, message: ApiMessage::Detail { what, why, how }, data: Some(data) }
        }
        (true, false) => {
            let what = "Passive database is unavailable or unhealthy".to_string();
            let why = "The application could not complete a basic health query against the Passive cluster (SELECT release_version FROM system.local) or the request failed.".to_string();
            let how = "Ensure the Passive database is running and reachable from this service. Verify host/port connectivity (firewall, security groups), credentials, and TLS settings if enabled. Check database logs for errors, then retry.".to_string();
            ApiResponse { code: CODE_FAILURE, message: ApiMessage::Detail { what, why, how }, data: Some(data) }
        }
        (false, false) => {
            let what = "Both Active and Passive databases are unavailable".to_string();
            let why = "The service failed to complete a health query against either cluster. This often points to connectivity issues, incorrect credentials/TLS configuration, or the clusters being down.".to_string();
            let how = "1) Confirm both clusters are up and accepting connections. 2) From this host, verify DNS and that the database ports are reachable. 3) Validate credentials and TLS configuration (CA file, certificate validity, insecure skip verify). 4) Review database and network logs for errors. 5) Retry after addressing the root cause.".to_string();
            ApiResponse { code: CODE_FAILURE, message: ApiMessage::Detail { what, why, how }, data: Some(data) }
        }
    }
}