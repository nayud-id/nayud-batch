use ntex::web;

use std::sync::Arc;

use crate::db::DbClients;
use crate::health::{service_health, db_health};
use crate::middleware::CorrelationId;

#[derive(Clone)]
pub struct AppState {
    pub db_clients: Arc<DbClients>,
}

#[web::get("/health-check/service")]
async fn health_service() -> impl web::Responder {
    let response = service_health();
    web::HttpResponse::Ok().json(&response)
}

#[web::get("/health-check/databases")]
async fn health_databases(state: web::types::State<AppState>) -> impl web::Responder {
    let response = db_health(&*state.db_clients).await;
    web::HttpResponse::Ok().json(&response)
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(health_service)
       .service(health_databases);
}

pub async fn start_server(db_clients: DbClients, bind_addr: &str) -> std::io::Result<()> {
    let app_state = AppState {
        db_clients: Arc::new(db_clients),
    };

    web::HttpServer::new(move || {
        web::App::new()
            .wrap(web::middleware::Logger::new("%{X-Correlation-Id}o %a %t \"%r\" %s %b %T"))
            .wrap(CorrelationId::new())
            .state(app_state.clone())
            .configure(configure_routes)
    })
    .bind(bind_addr)?
    .run()
    .await
}