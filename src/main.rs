use log::{info, warn};

use ntex::rt::System;

mod config;
mod db;
mod replication;
mod health;
mod errors;
mod types;
mod middleware;
mod utils;
mod web;

#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};

#[ntex::main]
async fn main() -> std::io::Result<()> {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init();

    info!("nayud-batch: initializing configuration");
    let cfg = config::AppConfig::from_file_or_env();

    let masked_user = utils::mask_secret(&cfg.active.username);
    let masked_pass = utils::mask_secret(&cfg.active.password);

    info!(
        "Active DB: {}:{} keyspace={} dc={} rack={} user={} pass={}",
        cfg.active.host, cfg.active.port, cfg.active.keyspace, cfg.active.datacenter, cfg.active.rack,
        masked_user, masked_pass
    );

    let masked_user_p = utils::mask_secret(&cfg.passive.username);
    let masked_pass_p = utils::mask_secret(&cfg.passive.password);

    info!(
        "Passive DB: {}:{} keyspace={} dc={} rack={} user={} pass={}",
        cfg.passive.host, cfg.passive.port, cfg.passive.keyspace, cfg.passive.datacenter, cfg.passive.rack,
        masked_user_p, masked_pass_p
    );

    let clients = db::init_clients(&cfg).await.unwrap_or_else(|e| {
        warn!("Database init error: {}. Continuing with empty clients.", e.to_message());
        db::DbClients::default()
    });

    ntex::rt::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("Shutdown signal received (Ctrl+C). Stopping system gracefully...");
            System::current().stop();
        }
    });

    #[cfg(unix)]
    ntex::rt::spawn(async move {
        if let Ok(mut term) = unix_signal(SignalKind::terminate()) {
            term.recv().await;
            info!("Shutdown signal received (SIGTERM). Stopping system gracefully...");
            System::current().stop();
        }
    });

    let bind_addr = cfg.server.bind_addr.clone();
    info!("Starting HTTP server on {bind_addr}");
    web::start_server(clients, &bind_addr).await
}