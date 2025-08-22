mod config;
mod db;
mod replication;
mod health;
mod errors;
mod types;
mod middleware;
mod utils;
mod web;

use ntex::rt::System;

#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};

#[ntex::main]
async fn main() -> std::io::Result<()> {
    println!("nayud-batch: initializing configuration");
    let cfg = config::AppConfig::from_env();
    println!(
        "Active DB: {}:{} keyspace={} dc={} rack={}",
        cfg.active.host, cfg.active.port, cfg.active.keyspace, cfg.active.datacenter, cfg.active.rack
    );
    println!(
        "Passive DB: {}:{} keyspace={} dc={} rack={}",
        cfg.passive.host, cfg.passive.port, cfg.passive.keyspace, cfg.passive.datacenter, cfg.passive.rack
    );

    let clients = db::init_clients(&cfg).await.unwrap_or_else(|e| {
        println!("Database init error: {}. Continuing with empty clients.", e.to_message());
        db::DbClients::default()
    });

    ntex::rt::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("Shutdown signal received (Ctrl+C). Stopping system gracefully...");
            System::current().stop();
        }
    });

    #[cfg(unix)]
    ntex::rt::spawn(async move {
        if let Ok(mut term) = unix_signal(SignalKind::terminate()) {
            term.recv().await;
            println!("Shutdown signal received (SIGTERM). Stopping system gracefully...");
            System::current().stop();
        }
    });

    let bind_addr = "127.0.0.1:8080";
    println!("Starting HTTP server on {bind_addr}");
    web::start_server(clients, bind_addr).await
}