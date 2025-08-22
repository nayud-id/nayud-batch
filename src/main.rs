mod config;
mod db;
mod replication;
mod health;
mod errors;
mod types;
mod middleware;
mod utils;
mod web;

fn main() {
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
}