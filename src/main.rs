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
    println!("nayud-batch: project structure initialized");
    let _cfg = config::AppConfig::default();
}