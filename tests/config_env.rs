use std::env;
use std::sync::{Mutex, OnceLock};

use nayud_batch::config::AppConfig;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn with_env_lock<F: FnOnce()>(f: F) {
    let m = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _g = m.lock().unwrap();
    f();
}

fn with_env_vars<T, F: FnOnce() -> T>(vars: &[&str], set: &[(&str, &str)], f: F) -> T {
    use std::collections::HashMap;
    let mut backup = HashMap::new();
    for &k in vars {
        backup.insert(k.to_string(), env::var(k).ok());
        unsafe { env::remove_var(k) };
    }
    for &(k, v) in set {
        unsafe { env::set_var(k, v) };
    }
    let out = f();
    for (k, vopt) in backup {
        match vopt {
            Some(v) => {
                unsafe { env::set_var(&k, v) };
            }
            None => {
                unsafe { env::remove_var(&k) };
            }
        }
    }
    out
}

#[test]
fn config_defaults_and_overrides() {
    with_env_lock(|| {
        let keys = [
            "ACTIVE_DB_HOST","ACTIVE_DB_PORT","ACTIVE_DB_KEYSPACE","ACTIVE_DB_DATACENTER","ACTIVE_DB_RACK","ACTIVE_DB_USERNAME","ACTIVE_DB_PASSWORD",
            "PASSIVE_DB_HOST","PASSIVE_DB_PORT","PASSIVE_DB_KEYSPACE","PASSIVE_DB_DATACENTER","PASSIVE_DB_RACK","PASSIVE_DB_USERNAME","PASSIVE_DB_PASSWORD",
            "DB_HOST","DB_PORT","DB_KEYSPACE","DB_DATACENTER","DB_RACK","DB_USERNAME","DB_PASSWORD",
        ];

        with_env_vars(&keys, &[], || {
            let cfg = AppConfig::from_env();
            assert_eq!(cfg.active.host, "127.0.0.1");
            assert_eq!(cfg.passive.port, 9043);
            assert_eq!(cfg.active.username, "cassandra");
        });

        with_env_vars(&keys, &[("DB_HOST","globalhost"),("DB_PORT","9999")], || {
            let cfg = AppConfig::from_env();
            assert_eq!(cfg.active.host, "globalhost");
            assert_eq!(cfg.passive.host, "globalhost");
            assert_eq!(cfg.active.port, 9999);
            assert_eq!(cfg.passive.port, 9999);
        });

        with_env_vars(&keys, &[("DB_HOST","globalhost"),("ACTIVE_DB_HOST","activehost")], || {
            let cfg = AppConfig::from_env();
            assert_eq!(cfg.active.host, "activehost");
            assert_eq!(cfg.passive.host, "globalhost");
        });

        with_env_vars(&keys, &[("ACTIVE_DB_PORT","not-a-number")], || {
            let cfg = AppConfig::from_env();
            assert_eq!(cfg.active.port, 9042);
        });

        with_env_vars(&keys, &[("ACTIVE_DB_USERNAME","")], || {
            let cfg = AppConfig::from_env();
            assert_eq!(cfg.active.username, "");
        });
    });
}