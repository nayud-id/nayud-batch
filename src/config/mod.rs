use std::env;

#[derive(Clone, Debug)]
pub struct DbEndpoint {
    pub host: String,
    pub port: u16,
    pub keyspace: String,
    pub datacenter: String,
    pub rack: String,
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub active: DbEndpoint,
    pub passive: DbEndpoint,
}

impl Default for DbEndpoint {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 9042,
            keyspace: "batch".into(),
            datacenter: "asia-southeast2".into(),
            rack: "asia-southeast2-a".into(),
            username: "cassandra".into(),
            password: "cassandra".into(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        let active = DbEndpoint::default();
        let mut passive = active.clone();
        passive.port = 9043;
        passive.rack = "asia-southeast2-b".into();
        Self { active, passive }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let defaults = AppConfig::default();
        let active = DbEndpoint::from_env_with_defaults("ACTIVE_DB", Some("DB"), &defaults.active);
        let passive_defaults = defaults.passive;
        let passive = DbEndpoint::from_env_with_defaults("PASSIVE_DB", Some("DB"), &passive_defaults);
        AppConfig { active, passive }
    }
}

impl DbEndpoint {
    fn from_env_with_defaults(prefix: &str, global_prefix: Option<&str>, defaults: &DbEndpoint) -> DbEndpoint {
        DbEndpoint {
            host: read_env_string(prefix, global_prefix, "HOST", defaults.host.clone()),
            port: read_env_u16(prefix, global_prefix, "PORT", defaults.port),
            keyspace: read_env_string(prefix, global_prefix, "KEYSPACE", defaults.keyspace.clone()),
            datacenter: read_env_string(prefix, global_prefix, "DATACENTER", defaults.datacenter.clone()),
            rack: read_env_string(prefix, global_prefix, "RACK", defaults.rack.clone()),
            username: read_env_string(prefix, global_prefix, "USERNAME", defaults.username.clone()),
            password: read_env_string(prefix, global_prefix, "PASSWORD", defaults.password.clone()),
        }
    }
}

fn read_env_string(prefix: &str, global_prefix: Option<&str>, name: &str, default: String) -> String {
    let specific = format!("{}_{}", prefix, name);
    if let Ok(val) = env::var(&specific) {
        if !val.is_empty() { return val; }
    }
    if let Some(gp) = global_prefix {
        let global = format!("{}_{}", gp, name);
        if let Ok(val) = env::var(&global) {
            if !val.is_empty() { return val; }
        }
    }
    default
}

fn read_env_u16(prefix: &str, global_prefix: Option<&str>, name: &str, default: u16) -> u16 {
    let specific = format!("{}_{}", prefix, name);
    if let Ok(val) = env::var(&specific) {
        if let Ok(port) = val.parse::<u16>() { return port; }
    }
    if let Some(gp) = global_prefix {
        let global = format!("{}_{}", gp, name);
        if let Ok(val) = env::var(&global) {
            if let Ok(port) = val.parse::<u16>() { return port; }
        }
    }
    default
}