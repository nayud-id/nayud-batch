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
    pub use_tls: bool,
    pub tls_ca_file: Option<String>,
    pub tls_insecure_skip_verify: bool,
}

#[derive(Clone, Debug, Default)]
pub struct DriverConfig {
    pub request_timeout_ms: Option<u64>,
    pub connection_timeout_ms: Option<u64>,
    pub tcp_keepalive_secs: Option<u64>,
    pub compression: Option<String>,
    pub default_page_size: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub active: DbEndpoint,
    pub passive: DbEndpoint,
    pub driver: DriverConfig,
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
            use_tls: false,
            tls_ca_file: None,
            tls_insecure_skip_verify: false,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        let active = DbEndpoint::default();
        let mut passive = active.clone();
        passive.port = 9043;
        passive.rack = "asia-southeast2-b".into();
        Self { active, passive, driver: DriverConfig::default() }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let defaults = AppConfig::default();
        let active = DbEndpoint::from_env_with_defaults("ACTIVE_DB", Some("DB"), &defaults.active);
        let passive_defaults = defaults.passive;
        let passive = DbEndpoint::from_env_with_defaults("PASSIVE_DB", Some("DB"), &passive_defaults);
        let driver = DriverConfig::from_env("DB");
        AppConfig { active, passive, driver }
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
            use_tls: read_env_bool(prefix, global_prefix, "USE_TLS", defaults.use_tls),
            tls_ca_file: read_env_opt_string_scoped(prefix, global_prefix, "TLS_CA_FILE").or_else(|| defaults.tls_ca_file.clone()),
            tls_insecure_skip_verify: read_env_bool(prefix, global_prefix, "TLS_INSECURE_SKIP_VERIFY", defaults.tls_insecure_skip_verify),
        }
    }
}

impl DriverConfig {
    pub fn from_env(global_prefix: &str) -> Self {
        let request_timeout_ms = read_env_opt_u64(global_prefix, "REQUEST_TIMEOUT_MS");
        let connection_timeout_ms = read_env_opt_u64(global_prefix, "CONNECTION_TIMEOUT_MS");
        let tcp_keepalive_secs = read_env_opt_u64(global_prefix, "TCP_KEEPALIVE_SECS");
        let compression = read_env_opt_string(global_prefix, "COMPRESSION");
        let default_page_size = read_env_opt_i32(global_prefix, "DEFAULT_PAGE_SIZE");
        Self { request_timeout_ms, connection_timeout_ms, tcp_keepalive_secs, compression, default_page_size }
    }
}

fn read_env_string(prefix: &str, global_prefix: Option<&str>, name: &str, default: String) -> String {
    let specific = format!("{}_{}", prefix, name);
    if let Ok(val) = env::var(&specific) { return val; }
    if let Some(gp) = global_prefix {
        let global = format!("{}_{}", gp, name);
        if let Ok(val) = env::var(&global) { return val; }
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

fn read_env_bool(prefix: &str, global_prefix: Option<&str>, name: &str, default: bool) -> bool {
    let specific = format!("{}_{}", prefix, name);
    if let Ok(val) = env::var(&specific) {
        if let Ok(b) = parse_bool(&val) { return b; }
    }
    if let Some(gp) = global_prefix {
        let global = format!("{}_{}", gp, name);
        if let Ok(val) = env::var(&global) {
            if let Ok(b) = parse_bool(&val) { return b; }
        }
    }
    default
}

fn parse_bool(s: &str) -> Result<bool, ()> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => Err(()),
    }
}

fn read_env_opt_u64(global_prefix: &str, name: &str) -> Option<u64> {
    env::var(format!("{}_{}", global_prefix, name)).ok().and_then(|v| v.parse::<u64>().ok())
}

fn read_env_opt_i32(global_prefix: &str, name: &str) -> Option<i32> {
    env::var(format!("{}_{}", global_prefix, name)).ok().and_then(|v| v.parse::<i32>().ok())
}

fn read_env_opt_string(global_prefix: &str, name: &str) -> Option<String> {
    env::var(format!("{}_{}", global_prefix, name)).ok()
}

fn read_env_opt_string_scoped(prefix: &str, global_prefix: Option<&str>, name: &str) -> Option<String> {
    let specific = format!("{}_{}", prefix, name);
    if let Ok(val) = env::var(&specific) { return Some(val); }
    if let Some(gp) = global_prefix {
        let global = format!("{}_{}", gp, name);
        if let Ok(val) = env::var(&global) { return Some(val); }
    }
    None
}