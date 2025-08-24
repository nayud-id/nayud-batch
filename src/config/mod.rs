use std::env;
use std::fs;
use std::path::Path;
use log::warn;
use serde::Deserialize;

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
    pub replication_factor: Option<u32>,
    pub durable_writes: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct DriverConfig {
    pub request_timeout_ms: Option<u64>,
    pub connection_timeout_ms: Option<u64>,
    pub tcp_keepalive_secs: Option<u64>,
    pub compression: Option<String>,
    pub default_page_size: Option<i32>,
}

#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    pub bind_addr: String,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub active: DbEndpoint,
    pub passive: DbEndpoint,
    pub driver: DriverConfig,
    pub server: ServerConfig,
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
            replication_factor: Some(3),
            durable_writes: Some(true),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        let active = DbEndpoint::default();
        let mut passive = active.clone();
        passive.port = 9043;
        passive.rack = "asia-southeast2-b".into();
        let driver = DriverConfig::default();
        let server = ServerConfig { bind_addr: "127.0.0.1:8080".into() };
        Self { active, passive, driver, server }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let defaults = AppConfig::default();
        let active = DbEndpoint::from_env_with_defaults("ACTIVE_DB", Some("DB"), &defaults.active);
        let passive_defaults = defaults.passive;
        let passive = DbEndpoint::from_env_with_defaults("PASSIVE_DB", Some("DB"), &passive_defaults);
        let driver = DriverConfig::from_env("DB");
        let server = ServerConfig::from_env("WEB").unwrap_or_else(|| defaults.server.clone());
        AppConfig { active, passive, driver, server }
    }

    pub fn from_file_or_env() -> Self {
        let candidates: Vec<String> = env::var("NAYUD_CONFIG_FILE")
            .ok()
            .into_iter()
            .chain(std::iter::once("config/nayud-batch.toml".to_string()))
            .collect();

        for p in candidates {
            let path = Path::new(&p);
            if path.exists() {
                match fs::read_to_string(path) {
                    Ok(s) => {
                        match toml::from_str::<TomlAppConfig>(&s) {
                            Ok(tcfg) => {
                                let cfg: AppConfig = tcfg.into();
                                return cfg;
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to parse config file {}: {}. Falling back to env.",
                                    p, e
                                );
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to read config file {}: {}. Falling back to env.",
                            p, e
                        );
                        break;
                    }
                }
            }
        }

        Self::from_env()
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
            replication_factor: defaults.replication_factor.clone(),
            durable_writes: defaults.durable_writes.clone(),
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

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct TomlDbEndpoint {
    host: String,
    port: u16,
    keyspace: String,
    datacenter: String,
    rack: String,
    username: String,
    password: String,
    use_tls: bool,
    tls_ca_file: Option<String>,
    tls_insecure_skip_verify: bool,
    replication_factor: Option<u32>,
    durable_writes: Option<bool>,
}

impl Default for TomlDbEndpoint {
    fn default() -> Self { DbEndpoint::default().into() }
}

macro_rules! make_endpoint {
    ($self_:ident, $src:expr) => {
        $self_ {
            host: $src.host,
            port: $src.port,
            keyspace: $src.keyspace,
            datacenter: $src.datacenter,
            rack: $src.rack,
            username: $src.username,
            password: $src.password,
            use_tls: $src.use_tls,
            tls_ca_file: $src.tls_ca_file,
            tls_insecure_skip_verify: $src.tls_insecure_skip_verify,
            replication_factor: $src.replication_factor,
            durable_writes: $src.durable_writes,
        }
    };
}

impl From<DbEndpoint> for TomlDbEndpoint {
    fn from(d: DbEndpoint) -> Self {
        make_endpoint!(Self, d)
    }
}

impl From<TomlDbEndpoint> for DbEndpoint {
    fn from(t: TomlDbEndpoint) -> Self {
        make_endpoint!(Self, t)
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(default)]
struct TomlDriverConfig {
    request_timeout_ms: Option<u64>,
    connection_timeout_ms: Option<u64>,
    tcp_keepalive_secs: Option<u64>,
    compression: Option<String>,
    default_page_size: Option<i32>,
}

impl From<TomlDriverConfig> for DriverConfig {
    fn from(t: TomlDriverConfig) -> Self {
        Self {
            request_timeout_ms: t.request_timeout_ms,
            connection_timeout_ms: t.connection_timeout_ms,
            tcp_keepalive_secs: t.tcp_keepalive_secs,
            compression: t.compression,
            default_page_size: t.default_page_size,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct TomlServerConfig {
    bind_addr: String,
}

impl Default for TomlServerConfig {
    fn default() -> Self { Self { bind_addr: "127.0.0.1:8080".into() } }
}

impl From<TomlServerConfig> for ServerConfig {
    fn from(t: TomlServerConfig) -> Self { ServerConfig { bind_addr: t.bind_addr } }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct TomlAppConfig {
    active: TomlDbEndpoint,
    passive: TomlDbEndpoint,
    driver: TomlDriverConfig,
    server: TomlServerConfig,
}

impl Default for TomlAppConfig {
    fn default() -> Self {
        Self {
            active: TomlDbEndpoint::from(DbEndpoint::default()),
            passive: TomlDbEndpoint::from({
                let mut p = DbEndpoint::default();
                p.port = 9043;
                p.rack = "asia-southeast2-b".into();
                p
            }),
            driver: TomlDriverConfig::default(),
            server: TomlServerConfig::default(),
        }
    }
}

impl From<TomlAppConfig> for AppConfig {
    fn from(t: TomlAppConfig) -> Self {
        Self {
            active: t.active.into(),
            passive: t.passive.into(),
            driver: t.driver.into(),
            server: t.server.into(),
        }
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

impl AppConfig {
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

fn read_env_opt_u64(global_prefix: &str, name: &str) -> Option<u64> {
    env::var(format!("{}_{}", global_prefix, name)).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_bool(s: &str) -> Result<bool, ()> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => Err(()),
    }
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
impl ServerConfig {
    pub fn from_env(prefix: &str) -> Option<Self> {
        let bind_addr = read_env_opt_string(prefix, "BIND_ADDR");
        if let Some(ba) = bind_addr { Some(ServerConfig { bind_addr: ba }) } else { None }
    }
}