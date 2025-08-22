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