use std::fmt;

#[derive(Debug)]
pub enum AppError {
    Config(String),
    Db(String),
    Web(String),
    Other(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Config(m) => write!(f, "Config: {}", m),
            AppError::Db(m) => write!(f, "Db: {}", m),
            AppError::Web(m) => write!(f, "Web: {}", m),
            AppError::Other(m) => write!(f, "Other: {}", m),
        }
    }
}

impl std::error::Error for AppError {}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn to_message(&self) -> String { self.to_string() }

    pub fn config(msg: impl Into<String>) -> Self { AppError::Config(msg.into()) }
    pub fn db(msg: impl Into<String>) -> Self { AppError::Db(msg.into()) }
    pub fn web(msg: impl Into<String>) -> Self { AppError::Web(msg.into()) }
    pub fn other(msg: impl Into<String>) -> Self { AppError::Other(msg.into()) }
}

impl From<&str> for AppError {
    fn from(value: &str) -> Self { AppError::Other(value.to_string()) }
}

impl From<String> for AppError {
    fn from(value: String) -> Self { AppError::Other(value) }
}