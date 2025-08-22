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
            AppError::Config(s) => write!(f, "config error: {}", s),
            AppError::Db(s) => write!(f, "db error: {}", s),
            AppError::Web(s) => write!(f, "web error: {}", s),
            AppError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for AppError {}

pub type AppResult<T> = Result<T, AppError>;