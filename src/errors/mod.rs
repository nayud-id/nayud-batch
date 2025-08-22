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
    pub fn to_message(&self) -> String {
        self.to_string()
    }
}