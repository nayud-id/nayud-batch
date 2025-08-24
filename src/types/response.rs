use serde::Serialize;

use crate::errors::AppError;

fn map_app_error_to_detail(err: &AppError) -> (String, String, String) {
    match err {
        AppError::Config(msg) => (
            format!("Configuration error: {}", msg),
            "The application configuration seems incomplete or contains an invalid value.".to_string(),
            "Review your app settings or environment variables and correct any typos or missing values. If unsure, restore the default config and try again.".to_string(),
        ),
        AppError::Db(msg) => (
            format!("Database error: {}", msg),
            "The app could not talk to the database or the database refused the request.".to_string(),
            "Please ensure the database is running and reachable. Check the host, port, username/password, and network connectivity. Then try again.".to_string(),
        ),
        AppError::Web(msg) => (
            format!("Request error: {}", msg),
            "Your request could not be completed due to a server-side issue.".to_string(),
            "Please retry in a moment. If it keeps happening, contact support and include the time of the error and what you tried to do.".to_string(),
        ),
        AppError::Other(msg) => (
            format!("Unexpected error: {}", msg),
            "An unexpected problem occurred.".to_string(),
            "Please try again. If the issue persists, contact support with a short description of the action you took and this error message.".to_string(),
        ),
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum ApiMessage {
    Text(String),
    Detail { what: String, why: String, how: String },
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiResponse<T> {
    pub code: &'static str,
    pub message: ApiMessage,
    pub data: Option<T>,
}

pub const CODE_SUCCESS: &str = "00";
pub const CODE_FAILURE: &str = "99";

impl<T> ApiResponse<T> {
    pub fn ok(message: impl Into<String>, data: Option<T>) -> Self {
        Self {
            code: CODE_SUCCESS,
            message: ApiMessage::Text(message.into()),
            data,
        }
    }

    pub fn success_with(message: impl Into<String>, data: T) -> Self {
        Self::ok(message, Some(data))
    }

    pub fn failure(message: impl Into<String>) -> Self {
        let what = message.into();
        let why = "Something went wrong while processing your request.".to_string();
        let how = "Please try again in a moment. If the problem continues, contact support with a brief description of what you were doing just before this happened.".to_string();
        Self::failure_detail(what, why, how)
    }

    pub fn failure_detail(what: impl Into<String>, why: impl Into<String>, how: impl Into<String>) -> Self {
        Self {
            code: CODE_FAILURE,
            message: ApiMessage::Detail { what: what.into(), why: why.into(), how: how.into() },
            data: None,
        }
    }

    pub fn is_success(&self) -> bool {
        self.code == CODE_SUCCESS
    }

    pub fn from_result(res: crate::errors::AppResult<T>, success_message: impl Into<String>) -> Self {
        match res {
            Ok(v) => Self::success_with(success_message, v),
            Err(e) => {
                let (what, why, how) = map_app_error_to_detail(&e);
                Self::failure_detail(what, why, how)
            }
        }
    }

    pub fn from_option(
        opt: Option<T>,
        some_message: impl Into<String>,
        none_message: impl Into<String>,
    ) -> Self {
        match opt {
            Some(v) => Self::success_with(some_message, v),
            None => Self::failure(none_message),
        }
    }
}

impl ApiResponse<()> {
    pub fn success(message: impl Into<String>) -> Self {
        ApiResponse::ok(message, None)
    }

    pub fn from_error(err: &AppError) -> Self {
        let (what, why, how) = map_app_error_to_detail(err);
        Self::failure_detail(what, why, how)
    }
}