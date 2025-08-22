pub const CODE_SUCCESS: &str = "00";
pub const CODE_FAILURE: &str = "99";

use crate::errors::AppError;

#[derive(Clone, Debug)]
pub struct ApiResponse<T> {
    pub code: &'static str,
    pub message: String,
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn ok(message: impl Into<String>, data: Option<T>) -> Self {
        Self {
            code: CODE_SUCCESS,
            message: message.into(),
            data,
        }
    }

    pub fn success_with(message: impl Into<String>, data: T) -> Self {
        Self::ok(message, Some(data))
    }

    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            code: CODE_FAILURE,
            message: message.into(),
            data: None,
        }
    }

    pub fn is_success(&self) -> bool {
        self.code == CODE_SUCCESS
    }

    pub fn from_result(res: crate::errors::AppResult<T>, success_message: impl Into<String>) -> Self {
        match res {
            Ok(v) => Self::success_with(success_message, v),
            Err(e) => Self::failure(e.to_message()),
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
        ApiResponse::failure(err.to_message())
    }
}