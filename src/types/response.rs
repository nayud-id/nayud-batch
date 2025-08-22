pub const CODE_SUCCESS: &str = "00";
pub const CODE_FAILURE: &str = "99";

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
}