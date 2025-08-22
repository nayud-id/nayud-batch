#[derive(Debug, Default, Clone)]
pub struct RequestContext {
    pub correlation_id: Option<String>,
}

impl RequestContext {
    pub fn new(correlation_id: Option<String>) -> Self {
        Self { correlation_id }
    }
}