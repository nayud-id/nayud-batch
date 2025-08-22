use ntex::http::header::{HeaderName, HeaderValue};
use ntex::service::{Middleware, Service, ServiceCtx};
use ntex::web;

#[derive(Debug, Default, Clone)]
pub struct RequestContext {
    pub correlation_id: Option<String>,
}

impl RequestContext {
    pub fn new(correlation_id: Option<String>) -> Self {
        Self { correlation_id }
    }
}

pub struct CorrelationId {
    header: HeaderName,
}

impl CorrelationId {
    pub fn new() -> Self {
        CorrelationId { header: HeaderName::from_static("x-correlation-id") }
    }
}

impl Default for CorrelationId { fn default() -> Self { Self::new() } }

pub struct CorrelationIdMiddleware<S> {
    service: S,
    header: HeaderName,
}

impl<S> Middleware<S> for CorrelationId {
    type Service = CorrelationIdMiddleware<S>;

    fn create(&self, service: S) -> Self::Service {
        CorrelationIdMiddleware { service, header: self.header.clone() }
    }
}

impl<S, Err> Service<web::WebRequest<Err>> for CorrelationIdMiddleware<S>
where
    S: Service<web::WebRequest<Err>, Response = web::WebResponse, Error = web::Error>,
    Err: web::ErrorRenderer,
{
    type Response = web::WebResponse;
    type Error = web::Error;

    ntex::forward_ready!(service);

    async fn call(&self, req: web::WebRequest<Err>, ctx: ServiceCtx<'_, Self>) -> Result<Self::Response, Self::Error> {
        let cid = req
            .headers()
            .get(&self.header)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let mut res = ctx.call(&self.service, req).await?;

        if let Ok(val) = HeaderValue::from_str(&cid) {
            res.headers_mut().insert(self.header.clone(), val);
        }

        Ok(res)
    }
}