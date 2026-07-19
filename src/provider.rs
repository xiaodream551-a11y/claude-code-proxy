use crate::anthropic::schema::MessagesRequest;
use crate::monitor::MonitorHandle;
use crate::traffic::TrafficCapture;
use anyhow::Result;
use async_trait::async_trait;
use axum::response::Response;
use clap::Subcommand;
use std::fmt;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug, Clone, Subcommand)]
pub enum AuthCommand {
    /// Sign in using browser-based authentication
    Login,
    /// Sign in using a device code
    Device,
    /// Show the current authentication status
    Status,
    /// Delete stored authentication credentials
    Logout,
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn supported_models(&self) -> Vec<String>;
    fn cli(&self) -> &'static dyn CliHandlers;
    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response;
    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response;
}

pub trait CliHandlers: Send + Sync {
    fn login(&self) -> Result<()>;
    fn device(&self) -> Result<()>;
    fn status(&self) -> Result<()>;
    fn logout(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub req_id: String,
    pub session_id: Option<String>,
    pub session_seq: Option<u64>,
    pub provider: String,
    pub traffic: Option<Arc<TrafficCapture>>,
    pub monitor: Option<MonitorHandle>,
    /// Keeps the request-body admission budget charged while a provider still
    /// needs the translated request for initial dispatch or replay. Long-lived
    /// provider caches must move retained data under their own bounded budget.
    pub request_byte_lease: Option<RequestByteLease>,
}

#[derive(Clone)]
pub struct RequestByteLease {
    inner: Arc<RequestByteLeaseInner>,
}

struct RequestByteLeaseInner {
    _permit: OwnedSemaphorePermit,
    buffered_bytes: usize,
}

impl RequestByteLease {
    pub(crate) fn new(permit: OwnedSemaphorePermit, buffered_bytes: usize) -> Self {
        Self {
            inner: Arc::new(RequestByteLeaseInner {
                _permit: permit,
                buffered_bytes,
            }),
        }
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        self.inner.buffered_bytes
    }
}

impl fmt::Debug for RequestByteLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestByteLease")
            .field("owners", &Arc::strong_count(&self.inner))
            .field("buffered_bytes", &self.inner.buffered_bytes)
            .finish()
    }
}
