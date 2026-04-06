use crate::error::Result;
use crate::retry::RetryPolicy;
use crate::stream::DurableStream;
use crate::transport::HttpTransport;
use crate::types::StreamPath;
use reqwest::Url;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub request_timeout: Duration,
    pub retry_policy: RetryPolicy,
    pub user_agent: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            retry_policy: RetryPolicy::default(),
            user_agent: Some(format!(
                "{}/{}",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DurableStreamsClient {
    transport: Arc<HttpTransport>,
}

impl DurableStreamsClient {
    /// Create a client with the default configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when `base_url` is not a valid absolute URL.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Self::with_config(base_url, ClientConfig::default())
    }

    /// Create a client with an explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when `base_url` is not a valid absolute URL or when
    /// the underlying `reqwest` client cannot be constructed.
    pub fn with_config(base_url: impl AsRef<str>, config: ClientConfig) -> Result<Self> {
        let base_url = Url::parse(base_url.as_ref()).map_err(|source| {
            crate::error::Error::InvalidBaseUrl {
                url: base_url.as_ref().to_string(),
                source,
            }
        })?;
        let transport = HttpTransport::new(base_url, config)?;
        Ok(Self {
            transport: Arc::new(transport),
        })
    }

    /// Create a typed handle for a specific stream path.
    ///
    /// # Errors
    ///
    /// Returns an error when `path` is not a valid absolute stream path.
    pub fn stream(&self, path: impl AsRef<str>) -> Result<DurableStream> {
        Ok(DurableStream::new(
            self.transport.clone(),
            StreamPath::new(path.as_ref())?,
        ))
    }
}
