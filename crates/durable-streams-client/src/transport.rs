use crate::client::ClientConfig;
use crate::error::{Error, Result};
use crate::protocol::headers;
use crate::retry::RetryPolicy;
use reqwest::{Method, RequestBuilder, Response, Url};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
pub struct HttpTransport {
    base_url: Url,
    client: reqwest::Client,
    retry_policy: RetryPolicy,
    request_timeout: Duration,
}

impl HttpTransport {
    pub fn new(base_url: Url, config: ClientConfig) -> Result<Self> {
        let mut builder = reqwest::Client::builder().tcp_keepalive(Duration::from_secs(30));
        if let Some(user_agent) = config.user_agent {
            builder = builder.user_agent(user_agent);
        }
        let client = builder.build().map_err(|source| Error::Transport {
            operation: "build-client",
            path: base_url.to_string(),
            source,
        })?;
        Ok(Self {
            base_url,
            client,
            retry_policy: config.retry_policy,
            request_timeout: config.request_timeout,
        })
    }

    pub fn url_for(&self, path: &str) -> Result<Url> {
        self.base_url
            .join(path)
            .map_err(|source| Error::InvalidBaseUrl {
                url: format!("{} + {}", self.base_url, path),
                source,
            })
    }

    pub fn request(&self, method: Method, path: &str) -> Result<RequestBuilder> {
        let url = self.url_for(path)?;
        Ok(self.client.request(method, url))
    }

    pub fn bounded_request(&self, method: Method, path: &str) -> Result<RequestBuilder> {
        Ok(self.request(method, path)?.timeout(self.request_timeout))
    }

    pub async fn execute_with_retry(
        &self,
        operation: &'static str,
        path: &str,
        method: Method,
        request_builder: impl Fn() -> Result<RequestBuilder>,
    ) -> Result<Response> {
        let retryable_method = matches!(method, Method::GET | Method::HEAD);

        let mut attempt = 1usize;
        loop {
            let request = request_builder()?;
            match request.send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) if response.status() == reqwest::StatusCode::NO_CONTENT => {
                    return Ok(response);
                }
                Ok(response) => {
                    let status = response.status();
                    let headers = response.headers().clone();
                    let body = response.bytes().await.map_err(|source| Error::Transport {
                        operation,
                        path: path.to_string(),
                        source,
                    })?;
                    let error = headers::http_error(operation, path, status, &headers, &body);
                    let should_retry = retryable_method
                        && matches!(
                            &error,
                            Error::Http(http_error) if http_error.is_retryable()
                        )
                        && attempt < self.retry_policy.max_attempts;
                    if should_retry {
                        let backoff = match &error {
                            Error::Http(http_error) => http_error
                                .retry_after
                                .unwrap_or_else(|| self.retry_policy.backoff_for_attempt(attempt)),
                            _ => self.retry_policy.backoff_for_attempt(attempt),
                        };
                        sleep(backoff).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(error);
                }
                Err(source) => {
                    let should_retry = retryable_method
                        && (source.is_timeout() || source.is_connect() || source.is_request())
                        && attempt < self.retry_policy.max_attempts;
                    if should_retry {
                        sleep(self.retry_policy.backoff_for_attempt(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::Transport {
                        operation,
                        path: path.to_string(),
                        source,
                    });
                }
            }
        }
    }
}
