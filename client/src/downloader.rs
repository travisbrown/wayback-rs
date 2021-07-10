use super::Item;
use bytes::Bytes;
use reqwest::{header::LOCATION, redirect, Client, StatusCode};
use std::time::Duration;
use thiserror::Error;
use tryhard::{backoff_strategies::BackoffStrategy, RetryPolicy};

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("HTTP client error: {0:?}")]
    ClientError(#[from] reqwest::Error),
    #[error("Unexpected redirect: {0:?}")]
    UnexpectedRedirect(Option<String>),
    #[error("Unexpected status code: {0:?}")]
    UnexpectedStatus(StatusCode),
}

impl Error {
    // An empty value represents the default
    fn custom_retry_policy(&self) -> Option<RetryPolicy> {
        match self {
            Error::IOError(_) => None,
            Error::ClientError(_) => None,
            // 502 (often Too Many Requests)
            Error::UnexpectedStatus(StatusCode::BAD_GATEWAY) => {
                Some(RetryPolicy::Delay(Duration::from_secs(30)))
            }
            _ => Some(RetryPolicy::Break),
        }
    }
}

struct RetryStrategy {
    delay: Duration,
}

impl RetryStrategy {
    fn new(delay: Duration) -> RetryStrategy {
        RetryStrategy { delay }
    }
}

impl<'a> BackoffStrategy<'a, Error> for RetryStrategy {
    type Output = RetryPolicy;

    fn delay(&mut self, _attempt: u32, error: &'a Error) -> RetryPolicy {
        error.custom_retry_policy().unwrap_or_else(|| {
            let prev_delay = self.delay;
            self.delay *= 2;
            RetryPolicy::Delay(prev_delay)
        })
    }
}

#[derive(Debug)]
pub enum Content {
    Direct {
        content: Bytes,
    },
    Redirect {
        location: String,
        original_content: Bytes,
        content: Bytes,
    },
}

impl Content {
    fn content(&self) -> &Bytes {
        match self {
            Content::Direct { content } => content,
            Content::Redirect { content, .. } => content,
        }
    }
}

#[derive(Clone)]
pub struct Downloader {
    client: Client,
    retry_count: usize,
    retry_delay: Duration,
}

impl Default for Downloader {
    fn default() -> Downloader {
        Downloader::new(7, Duration::from_millis(250)).unwrap()
    }
}

impl Downloader {
    const TCP_KEEPALIVE_SECS: u64 = 20;

    pub fn new(retry_count: usize, retry_delay: Duration) -> reqwest::Result<Self> {
        let tcp_keepalive = Some(Duration::from_secs(Self::TCP_KEEPALIVE_SECS));

        Ok(Self {
            client: Client::builder()
                .tcp_keepalive(tcp_keepalive)
                .redirect(redirect::Policy::none())
                .build()?,
            retry_count,
            retry_delay,
        })
    }

    fn wayback_url(url: &str, timestamp: &str, original: bool) -> String {
        format!(
            "http://web.archive.org/web/{}{}/{}",
            timestamp,
            if original { "id_" } else { "if_" },
            url
        )
    }

    pub async fn download_redirect(
        &self,
        url: &str,
        timestamp: &str,
    ) -> Result<(String, Bytes), Error> {
        let response = self
            .client
            .get(Downloader::wayback_url(url, timestamp, true))
            .send()
            .await?;

        match response.status() {
            StatusCode::FOUND => {
                match response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
                {
                    Some(location) => Ok((location, response.bytes().await?)),
                    None => Err(Error::UnexpectedRedirect(None)),
                }
            }
            other => Err(Error::UnexpectedStatus(other)),
        }
    }

    pub async fn resolve_redirect(&self, url: &str, timestamp: &str) -> Result<String, Error> {
        let response = self
            .client
            .get(Downloader::wayback_url(url, timestamp, true))
            .send()
            .await?;

        match response.status() {
            StatusCode::FOUND => {
                match response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
                {
                    Some(location) => Ok(location),
                    None => Err(Error::UnexpectedRedirect(None)),
                }
            }
            other => Err(Error::UnexpectedStatus(other)),
        }
    }

    async fn download(&self, url: &str, timestamp: &str, original: bool) -> Result<Bytes, Error> {
        tryhard::retry_fn(|| self.download_once(url, timestamp, original))
            .retries(self.retry_count as u32)
            .custom_backoff(RetryStrategy::new(self.retry_delay))
            .await
    }

    async fn download_once(
        &self,
        url: &str,
        timestamp: &str,
        original: bool,
    ) -> Result<Bytes, Error> {
        let response = self
            .client
            .get(Downloader::wayback_url(url, timestamp, original))
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => Ok(response.bytes().await?),
            other => Err(Error::UnexpectedStatus(other)),
        }
    }

    pub async fn download_item(&self, item: &Item) -> Result<Bytes, Error> {
        self.download(&item.url, &item.timestamp(), true).await
    }
}
