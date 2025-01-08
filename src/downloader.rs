use super::{
    item::UrlInfo,
    util::{retry_future, Retryable},
    Item,
};
use bytes::{Buf, Bytes};
use reqwest::{header::LOCATION, redirect, Client, StatusCode};
use std::time::Duration;
use thiserror::Error;
use tryhard::RetryPolicy;

const MAX_RETRIES: u32 = 7;
const RETRY_INITIAL_DELAY_DURATION: Duration = Duration::from_millis(250);
const BAD_GATEWAY_DELAY_DURATION: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_DURATION: Duration = Duration::from_secs(20);
const DEFAULT_REQUEST_TIMEOUT_DURATION: Duration = Duration::from_secs(10);

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("HTTP client error: {0:?}")]
    Client(#[from] reqwest::Error),
    #[error("Unexpected redirect: {0:?}")]
    UnexpectedRedirect(Option<String>),
    #[error("Unexpected redirect URL: {0:?}")]
    UnexpectedRedirectUrl(String),
    #[error("Unexpected status code: {0:?}")]
    UnexpectedStatus(StatusCode),
    #[error("Invalid UTF-8: {0:?}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

impl Retryable for Error {
    fn max_retries() -> u32 {
        MAX_RETRIES
    }

    fn log_level() -> Option<log::Level> {
        Some(log::Level::Warn)
    }

    fn default_initial_delay() -> Duration {
        RETRY_INITIAL_DELAY_DURATION
    }

    fn custom_retry_policy(&self) -> Option<RetryPolicy> {
        match self {
            Error::Io(_) => None,
            Error::Client(_) => None,
            // 502 (often Too Many Requests)
            Error::UnexpectedStatus(StatusCode::BAD_GATEWAY) => {
                Some(RetryPolicy::Delay(BAD_GATEWAY_DELAY_DURATION))
            }
            _ => Some(RetryPolicy::Break),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct RedirectResolution {
    pub url: String,
    pub timestamp: String,
    pub content: Bytes,
    pub valid_initial_content: bool,
    pub valid_digest: bool,
}

#[derive(Clone)]
pub struct Downloader {
    client: Client,
}

impl Downloader {
    pub fn new(request_timeout: Duration) -> reqwest::Result<Self> {
        let tcp_keepalive = Some(TCP_KEEPALIVE_DURATION);

        Ok(Self {
            client: Client::builder()
                .timeout(request_timeout)
                .tcp_keepalive(tcp_keepalive)
                .redirect(redirect::Policy::none())
                .build()?,
        })
    }

    fn wayback_url(url: &str, timestamp: &str, original: bool) -> String {
        format!(
            "https://web.archive.org/web/{}{}/{}",
            timestamp,
            if original { "id_" } else { "if_" },
            url
        )
    }

    pub async fn resolve_redirect(
        &self,
        url: &str,
        timestamp: &str,
        expected_digest: &str,
    ) -> Result<RedirectResolution, Error> {
        let initial_url = Self::wayback_url(url, timestamp, true);
        let initial_response = self.client.head(&initial_url).send().await?;

        match initial_response.status() {
            StatusCode::FOUND => {
                match initial_response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
                {
                    Some(location) => {
                        let info = location
                            .parse::<UrlInfo>()
                            .map_err(|_| Error::UnexpectedRedirectUrl(location))?;

                        let guess = super::util::redirect::guess_redirect_content(&info.url);
                        let mut guess_bytes = guess.as_bytes();
                        let guess_digest = super::digest::compute_digest(&mut guess_bytes)?;

                        let mut valid_initial_content = true;
                        let mut valid_digest = true;

                        let content = if guess_digest == expected_digest {
                            Bytes::from(guess)
                        } else {
                            log::warn!("Invalid guess, re-requesting");
                            let direct_bytes =
                                self.client.get(&initial_url).send().await?.bytes().await?;
                            let direct_digest =
                                super::digest::compute_digest(&mut direct_bytes.clone().reader())?;
                            valid_initial_content = false;
                            valid_digest = direct_digest == expected_digest;

                            direct_bytes
                        };

                        let actual_url = self
                            .direct_resolve_redirect(&info.url, &info.timestamp)
                            .await?;

                        let actual_info = actual_url
                            .parse::<UrlInfo>()
                            .map_err(|_| Error::UnexpectedRedirectUrl(actual_url))?;

                        Ok(RedirectResolution {
                            url: actual_info.url,
                            timestamp: actual_info.timestamp,
                            content,
                            valid_initial_content,
                            valid_digest,
                        })
                    }
                    None => Err(Error::UnexpectedRedirect(None)),
                }
            }
            other => Err(Error::UnexpectedStatus(other)),
        }
    }

    async fn direct_resolve_redirect(&self, url: &str, timestamp: &str) -> Result<String, Error> {
        let response = self
            .client
            .head(Self::wayback_url(url, timestamp, true))
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

    pub async fn resolve_redirect_shallow(
        &self,
        url: &str,
        timestamp: &str,
        expected_digest: &str,
    ) -> Result<(UrlInfo, String, bool), Error> {
        let initial_url = Self::wayback_url(url, timestamp, true);
        let initial_response = self.client.head(&initial_url).send().await?;

        match initial_response.status() {
            StatusCode::FOUND => {
                match initial_response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
                {
                    Some(location) => {
                        let info = location
                            .parse::<UrlInfo>()
                            .map_err(|_| Error::UnexpectedRedirectUrl(location))?;

                        let guess = super::util::redirect::guess_redirect_content(&info.url);
                        let mut guess_bytes = guess.as_bytes();
                        let guess_digest = super::digest::compute_digest(&mut guess_bytes)?;

                        let (content, valid_digest) = if guess_digest == expected_digest {
                            (guess, true)
                        } else {
                            log::warn!("Invalid guess, re-requesting");
                            let direct_bytes =
                                self.client.get(&initial_url).send().await?.bytes().await?;
                            let direct_digest =
                                super::digest::compute_digest(&mut direct_bytes.clone().reader())?;
                            (
                                std::str::from_utf8(&direct_bytes)?.to_string(),
                                direct_digest == expected_digest,
                            )
                        };

                        Ok((info, content, valid_digest))
                    }
                    None => Err(Error::UnexpectedRedirect(None)),
                }
            }
            other => Err(Error::UnexpectedStatus(other)),
        }
    }

    async fn download(&self, url: &str, timestamp: &str, original: bool) -> Result<Bytes, Error> {
        retry_future(|| self.download_once(url, timestamp, original)).await
    }

    async fn download_once(
        &self,
        url: &str,
        timestamp: &str,
        original: bool,
    ) -> Result<Bytes, Error> {
        let response = self
            .client
            .get(Self::wayback_url(url, timestamp, original))
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

impl Default for Downloader {
    fn default() -> Self {
        Self::new(DEFAULT_REQUEST_TIMEOUT_DURATION).unwrap()
    }
}
