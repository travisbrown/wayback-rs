use super::{
    util::{retry_future, Retryable},
    Item,
};
use bytes::{Buf, Bytes};
use reqwest::{header::LOCATION, redirect, Client, StatusCode};
use std::time::Duration;
use thiserror::Error;
use tryhard::RetryPolicy;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("HTTP client error: {0:?}")]
    ClientError(#[from] reqwest::Error),
    #[error("Unexpected redirect: {0:?}")]
    UnexpectedRedirect(Option<String>),
    #[error("Unexpected redirect URL: {0:?}")]
    UnexpectedRedirectUrl(String),
    #[error("Unexpected status code: {0:?}")]
    UnexpectedStatus(StatusCode),
}

impl Retryable for Error {
    fn max_retries() -> u32 {
        7
    }

    fn log_level() -> Option<log::Level> {
        Some(log::Level::Warn)
    }

    fn default_initial_delay() -> Duration {
        Duration::from_millis(250)
    }

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

    pub async fn resolve_redirect(
        &self,
        url: &str,
        timestamp: &str,
        expected_digest: &str,
    ) -> Result<RedirectResolution, Error> {
        let initial_url = Downloader::wayback_url(url, timestamp, true);
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
                            .parse::<super::item::UrlInfo>()
                            .map_err(|_| Error::UnexpectedRedirectUrl(location))?;

                        let guess = super::util::redirect::guess_redirect_content(&info.url);
                        let mut guess_bytes = guess.as_bytes();
                        let guess_digest = super::digest::compute_digest(&mut guess_bytes)?;

                        let mut valid_initial_content = true;
                        let mut valid_digest = true;

                        let content = if guess_digest == expected_digest {
                            Bytes::from(guess)
                        } else {
                            println!("Invalid guess, re-requesting");
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
                            .parse::<super::item::UrlInfo>()
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
            .head(Downloader::wayback_url(url, timestamp, true))
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