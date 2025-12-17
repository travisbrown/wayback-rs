use super::{
    item::UrlInfo,
    util::{
        observe::{ErrorClass, Observer, Surface},
        retry_future, Pacer, Retryable,
    },
    Item,
};
use bytes::{Buf, Bytes};
use reqwest::{header::LOCATION, redirect, Client, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
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
    pacer: Option<Arc<Pacer>>,
    observer: Option<Arc<dyn Observer>>,
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
            pacer: None,
            observer: None,
        })
    }

    /// Attach an opt-in request pacer.
    ///
    /// This is purely additive: unless called, behavior is unchanged.
    pub fn with_pacer(mut self, pacer: Arc<Pacer>) -> Self {
        self.pacer = Some(pacer);
        self
    }

    /// Attach an opt-in observer that receives request/response events.
    pub fn with_observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
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
        if let Some(pacer) = self.pacer.as_ref() {
            pacer.pace_content().await;
        }
        let initial_url_arc: Arc<str> = Arc::from(initial_url.clone());
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::start(
                Surface::Content,
                "HEAD",
                initial_url_arc.clone(),
            ));
        }
        let started = Instant::now();
        let initial_response = self
            .client
            .head(initial_url_arc.as_ref())
            .send()
            .await
            .map_err(|e| {
                if let Some(obs) = self.observer.as_ref() {
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else if e.is_connect() {
                        ErrorClass::Connect
                    } else {
                        ErrorClass::Other
                    };
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "HEAD",
                        initial_url_arc.clone(),
                        None,
                        Some(started.elapsed()),
                        class,
                    ));
                }
                Error::Client(e)
            })?;

        match initial_response.status() {
            StatusCode::FOUND => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::complete(
                        Surface::Content,
                        "HEAD",
                        initial_url_arc.clone(),
                        StatusCode::FOUND.as_u16(),
                        started.elapsed(),
                    ));
                }
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
                            if let Some(pacer) = self.pacer.as_ref() {
                                pacer.pace_content().await;
                            }
                            if let Some(obs) = self.observer.as_ref() {
                                obs.on_event(&super::util::observe::Event::start(
                                    Surface::Content,
                                    "GET",
                                    initial_url_arc.clone(),
                                ));
                            }
                            let started = Instant::now();
                            let response = self
                                .client
                                .get(initial_url_arc.as_ref())
                                .send()
                                .await
                                .map_err(|e| {
                                if let Some(obs) = self.observer.as_ref() {
                                    let class = if e.is_timeout() {
                                        ErrorClass::Timeout
                                    } else if e.is_connect() {
                                        ErrorClass::Connect
                                    } else {
                                        ErrorClass::Other
                                    };
                                    obs.on_event(&super::util::observe::Event::error(
                                        Surface::Content,
                                        "GET",
                                        initial_url_arc.clone(),
                                        None,
                                        Some(started.elapsed()),
                                        class,
                                    ));
                                }
                                Error::Client(e)
                            })?;
                            let status = response.status();
                            if status != StatusCode::OK {
                                if let Some(obs) = self.observer.as_ref() {
                                    obs.on_event(&super::util::observe::Event::error(
                                        Surface::Content,
                                        "GET",
                                        initial_url_arc.clone(),
                                        Some(status.as_u16()),
                                        Some(started.elapsed()),
                                        ErrorClass::Http,
                                    ));
                                }
                                return Err(Error::UnexpectedStatus(status));
                            }
                            if let Some(obs) = self.observer.as_ref() {
                                obs.on_event(&super::util::observe::Event::complete(
                                    Surface::Content,
                                    "GET",
                                    initial_url_arc.clone(),
                                    200,
                                    started.elapsed(),
                                ));
                            }
                            let direct_bytes = response.bytes().await?;
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
            other => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "HEAD",
                        initial_url_arc.clone(),
                        Some(other.as_u16()),
                        Some(started.elapsed()),
                        ErrorClass::Http,
                    ));
                }
                Err(Error::UnexpectedStatus(other))
            }
        }
    }

    async fn direct_resolve_redirect(&self, url: &str, timestamp: &str) -> Result<String, Error> {
        if let Some(pacer) = self.pacer.as_ref() {
            pacer.pace_content().await;
        }
        let req_url: Arc<str> = Arc::from(Self::wayback_url(url, timestamp, true));
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::start(
                Surface::Content,
                "HEAD",
                req_url.clone(),
            ));
        }
        let started = Instant::now();
        let response = self
            .client
            .head(req_url.as_ref())
            .send()
            .await
            .map_err(|e| {
                if let Some(obs) = self.observer.as_ref() {
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else if e.is_connect() {
                        ErrorClass::Connect
                    } else {
                        ErrorClass::Other
                    };
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "HEAD",
                        req_url.clone(),
                        None,
                        Some(started.elapsed()),
                        class,
                    ));
                }
                Error::Client(e)
            })?;

        match response.status() {
            StatusCode::FOUND => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::complete(
                        Surface::Content,
                        "HEAD",
                        req_url.clone(),
                        StatusCode::FOUND.as_u16(),
                        started.elapsed(),
                    ));
                }
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
            other => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "HEAD",
                        req_url.clone(),
                        Some(other.as_u16()),
                        Some(started.elapsed()),
                        ErrorClass::Http,
                    ));
                }
                Err(Error::UnexpectedStatus(other))
            }
        }
    }

    pub async fn resolve_redirect_shallow(
        &self,
        url: &str,
        timestamp: &str,
        expected_digest: &str,
    ) -> Result<(UrlInfo, String, bool), Error> {
        let initial_url = Self::wayback_url(url, timestamp, true);
        if let Some(pacer) = self.pacer.as_ref() {
            pacer.pace_content().await;
        }
        let initial_url_arc: Arc<str> = Arc::from(initial_url.clone());
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::start(
                Surface::Content,
                "HEAD",
                initial_url_arc.clone(),
            ));
        }
        let started = Instant::now();
        let initial_response = self
            .client
            .head(initial_url_arc.as_ref())
            .send()
            .await
            .map_err(|e| {
                if let Some(obs) = self.observer.as_ref() {
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else if e.is_connect() {
                        ErrorClass::Connect
                    } else {
                        ErrorClass::Other
                    };
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "HEAD",
                        initial_url_arc.clone(),
                        None,
                        Some(started.elapsed()),
                        class,
                    ));
                }
                Error::Client(e)
            })?;

        match initial_response.status() {
            StatusCode::FOUND => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::complete(
                        Surface::Content,
                        "HEAD",
                        initial_url_arc.clone(),
                        StatusCode::FOUND.as_u16(),
                        started.elapsed(),
                    ));
                }
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
                            if let Some(pacer) = self.pacer.as_ref() {
                                pacer.pace_content().await;
                            }
                            if let Some(obs) = self.observer.as_ref() {
                                obs.on_event(&super::util::observe::Event::start(
                                    Surface::Content,
                                    "GET",
                                    initial_url_arc.clone(),
                                ));
                            }
                            let started = Instant::now();
                            let response = self
                                .client
                                .get(initial_url_arc.as_ref())
                                .send()
                                .await
                                .map_err(|e| {
                                if let Some(obs) = self.observer.as_ref() {
                                    let class = if e.is_timeout() {
                                        ErrorClass::Timeout
                                    } else if e.is_connect() {
                                        ErrorClass::Connect
                                    } else {
                                        ErrorClass::Other
                                    };
                                    obs.on_event(&super::util::observe::Event::error(
                                        Surface::Content,
                                        "GET",
                                        initial_url_arc.clone(),
                                        None,
                                        Some(started.elapsed()),
                                        class,
                                    ));
                                }
                                Error::Client(e)
                            })?;
                            let status = response.status();
                            if status != StatusCode::OK {
                                if let Some(obs) = self.observer.as_ref() {
                                    obs.on_event(&super::util::observe::Event::error(
                                        Surface::Content,
                                        "GET",
                                        initial_url_arc.clone(),
                                        Some(status.as_u16()),
                                        Some(started.elapsed()),
                                        ErrorClass::Http,
                                    ));
                                }
                                return Err(Error::UnexpectedStatus(status));
                            }
                            if let Some(obs) = self.observer.as_ref() {
                                obs.on_event(&super::util::observe::Event::complete(
                                    Surface::Content,
                                    "GET",
                                    initial_url_arc.clone(),
                                    200,
                                    started.elapsed(),
                                ));
                            }
                            let direct_bytes = response.bytes().await?;
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
            other => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "HEAD",
                        initial_url_arc.clone(),
                        Some(other.as_u16()),
                        Some(started.elapsed()),
                        ErrorClass::Http,
                    ));
                }
                Err(Error::UnexpectedStatus(other))
            }
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
        if let Some(pacer) = self.pacer.as_ref() {
            pacer.pace_content().await;
        }
        let req_url: Arc<str> = Arc::from(Self::wayback_url(url, timestamp, original));
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::start(
                Surface::Content,
                "GET",
                req_url.clone(),
            ));
        }
        let started = Instant::now();
        let response = self
            .client
            .get(req_url.as_ref())
            .send()
            .await
            .map_err(|e| {
                if let Some(obs) = self.observer.as_ref() {
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else if e.is_connect() {
                        ErrorClass::Connect
                    } else {
                        ErrorClass::Other
                    };
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "GET",
                        req_url.clone(),
                        None,
                        Some(started.elapsed()),
                        class,
                    ));
                }
                Error::Client(e)
            })?;

        match response.status() {
            StatusCode::OK => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::complete(
                        Surface::Content,
                        "GET",
                        req_url.clone(),
                        200,
                        started.elapsed(),
                    ));
                }
                Ok(response.bytes().await?)
            }
            other => {
                if let Some(obs) = self.observer.as_ref() {
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Content,
                        "GET",
                        req_url.clone(),
                        Some(other.as_u16()),
                        Some(started.elapsed()),
                        ErrorClass::Http,
                    ));
                }
                Err(Error::UnexpectedStatus(other))
            }
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
