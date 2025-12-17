use super::{
    item,
    util::{
        observe::{ErrorClass, Observer, Surface},
        retry_future, Pacer, Retryable,
    },
    Item,
};
use futures::{Stream, TryStreamExt};
use reqwest::{header::USER_AGENT, Client};
use std::io::{BufReader, Read};
use std::sync::Arc;
use std::time::Instant;
use std::time::Duration;
use thiserror::Error;
use tryhard::RetryPolicy;

const TCP_KEEPALIVE_SECS: u64 = 20;
const DEFAULT_CDX_BASE: &str = "http://web.archive.org/cdx/search/cdx";
const CDX_OPTIONS: &str = "&output=json&fl=original,timestamp,digest,mimetype,length,statuscode";
const BLOCKED_SITE_ERROR_MESSAGE: &str =
        "org.archive.util.io.RuntimeIOException: org.archive.wayback.exception.AdministrativeAccessControlException: Blocked Site Error\n";

#[derive(Error, Debug)]
pub enum Error {
    #[error("Item parsing error: {0}")]
    ItemParsingError(#[from] item::Error),
    #[error("HTTP client error: {0}")]
    HttpClientError(#[from] reqwest::Error),
    #[error("JSON decoding error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Blocked query: {0}")]
    BlockedQuery(String),
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
            Error::HttpClientError(_) => Some(RetryPolicy::Delay(Duration::from_secs(30))),
            // The CDX server occasionally returns an empty body that results in a JSON parsing
            // failure.
            Error::JsonError(_) => Some(RetryPolicy::Delay(Duration::from_secs(30))),
            _ => Some(RetryPolicy::Break),
        }
    }
}

pub struct IndexClient {
    base: String,
    underlying: Client,
    pacer: Option<Arc<Pacer>>,
    user_agent: Option<String>,
    observer: Option<Arc<dyn Observer>>,
}

impl IndexClient {
    pub fn new(base: String) -> Result<Self, Error> {
        Ok(Self {
            base,
            underlying: Client::builder()
                .tcp_keepalive(Some(Duration::from_secs(TCP_KEEPALIVE_SECS)))
                .build()?,
            pacer: None,
            // Default User-Agent to avoid intermittent 400 HTML responses from CDX
            // when requests omit a UA header.
            user_agent: Some(format!("wayback-rs/{}", env!("CARGO_PKG_VERSION"))),
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

    /// Attach an opt-in User-Agent header to all CDX requests.
    ///
    /// This overrides the default `wayback-rs/<version>` header.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Disable sending a User-Agent header on CDX requests.
    pub fn without_user_agent(mut self) -> Self {
        self.user_agent = None;
        self
    }

    fn decode_rows(rows: Vec<Vec<String>>) -> Result<Vec<Item>, Error> {
        rows.into_iter()
            .skip(1)
            .map(|row| {
                Item::parse_optional_record(
                    row.first().map(|v| v.as_str()),
                    row.get(1).map(|v| v.as_str()),
                    row.get(2).map(|v| v.as_str()),
                    row.get(3).map(|v| v.as_str()),
                    row.get(4).map(|v| v.as_str()),
                    row.get(5).map(|v| v.as_str()),
                )
                .map_err(From::from)
            })
            .collect()
    }

    pub fn load_json<R: Read>(reader: R) -> Result<Vec<Item>, Error> {
        let buffered = BufReader::new(reader);

        let rows = serde_json::from_reader::<BufReader<R>, Vec<Vec<String>>>(buffered)?;

        Self::decode_rows(rows)
    }

    pub fn stream_search<'a>(
        &'a self,
        query: &'a str,
        limit: usize,
    ) -> impl Stream<Item = Result<Item, Error>> + 'a {
        futures::stream::try_unfold(Some(None), move |resume_key| async move {
            let next = match resume_key {
                Some(key) => {
                    let (items, resume_key) =
                        retry_future(|| self.search_with_resume_key(query, limit, &key)).await?;

                    log::info!("Resume key: {:?}", resume_key);

                    Some((items, resume_key.map(Some)))
                }
                None => None,
            };

            let result: Result<_, Error> = Ok(next);
            result
        })
        .map_ok(|items| futures::stream::iter(items.into_iter().map(Ok)))
        .try_flatten()
    }

    async fn search_with_resume_key(
        &self,
        query: &str,
        limit: usize,
        resume_key: &Option<String>,
    ) -> Result<(Vec<Item>, Option<String>), Error> {
        let resume_key_param = resume_key
            .as_ref()
            .map(|key| format!("&resumeKey={}", key))
            .unwrap_or_default();
        let query_url = format!(
            "{}?url={}{}&limit={}&showResumeKey=true{}",
            self.base, query, resume_key_param, limit, CDX_OPTIONS
        );
        log::info!("Search URL: {}", query_url);
        let url_arc: Arc<str> = Arc::from(query_url.clone());
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::start(
                Surface::Cdx,
                "GET",
                url_arc.clone(),
            ));
        }
        if let Some(pacer) = self.pacer.as_ref() {
            pacer.pace_cdx().await;
        }
        let mut req = self.underlying.get(&query_url);
        if let Some(ua) = self.user_agent.as_ref() {
            req = req.header(USER_AGENT, ua);
        }
        let started = Instant::now();
        let response = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                if let Some(obs) = self.observer.as_ref() {
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else if e.is_connect() {
                        ErrorClass::Connect
                    } else {
                        ErrorClass::Other
                    };
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Cdx,
                        "GET",
                        url_arc.clone(),
                        None,
                        Some(started.elapsed()),
                        class,
                    ));
                }
                return Err(Error::HttpClientError(e));
            }
        };
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let contents = response.text().await?;
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::complete(
                Surface::Cdx,
                "GET",
                url_arc.clone(),
                status.as_u16(),
                started.elapsed(),
            ));
        }

        if contents == BLOCKED_SITE_ERROR_MESSAGE {
            Err(Error::BlockedQuery(query.to_string()))
        } else {
            let mut rows = match serde_json::from_str::<Vec<Vec<String>>>(&contents) {
                Ok(v) => v,
                Err(e) => {
                    // Keep diagnostics low-noise: only log details at debug, and only on JSON
                    // failures. The retry logger already emits WARN-level messages.
                    let preview_len = contents.len().min(300);
                    let preview = &contents[..preview_len];
                    log::debug!(
                        "CDX response was not valid JSON (status: {}, content-type: {:?}, body_preview: {:?})",
                        status,
                        content_type,
                        preview
                    );
                    if let Some(obs) = self.observer.as_ref() {
                        obs.on_event(&super::util::observe::Event::error(
                            Surface::Cdx,
                            "GET",
                            url_arc.clone(),
                            Some(status.as_u16()),
                            Some(started.elapsed()),
                            ErrorClass::Decode,
                        ));
                    }
                    return Err(Error::JsonError(e));
                }
            };
            let len = rows.len();
            let next_resume_key = if len >= 2 && rows[len - 2].is_empty() {
                let mut last = rows.remove(len - 1);
                rows.remove(len - 2);
                Some(last.remove(0))
            } else {
                None
            };
            log::info!("Rows received {}", rows.len());

            Self::decode_rows(rows).map(|items| (items, next_resume_key))
        }
    }

    pub async fn search(
        &self,
        query: &str,
        timestamp: Option<&str>,
        digest: Option<&str>,
    ) -> Result<Vec<Item>, Error> {
        let mut filter = String::new();

        if let Some(value) = timestamp {
            filter.push_str(&format!("&filter=timestamp:{}", value));
        }

        if let Some(value) = digest {
            filter.push_str(&format!("&filter=digest:{}", value));
        }

        let query_url = format!("{}?url={}{}{}", self.base, query, filter, CDX_OPTIONS);
        if let Some(pacer) = self.pacer.as_ref() {
            pacer.pace_cdx().await;
        }
        let url_arc: Arc<str> = Arc::from(query_url.clone());
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::start(
                Surface::Cdx,
                "GET",
                url_arc.clone(),
            ));
        }
        let mut req = self.underlying.get(&query_url);
        if let Some(ua) = self.user_agent.as_ref() {
            req = req.header(USER_AGENT, ua);
        }
        let started = Instant::now();
        let response = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                if let Some(obs) = self.observer.as_ref() {
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else if e.is_connect() {
                        ErrorClass::Connect
                    } else {
                        ErrorClass::Other
                    };
                    obs.on_event(&super::util::observe::Event::error(
                        Surface::Cdx,
                        "GET",
                        url_arc.clone(),
                        None,
                        Some(started.elapsed()),
                        class,
                    ));
                }
                return Err(Error::HttpClientError(e));
            }
        };
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let contents = response.text().await?;
        if let Some(obs) = self.observer.as_ref() {
            obs.on_event(&super::util::observe::Event::complete(
                Surface::Cdx,
                "GET",
                url_arc.clone(),
                status.as_u16(),
                started.elapsed(),
            ));
        }

        if contents == BLOCKED_SITE_ERROR_MESSAGE {
            Err(Error::BlockedQuery(query.to_string()))
        } else {
            let rows = match serde_json::from_str(&contents) {
                Ok(v) => v,
                Err(e) => {
                    let preview_len = contents.len().min(300);
                    let preview = &contents[..preview_len];
                    log::debug!(
                        "CDX response was not valid JSON (status: {}, content-type: {:?}, body_preview: {:?})",
                        status,
                        content_type,
                        preview
                    );
                    if let Some(obs) = self.observer.as_ref() {
                        obs.on_event(&super::util::observe::Event::error(
                            Surface::Cdx,
                            "GET",
                            url_arc.clone(),
                            Some(status.as_u16()),
                            Some(started.elapsed()),
                            ErrorClass::Decode,
                        ));
                    }
                    return Err(Error::JsonError(e));
                }
            };
            Self::decode_rows(rows)
        }
    }
}

impl Default for IndexClient {
    fn default() -> Self {
        Self::new(DEFAULT_CDX_BASE.to_string()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::IndexClient;
    use std::fs::File;

    #[test]
    fn load_json() {
        let file = File::open("examples/wayback/cdx-result.json").unwrap();
        let result = IndexClient::load_json(file).unwrap();

        assert_eq!(result.len(), 37);
    }
}
