use super::{
    item,
    util::{retry_future, Retryable},
    Item,
};
use futures::{Stream, TryStreamExt};
use reqwest::Client;
use std::io::{BufReader, Read};
use std::time::Duration;
use thiserror::Error;
use tryhard::RetryPolicy;

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
}

impl IndexClient {
    const TCP_KEEPALIVE_SECS: u64 = 20;
    const DEFAULT_CDX_BASE: &'static str = "http://web.archive.org/cdx/search/cdx";
    const CDX_OPTIONS: &'static str =
        "&output=json&fl=original,timestamp,digest,mimetype,length,statuscode";
    const BLOCKED_SITE_ERROR_MESSAGE: &'static str =
        "org.archive.util.io.RuntimeIOException: org.archive.wayback.exception.AdministrativeAccessControlException: Blocked Site Error\n";

    pub fn new(base: String) -> Self {
        Self {
            base,
            underlying: Client::builder()
                .tcp_keepalive(Some(Duration::from_secs(Self::TCP_KEEPALIVE_SECS)))
                .build()
                .unwrap(),
        }
    }

    fn decode_rows(rows: Vec<Vec<String>>) -> Result<Vec<Item>, Error> {
        rows.into_iter()
            .skip(1)
            .map(|row| {
                Item::parse_optional_record(
                    row.get(0).map(|v| v.as_str()),
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

                    log::warn!("{:?}", resume_key);

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
            self.base,
            query,
            resume_key_param,
            limit,
            Self::CDX_OPTIONS
        );
        log::warn!("{}", query_url);
        let contents = self.underlying.get(&query_url).send().await?.text().await?;

        if contents == Self::BLOCKED_SITE_ERROR_MESSAGE {
            Err(Error::BlockedQuery(query.to_string()))
        } else {
            let mut rows = serde_json::from_str::<Vec<Vec<String>>>(&contents)?;
            let len = rows.len();
            let next_resume_key = if rows[len - 2].is_empty() {
                let mut last = rows.remove(len - 1);
                rows.remove(len - 2);
                Some(last.remove(0))
            } else {
                None
            };
            log::warn!("received {}", rows.len());

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

        let query_url = format!("{}?url={}{}{}", self.base, query, filter, Self::CDX_OPTIONS);
        let contents = self.underlying.get(&query_url).send().await?.text().await?;

        if contents == Self::BLOCKED_SITE_ERROR_MESSAGE {
            Err(Error::BlockedQuery(query.to_string()))
        } else {
            let rows = serde_json::from_str(&contents)?;
            Self::decode_rows(rows)
        }
    }
}

impl Default for IndexClient {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CDX_BASE.to_string())
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
