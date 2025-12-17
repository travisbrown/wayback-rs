use chrono::naive::NaiveDateTime;
use futures::future::BoxFuture;
use std::sync::Arc;

mod retries;
pub use retries::{retry_future, Retryable};

pub mod observe;

const DATE_FMT: &str = "%Y%m%d%H%M%S";

/// Opt-in request pacing hooks.
///
/// This is intentionally minimal and purely additive: unless callers explicitly
/// attach a `Pacer` to `IndexClient` / `Downloader`, there is **no** behavior
/// change for existing users.
///
/// A `Pacer` provides separate hooks for the CDX API surface and for content
/// retrieval. Each hook is an async closure that is awaited immediately before
/// the underlying HTTP request is sent.
#[derive(Clone)]
pub struct Pacer {
    cdx: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
    content: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
}

impl Pacer {
    /// Construct a pacer from two async closures.
    pub fn new<CF, CFFut, DF, DFFut>(cdx: CF, content: DF) -> Self
    where
        CF: Fn() -> CFFut + Send + Sync + 'static,
        CFFut: futures::Future<Output = ()> + Send + 'static,
        DF: Fn() -> DFFut + Send + Sync + 'static,
        DFFut: futures::Future<Output = ()> + Send + 'static,
    {
        Self {
            cdx: Arc::new(move || Box::pin(cdx())),
            content: Arc::new(move || Box::pin(content())),
        }
    }

    /// A pacer that performs no pacing.
    pub fn noop() -> Self {
        Self::new(|| async {}, || async {})
    }

    /// Await the CDX pacing hook.
    pub async fn pace_cdx(&self) {
        (self.cdx)().await
    }

    /// Await the content pacing hook.
    pub async fn pace_content(&self) {
        (self.content)().await
    }
}

/// Parse a 14-digit Wayback Machine timestamp into a date-time value.
pub fn parse_timestamp(input: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(input, DATE_FMT).ok()
}

/// Encode a date-time value as a 14-digit Wayback Machine timestamp.
pub fn to_timestamp(input: &NaiveDateTime) -> String {
    input.format(DATE_FMT).to_string()
}

pub mod redirect {
    /// Attempt to guess the contents of a redirect page stored by the Wayback
    /// Machine.
    ///
    /// When an item is listed as a 302 redirect in CDX results, the content of
    /// the page usually (but not always) has the following format, where the
    /// URL is the value of the location header.
    pub fn guess_redirect_content(url: &str) -> String {
        format!(
            "<html><body>You are being <a href=\"{}\">redirected</a>.</body></html>",
            url
        )
    }
}
