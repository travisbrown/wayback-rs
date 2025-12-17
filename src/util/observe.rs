use std::sync::Arc;
use std::time::Duration;

/// High-level surface area for a Wayback request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Surface {
    Cdx,
    Content,
}

/// High-level request phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Phase {
    Start,
    Complete,
    Error,
}

/// Coarse error classification for observer consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorClass {
    Timeout,
    Connect,
    Tls,
    Protocol,
    Decode,
    /// Indicates the Wayback service is explicitly blocking the requested site/query.
    ///
    /// This is distinct from generic decode errors (e.g., HTML error pages) and is useful
    /// for long-running tools that should avoid repeatedly retrying permanently blocked
    /// targets.
    Blocked,
    Http,
    Other,
}

/// An observation emitted by the library around HTTP operations.
///
/// This type is intentionally minimal and cheap to construct. It is passed by
/// reference to observers, so consumers should copy out the fields they need.
///
/// This type is marked `non_exhaustive` to allow adding new fields/variants in
/// the future without breaking downstream code.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Event {
    pub surface: Surface,
    pub phase: Phase,
    pub method: &'static str,
    pub url: Arc<str>,
    pub status: Option<u16>,
    pub elapsed: Option<Duration>,
    pub error: Option<ErrorClass>,
    /// Escape hatch for future enrichment without changing the typed core.
    ///
    /// For now this remains a cheap static slice. A future release may add an
    /// owned key/value collection for richer data behind the same hook.
    pub extras: &'static [(&'static str, &'static str)],
}

impl Event {
    pub(crate) fn start(surface: Surface, method: &'static str, url: Arc<str>) -> Self {
        Self {
            surface,
            phase: Phase::Start,
            method,
            url,
            status: None,
            elapsed: None,
            error: None,
            extras: &[],
        }
    }

    pub(crate) fn complete(
        surface: Surface,
        method: &'static str,
        url: Arc<str>,
        status: u16,
        elapsed: Duration,
    ) -> Self {
        Self {
            surface,
            phase: Phase::Complete,
            method,
            url,
            status: Some(status),
            elapsed: Some(elapsed),
            error: None,
            extras: &[],
        }
    }

    pub(crate) fn error(
        surface: Surface,
        method: &'static str,
        url: Arc<str>,
        status: Option<u16>,
        elapsed: Option<Duration>,
        error: ErrorClass,
    ) -> Self {
        Self {
            surface,
            phase: Phase::Error,
            method,
            url,
            status,
            elapsed,
            error: Some(error),
            extras: &[],
        }
    }
}

/// Observer for request/response events.
///
/// Implementations must be fast and non-blocking. If you need async processing,
/// enqueue events into a channel and handle them in a separate task.
pub trait Observer: Send + Sync {
    fn on_event(&self, event: &Event);
}
