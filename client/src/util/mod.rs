use chrono::naive::NaiveDateTime;

mod retries;
pub use retries::{retry_future, Retryable};

const DATE_FMT: &str = "%Y%m%d%H%M%S";

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
