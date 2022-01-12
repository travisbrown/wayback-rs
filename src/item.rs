use super::util::{parse_timestamp, to_timestamp};
use chrono::NaiveDateTime;
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Missing URL")]
    MissingUrl,
    #[error("Missing timestamp")]
    MissingTimestamp,
    #[error("Missing digest")]
    MissingDigest,
    #[error("Missing MIME type")]
    MissingMimeType,
    #[error("Missing length")]
    MissingLength,
    #[error("Missing status code")]
    MissingStatus,
    #[error("Invalid timestamp: {value}")]
    InvalidTimestamp { value: String },
    #[error("Invalid length: {value}")]
    InvalidLength { value: String },
    #[error("Invalid status code: {value}")]
    InvalidStatus { value: String },
    #[error("Invalid Wayback Machine URL: {value}")]
    InvalidWaybackUrl { value: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UrlInfo {
    pub url: String,
    pub timestamp: String,
}

impl UrlInfo {
    pub fn new(url: String, timestamp: String) -> UrlInfo {
        UrlInfo { url, timestamp }
    }
}

lazy_static::lazy_static! {
    static ref WAYBACK_URL_RE: regex::Regex = regex::Regex::new(
        r"^http(:?s)?://web.archive.org/web/(?P<timestamp>\d{14})(?:id_)?/(?P<url>.+)$",
    )
    .unwrap();
}

impl FromStr for UrlInfo {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let captures = WAYBACK_URL_RE.captures(s).ok_or(Error::InvalidWaybackUrl {
            value: s.to_string(),
        })?;

        Ok(UrlInfo::new(
            captures["url"].to_string(),
            captures["timestamp"].to_string(),
        ))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
/// Information about a single archived snapshot of a page.
pub struct Item {
    pub url: String,
    pub archived_at: NaiveDateTime,
    pub digest: String,
    pub mime_type: String,
    pub length: u32,
    pub status: Option<u16>,
}

impl Item {
    pub fn new(
        url: String,
        archived_at: NaiveDateTime,
        digest: String,
        mime_type: String,
        length: u32,
        status: Option<u16>,
    ) -> Item {
        Item {
            url,
            archived_at,
            digest,
            mime_type,
            length,
            status,
        }
    }

    pub fn wayback_url(&self, original: bool) -> String {
        format!(
            "http://web.archive.org/web/{}{}/{}",
            self.timestamp(),
            if original { "id_" } else { "if_" },
            self.url
        )
    }

    pub fn timestamp(&self) -> String {
        to_timestamp(&self.archived_at)
    }

    pub fn status_code(&self) -> String {
        self.status.map_or("-".to_string(), |v| v.to_string())
    }

    fn make_extension(&self) -> Option<String> {
        match self.mime_type.as_str() {
            "application/json" => Some("json".to_string()),
            "text/html" => Some("html".to_string()),
            _ => None,
        }
    }

    pub fn make_filename(&self) -> String {
        self.make_extension().map_or_else(
            || self.digest.clone(),
            |ext| format!("{}.{}", self.digest, ext),
        )
    }

    pub fn with_digest(&self, digest: &str) -> Item {
        let mut res = self.clone();
        res.digest = digest.to_string();
        res
    }

    fn parse(
        url: &str,
        timestamp: &str,
        digest: &str,
        mime_type: &str,
        length: &str,
        status: &str,
    ) -> Result<Item, Error> {
        let archived_at = parse_timestamp(timestamp).ok_or_else(|| Error::InvalidTimestamp {
            value: timestamp.to_string(),
        })?;

        let length_parsed = length.parse::<u32>().map_err(|_| Error::InvalidLength {
            value: length.to_string(),
        })?;

        let status_parsed = if status == "-" {
            None
        } else {
            Some(status.parse::<u16>().map_err(|_| Error::InvalidStatus {
                value: status.to_string(),
            })?)
        };

        Ok(Item::new(
            url.to_string(),
            archived_at,
            digest.to_string(),
            mime_type.to_string(),
            length_parsed,
            status_parsed,
        ))
    }

    pub fn parse_optional_record(
        url: Option<&str>,
        timestamp: Option<&str>,
        digest: Option<&str>,
        mime_type: Option<&str>,
        length: Option<&str>,
        status: Option<&str>,
    ) -> Result<Item, Error> {
        Self::parse(
            url.ok_or(Error::MissingUrl)?,
            timestamp.ok_or(Error::MissingTimestamp)?,
            digest.ok_or(Error::MissingDigest)?,
            mime_type.ok_or(Error::MissingMimeType)?,
            length.ok_or(Error::MissingLength)?,
            status.ok_or(Error::MissingStatus)?,
        )
    }

    pub fn to_record(&self) -> Vec<String> {
        vec![
            self.url.to_string(),
            self.timestamp(),
            self.digest.to_string(),
            self.mime_type.to_string(),
            self.length.to_string(),
            self.status_code(),
        ]
    }
}
