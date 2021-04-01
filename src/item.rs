use chrono::NaiveDateTime;
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Information about a single archived snapshot of a page
pub struct Item {
    pub url: String,
    pub archived_at: NaiveDateTime,
    pub digest: String,
    pub mime_type: String,
    pub length: u64,
    pub status: Option<u16>,
}

impl Item {
    const DATE_FMT: &'static str = "%Y%m%d%H%M%S";

    pub fn new(
        url: String,
        archived_at: NaiveDateTime,
        digest: String,
        mime_type: String,
        length: u64,
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
        self.archived_at.format(Item::DATE_FMT).to_string()
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

    fn parse(
        url: &str,
        timestamp: &str,
        digest: &str,
        mime_type: &str,
        length: &str,
        status: &str,
    ) -> Result<Item, Error> {
        let archived_at =
            NaiveDateTime::parse_from_str(&timestamp, Item::DATE_FMT).map_err(|_| {
                Error::InvalidTimestamp {
                    value: timestamp.to_string(),
                }
            })?;

        let length_parsed = length.parse::<u64>().map_err(|_| Error::InvalidLength {
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

    pub fn parse_optional(
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
}
