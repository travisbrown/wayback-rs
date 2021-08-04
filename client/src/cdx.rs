use super::item::{self, Item};
use reqwest::Client;
use std::io::{BufReader, Read};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Item parsing error: {0}")]
    ItemParsingError(#[from] item::Error),
    #[error("HTTP client error: {0}")]
    HttpClientError(#[from] reqwest::Error),
    #[error("JSON decoding error: {0}")]
    JsonError(#[from] serde_json::Error),
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
        let rows = self
            .underlying
            .get(&query_url)
            .send()
            .await?
            .json::<Vec<Vec<String>>>()
            .await?;

        Self::decode_rows(rows)
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
        let file = File::open("../examples/wayback/cdx-result.json").unwrap();
        let result = IndexClient::load_json(file).unwrap();

        assert_eq!(result.len(), 37);
    }
}
