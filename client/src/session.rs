use super::{
    cdx::{self, IndexClient},
    digest::compute_digest,
    Item,
};
use bytes::Buf;
use chrono::Utc;
use csv::{ReaderBuilder, WriterBuilder};
use flate2::{Compression, GzBuilder};
use futures::{StreamExt, TryStreamExt};
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0:?}")]
    IOError(#[from] std::io::Error),
    #[error("CDX error: {0:?}")]
    IndexClientError(#[from] cdx::Error),
    #[error("CSV writing error: {0:?}")]
    CsvError(#[from] csv::Error),
    #[error("Item parsing error: {0:?}")]
    ItemError(#[from] super::item::Error),
}

pub struct Session {
    base: PathBuf,
    known_digests: Option<PathBuf>,
    parallelism: usize,
    index_client: IndexClient,
    client: super::downloader::Downloader,
}

impl Session {
    const TIMESTAMP_FMT: &'static str = "%Y%m%d%H%M%S";

    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        base: P1,
        known_digests: Option<P2>,
        parallelism: usize,
    ) -> Session {
        Session {
            base: base.as_ref().to_path_buf(),
            known_digests: known_digests.map(|path| path.as_ref().to_path_buf()),
            parallelism,
            index_client: IndexClient::default(),
            client: super::downloader::Downloader::default(),
        }
    }

    pub fn new_timestamped<P: AsRef<Path>>(
        known_digests: Option<P>,
        parallelism: usize,
    ) -> Session {
        Self::new(
            Utc::now().format(Self::TIMESTAMP_FMT).to_string(),
            known_digests,
            parallelism,
        )
    }

    pub async fn save_cdx_results(&self, queries: &[String]) -> Result<(), Error> {
        create_dir_all(&self.base)?;
        let mut query_log = File::create(self.base.join("queries.txt"))?;
        query_log.write_all(queries.join("\n").as_bytes())?;

        let mut items: Vec<Item> = futures::stream::iter(queries.iter())
            .map(|query| Ok(self.index_client.search(query)))
            .try_buffer_unordered(self.parallelism)
            .map_ok(|items| {
                futures::stream::iter(items).map(|item| {
                    let res: Result<Item, Error> = Ok(item);
                    res
                })
            })
            .try_flatten()
            .err_into::<Error>()
            .try_collect()
            .await?;

        items.sort();
        items.dedup();

        let originals_item_log = File::create(self.base.join("originals.csv"))?;
        let redirects_item_log = File::create(self.base.join("redirects.csv"))?;

        let mut originals_csv = WriterBuilder::new().from_writer(originals_item_log);
        let mut redirects_csv = WriterBuilder::new().from_writer(redirects_item_log);

        for item in &items {
            if item.status == Some(302) {
                redirects_csv.write_record(item.to_record())?;
            } else {
                originals_csv.write_record(item.to_record())?;
            }
        }

        Ok(())
    }

    pub async fn resolve_redirects(&self) -> Result<(), Error> {
        let redirects_item_log = File::open(self.base.join("redirects.csv"))?;
        let items = Self::read_csv(redirects_item_log)?;

        create_dir_all(&self.base.join("data"))?;

        let results = futures::stream::iter(items.iter())
            .map(|item| async move {
                (
                    item,
                    self.client
                        .download_redirect(&item.url, &item.timestamp())
                        .await,
                )
            })
            .buffer_unordered(self.parallelism)
            .map(|(item, result)| async move {
                let (first_url, content) = result.map_err(|_| item)?;
                if compute_digest(&mut content.clone().reader()).unwrap() == item.digest {
                    let first_url_info = first_url
                        .parse::<super::item::UrlInfo>()
                        .map_err(|_| item)?;

                    let actual_url = self
                        .client
                        .resolve_redirect(&first_url_info.url, &first_url_info.timestamp)
                        .await
                        .map_err(|_| item)?;

                    let actual_url_info = actual_url
                        .parse::<super::item::UrlInfo>()
                        .map_err(|_| item)?;

                    let items = self
                        .index_client
                        .search(&actual_url_info.url)
                        .await
                        .map_err(|_| item)?;

                    let actual_item = items
                        .into_iter()
                        .find(|item| item.timestamp() == actual_url_info.timestamp)
                        .ok_or(item)?;

                    let output =
                        File::create(self.base.join("data").join(format!("{}.gz", item.digest)))
                            .map_err(|_| item)?;
                    let mut gz = GzBuilder::new()
                        .filename(item.make_filename())
                        .write(output, Compression::default());
                    gz.write_all(&content).map_err(|_| item)?;
                    gz.finish().map_err(|_| item)?;

                    Ok(actual_item)
                } else {
                    Err(item)
                }
            })
            .buffer_unordered(self.parallelism)
            .collect::<Vec<_>>()
            .await;

        create_dir_all(&self.base.join("errors"))?;

        let redirects_error_log = File::create(self.base.join("errors").join("redirects.csv"))?;
        let mut redirects_error_csv = WriterBuilder::new().from_writer(redirects_error_log);

        let extras_item_log = File::create(self.base.join("extras.csv"))?;
        let mut extras_item_csv = WriterBuilder::new().from_writer(extras_item_log);

        for result in results {
            match result {
                Ok(item) => {
                    extras_item_csv.write_record(item.to_record())?;
                }
                Err(item) => {
                    redirects_error_csv.write_record(item.to_record())?;
                }
            }
        }

        Ok(())
    }

    pub async fn download_items(&self) -> Result<(usize, usize, usize), Error> {
        let originals_file = File::open(self.base.join("originals.csv"))?;
        let mut items = Self::read_csv(originals_file)?;

        let extras_file = File::open(self.base.join("extras.csv"))?;
        items.extend(Self::read_csv(extras_file)?);

        let total_count = items.len();

        let mut digests = HashSet::new();

        items.retain(|item| digests.insert(item.digest.clone()));

        if let Some(path) = &self.known_digests {
            let file = File::open(path)?;
            for line in BufReader::new(file).lines() {
                digests.remove(line?.trim());
            }
        }

        items.retain(|item| digests.contains(&item.digest));

        let results = futures::stream::iter(items)
            .map(|item| async {
                let content = self
                    .client
                    .download_item(&item)
                    .await
                    .map_err(|_| item.clone())?;

                if compute_digest(&mut content.clone().reader()).unwrap() == item.digest {
                    let output =
                        File::create(self.base.join("data").join(format!("{}.gz", item.digest)))
                            .map_err(|_| item.clone())?;
                    let mut gz = GzBuilder::new()
                        .filename(item.make_filename())
                        .write(output, Compression::default());
                    gz.write_all(&content).map_err(|_| item.clone())?;
                    gz.finish().map_err(|_| item)?;

                    Ok(())
                } else {
                    Err(item)
                }
            })
            .buffer_unordered(self.parallelism)
            .collect::<Vec<_>>()
            .await;

        let error_log = File::create(self.base.join("errors").join("items.csv"))?;
        let mut error_csv = WriterBuilder::new().from_writer(error_log);
        let mut success_count = 0;
        let mut error_count = 0;

        for result in results {
            match result {
                Ok(_) => {
                    success_count += 1;
                }
                Err(item) => {
                    error_count += 1;
                    error_csv.write_record(item.to_record())?;
                }
            }
        }

        Ok((
            success_count,
            total_count - success_count - error_count,
            error_count,
        ))
    }

    fn read_csv<R: Read>(reader: R) -> Result<Vec<Item>, Error> {
        let mut csv_reader = ReaderBuilder::new().has_headers(false).from_reader(reader);

        csv_reader
            .records()
            .map(|record| {
                let row = record?;
                Ok(Item::parse_optional_record(
                    row.get(0),
                    row.get(1),
                    row.get(2),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                )?)
            })
            .collect()
    }
}
