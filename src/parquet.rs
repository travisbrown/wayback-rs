use super::item::Item;
use itertools::Itertools;
use parquet::{
    basic::{Compression, Encoding},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::{FileWriter, ParquetWriter, SerializedFileWriter},
    },
    schema::{
        parser::parse_message_type,
        types::{ColumnPath, Type},
    },
};
use parquet_format::FileMetaData;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Schema column error")]
    InvalidColumns,
    #[error("Item parsing error: {0}")]
    ItemParsingError(#[from] super::item::Error),
}

pub struct ParquetFile<W: ParquetWriter> {
    writer: SerializedFileWriter<W>,
}

impl ParquetFile<File> {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = File::create(path)?;
        let schema = parse_message_type(&std::fs::read_to_string("schemas/parquet/item.schema")?)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD)
            .build();
        let writer = SerializedFileWriter::new(file, Arc::new(schema), Arc::new(props))?;

        Ok(ParquetFile { writer })
    }
}

impl<W: ParquetWriter + 'static> ParquetFile<W> {
    pub fn close(&mut self) -> Result<FileMetaData, Error> {
        Ok(self.writer.close()?)
    }

    pub fn write_all<P: AsRef<Path>>(&mut self, directory: P) -> Result<(), Error> {
        let mut prefixes = Vec::with_capacity(33);
        prefixes.extend(super::digest::DIGEST_CHARS);
        // A placeholder to represent invalid digests.
        prefixes.push('_');

        let mut paths = std::fs::read_dir(&directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        paths.sort();

        for prefix in prefixes {
            log::info!("Collecting prefix {} from {} files", prefix, paths.len());
            let mut selected_items = HashSet::new();

            for path in &paths {
                let file = File::open(path)?;

                for item in Item::iter_csv(file) {
                    let item = item?;
                    let is_valid_digest = super::digest::is_valid_digest(&item.digest);
                    if (item.digest.starts_with(prefix) && is_valid_digest)
                        || (prefix == '_' && !is_valid_digest)
                    {
                        selected_items.insert(item);
                    }
                }
            }

            let original_count = selected_items.len();

            let mut items = selected_items.into_iter().collect::<Vec<_>>();
            items.sort();

            let items = items
                .into_iter()
                .group_by(|item| (item.url.clone(), item.archived_at, item.digest.clone()))
                .into_iter()
                .map(|(_, items)| {
                    // We know this group is non-empty.
                    items.max_by_key(|item| item.length).unwrap()
                })
                .collect::<Vec<_>>();

            log::info!(
                "Found {} items, removed {} as duplicates",
                original_count,
                original_count - items.len()
            );

            self.write(&items)?;
        }

        Ok(())
    }

    pub fn write(&mut self, items: &[Item]) -> Result<(), Error> {
        let mut row_group_writer = self.writer.next_row_group()?;

        let urls = items
            .iter()
            .map(|item| ByteArray::from(item.url.as_str()))
            .collect::<Vec<_>>();

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(Error::InvalidColumns)?;

        let url_writer = match column_writer {
            ColumnWriter::ByteArrayColumnWriter(ref mut writer) => Ok(writer),
            _ => Err(Error::InvalidColumns),
        }?;

        url_writer.write_batch(&urls, None, None)?;
        row_group_writer.close_column(column_writer)?;

        let archived_ats = items
            .iter()
            .map(|item| item.archived_at.timestamp() as i32)
            .collect::<Vec<_>>();

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(Error::InvalidColumns)?;

        let archived_at_writer = match column_writer {
            ColumnWriter::Int32ColumnWriter(ref mut writer) => Ok(writer),
            _ => Err(Error::InvalidColumns),
        }?;

        archived_at_writer.write_batch(&archived_ats, None, None)?;
        row_group_writer.close_column(column_writer)?;

        let digests = items
            .iter()
            .map(|item| ByteArray::from(item.digest.as_str()))
            .collect::<Vec<_>>();

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(Error::InvalidColumns)?;

        let digest_writer = match column_writer {
            ColumnWriter::ByteArrayColumnWriter(ref mut writer) => Ok(writer),
            _ => Err(Error::InvalidColumns),
        }?;

        digest_writer.write_batch(&digests, None, None)?;
        row_group_writer.close_column(column_writer)?;

        let mime_types = items
            .iter()
            .map(|item| ByteArray::from(item.mime_type.as_str()))
            .collect::<Vec<_>>();

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(Error::InvalidColumns)?;

        let mime_type_writer = match column_writer {
            ColumnWriter::ByteArrayColumnWriter(ref mut writer) => Ok(writer),
            _ => Err(Error::InvalidColumns),
        }?;

        mime_type_writer.write_batch(&mime_types, None, None)?;
        row_group_writer.close_column(column_writer)?;

        let lengths = items
            .iter()
            .map(|item| item.length as i32)
            .collect::<Vec<_>>();

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(Error::InvalidColumns)?;

        let length_writer = match column_writer {
            ColumnWriter::Int32ColumnWriter(ref mut writer) => Ok(writer),
            _ => Err(Error::InvalidColumns),
        }?;

        length_writer.write_batch(&lengths, None, None)?;
        row_group_writer.close_column(column_writer)?;

        let mut status_values = Vec::with_capacity(items.len());
        let mut status_defs = Vec::with_capacity(items.len());

        for item in items {
            match item.status {
                Some(status) => {
                    status_values.push(status as i32);
                    status_defs.push(1);
                }
                None => {
                    status_defs.push(0);
                }
            }
        }

        let mut column_writer = row_group_writer
            .next_column()?
            .ok_or(Error::InvalidColumns)?;

        let status_writer = match column_writer {
            ColumnWriter::Int32ColumnWriter(ref mut writer) => Ok(writer),
            _ => Err(Error::InvalidColumns),
        }?;

        status_writer.write_batch(&status_values, Some(&status_defs), None)?;
        row_group_writer.close_column(column_writer)?;

        row_group_writer.close()?;
        self.writer.close_row_group(row_group_writer)?;

        Ok(())
    }
}
