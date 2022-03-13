use chrono::{naive::serde::ts_seconds, NaiveDateTime};
use clap::Parser;
//use datafusion::prelude::*;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use std::fs::File;
use std::io::{BufRead, BufReader};
use wayback_rs::Item;

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
struct SnapshotUrl<'a> {
    #[serde(with = "ts_seconds")]
    archived_at: NaiveDateTime,
    url: &'a str,
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
struct SnapshotUrlSet<'a> {
    #[serde(borrow)]
    snapshot_urls: Vec<SnapshotUrl<'a>>,
}

impl<'a> SnapshotUrlSet<'a> {
    fn singleton(archived_at: NaiveDateTime, url: &'a str) -> Self {
        Self {
            snapshot_urls: vec![SnapshotUrl { archived_at, url }],
        }
    }

    fn add(&mut self, snapshot_url: SnapshotUrl<'a>) {
        if !self.snapshot_urls.contains(&snapshot_url) {
            self.snapshot_urls.push(snapshot_url);
        }
    }

    fn add_all(&mut self, other: Self) {
        for snapshot_url in other.snapshot_urls {
            self.add(snapshot_url);
        }
    }

    fn merge(_key: &[u8], old_value: Option<&[u8]>, merged_bytes: &[u8]) -> Option<Vec<u8>> {
        match old_value {
            Some(old_bytes) => {
                if let Ok((mut old_set, new_set)) =
                    bincode::deserialize::<SnapshotUrlSet>(old_bytes).and_then(|old_set| {
                        bincode::deserialize(merged_bytes).map(|new_set| (old_set, new_set))
                    })
                {
                    old_set.add_all(new_set);
                    if let Ok(bytes) = bincode::serialize(&old_set) {
                        Some(bytes)
                    } else {
                        old_value.map(|bytes| bytes.to_vec())
                    }
                } else {
                    old_value.map(|bytes| bytes.to_vec())
                }
            }
            None => Some(merged_bytes.to_vec()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    match opts.command {
        SubCommand::Import { input, output } => {
            let mut parquet = wayback_rs::parquet::ParquetFile::create(output)?;

            parquet.write_all(&input)?;
            parquet.close()?;
        }
        /*SubCommand::Search { input, query } => {
            let mut ctx = ExecutionContext::new();
            let df = ctx.read_parquet(input).await?;
            let df = df.filter(starts_with(col("url"), lit(query)))?;

            let results = df.collect().await?;

            for result in results {
                println!("{:?}", result);
            }
        }*/
        SubCommand::Extract { input, digests } => {
            let file = File::open(input)?;
            let reader = SerializedFileReader::new(file)?;
            let mut count = 0;

            let digests_file = BufReader::new(File::open(digests)?);
            let mut digests = std::collections::HashSet::new();

            for line in digests_file.lines() {
                let line = line?;
                digests.insert(line);
            }

            let mut csv = csv::WriterBuilder::new().from_writer(std::io::stdout());

            //let mut statuses: std::collections::HashMap<Option<u16>, usize> =
            //    std::collections::HashMap::new();

            for row in reader.get_row_iter(None)? {
                //let digest = row.get_string(2)?;
                let item = row_to_item(row).unwrap();

                if digests.remove(&item.digest) {
                    csv.write_record(item.to_record())?;
                }
            }
        }
        SubCommand::Redirects { input, output } => {
            let file = std::fs::File::open(input)?;
            let reader = SerializedFileReader::new(file)?;
            let mut count = 0;

            //let mut statuses: std::collections::HashMap<Option<u16>, usize> =
            //    std::collections::HashMap::new();

            for row in reader.get_row_iter(None)? {
                //let digest = row.get_string(2)?;
                let item = row_to_item(row).unwrap();

                if item.status == Some(302) {
                    println!("{},{}", item.url, item.digest);
                }
            }
        }
        SubCommand::Digests { input } => {
            let schema = parquet::schema::parser::parse_message_type(
                "
message item {
    REQUIRED BYTE_ARRAY digest (UTF8);
    REQUIRED INT32 archived_at;
    REQUIRED BYTE_ARRAY digest (UTF8);
}",
            )?;

            let file = std::fs::File::open(input)?;
            let reader = SerializedFileReader::new(file)?;
            let mut count = 0;

            for row in reader.get_row_iter(Some(schema))? {
                let digest = row.get_string(2)?;

                println!("{}", digest);
            }
        }
        SubCommand::ToDb { input, output } => {
            let schema = parquet::schema::parser::parse_message_type(
                "
message item {
    REQUIRED BYTE_ARRAY url (UTF8);
    REQUIRED INT32 archived_at;
    REQUIRED BYTE_ARRAY digest (UTF8);
}",
            )?;

            let tree = sled::Config::default()
                .path(output)
                .use_compression(true)
                .open()?;
            //tree.set_merge_operator(SnapshotUrlSet::merge);

            let file = std::fs::File::open(input)?;
            let reader = SerializedFileReader::new(file)?;
            let mut count = 0;

            for row in reader.get_row_iter(Some(schema))? {
                let url = row.get_string(0)?;
                let digest = row.get_string(2)?;
                if url.len() < 100 {
                    let archived_at = NaiveDateTime::from_timestamp(row.get_int(1)? as i64, 0);

                    tree.insert(
                        digest,
                        bincode::serialize(&SnapshotUrlSet::singleton(archived_at, url))?,
                    )?;

                    if count % 10000 == 1 {
                        log::info!("{}", count);
                    }
                    count += 1;
                } else {
                    log::warn!("Skipping URL for {}: {}", digest, url);
                }
            }
        }
        SubCommand::DownloadRedirects { input } => {
            let known_digests: Option<String> = None;
            let session = wayback_rs::session::Session::new(input, known_digests, 4)?;
            session.shallow_resolve_redirects().await?;
        }
    };

    Ok(())
}

fn row_to_item(row: parquet::record::Row) -> Option<wayback_rs::Item> {
    let columns = row.get_column_iter();
    let url = row.get_string(0).ok()?.clone();
    let archived_at = NaiveDateTime::from_timestamp(row.get_int(1).ok()? as i64, 0);
    let digest = row.get_string(2).ok()?.clone();
    let mime_type = row.get_string(3).ok()?.clone();
    let length = row.get_int(4).ok()? as u32;

    let status_field = columns.skip(5).next()?;
    let status = if *status_field.1 == parquet::record::Field::Null {
        None
    } else {
        Some(row.get_int(5).ok()? as u16)
    };

    Some(Item::new(
        url,
        archived_at,
        digest,
        mime_type,
        length,
        status,
    ))
}

#[derive(Parser)]
#[clap(name = "parquet", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// Convert a CSV file to Parquet
    Import {
        /// The input directory path
        #[clap(long)]
        input: String,
        /// The output file path
        #[clap(long)]
        output: String,
    },
    /*Search {
        /// The input file path
        #[clap(long)]
        input: String,
        /// URL prefix
        #[clap(long)]
        query: String,
    },*/
    ToDb {
        /// The input file path
        #[clap(long)]
        input: String,
        /// The output path
        #[clap(long)]
        output: String,
    },
    Digests {
        /// The input file path
        #[clap(long)]
        input: String,
    },
    Redirects {
        /// The input file path
        #[clap(long)]
        input: String,
        /// The output path
        #[clap(long)]
        output: String,
    },
    Extract {
        /// The input file path
        #[clap(long)]
        input: String,
        /// The digests file path
        #[clap(long)]
        digests: String,
    },
    DownloadRedirects {
        /// The input directory path
        #[clap(long)]
        input: String,
    },
}

use simplelog::LevelFilter;

fn select_log_level_filter(verbosity: i32) -> LevelFilter {
    match verbosity {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    }
}

pub fn init_logging(verbosity: i32) -> Result<(), log::SetLoggerError> {
    simplelog::TermLogger::init(
        select_log_level_filter(verbosity),
        simplelog::Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
}
