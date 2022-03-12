use chrono::{naive::serde::ts_seconds, NaiveDateTime};
use clap::Parser;
//use datafusion::prelude::*;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
struct SnapshotUrl {
    #[serde(with = "ts_seconds")]
    archived_at: NaiveDateTime,
    url: String,
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
struct SnapshotUrlSet {
    snapshot_urls: Vec<SnapshotUrl>,
}

impl SnapshotUrlSet {
    fn singleton(archived_at: NaiveDateTime, url: String) -> Self {
        Self {
            snapshot_urls: vec![SnapshotUrl { archived_at, url }],
        }
    }

    fn add(&mut self, snapshot_url: SnapshotUrl) {
        if !self.snapshot_urls.contains(&snapshot_url) {
            self.snapshot_urls.push(snapshot_url);
        }
    }

    fn add_all(&mut self, other: SnapshotUrlSet) {
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
        SubCommand::ToDb { input, output } => {
            let tree = sled::Config::default()
                .path(output)
                .use_compression(true)
                .open()?;
            tree.set_merge_operator(SnapshotUrlSet::merge);

            let file = std::fs::File::open(input)?;
            let reader = SerializedFileReader::new(file)?;

            for row in reader.get_row_iter(None)? {
                let mut url = row.get_string(0)?;
                /*if url.starts_with("https://twitter.com/") {
                    url.replace_range(..20, "");
                }*/
                let archived_at = NaiveDateTime::from_timestamp(row.get_int(1)? as i64, 0);
                let digest = row.get_string(2)?;

                if url.len() < 100 {
                    tree.merge(
                        digest,
                        bincode::serialize(&SnapshotUrlSet::singleton(archived_at, url.clone()))?,
                    )?;
                } else {
                    log::warn!("Skipping URL for {}: {}", digest, url);
                }
            }
        }
    };

    Ok(())
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
