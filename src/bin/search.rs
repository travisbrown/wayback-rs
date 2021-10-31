use clap::{crate_authors, crate_version, Parser};
use futures::stream::TryStreamExt;
use futures_locks::Mutex;
use simplelog::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    let client = wayback_rs::cdx::IndexClient::default();

    let mut queries = opts
        .query
        .split(',')
        .flat_map(|screen_name| expand_twitter_queries(screen_name.trim()))
        .collect::<Vec<_>>();
    queries.sort();
    queries.dedup();

    let limit = 10000;
    let csv = Mutex::new(csv::WriterBuilder::new().from_writer(std::io::stdout()));

    for query in queries {
        client
            .stream_search(&query, limit, opts.resume_key.clone())
            .map_err(Error::from)
            .try_for_each(|item| {
                let m = csv.clone();
                async move {
                    let mut w = m.lock().await;

                    Ok(w.write_record(item.to_record())?)
                }
            })
            .await?;
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("CDX error: {0:?}")]
    IndexClientError(#[from] wayback_rs::cdx::Error),
    #[error("CSV writing error: {0:?}")]
    CsvError(#[from] csv::Error),
}

fn expand_twitter_queries(screen_name: &str) -> Vec<String> {
    vec![
        format!("https://twitter.com/{}", screen_name),
        format!("https://mobile.twitter.com/{}", screen_name),
        format!("https://twitter.com/{}/*", screen_name),
        format!("https://mobile.twitter.com/{}/*", screen_name),
    ]
}

#[derive(Parser)]
#[clap(name = "test", version = crate_version!(), author = crate_authors!())]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Level of parallelism
    #[clap(short, long, default_value = "6")]
    parallelism: usize,
    /// Resume key
    #[clap(short, long)]
    resume_key: Option<String>,
    /// Query
    #[clap(short, long)]
    query: String,
}

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

fn init_logging(verbosity: i32) -> Result<(), log::SetLoggerError> {
    simplelog::TermLogger::init(
        select_log_level_filter(verbosity),
        simplelog::Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
}
