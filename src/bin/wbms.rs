use clap::Parser;
use log::LevelFilter;
use std::collections::HashSet;
use wayback_rs::store::data::Store;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    match opts.command {
        Command::Digests { prefix } => {
            if let Some(base) = opts.base {
                let store = Store::new(base);

                for res in store.paths_for_prefix(&prefix.unwrap_or_default()) {
                    let (digest, _) = res?;
                    println!("{}", digest);
                }
            } else {
                panic!("Must provide directory to list digests")
            }
        }
        Command::Download {
            query,
            twitter,
            known,
            parallelism,
        } => {
            let session = if let Some(base) = opts.base {
                wayback_rs::session::Session::new(base, known, parallelism)
            } else {
                wayback_rs::session::Session::new_timestamped(known, parallelism)
            }?;

            if let Some(query) = query {
                let queries = expand_queries(&query, twitter);
                session.save_cdx_results(&queries).await?;
                session.resolve_redirects().await?;
                let (success_count, invalid_count, skipped_count, error_count) =
                    session.download_items().await?;

                log::info!("Successfully downloaded: {}", success_count);
                log::info!("Downloaded by invalid hash: {}", invalid_count);
                log::info!("Skipped: {}", skipped_count);
                log::info!("Failed: {}", error_count);
            } else {
                session.resolve_redirects().await?;
                let (success_count, invalid_count, skipped_count, error_count) =
                    session.download_items().await?;

                log::info!("Successfully downloaded: {}", success_count);
                log::info!("Downloaded by invalid hash: {}", invalid_count);
                log::info!("Skipped: {}", skipped_count);
                log::info!("Failed: {}", error_count);
            }
        }
    };

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Logging initialization error")]
    LogInit(#[from] log::SetLoggerError),
    #[error("Store error")]
    Store(#[from] wayback_rs::store::data::Error),
    #[error("Session error")]
    Session(#[from] wayback_rs::session::Error),
}

#[derive(Parser)]
#[clap(name = "wbms", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// The base directory path
    #[clap(long)]
    base: Option<String>,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    /// Print the digests in the given store to stdout
    Digests {
        /// The digest search prefix
        #[clap(long, short)]
        prefix: Option<String>,
    },
    Download {
        /// The query to search for (if not provided, will resume processing)
        #[clap(long, short)]
        query: Option<String>,
        /// The query is a comma-separated list of Twitter screen names
        #[clap(long)]
        twitter: bool,
        /// Known digests file path
        #[clap(long)]
        known: Option<String>,
        /// Level of parallelism
        #[clap(long, default_value = "6")]
        parallelism: usize,
    },
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

fn expand_queries(query: &str, twitter: bool) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut result = Vec::with_capacity(1);

    for raw_part in query.split(',') {
        let trimmed = raw_part.trim();
        let cleaned = trimmed.to_lowercase();

        if !trimmed.is_empty() && !seen.contains(&cleaned) {
            seen.insert(cleaned);

            if twitter {
                result.extend(expand_twitter_queries(trimmed));
            } else {
                result.push(trimmed.to_string());
            }
        }
    }

    result
}

fn expand_twitter_queries(screen_name: &str) -> Vec<String> {
    vec![
        format!("https://twitter.com/{}", screen_name),
        format!("https://mobile.twitter.com/{}", screen_name),
        format!("https://twitter.com/{}/*", screen_name),
        format!("https://mobile.twitter.com/{}/*", screen_name),
    ]
}
