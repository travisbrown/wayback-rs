use clap::{crate_authors, crate_version, Clap};
use futures::TryStreamExt;
use simplelog::LevelFilter;
use std::io::Read;
use wayback_store::valid::ValidStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    let store = ValidStore::new(opts.path);

    /*println!(
        "{:?}",
        store
            //.paths_for_prefix_stream(&opts.prefix.unwrap_or("".to_string()))
            //.try_fold(0, |acc, _| futures::future::ok(acc + 1)).await?
            .paths_for_prefix(&opts.prefix.unwrap_or("".to_string()))
            .take(10).collect::<wayback_store::valid::Result<Vec<_>>>()?
    );*/
    store
        .paths_for_prefix(&opts.prefix.unwrap_or("".to_string()))
        .for_each(|p| println!("{}", p.unwrap().0));

    Ok(())
}

#[derive(Clap)]
#[clap(name = "test", version = crate_version!(), author = crate_authors!())]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Level of parallelism
    #[clap(short, long, default_value = "6")]
    parallelism: usize,
    /// Path
    #[clap(short, long)]
    path: String,
    /// Prefix
    #[clap(long)]
    prefix: Option<String>,
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
