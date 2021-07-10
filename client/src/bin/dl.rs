use clap::{crate_authors, crate_version, Clap};
use futures::{StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use simplelog::LevelFilter;
use std::io::Read;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    let session = wayback_client::session::Session::new_timestamped(opts.parallelism);
    let queries = opts
        .query
        .split(',')
        .flat_map(|screen_name| expand_twitter_queries(screen_name.trim()))
        .collect::<Vec<_>>();
    session.save_cdx_results(&queries).await?;
    session.resolve_redirects().await?;
    let (success_count, skipped_count, error_count) = session.download_items().await?;

    println!("Successfully downloaded: {}\nSkipped: {}\nFailed: {}", success_count, skipped_count, error_count);

    /*let index = wayback_client::cdx::IndexClient::default();
    let client = wayback_client::Downloader::default();

    let items = index.search(&opts.query).await?;
    let count = items.len();

    let pb = Arc::new(ProgressBar::new(count as u64));
    let pb1 = pb.clone();
    let pb2 = pb.clone();
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta})")
        .progress_chars("#>-"));

    let results = futures::stream::iter(items.into_iter().map(move |item| {
        let client = client.clone();
        let pb = pb2.clone();
        async move {
            pb.set_message(item.url.clone());
            (item.clone(), client.download_item(&item, true).await)
        }
    }))
    .buffer_unordered(opts.parallelism);

    results
        .for_each(move |(item, res)| {
            let pb = pb.clone();
            async move {
                pb.inc(1);
                match res {
                    Ok(wayback_client::downloader::Content::Direct { .. }) => {
                        pb.println(format!("direct:   {}", item.url))
                    }
                    Ok(wayback_client::downloader::Content::Redirect { location, .. }) => {
                        pb.println(format!("redirect: {} {}", item.url, location))
                    }
                    Err(err) => pb.println(format!("error:    {}", err)),
                }
            }
        })
        .await;
    pb1.finish_with_message("done!");*/

    //client.download_raw(&opts.query, true).await?;
    //client.download_raw(&opts.query, false).await?;

    Ok(())
}

fn expand_twitter_queries(screen_name: &str) -> Vec<String> {
    vec![
        format!("https://twitter.com/{}", screen_name),
        format!("https://mobile.twitter.com/{}", screen_name),
        format!("https://twitter.com/{}/*", screen_name),
        format!("https://mobile.twitter.com/{}/*", screen_name),
    ]
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
