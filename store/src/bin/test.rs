use clap::{crate_authors, crate_version, Parser};
use simplelog::LevelFilter;
use std::io::BufRead;
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

    let user_id_re = regex::Regex::new(r"data-user-id=.(\d+)[^\d]").unwrap();
    let screen_name_re = regex::Regex::new(r"href=./(\w+)[^\w]").unwrap();

    let mut users = std::collections::HashMap::new();

    store
        .paths_for_prefix(&opts.prefix.unwrap_or_default())
        .for_each(|result| {
            let (digest, _) = result.unwrap();
            let reader = store.extract_reader(&digest).unwrap().unwrap();
            users.clear();
            for line in reader.lines() {
                if let Ok(content) = line {
                    if let Some(user_id_caps) = user_id_re.captures(&content) {
                        if let Some(screen_name_caps) = screen_name_re.captures(&content) {
                            users.insert(
                                user_id_caps
                                    .get(1)
                                    .unwrap()
                                    .as_str()
                                    .parse::<u64>()
                                    .unwrap(),
                                screen_name_caps.get(1).unwrap().as_str().to_string(),
                            );

                            /*println!(
                                "{},{}",
                                user_id_caps.get(1).unwrap().as_str(),
                                screen_name_caps.get(1).unwrap().as_str()
                            );*/
                        }
                    }
                }
            }
            log::info!("{}: {}", digest, users.len());
            for (id, screen_name) in &users {
                println!("{},{},{}", id, screen_name, digest);
            }
        });

    Ok(())
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
