use clap::{crate_authors, crate_version, Parser};
use futures::{lock::Mutex, TryFutureExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use simplelog::LevelFilter;
use std::fs::File;
use std::io::{LineWriter, Write};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("Tokio error: {0:?}")]
    TaskError(#[from] tokio::task::JoinError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    let output = File::create(opts.output)?;
    let output = Mutex::new(LineWriter::new(output));

    let files = std::fs::read_dir(opts.dir)?.collect::<Vec<_>>();

    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta})")
        .progress_chars("#>-"));

    futures::stream::iter(files)
        .map_err(Error::from)
        .map_ok(|entry| {
            let path = entry.path();
            let pb = pb.clone();
            tokio::spawn(async move {
                let digest = path.file_stem().unwrap().to_string_lossy();
                let r = std::fs::File::open(&path)
                    .and_then(|mut file| wayback_client::digest::compute_digest_gz(&mut file));

                let result = match r {
                    Ok(value) if value == digest => None,
                    Ok(_value) => Some((path.into_boxed_path(), None)),
                    Err(error) => Some((path.into_boxed_path(), Some(error))),
                };

                pb.inc(1);

                let result: Result<Option<(Box<std::path::Path>, Option<std::io::Error>)>, Error> =
                    Ok(result);
                result
            })
            .err_into::<Error>()
        })
        .try_buffer_unordered(opts.parallelism)
        .try_filter_map(|x| async { x })
        .try_for_each(|result| async {
            let message = match result {
                (path, None) => format!("{}", path.display()),
                (path, Some(error)) => format!("{},{:?}", path.display(), error),
            };

            output
                .lock()
                .await
                .write_all(format!("{}\n", message).as_bytes())?;
            pb.println(message);

            Ok(())
        })
        .await?;

    Ok(())
}

#[derive(Parser)]
#[clap(name = "validate", version = crate_version!(), author = crate_authors!())]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    /// Level of parallelism
    #[clap(short, long, default_value = "6")]
    parallelism: usize,
    /// Item directory
    #[clap(short, long)]
    dir: String,
    /// Output file
    #[clap(short, long)]
    output: String,
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

/*use futures::{TryFutureExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("Tokio error: {0:?}")]
    TaskError(#[from] tokio::task::JoinError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let files = std::fs::read_dir(&args[1])?.collect::<Vec<_>>();

    let pb = Arc::new(ProgressBar::new(files.len() as u64));
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta})")
        .progress_chars("#>-"));

    //let mut invalid = vec![];

    let invalid = futures::stream::iter(files)
        .map_err(Error::from)
        .map_ok(|entry| {
            let path = entry.path().clone();
            let pb = pb.clone();
            tokio::spawn(async move {

                let digest = path.file_stem().unwrap().to_string_lossy();
                let mut file = std::fs::File::open(&path)?;

                let error = if wayback_client::digest::compute_digest_gz(&mut file)? != digest {
                    Some(path.into_boxed_path())
                } else {
                    None
                };

                pb.inc(1);

                let result: Result<Option<Box<std::path::Path>>, Error> = Ok(error);
                result
            })
            .err_into::<Error>()
        })
        .try_buffer_unordered(4)
        .try_filter_map(|x| async { x })
        .try_collect::<Vec<_>>()
        .await?;

    /*for result in files {
        let path = result?.path();

        let digest = path.file_stem().unwrap().to_string_lossy();
        let mut file = std::fs::File::open(&path)?;

        if wayback_client::digest::compute_digest_gz(&mut file)? != digest {
            invalid.push(path.into_boxed_path());
        }

        pb.inc(1);
    }*/

    if invalid.is_empty() {
        println!("All files are valid");
    } else {
        for path in invalid {
            println!("{}", path.to_string_lossy());
        }
    }

    Ok(())
}

/*
fn validate_files_gz<P: AsRef<std::path::Path>>(
    directory: P,
) -> std::io::Result<Vec<Box<std::path::Path>>> {
    let mut invalid = vec![];

    for result in std::fs::read_dir(directory)? {
        let path = result?.path();

        let digest = path.file_stem().unwrap().to_string_lossy();
        let mut file = std::fs::File::open(&path)?;

        if wayback_client::digest::compute_digest_gz(&mut file)? != digest {
            invalid.push(path.into_boxed_path());
        }
    }

    Ok(invalid)
}
*/
*/
