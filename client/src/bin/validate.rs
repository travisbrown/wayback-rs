use indicatif::{ProgressBar, ProgressStyle};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let files = std::fs::read_dir(&args[1])?.collect::<Vec<_>>();

    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>7}/{len:7} ({eta})")
        .progress_chars("#>-"));

    let mut invalid = vec![];

    for result in files {
        let path = result?.path();

        let digest = path.file_stem().unwrap().to_string_lossy();
        let mut file = std::fs::File::open(&path)?;

        if wayback_client::digest::compute_digest_gz(&mut file)? != digest {
            invalid.push(path.into_boxed_path());
        }

        pb.inc(1);
    }

    if invalid.is_empty() {
        println!("All files are valid");
    } else {
        for path in invalid {
            println!("{}", path.to_string_lossy());
        }
    }

    Ok(())
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
