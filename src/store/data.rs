use crate::digest::compute_digest_gz;
use flate2::read::GzDecoder;
use futures::{FutureExt, Stream, TryStreamExt};
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::fs::{read_dir, DirEntry, File};
use std::io::{self, BufReader, Read};
use std::iter::once;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unexpected item: {path:?}")]
    Unexpected { path: Box<Path> },
    #[error("Invalid digest or prefix: {0}")]
    InvalidDigest(String),
    #[error("I/O error")]
    IOError(#[from] io::Error),
    #[error("I/O error for {digest}: {error:?}")]
    ItemIOError { digest: String, error: io::Error },
    #[error("Unexpected error while computing digests")]
    DigestComputationError,
}

lazy_static! {
    static ref NAMES: HashSet<String> = {
        let mut names = HashSet::new();
        names.extend(('2'..='7').map(|c| c.to_string()));
        names.extend(('A'..='Z').map(|c| c.to_string()));
        names
    };
}

fn is_valid_char(c: char) -> bool {
    ('2'..='7').contains(&c) || c.is_ascii_uppercase()
}

/// A content-addressable store for compressed Wayback Machine pages.
pub struct Store {
    base: Box<Path>,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Store {
            base: path.as_ref().to_path_buf().into_boxed_path(),
        }
    }

    pub fn create<P: AsRef<Path>>(base: P) -> Result<Self, std::io::Error> {
        let path = base.as_ref();

        for name in NAMES.iter() {
            std::fs::create_dir_all(path.join(name))?;
        }

        Ok(Store {
            base: path.to_path_buf().into_boxed_path(),
        })
    }

    pub fn compute_digests(
        &self,
        prefix: Option<&str>,
        n: usize,
    ) -> impl Stream<Item = Result<(String, String), Error>> {
        futures::stream::iter(self.paths_for_prefix(prefix.unwrap_or("")))
            .map_ok(|(expected, path)| {
                tokio::spawn(async {
                    let mut file = File::open(path)?;
                    match compute_digest_gz(&mut file) {
                        Ok(actual) => Ok((expected, actual)),
                        Err(error) => Err(Error::ItemIOError {
                            digest: expected,
                            error,
                        }),
                    }
                })
                .map(|result| match result {
                    Ok(Err(error)) => Err(error),
                    Ok(Ok(value)) => Ok(value),
                    Err(_) => Err(Error::DigestComputationError),
                })
            })
            .try_buffer_unordered(n)
    }

    fn emit_error<T: 'static, E: Into<Error>>(e: E) -> Box<dyn Iterator<Item = Result<T, Error>>> {
        Box::new(once(Err(e.into())))
    }

    pub fn paths(&self) -> impl Iterator<Item = Result<(String, PathBuf), Error>> {
        match read_dir(&self.base).and_then(|it| it.collect::<std::result::Result<Vec<_>, _>>()) {
            Err(error) => Self::emit_error(error),
            Ok(mut dirs) => {
                dirs.sort_by_key(|entry| entry.file_name());
                Box::new(
                    dirs.into_iter()
                        .flat_map(|entry| match Self::check_dir_entry(&entry) {
                            Err(error) => Self::emit_error(error),
                            Ok(first) => match read_dir(entry.path()) {
                                Err(error) => Self::emit_error(error),
                                Ok(files) => Box::new(files.map(move |result| {
                                    result
                                        .map_err(Error::from)
                                        .and_then(|entry| Self::check_file_entry(&first, &entry))
                                })),
                            },
                        }),
                )
            }
        }
    }

    pub fn paths_for_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(String, PathBuf), Error>> {
        match prefix.chars().next() {
            None => Box::new(self.paths()),
            Some(first_char) => {
                if Self::is_valid_prefix(prefix) {
                    let first = first_char.to_string();
                    match read_dir(self.base.join(&first)) {
                        Err(error) => Self::emit_error(error),
                        Ok(files) => {
                            let p = prefix.to_string();
                            Box::new(
                                files
                                    .map(move |result| {
                                        result.map_err(Error::from).and_then(|entry| {
                                            Self::check_file_entry(&first, &entry)
                                        })
                                    })
                                    .filter(move |result| match result {
                                        Ok((name, _)) => name.starts_with(&p),
                                        Err(_) => true,
                                    }),
                            )
                        }
                    }
                } else {
                    Self::emit_error(Error::InvalidDigest(prefix.to_string()))
                }
            }
        }
    }

    pub fn check_file_location<P: AsRef<Path>>(
        &self,
        candidate: P,
    ) -> Result<Option<(String, Result<Box<Path>, String>)>, Error> {
        let path = candidate.as_ref();

        if let Some((name, ext)) = path
            .file_stem()
            .and_then(|os| os.to_str())
            .zip(path.extension().and_then(|os| os.to_str()))
        {
            if Self::is_valid_digest(name) && ext == "gz" {
                if let Some(location) = self.location(name) {
                    if location.is_file() {
                        Ok(None)
                    } else {
                        let mut file = File::open(path)?;
                        let digest = compute_digest_gz(&mut file)?;

                        Ok(Some((
                            name.to_string(),
                            if digest == name {
                                Ok(location)
                            } else {
                                Err(digest)
                            },
                        )))
                    }
                } else {
                    Err(Error::InvalidDigest(name.to_string()))
                }
            } else {
                Err(Error::InvalidDigest(name.to_string()))
            }
        } else {
            Err(Error::InvalidDigest(path.to_string_lossy().into_owned()))
        }
    }

    pub fn location(&self, digest: &str) -> Option<Box<Path>> {
        if Self::is_valid_digest(digest) {
            digest.chars().next().map(|first_char| {
                let path = self
                    .base
                    .join(first_char.to_string())
                    .join(format!("{}.gz", digest));

                path.into_boxed_path()
            })
        } else {
            None
        }
    }

    pub fn contains(&self, digest: &str) -> bool {
        self.lookup(digest).is_some()
    }

    pub fn lookup(&self, digest: &str) -> Option<Box<Path>> {
        self.location(digest).filter(|path| path.is_file())
    }

    pub fn extract_reader(
        &self,
        digest: &str,
    ) -> Option<Result<BufReader<GzDecoder<File>>, std::io::Error>> {
        self.lookup(digest).map(|path| {
            let file = File::open(path)?;

            Ok(BufReader::new(GzDecoder::new(file)))
        })
    }

    pub fn extract(&self, digest: &str) -> Option<Result<String, std::io::Error>> {
        self.lookup(digest).map(|path| {
            let file = File::open(path)?;
            let mut buffer = String::new();

            GzDecoder::new(file).read_to_string(&mut buffer)?;

            Ok(buffer)
        })
    }

    pub fn extract_bytes(&self, digest: &str) -> Option<Result<Vec<u8>, std::io::Error>> {
        self.lookup(digest).map(|path| {
            let file = File::open(path)?;
            let mut buffer = Vec::new();

            GzDecoder::new(file).read_to_end(&mut buffer)?;

            Ok(buffer)
        })
    }

    fn is_valid_digest(candidate: &str) -> bool {
        candidate.len() == 32 && candidate.chars().all(is_valid_char)
    }

    fn is_valid_prefix(candidate: &str) -> bool {
        candidate.len() <= 32 && candidate.chars().all(is_valid_char)
    }

    fn check_file_entry(first: &str, entry: &DirEntry) -> Result<(String, PathBuf), Error> {
        if entry.file_type()?.is_file() {
            match entry.path().file_stem().and_then(|os| os.to_str()) {
                None => Err(Error::Unexpected {
                    path: entry.path().into_boxed_path(),
                }),
                Some(name) => {
                    if name.starts_with(first) {
                        Ok((name.to_string(), entry.path()))
                    } else {
                        Err(Error::Unexpected {
                            path: entry.path().into_boxed_path(),
                        })
                    }
                }
            }
        } else {
            Err(Error::Unexpected {
                path: entry.path().into_boxed_path(),
            })
        }
    }

    fn check_dir_entry(entry: &DirEntry) -> Result<String, Error> {
        if entry.file_type()?.is_dir() {
            match entry.file_name().into_string() {
                Err(_) => Err(Error::Unexpected {
                    path: entry.path().into_boxed_path(),
                }),
                Ok(name) => {
                    if NAMES.contains(&name) {
                        Ok(name)
                    } else {
                        Err(Error::Unexpected {
                            path: entry.path().into_boxed_path(),
                        })
                    }
                }
            }
        } else {
            Err(Error::Unexpected {
                path: entry.path().into_boxed_path(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Store;
    use futures::stream::TryStreamExt;

    fn digests() -> Vec<String> {
        vec![
            "2G3EOT7X6IEQZXKSM3OJJDW6RBCHB7YE".to_string(),
            "5DECQVIU7Y3F276SIBAKKCRGDMVXJYFV".to_string(),
            "AJBB526CEZFOBT3FCQYLRMXQ2MSFHE3O".to_string(),
            "Y2A3M6COP2G6SKSM4BOHC2MHYS3UW22V".to_string(),
            "YJFNIRKJZTUBLTRDVCZC5EMUWOOYJN7L".to_string(),
        ]
    }

    fn correct_digest(input: &str) -> String {
        if input == "5DECQVIU7Y3F276SIBAKKCRGDMVXJYFV" {
            "5BPR3OBK6O7KJ6PKFNJRNUICXWNZ46QG".to_string()
        } else {
            input.to_string()
        }
    }

    #[tokio::test]
    async fn compute_digests() {
        let store = Store::new("examples/wayback/store/items/");

        let mut result = store
            .compute_digests(None, 2)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        result.sort();

        assert_eq!(
            result,
            digests()
                .into_iter()
                .map(|digest| (digest.clone(), correct_digest(&digest)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn paths() {
        let store = Store::new("examples/wayback/store/items/");

        let mut result = store
            .paths()
            .map(|res| res.map(|p| p.0))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        result.sort();

        assert_eq!(result, digests());
    }

    #[test]
    fn path_for_prefix_1() {
        let store = Store::new("examples/wayback/store/items/");

        let mut result = store
            .paths_for_prefix("Y")
            .map(|res| res.map(|p| p.0))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        result.sort();

        assert_eq!(
            result,
            digests()
                .into_iter()
                .filter(|digest| digest.starts_with("Y"))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn path_for_prefix_2() {
        let store = Store::new("examples/wayback/store/items/");

        let mut result = store
            .paths_for_prefix("YJ")
            .map(|res| res.map(|p| p.0))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        result.sort();

        assert_eq!(
            result,
            digests()
                .into_iter()
                .filter(|digest| digest.starts_with("YJ"))
                .collect::<Vec<_>>()
        );
    }
}
