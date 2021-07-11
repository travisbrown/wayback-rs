use data_encoding::BASE32;
use flate2::read::GzDecoder;
use sha1::{Digest, Sha1};
use std::io::BufWriter;
use std::io::Read;
use std::path::Path;

pub fn compute_digest<R: Read>(input: &mut R) -> std::io::Result<String> {
    let sha1 = Sha1::new();

    let mut buffered = BufWriter::new(sha1);
    std::io::copy(input, &mut buffered)?;

    let result = buffered.into_inner()?.finalize();

    let mut output = String::new();
    BASE32.encode_append(&result, &mut output);

    Ok(output)
}

pub fn compute_digest_gz<R: Read>(input: &mut R) -> std::io::Result<String> {
    compute_digest(&mut GzDecoder::new(input))
}

pub fn validate_files_gz<P: AsRef<std::path::Path>, F: Fn(&std::path::Path) -> Option<String>>(
    directory: P,
    expected: F,
) -> std::io::Result<Vec<Box<Path>>> {
    let mut invalid = vec![];

    for result in std::fs::read_dir(directory)? {
        let path = result?.path();

        if let Some(digest) = expected(&path) {
            let mut file = std::fs::File::open(&path)?;

            if compute_digest_gz(&mut file)? != digest {
                invalid.push(path.into_boxed_path());
            }
        }
    }

    Ok(invalid)
}

#[cfg(test)]
mod tests {
    #[test]
    fn compute_digest() {
        let digest = "ZHYT52YPEOCHJD5FZINSDYXGQZI22WJ4";
        let path = format!("../examples/wayback/{}", digest);

        let mut reader = std::io::BufReader::new(std::fs::File::open(path).unwrap());

        assert_eq!(super::compute_digest(&mut reader).unwrap(), digest);
    }
}
