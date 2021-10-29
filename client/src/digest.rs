//! Utilities for computing digests used by the Wayback Machine.
//!
//! The Wayback Machine's CDX index provides a digest for each page in its
//! search results. These digests can be computed by

use data_encoding::BASE32;
use flate2::read::GzDecoder;
use sha1::{Digest, Sha1};
use std::io::{BufWriter, Error, Read};

/// Decode a Base32 string into the SHA-1 bytes, returning an empty value if
/// the input is not a valid Base2-encoded SHA-1 hash.
pub fn string_to_bytes(digest: &str) -> Option<[u8; 20]> {
    if digest.len() == 32 {
        let mut output = [0; 20];
        let count = BASE32.decode_mut(digest.as_bytes(), &mut output).ok()?;

        if count == 20 {
            Some(output)
        } else {
            None
        }
    } else {
        None
    }
}

/// Encode a SHA-1 hash into a 32-character Base32 string.
pub fn bytes_to_string(bytes: &[u8; 20]) -> String {
    BASE32.encode(bytes)
}

/// Compute the SHA-1 hash for bytes read from a source and encode it as a
/// Base32 string.
pub fn compute_digest<R: Read>(input: &mut R) -> Result<String, Error> {
    let sha1 = Sha1::new();

    let mut buffered = BufWriter::new(sha1);
    std::io::copy(input, &mut buffered)?;

    let result = buffered.into_inner()?.finalize();

    let mut output = String::new();
    BASE32.encode_append(&result, &mut output);

    Ok(output)
}

/// Compute the SHA-1 hash for bytes read from a GZip-compressed source and
/// encode it as a Base32 string.
pub fn compute_digest_gz<R: Read>(input: &mut R) -> Result<String, Error> {
    compute_digest(&mut GzDecoder::new(input))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn compute_digest() {
        let digest = "ZHYT52YPEOCHJD5FZINSDYXGQZI22WJ4";
        let path = format!("../examples/wayback/{}", digest);

        let mut reader = BufReader::new(File::open(path).unwrap());

        assert_eq!(super::compute_digest(&mut reader).unwrap(), digest);
    }

    #[test]
    fn round_trip() {
        let digest = "ZHYT52YPEOCHJD5FZINSDYXGQZI22WJ4";

        let bytes = super::string_to_bytes(&digest).unwrap();
        let string = super::bytes_to_string(&bytes);

        assert_eq!(digest, string);
    }
}
