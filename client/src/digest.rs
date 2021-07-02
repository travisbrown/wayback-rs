use data_encoding::BASE32;
use sha1::{Digest, Sha1};
use std::io::BufWriter;
use std::io::Read;

pub fn compute_digest<R: Read>(input: &mut R) -> std::io::Result<String> {
    let sha1 = Sha1::new();

    let mut buffered = BufWriter::new(sha1);
    std::io::copy(input, &mut buffered)?;

    let result = buffered.into_inner()?.finalize();

    let mut output = String::new();
    BASE32.encode_append(&result, &mut output);

    Ok(output)
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
