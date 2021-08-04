use rocksdb::{DBCompressionType, Options, DB};
use wayback_client::digest;

type Void = Result<(), Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Void {
    let args: Vec<String> = std::env::args().collect();

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Lz4);

    let db = DB::open(&opts, args.get(2).unwrap())?;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(std::fs::File::open(args.get(1).unwrap())?);

    for result in reader.records() {
        let row = result?;
        let digest_string = row.get(2).unwrap();
        if let Some(key) = digest::string_to_bytes(digest_string) {
            let value_string = format!(
                "http://web.archive.org/web/{}/{}",
                row.get(1).unwrap(),
                row.get(0).unwrap()
            );

            db.put(key, value_string.as_bytes())?;
        } else {
            println!("Skipping: {}", digest_string);
        }
    }

    Ok(())
}
