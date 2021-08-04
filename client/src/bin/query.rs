use rocksdb::{DBCompressionType, Options, DB};
use wayback_client::digest;

type Void = Result<(), Box<dyn std::error::Error>>;

//#[tokio::main]
fn main() -> Void {
    let args: Vec<String> = std::env::args().collect();

    let mut opts = Options::default();
    opts.set_compression_type(DBCompressionType::Lz4);

    let db = DB::open(&opts, args.get(1).unwrap())?;

    let digests = args.get(2).unwrap().split(',');

    for d in digests {
        if let Some(value) = db.get(digest::string_to_bytes(d).unwrap())? {
            println!("{}: {}", d, String::from_utf8(value)?);
        }
    }

    Ok(())
}
