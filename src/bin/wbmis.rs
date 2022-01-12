use clap::Parser;
use wayback_rs::{index::Store, Item};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    match opts.command {
        SubCommand::Import { db } => {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(std::io::stdin());
            let items = reader
                .records()
                .map(|record| {
                    let row = record?;
                    Ok(Item::parse_optional_record(
                        row.get(0),
                        row.get(1),
                        row.get(2),
                        row.get(3),
                        row.get(4),
                        row.get(5),
                    )?)
                })
                .collect::<Result<Vec<Item>, Error>>()?;

            let mut store = Store::new(db)?;

            let stats = store.add_items(items).await?;

            println!("{:?}", stats);
        }
        SubCommand::Export { db } => {
            /*let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());

            let store = ItemStore::new(db, false)?;

            store
                .for_each_item(|item| writer.write_record(item.to_record()).unwrap())
                .await?;*/
        }
    };

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0:?}")]
    IOError(#[from] std::io::Error),
    #[error("CSV writing error: {0:?}")]
    CsvError(#[from] csv::Error),
    #[error("Item parsing error: {0:?}")]
    ItemError(#[from] wayback_rs::item::Error),
}

#[derive(Parser)]
#[clap(name = "wbmis", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    Import {
        /// The database file path
        #[clap(long)]
        db: String,
    },
    Export {
        /// The database file path
        #[clap(long)]
        db: String,
    },
}
