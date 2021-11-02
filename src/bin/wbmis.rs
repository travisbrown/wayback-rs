use clap::{crate_authors, crate_version, Parser};
use wayback_rs::{store::meta::ItemStore, Item};

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
                .map(|res| Item::from_record(&res.unwrap()).unwrap());

            let store = ItemStore::new(db, false)?;

            store.add_items(items).await?;
        }
        SubCommand::Export { db } => {
            let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());

            let store = ItemStore::new(db, false)?;

            store
                .for_each_item(|item| writer.write_record(item.to_record()).unwrap())
                .await?;
        }
    };

    Ok(())
}

#[derive(Parser)]
#[clap(name = "wbmis", version = crate_version!(), author = crate_authors!())]
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
