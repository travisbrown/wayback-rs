use clap::{crate_authors, crate_version, Parser};
use wayback_rs::store::data::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    match opts.command {
        SubCommand::Digests { base, prefix } => {
            let store = Store::new(base);

            for res in store.paths_for_prefix(&prefix.unwrap_or_else(|| "".to_string())) {
                let (digest, _) = res?;
                println!("{}", digest);
            }
        }
    };

    Ok(())
}

#[derive(Parser)]
#[clap(name = "wbms", version = crate_version!(), author = crate_authors!())]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// Print the digests in the given store to stdout
    Digests {
        /// The base directory path
        #[clap(long)]
        base: String,
        /// The digest search prefix
        #[clap(long)]
        prefix: Option<String>,
    },
}
