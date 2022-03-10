use clap::Parser;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    match opts.command {
        SubCommand::Import { input, output } => {
            let mut parquet = wayback_rs::parquet::ParquetFile::create(output)?;

            parquet.write_all(&input)?;
            parquet.close()?;
        }
        SubCommand::Search { input, query } => {
            let mut ctx = ExecutionContext::new();
            let df = ctx.read_parquet(input).await?;
            let df = df.filter(starts_with(col("url"), lit(query)))?;

            let results = df.collect().await?;

            for result in results {
                println!("{:?}", result);
            }
        }
    };

    Ok(())
}

#[derive(Parser)]
#[clap(name = "parquet", version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// Convert a CSV file to Parquet
    Import {
        /// The input directory path
        #[clap(long)]
        input: String,
        /// The output file path
        #[clap(long)]
        output: String,
    },
    Search {
        /// The input file path
        #[clap(long)]
        input: String,
        /// URL prefix
        #[clap(long)]
        query: String,
    },
}

use simplelog::LevelFilter;

fn select_log_level_filter(verbosity: i32) -> LevelFilter {
    match verbosity {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    }
}

pub fn init_logging(verbosity: i32) -> Result<(), log::SetLoggerError> {
    simplelog::TermLogger::init(
        select_log_level_filter(verbosity),
        simplelog::Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
}
