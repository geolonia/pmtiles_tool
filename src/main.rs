mod convert;
mod http_server;
mod reader;
mod writer_v2;
mod writer_v3;

use clap::{Parser, Subcommand};

use std::io;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(
  name = "pmtiles_tool",
  about = "A tool for working with pmtiles archives",
  version
)]
struct Cli {
  #[clap(subcommand)]
  command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
  #[clap(
    name = "convert",
    about = "Convert a mbtiles archive to a pmtiles archive"
  )]
  Convert {
    /// Input
    #[clap(value_parser)]
    input: PathBuf,

    /// Output
    #[clap(value_parser)]
    output: PathBuf,
  },
  #[clap(name = "info", about = "Get information about a pmtiles archive")]
  Info {
    /// Input
    #[clap(value_parser)]
    input: PathBuf,
  },
  #[clap(name = "serve", about = "Serve XYZ tiles from a pmtiles archive")]
  Serve {
    /// Input
    #[clap(value_parser)]
    input: PathBuf,

    /// Port
    #[clap(short, long, default_value = "8888")]
    port: u16,
  },
}

fn main() {
  let args = Cli::parse();

  match args.command {
    Commands::Convert { input, output } => {
      // fail if input file does not exist
      if !input.exists() {
        panic!("Input file does not exist");
      }

      // ask if we should overwrite the output file
      if output.exists() {
        print!("Output file already exists. Overwrite? (y/n) ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        if input.trim() != "y" {
          panic!("Aborted");
        }
        // remove the output file
        std::fs::remove_file(&output).unwrap();
      }

      convert::mbtiles_to_pmtiles(input, output);
    }

    Commands::Info { input } => {
      // fail if input file does not exist
      if !input.exists() {
        panic!("Input file does not exist");
      }

      // get the info
      let reader = reader::Reader::new(&input).unwrap();
      println!("Version: {}", reader.version);
      println!("metadata:");
      println!("{}", reader.get_metadata());
      println!("root entries: {}", reader.root_entries_len);
      println!("leaf directories: {}", reader.leaves_len);
    }

    Commands::Serve { input, port } => {
      // fail if input file does not exist
      if !input.exists() {
        panic!("Input file does not exist");
      }

      // serve the tiles
      println!("Starting server on port {}", port);
      http_server::start_server(&input, port);
    }
  }
}
