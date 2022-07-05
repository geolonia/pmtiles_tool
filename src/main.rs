mod convert;
mod writer;

use clap::Parser;

use std::path::PathBuf;
use std::io;
use std::io::Write;

#[derive(Parser, Debug)]
#[clap(version = "0.1.0")]
struct Args {
  /// Input
  #[clap(value_parser)]
  input: PathBuf,

  /// Output
  #[clap(value_parser)]
  output: PathBuf,
}

fn main() {
  let args = Args::parse();

  // fail if input file does not exist
  if !args.input.exists() {
    panic!("Input file does not exist");
  }

  // ask if we should overwrite the output file
  if args.output.exists() {
    print!("Output file already exists. Overwrite? (y/n) ");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    if input.trim() != "y" {
      panic!("Aborted");
    }
    // remove the output file
    std::fs::remove_file(&args.output).unwrap();
  }

  convert::mbtiles_to_pmtiles(args.input, args.output);
}
