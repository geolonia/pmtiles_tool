use clap::Parser;
use std::path::PathBuf;
use std::io::prelude::*;
use std::collections::HashMap;

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
    println!("Output file already exists. Overwrite? (y/n)");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    if input.trim() != "y" {
      panic!("Aborted");
    }
    // remove the output file
    std::fs::remove_file(&args.output).unwrap();
  }

  start_work(args.input, args.output);
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TileEntry {
  z: u64,
  x: u64,
  y: u64,
  offset: u64,
  length: u64,
  is_dir: bool,
}

fn find_leaf_level(entries: Vec<TileEntry>, max_dir_size: u64) -> u64 {
  entries.get(max_dir_size as usize).unwrap().z - 1
}

fn by_parent(leaf_level: u64, e: &TileEntry) -> (u64, u64, u64) {
  let level_diff = e.z - leaf_level;
  return (leaf_level, e.x / (1 << level_diff), e.y / (1 << level_diff));
}

fn make_pyramid(tile_entries: Vec<TileEntry>, start_leaf_offset: u64, maybe_max_dir_size: Option<usize>) {
  let max_dir_size = maybe_max_dir_size.unwrap_or(21845);
  tile_entries.sort();
  if tile_entries.len() <= max_dir_size {
    return tile_entries;
  }

  let mut leaf_dirs: Vec<Vec<TileEntry>> = Vec::new();
  let leaf_level = find_leaf_level(tile_entries, max_dir_size);


}

fn start_work(input: PathBuf, output: PathBuf) {
  let connection = sqlite::open(input).unwrap();
  connection.execute("PRAGMA query_only = true;").unwrap();

  // open output file
  let mut output_f = std::fs::OpenOptions::new()
    .read(true)
    .write(true)
    .create_new(true)
    .open(&output)
    .unwrap();
  let mut out = std::io::BufWriter::new(&mut output_f);

  let mut offset = 512000;
  // leave space for the header
  out.write_all(&[0; 512000]).unwrap();

  let mut tile_entries = Vec::<TileEntry>::new();
  let mut hash_to_offset = HashMap::<String, u64>::new();

  let mut statement = connection.prepare("
    SELECT
      tile_ref.zoom_level,
      tile_ref.tile_column,
      tile_ref.tile_row,
      tile_ref.tile_digest,
      images.tile_data
    FROM
      tile_ref
    JOIN images ON images.tile_digest = tile_ref.tile_digest
    ORDER BY zoom_level, tile_column, tile_row ASC
  ").unwrap();
  while let sqlite::State::Row = statement.next().unwrap() {
    let zoom_level = statement.read::<i64>(0).unwrap();
    let tile_column = statement.read::<i64>(1).unwrap();
    let tile_row = statement.read::<i64>(2).unwrap();
    let tile_digest = statement.read::<String>(3).unwrap();
    let tile_data = statement.read::<Vec<u8>>(4).unwrap();

    if let Some(tile_offset) = hash_to_offset.get(&tile_digest) {
      tile_entries.push(TileEntry {
        z: zoom_level as u64,
        x: tile_column as u64,
        y: tile_row as u64,
        offset: *tile_offset,
        length: tile_data.len() as u64,
        is_dir: false,
      });
    } else {
      let tile_data_len = tile_data.len() as u64;
      out.write_all(&tile_data).unwrap();
      hash_to_offset.insert(tile_digest, offset);
      tile_entries.push(TileEntry {
        z: zoom_level as u64,
        x: tile_column as u64,
        y: tile_row as u64,
        offset,
        length: tile_data_len,
        is_dir: false,
      });
      offset += tile_data_len;
    }
  }

  out.flush().unwrap();
  println!("Filled {} with all the good things", output.display());
}
