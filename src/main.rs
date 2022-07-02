use clap::Parser;
use std::path::PathBuf;
use std::io;
use std::io::Write;
use std::io::prelude::*;
use std::collections::HashMap;
use flate2::read::GzDecoder;

use itertools::Itertools;

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

  start_work(args.input, args.output);
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct TileEntry {
  z: u64,
  x: u64,
  y: u64,
  offset: u64,
  length: u64,
  is_dir: bool,
}

fn find_leaf_level(entries: &Vec<TileEntry>, max_dir_size: usize) -> u64 {
  entries.get(max_dir_size).unwrap().z - 1
}

fn entrysort(e: &TileEntry) ->  (u64, u64, u64) {
  return (e.z, e.x, e.y);
}

fn by_parent(leaf_level: u64, e: &TileEntry) -> (u64, u64, u64) {
  let level_diff = e.z - leaf_level;
  return (leaf_level, e.x / (1 << level_diff), e.y / (1 << level_diff));
}

fn make_pyramid(tile_entries: &Vec<TileEntry>, start_leaf_offset: u64, maybe_max_dir_size: Option<usize>) -> (Vec<TileEntry>, Vec<Vec<TileEntry>>) {
  let max_dir_size = maybe_max_dir_size.unwrap_or(21845);

  //  sorted_entries = sorted(tile_entries, key=entrysort)
  let mut sorted_entries = tile_entries.clone();
  sorted_entries.sort();
  if sorted_entries.len() <= max_dir_size {
    return (sorted_entries, vec![]);
    // return ();
  }

  let mut leaf_dirs: Vec<Vec<TileEntry>> = Vec::new();
  let leaf_level = find_leaf_level(&sorted_entries, max_dir_size);

  // root_entries = [e for e in sorted_entries if e.z < leaf_level]
  let mut root_entries = sorted_entries.iter().filter(|e| e.z < leaf_level).cloned().collect::<Vec<TileEntry>>();
  // entries_in_leaves = [e for e in sorted_entries if e.z >= leaf_level]
  let mut entries_in_leaves = sorted_entries.iter().filter(|e| e.z >= leaf_level).cloned().collect::<Vec<TileEntry>>();

  // # group the entries by their parent (stable)
  // entries_in_leaves.sort(key=by_parent)
  entries_in_leaves.sort_by(|a, b| by_parent(leaf_level, a).cmp(&by_parent(leaf_level, b)));

  // current_offset = start_leaf_offset
  let mut current_offset = start_leaf_offset;

  // # pack entries into groups
  let mut packed_entries: Vec<TileEntry> = Vec::new();
  let mut packed_roots: Vec<(u64, u64, u64)> = Vec::new();

  // for group in itertools.groupby(entries_in_leaves, key=by_parent):
  for (_key, group) in &entries_in_leaves.into_iter().group_by(|e| by_parent(leaf_level, e)) {
    // subpyramid_entries = list(group[1])
    let subpyramid_entries = group.collect::<Vec<TileEntry>>();

    // root = by_parent(subpyramid_entries[0])
    let root = by_parent(leaf_level, &subpyramid_entries[0]);

    // if len(packed_entries) + len(subpyramid_entries) <= max_dir_size:
    if packed_entries.len() + subpyramid_entries.len() <= max_dir_size {
      // packed_entries.extend(subpyramid_entries)
      packed_entries.extend(subpyramid_entries.iter().cloned().collect::<Vec<TileEntry>>());
      // packed_roots.append(root)
      packed_roots.push(root);
    } else {
      // # flush the current packed entries
      // for p in packed_roots:
      for (root_0, root_1, root_2) in &packed_roots {
        // root_entries.append(
        //       Entry(
        //         p[0], p[1], p[2], current_offset, 17 * len(packed_entries), True
        //     )
        // )
        let entry = TileEntry {
          z: *root_0,
          x: *root_1,
          y: *root_2,
          offset: current_offset,
          length: 17 * packed_entries.len() as u64,
          is_dir: true,
        };
        root_entries.push(entry);
      }

      // # re-sort the packed_entries by ZXY order
      // packed_entries.sort(key=entrysort)
      packed_entries.sort_by(|a, b| entrysort(a).cmp(&entrysort(b)));

      // leaf_dirs.append(packed_entries)
      leaf_dirs.push(packed_entries.clone());

      // current_offset += 17 * len(packed_entries)
      current_offset += 17 * packed_entries.len() as u64;

      // packed_entries = subpyramid_entries
      packed_entries = subpyramid_entries.into_iter().collect();

      // packed_roots = [(root[0], root[1], root[2])]
      packed_roots = vec![root];
    }
  }

  // # finalize the last set
  // if len(packed_entries):
  if packed_entries.len() > 0 {
    // for p in packed_roots:
    for (root_0, root_1, root_2) in &packed_roots {
      // root_entries.append(
      //       Entry(
      //         p[0], p[1], p[2], current_offset, 17 * len(packed_entries), True
      //     )
      // )
      let entry = TileEntry {
        z: *root_0,
        x: *root_1,
        y: *root_2,
        offset: current_offset,
        length: 17 * packed_entries.len() as u64,
        is_dir: true,
      };
      root_entries.push(entry);
    }

    // # re-sort the packed_entries by ZXY order
    // packed_entries.sort(key=entrysort)
    packed_entries.sort_by(|a, b| entrysort(a).cmp(&entrysort(b)));

    // leaf_dirs.append(packed_entries)
    leaf_dirs.push(packed_entries.clone());
  }

  // return (root_entries, leaf_dirs)
  return (root_entries, leaf_dirs);
}

fn write_entry(out: &mut std::io::BufWriter<&mut std::fs::File>, entry: TileEntry) {
  let mut z_bytes = entry.z as u8;
  if entry.is_dir {
    z_bytes = z_bytes | 0b10000000;
  }

  // if entry.is_dir:
  //     z_bytes = 0b10000000 | entry.z
  // else:
  //     z_bytes = entry.z
  // self.f.write(z_bytes.to_bytes(1, byteorder="little"))
  out.write(&z_bytes.to_le_bytes()).unwrap();

  // self.f.write(entry.x.to_bytes(3, byteorder="little"))
  let [x_0, x_1, x_2, _x_3] = (entry.x as u32).to_le_bytes();
  out.write(&[x_0, x_1, x_2]).unwrap();

  // self.f.write(entry.y.to_bytes(3, byteorder="little"))
  let [y_0, y_1, y_2, _y_3] = (entry.y as u32).to_le_bytes();
  out.write(&[y_0, y_1, y_2]).unwrap();

  // self.f.write(entry.offset.to_bytes(6, byteorder="little"))
  let [offset_0, offset_1, offset_2, offset_3, offset_4, offset_5, _offset_6, _offset_7] = (entry.offset).to_le_bytes();
  out.write(&[offset_0, offset_1, offset_2, offset_3, offset_4, offset_5]).unwrap();

  // self.f.write(entry.length.to_bytes(4, byteorder="little"))
  out.write(&(entry.length as u32).to_le_bytes()).unwrap();
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
    LIMIT 100000
  ").unwrap();
  while let sqlite::State::Row = statement.next().unwrap() {
    let zoom_level = statement.read::<i64>(0).unwrap();
    let tile_column = statement.read::<i64>(1).unwrap();
    let tile_row = statement.read::<i64>(2).unwrap();
    let tile_digest = statement.read::<String>(3).unwrap();
    let tile_data = statement.read::<Vec<u8>>(4).unwrap();

    // flipped = (1 << row[0]) - 1 - row[2]
    let flipped_row = (1 << zoom_level) - 1 - tile_row;

    if let Some(tile_offset) = hash_to_offset.get(&tile_digest) {
      tile_entries.push(TileEntry {
        z: zoom_level as u64,
        x: tile_column as u64,
        y: flipped_row as u64,
        offset: *tile_offset,
        length: tile_data.len() as u64,
        is_dir: false,
      });
    } else {
      // uncompress tile_data
      let mut decompressor = GzDecoder::new(tile_data.as_slice());
      let mut tile_data_uncompressed = Vec::<u8>::new();
      decompressor.read_to_end(&mut tile_data_uncompressed).unwrap();

      let tile_data_len = tile_data_uncompressed.len() as u64;
      out.write_all(&tile_data_uncompressed).unwrap();
      hash_to_offset.insert(tile_digest, offset);
      tile_entries.push(TileEntry {
        z: zoom_level as u64,
        x: tile_column as u64,
        y: flipped_row as u64,
        offset,
        length: tile_data_len,
        is_dir: false,
      });
      offset += tile_data_len;
    }
  }

  let (root_dir, leaf_dirs) = make_pyramid(&tile_entries, offset, None);
  if leaf_dirs.len() > 0 {
    for leaf_dir in leaf_dirs {
      for entry in leaf_dir {
        // write entry
        write_entry(&mut out, entry);
      }
    }
  }

  // Seek to the beginning of the file so we can write the header
  out.seek(std::io::SeekFrom::Start(0)).unwrap();

  // write header
  // self.f.write((0x4D50).to_bytes(2, byteorder="little"))
  out.write(&0x4D50u16.to_le_bytes()).unwrap();

  // self.f.write((2).to_bytes(2, byteorder="little"))
  out.write(&2u16.to_le_bytes()).unwrap();


  let mut metadata_raw = HashMap::<String, String>::new();
  let mut metadata_stmt = connection.prepare("
    SELECT name,value FROM metadata;
  ").unwrap();
  while let sqlite::State::Row = metadata_stmt.next().unwrap() {
    let name = metadata_stmt.read::<String>(0).unwrap();
    let value = metadata_stmt.read::<String>(1).unwrap();
    metadata_raw.insert(name, value);
  }
  // We are removing compression from the tile, so we need to remove the compression flag.
  metadata_raw.remove("compression");

  // metadata_serialized = json.dumps(metadata)
  let metadata_serialized = serde_json::to_string(&metadata_raw).unwrap();
  // # 512000 - (17 * 21845) - 2 (magic) - 2 (version) - 4 (jsonlen) - 2 (dictentries) = 140625
  // assert len(metadata_serialized) < 140625
  assert_eq!(metadata_serialized.len() < 140625, true);

  // self.f.write(len(metadata_serialized).to_bytes(4, byteorder="little"))
  out.write(&(metadata_serialized.len() as u32).to_le_bytes()).unwrap();

  // self.f.write(root_entries_len.to_bytes(2, byteorder="little"))
  out.write(&(root_dir.len() as u16).to_le_bytes()).unwrap();

  // self.f.write(metadata_serialized.encode("utf-8"))
  out.write(metadata_serialized.as_bytes()).unwrap();

  for entry in root_dir {
    // write entry
    write_entry(&mut out, entry);
  }

  out.flush().unwrap();
  println!("Filled {} with all the good things", output.display());
}
