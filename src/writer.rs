use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::time;

use crossbeam_utils::thread;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use flate2::read::GzDecoder;

use itertools::Itertools;

const INITIAL_OFFSET: u64 = 512000;
const DEFAULT_MAX_DIR_SIZE: u64 = 21845;
// 512000 - (17 * 21845) - 2 (magic) - 2 (version) - 4 (jsonlen) - 2 (dictentries) = 140625
const MAXIMUM_METADATA_SIZE: u64 = INITIAL_OFFSET - (17 * DEFAULT_MAX_DIR_SIZE) - 2 - 2 - 4 - 2;

pub struct WorkJob {
  pub zoom_level: i64,
  pub tile_column: i64,
  pub tile_row: i64,
  pub tile_data: Vec<u8>,
}

struct WorkResults {
  zoom_level: i64,
  tile_column: i64,
  tile_row: i64,
  tile_digest: u64,
  // uncompressed
  tile_data: Vec<u8>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct TileEntry {
  z: u64,
  x: u64,
  y: u64,
  offset: u64,
  length: u32,
  is_dir: bool,
}

// Find best base zoom to avoid extra indirection for as many tiles as we can
// precondition: entries is sorted, only tile entries, len(entries) > max_dir_size
fn find_leaf_level(entries: &[TileEntry], max_dir_size: usize) -> u64 {
  entries.get(max_dir_size).unwrap().z - 1
}

fn entrysort(e: &TileEntry) -> (u64, u64, u64) {
  (e.z, e.x, e.y)
}

fn by_parent(leaf_level: u64, e: &TileEntry) -> (u64, u64, u64) {
  let level_diff = e.z - leaf_level;
  (leaf_level, e.x / (1 << level_diff), e.y / (1 << level_diff))
}

fn make_pyramid(
  tile_entries: &[TileEntry],
  start_leaf_offset: u64,
  max_dir_size: usize,
) -> (Vec<TileEntry>, Vec<Vec<TileEntry>>) {
  println!("Making pyramid...");

  //  sorted_entries = sorted(tile_entries, key=entrysort)
  let mut sorted_entries = tile_entries.to_vec();
  sorted_entries.sort_by_key(entrysort);

  // If the number of entries is less than max_dir_size, we don't need to do anything
  if sorted_entries.len() <= max_dir_size {
    return (sorted_entries, vec![]);
    // return ();
  }

  let leaf_level = find_leaf_level(&sorted_entries, max_dir_size);
  println!("Tiles above z{} will be stored in leaves", leaf_level);

  // root_entries = [e for e in sorted_entries if e.z < leaf_level]
  let mut root_entries = sorted_entries
    .iter()
    .filter(|e| e.z < leaf_level)
    .cloned()
    .collect::<Vec<TileEntry>>();
  // entries_in_leaves = [e for e in sorted_entries if e.z >= leaf_level]
  let mut entries_in_leaves = sorted_entries
    .iter()
    .filter(|e| e.z >= leaf_level)
    .collect::<Vec<&TileEntry>>();

  let mut leaf_dirs: Vec<Vec<TileEntry>> =
    Vec::with_capacity(entries_in_leaves.len() / max_dir_size);

  // # group the entries by their parent (stable)
  // entries_in_leaves.sort(key=by_parent)
  entries_in_leaves.sort_by_key(|a| by_parent(leaf_level, a));

  // current_offset = start_leaf_offset
  let mut current_offset = start_leaf_offset;

  // # pack entries into groups
  let mut packed_entries: Vec<&TileEntry> = Vec::with_capacity(max_dir_size);
  let mut packed_roots: Vec<(u64, u64, u64)> = Vec::new();

  // for group in itertools.groupby(entries_in_leaves, key=by_parent):
  for (_key, group) in &entries_in_leaves
    .into_iter()
    .group_by(|e| by_parent(leaf_level, e))
  {
    // subpyramid_entries = list(group[1])
    let subpyramid_entries = group.collect::<Vec<&TileEntry>>();

    // root = by_parent(subpyramid_entries[0])
    let root = by_parent(leaf_level, subpyramid_entries[0]);

    // if len(packed_entries) + len(subpyramid_entries) <= max_dir_size:
    if packed_entries.len() + subpyramid_entries.len() <= max_dir_size {
      // packed_entries.extend(subpyramid_entries)
      packed_entries.extend(subpyramid_entries.to_vec());
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
          length: 17 * packed_entries.len() as u32,
          is_dir: true,
        };
        root_entries.push(entry);
      }

      // # re-sort the packed_entries by ZXY order
      // packed_entries.sort(key=entrysort)
      packed_entries.sort_by_key(|a| entrysort(a));

      // current_offset += 17 * len(packed_entries)
      current_offset += 17 * packed_entries.len() as u64;

      // leaf_dirs.append(packed_entries)
      leaf_dirs.push(
        packed_entries
          .into_iter()
          .cloned()
          .collect::<Vec<TileEntry>>(),
      );

      // packed_entries = subpyramid_entries
      packed_entries = subpyramid_entries.into_iter().collect();

      // packed_roots = [(root[0], root[1], root[2])]
      packed_roots = vec![root];
    }
  }

  // # finalize the last set
  // if len(packed_entries):
  if !packed_entries.is_empty() {
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
        length: 17 * packed_entries.len() as u32,
        is_dir: true,
      };
      root_entries.push(entry);
    }

    // # re-sort the packed_entries by ZXY order
    // packed_entries.sort(key=entrysort)
    packed_entries.sort_by_key(|a| entrysort(a));

    // leaf_dirs.append(packed_entries)
    leaf_dirs.push(
      packed_entries
        .into_iter()
        .cloned()
        .collect::<Vec<TileEntry>>(),
    );
  }

  println!(
    "root_entries is {} bytes",
    root_entries.len() * std::mem::size_of::<TileEntry>()
  );

  // return (root_entries, leaf_dirs)
  (root_entries, leaf_dirs)
}

fn maybe_decompress(data: Vec<u8>) -> Vec<u8> {
  if data[0] == 0x1f && data[1] == 0x8b {
    let mut out = Vec::with_capacity(data.len() * 2);
    let mut zlib = GzDecoder::new(data.as_slice());
    zlib.read_to_end(&mut out).unwrap();
    return out;
  }
  data
}

pub struct Writer {
  out_path: PathBuf,
}

impl Writer {
  pub fn new(out_path: &Path) -> Writer {
    Writer {
      out_path: out_path.to_path_buf(),
    }
  }

  pub fn run(
    &mut self,
    input_queue_rx: crossbeam_channel::Receiver<WorkJob>,
    metadata: &HashMap<String, String>,
  ) {
    let metadata_serialized = serde_json::to_string(metadata).unwrap();
    if metadata_serialized.len() >= MAXIMUM_METADATA_SIZE as usize {
      panic!(
        "Metadata is too large ({}). Maximum size is {} bytes.",
        metadata_serialized.len(),
        MAXIMUM_METADATA_SIZE
      );
    }

    thread::scope(|s| {
      let (result_queue_tx, result_queue_rx) = crossbeam_channel::unbounded::<WorkResults>();
      let out_path = self.out_path.clone();

      // writer thread
      s.spawn(move |_| {
        let mut out = io::BufWriter::with_capacity(512000, File::create(out_path).unwrap());
        // leave space for the header
        out.write_all(&[0; INITIAL_OFFSET as usize]).unwrap();

        let mut tile_entries = Vec::<TileEntry>::with_capacity(1_000_000);

        let mut offset = INITIAL_OFFSET;
        let mut hash_to_offset = HashMap::<u64, u64>::with_capacity(1_000_000);

        let mut current_count = 0;
        let mut last_ts = time::Instant::now();

        while let Ok(result) = result_queue_rx.recv() {
          let mut tile_entry = TileEntry {
            z: result.zoom_level as u64,
            x: result.tile_column as u64,
            y: result.tile_row as u64,
            offset: 0,
            length: result.tile_data.len() as u32,
            is_dir: false,
          };
          if let Some(tile_offset) = hash_to_offset.get(&result.tile_digest) {
            tile_entry.offset = *tile_offset;
          } else {
            let tile_data_len = result.tile_data.len() as u32;
            out.write_all(&result.tile_data).unwrap();
            hash_to_offset.insert(result.tile_digest, offset);
            tile_entry.offset = offset;
            offset += tile_data_len as u64;
          }

          tile_entries.push(tile_entry);

          current_count += 1;
          if current_count % 100_000 == 0 {
            let ts = time::Instant::now();
            let elapsed = ts.duration_since(last_ts);
            println!(
              "{} tiles added to archive in {}ms ({:.4}ms/tile).",
              current_count,
              elapsed.as_millis(),
              elapsed.as_millis() as f64 / 100_000_f64,
            );
            last_ts = ts;
          }
        }

        let (root_dir, leaf_dirs) =
          make_pyramid(&tile_entries, offset, DEFAULT_MAX_DIR_SIZE as usize);

        println!(
          "Calculated {} root entries, {} leaf dirs.",
          root_dir.len(),
          leaf_dirs.len()
        );
        if !leaf_dirs.is_empty() {
          for leaf_dir in leaf_dirs {
            for entry in leaf_dir {
              // write entry
              self.write_entry(&mut out, entry);
            }
          }
        }

        println!("Writing header and root dir...");
        // Seek to the beginning of the file so we can write the header
        out.seek(std::io::SeekFrom::Start(0)).unwrap();
        self.write_header(&mut out, metadata_serialized, root_dir.len() as u16);
        for entry in root_dir {
          self.write_entry(&mut out, entry);
        }
        out.flush().unwrap();
      });

      // worker threads
      let max_workers = std::cmp::max(num_cpus::get() / 2, 2);
      println!("Spawning {} workers.", max_workers);

      for thread_num in 0..max_workers {
        let thread_queue_rx = input_queue_rx.clone();
        let thread_results_tx = result_queue_tx.clone();
        s.spawn(move |_| {
          let mut work_done = 0;

          while let Ok(work) = thread_queue_rx.recv() {
            let tile_data_uncompressed = maybe_decompress(work.tile_data);

            let mut hasher = DefaultHasher::new();
            tile_data_uncompressed.hash(&mut hasher);
            let tile_digest = hasher.finish();

            work_done += 1;

            thread_results_tx
              .send(WorkResults {
                zoom_level: work.zoom_level,
                tile_column: work.tile_column,
                tile_row: work.tile_row,
                tile_digest,
                tile_data: tile_data_uncompressed,
              })
              .unwrap();
          }

          println!("Thread {} did {} jobs.", thread_num, work_done);
        });
      }
    })
    .unwrap();
  }

  // One entry is 17 bytes long
  fn write_entry(&mut self, out: &mut io::BufWriter<File>, entry: TileEntry) {
    let mut z_bytes = entry.z as u8;
    if entry.is_dir {
      z_bytes |= 0b10000000;
    }

    // if entry.is_dir:
    //     z_bytes = 0b10000000 | entry.z
    // else:
    //     z_bytes = entry.z
    // self.f.write(z_bytes.to_bytes(1, byteorder="little"))
    out.write_all(&z_bytes.to_le_bytes()).unwrap();

    // self.f.write(entry.x.to_bytes(3, byteorder="little"))
    let [x_0, x_1, x_2, _x_3] = (entry.x as u32).to_le_bytes();
    out.write_all(&[x_0, x_1, x_2]).unwrap();

    // self.f.write(entry.y.to_bytes(3, byteorder="little"))
    let [y_0, y_1, y_2, _y_3] = (entry.y as u32).to_le_bytes();
    out.write_all(&[y_0, y_1, y_2]).unwrap();

    // self.f.write(entry.offset.to_bytes(6, byteorder="little"))
    let [offset_0, offset_1, offset_2, offset_3, offset_4, offset_5, _offset_6, _offset_7] =
      (entry.offset).to_le_bytes();
    out
      .write_all(&[offset_0, offset_1, offset_2, offset_3, offset_4, offset_5])
      .unwrap();

    // self.f.write(entry.length.to_bytes(4, byteorder="little"))
    out.write_all(&(entry.length as u32).to_le_bytes()).unwrap();
  }

  fn write_header(
    &mut self,
    out: &mut io::BufWriter<File>,
    metadata_serialized: String, // &HashMap<String, String>,
    root_dir_len: u16,
  ) {
    // write header
    // self.f.write((0x4D50).to_bytes(2, byteorder="little"))
    out.write_all(&0x4D50u16.to_le_bytes()).unwrap();

    // self.f.write((2).to_bytes(2, byteorder="little"))
    out.write_all(&2u16.to_le_bytes()).unwrap();

    // self.f.write(len(metadata_serialized).to_bytes(4, byteorder="little"))
    out
      .write_all(&(metadata_serialized.len() as u32).to_le_bytes())
      .unwrap();

    // self.f.write(root_entries_len.to_bytes(2, byteorder="little"))
    out.write_all(&root_dir_len.to_le_bytes()).unwrap();

    // self.f.write(metadata_serialized.encode("utf-8"))
    out.write_all(metadata_serialized.as_bytes()).unwrap();
  }
}
