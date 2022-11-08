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
use flate2::write::GzEncoder;

use itertools::Itertools;

const HEADER_SIZE: usize = 127;
// However, the compressed size of the header plus root directory is required in v3 
// to be under 16,384 bytes.
const ROOT_DIR_SIZE: usize = 16_384 - HEADER_SIZE;

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
  tile_data: Vec<u8>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct TileEntry {
  tile_id: u64,
  offset: u64,
  length: u32,
  run_length: u32,
}

#[repr(u8)]
enum CompressionType {
  Unknown = 0,
  None = 1,
  Gzip = 2,
  Brotli = 3,
  Zstd = 4,
}

#[repr(u8)]
enum TileType {
  Unknown = 0,
  MVT = 1,
  PNG = 2,
  JPEG = 3,
  WEBP = 4,
}

struct Header {
  root_offset: u64,
  root_length: u64,
  metadata_offset: u64,
  metadata_length: u64,
  leaf_dir_offset: u64,
  leaf_dir_length: u64,
  tile_data_offset: u64,
  tile_data_length: u64,
  num_addressed_tiles: u64,
  num_tile_entries: u64,
  num_tile_contents: u64,
  clustered: u8,
  internal_compression: CompressionType,
  tile_compression: CompressionType,
  tile_type: TileType,
  minzoom: u8,
  maxzoom: u8,
  min_longitude: i32,
  min_latitude: i32,
  max_longitude: i32,
  max_latitude: i32,
  center_zoom: u8,
  center_longitude: i32,
  center_latitude: i32,
}

fn serialize_header(header: Header) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(127);
  // magic number
  bytes.write("PMTiles".as_bytes()).unwrap();
  // version
  bytes.write(&3u8.to_le_bytes()).unwrap();
  // root offset
  bytes.write(&header.root_offset.to_le_bytes()).unwrap();
  // root length
  bytes.write(&header.root_length.to_le_bytes()).unwrap();
  // metadata offset
  bytes.write(&header.metadata_offset.to_le_bytes()).unwrap();
  // metadata length
  bytes.write(&header.metadata_length.to_le_bytes()).unwrap();
  // leaf dir offset
  bytes.write(&header.leaf_dir_offset.to_le_bytes()).unwrap();
  // leaf dir length
  bytes.write(&header.leaf_dir_length.to_le_bytes()).unwrap();
  // tile data offset
  bytes.write(&header.tile_data_offset.to_le_bytes()).unwrap();
  // tile data length
  bytes.write(&header.tile_data_length.to_le_bytes()).unwrap();
  // num addressed tiles
  bytes
    .write(&header.num_addressed_tiles.to_le_bytes())
    .unwrap();
  // num tile entries
  bytes.write(&header.num_tile_entries.to_le_bytes()).unwrap();
  // num tile contents
  bytes
    .write(&header.num_tile_contents.to_le_bytes())
    .unwrap();
  // clustered
  bytes.write(&header.clustered.to_le_bytes()).unwrap();
  // internal compression enum
  bytes
    .write(&(header.internal_compression as u8).to_le_bytes())
    .unwrap();
  // tile compression enum
  bytes
    .write(&(header.tile_compression as u8).to_le_bytes())
    .unwrap();
  // tile type enum
  bytes
    .write(&(header.tile_type as u8).to_le_bytes())
    .unwrap();
  // minzoom
  bytes.write(&header.minzoom.to_le_bytes()).unwrap();
  // maxzoom
  bytes.write(&header.maxzoom.to_le_bytes()).unwrap();
  // min longitude
  bytes.write(&header.min_longitude.to_le_bytes()).unwrap();
  // min latitude
  bytes.write(&header.min_latitude.to_le_bytes()).unwrap();
  // max longitude
  bytes.write(&header.max_longitude.to_le_bytes()).unwrap();
  // max latitude
  bytes.write(&header.max_latitude.to_le_bytes()).unwrap();
  // center zoom
  bytes.write(&header.center_zoom.to_le_bytes()).unwrap();
  // center longitude
  bytes.write(&header.center_longitude.to_le_bytes()).unwrap();
  // center latitude
  bytes.write(&header.center_latitude.to_le_bytes()).unwrap();

  bytes
}

// def rotate(n, xy, rx, ry):
//     if ry == 0:
//         if rx == 1:
//             xy[0] = n - 1 - xy[0]
//             xy[1] = n - 1 - xy[1]
//         xy[0], xy[1] = xy[1], xy[0]

fn rotate(n: i64, xy: &mut [i64], rx: i64, ry: i64) {
  if ry == 0 {
    if rx == 1 {
      xy[0] = n - 1 - xy[0];
      xy[1] = n - 1 - xy[1];
    }
    let tmp = xy[0];
    xy[0] = xy[1];
    xy[1] = tmp;
  }
}

// def t_on_level(z, pos):
//     n = 1 << z
//     rx, ry, t = pos, pos, pos
//     xy = [0, 0]
//     s = 1
//     while s < n:
//         rx = 1 & (t // 2)
//         ry = 1 & (t ^ rx)
//         rotate(s, xy, rx, ry)
//         xy[0] += s * rx
//         xy[1] += s * ry
//         t //= 4
//         s *= 2
//     return z, xy[0], xy[1]
fn t_on_level(z: i64, pos: i64) -> (i64, i64, i64) {
  let n = 1 << z;
  let mut rx = 1 & (pos / 2);
  let mut ry = 1 & (pos ^ rx);
  let mut t = pos;
  let mut xy = [0, 0];
  let mut s = 1;
  while s < n {
    rx = 1 & (t / 2);
    ry = 1 & (t ^ rx);
    rotate(s, &mut xy, rx, ry);
    xy[0] += s * rx;
    xy[1] += s * ry;
    t /= 4;
    s *= 2;
  }
  (z, xy[0], xy[1])
}

// def zxy_to_tileid(z, x, y):
//     acc = 0
//     tz = 0
//     while tz < z:
//         acc += (0x1 << tz) * (0x1 << tz)
//         tz += 1
//     n = 1 << z
//     rx = 0
//     ry = 0
//     d = 0
//     xy = [x, y]
//     s = n // 2
//     while s > 0:
//         if (xy[0] & s) > 0:
//             rx = 1
//         else:
//             rx = 0
//         if (xy[1] & s) > 0:
//             ry = 1
//         else:
//             ry = 0
//         d += s * s * ((3 * rx) ^ ry)
//         rotate(s, xy, rx, ry)
//         s //= 2
//     return acc + d

fn zxy_to_tileid(z: i64, x: i64, y: i64) -> u64 {
  let mut acc = 0;
  let mut tz = 0;
  while tz < z {
    acc += (1 << tz) * (1 << tz);
    tz += 1;
  }
  let n = 1 << z;
  let mut rx = 0;
  let mut ry = 0;
  let mut d = 0;
  let mut xy = [x, y];
  let mut s = n / 2;
  while s > 0 {
    if (xy[0] & s) > 0 {
      rx = 1;
    } else {
      rx = 0;
    }
    if (xy[1] & s) > 0 {
      ry = 1;
    } else {
      ry = 0;
    }
    d += s * s * ((3 * rx) ^ ry);
    rotate(s, &mut xy, rx, ry);
    s /= 2;
  }
  acc + d as u64
}

// def tileid_to_zxy(tile_id):
//     num_tiles = 0
//     acc = 0
//     z = 0
//     while True:
//         num_tiles = (1 << z) * (1 << z)
//         if acc + num_tiles > tile_id:
//             return t_on_level(z, tile_id - acc)
//         acc += num_tiles
//         z += 1

fn tileid_to_zxy(tile_id: u64) -> (i64, i64, i64) {
  let mut num_tiles = 0;
  let mut acc = 0;
  let mut z = 0;
  loop {
    num_tiles = (1 << z) * (1 << z);
    if acc + num_tiles > tile_id {
      return t_on_level(z, (tile_id - acc) as i64);
    }
    acc += num_tiles;
    z += 1;
  }
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
    // magic number
    // self.f.write((0x4D50).to_bytes(2, byteorder="little"))
    out.write_all(&0x4D50u16.to_le_bytes()).unwrap();

    // version
    // self.f.write((2).to_bytes(2, byteorder="little"))
    out.write_all(&2u16.to_le_bytes()).unwrap();

    // metadata length
    // self.f.write(len(metadata_serialized).to_bytes(4, byteorder="little"))
    out
      .write_all(&(metadata_serialized.len() as u32).to_le_bytes())
      .unwrap();

    // root dir length
    // self.f.write(root_entries_len.to_bytes(2, byteorder="little"))
    out.write_all(&root_dir_len.to_le_bytes()).unwrap();

    // metadata
    // self.f.write(metadata_serialized.encode("utf-8"))
    out.write_all(metadata_serialized.as_bytes()).unwrap();
  }
}
