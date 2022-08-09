use memmap2::Mmap;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Eq, Hash, PartialEq)]
struct DirectoryKey {
  z: u8,
  x: u32,
  y: u32,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct DirectoryEntry {
  offset: u64,
  length: u32,
}

type Directory = HashMap<DirectoryKey, DirectoryEntry>;

pub struct Reader {
  mmap: Mmap,
  pub version: u16,
  metadata_len: usize,
  pub root_entries_len: usize,
  root_dir: Directory,
  leaves: Directory,
  pub leaves_len: usize,
  leaf_level: u8,
}

fn load_directory(mmap: &Mmap, offset: usize, num_entries: usize) -> (Directory, Directory, u8) {
  let mut directory = Directory::with_capacity(num_entries);
  let mut leaves = Directory::with_capacity(num_entries);
  let mut leaf_level: u8 = 0;

  for i in 0..num_entries {
    let i_offset = offset + (i * 17);

    let mut z_bytes = [0u8; 1];
    z_bytes.copy_from_slice(&mmap[i_offset..i_offset + 1]);
    let z = u8::from_le_bytes(z_bytes);

    let mut x_bytes = [0u8; 4];
    x_bytes[0..3].copy_from_slice(&mmap[i_offset + 1..i_offset + 4]);
    let x = u32::from_le_bytes(x_bytes);

    let mut y_bytes = [0u8; 4];
    y_bytes[0..3].copy_from_slice(&mmap[i_offset + 4..i_offset + 7]);
    let y = u32::from_le_bytes(y_bytes);

    let mut tile_off_bytes = [0u8; 8];
    tile_off_bytes[0..6].copy_from_slice(&mmap[i_offset + 7..i_offset + 13]);
    let tile_off = u64::from_le_bytes(tile_off_bytes);

    let mut tile_len_bytes = [0u8; 4];
    tile_len_bytes.copy_from_slice(&mmap[i_offset + 13..i_offset + 17]);
    let tile_len = u32::from_le_bytes(tile_len_bytes);

    // let key = DirectoryKey { z, x, y };
    let entry = DirectoryEntry {
      offset: tile_off,
      length: tile_len,
    };

    if z & 0b10000000 > 0 {
      let unwrapped_z = z & 0b01111111;
      if leaf_level == 0 {
        leaf_level = unwrapped_z;
      }
      // println!("{:02X?} {}/{}/{} (d) : {:?}", &mmap[i_offset..i_offset+17], unwrapped_z, x, y, entry);
      leaves.insert(
        DirectoryKey {
          z: unwrapped_z,
          x,
          y,
        },
        entry,
      );
    } else {
      // println!("{:02X?} {}/{}/{} (t) : {:?}", &mmap[i_offset..i_offset+17], z, x, y, entry);
      directory.insert(DirectoryKey { z, x, y }, entry);
    }
  }

  (directory, leaves, leaf_level)
}

impl Reader {
  pub fn new(path: &PathBuf) -> Result<Reader, std::io::Error> {
    // println!("Reading {}", path.display());
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    assert_eq!(&0x4D50u16.to_le_bytes(), &mmap[0..2]);

    let mut version_bytes = [0u8, 2];
    version_bytes.copy_from_slice(&mmap[2..4]);
    let version = u16::from_le_bytes(version_bytes);

    let mut root_entries_len_bytes = [0u8; 2];
    root_entries_len_bytes.copy_from_slice(&mmap[8..10]);
    let root_entries_len = u16::from_le_bytes(root_entries_len_bytes) as usize;

    let mut metadata_len_bytes = [0u8; 4];
    metadata_len_bytes.copy_from_slice(&mmap[4..8]);
    let metadata_len = u32::from_le_bytes(metadata_len_bytes) as usize;

    let first_entry_idx = 10 + metadata_len;
    let (root_dir, leaves, leaf_level) = load_directory(&mmap, first_entry_idx, root_entries_len);

    let leaf_tiles = HashSet::<_>::from_iter(leaves.values());
    let leaves_len = leaf_tiles.len();

    Ok(Reader {
      mmap,
      version,
      metadata_len,
      root_entries_len,
      root_dir,
      leaves,
      leaves_len,
      leaf_level,
    })
  }

  pub fn get_metadata(&self) -> serde_json::Value {
    let raw_json = &self.mmap[10..(10 + self.metadata_len)];
    serde_json::from_slice(raw_json).unwrap()
  }

  pub fn get(&self, z: u8, x: u32, y: u32) -> Option<&[u8]> {
    if let Some(val) = self.root_dir.get(&DirectoryKey { z, x, y }) {
      let offset = val.offset;
      let length = val.length;
      let slice = &self.mmap[offset as usize..(offset + length as u64) as usize];
      // println!("Found in root: z={}, x={}, y={}, offset={}, length={}, slice_len={}", z, x, y, offset, length, slice.len());
      return Some(slice);
    } else if self.leaves_len > 0 && z >= self.leaf_level {
      let level_diff = z - self.leaf_level;
      let leaf = DirectoryKey {
        z: self.leaf_level,
        x: x / (1 << level_diff),
        y: y / (1 << level_diff),
      };
      if let Some(val) = self.leaves.get(&leaf) {
        let (directory, _, _) =
          load_directory(&self.mmap, val.offset as usize, val.length as usize / 17);
        if let Some(val) = directory.get(&DirectoryKey { z, x, y }) {
          let offset = val.offset;
          let length = val.length;
          let slice = &self.mmap[offset as usize..(offset + length as u64) as usize];
          return Some(slice);
        }
      }
    }
    None
  }
}
