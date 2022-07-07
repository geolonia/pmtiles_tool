use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crate::writer;

pub fn mbtiles_to_pmtiles(input: PathBuf, output: PathBuf) {
  let mut writer = writer::Writer::new(&output);

  let input_queue_tx = writer.input_queue_tx.clone();
  let thread_input = input.clone();
  let input_done = Arc::clone(&writer.input_done);
  let input_thread_handle = thread::spawn(move || {
    let connection = sqlite::open(thread_input).unwrap();
    connection.execute("PRAGMA query_only = true;").unwrap();

    let mut statement = connection
      .prepare(
        "
      SELECT
        zoom_level,
        tile_column,
        tile_row,
        tile_data
      FROM
        tiles
      ORDER BY zoom_level, tile_column, tile_row ASC
    ",
      )
      .unwrap();
    while let sqlite::State::Row = statement.next().unwrap() {
      let zoom_level = statement.read::<i64>(0).unwrap();
      let tile_column = statement.read::<i64>(1).unwrap();
      let tile_row = statement.read::<i64>(2).unwrap();
      let tile_data = statement.read::<Vec<u8>>(3).unwrap();

      // flipped = (1 << row[0]) - 1 - row[2]
      let flipped_row = (1 << zoom_level) - 1 - tile_row;

      input_queue_tx
        .send(writer::WorkJob {
          zoom_level,
          tile_column,
          tile_row: flipped_row,
          tile_data,
        })
        .unwrap();
    }
    println!("Done reading input from mbtiles.");
    input_done.store(true);
  });

  let connection = sqlite::open(input).unwrap();
  connection.execute("PRAGMA query_only = true;").unwrap();
  let mut metadata_raw = HashMap::<String, String>::new();
  let mut metadata_stmt = connection
    .prepare(
      "
    SELECT name,value FROM metadata;
  ",
    )
    .unwrap();
  while let sqlite::State::Row = metadata_stmt.next().unwrap() {
    let name = metadata_stmt.read::<String>(0).unwrap();
    let value = metadata_stmt.read::<String>(1).unwrap();
    metadata_raw.insert(name, value);
  }
  // We are removing compression from the tile, so we need to remove the compression flag.
  metadata_raw.remove("compression");

  writer.run(&metadata_raw);
  input_thread_handle.join().unwrap();

  println!("Filled {} with all the good things", output.display());
}
