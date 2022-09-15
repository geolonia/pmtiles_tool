extern crate r2d2;

use crate::reader;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use lazy_static::lazy_static;
use regex::Regex;
use std::path::{Path, PathBuf};
use std::{convert::Infallible, net::SocketAddr};

struct ReaderConnectionManager {
  path: PathBuf,
}

impl ReaderConnectionManager {
  fn new(path: &Path) -> ReaderConnectionManager {
    ReaderConnectionManager {
      path: path.to_path_buf(),
    }
  }
}

impl r2d2::ManageConnection for ReaderConnectionManager {
  type Connection = reader::Reader;
  type Error = std::io::Error;

  fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
    reader::Reader::new(&self.path)
  }

  fn is_valid(&self, _conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
    Ok(())
  }

  fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
    false
  }
}

async fn handle(
  reader_pool: r2d2::Pool<ReaderConnectionManager>,
  _addr: SocketAddr,
  port: u16,
  req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
  lazy_static! {
    static ref META_RE: Regex = Regex::new(r"^/tiles\.json$").unwrap();
    static ref TILE_RE: Regex = Regex::new(r"^/([0-9]+)/([0-9]+)/([0-9]+)\.(mvt|pbf)$").unwrap();
  }
  let path = req.uri().path();
  if !TILE_RE.is_match(path) && !META_RE.is_match(path) {
    return Ok(
      Response::builder()
        .status(404)
        .body(Body::from("Not found"))
        .unwrap(),
    );
  }

  if META_RE.captures_iter(path).next().is_some() {
    println!("Serving tile.json");
    let reader = reader_pool.get().unwrap();
    let orig_metadata = reader.get_metadata();
    if let serde_json::Value::Object(mut metadata) = orig_metadata {
      if let Some(bounds) = metadata.get("bounds") {
        let bounds_array = bounds
          .as_str()
          .unwrap()
          .split(',')
          .map(|s| serde_json::Value::Number(s.parse().unwrap()))
          .collect();
        metadata.insert("bounds".to_string(), serde_json::Value::Array(bounds_array));
      }
      if let Some(center) = metadata.get("center") {
        let center_array = center
          .as_str()
          .unwrap()
          .split(',')
          .map(|s| serde_json::Value::Number(s.parse().unwrap()))
          .collect();
        metadata.insert("center".to_string(), serde_json::Value::Array(center_array));
      }
      if let Some(minzoom) = metadata.get("minzoom") {
        let minzoom_num = minzoom.as_str().unwrap().parse().unwrap();
        metadata.insert(
          "minzoom".to_string(),
          serde_json::Value::Number(minzoom_num),
        );
      }
      if let Some(maxzoom) = metadata.get("maxzoom") {
        let maxzoom_num = maxzoom.as_str().unwrap().parse().unwrap();
        metadata.insert(
          "maxzoom".to_string(),
          serde_json::Value::Number(maxzoom_num),
        );
      }
      if let Some(json) = metadata.get("json") {
        let raw_json = json.as_str().unwrap();
        let parsed_json: serde_json::Value = serde_json::from_str(raw_json).unwrap();
        metadata.remove("json");
        for (key, value) in parsed_json.as_object().unwrap().iter() {
          metadata.insert(key.to_string(), value.clone());
        }
      }

      metadata.insert(
        "tiles".to_string(),
        serde_json::Value::Array(vec![serde_json::Value::String(format!(
          "http://localhost:{}/{{z}}/{{x}}/{{y}}.pbf",
          port
        ))]),
      );

      return Ok(
        Response::builder()
          .status(200)
          .header("access-control-allow-origin", "*")
          .body(Body::from(serde_json::to_string(&metadata).unwrap()))
          .unwrap(),
      );
    } else {
      panic!();
    }
  }

  if let Some(cap) = TILE_RE.captures_iter(path).next() {
    let z = cap[1].parse::<u8>().unwrap();
    let x = cap[2].parse::<u32>().unwrap();
    let y = cap[3].parse::<u32>().unwrap();
    println!("Serving tile: {}/{}/{}", z, x, y);
    let reader = reader_pool.get().unwrap();
    if let Some(tile_data) = reader.get(z, x, y) {
      let my_tile_data: Vec<u8> = tile_data.to_vec();

      return Ok(
        Response::builder()
          .status(200)
          .header("content-type", "application/x-protobuf")
          .header("access-control-allow-origin", "*")
          .body(Body::from(my_tile_data))
          .unwrap(),
      );
    } else {
      return Ok(
        Response::builder()
          .status(204)
          .body(Body::from(""))
          .unwrap(),
      );
    }
  }

  Ok(Response::new("Hello, world!".into()))
}

#[tokio::main]
pub async fn start_server(input: &Path, port: u16) {
  let manager = ReaderConnectionManager::new(input);
  let pool = r2d2::Pool::builder().max_size(10).build(manager).unwrap();

  let addr = SocketAddr::from(([127, 0, 0, 1], port));
  let make_svc = make_service_fn(move |conn: &AddrStream| {
    let the_pool = pool.clone();
    let addr = conn.remote_addr();

    let service = service_fn(move |req| handle(the_pool.clone(), addr, port, req));

    async move { Ok::<_, Infallible>(service) }
  });
  let server = Server::bind(&addr).serve(make_svc);
  if let Err(e) = server.await {
    eprintln!("server error: {}", e);
  }
}
