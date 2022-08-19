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
  req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
  lazy_static! {
    static ref RE: Regex = Regex::new(r"^/([0-9]+)/([0-9]+)/([0-9]+)\.(mvt|pbf)$").unwrap();
  }
  if !RE.is_match(req.uri().path()) {
    return Ok(
      Response::builder()
        .status(404)
        .body(Body::from("Not found"))
        .unwrap(),
    );
  }

  if let Some(cap) = RE.captures_iter(req.uri().path()).next() {
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
  // GET /hello/warp => 200 OK with body "Hello, warp!"
  // let hello = warp::path!(u32 / u32 / u32)
  //     .map(|z, x, y| {
  //       // format!("Serving tile: {}/{}/{}", z, x, y)
  //       if let Some(tile) = &reader.get(z as u8, x, y) {
  //         return warp::http::Response::builder()
  //           .header("content-type", "application/x-protobuf")
  //           .body(tile);
  //       }

  //       format!("Tile not found: {}/{}/{}", z, x, y)
  //     });

  // warp::serve(hello)
  //   .run(([127, 0, 0, 1], port))
  //   .await;

  let manager = ReaderConnectionManager::new(input);
  let pool = r2d2::Pool::builder().max_size(10).build(manager).unwrap();

  let addr = SocketAddr::from(([127, 0, 0, 1], port));
  let make_svc = make_service_fn(move |conn: &AddrStream| {
    let the_pool = pool.clone();
    let addr = conn.remote_addr();

    // let input_1 = input.clone();

    let service = service_fn(move |req| handle(the_pool.clone(), addr, req));

    async move { Ok::<_, Infallible>(service) }
  });
  let server = Server::bind(&addr).serve(make_svc);
  if let Err(e) = server.await {
    eprintln!("server error: {}", e);
  }
}
