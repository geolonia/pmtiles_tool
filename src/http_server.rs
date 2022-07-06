use std::path::PathBuf;
use std::{convert::Infallible, net::SocketAddr};
use lazy_static::lazy_static;
use regex::Regex;
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use crate::reader;

async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
  lazy_static! {
    static ref RE: Regex = Regex::new(r"^/([0-9]+)/([0-9]+)/([0-9]+)\.(mvt|pbf)$").unwrap();
  }
  for cap in RE.captures_iter(&req.uri().path()) {
    let z = cap[1].parse::<u8>().unwrap();
    let x = cap[2].parse::<u32>().unwrap();
    let y = cap[3].parse::<u32>().unwrap();
    println!("{}/{}/{}", z, x, y);
  }
  println!("Body: {:?}", req);
  Ok(Response::new("Hello, world!".into()))
}

#[tokio::main]
pub async fn start_server(input: &PathBuf, port: u16) {
  let reader = reader::Reader::new(&input).unwrap();

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

  let addr = SocketAddr::from(([127, 0, 0, 1], port));
  let make_svc = make_service_fn(|_conn| async {
    Ok::<_, Infallible>(service_fn(handle))
  });
  let server = Server::bind(&addr).serve(make_svc);
  if let Err(e) = server.await {
    eprintln!("server error: {}", e);
  }
}
