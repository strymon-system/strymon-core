extern crate timely_query;
extern crate futures;

use std::env;
use std::net::SocketAddr;

use timely_query::coordinator;

fn main() {
    let addr = env::args().nth(1).unwrap_or("[::]:9189".to_string());
    let addr = addr.parse::<SocketAddr>().expect("invalid listener socket addr");
}
