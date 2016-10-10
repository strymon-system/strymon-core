extern crate timely_query;
extern crate env_logger;

use std::env;
use timely_query::coordinator2;

fn main() {
    drop(env_logger::init());

    // coordinator1
    //let addr = env::args().nth(1).unwrap_or("[::]:9189".to_string());
    //if let Err(err) = coordinator::coordinate(&addr) {
    //    panic!("failed to initalize coordinator: {:?}", err);
    //}
    coordinator2::coordinate(9189).unwrap();
}
