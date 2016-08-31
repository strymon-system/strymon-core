extern crate timely_query;
extern crate env_logger;

use std::env;
use timely_query::executor::Executor;

fn main() {
    drop(env_logger::init());

    let addr = env::args().nth(1).unwrap_or("localhost:9189".to_string());
    Executor::new(&addr).expect("failed to connect to coordinator");
}
