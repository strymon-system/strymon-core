extern crate timely_query;
extern crate env_logger;

use std::env;
use timely_query::executor::Executor;

fn main() {
    drop(env_logger::init());

    let coord = env::args().nth(1).unwrap_or("localhost:9189".to_string());
    let host = env::args().nth(1).unwrap_or("localhost".to_string());
    let ports = 2101..4000; // TODO!!
    let exec = Executor::new(coord, host, ports).expect("failed to connect to coordinator");
    exec.run();
}
