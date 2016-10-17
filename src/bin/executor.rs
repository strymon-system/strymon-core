extern crate timely_query;
extern crate env_logger;

use timely_query::executor;

fn main() {
    drop(env_logger::init());

    let executor = executor::Builder::default();
    executor.start().unwrap();
}
