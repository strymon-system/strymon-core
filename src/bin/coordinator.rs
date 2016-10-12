extern crate timely_query;
extern crate env_logger;

use timely_query::coordinator;

fn main() {
    drop(env_logger::init());

    if let Err(err) = coordinator::coordinate(9189) {
        panic!("failed to run coordinator: {:?}", err);
    }
}
