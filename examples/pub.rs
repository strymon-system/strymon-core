extern crate timely;
extern crate timely_query;
extern crate env_logger;

use std::time::Duration;
use std::thread;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;


fn main() {
    drop(env_logger::init());
    println!("hi");
    //timely_query::execute(|root, coord| {});
}
