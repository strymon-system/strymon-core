extern crate timely;
extern crate timely_query;

use timely::dataflow::operators::{Inspect, Filter, ToStream};

fn main() {
    timely_query::execute(|root, _| {
            root.dataflow::<u32, _, _>(|scope| {
                (0..100)
                    .to_stream(scope)
                    .filter(|x| x % 2 == 0)
                    .inspect(|x| println!("hello {:?}", x));
            });
        })
        .unwrap();
}
