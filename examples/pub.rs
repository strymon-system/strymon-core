extern crate timely;
extern crate timely_query;
extern crate env_logger;

use std::time::Duration;
use std::thread;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use timely_query::Publish;

fn main() {
    env_logger::init().unwrap();
    timely_query::execute(|root, catalog| {
        let mut input = root.scoped::<i32,_,_>(|scope| {
            let worker_id = (scope.index() + 1) as i32;
            let (input, stream) = scope.new_input();
            stream
                .map(move |i| (worker_id, i))
                .inspect(move |x| println!("pub({:?}): {:?}", worker_id, x))
                .publish("numbers", &catalog).expect("failed to publish stream");

            input
        });

        

        for round in 0..1000 {
            input.send(round as i32);
            input.advance_to(round + 1);
            root.step();
            thread::sleep(Duration::from_millis(10));
        }
    }).unwrap();
}
