extern crate timely;
extern crate paperboy;
extern crate env_logger;

use std::time::Duration;
use std::thread;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use timely_query::Subscribe;

fn main() {
    drop(env_logger::init());

    scope.subscribe::<i32, _>("foo", )

    timely_query::execute(|root| {
        let mut input = root.scoped::<i32,_,_>(|scope| {
            let (input, stream) = scope.new_input();
            stream
                .filter(|&(_, x)| x % 10 != 0)
                .inspect(|x| println!("sub: {:?}", x));
            
            input
        });

        let mut catalog = Catalog::from_env()
                                  .expect("failed to connect to coordinator");
        let topic = polling_lookup("numbers", &mut catalog);
        let subscriber = Subscriber::<(i32, i32)>::from(topic)
                                    .expect("failed to connect to publisher");

        for (item, ts) in subscriber.zip(1..) {
            input.send(item);
            input.advance_to(ts);
            root.step();
        }

    }).unwrap();
}
