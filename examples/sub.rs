extern crate timely;
extern crate timely_query;
extern crate env_logger;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use timely_query::Subscriber;

fn main() {
    drop(env_logger::init());

    timely_query::execute(|root, catalog| {
        let mut input = root.scoped::<i32,_,_>(|scope| {
            let (input, stream) = scope.new_input();
            stream
                .filter(|&(_, x)| x % 10 != 0)
                .inspect(|x| println!("sub: {:?}", x));
            
            input
        });


        let subscriber = Subscriber::<(i32, i32)>::from(&catalog, "numbers")
                                    .expect("failed to connect to publisher");

        for (item, ts) in subscriber.zip(1..) {
            input.send(item);
            input.advance_to(ts);
            root.step();
        }

    }).unwrap();
}
