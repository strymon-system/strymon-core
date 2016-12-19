extern crate timely;
extern crate timely_query;
extern crate env_logger;
extern crate futures;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use timely_query::model::Topic;

fn main() {
    drop(env_logger::init());

    timely_query::execute(|root, coord| {
            let mut input = root.scoped::<i32, _, _>(|scope| {
                let (input, stream) = scope.new_input();
                stream.inspect(|x| println!("topic event: {:?}", x));

                input
            });

            let subscriber = coord.subscribe_collection::<(Topic, i32)>("$topics")
                .unwrap()
                .into_iter()
                .flat_map(|vec| vec);

            for (item, ts) in subscriber.zip(1..) {
                input.send(item);
                input.advance_to(ts);
                root.step();
            }

        })
        .unwrap();
}
