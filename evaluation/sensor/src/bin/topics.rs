extern crate timely;
extern crate timely_query;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use timely_query::model::Topic;

fn main() {
    timely_query::execute(|root, coord| {
            let mut input = root.scoped::<i32, _, _>(|scope| {
                let (input, stream) = scope.new_input();

                stream.inspect(|&(ref topic, delta): &(Topic, i32)| {
                    if delta > 0 {
                        println!("Added {:#?}", topic)
                    } else {
                        println!("Removed Topic {:?}", topic.name)
                    }
                });

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
