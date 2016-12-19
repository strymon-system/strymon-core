extern crate timely;
extern crate timely_query;

use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::channels::pact::Pipeline;

use timely_query::publish::Partition as Part;

fn main() {
    timely_query::execute(|root, coord| {
            let (mut input, mut cap) = root.scoped(|scope| {
                let (input, stream) = scope.new_unordered_input();

                coord.publish("frontier", &stream, Part::Merge).unwrap();

                stream.unary_notify(Pipeline,
                                    "example",
                                    Vec::new(),
                                    |input, output, notificator| {
                    input.for_each(|time, data| {
                        println!("pub frontier in input: {:?}", notificator.frontier(0));
                        println!("pub {:?}: {:?}", &time.time(), &data[..]);
                        output.session(&time).give_content(data);
                    });
                });
                input
            });

            let mut _default = cap.clone();
            for round in 0..10i32 {
                input.session(cap.clone()).give(round);
                if round == 5 {
                    _default = cap.delayed(&RootTimestamp::new(10));
                }
                cap = cap.delayed(&RootTimestamp::new(round + 1));
                root.step();
            }
        })
        .unwrap();
}
