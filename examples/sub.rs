extern crate timely;
extern crate timely_query;
extern crate env_logger;
extern crate futures;

use futures::Future;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::channels::message::Content;

fn main() {
    drop(env_logger::init());

    timely_query::execute(|root, coord| {
            let mut input = root.scoped::<i32, _, _>(|scope| {
                let (input, stream) = scope.new_unordered_input();
                stream.filter(|&(_, x)| x % 10 != 0)
                    .inspect(|x| println!("sub: {:?}", x));

                input
            });

            let subscriber = coord.blocking_subscribe::<(i32, i32), _>("foo")
                .wait()
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
