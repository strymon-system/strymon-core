extern crate timely;
extern crate timely_query;

use timely::dataflow::operators::*;
use timely::dataflow::{Scope};
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::channels::pact::Pipeline;

fn main() {
    timely_query::execute(|root, coord| {
        let (mut input, mut cap) = root.scoped::<i32, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            stream.unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
                input.for_each(|time, data| {
                    println!("sub frontier in input: {:?}", notificator.frontier(0));
                    println!("sub {:?}: {:?}", &time.time(), &data[..]);
                    output.session(&time).give_content(data);
                });
            });
            input
        });
        
        let sub = coord.subscribe::<_, i32>("frontier".into(), cap).unwrap();
        for (time, data) in sub {
            input.session(time).give_iterator(data.into_iter());
            root.step();
        }
    }).unwrap();
}
