extern crate timely;
extern crate timely_query;

use timely::dataflow::operators::*;
use timely::dataflow::{Scope};
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::message::Content;

fn main() {
    timely_query::execute(|computation, coordinator| {
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let ((input, cap), stream) = scope.new_unordered_input();
            // here would be the dataflow graph instantation on `stream`
            (input, cap)
        });
        
        let sub = coordinator.subscribe::<_, String>("example".into(), cap).unwrap();
        for (time, data) in sub {
            input.session(time).give_content(&mut Content::Typed(data));
            computation.step();
        }
    }).unwrap();
}
