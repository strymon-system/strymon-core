extern crate time;
extern crate timely;
extern crate timely_query;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use sessionize::sessionize::*;
use sessionize_shared::Message;
use sessionize_shared::util::{log_discretize, dump_histogram_hash_map};

fn main() {
    let start = time::precise_time_ns();

    timely_query::execute(|computation, coord| {
        let worker_index = computation.index();
    
        type Msg = MessagesForSession<Message>;
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let (input, sessionize) = scope.new_unordered_input::<Msg>();

            // Leaf Query: Session duration 
            let histogram_log_span = sessionize.filter(|messages_for_session| messages_for_session.messages.len() >= 2)
                                                .map(|messages_for_session : MessagesForSession<Message>| messages_for_session.messages.iter()
                                                .map(|m| m.time).max().unwrap() - messages_for_session.messages.iter()
                                                .map(|m| m.time).min().unwrap()).histogram(|x| log_discretize(x.clone()));
            histogram_log_span.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("LogMessageSpan", worker_index, epoch, values, Some(|x| x), true);
            });

            input
        });

        let name = format!("sessionize.{}", worker_index);
        let messages = coord.subscribe::<_, Msg>(name, cap).unwrap();
        for (time, data) in messages {
            input.session(time).give_iterator(data.into_iter());
            computation.step();
        }
    }).unwrap();

    let end = time::precise_time_ns();
    println!("msgspan,{},{}", start, end);
}
