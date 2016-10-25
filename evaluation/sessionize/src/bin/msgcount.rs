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

            let histogram_message_count = sessionize.map(|messages_for_session| messages_for_session.messages.len()).histogram(|x| x.clone());
            histogram_message_count.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("MessageCountLog", worker_index, epoch, values, Some(|x| log_discretize(x as u64) as usize), false);
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
    println!("msgcount,{},{}", start, end);
}
