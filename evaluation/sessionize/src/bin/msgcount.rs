extern crate time;
extern crate timely;
extern crate timely_query;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::channels::message::Content;

use sessionize::sessionize::*;
use sessionize_shared::Message;
use sessionize_shared::util::{log_discretize, dump_histogram_hash_map};
use sessionize_shared::monitor::ThroughputPerSec;

fn main() {
    let start = time::precise_time_ns();

    let logdir = ::std::path::PathBuf::from(::std::env::args().nth(2)
        .expect("second arg needs to be logdir"));

    timely_query::execute(move |computation, coord| {
        let worker_index = computation.index();
    
        type Msg = MessagesForSession<Message>;
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let (input, sessionize) = scope.new_unordered_input::<Msg>();
            sessionize.throughput_per_sec(logdir.join(format!("msgcount_in.{}.csv", worker_index)));
            let histogram_message_count = sessionize.map(|messages_for_session| messages_for_session.messages.len()).histogram(|x| x.clone());

            histogram_message_count.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("MessageCountLog", worker_index, epoch, values, Some(|x| log_discretize(x as u64) as usize), false);
            });

            input
        });

        let name = format!("sessionize.{}", worker_index);
        let mut messages = coord.subscribe::<_, Msg>(&name, cap).unwrap().into_iter();
        loop {
            let input_start = time::precise_time_ns();
            let (time, mut data) = if let Some((time, data)) = messages.next() {
                (time, Content::Typed(data))
            } else {
                break;
            };
            input.session(time).give_content(&mut data);

            let process_start = time::precise_time_ns();
            computation.step();
            let iter_end = time::precise_time_ns();
            println!("iter:msgcount,{},{},{},{}", worker_index, input_start, process_start, iter_end);
        }
    }).unwrap();

    let end = time::precise_time_ns();
    println!("total:msgcount,{},{}", start, end);
}
