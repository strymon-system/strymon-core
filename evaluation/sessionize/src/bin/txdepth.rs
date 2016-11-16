extern crate time;
extern crate timely;
extern crate timely_query;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::channels::message::Content;

use sessionize::sessionize::*;
use sessionize_shared::util::{dump_histogram_hash_map};
use sessionize_shared::monitor::ThroughputPerSec;

fn main() {
    let start = time::precise_time_ns();

    let logdir = ::std::path::PathBuf::from(::std::env::args().nth(2)
        .expect("second arg needs to be logdir"));

    timely_query::execute(move |computation, coord| {
        let worker_index = computation.index();
    
        type Txns = Vec<Vec<u32>>;
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let (input, transactions) = scope.new_unordered_input::<Txns>();
            transactions.throughput_per_sec(logdir.join(format!("txdepth_in.{}.csv", worker_index)));
            // Leaf Query: Transaction tree depth
            let histogram_txn_depth = transactions.map(|txns_in_messages| txns_in_messages.iter().map(|x| x.len()).max().unwrap())
                                                                        .histogram(|x| x.clone());

            histogram_txn_depth.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("TrxnDepth", worker_index, epoch, values, Some(|x| x), true);
            });

            input
        });

        let name = format!("transactions.{}", worker_index);
        let mut messages = coord.subscribe::<_, Txns>(name, cap).unwrap().into_iter();
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
            println!("iter:txdepth,{},{},{},{}", worker_index, input_start, process_start, iter_end);
        }
    }).unwrap();

    let end = time::precise_time_ns();
    println!("total:txdepth,{},{}", start, end);
}
