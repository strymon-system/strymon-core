extern crate time;
extern crate timely;
extern crate timely_query;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use sessionize::sessionize::*;
use sessionize_shared::util::{dump_histogram_hash_map};

fn main() {
    let start = time::precise_time_ns();

    timely_query::execute(|computation, coord| {
        let worker_index = computation.index();
    
        type Txns = Vec<Vec<u32>>;
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let (input, transactions) = scope.new_unordered_input::<Txns>();

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
        let messages = coord.subscribe::<_, Txns>(name, cap).unwrap();
        for (time, data) in messages {
            input.session(time).give_iterator(data.into_iter());
            computation.step();
        }
    }).unwrap();

    let end = time::precise_time_ns();
    println!("txdepth,{},{}", start, end);
}
