extern crate time;
extern crate timely;
extern crate timely_query;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use sessionize::sessionize::*;
use sessionize_shared::util::{dump_histogram_hash_map};
use sessionize_shared::reconstruction;

fn main() {
    let start = time::precise_time_ns();

    timely_query::execute(|computation, coord| {
        let worker_index = computation.index();
    
        type Txns = Vec<Vec<u32>>;
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let (input, transactions) = scope.new_unordered_input::<Txns>();

            // Leaf Query: Top-k transaction tree patterns per epoch
            let histogram_txn_type = transactions.map(|txns_in_messages| reconstruction::reconstruct(&txns_in_messages))
                                        .filter(|txn_shape| txn_shape.len() <= 25)
                                        .topk(|x| x.clone(), 10);
            histogram_txn_type.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("TrxnTypeTop10", worker_index, epoch, values, Some(|x| x), true);
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
    println!("txsigtop10,{},{}", start, end);
}
