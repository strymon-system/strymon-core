extern crate timely;
extern crate timely_query;

use timely::dataflow::operators::{Filter, ToStream};

use timely_query::publish::Partition;

fn main() {
    timely_query::execute(|root, coord| {
            root.dataflow::<u64, _, _>(|scope| {
                let n = scope.peers();
                let numbers = (n * 100..(n + 1) * 100).to_stream(scope);
                // results in `n` topics: "numbers.0", "numbers.1", ...
                coord.publish("numbers", &numbers, Partition::PerWorker)
                    .expect("failed to publish topic");

                // filtering performed by each worker
                let primes = numbers.filter(|x| x % 2 == 1);

                // results in a single topic containing all "odds",
                // published by worker number 0
                coord.publish("odds", &primes, Partition::Merge)
                    .expect("failed to publish topic");

            });
        })
        .unwrap();
}
