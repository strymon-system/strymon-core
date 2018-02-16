// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate strymon_job;
extern crate timely;

use std::env;

use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::{Accumulate, Inspect};

fn main() {
    let num_partitions: usize = env::args().nth(1).unwrap().parse().unwrap();

    strymon_job::execute(move |root, coord| {
        root.dataflow::<u64, _, _>(move |scope| {
            let mut count = 0;

            source::<_, u32, _, _>(scope, "Source", move |root| {
                    let mut subscription = coord
                        .subscribe_group("partitioned_test", 0..num_partitions, root, true)
                        .expect("failed to subscribe")
                        .into_iter();

                    move |output| {
                        if let Some((t, data)) = subscription.next().map(Result::unwrap) {
                            output.session(&t)
                                  .give_iterator(data.into_iter());
                        }
                    }
            })
            .inspect_batch(|t, xs| assert!(xs.iter().all(|&x| x as u64 == t.inner)))
            .count()
            .inspect(move |&batch_count| {
                let expected = (num_partitions * (num_partitions + 1)) / 2;
                assert_eq!(expected, batch_count);

                count += 1;
                if (count % 1000) == 0 {
                    println!("Subscriber received {} batches", count);
                }
            });
        });

    }).unwrap();
}
