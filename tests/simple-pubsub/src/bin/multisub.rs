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
use timely::dataflow::operators::{Accumulate, CapabilitySet, Inspect};
use strymon_job::operators::subscribe::SubscriptionEvent;

fn main() {
    let num_partitions: usize = env::args().nth(1).unwrap().parse().unwrap();

    strymon_job::execute(move |root, coord| {
        root.dataflow::<u64, _, _>(move |scope| {
            let mut count = 0;

            source::<_, u32, _, _>(scope, "Source", move |root| {
                    let mut capabilities = CapabilitySet::new();
                    capabilities.insert(root);

                    let mut subscription = coord
                        .subscribe_group("partitioned_test", 0..num_partitions, true)
                        .expect("failed to subscribe")
                        .into_iter();

                    println!("Multisub: Subscription successful.");
                    move |output| {
                        if let Some(event) = subscription.next() {
                            match event.unwrap() {
                                SubscriptionEvent::Data(time, data) => {
                                    output.session(&capabilities.delayed(&time))
                                      .give_iterator(data.into_iter());
                                }
                                SubscriptionEvent::FrontierUpdate => {
                                    capabilities.downgrade(subscription.frontier());
                                }
                            }
                        }
                    }
            })
            .inspect_batch(|t, xs| assert!(xs.iter().all(|&x| x as u64 == t.inner)))
            .count()
            .inspect(move |&batch_count| {
                let expected = (num_partitions * (num_partitions + 1)) / 2;
                assert_eq!(expected, batch_count);

                count += 1;
                if (count % 100) == 0 {
                    println!("Subscriber received {} batches", count);
                }
            });
        });

    }).unwrap();
}
