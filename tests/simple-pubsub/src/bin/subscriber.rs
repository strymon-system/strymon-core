// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate strymon_job;
extern crate timely;

use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::{Accumulate, CapabilitySet, Inspect};
use strymon_job::operators::subscribe::SubscriptionEvent;

fn main() {
    strymon_job::execute(|root, coord| {
        root.dataflow::<u64, _, _>(|scope| {
            let mut count = 0;

            source::<_, u32, _, _>(scope, "Source", |root| {
                    let mut capabilities = CapabilitySet::new();
                    capabilities.insert(root);

                    let mut subscription = coord.subscribe("test", true)
                        .expect("failed to subscribe")
                        .into_iter();

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
            .accumulate(0, |sum, data| { for &x in data.iter() { *sum += x; } })
            // ensure that each batch contains all published tuples
            .inspect_batch(move |t, xs| {
                let i = (t.inner % 10) as u32;
                assert_eq!(i * (i+1), xs[0]);

                count += 1;
                if (count % 1000) == 0 {
                    println!("Subscriber received {} batches", count);
                }
            });
        });

    }).unwrap();
}
