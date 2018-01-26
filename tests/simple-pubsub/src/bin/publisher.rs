// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate strymon_job;
extern crate timely;

use timely::dataflow::operators::{Probe, UnorderedInput};
use timely::progress::timestamp::RootTimestamp;

use strymon_job::operators::publish::Partition;

fn main() {
    strymon_job::execute(|root, coord| {
        let ((mut input, mut cap), probe) = root.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input::<u32>();

            coord.publish("test", &stream, Partition::Merge)
                .expect("failed to publish test topic");

            (input, stream.probe())
        });

        // This producer has a specific pyramid pattern for timestamps, which
        // sends a high timestamp early bump the upper frontier.
        // 9, 8, .. 0, [close 0], 9, 8, .. 1 [close 1], 9, 8, .. 2 [close 2] ...
        for i in (0..).map(|i| i * 10) {
            for j in 0..10 {
                cap.downgrade(&RootTimestamp::new(i + j));
                for k in (j..10).rev() {
                    let time = cap.delayed(&RootTimestamp::new(i + k));
                    input.session(time).give(k as u32);
                }

                while probe.less_than(cap.time()) {
                    root.step();
                }
            }
        }
    }).unwrap();
}
