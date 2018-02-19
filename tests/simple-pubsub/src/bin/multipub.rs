// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate strymon_job;
extern crate timely;

use std::iter;

use timely::dataflow::operators::{Probe, UnorderedInput};
use timely::progress::timestamp::RootTimestamp;

use strymon_job::operators::publish::Partition;

fn main() {
    strymon_job::execute(|root, coord| {
        let ((mut input, mut cap), probe) = root.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input::<u32>();

            coord
                .publish("partitioned_test", &stream, Partition::PerWorker)
                .expect("failed to publish test topic");

            (input, stream.probe())
        });

        for i in 0.. {
            cap.downgrade(&RootTimestamp::new(i));
            input
                .session(cap.clone())
                .give_iterator(iter::repeat(i as u32).take(root.index()+1));
            while probe.less_than(cap.time()) {
                root.step();
            }
        }
    }).unwrap();
}
