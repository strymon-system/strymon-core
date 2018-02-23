// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
#![allow(deprecated)]

extern crate strymon_job;
extern crate timely;
extern crate differential_dataflow;
extern crate topology_generator;

use differential_dataflow::operators::{Count, Distinct, Group, Iterate, Join};
use differential_dataflow::collection::AsCollection;

use timely::dataflow::operators::{Capability, CapabilitySet, Map, UnorderedInput};
use timely::dataflow::operators::unordered_input::UnorderedHandle;

use timely::progress::nested::product::Product;
use timely::progress::timestamp::{Timestamp, RootTimestamp};

use strymon_job::operators::subscribe::SubscriptionEvent;

use topology_generator::Entity;
use topology_generator::service::{FetchTopology, TopologyUpdate, Version};

fn feed_input<T: Timestamp>(
    input: &mut UnorderedHandle<T, (Entity, T, isize)>,
    version: Capability<T>, data: TopologyUpdate)
{
    let capability = version;
    let version = capability.time().clone();
    let data = data.into_iter().map(|(entity, diff)| {
        (entity, version.clone(), diff as isize)
    });
    input.session(capability).give_iterator(data);
}

// This is the naive implementation of connected components based on the example
// in Frank McSherry's Differential Dataflow repository. For more details on
// how this could be optmized further, see:
// https://github.com/frankmcsherry/blog/blob/master/posts/2015-12-24.md
fn main() {
    strymon_job::execute(|root, coord| {
        let ((mut input, cap), probe) = root.dataflow::<Version, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input::<(Entity, Product<RootTimestamp, u64>, isize)>();

            use timely::dataflow::operators::Inspect;

            let edges = stream.flat_map(|(entity, ts, diff)| {
                if let Entity::Connection(conn) = entity {
                    Some((conn, ts, diff))
                } else {
                    None
                }
            }).as_collection();

            let nodes = stream.flat_map(|(entity, ts, diff)| {
                if let Entity::Switch(sw) = entity {
                    Some((sw, ts, diff))
                } else {
                    None
                }
            }).as_collection();

            // (node, label)
            let labels = nodes.map(|x| (x, x));
            // (src, dest)
            let edges = edges.map(|(from, to, _weight)| (from, to));
            // insert back edges as well
            let edges = edges.concat(&edges.map(|(from, to)| (to, from)));

            // iterate labels until there are no more changes
            let labels = labels.iterate(|inner| {
                let edges = edges.enter(&inner.scope());
                let labels = labels.enter(&inner.scope());

                inner.join_map(&edges, |_src,label,dst| (*dst,*label)) // propagate label
                     .concat(&labels)  // keep original labels
                     .group( |_,v,out| out.push( (*v[0].0, 1) ) ) // group by node, "min"
            });

            // count the number of connected components by counting the number of distinct labels
            let probe = labels.map(|(_node, label)| label)
                  .distinct()
                  .map(|_| ()) // put all labels into a single group for counting
                  .count()
                  .inspect(|&(((), num_partitions), _, diff)| {
                    if diff > 0 {
                        if num_partitions == 1 {
                            println!("All nodes in the graph are now connected.");
                        } else {
                            println!("There are now {} disconnected partitions in the graph!", num_partitions);
                        }
                    }
                  })
                  .probe();

            (input, probe)
        });

        let mut capabilities = CapabilitySet::new();
        capabilities.insert(cap);

        // subscribe to the update stream of the topology
        let mut subscription = coord
            .subscribe::<Product<RootTimestamp, Version>, (Entity, i32)>("topology", true)
            .expect("failed to subscribe to topology generator")
            .into_iter();

        // then fetch an initial of the topology to initialize the dataflow
        let initial_snapshot = coord
            .bind_service("topology_service", false)
            .expect("failed to connect to topology service")
            .request(&FetchTopology::new())
            .wait_unwrap();

        let (initial_version, snapshot) = initial_snapshot.unwrap();
        let initial_time = RootTimestamp::new(initial_version);


        // feed the initial topology
        feed_input(&mut input, capabilities.delayed(&initial_time), snapshot);
        capabilities.downgrade(subscription.frontier());
        root.step_while(|| {
            probe.less_than(&subscription.frontier()[0])
        });

        // wait for updates for versions newer than our initial snapshot
        while let Some(event) = subscription.next().map(Result::unwrap) {
            match event {
                SubscriptionEvent::Data(time, data) => {
                    if time > initial_time {
                        let cap = capabilities.delayed(&time);
                        feed_input(&mut input, cap, data);
                    }
                }
                SubscriptionEvent::FrontierUpdate => {
                    capabilities.downgrade(subscription.frontier());
                }
            }

            if let Some(timestamp) = subscription.frontier().get(0) {
                root.step_while(|| probe.less_than(timestamp));
            }
        }
    }).unwrap();
}
