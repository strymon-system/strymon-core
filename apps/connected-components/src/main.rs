// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate topology_generator;
extern crate strymon_runtime;
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::{Count, Distinct, Group, Iterate, JoinUnsigned};
use differential_dataflow::input::Input;
use topology_generator::Entity;

// This is the naive implementation of connected components based on the example
// in Frank McSherry's Differential Dataflow repository. For more details on
// how this could be optmized further, see:
// https://github.com/frankmcsherry/blog/blob/master/posts/2015-12-24.md
fn main() {
    strymon_runtime::query::execute(|root, coord| {
        let (mut edges, mut nodes, probe) = root.dataflow(|scope| {
            let (edge_input, edges) = scope.new_collection();
            let (node_input, nodes) = scope.new_collection();

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

                inner.join_map_u(&edges, |_src,label,dst| (*dst,*label)) // propagate label
                     .concat(&labels)  // keep original labels
                     .group_u( |_,v,out| out.push( (*v[0].0, 1) ) ) // group by node, "min"
            });

            // count the number of connected components by counting the number of
            // distinct labels
            let probe = labels.map(|(_node, label)| label)
                  .distinct_u()
                  .map(|_| ()) // put all labels into a single group for counting
                  .count()
                  .inspect(|&(((), num_partitions), _, diff)| {
                    if diff > 0 {
                        if num_partitions == 1 {
                            println!("All nodes in the graph are now connected.");
                        } else {
                            println!("The are now {} disconnected partitions in the graph!", num_partitions);
                        }
                    }
                  })
                  .probe();


            (edge_input, node_input, probe)
        });

        let topic = coord.subscribe_collection::<(Entity, i32)>("topology")
            .expect("failed to subscribe to topology generator");
        for (round, data) in topic.into_iter().enumerate() {
            for (entity, delta) in data {
                let diff = delta as isize;
                match entity {
                    Entity::Connection(c) => edges.update(c, diff),
                    Entity::Switch(s)     => nodes.update(s, diff),
                };
            }
            edges.advance_to(round + 1);
            nodes.advance_to(round + 1);
            edges.flush();
            nodes.flush();
            root.step_while(|| probe.less_than(edges.time()));
        }
    }).unwrap();
}
