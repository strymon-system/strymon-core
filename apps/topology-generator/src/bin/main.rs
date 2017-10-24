extern crate topology_generator;

extern crate strymon_runtime;
extern crate timely;

use std::time::Duration;
use std::thread;

use timely::dataflow::operators::{Input, Probe};
use strymon_runtime::query::publish::Partition;

use topology_generator::{fat_tree_topology, NodeId};
use topology_generator::Entity::{Connection, Switch};

fn main() {
    strymon_runtime::query::execute(|root, coord| {
        let (mut input, probe) = root.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input();

            let probe = coord.publish_collection("topology", &stream, Partition::Merge)
                .expect("failed to publish topology updates").probe();

            (input, probe)
        });

        if root.index() == 0 {
            let k = 16;
            let verbose = true;
            let max_weight = 100;
            let topo = fat_tree_topology(k, max_weight, verbose);

            // feed the initial topology into the dataflow
            for &c in topo.connections.iter() {
                input.send((Connection(c), 1i32));
            }
            
            for n in 0..(topo.switches as NodeId) {
                input.send((Switch(n), 1i32));
            }

            let duration = Duration::from_millis(100);
            for round in 1.. {
                input.advance_to(round);
                root.step_while(|| probe.less_than(input.time()));
                thread::sleep(duration);
            }
        }
    }).unwrap();
}
