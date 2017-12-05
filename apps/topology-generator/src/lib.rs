// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate rand;
extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate typename;

use rand::{Rng, SeedableRng, StdRng};

/// A unique, dense identifier for a topology node (i.e. a switch)
pub type NodeId = u32;

/// Weight of an edge or complete path, can also be called 'distance'
pub type LinkWeight = u64;

/// A unidirectional connection between two switches
pub type Connection = (NodeId, NodeId, LinkWeight);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Abomonation, TypeName)]
pub enum Entity {
    Connection(Connection),
    Switch(NodeId),
}

/// A minimal switch-to-switch network topology model. The version used in this
/// open-source release of Strymon only contains the bare-minimun needed
/// for running a simple incremental routing on top of it.
#[derive(Debug)]
pub struct Topology {
    pub switches: usize,
    pub connections: Vec<Connection>,
}

impl Topology {
    /// Creates a new empty topology
    fn with_capacity(n_connections: usize) -> Self {
        Topology {
            switches: 0,
            connections: Vec::with_capacity(n_connections),
        }
    }

    /// Insert a new switch into the topology, returns it's unique identifier
    fn add_switch(&mut self) -> NodeId {
        let id = self.switches as NodeId;
        self.switches += 1;
        id
    }
}

/// Generate a random (fixed seed) k-ary Fat-tree topology
///
/// The number of hosts, switches and ports per switch are defined by k:
///          hosts: k^3 / 4
///       switches: k^2 + k
/// ports / switch: k
pub fn fat_tree_topology(k: u32, max_weight: u64, verbose: bool) -> Topology {
    assert!(max_weight > 0);

    let n_hosts = k * k * k / 4;
    let n_switches = 5 * k * k / 4;
    let n_ports_per_switch = k;
    let n_connections = (n_switches * n_ports_per_switch - n_hosts) / 2;

    if verbose {
        println!("Hosts: {}, Switches: {}, Ports: {}, Links: {}", n_hosts, n_switches, n_ports_per_switch, n_connections);
    }

    let seed: &[_] = &[3, 4, 5, 6];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut topo = Topology::with_capacity(n_connections as usize);

    // create k/2 core groups
    let mut core_groups: Vec<Vec<NodeId>> = Vec::with_capacity((k/2) as usize);
    for _i_core_group in 0..k/2 {
        let mut core_group: Vec<NodeId> = Vec::with_capacity((k/2) as usize);
        // create k/2 switches for the core-group
        for _i_switch in 0..k/2 {
            let switch_id = topo.add_switch();
            core_group.push(switch_id);
        }
        core_groups.push(core_group);
    }

    // create the pods
    for _i_pod in 0..k {
        // create the edge-switches of the pod
        let mut edge: Vec<NodeId> = Vec::with_capacity((k/2) as usize);
        for _i_switch in 0..k/2 {
            let switch_id = topo.add_switch();
            edge.push(switch_id);
        }

        // create the aggregation-switches of the pod
        for i_core_group in 0..k/2 {
            let switch_id = topo.add_switch();

            // connect to all edge-switches within pod
            for &edge_switch_id in &edge {
                let w = if max_weight == 1 { 1 } else { rng.gen_range(1, max_weight) };
                topo.connections.push((switch_id, edge_switch_id, w));
            }

            // connect to all core switches of its core-group
            for &core_switch_id in &core_groups[i_core_group as usize] {
                let w = if max_weight == 1 { 1 } else { rng.gen_range(1, max_weight) };
                topo.connections.push((switch_id, core_switch_id, w));
            }
        }
    }

    assert_eq!(n_connections, topo.connections.len() as u32);
    assert_eq!(n_switches, topo.switches as u32);
    topo
}
