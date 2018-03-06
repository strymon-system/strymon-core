// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate rand;
extern crate futures;
extern crate futures_timer;

extern crate strymon_job;
extern crate strymon_communication;
extern crate timely;

extern crate topology_generator;

use std::io;
use std::time::Duration;

use futures::future::Future;
use futures::stream::{Stream, FuturesUnordered};
use futures_timer::Delay;

use rand::{ThreadRng, Rng};

use timely::dataflow::operators::{Input, Probe};

use strymon_job::operators::publish::Partition;
use strymon_job::operators::service::Service;
use strymon_communication::rpc::RequestBuf;

use topology_generator::{fat_tree_topology, Topology, Connection, Entity, NodeId};
use topology_generator::service::{TopologyService, Fault, FetchTopology, TopologyUpdate, Version};

struct TopologyManager {
    topo: Topology,
    version: Version,
    rng: ThreadRng,
}

impl TopologyManager {
    fn new(topo: Topology) -> Self {
        TopologyManager {
            topo: topo,
            rng: rand::thread_rng(),
            version: 0,
        }
    }

    fn inject_fault(&mut self, fault: Fault) -> (Version, Vec<Connection>) {
        let removals = match fault {
            Fault::DisconnectRandomLink => {
                // fails a randomly selected link
                let link_id = self.rng.gen_range(0, self.topo.connections.len());
                let edge = self.topo.connections.remove(link_id);
                println!("Disconnecting randomly chosen link #{} -> #{}", edge.0, edge.1);
                vec![edge]
            },
            Fault::DisconnectRandomSwitch => {
                let switch_id = self.rng.gen_range(0, self.topo.switches) as NodeId;
                println!("Disconnecting randomly chosen switch #{}", switch_id);

                // TODO(swicki): Use `drain_filter` for this once it's stabilized
                let removals = self.topo.connections.iter()
                    .filter(|e| e.0 == switch_id || e.1 == switch_id)
                    .cloned()
                    .collect::<Vec<_>>();
                self.topo.connections.retain(|e| e.0 != switch_id && e.1 != switch_id);

                removals
            }
        };
        self.version += 1;
        (self.version, removals)
    }

    fn restore_links(&mut self, batch: Vec<Connection>) -> (Version, TopologyUpdate) {
        let mut update = Vec::new();
        for link in batch {
            self.topo.connections.push(link);
            update.push((Entity::Connection(link), 1));
        }
        self.version += 1;
        (self.version, update)
    }

    fn dump(&self) -> (Version, TopologyUpdate) {
        (self.version, self.topo.dump())
    }
}

const FAULT_DURATION_SECS: u64 = 10;

struct TopologyServer {
    pending: FuturesUnordered<Box<Future<Item=Vec<Connection>, Error=io::Error>>>,
    server: Service<TopologyService>,
    topo: TopologyManager,
}

enum Event {
    RestoreLinks(Vec<Connection>),
    Request(RequestBuf<TopologyService>),
}

impl TopologyServer {
    fn new(topo: Topology, server: Service<TopologyService>) -> Self {
        TopologyServer {
            pending: FuturesUnordered::new(),
            server: server,
            topo: TopologyManager::new(topo),
        }
    }

    fn handle_request(&mut self, req: RequestBuf<TopologyService>)
        -> io::Result<Option<(Version, TopologyUpdate)>>
    {
        match req.name() {
            &TopologyService::FetchTopology => {
                let (_, resp) = req.decode::<FetchTopology>()?;
                resp.respond(Ok(self.topo.dump()));
                Ok(None)
            },
            &TopologyService::InjectRandomFault => {
                let (fault, resp) = req.decode::<Fault>()?;
                let (version, removed) = self.topo.inject_fault(fault);

                let update: Vec<_> = removed
                    .iter()
                    .map(|&e| (Entity::Connection(e), -1))
                    .collect();

                let restore = Delay::new(Duration::from_secs(FAULT_DURATION_SECS))
                    .and_then(move |()| Ok(removed));

                resp.respond(Ok(()));
                self.pending.push(Box::new(restore));
                Ok(Some((version, update)))
            }
        }
    }

    fn get_next_event(&mut self) -> io::Result<Event> {
        let requests = self.server.by_ref().map(Event::Request);
        let updates = self.pending.by_ref().map(Event::RestoreLinks);
        requests.select(updates).wait().next().unwrap_or_else(|| {
            Err(io::Error::new(io::ErrorKind::Other, "service was shut down"))
        })
    }

    fn snapshot(&self) -> (Version, TopologyUpdate) {
        self.topo.dump()
    }

    fn wait_for_changes(&mut self) -> io::Result<(Version, TopologyUpdate)> {
        loop {
            match self.get_next_event()? {
                Event::Request(req) => {
                    if let Some(update) = self.handle_request(req)? {
                        return Ok(update);
                    }
                },
                Event::RestoreLinks(links) => {
                    return Ok(self.topo.restore_links(links));
                }
            }
        }
    }
}

fn main() {
    strymon_job::execute(|root, coord| {
        let (mut input, probe) = root.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input();
            let probe = coord.publish("topology", &stream, Partition::Merge)
                .expect("failed to publish topology updates")
                .probe();
            (input, probe)
        });

        if root.index() == 0 {
            let k = 16; // number of ports
            let verbose = true; // print statistics about generated topology
            let max_weight = 100; // max link weights
            let topo = fat_tree_topology(k, max_weight, verbose);

            let server = coord.announce_service("topology_service").unwrap();
            let mut server = TopologyServer::new(topo, server);

            // fetch the initial topology for the dataflow
            let mut next_batch = server.snapshot();
            loop {
                let (version, mut changes) = next_batch;
                input.send_batch(&mut changes);
                input.advance_to(version + 1);
                root.step_while(|| probe.less_than(input.time()));
                next_batch = server.wait_for_changes().unwrap();
            }
        }
    }).unwrap();
}
