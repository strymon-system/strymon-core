extern crate topology_generator;

extern crate rand;

extern crate strymon_runtime;
extern crate timely;

use std::time::Duration;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, BufRead, Write};
use std::thread;

use rand::{ThreadRng, Rng};

use timely::dataflow::operators::{Input, Probe};
use strymon_runtime::query::publish::Partition;

use topology_generator::{fat_tree_topology, Topology, NodeId};
use topology_generator::Entity::{self, Connection, Switch};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Fault {
    DisconnectRandomSwitch,
    DisconnectRandomLink,
}

fn parse_faults(stream: TcpStream, send: Sender<Fault>) {
    let mut line = String::new();
    let mut stream = BufReader::new(stream);
    while let Ok(_) = stream.read_line(&mut line) {
        let fault = match line.trim() {
            "disconnect-random-link" => Ok(Fault::DisconnectRandomLink),
            "disconnect-random-switch" => Ok(Fault::DisconnectRandomSwitch),
            _ => Err("Invalid command")
        };

        // send injected fault to main thread
        let err = fault.and_then(|fault| {
            send.send(fault).map_err(|_| "Terminated")
        });

        // acknowledge command
        let _ = if let Err(msg) = err {
            writeln!(stream.get_mut(), "ERROR: {}", msg)
        } else {
            writeln!(stream.get_mut(), "OK")
        };

        line.clear();
    }
}

fn spawn_fault_injector() -> Receiver<Fault> {
    let (send, recv) = channel();

    let listener = TcpListener::bind("127.0.0.1:9201")
        .expect("failed to start open tcp socket");

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let send = send.clone();
                    thread::spawn(move || parse_faults(stream, send));
                }
                Err(e) => {
                    eprintln!("error: failed to accept incoming client: {}", e);
                    break;
                }
            }
        }
    });

    recv
}

const TICK_DURATION_MILLIS: u64 = 10;
const FAULT_DURATION_TICKS: u32 = 1000;

struct FaultManager {
    active: Vec<(u32, Entity)>,
    rng: ThreadRng,
    topo: Topology,
}

impl FaultManager {
    fn new(topo: Topology) -> Self {
        FaultManager {
            active: Vec::new(),
            topo: topo,
            rng: rand::thread_rng(),
        }
    }

    fn inject_fault(&mut self, fault: Fault) -> Vec<(Entity, i32)> {
        let removals = match fault {
            Fault::DisconnectRandomLink => {
                // fails a randomly selected link
                let link_id = self.rng.gen_range(0, self.topo.connections.len());
                let edge = self.topo.connections.remove(link_id);
                println!("Disconnecting randomly chosen link #{} -> #{}", edge.0, edge.1);
                vec![(Connection(edge), -1)]
            },
            Fault::DisconnectRandomSwitch => {
                let switch_id = self.rng.gen_range(0, self.topo.switches) as NodeId;
                println!("Disconnecting randomly chosen switch #{}", switch_id);

                // TODO(swicki): Use `drain_filter` for this once it's stabilized
                let removals = self.topo.connections.iter()
                    .filter(|e| e.0 == switch_id || e.1 == switch_id)
                    .map(|&e| (Connection(e), -1))
                    .collect::<Vec<_>>();
                self.topo.connections.retain(|e| e.0 != switch_id && e.1 != switch_id);

                removals
            },
        };

        // keep all selected edges to be able to restore them later
        self.active.extend(removals.iter().map(|&(e, _)| (FAULT_DURATION_TICKS, e)));
        removals
    }

    fn tick_and_restore(&mut self) -> Vec<(Entity, i32)> {
        let mut additions = Vec::new();
        for &mut (ref mut timer, ref edge) in &mut self.active {
            *timer -= 1;
            if *timer == 0 {
                additions.push((*edge, 1));
            }
        }

        self.active.retain(|&(t, _)| t > 0);
        additions
    }
}

fn main() {
    strymon_runtime::query::execute(|root, coord| {
        let (mut input, probe) = root.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input();

            let probe = coord.publish_collection("topology", &stream, Partition::Merge)
                .expect("failed to publish topology updates").probe();

            (input, probe)
        });

        if root.index() == 0 {
            let faults = spawn_fault_injector();

            let k = 16; // number of ports
            let verbose = true; // print statistics about generated topology
            let max_weight = 100; // max link weights
            let topo = fat_tree_topology(k, max_weight, verbose);

            // feed the initial topology into the dataflow
            for &c in topo.connections.iter() {
                input.send((Connection(c), 1i32));
            }

            for n in 0..(topo.switches as NodeId) {
                input.send((Switch(n), 1i32));
            }

            input.advance_to(1);

            let tick = Duration::from_millis(TICK_DURATION_MILLIS);
            let mut active_faults = FaultManager::new(topo);
            let mut timestamp = 1;
            loop {
                let mut updates = Vec::new();
                // check if we need to insert any new faults
                if let Ok(fault) = faults.try_recv() {
                    updates.extend_from_slice(&active_faults.inject_fault(fault));
                }
                // check if any faults expired and restore if necessary
                updates.extend_from_slice(&active_faults.tick_and_restore());

                // send submitted updates to dataflow
                if !updates.is_empty() {
                    timestamp += 1;
                    input.send_batch(&mut updates);
                    input.advance_to(timestamp);
                }

                loop {
                    // schedule the dataflow and any pending subscribers
                    root.step();
                    if !probe.less_than(input.time()) {
                        break;
                    }
                }

                thread::sleep(tick);
            }
        }
    }).unwrap();
}
