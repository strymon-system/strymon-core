// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![deny(missing_docs)]

//! The Strymon client-library. In order to register a Timely Dataflow job with
//! Strymon, users are expected to link against this library and eventually
//! invoke `strymon_job::execute`.
//!
//! In addition, this library also provides the operaters needed to use the
//! publish-subscribe mechanism.
//!
//! # Examples
//!
//! ```rust,no_run
//! extern crate strymon_job;
//! extern crate timely;
//!
//! use timely::dataflow::operators::ToStream;
//! use strymon_job::operators::publish::Partition;
//!
//! fn main() {
//!     strymon_job::execute(|worker, coord| {
//!         worker.dataflow::<u64, _, _>(|scope| {
//!             let stream = (0..1000).to_stream(scope);
//!             coord.publish("numbers", &stream, Partition::Merge)
//!                 .expect("failed to publish topic");
//!         });
//!     }).unwrap();
//! }
//! ```

extern crate timely;
extern crate timely_communication;
#[macro_use]
extern crate futures;
extern crate serde;
extern crate slab;
extern crate tokio_core;
#[macro_use]
extern crate log;
extern crate typename;
#[macro_use]
extern crate serde_derive;

extern crate strymon_communication;

extern crate strymon_model;
extern crate strymon_rpc;

mod protocol;
mod publisher;
mod subscriber;
pub mod operators;

use std::io;
use std::sync::Mutex;

use futures::Future;

use timely::dataflow::scopes::Root;

use timely_communication::Allocator;
use timely_communication::initialize::{Configuration, WorkerGuards};

use strymon_communication::Network;
use strymon_communication::rpc::Outgoing;

use strymon_rpc::coordinator::{QueryToken, AddWorkerGroup, CoordinatorRPC};
use strymon_model::QueryId;
use strymon_model::config::job::Process;

/// Handle to communicate with the Strymon coordinator.
///
/// The methods of this object can be used to publish or subscribe to
/// topics available in the catalog. In order to obtain a `Coordinator` instance,
/// users must register the current process with `strymon_job::execute`.
#[derive(Clone)]
pub struct Coordinator {
    token: QueryToken,
    network: Network,
    tx: Outgoing,
}

impl Coordinator {
    /// Registers the local job at the coordinator at address `coord`.
    fn initialize(id: QueryId, process: usize, coord: String, hostname: String) -> io::Result<Self> {
        let network = Network::with_hostname(hostname)?;
        let (tx, _) = network.client::<CoordinatorRPC, _>(&*coord)?;

        let announce = tx.request(&AddWorkerGroup {
            query: id,
            group: process,
        });

        let token = announce.wait()
            .map_err(|err| {
                err.and_then::<(), _>(|err| {
                    let err = format!("failed to register: {:?}", err);
                    Err(io::Error::new(io::ErrorKind::Other, err))
                })
            })
            .map_err(Result::unwrap_err)?;

        Ok(Coordinator {
            tx: tx,
            network: network,
            token: token,
        })
    }
}

/// Executes a Timely dataflow within this Strymon job.
///
/// This function requires that the calling process has been spawned by an executor.
/// Upon successful registration with the Strymon coordinator, the closure `func`
/// is invoked for each requested worker hosted by the current process.
///
/// This function intentionally mirrors `timely::execute`, with the difference that
/// the worker configuration is provided by the parent executor and that the running
/// worker gains the ability to talk to the coordinator.
pub fn execute<T, F>(func: F) -> Result<WorkerGuards<T>, String>
    where T: Send + 'static,
          F: Fn(&mut Root<Allocator>, Coordinator) -> T,
          F: Send + Sync + 'static
{
    let config = Process::from_env().map_err(|err| {
            format!(concat!("Failed to parse data from executor. ",
                            "Has this binary been launched by an executor? ",
                            "Error: {:?}"),
                    err)
        })?;

    // create timely configuration
    let timely_conf = if config.addrs.len() > 1 {
        info!("Configuration:Cluster({}, {}/{})",
              config.threads,
              config.index,
              config.addrs.len()-1);
        Configuration::Cluster(config.threads, config.index, config.addrs, true)
    } else if config.threads > 1 {
        info!("Configuration:Process({})", config.threads);
        Configuration::Process(config.threads)
    } else {
        info!("Configuration:Thread");
        Configuration::Thread
    };

    let coord = Coordinator::initialize(config.job_id, config.index, config.coord, config.hostname)
        .map_err(|err| format!("failed to connect to coordinator: {:?}", err))?;

    // wrap in mutex because the `Outgoing` is not `Sync` (due to std::mpsc)
    let coord = Mutex::new(coord);
    timely::execute(timely_conf, move |root| {
        let coord = coord.lock().unwrap().clone();
        func(root, coord)
    })
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::Mutex;

    use futures::future::Future;
    use futures::stream::Stream;

    use timely;
    use timely::progress::timestamp::RootTimestamp;
    use timely::progress::nested::product::Product;
    use timely::dataflow::operators::{Delay, Map, ToStream};
    use timely::dataflow::operators::capture::{Capture, Extract};

    use strymon_communication::Network;

    use subscriber::SubscriberGroup;
    use publisher::{Publisher, Addr};

    type ExampleTime = Product<RootTimestamp, u64>;

    fn publisher_thread(network: &Network) -> Addr {
        let (addr, publisher) =
            Publisher::<ExampleTime, String>::new(&network).unwrap();

        thread::spawn(move || {
            publisher.subscriber_barrier().expect("publisher died unexpectedly");

            let slot = Mutex::new(Some(publisher));
            timely::example(move |scope| {
                let publisher = slot.lock().unwrap().take().unwrap();
                (0..100)
                    .to_stream(scope)
                    .delay(|d, _| RootTimestamp::new((*d / 10) * 10))
                    .map(|d| d.to_string())
                    .capture_into(publisher);
            });
        });

        addr
    }

    #[test]
    fn raw_publisher() {
        let network = Network::with_hostname("localhost".to_string()).unwrap();
        let addr = publisher_thread(&network);

        use timely::dataflow::operators::generic::operator::source;
        let captured = timely::example(move |scope| {
            source(scope, "Source", |cap| {
                let socket = network.connect((&*addr.0, addr.1)).unwrap();
                let connecting = SubscriberGroup::<ExampleTime, String>::new(Some(socket), cap);
                let connected = connecting.wait().unwrap();
                let mut stream = connected.wait();
                move |output| {
                    // subscriber will drop remaining capabilities once the publisher is drained
                    if let Some(msg) = stream.next() {
                        let (cap, data) = msg.unwrap();
                        output.session(&cap)
                              .give_iterator(data.into_iter());
                    }
                }
            }).capture()
        });

        let expected: Vec<(ExampleTime, Vec<String>)> = (0..10)
            .map(|t| t * 10)
            .map(|t| (RootTimestamp::new(t),
                (0..10).map(|d| format!("{}", t + d)).collect()))
            .collect();
        assert_eq!(captured.extract(), expected);
    }
}
