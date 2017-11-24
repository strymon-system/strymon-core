// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io::{Error, ErrorKind};
use std::env;
use std::path::PathBuf;

use futures;
use futures::future::Future;
use futures::stream::Stream;
use tokio_core::reactor::{Core, Handle};

use strymon_communication::Network;
use strymon_communication::rpc::RequestBuf;

use strymon_model::*;

use strymon_rpc::coordinator::*;
use strymon_rpc::executor::*;

pub mod executable;

pub struct ExecutorService {
    id: ExecutorId,
    coord: String,
    host: String,
    network: Network,
    handle: Handle,
}

impl ExecutorService {
    pub fn new(id: ExecutorId, coord: String, network: Network, handle: Handle) -> Self {
        ExecutorService {
            id: id,
            coord: coord,
            host: network.hostname(),
            network: network,
            handle: handle,
        }
    }

    fn fetch(&self, url: &str) -> Result<PathBuf, SpawnError> {
        debug!("fetching: {:?}", url);
        if url.starts_with("tcp://") {
            self.network.download(url).map_err(|_| SpawnError::FetchFailed)
        } else {
            let path = if url.starts_with("file://") {
                PathBuf::from(&url[7..])
            } else {
                PathBuf::from(url)
            };

            if path.exists() {
                Ok(path.to_owned())
            } else {
                Err(SpawnError::FetchFailed)
            }
        }
    }

    fn spawn(&mut self, query: Query, hostlist: Vec<String>) -> Result<(), SpawnError> {
        let process = query.executors
            .iter()
            .position(|&id| self.id == id)
            .ok_or(SpawnError::InvalidRequest)?;
        let threads = query.workers / hostlist.len();
        let executable = self.fetch(&query.program.source)?;
        let args = &*query.program.args;
        let id = query.id;

        let mut exec = executable::Builder::new(&executable, args);

        exec.threads(threads)
            .process(process)
            .hostlist(&hostlist)
            .hostname(&self.host)
            .coord(&self.coord);

        exec.spawn(id, &self.handle)
    }

    pub fn dispatch(&mut self, req: RequestBuf) -> Result<(), Error> {
        match req.name() {
            "SpawnQuery" => {
                let (SpawnQuery { query, hostlist }, resp) = req.decode::<SpawnQuery>()?;
                debug!("got spawn request for {:?}", query);
                resp.respond(self.spawn(query, hostlist));
                Ok(())
            }
            _ => {
                let err = Error::new(ErrorKind::InvalidData, "invalid request");
                return Err(err);
            }
        }
    }
}

pub struct Builder {
    coord: String,
    ports: (u16, u16),
}

impl Builder {
    pub fn host(&mut self, host: String) {
        env::set_var("TIMELY_SYSTEM_HOSTNAME", host);
    }

    pub fn coordinator(&mut self, coord: String) {
        self.coord = coord;
    }

    pub fn ports(&mut self, min: u16, max: u16) {
        self.ports = (min, max);
    }
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            coord: String::from("localhost:9189"),
            ports: (2101, 4101),
        }
    }
}

#[cfg(unix)]
fn setup_termination_handler(handle: &Handle) -> Box<Future<Item=(), Error=Error>> {
    use tokio_signal::unix::{Signal, SIGTERM};

    Box::new(Signal::new(SIGTERM, &handle).and_then(|signal| {
        // terminate stream after first signal
        signal.take(1).for_each(|signum| {
            Ok(info!("received termination signal: {}", signum))
        })
    }))
}

#[cfg(not(unix))]
fn setup_termination_handler(handle: &Handle) -> Box<Future<Item=(), Error=Error>> {
    Box::new(future::empty())
}

impl Builder {
    pub fn start(self) -> Result<(), Error> {
        let Builder { ports, coord } = self;
        let network = Network::init()?;
        let host = network.hostname();
        let (tx, rx) = network.client(&*coord)?;

        let mut core = Core::new()?;
        let handle = core.handle();

        // define a signal handler for clean shutdown
        let sigterm = setup_termination_handler(&handle);

        // define main executor loop
        let service = futures::lazy(move || {
            // announce ourselves at the coordinator
            let id = tx.request(&AddExecutor {
                    host: host,
                    ports: ports,
                    format: ExecutionFormat::NativeExecutable,
                })
                .map_err(|e| e.unwrap_err());

            // once we get results, start the actual executor service
            id.and_then(move |id| {
                let mut executor = ExecutorService::new(id, coord, network, handle);
                rx.for_each(move |req| executor.dispatch(req))
            })
        });

        // terminate on whatever comes first: sigterm or service exits
        core.run(service.select2(sigterm).then(|result| match result {
            Ok(t) => Ok(t.split().0),
            Err(e) => Err(e.split().0),
        }))
    }
}
