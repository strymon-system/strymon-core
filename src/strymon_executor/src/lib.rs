// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate log;
extern crate libc;
extern crate time;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_process;
extern crate tokio_signal;

extern crate strymon_model;
extern crate strymon_rpc;
extern crate strymon_communication;

use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::ffi::OsStr;

use time::Timespec;

use futures::future::Future;
use futures::stream::Stream;
use tokio_core::reactor::{Core, Handle};

use strymon_communication::Network;
use strymon_communication::rpc::{Request, RequestBuf};

use strymon_model::*;

use strymon_rpc::coordinator::*;
use strymon_rpc::executor::*;

use self::executable::ProcessService;

const START_TIME_FMT: &'static str = "%Y%m%d%H%M%S";

pub mod executable;

pub struct ExecutorService {
    id: ExecutorId,
    network: Network,
    process: ProcessService,
    workdir: PathBuf,
}

impl ExecutorService {
    pub fn new(id: ExecutorId, coord: String, workdir: PathBuf, network: Network, handle: Handle) -> Self {
        let process = ProcessService::new(&handle, coord, network.hostname());

        ExecutorService {
            id: id,
            network: network,
            process: process,
            workdir: workdir,
        }
    }

    fn fetch(&self, url: &str, dest: PathBuf) -> Result<PathBuf, SpawnError> {
        debug!("fetching: {:?}", url);
        if url.starts_with("tcp://") {
            self.network.download(url, &dest).map_err(|_| SpawnError::FetchFailed)?;
            Ok(dest)
        } else {
            let path = if url.starts_with("file://") {
                PathBuf::from(&url[7..])
            } else {
                PathBuf::from(url)
            };

            if path.exists() {
                Ok(path.to_owned())
            } else {
                Err(SpawnError::FileNotFound)
            }
        }
    }

    fn spawn(&mut self, query: Job, hostlist: Vec<String>) -> Result<(), SpawnError> {
        let process = query.executors
            .iter()
            .position(|&id| self.id == id)
            .ok_or(SpawnError::InvalidRequest)?;

        // workers refers to the total number of workers. if there is more than
        // one executor, we have to divide by the number of executors
        let threads = if hostlist.is_empty() {
            query.workers
        } else {
            query.workers / hostlist.len()
        };

        // the job dir should be unique to this execution, we prepend a datetime
        // string to avoid overwriting the artifacts of previous strymon instances
        let tm = time::at_utc(Timespec::new(query.start_time as i64, 0));
        let start_time = tm.strftime(START_TIME_FMT).expect("invalid fmt");
        let jobdir = self.workdir.as_path()
            .join(format!("{}_{:04}", start_time, query.id.0))
            .join(process.to_string());

        // create the working directory for this process
        fs::create_dir_all(&jobdir).map_err(|_| SpawnError::WorkdirCreationFailed)?;

        let binary = jobdir.as_path().join(query.program.binary_name);
        let executable = self.fetch(&query.program.source, binary)?;
        let args = &*query.program.args;
        let id = query.id;

        let mut exec = executable::Builder::new(&executable, args)?;
        exec.threads(threads)
            .process(process)
            .hostlist(hostlist)
            .working_directory(&jobdir);

        self.process.spawn(id, exec)
    }

    pub fn dispatch(&mut self, req: RequestBuf<ExecutorRPC>) -> Result<(), Error> {
        match *req.name() {
            SpawnJob::NAME => {
                let (SpawnJob { query, hostlist }, resp) = req.decode::<SpawnJob>()?;
                debug!("spawn request for {:?}", query);
                resp.respond(self.spawn(query, hostlist));
                Ok(())
            }
            TerminateJob::NAME => {
                let (TerminateJob { query }, resp) = req.decode::<TerminateJob>()?;
                debug!("termination request for {:?}", query);
                resp.respond(self.process.terminate(query));
                Ok(())
            }
        }
    }
}

pub struct Builder {
    coord: String,
    ports: (u16, u16),
    workdir: PathBuf,
    hostname: Option<String>,
}

impl Builder {
    pub fn hostname(&mut self, host: String) -> &mut Self {
        self.hostname = Some(host);
        self
    }

    pub fn coordinator(&mut self, coord: String) -> &mut Self {
        self.coord = coord;
        self
    }

    pub fn ports(&mut self, min: u16, max: u16) -> &mut Self {
        self.ports = (min, max);
        self
    }

    pub fn workdir<P: AsRef<OsStr>>(&mut self, workdir: &P) -> &mut Self {
        self.workdir = PathBuf::from(workdir);
        self
    }
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            coord: String::from("localhost:9189"),
            ports: (2101, 4101),
            workdir: PathBuf::from("jobs"),
            hostname: None,
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
        let Builder { ports, coord, workdir, hostname } = self;
        let network = Network::new(hostname)?;
        let host = network.hostname();
        let (tx, rx) = network.client::<ExecutorRPC, _>(&*coord)?;

        let mut core = Core::new()?;
        let handle = core.handle();

        // define a signal handler for clean shutdown
        let sigterm = setup_termination_handler(&handle);

        // ensure the working directory exists
        fs::create_dir_all(&workdir)?;

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
                let mut executor = ExecutorService::new(id, coord, workdir, network, handle);

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
