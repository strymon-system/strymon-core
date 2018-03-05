// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The default implementation of the Strymon job executor.
//!
//! This library is intended for internal use and most likely not what end-users are looking for.
//! If you would like to spawn a new Strymon executor, please take a look at the
//! `strymon manage start-executor` subcommand of the
//! [Strymon command line utility](https://strymon-system.github.io/docs/command-line-interface).
//!
//! If however you would like to implement a custom executor implementation, or extend this current
//! implementation, please continue reading.
//!
//! ## Implementation
//!
//! The main logic of the Strymon executor is implemented in the
//! [**`ExecutorService`**](struct.ExecutorService.html) struct. It is commonly instantiated using
//! the [`Builder`](struct.Builder.html) type, which initializes the event loop and sends an initial
//! [executor registration request](../strymon_rpc/coordinator/struct.AddExecutor.html) to obtain
//! a unique executor id. Once the service is registered, incoming requests from the coordinator are
//! handled by the [`ExecutorService::dispatch`](struct.ExecutorService.html#method.dispatch)
//! method.
//!
//! When an executor receives a [`SpawnJob`](../strymon_rpc/executor/struct.SpawnJob.html) request
//! from the coordinator, it will fetch the corresponding job binary from the submitter and
//! create a new working directory of the form
//! `<workdir>/<submission-timestamp>_<job-id>/<process-index>/` and use it to store the job binary
//! and potential job artifacts.
//!
//! The current implementation of the `ExecutorService` only supports spawning Strymon jobs as
//! operating system processes. There is no network communication between spawned processes and
//! the parent executor. [Configuration data](../strymon_model/config/job/index.html) is passed
//! down to the spawned process using environment variables. Currently, the parent executor
//! captures the standard output and standard error of the spawned child process and forwards it
//! to the [`log`](https://docs.rs/log) crate. The process supervision logic is implemented in the
//! [`ProcessService`](executable/struct.ProcessService.html) type.

#![deny(missing_docs)]

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

/// `strftime` format string used to create unique job working directories.
const START_TIME_FMT: &'static str = "%Y%m%d%H%M%S";

pub mod executable;

/// A running Strymon executor instance.
pub struct ExecutorService {
    /// The id of this executor
    id: ExecutorId,
    /// A network handle used to fetch job binaries from job submitters.
    network: Network,
    /// The process service used to supervise spawned processes.
    process: ProcessService,
    /// The base path for job working directories.
    workdir: PathBuf,
}

impl ExecutorService {
    /// Creates a new instance for an newly registered executor.
    ///
    /// This assumes that a [`AddExecutor`](../strymon_rpc/coordinator/struct.AddExecutor.html)
    /// request has been sent to the coordinator. This constructor takes the following arguments:
    ///
    ///   - **`id`**: The identifier assigned to us in the `AddExecutor` response from the
    /// coordinator.
    ///   - **`coord`**: The URI of the coordinator which will be passed down to spawned jobs.
    ///   - **`workdir`**: The base path used to create working directories for spawned jobs.
    ///   - **`handle`**: A `tokio-core` reactor handle, used to create the `ProcessService`.
    ///
    pub fn new(id: ExecutorId, coord: String, workdir: PathBuf, network: Network, handle: Handle) -> Self {
        let process = ProcessService::new(&handle, coord, network.hostname());

        ExecutorService {
            id: id,
            network: network,
            process: process,
            workdir: workdir,
        }
    }

    /// Downloads a binary from the given `url` as file `dest`.
    ///
    /// If `url` refers to an *existing* local file (i.e. an url starting with `file:///`), the
    /// path to this local file is returned instead of `dest`.
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

    /// Spawns a new job process given its complete job description.
    ///
    /// The `hostlist` argument is used by the spawned job to initialize `timely_communication`.
    fn spawn(&mut self, job: Job, hostlist: Vec<String>) -> Result<(), SpawnError> {
        let process = job.executors
            .iter()
            .position(|&id| self.id == id)
            .ok_or(SpawnError::InvalidRequest)?;

        // workers refers to the total number of workers. if there is more than
        // one executor, we have to divide by the number of executors
        let threads = if hostlist.is_empty() {
            job.workers
        } else {
            job.workers / hostlist.len()
        };

        // the job dir should be unique to this execution, we prepend a datetime
        // string to avoid overwriting the artifacts of previous strymon instances
        let tm = time::at_utc(Timespec::new(job.start_time as i64, 0));
        let start_time = tm.strftime(START_TIME_FMT).expect("invalid fmt");
        let jobdir = self.workdir.as_path()
            .join(format!("{}_{:04}", start_time, job.id.0))
            .join(process.to_string());

        // create the working directory for this process
        fs::create_dir_all(&jobdir).map_err(|_| SpawnError::WorkdirCreationFailed)?;

        let binary = jobdir.as_path().join(job.program.binary_name);
        let executable = self.fetch(&job.program.source, binary)?;
        let args = &*job.program.args;
        let id = job.id;

        let mut exec = executable::Builder::new(&executable, args)?;
        exec.threads(threads)
            .process(process)
            .hostlist(hostlist)
            .working_directory(&jobdir);

        self.process.spawn(id, exec)
    }

    /// Decode and handle a request sent by the coordinator.
    pub fn dispatch(&mut self, req: RequestBuf<ExecutorRPC>) -> Result<(), Error> {
        match *req.name() {
            SpawnJob::NAME => {
                let (SpawnJob { job, hostlist }, resp) = req.decode::<SpawnJob>()?;
                debug!("spawn request for {:?}", job);
                resp.respond(self.spawn(job, hostlist));
                Ok(())
            }
            TerminateJob::NAME => {
                let (TerminateJob { job }, resp) = req.decode::<TerminateJob>()?;
                debug!("termination request for {:?}", job);
                resp.respond(self.process.terminate(job));
                Ok(())
            }
        }
    }
}

/// A builder type to initialize and run a new executor instance.
///
/// # Examples
/// ```rust,no_run
/// use strymon_executor::Builder;
///
/// let mut executor = Builder::default();
/// executor
///     .hostname("worker7".to_string())
///     .coordinator("master:9189".to_string());
/// executor.start().expect("failed to start executor");
/// ```
pub struct Builder {
    /// The address of the coordinator
    coord: String,
    /// A port range for use in `timely_communication` of spawned jobs.
    ports: (u16, u16),
    /// The base path for the job working directories.
    workdir: PathBuf,
    /// The externally reachable hostname of this machine.
    hostname: Option<String>,
}

impl Builder {
    /// Sets the externally reachable hostname of this machine
    /// (default: [*inferred*](../strymon_communication/struct.Network.html#method.new)).
    pub fn hostname(&mut self, host: String) -> &mut Self {
        self.hostname = Some(host);
        self
    }

    /// Sets the address of the coordinator (default: `"localhost:9189"`)
    pub fn coordinator(&mut self, coord: String) -> &mut Self {
        self.coord = coord;
        self
    }

    /// Sets a range of ports to be allocated to `timely_communication` workers
    /// (default: `2101-4101`)
    pub fn ports(&mut self, min: u16, max: u16) -> &mut Self {
        self.ports = (min, max);
        self
    }

    /// Sets the base path for job working directories (default: `"jobs"`).
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
    /// Starts a new executor instance and blocks until termination.
    ///
    /// Initiates the connection to the coordinator, sends an executor registration request and
    /// upon successful initialization, drives the `ExecutorService` with incoming requests from
    /// the coordinator.
    ///
    /// The service is terminated either until `SIGTERM` is received or the connection
    /// to the coordinator is closed.
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
