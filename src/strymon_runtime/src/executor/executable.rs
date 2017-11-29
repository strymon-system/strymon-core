// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::borrow::Cow;
use std::env;
use std::num;
use std::rc::Rc;
use std::cell::RefCell;
use std::process::{Command, Stdio};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::{Write, Display};
use std::io::{BufReader};

use libc;

use futures::{Future, Stream};
use tokio_io;
use tokio_core::reactor::Handle;
use tokio_process::{Child, CommandExt};

use strymon_model::QueryId;
use strymon_rpc::executor::{SpawnError, TerminateError};

pub const QUERY_ID: &'static str = "TIMELY_EXEC_CONF_QUERY_ID";
pub const THREADS: &'static str = "TIMELY_EXEC_CONF_THREADS";
pub const PROCESS: &'static str = "TIMELY_EXEC_CONF_PROCESS";
pub const HOSTLIST: &'static str = "TIMELY_EXEC_CONF_HOSTLIST";
pub const COORD: &'static str = "TIMELY_EXEC_CONF_COORD";
pub const HOST: &'static str = "TIMELY_SYSTEM_HOSTNAME";

#[derive(Debug)]
pub struct NativeExecutable {
    pub query_id: QueryId,
    pub threads: usize,
    pub process: usize,
    pub hostlist: Vec<String>,
    pub coord: String,
    pub host: String,
}

#[derive(Debug)]
pub enum ParseError {
    VarErr(env::VarError),
    IntErr(num::ParseIntError),
}

impl From<env::VarError> for ParseError {
    fn from(var: env::VarError) -> Self {
        ParseError::VarErr(var)
    }
}

impl From<num::ParseIntError> for ParseError {
    fn from(int: num::ParseIntError) -> Self {
        ParseError::IntErr(int)
    }
}

impl NativeExecutable {
    pub fn from_env() -> Result<Self, ParseError> {
        Ok(NativeExecutable {
            query_id: QueryId::from(env::var(QUERY_ID)?.parse::<u64>()?),
            threads: env::var(THREADS)?.parse::<usize>()?,
            process: env::var(PROCESS)?.parse::<usize>()?,
            hostlist: env::var(HOSTLIST)?.split('|').map(From::from).collect(),
            coord: env::var(COORD)?,
            host: env::var(HOST)?,
        })
    }
}

#[derive(Debug)]
pub struct Builder {
    // executable, including command line arguments
    cmd: Command,
    // timely config
    threads: usize,
    process: usize,
    hostlist: Cow<'static, str>,
}

impl Builder {
    /// Creates a new builder for the given native executable.
    pub fn new<T, S, I>(executable: T, args: I) -> Self
        where T: AsRef<OsStr>, S: AsRef<OsStr>, I: IntoIterator<Item = S>
    {
        let mut cmd = Command::new(executable);
        cmd.args(args);
        Builder {
            cmd: cmd,
            threads: 1,
            process: 0,
            hostlist: Cow::Borrowed("localhost"),
        }
    }

    /// Sets the number of Timely threads *per worker* (default: 1)
    pub fn threads(&mut self, threads: usize) -> &mut Self {
        self.threads = threads;
        self
    }

    /// Sets the current Timely process id (default: 0)
    pub fn process(&mut self, process: usize) -> &mut Self {
        self.process = process;
        self
    }

    /// Specify the host names of all Timely processes (default: ["localhost"])
    pub fn hostlist<S: Display, I: IntoIterator<Item=S>>(&mut self, hostlist: I) -> &mut Self {
        let mut list = hostlist.into_iter();
        let mut hostlist = list.next().expect("empty iterator").to_string();
        for host in list {
            write!(&mut hostlist, "|{}", host).unwrap();
        }
        self.hostlist = Cow::Owned(hostlist);
        self
    }
}

// On Windows, we might want to keep a RawHandle instead of the PID
type ProcessId = u32;

/// The process supervision logic.
///
/// This structure is responsible for keeping track of the currently running
/// child processes, their output and handles potential termination requests.
///
/// All supervised children are terminated when the tokio event loop is dropped,
/// though this might change in the future.
#[derive(Debug)]
pub struct ProcessService {
    /// currently running child processes
    running: Rc<RefCell<HashMap<QueryId, ProcessId>>>,
    /// handle for spawning futures in tokio
    handle: Handle,
    /// strymon configuration, to be passed down to spawned jobs
    coord: String,
    hostname: String,
}

impl ProcessService {
    /// Create a new service which will spawn processes using the tokio event
    /// loop of the `handle`. The `coord` and `hostname` configuration values
    /// are passed down to the spawned jobs
    pub fn new(handle: &Handle, coord: String, hostname: String) -> Self {
        ProcessService {
            running: Default::default(),
            handle: handle.clone(),
            coord: coord,
            hostname: hostname,
        }
    }

    /// handles and logs the stdout/stderr of the spawned child process
    fn log_output(&mut self, id: QueryId, child: &mut Child) {
        let stdout = child.stdout().take().unwrap();
        let reader = BufReader::new(stdout);
        let lines = tokio_io::io::lines(reader).map_err(|err| {
            error!("failed to read stdout: {}", err)
        });

        self.handle.spawn(lines.for_each(move |line| {
            Ok(info!("{:?} | {}", id, line))
        }));

        let stderr = child.stderr().take().unwrap();
        let reader = BufReader::new(stderr);
        let lines = tokio_io::io::lines(reader).map_err(|err| {
            error!("failed to read stderr: {}", err)
        });

        self.handle.spawn(lines.for_each(move |line| {
            Ok(warn!("{:?} | {}", id, line))
        }));
    }

    // sets up output handling, stores the PID for termination and installs a "SIGCHILD handler"
    fn supervise(&mut self, job_id: QueryId, mut child: Child) {
        // keep the pid of the running child for termination
        self.running.borrow_mut().insert(job_id, child.id());
        // setup the logging of stdout/stderr
        self.log_output(job_id, &mut child);
        // clone handle to child lookup table such it can be removed upon completion
        let running = self.running.clone();
        // the following continuation is executed when the child terminates
        self.handle.spawn(child.then(move |status| {
            // remove the childs pid from bookkeeping
            running.borrow_mut().remove(&job_id);
            // log the child's termination status
            match status {
                Ok(code) => if !code.success() {
                    warn!("child exited with non-zero code: {:?}", code.code());
                },
                Err(err) => error!("failed to supervise child: {}", err),
            };

            Ok(())
        }));
    }

    /// Spawns a new process to be supervised by this process service. The Timely
    /// configuration stored in the Builder is extracted and passed down to the
    /// spawned child process.
    pub fn spawn(&mut self, id: QueryId, mut process: Builder) -> Result<(), SpawnError> {
        let child = process.cmd
            .env(QUERY_ID, id.0.to_string())
            .env(THREADS, process.threads.to_string())
            .env(PROCESS, process.threads.to_string())
            .env(HOSTLIST, process.hostlist.as_ref())
            .env(COORD, self.coord.clone())
            .env(HOST, self.hostname.clone())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null())
            .spawn_async(&self.handle)
            .map_err(|_| SpawnError::ExecFailed)?;

        self.supervise(id, child);

        Ok(())
    }

    /// Tries to terminate the process assigned to a given job identifier, this
    /// will result in a SIGTERM being send to the child on Unix platforms.
    pub fn terminate(&mut self, job_id: QueryId) -> Result<(), TerminateError> {
        if let Some(pid) = self.running.borrow_mut().remove(&job_id) {
            terminate_process(pid)
        } else {
            // We trust that the coordinator only sends us termination requests
            // for jobs which exist and have been started. If we cannot find
            // a running process for a job, this means that it might have been
            // terminated already, so we just ignore this request.
           Ok(())
        }
    }
}

#[cfg(unix)]
fn terminate_process(pid: ProcessId) -> Result<(), TerminateError> {
    // we ignore the return value of `kill` here, as the only expected error
    // is ESRCH which means the process finished right before we sent the signal,
    // in which case we don't have to do anything.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }

    Ok(())
}

#[cfg(not(unix))]
fn terminate_process(pid: ProcessId) -> Result<(), TerminateError> {
    Err(TerminateError::OperationNotSupported)
}
