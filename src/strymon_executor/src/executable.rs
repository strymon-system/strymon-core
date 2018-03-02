// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::rc::Rc;
use std::cell::RefCell;
use std::process::{Command, Stdio};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::{BufReader};
use std::path::Path;

use libc;

use futures::{Future, Stream};
use tokio_io;
use tokio_core::reactor::Handle;
use tokio_process::{Child, CommandExt};

use strymon_model::JobId;
use strymon_model::config::job::Process;
use strymon_rpc::executor::{SpawnError, TerminateError};

#[derive(Debug)]
pub struct Builder {
    // executable, including command line arguments
    cmd: Command,
    // timely config
    threads: usize,
    process: usize,
    hostlist: Vec<String>,
}

impl Builder {
    /// Creates a new builder for the given native executable.
    pub fn new<T, S, I>(executable: T, args: I) -> Result<Self, SpawnError>
        where T: AsRef<Path>, S: AsRef<OsStr>, I: IntoIterator<Item = S>
    {
        let executable = executable
            .as_ref()
            .canonicalize()
            .map_err(|_| SpawnError::FileNotFound)?;
        let mut cmd = Command::new(executable);
        cmd.args(args);
        Ok(Builder {
            cmd: cmd,
            threads: 1,
            process: 0,
            hostlist: vec![],
        })
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

    /// Specify the host names of all Timely processes (default: [])
    pub fn hostlist(&mut self, hostlist: Vec<String>) -> &mut Self {
        self.hostlist = hostlist;
        self
    }

    /// Sets the working directory of the child process (default: the current working directory)
    pub fn working_directory<P: AsRef<Path>>(&mut self, workdir: P) -> &mut Self {
        self.cmd.current_dir(workdir);
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
    running: Rc<RefCell<HashMap<JobId, ProcessId>>>,
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
    fn log_output(&mut self, id: JobId, child: &mut Child) {
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
    fn supervise(&mut self, job_id: JobId, mut child: Child) {
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
    pub fn spawn(&mut self, id: JobId, builder: Builder) -> Result<(), SpawnError> {
        let conf = Process {
            job_id: id,
            index: builder.process,
            addrs: builder.hostlist,
            threads: builder.threads,
            coord: self.coord.clone(),
            hostname: self.hostname.clone(),
        };

        let mut command = builder.cmd;
        let child = command
            .envs(conf.into_env())
            .env("RUST_BACKTRACE", "1")
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
    pub fn terminate(&mut self, job_id: JobId) -> Result<(), TerminateError> {
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
