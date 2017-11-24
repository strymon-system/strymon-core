// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::env;
use std::num;
use std::process::{Command, Stdio};
use std::ffi::OsStr;
use std::fmt::{Write, Display};
use std::io::{BufReader};

use futures::{Future, Stream};
use tokio_io;
use tokio_core::reactor::Handle;
use tokio_process::CommandExt;

use strymon_model::QueryId;
use strymon_rpc::executor::SpawnError;

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
    threads: Option<usize>,
    process: Option<usize>,
    hostlist: Option<String>,
    // strymon config
    coord: Option<String>,
    hostname: Option<String>,
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
            threads: None,
            process: None,
            hostlist: None,
            coord: None,
            hostname: None,
        }
    }

    /// Sets the number of Timely threads *per worker* (default: 1)
    pub fn threads(&mut self, threads: usize) -> &mut Self {
        self.threads = Some(threads);
        self
    }

    /// Sets the current Timely process id (default: 0)
    pub fn process(&mut self, process: usize) -> &mut Self {
        self.process = Some(process);
        self
    }

    /// Specify the host names of all Timely processes (panics if not set)
    pub fn hostlist<S: Display, I: IntoIterator<Item=S>>(&mut self, hostlist: I) -> &mut Self {
        let mut list = hostlist.into_iter();
        let mut hostlist = list.next().map(|s| s.to_string());
        for host in list {
            // both of these unwraps cannot fail:
            // 1. hostlist cannot be None if the iterator yields more elements
            // 2. write_str to a String never fails
            write!(hostlist.as_mut().unwrap(), "|{}", host).unwrap();
        }
        self.hostlist = hostlist;
        self
    }

    /// Address of the coordinator (panics if not set)
    pub fn coord(&mut self, coord: &str) -> &mut Self {
        self.coord = Some(coord.to_string());
        self
    }

    /// Externally reachable hostname of the child (panics if not set)
    pub fn hostname(&mut self, hostname: &str) -> &mut Self {
        self.hostname = Some(hostname.to_string());
        self
    }

    /// Spawns the given command on the given event
    pub fn spawn(mut self, id: QueryId, handle: &Handle) -> Result<(), SpawnError> {
        let mut child = self.cmd
            .env(QUERY_ID, id.0.to_string())
            .env(THREADS, self.threads.unwrap_or(1).to_string())
            .env(PROCESS, self.threads.unwrap_or(0).to_string())
            .env(HOSTLIST, self.hostlist.expect("missing hostname"))
            .env(COORD, self.coord.expect("missing coordinator"))
            .env(HOST, self.hostname.expect("missing external hostname"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null())
            .spawn_async(handle)
            .map_err(|_| SpawnError::ExecFailed)?;

        // read lines from stdout and stderr
        let stdout = child.stdout().take().unwrap();
        let reader = BufReader::new(stdout);
        let lines = tokio_io::io::lines(reader).map_err(|err| {
            error!("failed to read stdout: {}", err)
        });

        handle.spawn(lines.for_each(move |line| {
            Ok(info!("{:?} | {}", id, line))
        }));

        let stderr = child.stderr().take().unwrap();
        let reader = BufReader::new(stderr);
        let lines = tokio_io::io::lines(reader).map_err(|err| {
            error!("failed to read stderr: {}", err)
        });

        handle.spawn(lines.for_each(move |line| {
            Ok(warn!("{:?} | {}", id, line))
        }));

        // wait for child to finish
        handle.spawn(child.then(|result| {
            match result {
                Ok(code) if code.success() => Ok(()),
                Ok(code) => Ok(warn!("child exited with non-zero code: {:?}", code.code())),
                Err(err) => Err(error!("failed to wait for child: {}", err)),
            }
        }));

        Ok(())
    }
}
