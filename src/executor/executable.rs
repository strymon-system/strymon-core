use std::env;
use std::num;
use std::process::{Command, Stdio};
use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Error, ErrorKind};
use std::thread;

use model::QueryId;
use executor::requests::SpawnError;

pub const QUERY_ID: &'static str = "TIMELY_EXEC_CONF_QUERY_ID";
pub const THREADS: &'static str = "TIMELY_EXEC_CONF_THREADS";
pub const PROCESS: &'static str = "TIMELY_EXEC_CONF_PROCESS";
pub const HOSTLIST: &'static str = "TIMELY_EXEC_CONF_HOSTLIST";
pub const COORD: &'static str = "TIMELY_EXEC_CONF_COORD";
pub const HOST: &'static str = "TIMELY_QUERY_HOSTNAME";

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

pub fn spawn<S: AsRef<OsStr>>(executable: S,
                              id: QueryId,
                              args: &[String],
                              threads: usize,
                              process: usize,
                              hostlist: &[String],
                              coord: &str,
                              host: &str)
                              -> Result<(), SpawnError> {
    let mut child = Command::new(executable).args(args)
        .env(QUERY_ID, id.0.to_string())
        .env(THREADS, threads.to_string())
        .env(PROCESS, process.to_string())
        .env(HOSTLIST, hostlist.join("|"))
        .env(COORD, coord)
        .env(HOST, host)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .spawn()
        .map_err(|_| SpawnError::ExecFailed)?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // TODO(swicki) On Unix, we could make this more efficient using RawFd and signals
    thread::spawn(move || {
        let mut stdout = BufReader::new(stdout.expect("no stdout?!")).lines();
        while let Some(Ok(line)) = stdout.next() {
            info!("{:?} | {}", id, line)
        }

        let result = child.wait().and_then(|code| if code.success() {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::Other, "child exited with non-zero code"))
        });

        if let Err(err) = result {
            error!("{:?} | child failed: {}", id, err.to_string())
        }
    });

    thread::spawn(move || {
        let mut stderr = BufReader::new(stderr.expect("no stderr?!")).lines();
        while let Some(Ok(line)) = stderr.next() {
            warn!("{:?} | {}", id, line)
        }
    });

    Ok(())
}
