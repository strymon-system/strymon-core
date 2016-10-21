use std::env;
use std::num;
use std::process::{Command, Child};
use std::ffi::OsStr;
use std::thread::Builder as ThreadBuilder;

use model::QueryId;
use executor::requests::SpawnError;

pub const QUERY_ID: &'static str = "TIMELY_EXEC_CONF_QUERY_ID";
pub const THREADS: &'static str = "TIMELY_EXEC_CONF_THREADS";
pub const PROCESS: &'static str = "TIMELY_EXEC_CONF_PROCESS";
pub const HOSTLIST: &'static str = "TIMELY_EXEC_CONF_HOSTLIST";
pub const COORD: &'static str = "TIMELY_EXEC_CONF_COORD";
pub const HOST: &'static str = "TIMELY_EXEC_CONF_HOST";

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
    let mut child = Command::new(executable)
        .args(args)
        .env("RUST_LOG", "debug") // TODO(swicki): this is only for debugging
        .env(QUERY_ID, id.0.to_string())
        .env(THREADS, threads.to_string())
        .env(PROCESS, process.to_string())
        .env(HOSTLIST, hostlist.join("|"))
        .env(COORD, coord)
        .env(HOST, host)
        .spawn()
        .map_err(|_| SpawnError::ExecFailed)?;

    // TODO(swicki): This a bit an expensive way to deal with zombies
    ThreadBuilder::new()
        .stack_size(128)
        .spawn(move || child.wait().ok())
        .expect("failed to spawn reaper thread");

    Ok(())
}
