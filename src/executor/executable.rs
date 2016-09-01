use std::env;
use std::num;
use std::process::Command;
use std::ffi::OsStr;

use query::{QueryId, QueryParams};
use executor::request::SpawnError;

pub const QUERY_ID: &'static str = "TIMELY_EXEC_CONF_QUERY_ID";
pub const THREADS: &'static str = "TIMELY_EXEC_CONF_THREADS";
pub const PROCESSES: &'static str = "TIMELY_EXEC_CONF_PROCESSES";
pub const HOSTLIST: &'static str = "TIMELY_EXEC_CONF_HOSTLIST";
pub const PROCINDEX: &'static str = "TIMELY_EXEC_CONF_PROCINDEX";
pub const COORD: &'static str = "TIMELY_EXEC_CONF_COORD";
pub const HOST: &'static str = "TIMELY_EXEC_CONF_HOST";

#[derive(Debug)]
pub struct ExecutableConfig {
    pub query: QueryParams,
    pub procindex: usize,
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

impl ExecutableConfig {
    pub fn from_env() -> Result<Self, ParseError> {  
        let hostlist = env::var(HOSTLIST)?.split('|').map(From::from).collect();
        let query = QueryParams {
            id: QueryId::from(env::var(QUERY_ID)?.parse::<u64>()?),
            threads: env::var(THREADS)?.parse::<usize>()?,
            processes: env::var(PROCESSES)?.parse::<usize>()?,
            hostlist: hostlist,
        };
        
        Ok(ExecutableConfig {
            query: query,
            procindex: env::var(PROCINDEX)?.parse::<usize>()?,
            coord: env::var(COORD)?,
            host: env::var(HOST)?
        })
    }
}

pub fn spawn<S: AsRef<OsStr>>(exec: S, query: &QueryParams, procindex: usize, coord: &str, host: &str) -> Result<(), SpawnError> {
    Command::new(exec)
        .env(QUERY_ID, query.id.0.to_string())
        .env(THREADS, query.threads.to_string())
        .env(PROCESSES, query.processes.to_string())
        .env(HOSTLIST, query.hostlist.join("|"))
        .env(PROCINDEX, procindex.to_string())
        .env(COORD, coord)
        .env(HOST, host)
        .spawn()
        .map(|_| ())
        .map_err(|_| SpawnError::ExecFailed)
}
