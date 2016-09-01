use std::env;
use std::num;
use std::process::Command;
use std::ffi::OsStr;

use query::QueryParams;
use executor::request::SpawnError;

pub const CONFIG_KEY_QUERYID: &'static str = "EXECUTOR_CONFIG_QUERYID";
pub const CONFIG_KEY_WORKER: &'static str = "EXECUTOR_CONFIG_WORKER";
pub const CONFIG_KEY_COORD: &'static str = "EXECUTOR_CONFIG_COORD";
pub const CONFIG_KEY_HOST: &'static str = "EXECUTOR_CONFIG_HOST";

pub struct ExecutableConfig {
    pub query: QueryParams,
    pub process: usize,
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
    /*
        Ok(WorkerConfig {
            query: QueryId::from(env::var(ENV_KEY_QUERY)?.parse::<u64>()?),
            index: env::var(ENV_KEY_INDEX)?.parse::<usize>()?,
            peers: env::var(ENV_KEY_PEERS)?.parse::<usize>()?,
            coord: env::var(ENV_KEY_COORD)?,
        })
*/
        unimplemented!()
    }
}

pub fn spawn<S: AsRef<OsStr>>(exec: S, query: &QueryParams, process: usize) -> Result<(), SpawnError> {
    unimplemented!()
}
