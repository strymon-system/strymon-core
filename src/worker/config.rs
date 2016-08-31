use std::env;
use std::num;

use query::QueryId;

pub type WorkerIndex = usize;

pub const ENV_KEY_QUERY: &'static str = "WORKER_CONFIG_QUERY";
pub const ENV_KEY_INDEX: &'static str = "WORKER_CONFIG_INDEX";
pub const ENV_KEY_PEERS: &'static str = "WORKER_CONFIG_PEERS";
pub const ENV_KEY_COORD: &'static str = "WORKER_CONFIG_COORD";

pub struct WorkerConfig {
    pub query: QueryId,
    pub index: WorkerIndex,
    pub peers: usize,
    pub coord: String,
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

impl From<::std::num::ParseIntError> for ParseError {
    fn from(int: num::ParseIntError) -> Self {
        ParseError::IntErr(int)
    }
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ParseError> {  
        Ok(WorkerConfig {
            query: QueryId::from(env::var(ENV_KEY_QUERY)?.parse::<u64>()?),
            index: env::var(ENV_KEY_INDEX)?.parse::<usize>()?,
            peers: env::var(ENV_KEY_PEERS)?.parse::<usize>()?,
            coord: env::var(ENV_KEY_COORD)?,
        })
    }
}
