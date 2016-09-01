use std::env;
use std::num;

use query::QueryId;
use worker::WorkerIndex;

// TODO remove
pub struct WorkerConfig {
    pub query: QueryId,
    pub index: WorkerIndex,
    pub peers: usize,
    pub coord: String,
}
