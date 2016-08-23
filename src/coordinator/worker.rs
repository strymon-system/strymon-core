use std::io::Result;

use worker::WorkerIndex;
use query::QueryId;

use super::Connection;

pub struct Worker {
    
}

impl Worker {
    pub fn new(query_id: QueryId, worker_index: WorkerIndex, conn: Connection) -> Self {
        Worker {}
    }

    pub fn run(&mut self) -> Result<()> {
        Ok(())
    }
}
