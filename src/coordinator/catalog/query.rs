use query::{QueryConfig, QueryId};

use coordinator::worker::WorkerRef;

pub struct Query {
    id: QueryId,
    config: QueryConfig,
    workers: Vec<WorkerRef>,
}

impl Query {
    pub fn new(id: QueryId, config: QueryConfig, workers: Vec<WorkerRef>) -> Self {
        assert_eq!(config.num_workers * config.num_executors, workers.len());

        Query {
            id: id,
            config: config,
            workers: workers,
        }
    }
}
