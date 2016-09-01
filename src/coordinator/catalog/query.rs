use query::{QueryParams, QueryId};

use coordinator::worker::WorkerRef;

pub struct Query {
    params: QueryParams,
    workers: Vec<WorkerRef>,
}

impl Query {
    pub fn new(query: QueryParams, workers: Vec<WorkerRef>) -> Self {
        assert_eq!(query.processes * query.threads, workers.len());

        Query {
            params: query,
            workers: workers,
        }
    }
}
