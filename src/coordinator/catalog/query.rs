use query::{QueryParams};

use coordinator::worker::WorkerRef;

pub struct Query {
    _params: QueryParams,
    _workers: Vec<WorkerRef>,
}

impl Query {
    pub fn new(query: QueryParams, workers: Vec<WorkerRef>) -> Self {
        assert_eq!(query.processes * query.threads, workers.len());

        Query {
            _params: query,
            _workers: workers,
        }
    }
}
