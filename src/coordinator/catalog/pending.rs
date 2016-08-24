use coordinator::worker::WorkerRef;
use coordinator::request::{SubmissionError, WorkerError};
use coordinator::catalog::query::Query;

use query::{QueryConfig, QueryId};
use worker::WorkerIndex;

use util::promise::Promise;

pub struct Pending {
    id: QueryId,
    config: QueryConfig,

    submission: Promise<QueryId, SubmissionError>,
    workers: Vec<Option<(WorkerRef, Promise<(), WorkerError>)>>,
}

impl Pending {
    pub fn new(id: QueryId,
               config: QueryConfig,
               promise: Promise<QueryId, SubmissionError>)
               -> Self {
        let total_workers = config.num_executors * config.num_workers;
        Pending {
            id: id,
            config: config,
            submission: promise,
            workers: (0..total_workers).map(|_| None).collect(),
        }
    }

    pub fn add_worker(&mut self,
                      index: WorkerIndex,
                      worker_ref: WorkerRef,
                      promise: Promise<(), WorkerError>) {
        // check validity of worker index
        if index >= self.workers.len() || self.workers[index].is_some() {
            return promise.failed(WorkerError::InvalidWorkerId);
        }

        self.workers[index] = Some((worker_ref, promise));
    }

    pub fn ready(&self) -> bool {
        self.workers.iter().all(|entry| entry.is_some())
    }

    pub fn promote(self) -> Query {
        assert!(self.ready(), "cannot finish pending query");

        let workers = self.workers
            .into_iter()
            .map(|opt| opt.expect("missing worker in pending query"))
            .map(|(worker, promise)| {
                promise.fulfil(());
                worker
            })
            .collect();

        self.submission.fulfil(self.id);

        Query::new(self.id, self.config, workers)
    }
}
