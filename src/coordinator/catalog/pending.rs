use query::{QueryConfig, QueryId};
use worker::WorkerIndex;

use messaging::request::Complete;
use messaging::request::handler::Handoff;

use coordinator::worker::WorkerRef;
use coordinator::request::*;
use coordinator::catalog::query::Query;

pub struct Pending {
    id: QueryId,
    config: QueryConfig,

    submission: Handoff<Submission>,
    workers: Vec<Option<(WorkerRef, Complete<WorkerReady>)>>,
}

impl Pending {
    pub fn new(id: QueryId, config: QueryConfig, promise: Handoff<Submission>) -> Self {
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
                      promise: Complete<WorkerReady>) {
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
                promise.success(());
                worker
            })
            .collect();

        self.submission.success(self.id);

        Query::new(self.id, self.config, workers)
    }
}
