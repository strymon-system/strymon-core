use query::{QueryId, QueryParams};
use worker::WorkerIndex;

use messaging::request::Complete;

use coordinator::worker::WorkerRef;
use coordinator::request::*;
use coordinator::catalog::query::Query;

pub struct Pending {
    params: QueryParams,

    submission: Complete<Submission>,
    workers: Vec<Option<(WorkerRef, Complete<WorkerReady>)>>,
}

impl Pending {
    pub fn new(query: QueryParams, promise: Complete<Submission>) -> Self {
        let total_workers = query.threads * query.processes;

        Pending {
            params: query,
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

        self.submission.success(self.params.id);

        Query::new(self.params, workers)
    }
}
