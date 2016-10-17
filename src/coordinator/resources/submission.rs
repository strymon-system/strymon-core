use async::promise::{promise, Complete, Promise};
use network::reqresp::Outgoing;

use model::{QueryId, Query};
use coordinator::requests::*;

pub struct SubmissionState {
    template: Query,
    response: Complete<QueryId, SubmissionError>,
    worker: Vec<Option<(Outgoing, Complete<QueryToken, WorkerGroupError>)>>,
}

impl SubmissionState {
    pub fn new(query: Query) -> (Self, Promise<QueryId, SubmissionError>) {
        let (tx, rx) = promise();
        let worker_groups = query.workers / query.executors.len();
        let state = SubmissionState {
            template: query,
            response: tx,
            worker: (0..worker_groups).map(|_| None).collect(),
        };

        (state, rx)
    }

    pub fn add_worker_group(mut self,
                            group: usize,
                            out: Outgoing)
                            -> Promise<QueryToken, WorkerGroupError> {
        let (tx, rx) = promise();
        if group >= self.worker.len() {
            tx.complete(Err(WorkerGroupError::InvalidWorkerGroup));
        } else {
            self.worker[group] = Some((out, tx));
        }

        rx
    }
}
