use async::promise::{promise, Complete, Promise};
use network::reqresp::Outgoing;

use model::{QueryId, Query};
use coordinator::resources::query::QueryState;
use coordinator::requests::*;

pub struct SubmissionState {
    template: Query,
    response: Complete<QueryId, SubmissionError>,
    worker: Vec<Option<(Outgoing, Complete<QueryToken, WorkerGroupError>)>>,
}

impl SubmissionState {
    pub fn new(query: Query) -> (Self, Promise<QueryId, SubmissionError>) {
        let (tx, rx) = promise();
        let worker_groups = query.executors.len();
        let state = SubmissionState {
            template: query,
            response: tx,
            worker: (0..worker_groups).map(|_| None).collect(),
        };

        (state, rx)
    }

    pub fn add_worker_group(&mut self,
                            group: usize,
                            out: Outgoing)
                            -> (bool, Promise<QueryToken, WorkerGroupError>) {
        let (tx, rx) = promise();
        if group >= self.worker.len() {
            tx.complete(Err(WorkerGroupError::InvalidWorkerGroup));
        } else {
            self.worker[group] = Some((out, tx));
        }

        debug!("added worker group {} ({} total)", group, self.worker.len());

        (self.worker.iter().all(Option::is_some), rx)
    }
    
    pub fn promote(mut self) -> Result<QueryState, Self> {
        if self.worker.iter().any(Option::is_none) {
            return Err(self);
        }

        let state = QueryState::new();
        let worker = self.worker.into_iter().map(Option::unwrap);
        for (_, complete) in worker {
            complete.complete(Ok(state.token()))
        }
        self.response.complete(Ok(self.template.id));
        Ok(state)
    }
}
