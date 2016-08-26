use std::sync::mpsc;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::thread;

use query::{QueryConfig, QueryId};
use executor::{ExecutorId, ExecutorType};
use topic::Topic;
use worker::WorkerIndex;

use util::Generator;

use messaging::request::Complete;
use messaging::request::handler::Handoff;

use coordinator::request::*;
use coordinator::worker::WorkerRef;
use coordinator::executor::ExecutorRef;

use self::pending::*;
use self::query::*;
use self::executors::*;

mod pending;
mod executors;
mod query;

#[derive(Clone)]
pub struct CatalogRef(mpsc::Sender<Message>);

impl CatalogRef {
    pub fn send(&self, msg: Message) {
        self.0.send(msg).expect("invalid catalog ref")
    }
}

pub enum Message {
    Submission(Submission, Handoff<Submission>),
    WorkerReady(WorkerReady, WorkerRef, Complete<WorkerReady>),
    ExecutorReady(ExecutorReady, ExecutorRef, Complete<ExecutorReady>),
}

pub struct Catalog {
    pending: BTreeMap<QueryId, Pending>,
    queries: BTreeMap<QueryId, Query>,

    executors: Executors,

    query_id: Generator<QueryId>,
    requests: mpsc::Receiver<Message>,
}

impl Catalog {
    pub fn new() -> (CatalogRef, Catalog) {
        let (tx, rx) = mpsc::channel();

        let catalog_ref = CatalogRef(tx);
        let catalog = Catalog {
            pending: BTreeMap::new(),
            queries: BTreeMap::new(),
            executors: Executors::new(),

            query_id: Generator::new(),
            requests: rx,
        };

        (catalog_ref, catalog)
    }

    pub fn run(&mut self) {
        while let Ok(request) = self.requests.recv() {
            self.process(request);
        }
    }

    pub fn detach(mut self) {
        thread::spawn(move || self.run());
    }

    pub fn process(&mut self, request: Message) {
        use self::Message::*;
        match request {
            Submission(submission, promise) => self.submission(submission, promise),
            WorkerReady(worker, worker_ref, promise) => {
                self.worker_ready(worker.query, worker.index, worker_ref, promise);
            }
            ExecutorReady(executor, executor_ref, promise) => {
                self.executors.executor_ready(executor, executor_ref, promise);
            }
        };
    }

    pub fn worker_ready(&mut self,
                        query_id: QueryId,
                        index: WorkerIndex,
                        worker_ref: WorkerRef,
                        promise: Complete<WorkerReady>) {
        match self.pending.entry(query_id) {
            Entry::Occupied(mut pending) => {
                // add worker to wait list
                pending.get_mut().add_worker(index, worker_ref, promise);

                // if it was the last worker, we move the query to the ready list
                if pending.get().ready() {
                    let query = pending.remove().promote();
                    self.queries.insert(query_id, query);
                }
            }
            Entry::Vacant(_) => {
                return promise.failed(WorkerError::InvalidQueryId);
            }
        }
    }

    pub fn submission(&mut self, submission: Submission, promise: Handoff<Submission>) {
        let id = self.query_id.generate();
        let config = submission.config;

        // find suiting executors
        let selected = self.executors.select(config.binary, config.num_executors);

        // check if we have enough executors of the right type
        let executors = match selected {
            Some(ref executors) if executors.len() < config.num_executors => {
                return promise.failed(SubmissionError::NotEnoughExecutors);
            }
            None => {
                return promise.failed(SubmissionError::NoExecutorsForType);
            }
            Some(executors) => executors,
        };

        // ask executors to spawn a new query
        for executor in executors {
            executor.spawn(id, &config);
        }

        // install pending query
        let pending = Pending::new(id, config, promise);

        // TODO maybe we should add a timeout for the pending query..?!
        self.pending.insert(id, pending);
    }
}
