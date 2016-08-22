use std::sync::mpsc;
use std::ops::RangeFrom;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use query::{QueryId, QueryConfig};
use executor::{ExecutorId, ExecutorType};
use messaging::response::Promise;

pub use self::request::*;

use self::pending::*;
use self::query::*;
use self::executor::*;

mod request;
mod pending;
mod executor;
mod query;

pub type CatalogRef = mpsc::Sender<Request>;

pub struct Catalog {
    pending: BTreeMap<QueryId, Pending>,
    queries: BTreeMap<QueryId, Query>,

    executors: Executors,

    query_id: Generator<QueryId>,
    requests: mpsc::Receiver<Request>,
}

impl Catalog {
    pub fn new() -> (CatalogRef, Catalog) {
        let (tx, rx) = mpsc::channel();

        let catalog = Catalog {
            pending: BTreeMap::new(),
            queries: BTreeMap::new(),
            executors: Executors::new(),

            query_id: Generator::new(),
            requests: rx,
        };

        (tx, catalog)
    }

    pub fn run(&mut self) {
        while let Ok(request) = self.requests.recv() {
            self.process(request);
        }
    }

    pub fn process(&mut self, request: Request) {
        use self::Request::*;
        match request {
            Submission(submission, promise) => self.submission(submission, promise),
        };
    }

    pub fn submission(&mut self,
                      submission: Submission,
                      promise: Promise<QueryId, SubmissionError>) {
        let id = self.query_id.generate();
        let pending = Pending::new(id, submission.config, promise);
        // TODO select and inform executors
        self.pending.insert(id, pending);
    }
}

struct Generator<T> {
    generator: RangeFrom<u64>,
    marker: PhantomData<T>,
}

impl<T: From<u64>> Generator<T> {
    fn new() -> Self {
        Generator {
            generator: 0..,
            marker: PhantomData,
        }
    }

    fn generate(&mut self) -> T {
        From::from(self.generator.next().unwrap())
    }
}
