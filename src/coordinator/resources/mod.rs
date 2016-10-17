use std::collections::BTreeMap;
use std::rc::{Rc, Weak};
use std::cell::RefCell;

use futures::Future;
use futures::stream::Stream;
use rand;

use async;
use async::promise::{promise, Promise, Complete, Cancellation};
use network::reqresp::Outgoing;
use model::*;
use executor::requests::*;

use coordinator::requests::*;
use coordinator::catalog::Catalog;

use self::executor::ExecutorState;
use self::submission::SubmissionState;
use self::util::Generator;

pub mod executor;
pub mod submission;

mod util;

pub struct Coordinator {
    handle: Weak<RefCell<Coordinator>>,
    catalog: Catalog,

    queryid: Generator<QueryId>,
    executorid: Generator<ExecutorId>,

    executors: BTreeMap<ExecutorId, ExecutorState>,
    submissions: BTreeMap<QueryId, SubmissionState>,
}

impl Coordinator {
    pub fn new() -> CoordinatorRef {
        let coord = Coordinator {
            handle: Weak::new(),
            catalog: Catalog::new(),
            queryid: Generator::new(),
            executorid: Generator::new(),
            executors: BTreeMap::new(),
            submissions: BTreeMap::new(),
        };

        // we use weak references to avoid cycles
        let coord = Rc::new(RefCell::new(coord));
        coord.borrow_mut().handle = Rc::downgrade(&coord);
        CoordinatorRef { coord: coord }
    }

    fn handle(&self) -> CoordinatorRef {
        let coord = self.handle.upgrade().unwrap();
        CoordinatorRef { coord: coord }
    }

    fn submission(&mut self, req: Submission) -> Promise<QueryId, SubmissionError> {
        // workaround: prevent closures borrowing `self`
        let handle = self.handle();
        let executor_res = &mut self.executors;

        // step 1: generate query id
        let queryid = self.queryid.generate();

        // step 2: Select suitable executors
        let (executors, num_executors, num_workers) = {
            // step 2.1: filter out executors with the wrong format,
            // and the ones with no more free network ports
            let format = &req.query.format;
            let executors = self.catalog
                .executors()
                .filter(|e| e.format == *format)
                .filter(|e| !executor_res[&e.id].has_ports());

            // step 2.2: select executors according to user placment
            let (executors, num_executors, num_workers) = match req.placement {
                Placement::Random(num_executors, num_workers) => {
                    let mut rng = rand::thread_rng();
                    let executors = rand::sample(&mut rng, executors, num_executors);
                    (executors, num_executors, num_workers)
                }
                Placement::Fixed(executor_ids, num_workers) => {
                    let num_executors = executor_ids.len();
                    let executors = executors.filter(|e| executor_ids.contains(&e.id))
                        .collect();
                    (executors, num_executors, num_workers)
                }
            };

            // step 2.3: check if we actually have enough executors
            if executors.len() != num_executors {
                // TODO(swicki): use futures::failed here
                let (tx, rx) = promise();
                tx.complete(Err(SubmissionError::ExecutorsNotFound));
                return rx;
            }

            (executors, num_executors, num_workers)
        };

        // step 3: create the Timely configuration
        let hostlist: Vec<String> = executors.iter()
            .map(|executor| {
                let resources = executor_res.get_mut(&executor.id).unwrap();
                let port = resources.allocate_port();
                format!("{}:{}", executor.host, port)
            })
            .collect();

        let executor_ids = executors.iter().map(|e| e.id).collect();
        let query = Query {
            id: queryid,
            name: req.name,
            program: req.query,
            workers: num_executors * num_workers,
            executors: executor_ids,
        };
        let spawnquery = SpawnQuery {
            query: query.clone(),
            hostlist: hostlist,
        };

        // step 4: send requests to the selected coordinators
        let spawn = executors.iter().map(|executor| {
            let handle = handle.clone();
            let executor = &executor_res[&executor.id];
            let response = executor.spawn(&spawnquery)
                .map_err(move |_| {
                    // TODO(swicki): convert executor error to real error
                    // TODO(swicki): free port on failure
                    let err = SubmissionError::SpawnError;
                    handle.cancel_submission(queryid, err);
                });

            async::spawn(response);
        });

        // TODO(swicki) add a timeout that triggers SpawnFailed here
        let (submission, rx) = SubmissionState::new(query);
        self.submissions.insert(queryid, submission);

        return rx;
    }

    pub fn add_executor(&mut self, req: AddExecutor, tx: Outgoing) -> ExecutorId {
        let id = self.executorid.generate();
        let executor = ExecutorState::new(tx, req.ports);
        debug!("adding executor {:?} to pool", id);
        self.executors.insert(id, executor);
        id
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        debug!("removing executor {:?} from pool", id);
        self.executors.remove(&id);
    }
}

#[derive(Clone)]
pub struct CoordinatorRef {
    coord: Rc<RefCell<Coordinator>>,
}

impl CoordinatorRef {
    pub fn submission(&self, req: Submission) -> Promise<QueryId, SubmissionError> {
        self.coord.borrow_mut().submission(req)
    }

    pub fn add_executor(&mut self, req: AddExecutor, tx: Outgoing) -> ExecutorId {
        self.coord.borrow_mut().add_executor(req, tx)
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        self.coord.borrow_mut().remove_executor(id)
    }

    fn cancel_submission(&self, id: QueryId, err: SubmissionError) {
        self.coord.borrow_mut().submissions.remove(&id).map(|tx| {
            // TODO
            // tx.complete(Err(err));
        });
    }
}
