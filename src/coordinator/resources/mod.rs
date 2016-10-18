use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Entry;
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
use self::query::QueryState;
use super::util::Generator;

pub mod executor;
pub mod submission;
pub mod query;

pub struct Coordinator {
    handle: Weak<RefCell<Coordinator>>,
    catalog: Catalog,

    queryid: Generator<QueryId>,
    executorid: Generator<ExecutorId>,

    executors: BTreeMap<ExecutorId, ExecutorState>,
    // TODO use enum here..?
    submissions: BTreeMap<QueryId, SubmissionState>,
    queries: BTreeMap<QueryId, QueryState>,
    lookups: HashMap<String, Vec<Complete<Topic, SubscribeError>>>,
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
            queries: BTreeMap::new(),
            lookups: HashMap::new(),
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
                .filter(|e| executor_res[&e.id].has_ports());

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
        debug!("selected executors for {:?}:{:?}", query.id, executors);
        for executor in &executors {
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
        }

        // TODO(swicki) add a timeout that triggers SpawnFailed here
        
        debug!("add pending submission for {:?}", query.id);
        let (submission, rx) = SubmissionState::new(query);
        self.submissions.insert(queryid, submission);

        return rx;
    }

    fn add_executor(&mut self, req: AddExecutor, tx: Outgoing) -> ExecutorId {
        let id = self.executorid.generate();
        debug!("adding executor {:?} to pool", id);

        let state = ExecutorState::new(tx, req.ports);
        let executor = Executor {
            id: id,
            host: req.host,
            format: req.format,
        };

        self.executors.insert(id, state);
        self.catalog.add_executor(executor);
        id
    }

    fn remove_executor(&mut self, id: ExecutorId) {
        debug!("removing executor {:?} from pool", id);
        self.executors.remove(&id);
        self.catalog.remove_executor(id);
    }
    
    fn add_worker_group(&mut self, id: QueryId, group: usize, out: Outgoing)
        -> Promise<QueryToken, WorkerGroupError>
    {
        debug!("adding worker {:?} of {:?}", group, id);
        match self.submissions.entry(id) {
            Entry::Occupied(mut submission) => {
                // add worker to wait list
                let (ready, rx) = submission.get_mut().add_worker_group(group, out);

                if ready {
                // if it was the last worker, we move the query to the ready list
                    let query = match submission.remove().promote() {
                        Ok(query) => query,
                        Err(_) => panic!("failed to promote submission"),
                    };
                    self.queries.insert(id, query);
                }
                
                rx
            }
            Entry::Vacant(_) => {
                // TODO(swicki): use futures::failed
                let (tx, rx) = promise();
                tx.complete(Err(WorkerGroupError::InvalidQueryId));
                rx
            }
        }
    }

    fn check_token(&self, token: &QueryToken) -> bool {
        if let Some(ref reference) = self.queries.get(&token.0) {
            *token == reference.token()
        } else {
            false
        }
    }

    fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        if !self.check_token(&req.token) {
            Err(PublishError::AuthenticationFailure)
        } else {
            let query = req.token.0;
            let result = self.catalog.publish(query, req.name, req.addr, req.kind);
            if let Ok(ref topic) = result {
                if let Some(pending) = self.lookups.remove(&topic.name) {
                    for tx in pending {
                        tx.complete(Ok(topic.clone()));
                    }
                }
            }
            result
        }
    }

    fn subscribe(&mut self, req: Subscribe) -> Promise<Topic, SubscribeError> {
        let (tx, rx) = promise();
        if !self.check_token(&req.token) {
            tx.complete(Err(SubscribeError::AuthenticationFailure));
        } else {
            let query = req.token.0;

            if let Some(topic) = self.catalog.lookup(&req.name) {
                self.catalog.subscribe(query, topic.id);
                tx.complete(Ok(topic));
            } else if req.blocking {
                self.lookups.entry(req.name).or_insert(Vec::new()).push(tx);
            } else {
                tx.complete(Err(SubscribeError::TopicNotFound));
            }
        }
        
        rx
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

    pub fn add_worker_group(&mut self, id: QueryId, group: usize, out: Outgoing)
        -> Promise<QueryToken, WorkerGroupError>
    {
        self.coord.borrow_mut().add_worker_group(id, group, out)
    }

    pub fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        self.coord.borrow_mut().publish(req)
    }
    
    pub fn subscribe(&mut self, req: Subscribe) -> Promise<Topic, SubscribeError> {
        self.coord.borrow_mut().subscribe(req)
    }

    fn cancel_submission(&self, id: QueryId, err: SubmissionError) {
        self.coord.borrow_mut().submissions.remove(&id).map(|tx| {
            // TODO
            // tx.complete(Err(err));
        });
    }
}
