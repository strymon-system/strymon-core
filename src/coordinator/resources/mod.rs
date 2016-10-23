use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Entry;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::mem;

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
use super::util::Generator;

pub mod executor;

enum QueryState {
    Spawning {
        submitter: Complete<QueryId, SubmissionError>,
        waiting: Vec<Complete<QueryToken, WorkerGroupError>>,
    },
    Running,
    Terminating,
}

struct WorkerGroup {
    state: QueryState,
    count: usize,
}

pub struct Coordinator {
    handle: Weak<RefCell<Coordinator>>,
    catalog: Catalog,

    queryid: Generator<QueryId>,
    executorid: Generator<ExecutorId>,

    executors: BTreeMap<ExecutorId, ExecutorState>,
    queries: BTreeMap<QueryId, WorkerGroup>,
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
        let (tx, rx) = promise();

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
                    handle.remove_submission(queryid, err);
                });

            async::spawn(response);
        }

        // TODO(swicki) add a timeout that triggers SpawnFailed here
        debug!("add pending submission for {:?}", query.id);
        let state = QueryState::Spawning {
            submitter: tx,
            waiting: vec![],
        };

        let worker_group = WorkerGroup {
            state: state,
            count: executors.len(),
        };
        self.queries.insert(queryid, worker_group);

        return rx;
    }

    fn remove_submission(&mut self, id: QueryId, err: SubmissionError) {
        if let Some(query) = self.queries.remove(&id) {
            if let QueryState::Spawning { submitter, waiting } = query.state {
                submitter.complete(Err(err));
                for worker in waiting {
                    worker.complete(Err(WorkerGroupError::PeerFailed));
                }
            }
        }
    }

    fn add_worker_group(&mut self, id: QueryId, group: usize)
        -> Promise<QueryToken, WorkerGroupError>
    {
        let (tx, rx) = promise();
        let query = self.queries.get_mut(&id);

        // step 1: check if we actually know about this query
        let query = if query.is_none() {
            tx.complete(Err(WorkerGroupError::SpawningAborted));
            return rx;
        } else {
            query.unwrap()
        };

        // step 2: add current request to waiting workers
        let connected = match query.state {
            QueryState::Spawning { ref mut waiting, .. } => {
                waiting.push(tx);
                waiting.len()
            },
            QueryState::Running | QueryState::Terminating => {
                tx.complete(Err(WorkerGroupError::InvalidWorkerGroup));
                return rx;
            }
        };
        
        // check if we need to wait for others to arrive
        debug!("{:?}: {} of {} are connected", id, connected, query.count);
        if connected < query.count {
            return rx;
        }

        // step 3: at this point, all worker groups have registered themselves
        let waiting = mem::replace(&mut query.state, QueryState::Running);
        let (submitter, waiting) = match waiting {
            QueryState::Spawning { submitter, waiting } => (submitter, waiting),
            _ => unreachable!()
        };

        // step 4: respond to everyone
        let token = QueryToken {
            id: id,
            auth: rand::random::<u64>(),
        };
        for worker in waiting {
            worker.complete(Ok(token));
        }

        submitter.complete(Ok(id));

        rx
    }
    
    fn remove_worker_group(&mut self, id: QueryId) {
        let mut query = match self.queries.entry(id) {
            Entry::Occupied(query) => query,
            Entry::Vacant(_) => {
                warn!("request to remove inexisting worker group");
                return;
            }
        };

        // decrease counter, set to terminating
        let count = {
            let query = query.get_mut();
            query.state = QueryState::Terminating;
            query.count -= 1;

            query.count
        };

        // and we're done
        if count == 0 {
            query.remove();
        }
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

    fn check_token(&self, token: &QueryToken) -> bool {
        // TODO 
        /*if let Some(ref reference) = self.queries.get(&token.0) {
            *token == reference.token()
        } else {
            false
        }*/
        true
    }

    fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        if !self.check_token(&req.token) {
            Err(PublishError::AuthenticationFailure)
        } else {
            let query = req.token.id;
            let result = self.catalog.publish(query, req.name, req.addr, req.schema);
            if let Ok(ref topic) = result {
                debug!("resolving lookup for topic: {:?}", &topic.name);
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
            let query = req.token.id;

            if let Some(topic) = self.catalog.lookup(&req.name) {
                self.catalog.subscribe(query, topic.id);
                tx.complete(Ok(topic));
            } else if req.blocking {
                debug!("inserting blocking lookup for topic: {:?}", &req.name);
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

    pub fn add_worker_group(&mut self, id: QueryId, group: usize)
        -> Promise<QueryToken, WorkerGroupError>
    {
        self.coord.borrow_mut().add_worker_group(id, group)
    }

    pub fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        self.coord.borrow_mut().publish(req)
    }
    
    pub fn subscribe(&mut self, req: Subscribe) -> Promise<Topic, SubscribeError> {
        self.coord.borrow_mut().subscribe(req)
    }

    fn remove_submission(&self, id: QueryId, err: SubmissionError) {
        self.coord.borrow_mut().remove_submission(id, err)
    }
}
