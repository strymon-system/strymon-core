use std::collections::{BTreeMap, HashMap, VecDeque};
use std::collections::btree_map::Entry;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::mem;

use futures::{self, Future};
use futures::unsync::oneshot::{channel, Sender};
use tokio_core::reactor::Handle;

use rand;

use strymon_communication::rpc::{Outgoing, Response};

use model::*;
use executor::requests::*;

use coordinator::requests::*;
use coordinator::catalog::Catalog;

use super::util::Generator;

struct ExecutorState {
    tx: Outgoing,
    ports: VecDeque<u16>,
}

impl ExecutorState {
    fn new(tx: Outgoing, ports: (u16, u16)) -> Self {
        let ports = (ports.0..(ports.1 + 1)).collect();
        ExecutorState {
            tx: tx,
            ports: ports,
        }
    }

    fn has_ports(&self) -> bool {
        !self.ports.is_empty()
    }

    fn allocate_port(&mut self) -> u16 {
        self.ports.pop_front().expect("coordinator has no free ports")
    }

    fn free_port(&mut self, port: u16) {
        self.ports.push_back(port);
    }

    fn spawn(&self, req: &SpawnQuery) -> Response<SpawnQuery> {
        debug!("issue spawn request {:?}", req);
        self.tx.request(req)
    }
}

enum QueryState {
    Spawning {
        query: Query,
        submitter: Sender<Result<QueryId, SubmissionError>>,
        waiting: Vec<Sender<Result<QueryToken, WorkerGroupError>>>,
    },
    Running,
    Terminating,
}

struct WorkerGroup {
    state: QueryState,
    count: usize,
    ports: Vec<(ExecutorId, u16)>,
}

struct KeeperState {
    /// Used for load balancing.
    workers: Vec<(usize, (String, u16))>,
    next_worker: usize,
}

pub struct Coordinator {
    handle: Weak<RefCell<Coordinator>>,
    reactor: Handle,
    catalog: Catalog,

    queryid: Generator<QueryId>,
    executorid: Generator<ExecutorId>,
    keeperid: Generator<KeeperId>,

    executors: BTreeMap<ExecutorId, ExecutorState>,
    queries: BTreeMap<QueryId, WorkerGroup>,
    lookups: HashMap<String, Vec<Sender<Result<Topic, SubscribeError>>>>,
    keepers: BTreeMap<KeeperId, KeeperState>,
    keepers_directory: HashMap<String, KeeperId>,
}

impl Coordinator {
    pub fn new(catalog: Catalog, reactor: Handle) -> CoordinatorRef {
        let coord = Coordinator {
            handle: Weak::new(),
            catalog: catalog,
            queryid: Generator::new(),
            executorid: Generator::new(),
            keeperid: Generator::new(),
            executors: BTreeMap::new(),
            queries: BTreeMap::new(),
            lookups: HashMap::new(),
            keepers: BTreeMap::new(),
            keepers_directory: HashMap::new(),
            reactor: reactor,
        };

        // we use weak references to avoid cycles
        let coord = Rc::new(RefCell::new(coord));
        coord.borrow_mut().handle = Rc::downgrade(&coord);
        CoordinatorRef::from(coord)
    }

    fn handle(&self) -> Rc<RefCell<Coordinator>> {
        self.handle.upgrade().expect("`self` has been deallocated?!")
    }

    fn submission(&mut self, req: Submission) -> Box<Future<Item=QueryId, Error=SubmissionError>> {
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
                    let selected = rand::sample(&mut rng, executors, num_executors);
                    (selected, num_executors, num_workers)
                }
                Placement::Fixed(executor_ids, num_workers) => {
                    let num_executors = executor_ids.len();
                    let mut selected = vec![];

                    for executor in executors {
                        for &id in &executor_ids {
                            if executor.id == id {
                                selected.push(executor);
                            }
                        }
                    }

                    (selected, num_executors, num_workers)
                }
            };

            // step 2.3: check if we actually have enough executors
            if executors.len() != num_executors {
                return Box::new(futures::failed(SubmissionError::ExecutorsNotFound));
            }

            (executors, num_executors, num_workers)
        };

        // step 3: create the Timely configuration
        let ports: Vec<(ExecutorId, u16)> = executors.iter()
            .map(|executor| {
                let id = executor.id;
                let executor = executor_res.get_mut(&id).unwrap();
                (id, executor.allocate_port())
            })
            .collect();

        let hostlist: Vec<String> = executors.iter()
            .zip(ports.iter())
            .map(|(executor, &(_, port))| format!("{}:{}", executor.host, port))
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
                .map_err(move |err| {
                    let err = match err {
                        Ok(err) => SubmissionError::SpawnError(err),
                        Err(err) => {
                            error!("executor request failed: {}", err);
                            SubmissionError::ExecutorUnreachable
                        }
                    };
                    handle.borrow_mut().cancel_submission(queryid, err);
                });

            self.reactor.spawn(response);
        }

        // TODO(swicki) add a timeout that triggers SpawnFailed here
        debug!("add pending submission for {:?}", query.id);
        let (tx, rx) = channel();
        let state = QueryState::Spawning {
            query: query,
            submitter: tx,
            waiting: vec![],
        };

        let worker_group = WorkerGroup {
            state: state,
            count: executors.len(),
            ports: ports,
        };
        self.queries.insert(queryid, worker_group);

        Box::new(rx.then(|res| res.expect("submission canceled?!")))
    }

    fn cancel_submission(&mut self, id: QueryId, err: SubmissionError) {
        debug!("canceling pending submission for {:?}", id);
        if let Some(query) = self.queries.remove(&id) {
            if let QueryState::Spawning { submitter, waiting, .. } = query.state {
                let _ = submitter.send(Err(err));
                for worker in waiting {
                    let _ = worker.send(Err(WorkerGroupError::PeerFailed));
                }
            }

            for (id, port) in query.ports {
                self.executors.get_mut(&id).map(|e| e.free_port(port));
            }
        }
    }

    fn add_worker_group(&mut self, id: QueryId,_group: usize)
        -> Box<Future<Item=QueryToken, Error=WorkerGroupError>>
    {
        let query = self.queries.get_mut(&id);

        // step 1: check if we actually know about this query
        let query = if query.is_none() {
            return Box::new(futures::failed(WorkerGroupError::SpawningAborted));
        } else {
            query.unwrap()
        };

        // step 2: add current request to waiting workers
        let (connected, rx) = match query.state {
            QueryState::Spawning { ref mut waiting, .. } => {
                let (tx, rx) = channel();
                let rx = rx.then(|res| res.expect("spawning worker group failed"));
                waiting.push(tx);
                (waiting.len(), Box::new(rx))
            }
            QueryState::Running | QueryState::Terminating => {
                return Box::new(futures::failed(WorkerGroupError::InvalidWorkerGroup))
            }
        };

        // check if we need to wait for others to arrive
        debug!("{:?}: {} of {} are connected", id, connected, query.count);
        if connected < query.count {
            return rx;
        }

        // step 3: at this point, all worker groups have registered themselves
        let waiting = mem::replace(&mut query.state, QueryState::Running);
        let (submitter, waiting, query) = match waiting {
            QueryState::Spawning { submitter, waiting, query } => {
                (submitter, waiting, query)
            }
            _ => unreachable!(),
        };

        // step 4: add query to catalog
        self.catalog.add_query(query);

        // step 5: respond to everyone
        let token = QueryToken {
            id: id,
            auth: rand::random::<u64>(),
        };
        for worker in waiting {
            let _ = worker.send(Ok(token));
        }

        let _ = submitter.send(Ok(id));

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
            self.catalog.remove_query(id);
            let query = query.remove();

            for (id, port) in query.ports {
                self.executors.get_mut(&id).map(|e| e.free_port(port));
            }
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

    fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        let query = req.token.id;
        let result = self.catalog.publish(query, req.name, req.addr, req.schema);
        if let Ok(ref topic) = result {
            debug!("resolving lookup for topic: {:?}", &topic.name);
            if let Some(pending) = self.lookups.remove(&topic.name) {
                for tx in pending {
                    let _ = tx.send(Ok(topic.clone()));
                }
            }
        }
        result
    }

    fn unpublish(&mut self,
                 query_id: QueryId,
                 topic_id: TopicId)
                 -> Result<(), UnpublishError> {
        self.catalog.unpublish(query_id, topic_id)
    }

    fn subscribe(&mut self,
                 req: Subscribe)
                 -> Box<Future<Item = Topic, Error = SubscribeError>> {
        let query = req.token.id;

        if let Some(topic) = self.catalog.lookup(&req.name) {
            self.catalog.subscribe(query, topic.id);
            return Box::new(futures::finished(topic));
        } else if req.blocking {
            debug!("inserting blocking lookup for topic: {:?}", &req.name);
            let (lookup, result) = channel();
            self.lookups.entry(req.name).or_insert(Vec::new()).push(lookup);

            let handle = self.handle();            
            let result = result
                .then(|res| {
                    res.unwrap_or(Err(SubscribeError::TopicNotFound))
                })
                .and_then(move |topic: Topic| {
                    handle.borrow_mut().catalog.subscribe(query, topic.id);
                    Ok(topic)
                });

            return Box::new(result);
        } else {
            return Box::new(futures::failed(SubscribeError::TopicNotFound));
        }
    }

    fn unsubscribe(&mut self,
                   query_id: QueryId,
                   topic_id: TopicId)
                   -> Result<(), UnsubscribeError> {
        self.catalog.unsubscribe(query_id, topic_id)
    }

    fn lookup(&self, name: &str) -> Result<Topic, ()> {
        match self.catalog.lookup(name) {
            Some(topic) => Ok(topic),
            None => Err(()),
        }
    }

    fn add_keeper_worker(&mut self,
                         name: String,
                         worker_num: usize,
                         addr: (String, u16))
                         -> Result<(), AddKeeperWorkerError> {
        let keeper_id = match self.keepers_directory.get(&name).map(|x| x.clone())  {
            Some(id) => id,
            None => {
                let id = self.keeperid.generate();
                self.keepers_directory.insert(name.clone(), id.clone());
                self.catalog.add_keeper(Keeper {
                                            id: id.clone(),
                                            name: name.clone(),
                                            workers: Vec::new(),
                                        });
                id
            }
        };

        let keeper_state =
            self.keepers.entry(keeper_id.clone()).or_insert_with(|| {
                KeeperState {
                    workers: Vec::new(),
                    next_worker: 0,
                }
            });

        for &(ref num, _) in &keeper_state.workers {
            if *num == worker_num {
                return Err(AddKeeperWorkerError::WorkerAlreadyExists);
            }
        }

        keeper_state.workers.push((worker_num, addr.clone()));

        self.catalog.add_keeper_worker(&keeper_id, worker_num, addr)
            .expect("Invariant broken: keeper is present in coordinator but not in its catalog.");

        Ok(())
    }

    fn get_keeper_address(&mut self,
                          name: String)
                          -> Result<(String, u16), GetKeeperAddressError> {
        let keeper_id = match self.keepers_directory.get(&name) {
            Some(id) => id,
            None => return Err(GetKeeperAddressError::KeeperNotFound),
        };
        let keeper_state =
            self.keepers.get_mut(keeper_id).expect("Invariant broken: \
                    keeper_id present in keepers_directory but not in keepers.");
        let workers_len = keeper_state.workers.len();
        if workers_len == 0 {
            return Err(GetKeeperAddressError::KeeperHasNoWorkers);
        }
        let addr = keeper_state.workers[keeper_state.next_worker].1.clone();
        keeper_state.next_worker += 1;
        keeper_state.next_worker %= workers_len;
        Ok(addr)
    }

    fn remove_keeper_worker(&mut self,
                            name: String,
                            worker_num: usize)
                            -> Result<(), RemoveKeeperWorkerError> {
        let keeper_id = match self.keepers_directory.get(&name) {
            Some(id) => id,
            None => return Err(RemoveKeeperWorkerError::KeeperDoesntExist),
        }.clone();

        let old_workers_len;
        let workers_len;

        {
            let keeper_state = self.keepers.get_mut(&keeper_id)
            .expect("Invariant broken: keeper_id present in keepers_directory but not in keepers.");

            old_workers_len = keeper_state.workers.len();
            keeper_state.workers.retain(|ref x| x.0 != worker_num);
            workers_len = keeper_state.workers.len();
            if old_workers_len == workers_len {
                return Err(RemoveKeeperWorkerError::KeeperWorkerDoesntExist);
            }
            if workers_len != 0 {
                keeper_state.next_worker %= workers_len;
            }
        }

        if workers_len == 0 {
            self.keepers.remove(&keeper_id);
            self.keepers_directory.remove(&name);
        }

        let mut keeper = self.catalog.remove_keeper(&keeper_id)
            .expect("Invariant broken: keeper present in state but not in catalog");
        if workers_len > 0 {
            keeper.workers.retain(|ref x| x.0 != worker_num);
            self.catalog.add_keeper(keeper);
        }

        Ok(())
    }
}

struct State {
    query: Vec<QueryToken>,
    executor: Vec<ExecutorId>,
    publication: Vec<(QueryId, TopicId)>,
    subscription: Vec<(QueryId, TopicId)>,
}

impl State {
    fn empty() -> Self {
        State {
            query: Vec::new(),
            executor: Vec::new(),
            publication: Vec::new(),
            subscription: Vec::new(),
        }
    }

    fn authenticate(&self, auth: &QueryToken) -> bool {
        self.query.iter().any(|token| auth == token)
    }
}

pub struct CoordinatorRef {
    coord: Rc<RefCell<Coordinator>>,
    state: Rc<RefCell<State>>,
}

impl CoordinatorRef {
    fn from(coord: Rc<RefCell<Coordinator>>) -> Self {
        CoordinatorRef {
            coord: coord,
            state: Rc::new(RefCell::new(State::empty())),
        }
    }

    pub fn submission(&self,
                      req: Submission)
                      -> Box<Future<Item = QueryId, Error = SubmissionError>> {
        self.coord.borrow_mut().submission(req)
    }

    pub fn add_executor(&mut self, req: AddExecutor, tx: Outgoing) -> ExecutorId {
        let id = self.coord.borrow_mut().add_executor(req, tx);
        self.state.borrow_mut().executor.push(id);
        id
    }

    pub fn add_worker_group(&mut self, id: QueryId, group: usize)
         -> Box<Future<Item = QueryToken, Error = WorkerGroupError>>
    {
        let state = self.state.clone();
        let future = self.coord
            .borrow_mut()
            .add_worker_group(id, group)
            .and_then(move |token| {
                state.borrow_mut().query.push(token);
                Ok(token)
            });

        Box::new(future)
    }

    pub fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        let query = req.token;
        if !self.state.borrow().authenticate(&query) {
            return Err(PublishError::AuthenticationFailure);
        }

        self.coord
            .borrow_mut()
            .publish(req)
            .and_then(|topic| {
                let topic_id = topic.id;
                let query_id = query.id;
                self.state.borrow_mut().publication.push((query_id, topic_id));
                Ok(topic)
            })
    }

    pub fn unpublish(&mut self,
                     query: QueryToken,
                     topic_id: TopicId)
                     -> Result<(), UnpublishError> {
        if !self.state.borrow().authenticate(&query) {
            return Err(UnpublishError::AuthenticationFailure);
        }

        let query_id = query.id;
        self.coord
            .borrow_mut()
            .unpublish(query_id, topic_id)
            .and_then(|_| {
                let to_remove = (query_id, topic_id);
                self.state.borrow_mut().publication.retain(|&p| p != to_remove);
                Ok(())
            })
    }

    pub fn subscribe(&mut self,
                     req: Subscribe)
                     -> Box<Future<Item = Topic, Error = SubscribeError>> {
        let query = req.token;
        if !self.state.borrow().authenticate(&query) {
            return Box::new(futures::failed(SubscribeError::AuthenticationFailure));
        }

        let state = self.state.clone();
        let future = self.coord
            .borrow_mut()
            .subscribe(req)
            .and_then(move |topic| {
                let topic_id = topic.id;
                let query_id = query.id;
                state.borrow_mut().subscription.push((query_id, topic_id));
                Ok(topic)
            });
        Box::new(future)
    }

    pub fn unsubscribe(&mut self,
                       query: QueryToken,
                       topic_id: TopicId)
                       -> Result<(), UnsubscribeError> {
        if !self.state.borrow().authenticate(&query) {
            return Err(UnsubscribeError::AuthenticationFailure);
        }

        let query_id = query.id;
        self.coord
            .borrow_mut()
            .unsubscribe(query_id, topic_id)
            .and_then(|_| {
                let to_remove = (query_id, topic_id);
                let mut state = self.state.borrow_mut();
                if let Some(pos) = state.subscription
                    .iter()
                    .position(|&p| p == to_remove) {
                    state.subscription.swap_remove(pos);
                } else {
                    warn!("cannot find state to remove for subscription?!")
                }
                Ok(())
            })
    }

    pub fn lookup(&self, name: &str) -> Result<Topic, ()> {
        self.coord.borrow().lookup(name)
    }

    pub fn add_keeper_worker(&mut self,
                             name: String,
                             worker_num: usize,
                             addr: (String, u16))
                             -> Result<(), AddKeeperWorkerError> {
        self.coord.borrow_mut().add_keeper_worker(name, worker_num, addr)
    }

    pub fn get_keeper_address(&mut self,
                              name: String)
                              -> Result<(String, u16), GetKeeperAddressError> {
        self.coord.borrow_mut().get_keeper_address(name)
    }

    pub fn remove_keeper_worker(&mut self,
                                name: String,
                                worker_num: usize)
                                -> Result<(), RemoveKeeperWorkerError> {
        self.coord.borrow_mut().remove_keeper_worker(name, worker_num)
    }
}

// will only clone the coordinator ref, not the tracked calls
impl Clone for CoordinatorRef {
    fn clone(&self) -> Self {
        CoordinatorRef::from(self.coord.clone())
    }
}

impl Drop for CoordinatorRef {
    fn drop(&mut self) {
        // here we clean up any state that we might own
        let mut state = self.state.borrow_mut();
        let mut coord = self.coord.borrow_mut();

        for (query, topic) in state.subscription.drain(..) {
            if let Err(err) = coord.unsubscribe(query, topic) {
                warn!("error while cleaning subscriptions: {:?}", err);
            }
        }

        for (query, topic) in state.publication.drain(..) {
            if let Err(err) = coord.unpublish(query, topic) {
                warn!("error while cleaning publications: {:?}", err);
            }
        }

        for query in state.query.drain(..) {
            coord.remove_worker_group(query.id);
        }

        for executor in state.executor.drain(..) {
            coord.remove_executor(executor);
        }
    }
}
