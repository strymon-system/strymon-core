// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The coordinator request handler.
//!
//! The [`Coordinator`](struct.Coordinator.html) type implements most of the coordinator
//! logic. The [`Coordinator`](struct.Coordinator.html) stores some additional state not
//! exposed in the catalog, such as connection handles or partially spawned jobs. Clients
//! submit their requests through a [`CoordinatorRef`](struct.Coordinator.html), which tracks
//! client-associated state.

use std::io;
use std::mem;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::collections::btree_map::Entry;
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{self, Future};
use futures::stream::{self, Stream};
use futures::unsync::oneshot::{channel, Sender};
use futures_timer::ext::FutureExt;
use tokio_core::reactor::Handle;

use rand;

use strymon_communication::rpc::{Outgoing, Response, RequestBuf};

use strymon_model::*;
use strymon_rpc::executor::*;

use strymon_rpc::coordinator::*;
use strymon_rpc::coordinator::catalog::CatalogRPC;

use catalog::Catalog;

use super::util::Generator;

const JOB_SUBMISSION_TIMEOUT_SECS: u64 = 30;

/// The connection and available `timely_communication` ports of an registered executor.
struct ExecutorResources {
    tx: Outgoing,
    ports: VecDeque<u16>,
}

impl ExecutorResources {
    /// Creates a new instance
    fn new(tx: Outgoing, ports: (u16, u16)) -> Self {
        let ports = (ports.0..(ports.1 + 1)).collect();
        ExecutorResources {
            tx: tx,
            ports: ports,
        }
    }

    /// Returns `true` if this executor still has free ports
    fn has_ports(&self) -> bool {
        !self.ports.is_empty()
    }

    /// Allocates a port to be assigned to a `timely_communcation` worker.
    fn allocate_port(&mut self) -> u16 {
        self.ports.pop_front().expect("coordinator has no free ports")
    }

    /// Frees a previously allocated port.
    fn free_port(&mut self, port: u16) {
        self.ports.push_back(port);
    }

    /// Sends an asynchronous job spawn request to this executor.
    fn spawn(&self, req: &SpawnJob) -> Response<ExecutorRPC, SpawnJob> {
        debug!("issue spawn request {:?}", req);
        self.tx.request(req)
    }

    /// Sends an asynchronous job termination request to this executor.
    fn terminate(&self, job_id: JobId) -> Response<ExecutorRPC, TerminateJob> {
        self.tx.request(&TerminateJob { job: job_id })
    }
}

/// Represents the states a job can be in.
enum JobState {
    /// The job has been submitted to the system, but not all worker groups have yet registered
    /// themselves at the coordinator.
    Spawning {
        /// Meta-data about the job being spawned, stored in the catalog once it is running.
        job: Job,
        /// A handle to the submitter of this job which we use to send back a response once the
        /// submitted job is running or if spawning has failed.
        submitter: Sender<Result<JobId, SubmissionError>>,
        /// A handle to each worker group of this job, which we respond to once all groups have
        /// registered themselves.
        waiting: Vec<Sender<Result<JobToken, WorkerGroupError>>>,
    },
    /// A normally running job.
    Running,
    /// This job is currently being terminated.
    Terminating,
}

/// The state associated with a job which is not exposed in the catalog.
struct JobResources {
    /// The phase of the job.
    state: JobState,
    /// Number of worker groups this job has
    count: usize,
    /// Allocated `timely_communication` ports for this job. To be returned to the port pool of
    /// the corresponding executor once this job terminates.
    ports: Vec<(ExecutorId, u16)>,
    /// The list of executors currently running this job.
    executors: Vec<ExecutorId>,
}

/// The coordinator instance, contains the current system state and handles incoming requests.
pub struct Coordinator {
    /// A weak reference to `Self`, allowing us to hand out new `Rc` instances
    handle: Weak<RefCell<Coordinator>>,
    /// Handle to the `tokio-core` reactore for spawning background tasks.
    reactor: Handle,
    /// The catalog of this coordinator instance.
    catalog: Catalog,

    /// Generates the next job id.
    jobid: Generator<JobId>,
    /// Generates the next executor id.
    executorid: Generator<ExecutorId>,

    /// A list of available executors and their available resources.
    executors: BTreeMap<ExecutorId, ExecutorResources>,
    /// A list of currently spawning, running or terminating jobs and their resources.
    jobs: BTreeMap<JobId, JobResources>,
    /// The list of currently pending (blocking) topic lookups with their response handle.
    lookups: HashMap<String, Vec<Sender<Result<Topic, SubscribeError>>>>,
}

impl Coordinator {
    /// Creates a new coordinator instance, returning a cloneable client handle for it.
    pub fn new(catalog: Catalog, reactor: Handle) -> CoordinatorRef {
        let coord = Coordinator {
            handle: Weak::new(),
            catalog: catalog,
            jobid: Generator::new(),
            executorid: Generator::new(),
            executors: BTreeMap::new(),
            jobs: BTreeMap::new(),
            lookups: HashMap::new(),
            reactor: reactor,
        };

        // we use weak references to avoid cycles
        let coord = Rc::new(RefCell::new(coord));
        coord.borrow_mut().handle = Rc::downgrade(&coord);
        CoordinatorRef::from(coord)
    }

    /// Creates a new reference counted handle to `Self`
    fn handle(&self) -> Rc<RefCell<Coordinator>> {
        self.handle.upgrade().expect("`self` has been deallocated?!")
    }

    /// Handles a job submission request.
    fn submission(&mut self, req: Submission) -> Box<Future<Item=JobId, Error=SubmissionError>> {
        // workaround: prevent closures borrowing `self`
        let handle = self.handle();
        let executor_res = &mut self.executors;

        // step 1: generate job id
        let jobid = self.jobid.generate();

        // step 2: Select suitable executors
        let (executors, num_executors, num_workers) = {
            // step 2.1: filter out executors with the wrong format,
            // and the ones with no more free network ports
            let format = &req.job.format;
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
        let ports: Vec<(ExecutorId, u16)> = if num_executors > 1 {
            // allocate a port per executor
            executors.iter()
                .map(|executor| {
                    let id = executor.id;
                    let executor = executor_res.get_mut(&id).unwrap();
                    (id, executor.allocate_port())
                })
                .collect()
        } else {
            // no need to allocate any ports for non-cluster jobs
            Vec::new()
        };

        // assemble the hostlist passed down to `timely_communication`
        let hostlist: Vec<String> = executors.iter()
            .zip(ports.iter())
            .map(|(executor, &(_, port))| format!("{}:{}", executor.host, port))
            .collect();

        // submission time (at the coordinator) of the current job
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let executor_ids: Vec<_> = executors.iter().map(|e| e.id).collect();
        let job = Job {
            id: jobid,
            name: req.name,
            program: req.job,
            workers: num_executors * num_workers,
            executors: executor_ids.clone(),
            start_time: start_time,
        };
        // the request to be sent out to all involved executors
        let spawnjob = SpawnJob {
            job: job.clone(),
            hostlist: hostlist,
        };

        // step 4: send requests to the selected coordinators
        debug!("selected executors for {:?}:{:?}", job.id, executors);
        for executor in &executors {
            let handle = handle.clone();
            let executor = &executor_res[&executor.id];
            let response = executor.spawn(&spawnjob)
                .map_err(move |err| {
                    let err = match err {
                        Ok(err) => SubmissionError::SpawnError(err),
                        Err(err) => {
                            error!("executor request failed: {}", err);
                            SubmissionError::ExecutorUnreachable
                        }
                    };
                    handle.borrow_mut().cancel_submission(jobid, err);
                });

            self.reactor.spawn(response);
        }

        debug!("add pending submission for {:?}", job.id);
        let (tx, rx) = channel();
        let state = JobState::Spawning {
            job: job,
            submitter: tx,
            waiting: vec![],
        };

        let job_resources = JobResources {
            state: state,
            count: executors.len(),
            ports: ports,
            executors: executor_ids,
        };
        self.jobs.insert(jobid, job_resources);

        let response = rx
            .then(|res| res.unwrap_or(Err(SubmissionError::Other)))
            .timeout(Duration::from_secs(JOB_SUBMISSION_TIMEOUT_SECS))
            .map_err(move |err| {
                handle.borrow_mut().cancel_submission(jobid, err.clone());
                err
            });

        Box::new(response)
    }

    /// Cancels a pending submission, informing already available workers to shut down.
    fn cancel_submission(&mut self, id: JobId, err: SubmissionError) {
        debug!("canceling pending submission for {:?}", id);
        if let Some(job) = self.jobs.remove(&id) {
            if let JobState::Spawning { submitter, waiting, .. } = job.state {
                let _ = submitter.send(Err(err));
                for worker in waiting {
                    let _ = worker.send(Err(WorkerGroupError::PeerFailed));
                }
            }

            for (id, port) in job.ports {
                self.executors.get_mut(&id).map(|e| e.free_port(port));
            }
        }
    }

    /// A new worker group (i.e. a process hosting a bunch of worker thread) has announced itself
    /// at the coordinator. This adds the newly registered group to the list of ready worker groups
    /// and returns a future which is resolved once all groups have registered themselves.
    ///
    /// If the newly added group is the last group to arrive, this method complets the submission
    /// by adding the job to the catalog, responding to all worker groups and the submitter with
    /// the job id of the newly spawned job.
    fn add_worker_group(&mut self, id: JobId,_group: usize)
        -> Box<Future<Item=JobToken, Error=WorkerGroupError>>
    {
        let job = self.jobs.get_mut(&id);

        // step 1: check if we actually know about this job
        let job = if job.is_none() {
            return Box::new(futures::failed(WorkerGroupError::SpawningAborted));
        } else {
            job.unwrap()
        };

        // step 2: add current request to waiting workers
        let (connected, rx) = match job.state {
            JobState::Spawning { ref mut waiting, .. } => {
                let (tx, rx) = channel();
                let rx = rx.then(|res| res.expect("spawning worker group failed"));
                waiting.push(tx);
                (waiting.len(), Box::new(rx))
            }
            JobState::Running | JobState::Terminating => {
                return Box::new(futures::failed(WorkerGroupError::InvalidWorkerGroup))
            }
        };

        // check if we need to wait for others to arrive
        debug!("{:?}: {} of {} are connected", id, connected, job.count);
        if connected < job.count {
            return rx;
        }

        // step 3: at this point, all worker groups have registered themselves
        let waiting = mem::replace(&mut job.state, JobState::Running);
        let (submitter, waiting, job) = match waiting {
            JobState::Spawning { submitter, waiting, job } => {
                (submitter, waiting, job)
            }
            _ => unreachable!(),
        };

        // step 4: add job to catalog
        self.catalog.add_job(job);

        // step 5: respond to everyone
        let token = JobToken {
            id: id,
            auth: rand::random::<u64>(),
        };
        for worker in waiting {
            let _ = worker.send(Ok(token));
        }

        let _ = submitter.send(Ok(id));

        rx
    }

    /// Removes a worker group from the system. If it is the first worker group of a job to
    /// terminate, we put the job into the `Terminating` state. If it is the last group of a job,
    /// we remove the job completely from the system.
    fn remove_worker_group(&mut self, id: JobId) {
        let mut job = match self.jobs.entry(id) {
            Entry::Occupied(job) => job,
            Entry::Vacant(_) => {
                warn!("request to remove inexisting worker group");
                return;
            }
        };

        // decrease counter, set to terminating
        let count = {
            let job = job.get_mut();
            job.state = JobState::Terminating;
            job.count -= 1;

            job.count
        };

        // and we're done
        if count == 0 {
            self.catalog.remove_job(id);
            let job = job.remove();

            for (id, port) in job.ports {
                self.executors.get_mut(&id).map(|e| e.free_port(port));
            }
        }
    }

    /// Handles a job termination request from a user. This sends a termination request to each
    /// involved executor. The returned future resolves once either all executors have successfully
    /// terminated the request, or once of the executors has responded with an error.
    fn termination(&mut self, req: Termination) -> Box<Future<Item=(), Error=TerminationError>> {
        let job_id = req.job;
        // extract the executor ids from the worker groups
        let executor_ids = match self.jobs.get(&job_id) {
            Some(group) => group.executors.iter(),
            None => return Box::new(futures::failed(TerminationError::NotFound)),
        };

        let ref executors = self.executors;
        let responses = executor_ids
            .flat_map(|id| {
                // send termination request to each executor
                if let Some(executor) = executors.get(id) {
                    let response = executor.terminate(job_id).map_err(|res| {
                        match res {
                            // propagate the error returned by the executor
                            Ok(err) => TerminationError::TerminateError(err),
                            // the request never reached the executor
                            Err(err) => {
                                error!("executor request failed: {}", err);
                                TerminationError::ExecutorUnreachable
                            }
                        }
                    });

                    Some(response)
                } else {
                    // if the executor where the job is running has vanished,
                    // we still send the termination request to the remaining
                    // peers, in case the user is trying to shut things down.
                    error!("Unable to find executor {:?} for {:?}", id, job_id);
                    None
                }
            });

        // only the first error is propagated back to the caller. otherwise we wait
        // for all executors to respond before we treat the request as completed
        let terminated = stream::futures_unordered(responses).for_each(|()| {
            Ok(())
        });

        Box::new(terminated)
    }

    /// Adds a new executor to the system, returning its newly assigned executor id.
    fn add_executor(&mut self, req: AddExecutor, tx: Outgoing) -> ExecutorId {
        let id = self.executorid.generate();
        debug!("adding executor {:?} to pool", id);

        let state = ExecutorResources::new(tx, req.ports);
        let executor = Executor {
            id: id,
            host: req.host,
            format: req.format,
        };

        self.executors.insert(id, state);
        self.catalog.add_executor(executor);
        id
    }

    /// Removes an executor. It cannot be reached anymore for submission or termiation requests
    /// after this.
    fn remove_executor(&mut self, id: ExecutorId) {
        debug!("removing executor {:?} from pool", id);
        self.executors.remove(&id);
        self.catalog.remove_executor(id);
    }

    /// Creates a new publication in the catalog, returning the created topic.
    fn publish(&mut self, req: Publish) -> Result<Topic, PublishError> {
        let job = req.token.id;
        let result = self.catalog.publish(job, req.name, req.addr, req.schema);
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

    /// Removes a publication from the catalog.
    fn unpublish(&mut self,
                 job_id: JobId,
                 topic_id: TopicId)
                 -> Result<(), UnpublishError> {
        self.catalog.unpublish(job_id, topic_id)
    }

    /// Creates a new subscription for a topic. The returned future resolves either immediately
    /// for non-blocking lookups, or once a matching topic has been created by a later publication.
    fn subscribe(&mut self,
                 req: Subscribe)
                 -> Box<Future<Item = Topic, Error = SubscribeError>> {
        let job = req.token.id;

        if let Some(topic) = self.catalog.lookup(&req.name) {
            self.catalog.subscribe(job, topic.id);
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
                    handle.borrow_mut().catalog.subscribe(job, topic.id);
                    Ok(topic)
                });

            return Box::new(result);
        } else {
            return Box::new(futures::failed(SubscribeError::TopicNotFound));
        }
    }

    /// Removes a subscription from the catalog.
    fn unsubscribe(&mut self,
                   job_id: JobId,
                   topic_id: TopicId)
                   -> Result<(), UnsubscribeError> {
        self.catalog.unsubscribe(job_id, topic_id)
    }

    /// Performs a non-blocking, non-subscribing topic lookup.
    fn lookup(&self, name: &str) -> Result<Topic, ()> {
        match self.catalog.lookup(name) {
            Some(topic) => Ok(topic),
            None => Err(()),
        }
    }
}

/// State associated with a specific client.
struct ClientState {
    /// If this client is one (or more) job worker groups, keep track of their job tokens.
    job: Vec<JobToken>,
    /// If this client is one (or more) executors, track their ids for deregistration.
    executor: Vec<ExecutorId>,
    /// The list of publications issued by this client.
    publication: Vec<(JobId, TopicId)>,
    /// The list of subscriptions issued by this client.
    subscription: Vec<(JobId, TopicId)>,
}

impl ClientState {
    /// Creates a new default instance.
    fn empty() -> Self {
        ClientState {
            job: Vec::new(),
            executor: Vec::new(),
            publication: Vec::new(),
            subscription: Vec::new(),
        }
    }

    /// Checks if this client is indeed the job it is claiming to be.
    fn authenticate(&self, auth: &JobToken) -> bool {
        self.job.iter().any(|token| auth == token)
    }
}

impl Default for ClientState {
    fn default() -> ClientState {
            ClientState::empty()
    }
}

/// A cloneable reference to the coordinator instance, tracking any state created by this handle.
///
/// The initial handle is obtained by creating a new coordinator instance with
/// [`Coordinator::new`](../struct.Coordinator.html#method.new).
///
/// Note that cloning this handle will not clone any tracked state.
pub struct CoordinatorRef {
    coord: Rc<RefCell<Coordinator>>,
    state: Rc<RefCell<ClientState>>,
}

impl CoordinatorRef {
    /// Creates a new empty instance
    fn from(coord: Rc<RefCell<Coordinator>>) -> Self {
        CoordinatorRef {
            coord: coord,
            state: Default::default(),
        }
    }

    /// Handles a new job submission request.
    ///
    /// The returned future resolves with the allocated job identifier once all processes have
    /// registered themselves at the coordinator (or when an error occurs).
    pub fn submission(&self,
                      req: Submission)
                      -> Box<Future<Item = JobId, Error = SubmissionError>> {
        trace!("incoming {:?}", req);
        self.coord.borrow_mut().submission(req)
    }

    /// Handles a new job termination request.
    ///
    /// The returned future resolves once all executors have acknowledged the request, or the
    /// first executor has reported an error.
    pub fn termination(&self, req: Termination)
                      -> Box<Future<Item = (), Error = TerminationError>> {
        self.coord.borrow_mut().termination(req)
    }

    /// Registers a new executor instance. Returns the newly assigned identifier for this executor.
    pub fn add_executor(&self, req: AddExecutor, tx: Outgoing) -> ExecutorId {
        trace!("incoming {:?}", req);
        let id = self.coord.borrow_mut().add_executor(req, tx);
        self.state.borrow_mut().executor.push(id);
        id
    }

    /// Marks job worker group (e.g. a process hosting some worker threads) as ready.
    ///
    /// The returned future resolves with the job identifier for the newly spawned job once all
    /// other worker groups have registered themselves, or if an error occured.
    pub fn add_worker_group(&self, id: JobId, group: usize)
         -> Box<Future<Item = JobToken, Error = WorkerGroupError>>
    {
        trace!("incoming AddWorkerGroup {{ id: {:?} }}", id);
        let state = self.state.clone();
        let future = self.coord
            .borrow_mut()
            .add_worker_group(id, group)
            .and_then(move |token| {
                state.borrow_mut().job.push(token);
                Ok(token)
            });

        Box::new(future)
    }

    /// Creates a new publication in the catalog. Returns the created topic.
    pub fn publish(&self, req: Publish) -> Result<Topic, PublishError> {
        trace!("incoming {:?}", req);
        let job = req.token;
        if !self.state.borrow().authenticate(&job) {
            return Err(PublishError::AuthenticationFailure);
        }

        self.coord
            .borrow_mut()
            .publish(req)
            .and_then(|topic| {
                let topic_id = topic.id;
                let job_id = job.id;
                self.state.borrow_mut().publication.push((job_id, topic_id));
                Ok(topic)
            })
    }

    /// Removes a publication from the catalog.
    pub fn unpublish(&self,
                     job: JobToken,
                     topic_id: TopicId)
                     -> Result<(), UnpublishError> {
        trace!("incoming Unpublish {{ topic_id: {:?} }}", topic_id);
        if !self.state.borrow().authenticate(&job) {
            return Err(UnpublishError::AuthenticationFailure);
        }

        let job_id = job.id;
        self.coord
            .borrow_mut()
            .unpublish(job_id, topic_id)
            .and_then(|_| {
                let to_remove = (job_id, topic_id);
                self.state.borrow_mut().publication.retain(|&p| p != to_remove);
                Ok(())
            })
    }

    /// Handles a new subscription request.
    ///
    /// The returned future resolves either immediately for non-blocking lookups, or once a
    /// matching topic is created by a later publication.
    pub fn subscribe(&self,
                     req: Subscribe)
                     -> Box<Future<Item = Topic, Error = SubscribeError>> {
        trace!("incoming {:?}", req);
        let job = req.token;
        if !self.state.borrow().authenticate(&job) {
            return Box::new(futures::failed(SubscribeError::AuthenticationFailure));
        }

        let state = self.state.clone();
        let future = self.coord
            .borrow_mut()
            .subscribe(req)
            .and_then(move |topic| {
                let topic_id = topic.id;
                let job_id = job.id;
                state.borrow_mut().subscription.push((job_id, topic_id));
                Ok(topic)
            });
        Box::new(future)
    }

    /// Removes a subscription from the catalog.
    pub fn unsubscribe(&self,
                       job: JobToken,
                       topic_id: TopicId)
                       -> Result<(), UnsubscribeError> {
        trace!("incoming Unsubscribe {{ topic_id: {:?} }}", topic_id);
        if !self.state.borrow().authenticate(&job) {
            return Err(UnsubscribeError::AuthenticationFailure);
        }

        let job_id = job.id;
        self.coord
            .borrow_mut()
            .unsubscribe(job_id, topic_id)
            .and_then(|_| {
                let to_remove = (job_id, topic_id);
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

    /// Performs a non-blocking, non-subscribing topic lookup.
    pub fn lookup(&self, name: &str) -> Result<Topic, ()> {
        trace!("incoming Lookup {{ name: {:?} }}", name);
        self.coord.borrow().lookup(name)
    }

    /// Forwards an encoded query to the catalog.
    ///
    /// The catalog which will decode and execute the query and respond to it immediately. Might
    /// fail if the request could not be decoded.
    pub fn catalog_request(&self, req: RequestBuf<CatalogRPC>) -> io::Result<()> {
        self.coord.borrow().catalog.request(req)
    }
}

/// Performs a (shallow) clone to obtain a new handle to the coordinator.
///
/// This will not clone any state tracked by the `self`.
impl Clone for CoordinatorRef {
    fn clone(&self) -> Self {
        CoordinatorRef::from(self.coord.clone())
    }
}

/// Removes any state associated with the client owning this handle.
impl Drop for CoordinatorRef {
    fn drop(&mut self) {
        // here we clean up any state that we might own
        let mut state = self.state.borrow_mut();
        let mut coord = self.coord.borrow_mut();

        for (job, topic) in state.subscription.drain(..) {
            if let Err(err) = coord.unsubscribe(job, topic) {
                warn!("error while cleaning subscriptions: {:?}", err);
            }
        }

        for (job, topic) in state.publication.drain(..) {
            if let Err(err) = coord.unpublish(job, topic) {
                warn!("error while cleaning publications: {:?}", err);
            }
        }

        for job in state.job.drain(..) {
            coord.remove_worker_group(job.id);
        }

        for executor in state.executor.drain(..) {
            coord.remove_executor(executor);
        }
    }
}
