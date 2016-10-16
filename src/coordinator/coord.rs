use std::collections::{VecDeque, BTreeMap};
use std::io::Error as IoError;

use futures::Future;
use futures::stream::Stream;
use rand;

use async::{self, TaskFuture};
use async::queue::{channel, Sender};

use network::reqresp::{Responder, Outgoing};

use model::*;

use executor::requests::*;

use super::requests::*;
use super::catalog::Catalog;
use super::util::Generator;

pub enum Event {
    Submission(Submission, Responder<Submission>),
    SpawnFailed(QueryId, Result<SpawnError, IoError>),
}

#[derive(Clone)]
pub struct CoordinatorRef {
    coord: Sender<Event, ()>,
}

impl CoordinatorRef {
    pub fn send(&self, event: Event) {
        if let Err(_) = self.coord.send(Ok(event)) {
            panic!("coordinator event queue unexpectedly deallocated")
        }
    }
}

pub struct Coordinator {
    handle: CoordinatorRef,
    catalog: Catalog,

    queryid: Generator<QueryId>,
    executorid: Generator<ExecutorId>,
    
    executors: BTreeMap<ExecutorId, ExecutorState>,
    submissions: BTreeMap<QueryId, Responder<Submission>>,
}

struct ExecutorState {
    reqs: Outgoing,
    ports: VecDeque<u16>,
}

impl Coordinator {
    pub fn new() -> (TaskFuture, CoordinatorRef) {
        let (tx, rx) = channel();

        // handle for connections to use
        let handle = CoordinatorRef { coord: tx };

        let mut coord = Coordinator {
            handle: handle.clone(),
            catalog: Catalog::new(),
            queryid: Generator::new(),
            executorid: Generator::new(),
            executors: BTreeMap::new(),
            submissions: BTreeMap::new(),
        };

        let task = Box::new(rx.for_each(move |event| {
            Ok(coord.dispatch(event))
        }));

        (task, handle)
    }

    fn submission(&mut self, req: Submission, resp: Responder<Submission>) {
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
                .filter(|e| !executor_res[&e.id].ports.is_empty());

            // step 2.2: select executors according to user placment
            let (executors, num_executors, num_workers) = match req.placement {
                Placement::Random(num_executors, num_workers) => {
                    let mut rng = rand::thread_rng();
                    let executors = rand::sample(&mut rng, executors, num_executors);
                    (executors, num_executors, num_workers)
                }
                Placement::Fixed(executor_ids, num_workers) => {
                    let num_executors = executor_ids.len();
                    let executors = executors
                        .filter(|e| executor_ids.contains(&e.id))
                        .collect();
                    (executors, num_executors, num_workers)
                }
            };

            // step 2.3: check if we actually have enough executors
            if executors.len() != num_executors {
                return resp.respond(Err(SubmissionError::ExecutorsNotFound))
            }
            
            (executors, num_executors, num_workers)
        };

        // step 3: create the Timely configuration
        let hostlist: Vec<String> = executors.iter().map(|executor| {
            let resources = executor_res.get_mut(&executor.id).unwrap();
            let port = resources.ports.pop_front().unwrap();
            format!("{}:{}", executor.host, port)
        }).collect();

        let executor_ids = executors.iter().map(|e| e.id).collect();
        let spawnquery = SpawnQuery {
            query: Query {
                id: queryid,
                name: req.name,
                program: req.query,
                workers: num_executors * num_workers,
                executors: executor_ids,
            },
            hostlist: hostlist,
        };

        // step 4: send requests to the selected coordinators
        let handle = &self.handle;   
        let spawn = executors.iter().map(|executor| {
            let handle = handle.clone();
            let response = executor_res[&executor.id].reqs
                .request(&spawnquery)
                .map_err(move |err| {
                    handle.send(Event::SpawnFailed(queryid, err));
                });
            
            async::spawn(response);
        });

        // TODO(swicki) add a timeout that triggers SpawnFailed here
        self.submissions.insert(queryid, resp);
    }
       
    fn dispatch(&mut self, event: Event) {
        match event {
            Event::Submission(req, resp) => {
                self.submission(req, resp)
            }
            Event::SpawnFailed(query, err) => {
                if let Some(resp) = self.submissions.remove(&query) {
                    // TODO free ports?
                    resp.respond(Err(SubmissionError::SpawnError))
                }
            }
        }
    }
}
