use std::collections::BTreeMap;
use std::ops::Range;

use rand;

use query::{QueryId, QueryParams};
use executor::{ExecutorId, ExecutorType};
use executor::request::{Spawn, SpawnError};
use util::Generator;

use messaging::request::{self, AsyncResult, Complete};

use coordinator::executor::{ExecutorRef, Message as ExecutorMessage};
use coordinator::request::ExecutorReady;

pub type ExecutorTypeId = u8;

pub struct Executor {
    id: ExecutorId,
    ty: ExecutorType,
    host: String,
    ports: Range<u16>,
    tx: ExecutorRef,
}

pub struct Executors {
    executor_id: Generator<ExecutorId>,
    executors: BTreeMap<ExecutorTypeId, BTreeMap<ExecutorId, Executor>>,
}

impl Executors {
    pub fn new() -> Self {
        Executors {
            executor_id: Generator::new(),
            executors: BTreeMap::new(),
        }
    }

    pub fn executor_ready(&mut self,
                          req: ExecutorReady,
                          tx: ExecutorRef,
                          promise: Complete<ExecutorReady>) {
        let id = self.executor_id.generate();
        let executor = Executor {
            id: id,
            ty: req.ty,
            host: req.host,
            ports: req.ports,
            tx: tx,
        };

        self.executors
            .entry(executor.ty as ExecutorTypeId)
            .or_insert(BTreeMap::new())
            .insert(id, executor);

        debug!("successfully added new executor: {:?}", id);
        promise.success(id);
    }

    pub fn select<'a>(&'a mut self,
                      ty: ExecutorType,
                      num_executors: usize)
                      -> Option<Vec<&'a mut Executor>> {
        if let Some(executors) = self.executors.get_mut(&(ty as ExecutorTypeId)) {
            let mut rng = rand::thread_rng();
            Some(rand::sample(&mut rng, executors.values_mut(), num_executors))
        } else {
            None
        }
    }
}

impl Executor {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn allocate_port(&mut self) -> u16 {
        // TODO need a mechanism to free ports
        self.ports.next().expect("run out of tcp ports to allocate!")
    }

    pub fn spawn(&self,
                 fetch: &str,
                 query: &QueryParams,
                 procindex: usize)
                 -> AsyncResult<(), SpawnError> {
        debug!("spawn request for {:?} on {:?}", query.id, self.id);

        let (tx, rx) = request::promise::<Spawn>();
        let spawn = Spawn {
            fetch: fetch.to_string(),
            query: query.clone(),
            procindex: procindex,
        };

        self.tx.send(ExecutorMessage::Spawn(spawn, tx));
        rx
    }
}
