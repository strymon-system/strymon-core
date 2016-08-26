use std::collections::BTreeMap;

use rand;

use query::{QueryConfig, QueryId};
use executor::{ExecutorId, ExecutorType};
use util::Generator;

use messaging::request::Complete;

use coordinator::executor::{ExecutorRef, Message as ExecutorMessage};
use coordinator::request::ExecutorReady;

pub type ExecutorTypeId = u8;

pub struct Executor {
    id: ExecutorId,
    ty: ExecutorType,
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
            tx: tx,
        };
    }

    pub fn select<'a>(&'a self,
                      ty: ExecutorType,
                      num_executors: usize)
                      -> Option<Vec<&'a Executor>> {
        if let Some(executors) = self.executors.get(&(ty as ExecutorTypeId)) {
            let mut rng = rand::thread_rng();
            Some(rand::sample(&mut rng, executors.values(), num_executors))
        } else {
            None
        }
    }
}

impl Executor {
    pub fn spawn(&self, id: QueryId, config: &QueryConfig) {
        self.tx.send(ExecutorMessage::Spawn(id, config.clone()))
    }
}
