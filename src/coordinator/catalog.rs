use std::collections::BTreeMap;

use model::*;

pub struct Catalog {
    executors: BTreeMap<ExecutorId, Executor>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            executors: BTreeMap::new(),
        }
    }
    
    fn add_executor(&mut self, executor: Executor) {
        self.executors.insert(executor.id, executor);
    }
    
    fn remove_executor(&mut self, id: ExecutorId) {
        self.executors.remove(&id);
    }
}
