use std::collections::btree_map::{BTreeMap, Values};

use model::*;

pub struct Catalog {
    executors: BTreeMap<ExecutorId, Executor>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog { executors: BTreeMap::new() }
    }

    pub fn add_executor(&mut self, executor: Executor) {
        self.executors.insert(executor.id, executor);
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        self.executors.remove(&id);
    }

    pub fn executors<'a>(&'a self) -> Executors<'a> {
        Executors { values: self.executors.values() }
    }
}

pub struct Executors<'a> {
    values: Values<'a, ExecutorId, Executor>,
}

impl<'a> Iterator for Executors<'a> {
    type Item = &'a Executor;

    fn next(&mut self) -> Option<Self::Item> {
        self.values.next()
    }
}
