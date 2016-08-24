use abomonation::Abomonation;

use executor::ExecutorType;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(pub u64);

impl From<u64> for QueryId {
    fn from(id: u64) -> QueryId {
        QueryId(id)
    }
}

#[derive(Clone, Debug)]
pub struct QueryConfig {
    pub fetch: String,
    pub binary: ExecutorType,
    pub num_executors: usize,
    pub num_workers: usize, // per executor
}

unsafe_abomonate!(QueryId);
unsafe_abomonate!(QueryConfig: fetch, num_executors, num_workers);
