pub use std::any::TypeId;

mod imp;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicType {
    pub id: TypeId,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topic {
    pub id: TopicId,
    pub name: String,
    pub addr: (String, u16),
    pub kind: TopicType,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    pub id: QueryId,
    pub name: Option<String>,
    pub program: QueryProgram,
    pub workers: usize, // total
    pub executors: Vec<ExecutorId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryProgram {
    pub format: ExecutionFormat,
    pub source: String, // TODO(swicki) use Url crate for this?
    pub args: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionFormat {
    NativeExecutable,
    Other,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutorId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Executor {
    pub id: ExecutorId,
    pub host: String,
    pub format: ExecutionFormat,
}
