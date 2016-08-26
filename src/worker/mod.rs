use query::QueryId;

pub mod coordinator;

pub type WorkerIndex = usize;

pub struct Worker {
    query: QueryId,
    index: WorkerIndex,

    coordinator: String, // TODO external ip address.. inherit from executor?
}
