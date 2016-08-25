use query::QueryId;

pub mod coordinator;

pub type WorkerIndex = usize;

pub struct Worker {
    query_id: QueryId,
    worker_index: WorkerIndex,

    coordinator: String, // TODO external ip address.. inherit from executor?
}
