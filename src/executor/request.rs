use abomonation::Abomonation;

use query::{QueryId, QueryParams};

use messaging::request::Request;

#[derive(Debug, Clone)]
pub struct Spawn {
    pub fetch: String,
    pub query: QueryParams,
    pub process: usize,
}

#[derive(Debug, Clone)]
pub enum SpawnError {
    FetchFailed,
    ExecFailed,
}

impl Request for Spawn {
    type Success = ();
    type Error = SpawnError;
}

unsafe_abomonate!(Spawn : fetch, query, process);
unsafe_abomonate!(SpawnError);
