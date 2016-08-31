use abomonation::Abomonation;

use query::{QueryId, QueryConfig};

use messaging::request::Request;

#[derive(Debug, Clone)]
pub struct Spawn {
    pub id: QueryId,
    pub query: QueryConfig,
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

unsafe_abomonate!(Spawn : id, query);
unsafe_abomonate!(SpawnError);
