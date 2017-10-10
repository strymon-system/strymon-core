use model::*;
use strymon_communication::rpc::Request;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnQuery {
    pub query: Query,
    pub hostlist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpawnError {
    InvalidRequest,
    FetchFailed,
    ExecFailed,
}

impl Request for SpawnQuery {
    type Success = ();
    type Error = SpawnError;

    fn name() -> &'static str {
        "SpawnQuery"
    }
}
