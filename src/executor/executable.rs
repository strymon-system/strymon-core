use executor::request::SpawnError;
use query::{QueryId, QueryConfig};

pub fn spawn(id: QueryId, query: &QueryConfig) -> Result<(), SpawnError> {
    Ok(())
}
