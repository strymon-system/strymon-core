use model::*;
use network::reqresp::Request;

mod imp;

#[derive(Debug, Clone)]
pub enum Placement {
    Random(usize, usize), // (num executors, num workers)
    Fixed(Vec<ExecutorId>, usize), // (executors, num workers)
}

#[derive(Debug, Clone)]
pub struct Submission {
    pub query: QueryProgram,
    pub name: Option<String>,
    pub placement: Placement,
}

#[derive(Clone, Debug)]
pub enum SubmissionError {
    ExecutorsNotFound,
    SpawnError,
}

impl Request for Submission {
    type Success = QueryId;
    type Error = SubmissionError;

    fn name() -> &'static str {
        "Submission"
    }
}

#[derive(Clone, Debug)]
pub struct AddExecutor {
    pub host: String,
    pub ports: (u16, u16),
    pub format: ExecutionFormat,
}

#[derive(Clone, Debug)]
pub struct ExecutorError;

impl Request for AddExecutor {
    type Success = ExecutorId;
    type Error = ExecutorError;

    fn name() -> &'static str {
        "AddExecutor"
    }
}
