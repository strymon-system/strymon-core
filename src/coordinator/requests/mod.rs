mod imp;

use model::*;
use network::reqresp::Request;

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
    NoExecutorsForType,
    NotEnoughExecutors,
    InvalidExecutorId,
}

impl Request for Submission {
    type Success = QueryId;
    type Error = SubmissionError;

    fn name() -> &'static str {
        "Submission"
    }
}
