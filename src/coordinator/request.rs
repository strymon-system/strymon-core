use abomonation::Abomonation;

use query::{QueryConfig, QueryId};
use worker::WorkerIndex;
use executor::{ExecutorType, ExecutorId};
use topic::{Topic, TopicId, TypeId};

use messaging::request::Request;

#[derive(Clone, Debug)]
pub struct WorkerReady {
    pub query: QueryId,
    pub index: WorkerIndex,
}

#[derive(Clone, Debug)]
pub enum WorkerError {
    InvalidQueryId,
    InvalidWorkerId,
    FailedPeer,
}

impl Request for WorkerReady {
    type Success = ();
    type Error = WorkerError;
}

unsafe_abomonate!(WorkerReady : query, index);
unsafe_abomonate!(WorkerError);

#[derive(Clone, Debug)]
pub struct Submission {
    pub config: QueryConfig,
}

#[derive(Clone, Debug)]
pub enum SubmissionError {
    NoExecutorsForType,
    NotEnoughExecutors,
}

impl Request for Submission {
    type Success = QueryId;
    type Error = SubmissionError;
}

unsafe_abomonate!(Submission: config);
unsafe_abomonate!(SubmissionError);

#[derive(Clone, Debug)]
pub struct ExecutorReady {
    ty: ExecutorType,
}

#[derive(Clone, Debug)]
pub struct ExecutorError;

impl Request for ExecutorReady {
    type Success = ExecutorId;
    type Error = ExecutorError;
}

unsafe_abomonate!(ExecutorReady: ty);
unsafe_abomonate!(ExecutorError);
