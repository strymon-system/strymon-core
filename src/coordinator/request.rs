use std::fmt;
use std::error::Error;
use std::ops::Range;

use abomonation::Abomonation;

use query::QueryId;
use worker::WorkerIndex;
use executor::{ExecutorId, ExecutorType};

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

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WorkerError::InvalidQueryId => write!(f, "Invalid query identifier"),
            WorkerError::InvalidWorkerId => write!(f, "Invalid worker identifier"),
            WorkerError::FailedPeer => write!(f, "A peer failed to connect to the coordinator"),
        }
    }
}

impl Error for WorkerError {
    fn description(&self) -> &str {
        match *self {
            WorkerError::InvalidQueryId => "Invalid query id",
            WorkerError::InvalidWorkerId => "Invalid worker id",
            WorkerError::FailedPeer => "Failed peer",
        }
    }
}

impl Request for WorkerReady {
    type Success = ();
    type Error = WorkerError;
}

unsafe_abomonate!(WorkerReady: query, index);
unsafe_abomonate!(WorkerError);

#[derive(Clone, Debug)]
pub struct Submission {
    pub fetch: String,
    pub binary: ExecutorType,
    pub num_executors: usize,
    pub num_workers: usize, // per executor
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

unsafe_abomonate!(Submission: fetch, binary, num_executors, num_workers);
unsafe_abomonate!(SubmissionError);

#[derive(Clone, Debug)]
pub struct ExecutorReady {
    pub ty: ExecutorType,
    pub host: String,
    pub ports: Range<u16>,
}

#[derive(Clone, Debug)]
pub struct ExecutorError;

impl Request for ExecutorReady {
    type Success = ExecutorId;
    type Error = ExecutorError;
}

unsafe_abomonate!(ExecutorReady: ty, host);
unsafe_abomonate!(ExecutorError);
