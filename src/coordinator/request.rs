use abomonation::Abomonation;

use query::{QueryId, QueryConfig};
use worker::WorkerIndex;
use executor::ExecutorType;

use messaging::response::Promise;

#[derive(Debug, Clone)]
pub enum Announce {
    Worker(QueryId, WorkerIndex),
    Executor(ExecutorType),
    Client(Submission),
}

#[derive(Clone, Debug)]
pub struct Submission {
    pub config: QueryConfig,
}

pub type SubmissionPromise = Promise<QueryId, SubmissionError>;

#[derive(Clone, Debug)]
pub enum SubmissionError {
    NoExecutorsForType,
    NotEnoughExecutors,
}

unsafe_abomonate!(Submission: config);
unsafe_abomonate!(SubmissionError);

impl Abomonation for Announce {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            Announce::Worker(ref mut query, ref mut index) => {
                query.embalm();
                index.embalm();
            }
            Announce::Executor(ref mut ty) => ty.embalm(),
            Announce::Client(ref mut sub) => sub.embalm(),
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            Announce::Worker(ref query, ref index) => {
                query.entomb(bytes);
                index.entomb(bytes);
            }
            Announce::Executor(ref ty) => ty.entomb(bytes),
            Announce::Client(ref sub) => sub.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Announce::Worker(ref mut query, ref mut index) => {
                Some(bytes)
                    .and_then(|bytes| query.exhume(bytes))
                    .and_then(|bytes| index.exhume(bytes))
            }
            Announce::Executor(ref mut ty) => ty.exhume(bytes),
            Announce::Client(ref mut sub) => sub.exhume(bytes),
        }
    }
}
