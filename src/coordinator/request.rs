use abomonation::Abomonation;

use query::{QueryConfig, QueryId};
use worker::WorkerIndex;
use executor::ExecutorType;
use topic::{Topic, TopicId, TypeId};

use util::promise::Promise;

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

#[derive(Clone, Debug)]
pub enum SubmissionError {
    NoExecutorsForType,
    NotEnoughExecutors,
}

#[derive(Debug, Clone)]
pub enum PubSubRequest {
    Subscribe(String, TypeId),
    Publish(String, String, TypeId),
    Unpublish(TopicId),
}

#[derive(Clone, Debug)]
pub enum TopicError {
    AlreadyExists,
    NotFound,
}

#[derive(Clone, Debug)]
pub enum WorkerError {
    InvalidQueryId,
    InvalidWorkerId,
    FailedPeer,
}

unsafe_abomonate!(TopicError);
unsafe_abomonate!(WorkerError);
unsafe_abomonate!(Submission: config);
unsafe_abomonate!(SubmissionError);

impl Abomonation for PubSubRequest {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            PubSubRequest::Subscribe(ref mut string, _) => string.embalm(),
            PubSubRequest::Publish(ref mut string, ref mut addr, _) => {
                string.embalm();
                addr.embalm();
            }
            PubSubRequest::Unpublish(ref mut id) => id.embalm(),
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            PubSubRequest::Subscribe(ref string, _) => string.entomb(bytes),
            PubSubRequest::Publish(ref string, ref addr, _) => {
                string.entomb(bytes);
                addr.entomb(bytes);
            }
            PubSubRequest::Unpublish(ref id) => id.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            PubSubRequest::Subscribe(ref mut string, _) => string.exhume(bytes),
            PubSubRequest::Publish(ref mut string, ref mut addr, _) => {
                Some(bytes)
                    .and_then(|bytes| string.exhume(bytes))
                    .and_then(|bytes| addr.exhume(bytes))
            }
            PubSubRequest::Unpublish(ref mut id) => id.exhume(bytes),
        }
    }
}

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
