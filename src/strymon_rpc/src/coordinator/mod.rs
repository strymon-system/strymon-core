// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use num_traits::FromPrimitive;

use strymon_model::*;
use strymon_communication::rpc::{Name, Request};

#[derive(Primitive, Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum CoordinatorRPC {
    Submission = 1,
    Termination = 2,
    AddExecutor = 3,
    AddWorkerGroup = 4,
    Subscribe = 5,
    Unsubscribe = 6,
    Publish = 7,
    Unpublish = 8,
    Lookup = 9,
    AddKeeperWorker = 10,
    GetKeeperAddress = 11,
    RemoveKeeperWorker = 12,
}

impl Name for CoordinatorRPC {
    type Discriminant = u8;
    fn discriminant(&self) -> Self::Discriminant {
        *self as Self::Discriminant
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        FromPrimitive::from_u8(*value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Placement {
    Random(usize, usize), // (num executors, num workers)
    Fixed(Vec<ExecutorId>, usize), // (executors, num workers)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Submission {
    pub query: QueryProgram,
    pub name: Option<String>,
    pub placement: Placement,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubmissionError {
    ExecutorsNotFound,
    ExecutorUnreachable,
    SpawnError(::executor::SpawnError),
}

impl Request<CoordinatorRPC> for Submission {
    type Success = QueryId;
    type Error = SubmissionError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Submission;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Termination {
    pub query: QueryId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TerminationError {
    NotFound,
    ExecutorUnreachable,
    TerminateError(::executor::TerminateError),
}

impl Request<CoordinatorRPC> for Termination {
    type Success = ();
    type Error = TerminationError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Termination;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddExecutor {
    pub host: String,
    pub ports: (u16, u16),
    pub format: ExecutionFormat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutorError;

impl Request<CoordinatorRPC> for AddExecutor {
    type Success = ExecutorId;
    type Error = ExecutorError;

    const NAME: CoordinatorRPC = CoordinatorRPC::AddExecutor;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueryToken {
    pub id: QueryId,
    pub auth: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddWorkerGroup {
    pub query: QueryId,
    pub group: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerGroupError {
    InvalidWorkerGroup,
    SpawningAborted,
    PeerFailed,
}

impl Request<CoordinatorRPC> for AddWorkerGroup {
    type Success = QueryToken;
    type Error = WorkerGroupError;

    const NAME: CoordinatorRPC = CoordinatorRPC::AddWorkerGroup;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscribe {
    pub name: String,
    pub blocking: bool,
    pub token: QueryToken,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscribeError {
    TopicNotFound,
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Subscribe {
    type Success = Topic;
    type Error = SubscribeError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Subscribe;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Unsubscribe {
    pub topic: TopicId,
    pub token: QueryToken,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnsubscribeError {
    InvalidTopicId,
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Unsubscribe {
    type Success = ();
    type Error = UnsubscribeError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Unsubscribe;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publish {
    pub name: String,
    pub addr: (String, u16),
    pub schema: TopicSchema,
    pub token: QueryToken,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PublishError {
    TopicAlreadyExists,
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Publish {
    type Success = Topic;
    type Error = PublishError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Publish;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Unpublish {
    pub topic: TopicId,
    pub token: QueryToken,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnpublishError {
    InvalidTopicId,
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Unpublish {
    type Success = ();
    type Error = UnpublishError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Unpublish;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lookup {
    pub name: String,
}

impl Request<CoordinatorRPC> for Lookup {
    type Success = Topic;
    type Error = ();

    const NAME: CoordinatorRPC = CoordinatorRPC::Lookup;
}

/// Add a worker of a Keeper.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddKeeperWorker {
    pub name: String,
    pub worker_num: usize,
    pub addr: (String, u16),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AddKeeperWorkerError {
    WorkerAlreadyExists,
}

impl Request<CoordinatorRPC> for AddKeeperWorker {
    type Success = ();
    type Error = AddKeeperWorkerError;

    const NAME: CoordinatorRPC = CoordinatorRPC::AddKeeperWorker;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetKeeperAddress {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetKeeperAddressError {
    KeeperNotFound,
    KeeperHasNoWorkers,
}

impl Request<CoordinatorRPC> for GetKeeperAddress {
    type Success = (String, u16);
    type Error = GetKeeperAddressError;

    const NAME: CoordinatorRPC = CoordinatorRPC::GetKeeperAddress;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoveKeeperWorker {
    pub name: String,
    pub worker_num: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RemoveKeeperWorkerError {
    KeeperDoesntExist,
    KeeperWorkerDoesntExist,
}

impl Request<CoordinatorRPC> for RemoveKeeperWorker {
    type Success = ();
    type Error = RemoveKeeperWorkerError;

    const NAME: CoordinatorRPC = CoordinatorRPC::RemoveKeeperWorker;
}
