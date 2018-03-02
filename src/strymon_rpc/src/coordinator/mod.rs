// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module contains all requests (and the respones) to the the coordinator by the
//! `strymon submit` tool, submitted and running jobs, as well as connecting executors.

pub mod catalog;

use num_traits::FromPrimitive;

use strymon_model::*;
use strymon_communication::rpc::{Name, Request};

/// The list of supported RPC methods at the coordinator.
#[derive(Primitive, Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum CoordinatorRPC {
    /// Requests a new job submission.
    Submission = 1,
    /// Requests the termination of a running job.
    Termination = 2,
    /// Registers a new executor at the coordinator.
    AddExecutor = 3,
    /// Registers a spawned job worker group.
    AddWorkerGroup = 4,
    /// Subscribes to an topic in the catalog.
    Subscribe = 5,
    /// Unsubscribes from a topic.
    Unsubscribe = 6,
    /// Publishes a new topic in the catalog.
    Publish = 7,
    /// Unpublishes a new topic.
    Unpublish = 8,
    /// Performs a topic lookup without subscribing to it.
    Lookup = 9,
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

/// Defines the placement of job workers on the available executors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Placement {
    /// Randomly picks *(Number of Executors, Number of Worker Threads)* workers. The number of threads is per executor.
    Random(usize, usize), // (num executors, num workers)
    /// Spawns the specified number of worker threads on each of the selected executors.
    Fixed(Vec<ExecutorId>, usize), // (executors, num workers)
}

/// A new job submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Submission {
    /// Specifies the job executable.
    pub job: JobProgram,
    /// An optional human-readable description.
    pub name: Option<String>,
    /// The placement of workers in the cluster.
    pub placement: Placement,
}

/// The error type for failed job submissions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubmissionError {
    /// The specified executor list or the request number of executors is not available.
    ExecutorsNotFound,
    /// The coordinator was unable to reach a required executor.
    ExecutorUnreachable,
    /// An executor reported an error while spawning.
    SpawnError(::executor::SpawnError),
}

impl Request<CoordinatorRPC> for Submission {
    type Success = JobId;
    type Error = SubmissionError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Submission;
}

/// A job termination request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Termination {
    /// Identifier of the job to terminate.
    pub job: JobId,
}

/// The error type for failed job termination requests.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TerminationError {
    /// The specified job was not found
    NotFound,
    /// The coordinator was unable to reach a required executors.
    ExecutorUnreachable,
    /// An executor reported an error while terminating the job.
    TerminateError(::executor::TerminateError),
}

impl Request<CoordinatorRPC> for Termination {
    type Success = ();
    type Error = TerminationError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Termination;
}

/// The message sent by new executors to register themselves at the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddExecutor {
    /// The hostname of the machine on which this the new executor is running.
    pub host: String,
    /// A range of ports to be assigned for the `timely_communication` channels.
    pub ports: (u16, u16),
    /// The format of the executables this executor can spawn.
    pub format: ExecutionFormat,
}

/// Error which occurs when coordinator rejects the registration of a new executor.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutorError;

impl Request<CoordinatorRPC> for AddExecutor {
    type Success = ExecutorId;
    type Error = ExecutorError;

    const NAME: CoordinatorRPC = CoordinatorRPC::AddExecutor;
}

/// An opaque token used by job worker groups to authenticate themselves at the coordinator.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JobToken {
    /// The job identifier of the token owner.
    pub id: JobId,
    /// A opaque random number only known to the job process and the coordinator.
    pub auth: u64,
}

/// Registers a newly spawned worker group at the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddWorkerGroup {
    /// The identifier of the job this group belongs to.
    pub job: JobId,
    /// The index of this group within the list of groups of the job.
    pub group: usize,
}

/// The error cause sent back to worker groups when job spawning fails.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerGroupError {
    /// The provided worker group meta-data was invalid.
    InvalidWorkerGroup,
    /// The spawning of the job has been aborted.
    SpawningAborted,
    /// A peer worker group caused this job submission to fail.
    PeerFailed,
}

impl Request<CoordinatorRPC> for AddWorkerGroup {
    type Success = JobToken;
    type Error = WorkerGroupError;

    const NAME: CoordinatorRPC = CoordinatorRPC::AddWorkerGroup;
}

/// A topic subscription request, sent by a a spawned job the the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscribe {
    /// The name of the topic this job would like to subscribe to.
    pub name: String,
    /// If `blocking` is true, the response is delayed until a topic with a matching name is published.
    /// Otherwise, an error message is returned indicating that the requested topic does not exist.
    pub blocking: bool,
    /// A token authenticating the the submitter as a successfully spawned job.
    pub token: JobToken,
}

/// The error message sent back to unsuccessful subscription requests.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscribeError {
    /// The requested topic does not exist.
    TopicNotFound,
    /// The provided authentication token was invalid.
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Subscribe {
    type Success = Topic;
    type Error = SubscribeError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Subscribe;
}

/// A request to unsubscribe from a topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Unsubscribe {
    /// The identifier of the subscribed topic.
    pub topic: TopicId,
    /// A token authenticating the the submitter as a successfully spawned job.
    pub token: JobToken,
}

/// The error message sent back for failed unsubscription request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnsubscribeError {
    /// No subscription found for the requested topic.
    InvalidTopicId,
    /// The provided authentication token was invalid.
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Unsubscribe {
    type Success = ();
    type Error = UnsubscribeError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Unsubscribe;
}

/// A request to publish a topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publish {
    /// The name of the topic to be created.
    pub name: String,
    /// A `strymon_communication` endpoint address on which subscribers can access the publication.
    pub addr: (String, u16),
    /// The kind of topic being published.
    pub schema: TopicSchema,
    /// A token authenticating the the submitter as a successfully spawned job.
    pub token: JobToken,
}

/// The error message sent back for failed publication request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PublishError {
    /// A topic with the same name already exists.
    TopicAlreadyExists,
    /// The provided authentication token was invalid.
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Publish {
    type Success = Topic;
    type Error = PublishError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Publish;
}

/// A request to unpublish a published topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Unpublish {
    /// The identifier of the topic to unpublish.
    pub topic: TopicId,
    /// A token authenticating the the submitter as a successfully spawned job.
    pub token: JobToken,
}

/// The error message sent back for failed unpublication request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnpublishError {
    /// No publication found for the requested topic.
    InvalidTopicId,
    /// The provided authentication token was invalid.
    AuthenticationFailure,
}

impl Request<CoordinatorRPC> for Unpublish {
    type Success = ();
    type Error = UnpublishError;

    const NAME: CoordinatorRPC = CoordinatorRPC::Unpublish;
}

/// Looks up a topic at the coordinator without registering a subscription for it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lookup {
    /// The name of the topic to look up.
    pub name: String,
}

impl Request<CoordinatorRPC> for Lookup {
    type Success = Topic;
    type Error = ();

    const NAME: CoordinatorRPC = CoordinatorRPC::Lookup;
}
