// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The interface exposed by executors. This interface is currently only intended to be used by the
//! coordinator to send requests to a registered executor.

use num_traits::FromPrimitive;

use strymon_model::*;
use strymon_communication::rpc::{Name, Request};

/// The list of available requests an executor can serve.
#[derive(Primitive, Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum ExecutorRPC {
    /// Request to spawn a new job worker group.
    SpawnJob = 1,
    /// Request to terminate a running job.
    TerminateJob = 2,
}

impl Name for ExecutorRPC {
    type Discriminant = u8;

    fn discriminant(&self) -> Self::Discriminant {
        *self as Self::Discriminant
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        FromPrimitive::from_u8(*value)
    }
}

/// A request to spawn a new worker group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnJob {
    /// The meta-data of the job worker group to spawn.
    pub job: Job,
    /// The hostlist to be passed to `timely_communication`.
    pub hostlist: Vec<String>,
}

/// The error message returned by the executor for failed spawn requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpawnError {
    /// The request was invalid.
    InvalidRequest,
    /// The executor was unable to locate the downloaded file.
    FileNotFound,
    /// The working directory for the job could not be created.
    WorkdirCreationFailed,
    /// The executable was unable to fetch the executable file.
    FetchFailed,
    /// Launching the executable failed.
    ExecFailed,
}

impl Request<ExecutorRPC> for SpawnJob {
    type Success = ();
    type Error = SpawnError;

    const NAME: ExecutorRPC = ExecutorRPC::SpawnJob;
}

/// A request to terminate a running job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminateJob {
    /// The job to terminate.
    pub job: JobId,
}

/// The error message returned by the executor if job termination fails.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TerminateError {
    /// The specified job is not running on this executor.
    NotFound,
    /// This implementation of the executor does not support job termination.
    OperationNotSupported,
}

impl Request<ExecutorRPC> for TerminateJob {
    type Success = ();
    type Error = TerminateError;

    const NAME: ExecutorRPC = ExecutorRPC::TerminateJob;
}
