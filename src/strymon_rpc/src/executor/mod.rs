// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use num_traits::{FromPrimitive, ToPrimitive};

use strymon_model::*;
use strymon_communication::rpc::{Name, Request};

#[derive(Primitive, Debug, PartialEq, Eq)]
pub enum ExecutorRPC {
    SpawnQuery = 1,
    TerminateQuery = 2,
}

impl Name for ExecutorRPC {
    type Discriminant = u8;

    fn discriminant(&self) -> Option<Self::Discriminant> {
        self.to_u8()
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        FromPrimitive::from_u8(*value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnQuery {
    pub query: Query,
    pub hostlist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpawnError {
    InvalidRequest,
    FileNotFound,
    WorkdirCreationFailed,
    FetchFailed,
    ExecFailed,
}

impl Request<ExecutorRPC> for SpawnQuery {
    type Success = ();
    type Error = SpawnError;

    const NAME: ExecutorRPC = ExecutorRPC::SpawnQuery;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminateQuery {
    pub query: QueryId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TerminateError {
    NotFound,
    OperationNotSupported,
}

impl Request<ExecutorRPC> for TerminateQuery {
    type Success = ();
    type Error = TerminateError;

    const NAME: ExecutorRPC = ExecutorRPC::TerminateQuery;
}
