// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use strymon_model::*;
use strymon_communication::rpc::Request;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnQuery {
    pub query: Query,
    pub hostlist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpawnError {
    InvalidRequest,
    FetchFailed,
    ExecFailed,
}

impl Request for SpawnQuery {
    type Success = ();
    type Error = SpawnError;

    const NAME: &'static str = "SpawnQuery";
}
