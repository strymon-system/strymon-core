// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module contains the queries which can be submitted to the catalog service to inspect
//! the current state of the system.

use num_traits::FromPrimitive;

use strymon_model::*;
use strymon_communication::rpc::{Name, Request};

/// The list of available methods to query the catalog.
#[derive(Primitive, Debug, PartialEq, Eq, Clone, Copy, TypeName)]
#[repr(u8)]
pub enum CatalogRPC {
    /// Return a list of all created topics.
    AllTopics = 1,
    /// Return a list of all available executors.
    AllExecutors = 2,
    /// Return a list of all running jobs.
    AllQueries = 3,
    /// Return a list of all publications.
    AllPublications = 4,
    /// Return a list of all subscriptions.
    AllSubscriptions = 5,
}

impl Name for CatalogRPC {
    type Discriminant = u8;
    fn discriminant(&self) -> Self::Discriminant {
        *self as Self::Discriminant
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        FromPrimitive::from_u8(*value)
    }
}

macro_rules! impl_request {
    ( $req:ident, $resp:ty, $doc:expr) => {
        // TODO(swicki): All these requests could be represented as a unit struct,
        // however due to https://github.com/3Hren/msgpack-rust/issues/159 we currently
        // need to have some some dummy struct members.

        #[derive(Debug, Clone, Serialize, Deserialize)]
        #[doc=$doc]
        pub struct $req(());

        impl $req {
            #[doc="Creates a new instance of this request type."]
            pub fn new() -> Self {
                $req (())
            }
        }

        impl Request<CatalogRPC> for $req {
            const NAME: CatalogRPC = CatalogRPC::$req;

            type Success = $resp;
            type Error = ();
        }
    }
}

impl_request!(AllTopics, Vec<Topic>, "The request type use to query a list of all topics.");
impl_request!(AllExecutors, Vec<Executor>, "The request type use to query a list of all executors.");
impl_request!(AllQueries, Vec<Query>, "The request type use to query a list of all jobs.");
impl_request!(AllPublications, Vec<Publication>, "The request type use to query a list of all publications.");
impl_request!(AllSubscriptions, Vec<Subscription>, "The request type use to query a list of all subscriptions.");
