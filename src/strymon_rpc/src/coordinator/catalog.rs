// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use num_traits::FromPrimitive;

use strymon_model::*;
use strymon_communication::rpc::{Name, Request};

#[derive(Primitive, Debug, PartialEq, Eq, Clone, Copy, TypeName)]
#[repr(u8)]
pub enum CatalogRPC {
    AllTopics = 1,
    AllExecutors = 2,
    AllQueries = 3,
    AllPublications = 4,
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
    ( $req:ident, $resp:ty ) => {
        // TODO(swicki): All these requests could be represented as a unit struct,
        // however due to https://github.com/3Hren/msgpack-rust/issues/159 we currently
        // need to have some some dummy struct members.

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct $req(());

        impl $req {
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

impl_request!(AllTopics, Vec<Topic>);
impl_request!(AllExecutors, Vec<Executor>);
impl_request!(AllQueries, Vec<Query>);
impl_request!(AllPublications, Vec<Publication>);
impl_request!(AllSubscriptions, Vec<Subscription>);
