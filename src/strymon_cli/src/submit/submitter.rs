// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io::Result;
use std::net::ToSocketAddrs;

use futures::Future;

use strymon_communication::Network;
use strymon_communication::rpc::{Outgoing, Response};

use strymon_model::*;
use strymon_rpc::coordinator::*;
use strymon_rpc::coordinator::catalog::*;

pub struct Submitter {
    coord: Outgoing,
    catalog: Outgoing,
}

impl Submitter {
    pub fn new<E: ToSocketAddrs>(network: &Network, addr: E) -> Result<Self> {
        let (coord, _) = network.client::<CoordinatorRPC, _>(addr)?;
        let topic = coord
            .request(&Lookup { name: String::from("$catalog") })
            .wait()
            .map_err(|err| err.expect_err("$catalog topic not found"))?;
        let addr = (&*topic.addr.0, topic.addr.1);
        let (catalog, _) = network.client::<CoordinatorRPC, _>(addr)?;

        Ok(Submitter { coord, catalog })
    }

    pub fn submit<N>(
        &self,
        query: QueryProgram,
        name: N,
        placement: Placement,
    ) -> Response<CoordinatorRPC, Submission>
    where
        N: Into<Option<String>>,
    {
        let submission = Submission {
            query: query,
            name: name.into(),
            placement: placement,
        };

        self.coord.request(&submission)
    }

    pub fn terminate(&self, id: JobId) -> Response<CoordinatorRPC, Termination> {
        let termination = Termination { query: id };

        self.coord.request(&termination)
    }

    pub fn topics(&self) -> Result<Vec<Topic>> {
        self.catalog.request(&AllTopics::new()).wait().map_err(
            |err| {
                err.unwrap_err()
            },
        )
    }

    pub fn executors(&self) -> Result<Vec<Executor>> {
        self.catalog.request(&AllExecutors::new()).wait().map_err(
            |err| {
                err.unwrap_err()
            },
        )
    }

    pub fn queries(&self) -> Result<Vec<Query>> {
        self.catalog.request(&AllQueries::new()).wait().map_err(
            |err| {
                err.unwrap_err()
            },
        )
    }

    pub fn publications(&self) -> Result<Vec<Publication>> {
        self.catalog
            .request(&AllPublications::new())
            .wait()
            .map_err(|err| err.unwrap_err())
    }

    pub fn subscriptions(&self) -> Result<Vec<Subscription>> {
        self.catalog
            .request(&AllSubscriptions::new())
            .wait()
            .map_err(|err| err.unwrap_err())
    }
}
