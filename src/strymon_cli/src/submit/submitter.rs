// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The client stub of a subset of the `CoordinatorRPC` and `CatalogRPC` interface used for
//! submitting jobs the coordinator.

use std::io::Result;
use std::net::ToSocketAddrs;

use futures::Future;

use strymon_communication::Network;
use strymon_communication::rpc::{Outgoing, Response};

use strymon_model::*;
use strymon_rpc::coordinator::*;
use strymon_rpc::coordinator::catalog::*;

/// The submitter is a client handle which allows us to send requests to the coordinator
pub struct Submitter {
    /// Outgoing requests of type `CoordinatorRPC`.
    coord: Outgoing,
    /// Outgoing requests of type `CatalogRPC`.
    catalog: Outgoing,
}

impl Submitter {
    /// Creates a new instance for the coordinator at endpoint address `addr`.
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

    /// Issues a new asynchronous job submission request.
    pub fn submit<N>(
        &self,
        job: JobProgram,
        name: N,
        placement: Placement,
    ) -> Response<CoordinatorRPC, Submission>
    where
        N: Into<Option<String>>,
    {
        let submission = Submission {
            job: job,
            name: name.into(),
            placement: placement,
        };

        self.coord.request(&submission)
    }

    /// Issues an asynchronous job termination request.
    pub fn terminate(&self, id: JobId) -> Response<CoordinatorRPC, Termination> {
        let termination = Termination { job: id };

        self.coord.request(&termination)
    }

    /// Dumps the list of topics in the catalog.
    pub fn topics(&self) -> Result<Vec<Topic>> {
        self.catalog.request(&AllTopics::new()).wait().map_err(
            |err| {
                err.unwrap_err()
            },
        )
    }

    /// Dumps the list of executors in the catalog.
    pub fn executors(&self) -> Result<Vec<Executor>> {
        self.catalog.request(&AllExecutors::new()).wait().map_err(
            |err| {
                err.unwrap_err()
            },
        )
    }

    /// Dumps the list of jobs in the catalog.
    pub fn jobs(&self) -> Result<Vec<Job>> {
        self.catalog.request(&AllJobs::new()).wait().map_err(
            |err| {
                err.unwrap_err()
            },
        )
    }

    /// Dumps the list of publications in the catalog.
    pub fn publications(&self) -> Result<Vec<Publication>> {
        self.catalog
            .request(&AllPublications::new())
            .wait()
            .map_err(|err| err.unwrap_err())
    }

    /// Dumps the list of subscriptions in the catalog.
    pub fn subscriptions(&self) -> Result<Vec<Subscription>> {
        self.catalog
            .request(&AllSubscriptions::new())
            .wait()
            .map_err(|err| err.unwrap_err())
    }
}
