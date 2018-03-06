// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![deny(missing_docs)]

//! Internal APIs of the Strymon coordinator.
//!
//! This library contains the implementation and the internals of the Strymon coordinator. This
//! crate is not intended to be used by end-users directly. Most likely, you will want to use the
//! [`strymon` command line utility](https://strymon-system.github.io/docs/command-line-interface)
//! to start a new coordinator instead.
//!
//! # Implementation
//!
//! The Strymon coordinator maintains a connection with most components of a running Strymon
//! cluster. In order to be able to handle concurrent requests, it's implementation is heavly
//! based on on [`futures`](https://docs.rs/futures). Each potentially blocking request is
//! transformed into a future and polled to completion by a `tokio-core` reactor (this dependency is
//! likely to be replaced by the `LocalPool` executor in futures 0.2, as the implementation does
//! not rely on Tokio's asynchronous I/O primitives).
//!
//! Part of the coordinator implementation is also the [**Catalog**](catalog/index.html), a data
//! structure representing the current state of the Strymon cluster.
//!
//! ## Exposed network services
//!
//! The coordinator exposes two
//! **[`strymon_communication::rpc`](../strymon_communication/rpc/index.html)** interfaces:
//!
//!   1. [`CoordinatorRPC`](../strymon_rpc/coordinator/index.html) for submitting and managing jobs.
//!   It's address has to be known by any client in advance. By default, the coordinator will try
//!   to expose this service on TCP port `9189`.
//!   2. [`CatalogRPC`](../strymon_rpc/coordinator/catalog/index.html) for querying the catalog
//!   using the [`Client`](../strymon_job/operators/service/struct.Client.html) infrastructure of
//!   [`strymon_job`](../strymon_job/index.html). It is exported on an ephemerial TCP port which
//!   can be obtained through a `Subscription` or `Lookup` request on the coordinator interface.
//!
//! ## Handling clients and concurrent requests
//!
//! The coordinator maintains a connection to each connected client (which can be a submitter, an
//! executor or a job). Incoming client requests are handled by the
//! [`Dispatch`](dispatch/struct.Dispatch.html) type, which is created for each accepted
//! connection.
//!
//! The [`Coordinator`](handler/struct.Coordinator.html) type implements the bulk of request
//! handling and contains the state shared by all clients. Its external interface is mirrored
//! through the cloneable [`CoordinatorRef`](handler/struct.CoordinatorRef.html) handle. It is
//! essentially wrapper around `Rc<RefCell<Coordinator>>`, however it also tracks state created
//! by this client (such as issued publications). This allows us to automatically remove the state
//! once the associated client disconnects.
//!
//! A client might issue a request which cannot be handled immediately. Such requests (e.g.
//! a blocking subscription request which only resolves once a matching topic is published) are
//! implemented as a future, which are polled to completion by the internal `tokio-core` reactor.


#[macro_use]
extern crate log;
extern crate rand;
extern crate futures;
extern crate futures_timer;
extern crate tokio_core;

extern crate strymon_rpc;
extern crate strymon_model;
extern crate strymon_communication;

use std::io::Result;

use futures::stream::Stream;
use tokio_core::reactor::Core;

use strymon_communication::Network;

use strymon_rpc::coordinator::CoordinatorRPC;

use self::handler::Coordinator;
use self::dispatch::Dispatch;
use self::catalog::Catalog;

pub mod handler;
pub mod catalog;
pub mod dispatch;

mod util;

/// Creates a new coordinator instance.
///
/// # Examples
/// ```rust,no_run
/// use strymon_coordinator::Builder;
///
/// let mut coord = Builder::default();
/// coord
///     .hostname("localhost".to_string())
///     .port(9189);
/// coord
///     .run()
///     .expect("failed to run coordinator");
/// ```
pub struct Builder {
    port: u16,
    hostname: Option<String>,
}

impl Builder {
    /// Sets the externally reachable hostname of this machine
    /// (default: [*inferred*](../strymon_communication/struct.Network.html#method.new)).
    pub fn hostname(&mut self, hostname: String) -> &mut Self {
        self.hostname = Some(hostname);
        self
    }

    /// Sets the port on which the coordinator service is exposed (default: `9189`).
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }
}

impl Default for Builder {
    fn default() -> Self {
        Builder { port: 9189, hostname: None }
    }
}

impl Builder {
    /// Starts and runs a new coordinator instance.
    ///
    /// This blocks the current thread until the coordinator service shuts down, which currently
    /// only happens if an error occurs.
    ///
    /// Internally, this first creates a new
    /// [`CoordinatorRPC`](../strymon_rpc/coordinator/index.html) and a
    /// [`CatalogRPC`](../strymon_rpc/coordinator/catalog/index.html) service, instantiates an
    /// empty [`Catalog`](catalog/struct.Catalog.html) and then dispatches requests to be handled
    /// by request [`handler`](handler/index.html) logic.
    pub fn run(self) -> Result<()> {
        let network = Network::new(self.hostname)?;
        let server = network.server::<CoordinatorRPC, _>(self.port)?;

        let mut core = Core::new()?;
        let handle = core.handle();
        let (catalog_addr, catalog_service) = catalog::Service::new(&network, &handle)?;

        let coordinate = futures::lazy(move || {
            let catalog = Catalog::new(catalog_addr);
            let coord = Coordinator::new(catalog, handle.clone());

            // dispatch requests for the catalog
            let catalog_coord = coord.clone();
            handle.spawn(catalog_service.for_each(move |req| {
                catalog_coord.catalog_request(req).map_err(|err| {
                    error!("Invalid catalog request: {:?}", err)
                })
            }));

            server.for_each(move |(tx, rx)| {
                let disp = Dispatch::new(coord.clone(), handle.clone(), tx);
                disp.client(rx)
            })
        });

        core.run(coordinate)
    }
}
