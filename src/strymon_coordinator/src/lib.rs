// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate log;
extern crate rand;
extern crate futures;
extern crate tokio_core;

extern crate strymon_rpc;
extern crate strymon_model;
extern crate strymon_communication;

use std::io::Result;
use std::env;

use futures::future::Future;
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

pub struct Builder {
    port: u16,
}

impl Builder {
    pub fn host(&mut self, host: String) {
        env::set_var("TIMELY_SYSTEM_HOSTNAME", host);
    }

    pub fn port(&mut self, port: u16) {
        self.port = port;
    }
}

impl Default for Builder {
    fn default() -> Self {
        Builder { port: 9189 }
    }
}

impl Builder {
    pub fn run(self) -> Result<()> {
        let network = Network::init()?;
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
                // every connection gets its own handle
                let mut disp = Dispatch::new(coord.clone(), handle.clone(), tx);
                let client = rx.for_each(move |req| disp.dispatch(req))
                    .map_err(|err| {
                        error!("failed to dispatch client: {:?}", err);
                    });

                // handle client asynchronously
                handle.spawn(client);
                Ok(())
            })
        });

        core.run(coordinate)
    }
}
