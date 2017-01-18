use std::io::Result;
use std::env;

use futures;
use futures::future::Future;
use futures::stream::Stream;

use async;
use network::Network;

use self::handler::Coordinator;
use self::dispatch::Dispatch;
use self::catalog::Catalog;

pub mod requests;

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
        let server = network.server(self.port)?;

        let coordinate = futures::lazy(move || {
            // TODO(swicki) we should return an I/O error instead
            let catalog = Catalog::new(&network).expect("failed to create catalog");
            let coord = Coordinator::new(catalog);

            server.for_each(move |(tx, rx)| {
                // every connection gets its own handle
                let mut disp = Dispatch::new(coord.clone(), tx);
                let client = rx.for_each(move |req| disp.dispatch(req))
                    .map_err(|err| {
                        error!("failed to dispatch client: {:?}", err);
                    });

                // handle client asynchronously
                async::spawn(client);
                Ok(())
            })
        });

        async::finish(coordinate)
    }
}
