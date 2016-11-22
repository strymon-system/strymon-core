use std::io::Result;

use futures::{self, Future};
use futures::stream::Stream;

use async;
use async::do_while::DoWhileExt;
use network::Network;
use network::reqresp;

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
    host: Option<String>,
}

impl Builder {
    pub fn host(&mut self, host: String) {
        self.host = Some(host);
    }

    pub fn port(&mut self, port: u16) {
        self.port = port;
    }
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            host: None,
            port: 9189,
        }
    }
}

impl Builder {
    pub fn run(self) -> Result<()> {
        let network = Network::init(self.host)?;
        let listener = network.listen(self.port)?;

        let coordinate = futures::lazy(move || {
            let catalog = Catalog::new(&network).expect("failed to create catalog"); // TODO
            let coord = Coordinator::new(catalog);

            listener.map(reqresp::multiplex).for_each(move |(tx, rx)| {
                // every connection gets its own handle
                let mut disp = Dispatch::new(coord.clone(), tx);
                let client = rx.do_while(move |req| disp.dispatch(req))
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


