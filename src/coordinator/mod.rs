use std::io::Result;

use futures::{self, Future};
use futures::stream::Stream;

use network::service::{Service, Listener};
use async;

pub mod interface;

//use self::catalog::{Catalog, CatalogMut};

pub fn coordinate(port: u16) -> Result<()> {
    //let catalog = CatalogMut::from(Catalog::new());

    let network = Service::init(None)?;
    let listener = network.listen(port)?;
    let incoming = listener.for_each(|(tx, rx)| {
        // receive one message (handshake) from the client
        let client = rx.into_future().and_then(move |(handshake, rx)| {
            println!("handshake {:?}", handshake.is_some());
            Ok(())
        }).map_err(|(err, _)| {
            warn!("error while handling connection: {:?}", err);
        });
        
        // handle client asynchronously
        Ok(async::spawn(client))
    });

    async::finish(incoming)
}
