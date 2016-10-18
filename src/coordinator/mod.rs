use std::io::Result;

use futures::Future;
use futures::stream::Stream;

use async;
use async::do_while::DoWhileExt;
use network::Network;
use network::reqresp;

use self::resources::Coordinator;
use self::dispatch::Dispatch;

pub mod requests;

pub mod resources;
pub mod catalog;
pub mod dispatch;

pub fn coordinate(port: u16) -> Result<()> {
    let network = Network::init(None)?;
    let listener = network.listen(port)?;

    let coord = Coordinator::new();
    let server = listener.map(reqresp::multiplex).for_each(move |(tx, rx)| {
        // every connection gets its own handle
        let mut disp = Dispatch::new(coord.clone(), tx);
        let client = rx.do_while(move |req| disp.dispatch(req))
            .map_err(|err| {
                error!("failed to dispatch client: {:?}", err);
            });

        // handle client asynchronously
        async::spawn(client);
        Ok(())
    });

    async::finish(server)
}
