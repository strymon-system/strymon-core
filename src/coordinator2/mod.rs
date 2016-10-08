use std::io::Result;

use futures::Future;
use futures::stream::Stream;

use network::{Service, Listener};
use async::queue::{Sender, Receiver};
use async::select::Select;

enum Event {

}

pub fn coordinate(port: u16) -> Result<()> {
    let network = Service::init(None)?;
    let select = Select::<Event>::new();

    let listener = network.listen(port)?;
    let incoming = listener.and_then(|(tx, rx)| {
        rx.into_future().and_then(move |(msg, rx)| {
            Ok(())
        }).map_err(|(err, _)| err)
    });


    Ok(())
}
