

use async::{self, TaskFuture};
use async::queue::{Sender, Receiver};
use futures::{self, Future};
use futures::stream::Stream;
use network::reqresp::{self, RequestBuf, Incoming, Outgoing};
use network::service::Service;

use self::requests::*;
use std::io::{Result, Error, ErrorKind};

use self::coord::{Coordinator, CoordinatorRef, Event};

pub mod requests;

mod coord;
mod catalog;
mod util;

struct Connection {
    coord: CoordinatorRef,
    tx: Outgoing,
    rx: Incoming,
}

impl Connection {
    fn new(coord: CoordinatorRef, tx: Outgoing, rx: Incoming) -> Self {
        Connection {
            coord: coord,
            tx: tx,
            rx: rx,
        }
    }

    fn dispatch(self, initial: RequestBuf) -> Result<()> {
        match initial.name() {
            "Submission" => {
                let (req, resp) = initial.decode::<Submission>()?;
                self.coord.send(Event::Submission(req, resp));
            },
            "WorkerGroup" => {

            },
            "AddExecutor" => {

            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "invalid initial request"))
        }
        
        Ok(())
    }
}


pub fn coordinate(port: u16) -> Result<()> {
    let service = Service::init(None)?;
    let listener = service.listen(port)?;

    let (coord_task, coord) = Coordinator::new();

    let server = listener.map(reqresp::multiplex).for_each(move |(tx, rx)| {
        // every connection gets its own handle
        let coord = coord.clone();

        // receive one initial request from the client,
        // then create and spawn a task based on this initial request
        let client = rx.into_future()
            .map_err(|(err, _)| err)
            .and_then(move |(initial, rx)| {
                let conn = Connection::new(coord, tx, rx);
                conn.dispatch(initial.unwrap())
            })
            .map_err(|err| {
                error!("failed to dispatch incoming client: {:?}", err);
            });

        // handle client asynchronously
        async::spawn(client);
        Ok(())
    });

    async::finish(futures::lazy(move || {
        async::spawn(coord_task);

        server
    }))
}
