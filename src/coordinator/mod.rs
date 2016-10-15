use std::io::{Result as IoResult, Error, ErrorKind};
use futures::{self, Future};
use futures::stream::Stream;

use async::{self, TaskFuture};
use async::queue::{Sender, Receiver};
use network::service::{Service};
use network::reqresp::{self, RequestBuf, Incoming, Outgoing};
use network::message::abomonate::DecodeError;

use self::requests::*;

pub mod requests;

mod coord;
use self::coord::{Coordinator, CoordinatorRef};

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

    fn dispatch(self, initial: RequestBuf) -> Result<TaskFuture, DecodeError> {
        unimplemented!()
    }
}

struct SubmissionState {
    conn: Connection,
}



pub fn coordinate(port: u16) -> IoResult<()> {
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
                conn.dispatch(initial.unwrap()).map_err(|err| {
                    Error::new(ErrorKind::Other, "unable to decode initial request")
                })
            })
            .map_err(|err| {
                error!("failed to dispatch incoming client: {:?}", err);
            })
            .and_then(|task| task);
            

        // handle client asynchronously
        async::spawn(client);
        Ok(())
    });

    async::finish(futures::lazy(move || {
        async::spawn(coord_task);

        server
    }))
}
