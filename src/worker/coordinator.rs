use std::io::{Error, ErrorKind, Result};

use worker::Worker;

use messaging;
use messaging::{Receiver, Sender};
use messaging::request::handler::{AsyncHandler, Req, Resp};

use coordinator::request::WorkerReady;

pub struct Coordinator {
    tx: Sender,
    rx: Receiver,
    handler: AsyncHandler,
}

impl Coordinator {
    pub fn announce(worker: Worker) -> Result<Self> {
        let (tx, rx) = try!(messaging::connect(&worker.coordinator));
        let mut handler = AsyncHandler::new();

        let (req, resp) = handler.submit(WorkerReady {
            query: worker.query,
            index: worker.index,
        });

        try!(resp.await().map_err(|err| Error::new(ErrorKind::Other, err)));

        Ok(Coordinator {
            rx: rx,
            tx: tx,
            handler: handler,
        })
    }
}
