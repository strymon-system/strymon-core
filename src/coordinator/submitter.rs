use std::io::Result;

use messaging::{Receiver, Sender};
use messaging::request;
use messaging::request::handshake::{Handshake, Response};

use super::catalog::{CatalogRef, Message};
use super::request::{Submission};
use super::Connection;

pub struct Submitter {
    _tx: Sender,
    _rx: Receiver,
    _catalog: CatalogRef,
}

impl Submitter {
    pub fn new(req: Handshake<Submission>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;

        let (ready_tx, ready_rx) = request::promise::<Submission>();
        catalog.send(Message::Submission(req.0, ready_tx));

        let resp = Response::<Submission>::from(ready_rx.await());
        tx.send(&resp);

        Submitter {
            _tx: tx,
            _rx: rx,
            _catalog: catalog,
        }
    }

    pub fn run(self) -> Result<()> {
        Ok(())
    }
}
