use std::io::Result;

use messaging::{Receiver, Sender};
use messaging::request;
use messaging::request::handshake::{Handshake, Response};

use super::catalog::{CatalogRef, Message};
use super::request::{Submission, SubmissionError};
use super::Connection;

pub struct Submitter {
    tx: Sender,
    rx: Receiver,
    catalog: CatalogRef,
    submission: Submission,
}

impl Submitter {
    pub fn new(req: Handshake<Submission>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;
        Submitter {
            tx: tx,
            rx: rx,
            catalog: catalog,
            submission: req.0,
        }
    }

    pub fn run(mut self) -> Result<()> {
        let (ready_tx, ready_rx) = request::promise::<Submission>();
        self.catalog.send(Message::Submission(self.submission, ready_tx));

        let resp = Response::<Submission>::from(ready_rx.await());
        self.tx.send(&resp);
        
        Ok(())
    }
}
