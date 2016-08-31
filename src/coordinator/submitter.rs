use std::io::Result;

use messaging::{Receiver, Sender};
use messaging::request::handler::{self, AsyncReq};

use super::catalog::{CatalogRef, Message};
use super::request::{Submission, SubmissionError};
use super::Connection;

pub struct Submitter {
    tx: Sender,
    rx: Receiver,
    catalog: CatalogRef,
    submission: AsyncReq<Submission>,
}

impl Submitter {
    pub fn new(req: AsyncReq<Submission>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;
        Submitter {
            tx: tx,
            rx: rx,
            catalog: catalog,
            submission: req,
        }
    }

    pub fn run(mut self) -> Result<()> {
        let (req, resp) = handler::handoff(self.submission, self.tx);
        self.catalog.send(Message::Submission(req, resp));
        Ok(())
    }
}
