use std::io::Result;
use std::sync::mpsc;

use worker::WorkerIndex;
use query::QueryId;

use messaging::request;
use messaging::request::handshake::{Handshake, Response};

use super::catalog::{CatalogRef, Message as CatalogMessage};
use super::request::WorkerReady;
use super::Connection;

use messaging::Sender;

pub struct Worker {
    catalog: CatalogRef,
    sender: Sender,
    events: mpsc::Receiver<Event>,
}

pub struct WorkerRef(mpsc::Sender<Event>);

impl WorkerRef {
    pub fn send(&self, msg: Message) {
        self.0.send(Event::Catalog(msg)).expect("invalid worker ref")
    }
}

pub enum Message {
    Terminate,
}

enum Event {
    Catalog(Message),
}

impl Worker {
    pub fn new(req: Handshake<WorkerReady>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;
        let (tx_event, rx_event) = mpsc::channel();

        let worker_ref = WorkerRef(tx_event.clone());
        let (ready_tx, ready_rx) = request::promise::<WorkerReady>();
        catalog.send(CatalogMessage::WorkerReady(req.0, worker_ref, ready_tx));

        // wait for catalog, then send back response
        let resp = Response::<WorkerReady>::from(ready_rx.await());
        tx.send(&resp);

        Worker {
            catalog: catalog,
            sender: tx,
            events: rx_event,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        while let Ok(event) = self.events.recv() {

        }

        Ok(())
    }
}
