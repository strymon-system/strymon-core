use std::io::Result;
use std::sync::mpsc;

use worker::WorkerIndex;
use query::QueryId;

use coordinator::catalog::{CatalogRef, Message as CatalogMessage};
use coordinator::request::PubSubRequest;
use coordinator::Connection;

use messaging::Sender;
use util::promise;

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
    Network(PubSubRequest),
}

impl Worker {
    pub fn new(query_id: QueryId, worker_index: WorkerIndex, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;
        let (tx_event, rx_event) = mpsc::channel();

        let worker_ref = WorkerRef(tx_event.clone());
        let (ready_tx, ready_rx) = promise::pair();
        catalog.send(CatalogMessage::WorkerReady(query_id, worker_index, worker_ref, ready_tx));
        // match ready_rx.wait() {
        // TODO response type
        // Ok(id) => tx.send(id),
        // Err(err) => tx.send(err),
        // }
        //
        //
        // Dispatcher::new()
        // .on_error(move |err| err_tx.send(Event::NetworkError(err)).unwrap())
        // .on_recv<Publish, _>(move |publish| pub_tx.send())
        // .detach();
        //

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
