use std::sync::mpsc;
use std::io::Result;

use messaging::{Message as NetworkMessage, Receiver, Sender};
use messaging::decoder::Decoder;
use messaging::request::{self, Complete};
use messaging::request::handler::{AsyncHandler, AsyncResponse};
use messaging::request::handshake::{Handshake, Response};

use query::QueryId;
use executor::{ExecutorId, ExecutorType};
use executor::request::Spawn;

use super::Connection;
use super::request::ExecutorReady;
use super::catalog::{CatalogRef, Message as CatalogMessage};

pub struct ExecutorRef(mpsc::Sender<Event>);

impl ExecutorRef {
    pub fn send(&self, msg: Message) {
        self.0.send(Event::Catalog(msg)).expect("invalid executor ref")
    }
}

pub enum Message {
    Spawn(Spawn, Complete<Spawn>),
}

enum Event {
    Catalog(Message),
    Network(Result<NetworkMessage>),
}

pub struct Executor {
    tx: Sender,
    rx: Receiver,
    catalog: CatalogRef,
    events: mpsc::Receiver<Event>,
}

impl Executor {
    pub fn new(req: Handshake<ExecutorReady>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;

        let (tx_event, rx_event) = mpsc::channel();
        let (ready_tx, ready_rx) = request::promise::<ExecutorReady>();
        let executor_ref = ExecutorRef(tx_event.clone());

        catalog.send(CatalogMessage::ExecutorReady(req.0, executor_ref, ready_tx));
        let resp = Response::<ExecutorReady>::from(ready_rx.await());
        tx.send(&resp);

        Executor {
            tx: tx,
            rx: rx,
            catalog: catalog,
            events: rx_event,
        }
    }

    pub fn run(self) -> Result<()> {
        let mut handler = AsyncHandler::new();

        while let Ok(event) = self.events.recv() {
            match event {
                Event::Catalog(req) => {
                    match req {
                        Message::Spawn(req, resp) => {
                            let packet = handler.submit(req, resp);
                            self.tx.send(&packet);
                        }
                    }
                }
                Event::Network(Ok(msg)) => {
                    Decoder::from(msg)
                        .when::<AsyncResponse, _>(|reply| handler.resolve(reply))
                        .expect("unable to executor response");
                }
                Event::Network(Err(err)) => {
                    // TODO unregister
                    return Err(err);
                }
            }
        }

        Ok(())
    }
}
