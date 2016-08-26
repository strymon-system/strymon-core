use std::sync::mpsc;
use std::io::Result;

use messaging::{Message as NetMessage, Receiver, Sender};
use messaging::request;
use messaging::request::handler::Req;

use query::{QueryConfig, QueryId};
use executor::{ExecutorId, ExecutorType};

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
    Spawn(QueryId, QueryConfig),
}

enum Event {
    Catalog(Message),
    Network(Result<NetMessage>),
}

pub struct Executor {
    tx: Sender,
    rx: Receiver,
    catalog: CatalogRef,
    req: Req<ExecutorReady>,
}

impl Executor {
    pub fn new(req: Req<ExecutorReady>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;
        Executor {
            tx: tx,
            rx: rx,
            catalog: catalog,
            req: req,
        }
    }

    pub fn run(self) -> Result<()> {
        let (tx_event, rx_event) = mpsc::channel();
        let executor_ref = ExecutorRef(tx_event.clone());

        let (ready_tx, ready_rx) = request::promise::<ExecutorReady>();
        self.catalog
            .send(CatalogMessage::ExecutorReady((&*self.req).clone(), executor_ref, ready_tx));

        let result = ready_rx.await();

        Ok(())
    }
}
