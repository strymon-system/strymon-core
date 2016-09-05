use std::io::Result;
use std::sync::mpsc;

use worker::WorkerIndex;
use query::QueryId;

use messaging::{Message as NetworkMessage, Sender};
use messaging::decoder::Decoder;
use messaging::request::{self, Request};
use messaging::request::handshake::{Handshake, Response};
use messaging::request::handler::{AsyncReq, Handoff, handoff};

use coordinator::catalog::TopicRequest;
use coordinator::catalog::request::*;

use super::catalog::{CatalogRef, Message as CatalogMessage};
use super::request::WorkerReady;
use super::Connection;

pub struct Worker {
    query_id: QueryId,
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
    // TODO
}

enum Event {
    Catalog(Message),
    Network(Result<NetworkMessage>),
}

impl Worker {
    pub fn new(req: Handshake<WorkerReady>, conn: Connection) -> Self {
        let Connection { tx, rx, catalog } = conn;
        let (tx_event, rx_event) = mpsc::channel();

        let Handshake(worker) = req;

        let query_id = worker.query;
        let worker_ref = WorkerRef(tx_event.clone());
        let (ready_tx, ready_rx) = request::promise::<WorkerReady>();
        catalog.send(CatalogMessage::WorkerReady(worker, worker_ref, ready_tx));

        // wait for catalog, then send back response
        let resp = Response::<WorkerReady>::from(ready_rx.await());
        tx.send(&resp);

        rx.detach(move |res| {
            let _ = tx_event.send(Event::Network(res));
        });

        Worker {
            query_id: query_id,
            catalog: catalog,
            sender: tx,
            events: rx_event,
        }
    }

    fn request<R: Request, F>(&self, f: F, req: AsyncReq<R>)
        where F: Fn(QueryId, R, Handoff<R>) -> TopicRequest
    {
        let (req, handoff) = handoff::<R>(req, self.sender.clone());
        let topic_req = f(self.query_id, req, handoff);
        self.catalog.send(CatalogMessage::TopicRequest(topic_req))
    }

    fn decode(&mut self, message: NetworkMessage) {
        Decoder::from(message)
            .when::<AsyncReq<Publish>, _>(|req| self.request(TopicRequest::Publish, req))
            .when::<AsyncReq<Subscribe>, _>(|req| self.request(TopicRequest::Subscribe, req))
            .when::<AsyncReq<Unpublish>, _>(|req| self.request(TopicRequest::Unpublish, req))
            .when::<AsyncReq<Unsubscribe>, _>(|req| self.request(TopicRequest::Unsubscribe, req))
            .expect("failed to parse request from woker")
    }

    pub fn run(&mut self) -> Result<()> {
        while let Ok(event) = self.events.recv() {
            match event {
                Event::Network(Ok(message)) => self.decode(message),
                Event::Network(Err(_)) => {
                    // TODO deal
                    break;
                }
                Event::Catalog(_) => unreachable!(),
            }
        }

        Ok(())
    }
}
