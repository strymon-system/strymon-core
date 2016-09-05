use std::io::{Error, ErrorKind, Result};
use std::any::Any;
use std::sync::mpsc;
use std::thread;
use std::fmt::Debug;

use abomonation::Abomonation;

use messaging;
use messaging::{Receiver, Sender, Message as NetworkMessage};
use messaging::decoder::Decoder;
use messaging::request::{self, Request, AsyncResult, Complete};
use messaging::request::handshake::{Handshake, Response};
use messaging::request::handler::{AsyncHandler, AsyncResponse};

use query::QueryId;
use worker::WorkerIndex;
use topic::{Topic, TopicId, TypeId};

use coordinator::request::WorkerReady;
use coordinator::catalog::request::*;

pub enum Message {
    Publish(Publish, Complete<Publish>),
    Subscribe(Subscribe, Complete<Subscribe>),
    Unpublish(Unpublish, Complete<Unpublish>),
    Unsubscribe(Unsubscribe, Complete<Unsubscribe>),
    Network(Result<NetworkMessage>),
}

pub struct Coordinator {
    tx: Sender,
    queue: mpsc::Receiver<Message>,
    handler: AsyncHandler,
}

impl Coordinator {
    pub fn announce(coord: &str, query: QueryId, worker: WorkerIndex) -> Result<(Self, Catalog)> {
        let (tx, rx) = try!(messaging::connect(&coord));

        let handshake = Handshake(WorkerReady {
            query: query,
            index: worker,
        });

        let resp = try!(handshake.wait(&tx, &rx));
        try!(resp.into_result().map_err(|err| Error::new(ErrorKind::Other, err)));

        let (queue_tx, queue_rx) = mpsc::channel();
        let queue_net = queue_tx.clone();
        rx.detach(move |res| {
            let _ = queue_net.send(Message::Network(res));
        });

        let coord = Coordinator {
            tx: tx,
            queue: queue_rx,
            handler: AsyncHandler::new(),
        };
        let catalog = Catalog {
            tx: queue_tx,
        };

        Ok((coord, catalog))
    }
    
    pub fn detach(mut self) {
        thread::spawn(move || self.run());
    }

    fn submit<R: Request>(&mut self, r: R, tx: Complete<R>)
        where R: Abomonation + Clone + Any
    {
        let asyncreq = self.handler.submit(r, tx);
        self.tx.send(&asyncreq);
    }

    pub fn run(&mut self) {
        while let Ok(msg) = self.queue.recv() {
            match msg {
                Message::Network(result) => {
                    Decoder::from(result)
                        .when::<AsyncResponse, _>(|resp| {
                            self.handler.resolve(resp);
                        })
                        .expect("error while receiving from coordinator");
                },
                Message::Publish(req, tx) => self.submit(req, tx),
                Message::Subscribe(req, tx) => self.submit(req, tx),
                Message::Unpublish(req, tx) => self.submit(req, tx),
                Message::Unsubscribe(req, tx) => self.submit(req, tx),
            }
        }
    }
}

#[derive(Clone)]
pub struct Catalog {
    tx: mpsc::Sender<Message>,
}



impl Catalog {
    fn request<R: Request + Debug, F>(&self, f: F, r: R) -> AsyncResult<R::Success, R::Error>
        where F: Fn(R, Complete<R>) -> Message
    {
        debug!("submitting request: {:?}", r);
        let (tx, rx) = request::promise::<R>();
        self.tx.send(f(r, tx)).expect("failed to issue request");
        rx
    }

    pub fn publish(&self, name: String, addr: String, dtype: TypeId) -> AsyncResult<Topic, PublishError> {
        self.request(Message::Publish, Publish {
            name: name,
            addr: addr,
            dtype: dtype,
        })
    }

    pub fn subscribe(&self, name: String, blocking: bool) -> AsyncResult<Topic, SubscribeError> {
        self.request(Message::Subscribe, Subscribe { name: name, blocking: blocking })
    }

    pub fn unpublish(&self, id: TopicId) -> AsyncResult<(), UnpublishError> {
        self.request(Message::Unpublish, Unpublish { topic: id })
    }

    pub fn unsubscribe(&self, id: TopicId) -> AsyncResult<(), UnsubscribeError> {
        self.request(Message::Unsubscribe, Unsubscribe { topic: id })
    }
}
