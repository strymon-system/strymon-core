use std::io::{Error, ErrorKind, Result};
use std::sync::mpsc;
use std::thread;

use messaging;
use messaging::{Receiver, Sender, Message as NetworkMessage};
use messaging::decoder::Decoder;
use messaging::request::handshake::{Handshake, Response};
use messaging::request::handler::{AsyncHandler, AsyncResponse};

use query::QueryId;
use worker::WorkerIndex;

use coordinator::request::WorkerReady;

pub enum Message {
    Network(Result<NetworkMessage>),
}

pub struct Coordinator {
    tx: Sender,
    queue: mpsc::Receiver<Message>,
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
        };
        let catalog = Catalog {
            tx: queue_tx,
        };

        Ok((coord, catalog))
    }
    
    pub fn detach(mut self) {
        thread::spawn(move || self.run());
    }
    
    pub fn run(&mut self) {
        let mut  handler = AsyncHandler::new();
        while let Ok(msg) = self.queue.recv() {
            match msg {
                Message::Network(result) => {
                    Decoder::from(result)
                        .when::<AsyncResponse, _>(|resp| {
                            handler.resolve(resp);
                        })
                        .expect("error while receiving from coordinator");
                },

            }
        }
    }
}

#[derive(Clone)]
pub struct Catalog {
    tx: mpsc::Sender<Message>,
}

impl Catalog {

}
