use std::io::{Error, ErrorKind, Result};
use std::sync::mpsc;

use messaging;
use messaging::{Receiver, Sender};
use messaging::request::handshake::{Handshake, Response};
use messaging::request::handler::{AsyncHandler};

use query::QueryId;
use worker::WorkerIndex;

use coordinator::request::WorkerReady;

pub enum Message {
    Publish,
    Subscribe,
}

pub struct Coordinator {
    tx: Sender,
    rx: Receiver,
    host: String,
}

impl Coordinator {
    pub fn announce(coord: &str, host: &str, query: QueryId, worker: WorkerIndex) -> Result<Self> {
        let (tx, rx) = try!(messaging::connect(&coord));

        let handshake = Handshake(WorkerReady {
            query: query,
            index: worker,
        });

        let resp = try!(handshake.wait(&tx, &rx));
        try!(resp.into_result().map_err(|err| Error::new(ErrorKind::Other, err)));

        Ok(Coordinator {
            rx: rx,
            tx: tx,
            host: host.to_string()
        })
    }
    
    pub fn catalog(&self) -> Catalog {
        // TODO
        Catalog {
            tx: mpsc::channel().0
        }
    }
}

pub struct Catalog {
    tx: mpsc::Sender<Message>,
}
