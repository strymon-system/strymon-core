use std::io::{Error, ErrorKind, Result};
use std::sync::mpsc;

use worker::config::WorkerConfig;

use messaging;
use messaging::{Receiver, Sender};
use messaging::request::handshake::{Handshake, Response};
use messaging::request::handler::{AsyncHandler};

use coordinator::request::WorkerReady;

pub enum Message {
    Publish,
    Subscribe,
}

pub struct Coordinator {
    tx: Sender,
    rx: Receiver,
}

impl Coordinator {
    pub fn announce(worker: WorkerConfig) -> Result<Self> {
        let (tx, rx) = try!(messaging::connect(&worker.coord));
        
        let handshake = Handshake(WorkerReady {
            query: worker.query,
            index: worker.index,
        });

        let resp = try!(handshake.wait(&tx, &rx));
        try!(resp.into_result().map_err(|err| Error::new(ErrorKind::Other, err)));

        Ok(Coordinator {
            rx: rx,
            tx: tx,
        })
    }
}

pub struct Catalog {
    tx: mpsc::Sender<Message>,
}
