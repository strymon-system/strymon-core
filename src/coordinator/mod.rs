use std::io::{Error, Result};
use std::thread;

use messaging::{self, Receiver, Sender};
use self::catalog::{Catalog, CatalogRef};
use self::request::Announce;
use self::worker::Worker;
use self::executor::Executor;
use self::client::Client;

pub mod catalog;
pub mod request;

mod client;
mod executor;
mod worker;

pub struct Connection {
    tx: Sender,
    rx: Receiver,
    catalog: CatalogRef,
}

impl Connection {
    fn new(tx: Sender, rx: Receiver, catalog: CatalogRef) -> Self {
        Connection {
            tx: tx,
            rx: rx,
            catalog: catalog,
        }
    }

    fn dispatch(self) -> Result<()> {
        match try!(self.rx.recv_decode::<request::Announce>()) {
            Announce::Worker(queryid, workerindex) => Worker::new(queryid, workerindex, self).run(),
            Announce::Executor(executortype) => Executor::new(executortype, self).run(),
            Announce::Client(submission) => Client::new(submission, self).run(),
        }
    }
}

pub fn coordinate(addr: &str) -> Result<()> {
    let (catalog_ref, catalog) = Catalog::new();
    catalog.detach();

    let mut listener = try!(messaging::listen(Some(addr)));
    loop {
        let (tx, rx) = try!(listener.accept());
        let catalog = catalog_ref.clone();
        thread::spawn(move || {
            debug!("accepted new connection");
            let connection = Connection::new(tx, rx, catalog);
            if let Err(err) = connection.dispatch() {
                error!("dispatch failed: {:?}", err);
            }
        });
    }
}
