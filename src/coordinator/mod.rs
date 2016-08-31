use std::io::Result;
use std::thread;

use messaging::{self, Receiver, Sender};
use messaging::decoder::Decoder;
use messaging::request::handler::AsyncReq;

use self::catalog::{Catalog, CatalogRef};
use self::request::{ExecutorReady, Submission, WorkerReady};
use self::worker::Worker;
use self::executor::Executor;
use self::submitter::Submitter;

pub mod catalog;
pub mod request;

mod submitter;
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
        enum Incoming {
            Worker(AsyncReq<WorkerReady>),
            Executor(AsyncReq<ExecutorReady>),
            Submission(AsyncReq<Submission>),
        }

        let incoming = Decoder::from(self.rx.recv())
            .when::<AsyncReq<WorkerReady>, _>(Incoming::Worker)
            .when::<AsyncReq<ExecutorReady>, _>(Incoming::Executor)
            .when::<AsyncReq<Submission>, _>(Incoming::Submission)
            .expect("failed to dispatch connection");

        match incoming {
            Incoming::Worker(worker) => Worker::new(worker, self).run(),
            Incoming::Executor(executor) => Executor::new(executor, self).run(),
            Incoming::Submission(submission) => Submitter::new(submission, self).run(),
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
