use std::io;

use abomonation::Abomonation;

use coordinator::request::ExecutorReady;

use query::{QueryId, QueryConfig};

use messaging::{self, Sender, Receiver};
use messaging::decoder::Decoder;
use messaging::request::handshake::Handshake;
use messaging::request::handler::AsyncReq;

use self::request::*;

pub mod request;
mod executable;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutorId(pub u64);

impl From<u64> for ExecutorId {
    fn from(id: u64) -> ExecutorId {
        ExecutorId(id)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ExecutorType {
    Executable,
    DynamicSharedObject,
}

unsafe_abomonate!(ExecutorId);
unsafe_abomonate!(ExecutorType);

pub struct Executor {
    id: ExecutorId,
    tx: Sender,
    rx: Receiver,
}

impl Executor {
    pub fn new(coordinator: &str) -> io::Result<Self> {
        let (tx, rx) = try!(messaging::connect(coordinator));

        let handshake = Handshake(ExecutorReady {
            ty: ExecutorType::Executable
        });

        let resp = try!(handshake.wait(&tx, &rx));
        // TODO should not panic here
        let id = resp.into_result().expect("failed to get executor id");
        debug!("successfully received executor id: {:?}", id);

        Ok(Executor {
            id: id,
            tx: tx,
            rx: rx,
        })
    }

    pub fn run(self) {
        while let Ok(message) = self.rx.recv() {
            Decoder::from(message)
                .when::<AsyncReq<Spawn>, _>(|req| {
                    let res = executable::spawn(req.id, &req.query);
                    self.tx.send(&req.reply(res));
                })
                .expect("failed to decode executor request");
        }
    }
}
