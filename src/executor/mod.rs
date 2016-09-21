use std::io;
use std::path::PathBuf;
use std::ops::Range;

use abomonation::Abomonation;

use coordinator::request::ExecutorReady;

use query::{QueryParams};

use messaging::{self, Receiver, Sender};
use messaging::decoder::Decoder;
use messaging::request::handshake::Handshake;
use messaging::request::handler::AsyncReq;

use self::request::*;

pub mod request;
pub mod executable;


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
    _id: ExecutorId,
    tx: Sender,
    rx: Receiver,
    coord: String,
    host: String,
}

impl Executor {
    pub fn new(coord: String, host: String, ports: Range<u16>) -> io::Result<Self> {
        let (tx, rx) = try!(messaging::connect(&coord));

        let handshake = Handshake(ExecutorReady {
            ty: ExecutorType::Executable,
            host: host.clone(),
            ports: ports,
        });

        let resp = try!(handshake.wait(&tx, &rx));
        // TODO should not panic here
        let id = resp.into_result().expect("failed to get executor id");
        debug!("successfully received executor id: {:?}", id);

        Ok(Executor {
            _id: id,
            tx: tx,
            rx: rx,
            coord: coord,
            host: host,
        })
    }

    pub fn run(self) {
        while let Ok(message) = self.rx.recv() {
            Decoder::from(message)
                .when::<AsyncReq<Spawn>, _>(|req| {
                    let res = self.spawn(&req.fetch, &req.query, req.procindex);
                    self.tx.send(&req.response(res));
                })
                .expect("failed to decode executor request");
        }
    }

    pub fn spawn(&self,
                 fetch: &str,
                 query: &QueryParams,
                 procindex: usize)
                 -> Result<(), SpawnError> {
        let executable = PathBuf::from(fetch);
        if !executable.exists() {
            return Err(SpawnError::FetchFailed);
        }

        executable::spawn(executable, query, procindex, &self.coord, &self.host)
    }
}
