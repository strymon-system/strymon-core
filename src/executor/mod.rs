use std::io;

use abomonation::Abomonation;

use coordinator::request::ExecutorReady;

use messaging::{self, Sender, Receiver};

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
    tx: Sender,
    rx: Receiver,
}

impl Executor {
    pub fn new(coordinator: &str) -> io::Result<Self> {
        let (tx, rx) = try!(messaging::connect(coordinator));
        

        Ok(Executor {
            tx: tx,
            rx: rx,
        })
    }
}
