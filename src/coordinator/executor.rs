use std::sync::mpsc;
use std::io::Result;

use messaging;
use query::{QueryId, QueryConfig};
use executor::ExecutorType;

use super::Connection;

pub struct ExecutorRef(mpsc::Sender<Event>);

impl ExecutorRef {
    pub fn send(&self, msg: Message) {
        self.0.send(Event::Catalog(msg)).expect("invalid executor ref")
    }
}

pub enum Message {
    Spawn(QueryId, QueryConfig),
}

enum Event {
    Catalog(Message),
    Network,
}

pub struct Executor {
    
}

impl Executor {
    pub fn new(ty: ExecutorType, conn: Connection) -> Self {
        Executor {}
    }
    
    pub fn run(&mut self) -> Result<()> {
        Ok(())
    }
}
