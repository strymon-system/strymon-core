use std::sync::mpsc;
use std::io::Result;

use messaging::request::handler::Req;

use query::{QueryConfig, QueryId};
use executor::{ExecutorType, ExecutorId};

use super::Connection;
use super::request::ExecutorReady;

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
    pub fn new(req: Req<ExecutorReady>, conn: Connection) -> Self {
        Executor {}
    }

    pub fn run(self) -> Result<()> {
        Ok(())
    }
}
