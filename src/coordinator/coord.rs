use futures::stream::Stream;

use async::{self, TaskFuture};
use async::queue::{channel, Sender, Receiver};

use network::reqresp::Responder;

use model::*;

use super::requests::*;
use super::catalog::Catalog;
use super::util::Generator;

pub enum Event {
    Submission(Submission, Responder<Submission>),
}

#[derive(Clone)]
pub struct CoordinatorRef {
    coord: Sender<Event, ()>,
}

impl CoordinatorRef {
    pub fn send(&self, event: Event) {
        if let Err(_) = self.coord.send(Ok(event)) {
            panic!("coordinator queue deallocated")
        }
    }
}

pub struct Coordinator {
    catalog: Catalog,

    queryid: Generator<QueryId>,
    executorid: Generator<ExecutorId>,
}

impl Coordinator {
    pub fn new() -> (TaskFuture, CoordinatorRef) {
        let (tx, rx) = channel();

        let mut coord = Coordinator {
            catalog: Catalog::new(),
            queryid: Generator::new(),
            executorid: Generator::new(),
        };

        // handle for connections to use
        let handle = CoordinatorRef { coord: tx };

        let task = Box::new(rx.for_each(move |event| {
            Ok(coord.dispatch(event))
        }));

        (task, handle)
    }
    
    fn dispatch(&mut self, event: Event) {
        match event {
            Event::Submission(req, resp) => {
                
            }
        }
    }
}
