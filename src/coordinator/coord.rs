use futures::stream::Stream;

use async::{self, TaskFuture};
use async::queue::{channel, Sender, Receiver};

use network::reqresp::Response;

use super::requests::*;

pub enum Event {
    Submission(Submission, Response<Submission>),
}

#[derive(Clone)]
pub struct CoordinatorRef {
    coord: Sender<Event, ()>,
}

pub struct Coordinator {
    
}

impl Coordinator {
    pub fn new() -> (TaskFuture, CoordinatorRef) {
        let (tx, rx) = channel();
        let task = Box::new(rx.for_each(|event| {
            Ok(())
        }));
        // handle for connections to use
        let handle = CoordinatorRef {
            coord: tx,
        };
        
        (task, handle)
    }
}
