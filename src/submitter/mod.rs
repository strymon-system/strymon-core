use std::io;

use coordinator::request::{Submission, SubmissionError};

use query::{QueryId, QueryConfig};

use messaging::{self, Sender, Receiver};
use messaging::decoder::Decoder;
use messaging::request::AsyncResult;
use messaging::request::handler::{AsyncHandler};

pub enum SubmitError {
    Io(io::Error),
    Submission(SubmissionError),
}

impl From<io::Error> for SubmitError {
    fn from(io: io::Error) -> Self {
        SubmitError::Io(io)
    }
}

impl From<SubmissionError> for SubmitError {
    fn from(s: SubmissionError) -> Self {
        SubmitError::Submission(s)
    }
}

pub struct Submit {
    tx: Sender,
    rx: Receiver,
    handler: AsyncHandler,
}

impl Submit {
    pub fn new(coordinator: &str) -> io::Result<Self> {
        let (tx, rx) = try!(messaging::connect(coordinator));
        Ok(Submit {
            tx: tx,
            rx: rx,
            handler: AsyncHandler::new(),
        })
    }
    
    pub fn query(&mut self, query: QueryConfig) -> Result<QueryId, SubmitError> {
        unimplemented!()
    }
}
