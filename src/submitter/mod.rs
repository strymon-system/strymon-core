use std::io;

use coordinator::request::{Submission, SubmissionError};

use query::{QueryId, QueryConfig};

use messaging::{self, Sender, Receiver};

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
}

impl Submit {
    pub fn new(coordinator: &str) -> io::Result<Self> {
        let (tx, rx) = try!(messaging::connect(coordinator));
        Ok(Submit {
            tx: tx,
            rx: rx,
        })
    }
    
    pub fn query(query: QueryConfig) -> Result<QueryId, SubmitError> {
        unimplemented!()
    }
}
