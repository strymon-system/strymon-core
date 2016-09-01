use std::io;

use coordinator::request::{Submission as QuerySubmission, SubmissionError};

use query::QueryId;

use messaging::{self, Sender, Receiver};
use messaging::decoder::Decoder;
use messaging::request::handshake::{Handshake, Response};

#[derive(Debug)]
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

pub struct Submission {
    tx: Sender,
    rx: Receiver,
}

impl Submission {
    pub fn connect(coordinator: &str) -> io::Result<Self> {
        let (tx, rx) = try!(messaging::connect(coordinator));
        Ok(Submission {
            tx: tx,
            rx: rx,
        })
    }

    pub fn query(self, submission: QuerySubmission) -> Result<QueryId, SubmitError> {
        let handshake = Handshake(submission);
        let response = try!(handshake.wait(&self.tx, &self.rx));

        response.into_result().map_err(SubmitError::from)
    }
}
