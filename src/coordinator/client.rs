use std::io::Result;

use messaging::request::handler::Req;

use query::QueryId;

use super::catalog::Message;
use super::request::{Submission, SubmissionError};
use super::Connection;

pub struct Client {
    conn: Connection,
    submission: Req<Submission>,
}

impl Client {
    pub fn new(req: Req<Submission>, conn: Connection) -> Self {
        Client {
            conn: conn,
            submission: req,
        }
    }

    pub fn run(mut self) -> Result<()> {
        //let (tx, rx) = promise::pair();
        //self.conn.catalog.send(Message::Submission(self.submission, tx));
        // match rx.wait() {
        // TODO response type
        // Ok(id) => self.conn.tx.send(id),
        // Err(err) => self.conn.tx.send(err),
        // }
        //
        Ok(())
    }
}
