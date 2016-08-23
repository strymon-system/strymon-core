use std::io::Result;

use super::request::Submission;
use super::Connection;

pub struct Client {
    
}

impl Client {
    pub fn new(submission: Submission, conn: Connection) -> Self {
        Client {}
    }

    pub fn run(&mut self) -> Result<()> {
        Ok(())
    }
}
