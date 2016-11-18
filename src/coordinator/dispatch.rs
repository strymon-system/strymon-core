use std::io::{Error, ErrorKind};

use futures::Future;

use async;
use async::do_while::Stop;
use network::reqresp::{Outgoing, RequestBuf};

use super::handler::CoordinatorRef;
use super::requests::*;

pub struct Dispatch {
    coord: CoordinatorRef,
    tx: Outgoing,
}

impl Dispatch {
    pub fn new(coord: CoordinatorRef, tx: Outgoing) -> Self {
        debug!("dispatching on new incoming connection");
        Dispatch {
            coord: coord,
            tx: tx,
        }
    }

    pub fn dispatch(&mut self, req: RequestBuf) -> Result<(), Stop<Error>> {
        debug!("dispatching request {}", req.name());
        match req.name() {
            "Submission" => {
                let (req, resp) = req.decode::<Submission>()?;
                let submission = self.coord
                    .submission(req)
                    .then(|res| Ok(resp.respond(res)));

                async::spawn(submission);
            }
            "AddWorkerGroup" => {
                let (AddWorkerGroup { query, group }, resp) = req.decode::<AddWorkerGroup>()?;
                let response = self.coord
                    .add_worker_group(query, group)
                    .then(|res| Ok(resp.respond(res)));
                async::spawn(response)
            }
            "AddExecutor" => {
                let (req, resp) = req.decode::<AddExecutor>()?;
                let id = self.coord.add_executor(req, self.tx.clone());
                resp.respond(Ok((id)));
            },
            "Publish" => {
                let (req, resp) = req.decode::<Publish>()?;
                resp.respond(self.coord.publish(req));
            },
            "Unpublish" => {
                let (Unpublish { token, topic }, resp) = req.decode::<Unpublish>()?;
                resp.respond(self.coord.unpublish(token, topic));
            }
            "Subscribe" => {
                let (req, resp) = req.decode::<Subscribe>()?;
                let subscribe = self.coord.subscribe(req)
                    .then(|res| Ok(resp.respond(res)));
                async::spawn(subscribe);
            },
            "Unsubscribe" => {
                let (Unsubscribe { token, topic }, resp) = req.decode::<Unsubscribe>()?;
                resp.respond(self.coord.unsubscribe(token, topic));
            }
            _ => {
                let err = Error::new(ErrorKind::InvalidData, "invalid request");
                return Err(Stop::Fail(err));
            }
        }

        Ok(())
    }
}
