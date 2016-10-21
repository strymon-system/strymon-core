use std::collections::BTreeSet;
use std::io::{Error, ErrorKind};

use futures::Future;

use async;
use async::do_while::Stop;
use network::reqresp::{Outgoing, RequestBuf};

use super::resources::CoordinatorRef;
use super::requests::*;
use model::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum State {
    Executor(ExecutorId),
    Query(QueryToken),
    Publication(TopicId),
    Subscription(TopicId),
}

pub struct Dispatch {
    coord: CoordinatorRef,
    associated: BTreeSet<State>, // TODO this needs to be a multiset
    tx: Outgoing,
}

impl Dispatch {
    pub fn new(coord: CoordinatorRef, tx: Outgoing) -> Self {
        debug!("dispatching on new incoming connection");
        Dispatch {
            coord: coord,
            associated: BTreeSet::new(),
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
                    .map_err(|e| e.expect("submission promise canceled?!"))
                    .then(|res| Ok(resp.respond(res)));

                async::spawn(submission);
            }
            "AddWorkerGroup" => {
                let (AddWorkerGroup { query, group }, resp) = req.decode::<AddWorkerGroup>()?;
                let response = self.coord
                    .add_worker_group(query, group)
                    .map_err(|e| e.expect("worker group promise canceled?!"))
                    .then(|res| Ok(resp.respond(res)));
                async::spawn(response)
            }
            "AddExecutor" => {
                let (req, resp) = req.decode::<AddExecutor>()?;
                let id = self.coord.add_executor(req, self.tx.clone());
                self.associated.insert(State::Executor(id));
                resp.respond(Ok((id)));
            },
            "Publish" => {
                let (req, resp) = req.decode::<Publish>()?;
                resp.respond(self.coord.publish(req));
            },
            "Subscribe" => {
                let (req, resp) = req.decode::<Subscribe>()?;
                let subscribe = self.coord.subscribe(req)
                    // TODO(swicki): turn panic into not found error
                    .map_err(|e| e.expect("subscription promise canceled?!"))
                    .then(|res| Ok(resp.respond(res)));
                async::spawn(subscribe);
            },
            _ => {
                let err = Error::new(ErrorKind::InvalidData, "invalid request");
                return Err(Stop::Fail(err));
            }
        }

        Ok(())
    }
}

impl Drop for Dispatch {
    fn drop(&mut self) {
        for state in &self.associated {
            match *state {
                State::Executor(id) => self.coord.remove_executor(id),
                _ => unimplemented!()
            }
        }
    }
}
