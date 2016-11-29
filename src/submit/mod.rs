use std::io::{Error, Result, ErrorKind};
use std::net::ToSocketAddrs;

use futures::{self, Future};

use network::Network;
use network::reqrep::{Outgoing, Response};

use coordinator::requests::*;
use model::*;

pub struct Submitter {
    tx: Outgoing,
}

impl Submitter {
    pub fn new<E: ToSocketAddrs>(network: &Network, addr: E) -> Result<Self> {
        let (tx, _) = network.client(addr)?;
        Ok(Submitter { tx: tx })
    }

    pub fn submit<N>(&self, query: QueryProgram, name: N, placement: Placement)
        -> Response<Submission> where N: Into<Option<String>>
    {
        let submission = Submission {
            query: query,
            name: name.into(),
            placement: placement,
        };

        self.tx.request(&submission)
    }
    
    fn lookup(&self, name: &str) -> Result<Topic> {
        self.tx
            .request(&Lookup { name: name.into() })
            .map_err(|e| match e {
                Ok(()) => Error::new(ErrorKind::Other, "topic not found"),
                Err(err) => err,
            })
            .wait()
    }

    pub fn topics(&self) -> Result<Vec<Topic>> {
        
        unimplemented!()
    }

}
