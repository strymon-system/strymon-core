use std::io::{Error, ErrorKind};

use futures::{self, Future};
use futures::stream::Stream;

use async;
use async::do_while::{DoWhileExt, Stop};
use async::queue;

use network::Network;
use network::reqresp::{self, RequestBuf};

use model::*;

use coordinator::requests::*;
use executor::requests::*;

pub mod requests;
pub mod executable;

pub struct ExecutorService {
    id: ExecutorId,
    coord: String,
    host: String,
}

impl ExecutorService {
    pub fn new(id: ExecutorId, coord: String, host: String) -> Self {
        ExecutorService {
            id: id,
            coord: coord,
            host: host,
        }
    }

    fn spawn(&mut self, query: Query, hostlist: Vec<String>) -> Result<(), SpawnError> {
        let process = query.executors
            .iter()
            .position(|&id| self.id == id)
            .ok_or(SpawnError::InvalidRequest)?;
        let threads = query.workers / hostlist.len();
        let executable = query.program.source;
        let args = &*query.program.args;
        let id = query.id;
        let _child = executable::spawn(&executable,
                                      id,
                                      args,
                                      threads,
                                      process,
                                      &*hostlist,
                                      &self.coord,
                                      &self.host)?;

        Ok(())
    }

    pub fn dispatch(&mut self, req: RequestBuf) -> Result<(), Stop<Error>> {
        match req.name() {
            "SpawnQuery" => {
                let (SpawnQuery { query, hostlist }, resp) = req.decode::<SpawnQuery>()?;
                debug!("got spawn request for {:?}", query);
                resp.respond(self.spawn(query, hostlist));
                Ok(())
            }
            _ => {
                let err = Error::new(ErrorKind::InvalidData, "invalid request");
                return Err(Stop::Fail(err));
            }
        }
    }
}

pub struct Builder {
    host: String,
    coord: String,
    ports: (u16, u16),
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            host: String::from("localhost"),
            coord: String::from("localhost:9189"),
            ports: (2101, 4101),
        }
    }
}

impl Builder {
    pub fn start(self) -> Result<(), Error> {
        let Builder { host, ports, coord } = self;
        let network = Network::init(host.clone())?;
        let (tx, rx) = network.connect(&*coord).map(reqresp::multiplex)?;

        async::finish(futures::lazy(move || {
            // TODO(swicki): this is not so nice currently,
            // but we need to poll on `rx` in order to make progress
            let (buf_tx, buf_rx) = queue::channel();
            async::spawn(rx.then(Ok)
                .do_while(move |res| buf_tx.send(res).map_err(|_| Stop::Terminate)));

            // announce ourselves at the coordinator
            let id = tx.request(&AddExecutor {
                    host: host.clone(),
                    ports: ports,
                    format: ExecutionFormat::NativeExecutable,
                })
                .map_err(|e| e.unwrap_err());

            // once we get results, start the actual executor service
            id.and_then(move |id| {
                let mut executor = ExecutorService::new(id, coord, host);
                buf_rx.do_while(move |req| executor.dispatch(req))
            })
        }))
    }
}
