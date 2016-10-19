use std::io::{Error as IoError, ErrorKind};
use std::sync::Mutex;
use std::thread;

use timely_communication::{Allocator, WorkerGuards};
use timely::{self, Configuration};
use timely::dataflow::scopes::Root;

use futures::Future;
use futures::stream::Stream;

use network::{Network};
use network::reqresp::{self, Outgoing};

use executor::executable::NativeExecutable;
use model::QueryId;
use coordinator::requests::{AddWorkerGroup, QueryToken};

pub mod subscribe;
pub mod publish;

#[derive(Clone)]
pub struct Coordinator {
    token: QueryToken,
    network: Network,
    tx: Outgoing,
}

fn initialize(id: QueryId, process: usize, coord: String, host: String) -> Result<Coordinator, IoError> {
    println!("initialze");
    let network = Network::init(host)?;
    let (tx, rx) = network.connect(&*coord).map(reqresp::multiplex)?;

    // TODO(swicki) get rid of this
    thread::spawn(move || rx.for_each(|_| Ok(())).wait().expect("coordinator connection dropped"));

    let announce = tx.request(&AddWorkerGroup {
        query: id,
        group: process,
    });

    let token = announce.wait()
        .map_err(|err| err.and_then::<(), _>(|err| {
            let err = format!("failed to register: {:?}", err);
            Err(IoError::new(ErrorKind::Other, err))
        }))
        .map_err(Result::unwrap_err)?;
    
    Ok(Coordinator {
        tx: tx,
        network: network,
        token: token,
    })
}

pub fn execute<T, F>(func: F) -> Result<WorkerGuards<T>, String>
    where T: Send + 'static,
          F: Fn(&mut Root<Allocator>, Coordinator) -> T,
          F: Send + Sync + 'static
{
    let config = NativeExecutable::from_env()
        .map_err(|err| format!("parse failure. not running on an executor?"))?;

    // create timely configuration
    let timely_conf = if config.hostlist.len() > 1 {
        Configuration::Cluster(config.threads, config.process, config.hostlist, true)
    } else if config.threads > 1 {
        Configuration::Process(config.threads)
    } else {
        Configuration::Thread
    };

    let coord = initialize(config.query_id, config.process, config.coord, config.host)
        .map_err(|err| format!("failed to connect to coordinator: {:?}", err))?;

    // wrap in mutex because timely requires `Sync` for some reason
    let coord = Mutex::new(coord);
    timely::execute(timely_conf, move |root| {
        let coord = coord.lock().unwrap().clone();
        func(root, coord)
    })
}
