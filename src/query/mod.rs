use std::io::{Error as IoError, ErrorKind};
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;

use timely_communication::{Allocator, WorkerGuards};
use timely::{self, Configuration};
use timely::dataflow::scopes::Root;

use futures::{self, Future};
use futures::stream::Stream;

use async;
use network::{Network, reqresp};

use executor::executable::NativeExecutable;
use model::QueryId;
use coordinator::requests::AddWorkerGroup;

pub use self::coordinator::Coordinator;

mod coordinator;

fn initialize(id: QueryId, process: usize, coord: String, host: String) -> Result<Coordinator, IoError> {
    println!("initialze");
    let network = Network::init(host)?;
    let (tx, rx) = network.connect(&*coord).map(reqresp::multiplex)?;

    println!("starting thread");
    // TODO(swicki) get rid of this
    thread::spawn(move || rx.for_each(|_| Ok(())).wait().expect("coordinator connection dropped"));
    println!("announing myself");
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
    
    Ok(coordinator::new(tx, token))
}

pub fn execute<T, F>(func: F) -> Result<WorkerGuards<T>, String>
    where T: Send + 'static,
          F: Fn(&mut Root<Allocator>, Coordinator) -> T,
          F: Send + Sync + 'static
{
    let config = NativeExecutable::from_env()
        .map_err(|err| format!("unable to parse executor data: {:?}", err))?;

    println!("timely_query::execute in {:?}", config.query_id);

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

    println!("initialzed");

    // wrap in mutex because timely requires `Sync` for some reason
    let coord = Mutex::new(coord);
    timely::execute(timely_conf, move |root| {
        let coord = coord.lock().unwrap().clone();
        func(root, coord)
    })
}
