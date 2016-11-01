extern crate timely_query;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate futures;

use std::env;
use std::fs;
use std::io;
use std::thread;

use futures::Future;
use futures::stream::Stream;

use timely_query::coordinator::requests::*;
use timely_query::model::*;
use timely_query::network::Network;
use timely_query::network::reqresp;

fn main() {
    drop(env_logger::init());

    let processes: usize = env::args().nth(1).unwrap().parse().unwrap();
    let threads: usize = env::args().nth(2).unwrap().parse().unwrap();
    let binary = env::args().nth(3).expect("missing binary path");
    let coord = "localhost:9189".to_string();

    let source = fs::canonicalize(binary)
        .and_then(|path| {
            path.to_str()
                .map(|s| s.to_string())
                .ok_or(io::Error::new(io::ErrorKind::Other, "invalid path"))
        })
        .expect("binary not found");

    let query = QueryProgram {
        source: source,
        format: ExecutionFormat::NativeExecutable,
        args: env::args().skip(4).collect(),
    };
    
    let submission = Submission {
        query: query,
        name: None,
        placement: Placement::Random(processes, threads), // hosts, threads
    };

    let network = Network::init(None).unwrap();
    let (tx, rx) = network.connect(&*coord).map(reqresp::multiplex).unwrap();
    thread::spawn(move || rx.for_each(|_| Ok(())).wait().expect("coordinator connection dropped"));

    let id = tx.request(&submission)
        .map_err(|e| e.unwrap_err())
        .wait().expect("spawn error");
    println!("spawned query: {:?}", id);
}
