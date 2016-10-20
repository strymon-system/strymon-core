extern crate time;
extern crate timely;
extern crate timely_query;
extern crate env_logger;
#[macro_use] extern crate log;

use std::hash::Hash;

use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::{EventReader, Replay};

fn validate<S: Scope, D: Data>(stream: &Stream<S, D>, msgcount: usize)
    where S::Timestamp: Hash {
    stream.count().inspect(move |&count| {
        assert_eq!(count, msgcount, "did not receive all messages");
        debug!("got {} messages", count);
    });
}

fn main() {
    env_logger::init().unwrap();

    let msgcount: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let _msgsize: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    timely_query::execute(move |root, coord| {
        root.scoped::<(),_,_>(move |scope| {
            if cfg!(feature = "pubsub") {
                debug!("request subscribe");
                let topic = coord.blocking_subscribe::<Vec<u8>, _>("bench").unwrap();

                let stream = topic.into_iter().flat_map(|batch| batch).to_stream(scope);
                validate(&stream, msgcount);
            } else if cfg!(feature = "caprep") {
                use std::net::TcpListener;
                let server = TcpListener::bind("localhost:8000").unwrap();
                let producer = server.incoming().next().unwrap().unwrap();
                let stream = EventReader::<_,Vec<u8>,_>::new(producer)
                    .replay_into(scope);
                validate(&stream, msgcount);
            }
        });

        let start = time::precise_time_ns();
        while root.step() { }
        let stop = time::precise_time_ns();

        println!("consumer: total time: {}", stop - start);

        // TODO(swicki) this is an ugly hack :D
        use std::process::Command;
        Command::new("killall").arg("coordinator")
            .spawn().unwrap()
            .wait().unwrap();
    }).unwrap();
}
