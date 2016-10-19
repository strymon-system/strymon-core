extern crate time;
extern crate timely;
extern crate timely_query;

use std::io::prelude::*;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::{EventReader, Replay};

fn main() {
    let msgcount: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let msgsize: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    timely_query::execute(move |root, coord| {
        use std::net::TcpListener;
        let server = TcpListener::bind("localhost:8000").unwrap();
        let mut producer = server.incoming().next().unwrap().unwrap();

        root.scoped::<(),_,_>(move |scope| {
            if cfg!(feature = "pubsub") {
                let topic = coord.blocking_subscribe::<Vec<u8>, _>("bench").unwrap();

                topic.into_iter().flat_map(|batch| batch).to_stream(scope)
                .count().inspect(move |&count| assert_eq!(count, msgcount))
            } else if cfg!(feature = "caprep") {
                EventReader::<_,Vec<u8>,_>::new(producer)
                    .replay_into(scope)
                    .count().inspect(move |&count| assert_eq!(count, msgcount))
            } else {
                // TODO
                unimplemented!()
            };
        });

        let start = time::precise_time_ns();
        while root.step() { }
        let stop = time::precise_time_ns();

        println!("total time: {}", stop - start);

    }).unwrap();
}
