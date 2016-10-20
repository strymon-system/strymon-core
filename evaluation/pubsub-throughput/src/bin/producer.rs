extern crate time;
extern crate timely;
extern crate timely_query;
extern crate env_logger;
#[macro_use] extern crate log;

use std::thread;
use std::time::Duration;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::{EventWriter};

fn main() {
    env_logger::init().unwrap();

    let msgcount: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let msgsize: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let worker = timely_query::execute(move |root, coord| {
        let payload = vec![1u8; msgsize];

        root.scoped::<(),_,_>(|scope| {
            let stream = (0..msgcount).map(move |_| payload.clone()).to_stream(scope);
            if cfg!(feature = "pubsub") {
                coord.publish("bench", &stream).unwrap();
                // allow the subscriber to catch up, it will panic if not.
                thread::sleep(Duration::from_secs(3));
            } else if cfg!(feature = "caprep") {
                use std::net::TcpStream;
                let consumer = TcpStream::connect("localhost:8000").unwrap();

                stream.capture_into(EventWriter::new(consumer));
            }
        });

        time::precise_time_ns()
    }).unwrap().join();
    // we measure outside the query, since we need to make sure that
    // the network is finished
    let stop = time::precise_time_ns();
    let start = *worker[0].as_ref().unwrap();
    println!("producer: total time: {}", stop - start);
}
