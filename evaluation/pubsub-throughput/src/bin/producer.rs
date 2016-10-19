extern crate time;
extern crate timely;
extern crate timely_query;

use std::io::prelude::*;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::{EventWriter};

fn main() {
    let msgcount: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let msgsize: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    timely_query::execute(move |root, coord| {
        let payload = vec![1u8; msgsize];

        root.scoped::<(),_,_>(|scope| {
            let stream = (0..msgcount).map(move |_| payload.clone()).to_stream(scope);
            if cfg!(feature = "pubsub") {
                coord.publish("bench", &stream).unwrap();
            } else {
                use std::net::TcpStream;
                let mut consumer = TcpStream::connect("localhost:8000").unwrap();

                if cfg!(feature = "caprep") {
                    stream.capture_into(EventWriter::new(consumer));
                } else {
                    stream.inspect(move |data| {
                        consumer.write_all(data).unwrap();
                    });
                }
            }
        });

        let start = time::precise_time_ns();
        while root.step() { }
        let stop = time::precise_time_ns();

        println!("total time: {}", stop - start);

    }).unwrap();
}
