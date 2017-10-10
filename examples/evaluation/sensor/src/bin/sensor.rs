extern crate timely;
extern crate timely_query;

use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;
use std::path::Path;
use std::thread;
use std::time::Duration;

use timely::dataflow::Scope;
use timely::dataflow::operators::Input;

use timely_query::publish::Partition;

// timestamp,humidity,pir,motion,mic,temperature
type SensorData = (u64, (f32, i32, i32, i32, f32));

fn main() {
    timely_query::execute(|root, coord| {
            let mut input = root.scoped::<u64, _, _>(|scope| {
                let (input, stream) = scope.new_input();

                coord.publish("sensor", &stream, Partition::Merge)
                    .expect("failed to publish");

                input
            });

            let n = root.index() + 1;
            let path = ::std::env::args().skip(1).next().expect("require path argument");

            let sensor = Sensor::read(format!("{}/sensor{}.csv", path, n));
            for (ts, data) in sensor {
                if &ts > input.epoch() {
                    input.advance_to(ts);
                }
                input.send(data);
                root.step();
            }
        })
        .unwrap();
}

struct Sensor {
    source: Box<Iterator<Item = SensorData>>,
}

impl Sensor {
    fn read<P: AsRef<Path>>(path: P) -> Self {
        let f = File::open(path).expect("failed to open file");
        let r = BufReader::new(f);

        let lines = r.lines().skip(1).map(|r| r.expect("failed to read line"));
        let iter = lines.map(|l| {
            let mut data = l.split(",");
            let ts = data.next().unwrap().parse::<u64>().expect("ts");
            let hum = data.next().unwrap().parse::<f32>().unwrap_or(0.0);
            let pir = data.next().unwrap().parse::<i32>().unwrap_or(0);
            let motion = data.next().unwrap().parse::<i32>().unwrap_or(0);
            let mic = data.next().unwrap().parse::<i32>().unwrap_or(0);
            let temp = data.next().unwrap().parse::<f32>().unwrap_or(0.0);

            thread::sleep(Duration::from_millis(1));

            (ts, (hum, pir, motion, mic, temp))
        });

        Sensor { source: Box::new(iter) }
    }
}

impl Iterator for Sensor {
    type Item = SensorData;

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}
