use std::time::{Duration, UNIX_EPOCH};
use std::fs::File;
use std::path::Path;
use std::io::BufWriter;

use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Inspect;

pub struct Throughput {
    count: u64,
    last: u32,
    file: BufWriter<File>,
}

impl Throughput {
    fn now() -> u32 {
        // 0 signals an error, we don't want to panic
        UNIX_EPOCH.elapsed().unwrap_or(Duration::from_secs(0)).as_secs() as u32
    }

    fn new<P: AsRef<Path>>(file: P) -> Self {
        Throughput {
            count: 0,
            last: Throughput::now(),
            file: BufWriter::new(File::create(file)
                    .expect("failed to create throughput log")),
        }
    }

    fn flush(&mut self) {
        use ::std::io::Write;
        let res = write!(self.file, "{},{}\n", self.last + 1, self.count);
        if let Err(err) = res {
            println!("warning: failed to dump buffer in {:?}", err);
        }
    }

    fn arrival(&mut self, additional: u64) {
        let now = Throughput::now();
        if now > self.last {
            if self.count > 0 {
                self.flush();
            }
            self.last = now;
            self.count = additional;
        } else {
            self.count += additional;
        }
    }
}

impl Drop for Throughput {
    fn drop(&mut self) {
        self.flush();
    }
}

pub trait ThroughputPerSec {
    fn throughput_per_sec<P: AsRef<Path>>(&self, logfile: P) -> Self;
}

impl<S: Scope, D: Data> ThroughputPerSec for Stream<S, D> {
    fn throughput_per_sec<P: AsRef<Path>>(&self, logfile: P) -> Self {
        let mut monitor = Throughput::new(logfile);
        self.inspect_batch(move |_, xs| monitor.arrival(xs.len() as u64))
    }
}

