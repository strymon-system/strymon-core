extern crate timely_query;
extern crate env_logger;
extern crate getopts;

use std::io::{self, Write};
use std::env;
use std::process;

use getopts::Options;
use timely_query::executor;

fn usage(opts: Options, err: Option<String>) -> ! {
    let program = env::args().next().unwrap_or(String::from("executor"));
    let brief = opts.short_usage(&program);

    if let Some(msg) = err {
        writeln!(io::stderr(), "{}\n\n{}", msg, brief).unwrap();
        process::exit(1);
    } else {
        println!("{}", opts.usage(&brief));
        process::exit(0);
    }
}

fn main() {
    drop(env_logger::init());   

    let args: Vec<String> = env::args().skip(1).collect();
    let mut options = Options::new();
    options
        .optflag("h", "help", "display this help and exit")
        .optopt("c", "coordinator", "address of the coordinator", "ADDR")
        .optopt("e", "external", "externally reachable hostname of this machine", "HOSTNAME")
        .optopt("p", "ports", "port range of spawned queries", "MIN..MAX");

    let matches = match options.parse(&args) {
        Ok(m) => m,
        Err(f) => usage(options, Some(f.to_string()))
    };

    if matches.opt_present("h") {
        usage(options, None);
    } else if !matches.free.is_empty() {
        let free = matches.free.join(", ");
        usage(options, Some(format!("Unrecognized options: {}", free)))
    }

    let mut executor = executor::Builder::default();

    // host and port of the executor
    if let Some(addr) = matches.opt_str("c") {
        executor.coordinator(addr);
    }

    // externally reachable hostname of this executor
    if let Some(host) = matches.opt_str("e") {
        executor.host(host);
    }

    // parse port range to be used for spawned Timely processes
    if let Some(ports) = matches.opt_str("p") {
        let split: Vec<&str> = ports.split("..").collect();
        let min = split.get(0).and_then(|m| m.parse::<u16>().ok());
        let max = split.get(1).and_then(|m| m.parse::<u16>().ok());
        if split.len() != 2 || min.is_none() || max.is_none() {
            usage(options, Some(format!("Invalid port range: {}", ports)))
        }

        executor.ports(min.unwrap(), max.unwrap());
    }

    if let Err(err) = executor.start() {
        writeln!(io::stderr(), "Failed to start executor: {}", err).unwrap();
        process::exit(1);
    }
}
