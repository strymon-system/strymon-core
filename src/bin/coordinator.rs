extern crate timely_query;
extern crate env_logger;
extern crate getopts;

use std::io::{self, Write};
use std::env;
use std::process;

use getopts::Options;
use timely_query::coordinator;

fn usage(opts: Options, err: Option<String>) -> ! {
    let program = env::args().next().unwrap_or(String::from("coordinator"));
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
    options.optflag("h", "help", "display this help and exit")
        .optopt("e",
                "external",
                "externally reachable hostname of this machine",
                "HOSTNAME")
        .optopt("p", "port", "port to listen on", "PORT");

    let matches = match options.parse(&args) {
        Ok(m) => m,
        Err(f) => usage(options, Some(f.to_string())),
    };

    if matches.opt_present("h") {
        usage(options, None);
    } else if !matches.free.is_empty() {
        let free = matches.free.join(", ");
        usage(options, Some(format!("Unrecognized options: {}", free)))
    }

    let mut coordinator = coordinator::Builder::default();

    // network port of the coordinator
    if let Some(port) = matches.opt_str("p") {
        let parsed = port.parse::<u16>();
        if let Err(err) = parsed {
            usage(options,
                  Some(format!("Unable to parse port {:?}: {}", port, err)))
        }

        coordinator.port(parsed.unwrap());
    }

    // externally reachable hostname of the coordinator
    if let Some(host) = matches.opt_str("e") {
        coordinator.host(host);
    }

    if let Err(err) = coordinator.run() {
        writeln!(io::stderr(), "Failed to run coordinator: {}", err).unwrap();
        process::exit(1);
    }
}
