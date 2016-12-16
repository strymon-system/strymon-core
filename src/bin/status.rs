extern crate timely_query;
extern crate env_logger;
extern crate getopts;

use std::process;
use std::env;
use std::io::{self, Write, Result};
use std::collections::BTreeMap;

use getopts::Options;
use timely_query::network::Network;
use timely_query::submit::Submitter;

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

fn print_status(coord: &str) -> Result<()> {
    let network = Network::init()?;
    let submitter = Submitter::new(&network, &*coord)?;
    
    let executors = submitter.executors()?;
    let queries = submitter.queries()?;
    let publications = submitter.publications()?;
    let subscriptions = submitter.subscriptions()?;
    let topics = submitter.topics()?
                    .into_iter()
                    .map(|t| (t.id, t))
                    .collect::<BTreeMap<_, _>>();

    println!("Coordinator: {}", coord);
    for executor in executors {
        let id = executor.id.0;
        println!(" Executor {}: host={:?}", id, executor.host);
        for query in queries.iter().filter(|q| q.executors.contains(&executor.id)) {
            let id = query.id.0;
            let name = query.name.as_ref()
                            .map(|n| format!("{:?}", n))
                            .unwrap_or_else(|| String::from("<unnamed>"));

            println!("  Query {}: name={}, workers={}", id, name, query.workers);
            
            for publication in publications.iter().filter(|p| p.0 == query.id) {
                let topic = &topics[&publication.1];
                println!("   Publication on Topic {}: name={:?}, schema={}",
                    topic.id.0, topic.name, topic.schema);
            }
            
            for subscription in subscriptions.iter().filter(|p| p.0 == query.id) {
                let topic = &topics[&subscription.1];
                println!("   Subscription on Topic {}: name={:?}, schema={}",
                    topic.id.0, topic.name, topic.schema);
            }
        }
    }
    
    Ok(())
}

fn main() {
    drop(env_logger::init());   

    let args: Vec<String> = env::args().skip(1).collect();
    let mut options = Options::new();
    options
        .optflag("h", "help", "display this help and exit")
        .optopt("c", "coordinator", "address of the coordinator", "ADDR");

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

    let coord = matches.opt_str("c").unwrap_or(String::from("localhost:9189"));

    if let Err(err) = print_status(&coord) {
        writeln!(io::stderr(), "Failed to communicate with coordinator at {:?}", coord).ok();
        writeln!(io::stderr(), "{}", err).ok();
        process::exit(1);
    }
}
