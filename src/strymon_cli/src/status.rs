// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::BTreeMap;

use clap::{App, Arg, ArgMatches, SubCommand};
use failure::Error;

use strymon_communication::Network;
use super::submit::Submitter;

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("status")
        .about("Prints status information about a running Strymon instance")
        .arg(
            Arg::with_name("coordinator")
                .short("c")
                .long("coordinator")
                .value_name("ADDR")
                .help("Address of the coordinator")
                .takes_value(true),
        )
}

pub fn main(args: &ArgMatches) -> Result<(), Error> {
    let network = Network::new(None)?;
    let coord = args.value_of("coordinator").unwrap_or("localhost:9189");
    let submitter = Submitter::new(&network, &*coord)?;

    let executors = submitter.executors()?;
    let jobs = submitter.jobs()?;
    let publications = submitter.publications()?;
    let subscriptions = submitter.subscriptions()?;
    let topics = submitter
        .topics()?
        .into_iter()
        .map(|t| (t.id, t))
        .collect::<BTreeMap<_, _>>();

    println!("Coordinator: {}", coord);
    for executor in executors {
        let id = executor.id.0;
        println!(" Executor {}: host={:?}", id, executor.host);
        for job in jobs.iter().filter(
            |q| q.executors.contains(&executor.id),
        )
        {
            let id = job.id.0;
            let name = job
                .name
                .as_ref()
                .map(|n| format!("{:?}", n))
                .unwrap_or_else(|| String::from("<unnamed>"));

            println!("  Job {}: name={}, workers={}", id, name, job.workers);

            for publication in publications.iter().filter(|p| p.0 == job.id) {
                let topic = &topics[&publication.1];
                println!(
                    "   Publication on Topic {}: name={:?}, schema={}",
                    topic.id.0,
                    topic.name,
                    topic.schema
                );
            }

            for subscription in subscriptions.iter().filter(|p| p.0 == job.id) {
                let topic = &topics[&subscription.1];
                println!(
                    "   Subscription on Topic {}: name={:?}, schema={}",
                    topic.id.0,
                    topic.name,
                    topic.schema
                );
            }
        }
    }

    Ok(())
}
