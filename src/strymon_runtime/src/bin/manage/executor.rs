// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use clap::{App, Arg, ArgMatches, SubCommand};

use strymon_runtime::executor;

use errors::*;

pub mod start {
    use super::*;

    pub fn usage<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("start-executor")
            .about("Start an instance of a Strymon executor")
            .arg(Arg::with_name("port-range")
                .short("p")
                .long("port-range")
                .value_name("MIN..MAX")
                .help("Port range of spawned children on this executor")
                .takes_value(true))
            .arg(Arg::with_name("external-hostname")
                .short("e")
                .long("external-hostname")
                .value_name("HOST")
                .help("Externally reachable hostname of the spawned coordinator")
                .takes_value(true))
            .arg(Arg::with_name("coordinator")
                .short("c")
                .long("coordinator")
                .value_name("ADDR")
                .help("Address of the coordinator")
                .takes_value(true))
    }

    pub fn main(args: &ArgMatches) -> Result<()> {
        let mut executor = executor::Builder::default();

        // host and port of the executor
        if let Some(addr) = args.value_of("coordinator") {
            executor.coordinator(addr.to_owned());
        }

        // externally reachable hostname of this executor
        if let Some(host) = args.value_of("external-hostname") {
            executor.host(host.to_owned());
        }

        // parse port range to be used for spawned Timely processes
        if let Some(ports) = args.value_of("port-range") {
            let split: Vec<&str> = ports.split("..").collect();
            let min = split.get(0).and_then(|m| m.parse::<u16>().ok());
            let max = split.get(1).and_then(|m| m.parse::<u16>().ok());
            if split.len() != 2 || min.is_none() || max.is_none() || min >= max {
                bail!("Invalid port range: {}", ports)
            }

            executor.ports(min.unwrap(), max.unwrap());
        }

        executor.start().chain_err(|| "Failed to start executor")
    }
}
