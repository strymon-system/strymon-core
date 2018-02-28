// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use clap::{App, Arg, ArgMatches, SubCommand};

use strymon_coordinator::Builder;

use errors::*;

pub mod start {
    use super::*;

    pub fn usage<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("start-coordinator")
            .about("Start the Strymon coordinator service")
            .arg(Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Port to listen on")
                .takes_value(true))
            .arg(Arg::with_name("external-hostname")
                .short("e")
                .long("external-hostname")
                .value_name("HOST")
                .help("Externally reachable hostname of the spawned coordinator")
                .takes_value(true))
    }

    pub fn main(args: &ArgMatches) -> Result<()> {
        let mut coordinator = Builder::default();

        if let Some(port) = args.value_of("port") {
            let parsed = port.parse::<u16>()
                .chain_err(|| "unable to parse port number")?;
            coordinator.port(parsed);
        }

        // externally reachable hostname of the coordinator
        if let Some(host) = args.value_of("external-hostname") {
            coordinator.hostname(host.to_owned());
        }

        coordinator.run().chain_err(|| "Failed to run coordinator")
    }
}
