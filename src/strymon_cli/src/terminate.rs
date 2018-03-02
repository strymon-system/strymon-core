// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use clap::{App, Arg, ArgMatches, SubCommand};
use failure::{Error, ResultExt};

use strymon_model::JobId;
use strymon_communication::Network;
use super::submit::Submitter;

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("terminate")
        .about("Send the termination signal to a running Strymon job")
        .arg(
            Arg::with_name("job")
                .value_name("JOB_ID")
                .required(true)
                .help("Numeric job identifier"),
        )
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

    let id = value_t!(args.value_of("job"), u64).context(
        "Unable to parse job id",
    )?;
    submitter.terminate(JobId(id)).wait_unwrap().map_err(
        |e| {
            format_err!("Failed to terminate job: {:?}", e)
        },
    )?;
    Ok(())
}
