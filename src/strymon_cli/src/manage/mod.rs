// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use clap::{App, AppSettings, ArgMatches, SubCommand};
use failure::Error;

mod coordinator;
mod executor;

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("manage")
        .about("Manage the Strymon cluster")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::Hidden)
        .subcommand(coordinator::start::usage())
        .subcommand(executor::start::usage())
}

pub fn main(args: &ArgMatches) -> Result<(), Error> {
    match args.subcommand() {
        ("start-coordinator", Some(args)) => coordinator::start::main(args),
        ("start-executor", Some(args)) => executor::start::main(args),
        _ => unreachable!("invalid subcommand"),
    }
}
