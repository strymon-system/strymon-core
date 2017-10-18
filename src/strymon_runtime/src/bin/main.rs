extern crate clap;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate serde;
extern crate serde_json;

extern crate strymon_runtime;

mod errors;
mod create;
mod submit;
mod manage;

use std::env;

use clap::{App, AppSettings, Arg};
use env_logger::{LogBuilder, LogTarget};

use errors::*;

quick_main!(dispatch);

fn dispatch() -> Result<()> {
    let matches = App::new("Strymon")
        .version("0.1")
        .author("Systems Group, ETH Zürich")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(create::usage())
        .subcommand(submit::usage())
        .subcommand(manage::usage())
        .arg(Arg::with_name("log-level")
            .short("l")
            .long("log-level")
            .takes_value(true)
            .value_name("RUST_LOG")
            .help("Set level and filters for logging"))
        .get_matches();

    // configure env_logger
    let mut logger = LogBuilder::new();
    logger.target(LogTarget::Stdout);
    if let Some(s) = matches.value_of("log-level") {
        logger.parse(s);
    } else if let Ok(s) = env::var("RUST_LOG") {
        logger.parse(&s);
    }
    logger.init().expect("failed to initialize logger");

    match matches.subcommand() {
        ("create", Some(args)) => create::main(args),
        ("submit", Some(args)) => submit::main(args),
        ("manage", Some(args)) => manage::main(args),
        _ => unreachable!("invalid subcommand"),
    }
}
