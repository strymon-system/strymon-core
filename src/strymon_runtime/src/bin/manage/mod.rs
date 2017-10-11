use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};

mod coordinator;
mod executor;

use errors::*;

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("manage")
        .about("Manage the Strymon cluster")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(coordinator::start::usage())
        .subcommand(executor::start::usage())
}

pub fn main(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        ("start-coordinator", Some(args)) => coordinator::start::main(args),
        ("start-executor", Some(args)) => executor::start::main(args),
        _ => unreachable!("invalid subcommand"),
    }
}
