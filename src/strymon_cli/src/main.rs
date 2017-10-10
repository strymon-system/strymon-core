#[macro_use]
extern crate error_chain;
extern crate clap;

mod errors;
mod create;


use clap::{App, AppSettings, Arg, SubCommand};

use errors::*;

quick_main!(dispatch);

fn dispatch() -> Result<()> {
    let matches = App::new("Strymon")
        .version("0.1")
        .author("Systems Group, ETH Zürich")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("create")
                .about("Create a new Strymon application")
                .arg(Arg::with_name("path").required(true)),
        )
        .subcommand(
            SubCommand::with_name("submit")
                .about("Launch a Strymon application")
                .arg(Arg::with_name("path").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("create",  Some(matches)) => create::run(matches),
        ("submit",   Some(matches)) => unimplemented!(),
        _ => unreachable!("invalid subcommand"),
    }
}
