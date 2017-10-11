extern crate clap;
#[macro_use]
extern crate error_chain;

mod errors;
mod create;
mod submit;

use clap::{App, AppSettings, Arg, SubCommand};

use errors::*;

quick_main!(dispatch);

fn dispatch() -> Result<()> {
    let matches = App::new("Strymon")
        .version("0.1")
        .author("Systems Group, ETH Zürich")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(create::usage())
        .subcommand(submit::usage())
        .get_matches();

    match matches.subcommand() {
        ("create", Some(args)) => create::main(args),
        ("submit", Some(args)) => submit::main(args),
        _ => unreachable!("invalid subcommand"),
    }
}
