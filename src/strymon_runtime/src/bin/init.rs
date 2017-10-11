use std::path::Path;

use clap::{App, Arg, ArgMatches, SubCommand};

use errors::*;

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("create")
        .about("Create a new Strymon application")
        .arg(Arg::with_name("path").required(true))
}

pub fn main(args: &ArgMatches) -> Result<()> {
    let path = Path::new(args.value_of("path").unwrap());

    Err("not yet implemented")?
}
