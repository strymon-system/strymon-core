// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.


use std::path::Path;
use std::ffi::OsStr;

use clap::{App, AppSettings, Arg, ArgGroup, ArgMatches, SubCommand};
use failure::{Error, ResultExt};

use strymon_communication::Network;
use strymon_model::{QueryProgram, JobId, ExecutionFormat, Executor, ExecutorId};
use strymon_rpc::coordinator::Placement;

pub use self::submitter::Submitter;

mod submitter;
mod build;

fn parse_placement(args: &ArgMatches, executors: Vec<Executor>) -> Result<Placement, Error> {
    fn parse_err(arg: &str) -> String {
        format!("Failed to parse value of '--{}' option", arg)
    }

    // number of workers per machine
    let workers = if let Some(w) = args.value_of("workers") {
        w.parse::<usize>().with_context(|_| parse_err("workers"))?
    } else {
        1
    };

    // parse placement strategy and its arguments
    match args.value_of("placement-strategy") {
        Some("random") => {
            let num_executors = args.value_of("num-executors")
                .expect("missing `num-executors` argument")
                .parse::<usize>()
                .with_context(|_| parse_err("num-executors"))?;
            Ok(Placement::Random(num_executors, workers))
        }
        Some("pinned") => {
            let mut pinned = vec![];
            if let Some(ids) = args.values_of("pinned-id") {
                for num in ids {
                    let id = num.parse::<u64>().with_context(|_| parse_err("pinned-id"))?;
                    pinned.push(ExecutorId(id));
                }
            }
            if let Some(hosts) = args.values_of("pinned-host") {
                for name in hosts {
                    let executor = executors.iter().find(|e| e.host == name);
                    if let Some(executor) = executor {
                        pinned.push(executor.id);
                    } else {
                        bail!("Unknown executor host '{}'", name);
                    }
                }
                Ok(Placement::Fixed(pinned, workers))
            } else {
                bail!("Missing executors list for pinning")
            }
        }
        _ => {
            // by default choose a random executor
            Ok(Placement::Random(1, workers))
        }
    }
}

fn submit_binary(binary: String, args: &ArgMatches) -> Result<JobId, Error> {
    eprintln!("Submitting binary {:?}", binary);

    let coord = args.value_of("coordinator").unwrap_or("localhost:9189");
    let desc = args.value_of("description").map(String::from);

    // external hostname
    let hostname = args.value_of("external-hostname").map(String::from);
    let network = Network::new(hostname).context(
        "Failed to initialize network",
    )?;

    let submitter = Submitter::new(&network, &*coord).context(
        "Unable to connect to coordinator",
    )?;
    let executors = submitter.executors().context(
        "Failed to fetch list of executors",
    )?;
    let placement = parse_placement(args, executors)?;

    let binary_name = Path::new(&binary)
        .file_name()
        .unwrap_or(OsStr::new("job_binary"))
        .to_string_lossy()
        .into_owned();

    // expose the binary on a randomly selected TCP port
    let (url, upload) = if args.is_present("no-upload") {
        (format!("file://{}", binary), None)
    } else {
        let handle = network.upload(binary)?;
        (handle.url(), Some(handle))
    };

    // collect command line arguments and pass them to spawned binary
    let args: Vec<String> = if let Some(args) = args.values_of("args") {
        args.map(String::from).collect()
    } else {
        Vec::new()
    };

    let query = QueryProgram {
        binary_name: binary_name,
        source: url,
        format: ExecutionFormat::NativeExecutable,
        args: args,
    };

    let res = submitter
        .submit(query, desc, placement)
        .wait_unwrap()
        .map_err(|e| format_err!("Failed to submit job: {:?}", e).into());

    drop(upload);

    res
}

static AFTER_HELP: &'static str = "
By default, the submitted binary is uploaded using a randomly selected TCP port. \
For this reason, the external hostname of the local machine must be known. \
Either using the `--external-hostname` option, or by setting the \
STRYMON_COMM_HOSTNAME environment variable. This functionality can be disabled \
by using the `--no-upload` option.

The `--placement-strategy` argument is used to specify to which machines the \
submitted binary is spawned on. By default, jobs will be placed on a single \
randomly selected executor. This is equivalent to `--placement-strategy random \
--num-executors 1`. To pin a job to a certain set of executors, use \
`--placement-strategy pinned` together with either `--pinned-id 0,1,2` to \
select executors based on their executor id, or use `--pinned-host host1,host2,host3` \
to specify them by hostname.

The number of worker threads per executors (default 1) can set using the \
`--workers` option. The optional job name is given through the `--description` \
option.
";

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("submit")
        .setting(AppSettings::UnifiedHelpMessage)
        .about("Submit a new Strymon application")
        .after_help(AFTER_HELP)
        .arg(Arg::with_name("path")
                .required(true)
                .help("Path to the Cargo project directory"))
        .arg(Arg::with_name("binary-path")
                .long("binary-path")
                .takes_value(true)
                .value_name("PATH")
                .help("Submit a prebuilt binary (skips invoking `cargo build`)"))
        // arguments passed down to Cargo when building
        .arg(Arg::with_name("features")
                .long("features")
                .takes_value(true)
                .multiple(true)
                .require_delimiter(true)
                .value_name("FEATURES")
                .conflicts_with("all-features")
                .display_order(101)
                .help("Comma-separated list of features to activate"))
        .arg(Arg::with_name("all-features")
                .long("all-features")
                .conflicts_with("features")
                .display_order(102)
                .help("Build all available features"))
        .arg(Arg::with_name("no-default-features")
                .long("no-default-features")
                .display_order(103)
                .help("Do not build the `default` feature"))
        .arg(Arg::with_name("bin")
                .long("bin")
                .takes_value(true)
                .value_name("NAME")
                .conflicts_with("example")
                .display_order(104)
                .help("Build and submit only the specified binary"))
        .arg(Arg::with_name("example")
                .long("example")
                .takes_value(true)
                .value_name("NAME")
                .conflicts_with("bin")
                .display_order(105)
                .help("Build and submit only the specified example"))
        .arg(Arg::with_name("debug")
                .long("debug")
                .display_order(106)
                .help("Build in debug mode instead of release mode"))
        // Strymon environment
        .arg(Arg::with_name("external-hostname")
                .short("e")
                .long("external-hostname")
                .value_name("HOST")
                .takes_value(true)
                .display_order(201)
                .help("Externally reachable hostname of the spawned coordinator"))
        .arg(Arg::with_name("coordinator")
                .short("c")
                .long("coordinator")
                .takes_value(true)
                .value_name("ADDR")
                .display_order(202)
                .help("Address of the coordinator"))
        // Job submission and description
        .arg(Arg::with_name("description")
                .long("description")
                .takes_value(true)
                .value_name("DESC")
                .display_order(301)
                .help("Human-readable description of the submitted job"))
        // Submitted job run-time configuration
        .arg(Arg::with_name("workers")
                .long("workers")
                .takes_value(true)
                .value_name("NUM")
                .display_order(401)
                .help("Number of workers per machine"))
        .arg(Arg::with_name("placement-strategy")
                .long("placement-strategy")
                .takes_value(true)
                .value_name("STRATEGY")
                .possible_values(&["pinned", "random"])
                .requires_if("pinned", "pinned-group")
                .requires_if("random", "random-group")
                .display_order(402)
                .help("Job placement strategy"))
        .arg(Arg::with_name("pinned-id")
                .long("pinned-id")
                .takes_value(true)
                .value_name("ID")
                .multiple(true)
                .require_delimiter(true)
                .conflicts_with("pinned-host")
                .display_order(403)
                .help("Comma-separated list of executor ids for the `pinned` placement strategy"))
        .arg(Arg::with_name("pinned-host")
                .long("pinned-host")
                .takes_value(true)
                .value_name("HOST")
                .multiple(true)
                .require_delimiter(true)
                .conflicts_with("pinned-id")
                .display_order(404)
                .help("Comma-separated list of executor host names for the `pinned` placement strategy"))
        .arg(Arg::with_name("num-executors")
                .long("num-executors")
                .takes_value(true)
                .value_name("NUM")
                .display_order(405)
                .help("Number of executors for the `random` placement strategy"))
        .arg(Arg::with_name("no-upload")
                .long("no-upload")
                .display_order(406)
                .help("Let the executors read the binary from their local filesystem"))
        // catch-all args after --
        .arg(Arg::with_name("args")
            .multiple(true)
            .last(true)
            .help("Arguments passed to the the spawned binary"))
        // groups
        .group(ArgGroup::with_name("pinned-group")
             .args(&["pinned-host", "pinned-id"])
             .conflicts_with("random-group"))
        .group(ArgGroup::with_name("random-group")
             .args(&["num-executors"])
             .conflicts_with("pinned-group"))
        .group(ArgGroup::with_name("binary-source-group")
             .args(&["path", "binary-path"])
             .required(true))
}

pub fn main(args: &ArgMatches) -> Result<(), Error> {
    let binary = if let Some(path) = args.value_of("path").map(Path::new) {
        build::binary(path, args)?
    } else {
        args.value_of("binary-path")
            .expect("no binary specified")
            .to_owned()
    };

    let id = submit_binary(binary, args)?;

    println!("Successfully spawned job: {}", id.0);

    Ok(())
}
