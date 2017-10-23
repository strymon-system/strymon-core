use std::io;
use std::env;
use std::path::{Path};
use std::process::{Command, Stdio};

use serde_json::{Value, Deserializer};
use clap::{App, AppSettings, Arg, ArgGroup, ArgMatches, SubCommand};
use log::LogLevel;

use strymon_communication::Network;
use strymon_runtime::submit::Submitter;
use strymon_runtime::model::{QueryProgram, QueryId, ExecutionFormat, Executor, ExecutorId};
use strymon_runtime::coordinator::requests::Placement;

use errors::*;

fn build_binary(path: Option<&Path>, args: &ArgMatches) -> Result<String> {
    let mut cargo = Command::new("cargo");
    cargo.arg("build")
        .args(&["--message-format", "json"]);

    // translate project directory to manifest path
    let manifest = path.map(|p| p.join("Cargo.toml"));
    if let Some(toml) = manifest {
        cargo.arg("--manifest-path").arg(toml);
    }

    // pass down custom cargo flags
    if !args.is_present("--debug") {
        cargo.arg("--release");
    }

    if args.is_present("no-default-features") {
        cargo.arg("--no-default-features");
    }
    if args.is_present("all-features") {
        cargo.arg("--all-features");
    } else if let Some(list) = args.values_of("features") {
        cargo.arg("--features").arg(list.collect::<Vec<_>>().join(" "));
    };

    if let Some(name) = args.value_of("bin") {
        cargo.args(&["--bin", name]);
    } else if let Some(name) = args.value_of("example") {
        cargo.args(&["--example", name]);
    }

    // captures stderr and stdout
    cargo.stdin(Stdio::null());
    cargo.stderr(Stdio::piped());
    cargo.stdout(Stdio::piped());

    // list of all compiled binaries, we need exactly one
    let mut binaries: Vec<String> = vec![];

    info!("running: `{:?}`", cargo);
    eprintln!("Building job binary with `cargo build` (this will take a while)");

    // spawn cargo as a child process
    let mut child = cargo.spawn()?;

    // TODO(swicki): Have proper deserialize struct for parsing the messages
    let stream = Deserializer::from_reader(child.stdout.take().unwrap()).into_iter::<Value>();
    for result in stream {
        let msg = result?;
        match msg["reason"].as_str().ok_or("missing reason field in cargo message")? {
            "compiler-message" => {
                let rustc_msg = msg.get("message").ok_or("missing compiler message")?;
                let level = match rustc_msg["level"].as_str().ok_or("unable to parse message level")? {
                    "note" | "help" => LogLevel::Info,
                    "warning" => LogLevel::Warn,
                    "error" => LogLevel::Error,
                    _ => LogLevel::Debug,
                };
                log!(level, "cargo: {}", msg);
            },
            "compiler-artifact" => {
                if let Value::String(ref crate_name) = msg["package_id"] {
                    info!("compiled crate: {}", crate_name);
                }
                // TODO(swicki): When do artifacts have more than one type/kind?
                let ref crate_type = msg["target"]["crate_types"][0];
                let ref kind = msg["target"]["kind"][0];
                if crate_type == "bin" && (kind == "bin" || kind == "example") {
                    let file = msg["filenames"][0].as_str().ok_or("missing filename")?;
                    binaries.push(file.to_owned());
                }
            }
            _ => {
                debug!("unhandled cargo message: {}", msg);
            }
        };
    }

    // cargo has closed stdout, wait for it to finish
    let status = child.wait()?;
    if !status.success() {
        io::copy(child.stderr.as_mut().unwrap(), &mut io::stderr())?;
        bail!("Cargo failed with exit code {}", status.code().unwrap_or(-1));
    }

    if binaries.is_empty() {
        bail!("Cargo did not produce any binaries, a `bin` target must be available");
    }

    if binaries.len() > 1 {
        bail!("Multiple binaries for this project, please specify which one to submit using `--bin` or `--example`");
    }

    let binary = binaries.pop().unwrap();
    Ok(binary)
}

fn parse_placement(args: &ArgMatches, executors: Vec<Executor>) -> Result<Placement> {
    fn parse_err(arg: &str) -> String {
        format!("Failed to parse value of '--{}' option", arg)
    }

    // number of workers per machine
    let workers = if let Some(w) = args.value_of("workers") {
        w.parse().chain_err(|| parse_err("workers"))?
    } else {
        1
    };

    // parse placement strategy and its arguments
    match args.value_of("placement-strategy") {
        Some("random") => {
            let num_executors = args
                .value_of("num-executors")
                .expect("missing `num-executors` argument")
                .parse().chain_err(|| parse_err("num-executors"))?;
            Ok(Placement::Random(num_executors, workers))
        },
        Some("pinned") => {
            let mut pinned = vec![];
            if let Some(ids) = args.values_of("pinned-id") {
                for num in ids {
                    let id = num.parse::<u64>().chain_err(|| parse_err("pinned-id"))?;
                    pinned.push(ExecutorId(id));
                }
            } if let Some(hosts) = args.values_of("pinned-host") {
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
        },
        _ => {
            // by default choose a random executor
            Ok(Placement::Random(1, workers))
        }
    }
}

fn submit_binary(binary: String, args: &ArgMatches) -> Result<QueryId> {
    eprintln!("Submitting binary {:?}", binary);

    let coord = args.value_of("coordinator").unwrap_or("localhost:9189");
    let desc = args.value_of("description").map(String::from);

    // external hostname
    if let Some(host) = args.value_of("external-hostname") {
        env::set_var("TIMELY_SYSTEM_HOSTNAME", host);
    }

    let network = Network::init()
        .chain_err(|| "Failed to initialize network")?;
    let submitter = Submitter::new(&network, &*coord)
            .chain_err(|| "Unable to connect to coordinator")?;
    let executors = submitter.executors()
            .chain_err(|| "Failed to fetch list of executors")?;
    let placement = parse_placement(args, executors)?;

    let handle = network.upload(binary)?;

    let query = QueryProgram {
        source: handle.url(),
        format: ExecutionFormat::NativeExecutable,
        args: vec![],
    };

    submitter
        .submit(query, desc, placement)
        .wait_unwrap()
        .map_err(|e| format!("Failed to submit job: {:?}", e).into())

/*
    // assemble the placement configuration
    let placement = match parse_placement(m, executors) {
        Ok(placement) => placement,
        Err(err) => usage(options, Some(err)),
    };

    // prepare location for executors to fetch binary
    let (url, upload) = if local {
        (format!("file://{}", source), None)
    } else {
        let handle = network.upload(source).unwrap();
        (handle.url(), Some(handle))
    };

    let query = QueryProgram {
        source: url,
        format: ExecutionFormat::NativeExecutable,
        args: args,
    };

    let id = submitter.submit(query, desc, placement).wait_unwrap();
    println!("spawned query: {:?}", id);*/
}

pub fn usage<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("submit")
        .setting(AppSettings::UnifiedHelpMessage)
        .about("Submit a new Strymon application")
        .arg(Arg::with_name("path")
                .required(true)
                .help("Path to the Cargo project directory"))
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
        // Submitted job run-time configuration
        .arg(Arg::with_name("workers")
                .long("workers")
                .takes_value(true)
                .value_name("NUM")
                .display_order(301)
                .help("Number of workers per machine"))
        .arg(Arg::with_name("placement-strategy")
                .long("placement-strategy")
                .takes_value(true)
                .value_name("STRATEGY")
                .possible_values(&["pinned", "random"])
                .requires_if("pinned", "pinned-group")
                .requires_if("random", "random-group")
                .display_order(302)
                .help("Job placement strategy"))
        .arg(Arg::with_name("pinned-id")
                .long("pinned-id")
                .takes_value(true)
                .value_name("ID")
                .multiple(true)
                .require_delimiter(true)
                .conflicts_with("pinned-host")
                .display_order(303)
                .help("Comma-separated list of executor ids for `pinned` placement strategy"))
        .arg(Arg::with_name("pinned-host")
                .long("pinned-host")
                .takes_value(true)
                .value_name("HOST")
                .multiple(true)
                .require_delimiter(true)
                .conflicts_with("pinned-id")
                .display_order(304)
                .help("Comma-separated list of executor host names for `pinned` placement strategy"))
        .arg(Arg::with_name("num-executors")
                .long("num-executors")
                .takes_value(true)
                .value_name("NUM")
                .display_order(303)
                .help("Number of executors for `random` placement strategy"))
        .group(ArgGroup::with_name("pinned-group")
             .args(&["pinned-host", "pinned-id"])
             .conflicts_with("random-group"))
        .group(ArgGroup::with_name("random-group")
             .args(&["num-executors"])
             .conflicts_with("pinned-group"))
}

pub fn main(args: &ArgMatches) -> Result<()> {
    let path = args.value_of("path").map(Path::new);
    let binary = build_binary(path, args)?;
    let id = submit_binary(binary, args)?;

    println!("Successfully spawned job: {}", id.0);

    Ok(())
}
