use std::io;
use std::path::{Path};
use std::process::{Command, Stdio};

use serde_json::{Value, Deserializer};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use log::LogLevel;

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
    } else if let Some(list) = args.value_of("features") {
        cargo.args(&["--features", list]);
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
                // TODO(swicki): When do artifacts have more than one type/kind
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

    Ok(binaries.pop().unwrap())
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
                .value_name("FEATURES")
                .conflicts_with("all-features")
                .display_order(101)
                .help("Space-separated list of features to activate"))
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
        .arg(Arg::with_name("placement")
                .long("placement")
                .takes_value(true)
                .possible_values(&["fixed", "random"])
                .display_order(302)
                .help("Job placement strategy"))
        .arg(Arg::with_name("fixed-ids")
                .long("fixed-ids")
                .takes_value(true)
                .display_order(303)
                .help("List of executor ids for `fixed` placement"))
        .arg(Arg::with_name("fixed-hosts")
                .long("fixed-hosts")
                .takes_value(true)
                .display_order(304)
                .help("List of executor host names for `fixed` placement"))
}

pub fn main(args: &ArgMatches) -> Result<()> {
    let path = args.value_of("path").map(Path::new);
    let binary = build_binary(path, args)?;

    Err("not yet implemented")?
}
