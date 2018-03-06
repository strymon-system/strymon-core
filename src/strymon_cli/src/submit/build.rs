// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;
use std::path::Path;
use std::process::{Command, Stdio};

use clap::ArgMatches;
use failure::{Error, err_msg};
use log::Level;
use serde_json::{Value, Deserializer};

/// Given a `path` to a Cargo project, builds it and returns a path to the binary.
///
/// This makes use of Cargo's JSON metadata output. It also parses additional Cargo arguments
/// passed down in `args` (e.g. for feature selection), see the `usage` function for details.
pub fn binary(path: &Path, args: &ArgMatches) -> Result<String, Error> {
    let mut cargo = Command::new("cargo");
    cargo.arg("build").args(&["--message-format", "json"]);

    // translate project directory to manifest path
    let manifest = path.join("Cargo.toml");
    cargo.arg("--manifest-path").arg(manifest);

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
        cargo.arg("--features").arg(
            list.collect::<Vec<_>>().join(" "),
        );
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
        match msg["reason"].as_str().ok_or(err_msg(
            "missing reason field in cargo message",
        ))? {
            "compiler-message" => {
                let rustc_msg = msg.get("message").ok_or(
                    err_msg("missing compiler message"),
                )?;
                let level = match rustc_msg["level"].as_str().ok_or(err_msg(
                    "unable to parse message level",
                ))? {
                    "note" | "help" => Level::Info,
                    "warning" => Level::Warn,
                    "error" => Level::Error,
                    _ => Level::Debug,
                };
                log!(level, "cargo: {}", msg);
            }
            "compiler-artifact" => {
                if let Value::String(ref crate_name) = msg["package_id"] {
                    info!("compiled crate: {}", crate_name);
                }
                // TODO(swicki): When do artifacts have more than one type/kind?
                let ref crate_type = msg["target"]["crate_types"][0];
                let ref kind = msg["target"]["kind"][0];
                if crate_type == "bin" && (kind == "bin" || kind == "example") {
                    let file = msg["filenames"][0].as_str().ok_or(
                        err_msg("missing filename"),
                    )?;
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
        bail!(
            "Cargo failed with exit code {}",
            status.code().unwrap_or(-1)
        );
    }

    if binaries.is_empty() {
        bail!("Cargo did not produce any binaries, a `bin` target must be available");
    }

    if binaries.len() > 1 {
        bail!(
            "Multiple binaries for this project, please specify which one to submit using `--bin` or `--example`"
        );
    }

    let binary = binaries.pop().unwrap();
    Ok(binary)
}
