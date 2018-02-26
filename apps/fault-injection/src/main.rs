// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
#![allow(deprecated)]

extern crate topology_generator;
extern crate strymon_job;

use topology_generator::service::Fault;

fn main() {
    strymon_job::execute(|_, coord| {
        let arg = std::env::args().nth(1);
        let fault = match arg.as_ref().map(|s| s.as_str()) {
            Some("disconnect-random-switch") => Fault::DisconnectRandomSwitch,
            Some("disconnect-random-link") => Fault::DisconnectRandomSwitch,
            _ => {
                eprintln!("Usage: <disconnect-random-switch|disconnect-random-link>");
                std::process::exit(1);
            }
        };

        coord
            .bind_service("topology_service", false)
            .expect("failed to connect to topology generator")
            .request(&fault)
            .wait_unwrap()
            .expect("failed to inject a network fault!");
    }).unwrap();
}
