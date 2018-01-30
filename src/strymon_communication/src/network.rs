// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Network type.
//  This is in its own module so that rustdoc renders its methods first.

use std::io;
use std::env;
use std::sync::Arc;

use Network;

impl Network {
    /// Creates a new default network handle.
    ///
    /// This will try to use the externally reachable hostname of the current
    /// process by reading the `STRYMON_COMM_HOSTNAME` environment variable. It will
    /// fall-back to `"localhost"` if the environment variable is not available.
    pub fn init() -> io::Result<Self> {
        // try to guess external hostname
        let hostname = if let Ok(hostname) = env::var("TIMELY_SYSTEM_HOSTNAME") {
            hostname
        } else if let Ok(hostname) = env::var("STRYMON_COMM_HOSTNAME") {
            hostname
        } else {
            warn!("unable to retrieve external hostname of machine.");
            warn!("falling back to 'localhost', set TIMELY_SYSTEM_HOSTNAME to override");

            String::from("localhost")
        };

        Network::with_hostname(hostname)
    }

    /// Creates a new instance with a set externally reachable hostname.
    pub fn with_hostname(hostname: String) -> io::Result<Self> {
        Ok(Network {
            hostname: Arc::new(hostname),
        })
    }

    /// Returns the configured externally reachable hostname of the current process.
    pub fn hostname(&self) -> String {
        (*self.hostname).clone()
    }
}
