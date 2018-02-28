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
    /// The `hostname` argument should be set to an externally reachable
    /// hostname of the current process. If it is `None`, this constructor
    /// will try to retrieve it by reading the `STRYMON_COMM_HOSTNAME` environment
    /// variable. If both the argument and the environment variable are missing,
    /// it falls back to `"localhost"`.
    pub fn new<T: Into<Option<String>>>(hostname: T) -> io::Result<Self> {
        // try to guess external hostname
        let hostname = if let Some(hostname_arg) = hostname.into() {
            hostname_arg
        } else if let Ok(hostname_env) = env::var("STRYMON_COMM_HOSTNAME") {
            hostname_env
        } else {
            warn!("unable to retrieve external hostname of machine.");
            warn!("falling back to 'localhost', set STRYMON_COMM_HOSTNAME to override");

            String::from("localhost")
        };

        Ok(Network {
            hostname: Arc::new(hostname),
        })
    }

    /// Returns the configured externally reachable hostname of the current process.
    pub fn hostname(&self) -> String {
        (*self.hostname).clone()
    }
}
