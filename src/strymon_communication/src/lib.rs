// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate bytes;
extern crate byteorder;

extern crate serde;
extern crate rmp_serde;

extern crate futures;

extern crate rand;
#[macro_use] extern crate log;

use std::io;
use std::env;
use std::sync::Arc;

pub mod transport;
pub mod message;
pub mod fetch;
pub mod rpc;

#[derive(Clone, Debug)]
pub struct Network {
    hostname: Arc<String>,
}

impl Network {
    pub fn init() -> io::Result<Self> {
        // try to guess external hostname
        let hostname = if let Ok(hostname) = env::var("TIMELY_SYSTEM_HOSTNAME") {
            hostname
        } else {
            warn!("unable to retrieve external hostname of machine.");
            warn!("falling back to 'localhost', set TIMELY_SYSTEM_HOSTNAME to override");

            String::from("localhost")
        };

        Ok(Network {
            hostname: Arc::new(hostname),
        })
    }

    pub fn hostname(&self) -> String {
        (*self.hostname).clone()
    }
}
