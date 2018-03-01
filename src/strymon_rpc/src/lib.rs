// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
#![deny(missing_docs)]

//! This crate defines the remote-procedure call protocol used between the different components
//! in Strymon Core.
//!
//! All types here are based on the framework outlined in
//! [**`strymon_communication::rpc`**](../strymon_communication/rpc/index.html). Each submodule
//! contains the type definitions for one interface. The [`coordinator`](coordinator/index.html)
//! module contains all requests sent to the coordinator by various components, while the
//! [`executor`](executor/index.html) contains the messages sent from the coordinator to a
//! registered executor.

#[macro_use]
extern crate enum_primitive_derive;
extern crate num_traits;

extern crate serde;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate typename;

extern crate strymon_communication;
extern crate strymon_model;

pub mod coordinator;
pub mod executor;
