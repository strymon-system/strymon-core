// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate enum_primitive_derive;
extern crate num_traits;

extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate strymon_communication;
extern crate strymon_model;

pub mod coordinator;
pub mod executor;
