// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![deny(missing_docs)]

//! Communication primitives for Strymon.
//!
//! This crate contains the networking primitives for Strymon. It provides
//! three different abstraction layers:
//!
//!   - [`message`](message/index.html): A frame-delimited multi-part message format.
//!   - [`transport`](transport/index.html): Bi-directional asynchronous message channels,
//!     for sending untyped [`MessageBuf`](message/struct.MessageBuf.html)s across the network.
//!   - [`rpc`](rpc/index.html): Semi-typed asynchronous, multiplexed request-response channels.
//!
//! In order to use the networking functionality, client code must initialize a
//! [`Network`](struct.Network.html) handle first.


extern crate bytes;
extern crate byteorder;

extern crate serde;
extern crate rmp_serde;
#[cfg(feature = "tracing")]
extern crate rmpv;

extern crate futures;

#[macro_use] extern crate log;

mod network;

pub mod transport;
pub mod message;
pub mod rpc;
pub mod fetch;

use std::sync::Arc;

/// Handle to the networking subsystem.
///
/// Currently, this handle only stores information about the externally reachable
/// hostname of the current process. In the future, it might be backed by a
/// dedicated networking thread to implement its methods. Thus it recommended to
/// only initialize it once per process.
#[derive(Clone, Debug)]
pub struct Network {
    hostname: Arc<String>,
}
